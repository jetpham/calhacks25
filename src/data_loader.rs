use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;
use std::time::Instant;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use crate::profiler::{ProfilingConfig, ProfilingMode, setup_profiling};
use rayon::prelude::*;
use indicatif::{ProgressBar, ProgressStyle};

const TABLE_NAME: &str = "events";

/// Generate all index combinations for sequential processing
fn generate_index_combinations(columns: &[&str]) -> Vec<(String, String)> {
    let mut combinations = Vec::new();
    
    // Single column indexes
    for col in columns {
        combinations.push((col.to_string(), col.to_string()));
    }
    
    // Two-column combinations
    for i in 0..columns.len() {
        for j in (i+1)..columns.len() {
            let cols = format!("{}, {}", columns[i], columns[j]);
            let name = format!("{}_{}", columns[i], columns[j]);
            combinations.push((name, cols));
        }
    }
    
    // Three-column combinations
    for i in 0..columns.len() {
        for j in (i+1)..columns.len() {
            for k in (j+1)..columns.len() {
                let cols = format!("{}, {}, {}", columns[i], columns[j], columns[k]);
                let name = format!("{}_{}_{}", columns[i], columns[j], columns[k]);
                combinations.push((name, cols));
            }
        }
    }
    
    combinations
}

/// Load data from CSV files into DuckDB with persistent storage and indexes
pub fn load_data(con: &Connection, data_dir: &PathBuf, db_path: &PathBuf) -> Result<()> {
    let total_start = Instant::now();
    
    // Step 0: Configure DuckDB for persistent storage with memory limits
    let step_start = Instant::now();
    
    // Set memory limit to 16GB to prevent memory explosion
    con.execute("SET memory_limit = '16GB'", [])?;
    
    // Configure temp directory for better disk management
    con.execute("SET temp_directory = 'profiling/temp'", [])?;
    
    con.execute("SET enable_progress_bar = false", [])?;
    
    // Setup profiling
    let profiling_config = ProfilingConfig {
        mode: ProfilingMode::Json,
        output_dir: PathBuf::from("profiling"),
        enable_detailed: true,
        enable_optimizer_metrics: true,
        enable_planner_metrics: true,
        enable_physical_planner_metrics: true,
    };
    
    setup_profiling(con, &profiling_config)?;
    let config_time = step_start.elapsed();
    println!("DuckDB configuration: {:.3}s", config_time.as_secs_f64());
    
    // Step 1: File discovery
    let step_start = Instant::now();
    let csv_files: Vec<_> = fs::read_dir(data_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "csv" && path.file_name()?.to_string_lossy().starts_with("events_part_") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    let file_discovery_time = step_start.elapsed();
    println!("File discovery: {:.3}s (found {} files)", file_discovery_time.as_secs_f64(), csv_files.len());

    if csv_files.is_empty() {
        return Err(anyhow::anyhow!("No events_part_*.csv files found in {:?}", data_dir));
    }

    // Step 2: Table creation and data loading with optimized data types
    let step_start = Instant::now();
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.display());
    
    con.execute(&format!(
        r#"
        CREATE OR REPLACE TABLE {} AS
        WITH raw AS (
          SELECT *
          FROM read_csv(
            '{}',
            AUTO_DETECT = FALSE,
            HEADER = TRUE,
            union_by_name = TRUE,
            COLUMNS = {{
              'ts': 'BIGINT',
              'type': 'VARCHAR',
              'auction_id': 'VARCHAR',
              'advertiser_id': 'INTEGER',
              'publisher_id': 'INTEGER',
              'bid_price': 'DOUBLE',
              'user_id': 'BIGINT',
              'total_price': 'DOUBLE',
              'country': 'VARCHAR'
            }}
          )
        ),
        casted AS (
          SELECT
            to_timestamp(ts / 1000.0)                    AS ts,
            type,
            auction_id,
            advertiser_id,
            publisher_id,
            COALESCE(bid_price, 0.0)                     AS bid_price,
            user_id,
            COALESCE(total_price, 0.0)                   AS total_price,
            country,
            CASE 
              WHEN ts IS NOT NULL AND ts > 0 THEN DATE(DATE_TRUNC('week', to_timestamp(ts / 1000.0)))
              ELSE NULL 
            END AS week,
            DATE(to_timestamp(ts / 1000.0))              AS day,
            DATE_TRUNC('hour', to_timestamp(ts / 1000.0)) AS hour,
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-%m-%d %H:%M') AS minute
          FROM raw
        )
        SELECT
          ts,
          week,
          day,
          hour,
          minute,
          type,
          auction_id,
          advertiser_id,
          publisher_id,
          bid_price,
          user_id,
          total_price,
          country
        FROM casted;
        "#,
        TABLE_NAME, csv_pattern
    ), [])?;
    let table_creation_time = step_start.elapsed();
    println!("Table creation & data loading: {:.3}s", table_creation_time.as_secs_f64());

    // Step 3: Create ALL possible index permutations for maximum query speed
    let step_start = Instant::now();
    
    // Define all columns for indexing (excluding problematic week column)
    let columns = vec![
        "type", "country", "day", "hour", "minute", 
        "advertiser_id", "publisher_id", "user_id"
    ];
    
    // Generate all index combinations for parallel processing
    let index_combinations = generate_index_combinations(&columns);
    let total_indexes = index_combinations.len();
    
    println!("Creating {} indexes in parallel...", total_indexes);
    
    // Process indexes in parallel with infinite retry until success
    let success_counter = Arc::new(AtomicUsize::new(0));
    
    // Create progress bar
    let pb = ProgressBar::new(total_indexes as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .unwrap()
            .progress_chars("#>-")
    );
    pb.set_message("Creating indexes in parallel...");
    
    println!("Processing {} indexes in parallel with infinite retry...", total_indexes);
    
    // Frankenstein approach: Each index retries infinitely until success
    index_combinations
        .par_iter()
        .for_each(|(name, cols)| {
            let sql = format!("CREATE INDEX IF NOT EXISTS idx_{}_{} ON {}({})", 
                TABLE_NAME, name, TABLE_NAME, cols);
            
            // Retry infinitely until success
            loop {
                // Create connection and execute
                match Connection::open(db_path) {
                    Ok(thread_con) => {
                            match thread_con.execute(&sql, []) {
                                Ok(_) => {
                                    let current_count = success_counter.fetch_add(1, Ordering::Relaxed);
                                    pb.set_position((current_count + 1) as u64);
                                    pb.set_message(format!("Created: {}", name));
                                    return;
                                },
                                Err(e) => {
                                    // Print error and retry immediately
                                    println!("Error creating index {}: {} - retrying...", name, e);
                                    continue;
                                }
                            }
                    },
                    Err(e) => {
                        // Print connection error and retry immediately
                        println!("Connection error for index {}: {} - retrying...", name, e);
                        continue;
                    }
                }
            }
        });
    
    pb.finish_with_message("All indexes created successfully!");
    
    // All indexes should succeed since we retry infinitely
    let final_success = success_counter.load(Ordering::Relaxed);
    println!("All {} indexes created successfully!", final_success);
    
    let all_indexes_time = step_start.elapsed();
    println!("All index permutations created: {:.3}s", all_indexes_time.as_secs_f64());
    
    // Step 4: Pre-warm the database
    let step_start = Instant::now();
    con.execute("SELECT COUNT(*) FROM events", [])?;
    let prewarm_time = step_start.elapsed();
    println!("Database pre-warming: {:.3}s", prewarm_time.as_secs_f64());
    
    // Step 5: Analyze the table for better query planning
    let step_start = Instant::now();
    con.execute("ANALYZE events", [])?;
    let analyze_time = step_start.elapsed();
    println!("Table analysis: {:.3}s", analyze_time.as_secs_f64());
    
    // Step 6: DuckDB optimization settings
    let step_start = Instant::now();
    con.execute("SET enable_progress_bar = false", [])?; // Disable progress bar for cleaner output
    
    // Additional DuckDB optimizations for better index usage
    con.execute("SET enable_progress_bar = false", [])?;
    
    let optimization_time = step_start.elapsed();
    println!("DuckDB optimization settings: {:.3}s", optimization_time.as_secs_f64());

    let total_time = total_start.elapsed();
    println!("\n=== Data Loading Complete ===");
    println!("Total time: {:.3}s", total_time.as_secs_f64());
    println!("Successfully created {}/{} indexes", final_success, total_indexes);
    println!("Database saved to: {:?}", db_path);
    println!("Ready for lightning-fast query execution! ⚡");
    
    Ok(())
}

/// Load a database from a file
pub fn load_database_from_file(db_path: &PathBuf) -> Result<Connection> {
    let start = Instant::now();
    
    // Check if the database file exists
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Database file does not exist: {:?}", db_path));
    }
    
    // Open the database file
    let con = Connection::open(db_path)?;
    
    let duration = start.elapsed();
    println!("Database loaded from {:?} in {:.3}s", db_path, duration.as_secs_f64());
    
    Ok(con)
}

