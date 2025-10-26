use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;
use std::time::Instant;
use crate::profiler::{ProfilingConfig, ProfilingMode, setup_profiling};

const TABLE_NAME: &str = "events";

/// Load data from CSV files into DuckDB with persistent storage
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
        -- Create optimized enums
        CREATE TYPE event_type AS ENUM ('serve', 'impression', 'click', 'purchase');
        CREATE TYPE country_code AS ENUM ('US', 'CA', 'DE', 'FR', 'JP', 'MX', 'GB', 'BR', 'KR', 'AU', 'IN', 'ES');
        
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
              'bid_price': 'DECIMAL(10,4)',
              'user_id': 'BIGINT',
              'total_price': 'DECIMAL(10,4)',
              'country': 'VARCHAR'
            }}
          )
        ),
        casted AS (
          SELECT
            to_timestamp(ts / 1000.0)                    AS ts,
            type::event_type                             AS type,
            auction_id::UUID                             AS auction_id,
            advertiser_id,
            publisher_id,
            COALESCE(bid_price, 0.0)                     AS bid_price,
            user_id,
            ROUND(COALESCE(total_price, 0.0), 2)         AS total_price,
            country::country_code                        AS country,
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

    // Step 3: Pre-warm the database
    let step_start = Instant::now();
    con.execute("SELECT COUNT(*) FROM events", [])?;
    let prewarm_time = step_start.elapsed();
    println!("Database pre-warming: {:.3}s", prewarm_time.as_secs_f64());
    
    // Step 4: Analyze the table for better query planning
    let step_start = Instant::now();
    con.execute("ANALYZE events", [])?;
    let analyze_time = step_start.elapsed();
    println!("Table analysis: {:.3}s", analyze_time.as_secs_f64());
    
    // Step 5: DuckDB optimization settings
    let step_start = Instant::now();
    con.execute("SET enable_progress_bar = false", [])?; // Disable progress bar for cleaner output
    
    let optimization_time = step_start.elapsed();
    println!("DuckDB optimization settings: {:.3}s", optimization_time.as_secs_f64());

    let total_time = total_start.elapsed();
    println!("\n=== Optimized Data Loading Complete ===");
    println!("Total time: {:.3}s", total_time.as_secs_f64());
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

