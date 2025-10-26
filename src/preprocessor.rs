use duckdb::Connection;
use anyhow::Result;
use rand::Rng;
use std::io::{self, Write};
use std::time::Duration;
use std::thread;
use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use rayon::prelude::*;

use crate::data_loader::MATERIALIZED_TABLE;

const MAX_RETRIES: u32 = 5;
const BASE_DELAY_MS: u64 = 100;

#[derive(Clone)]
struct SqlTask {
    sql: String,
    description: String,
}

struct TaskBatch {
    tasks: Vec<SqlTask>,
    description: String,
}

fn execute_with_retry(con: &Connection, sql: &str, description: &str) -> Result<()> {
    let mut retry_count = 0;
    let mut delay = BASE_DELAY_MS;
    
    loop {
        match con.execute(sql, []) {
            Ok(_) => return Ok(()),
            Err(e) => {
                retry_count += 1;
                if retry_count >= MAX_RETRIES {
                    eprintln!("Warning: Failed to {} after {} retries: {}", description, MAX_RETRIES, e);
                    return Ok(());
                }
                
                let jitter = rand::thread_rng().gen_range(0..=50);
                let total_delay = delay + jitter;
                eprintln!("Warning: {} failed (attempt {}/{}): {}. Retrying in {}ms...", 
                    description, retry_count, MAX_RETRIES, e, total_delay);
                
                thread::sleep(Duration::from_millis(total_delay));
                delay *= 2;
            }
        }
    }
}

/// Generate all index combinations for parallel processing
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

fn build_index_batch() -> TaskBatch {
    let columns = vec![
        "week", "day", "hour", 
        "type", "advertiser_id", "publisher_id",
        "country"
    ];
    
    let mut tasks = Vec::new();
    
    for column in &columns {
        let index_name = format!("idx_{}", column);
        let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}({})", index_name, MATERIALIZED_TABLE, column);
        tasks.push(SqlTask {
            sql,
            description: format!("create index {}", index_name),
        });
    }
    
    for (i, col1) in columns.iter().enumerate() {
        for col2 in &columns[i + 1..] {
            let index_name = format!("idx_{}_{}", col1, col2);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}({}, {})", index_name, MATERIALIZED_TABLE, col1, col2);
            tasks.push(SqlTask {
                sql,
                description: format!("create composite index {}", index_name),
            });
        }
    }
    
    let total_single = columns.len();
    let total_composite: usize = (0..columns.len()).map(|i| columns.len() - i - 1).sum();
    let total = total_single + total_composite;
    
    TaskBatch {
        tasks,
        description: format!("Creating {} indexes ({} single, {} composite)...", total, total_single, total_composite),
    }
}

fn build_rollup_batch() -> TaskBatch {
    let rollups = vec![
        ("advertiser_country_rollups", "advertiser_id, country"),
        ("advertiser_publisher_rollups", "advertiser_id, publisher_id"),
        ("advertiser_type_country_rollups", "advertiser_id, type, country"),
        ("advertiser_type_publisher_rollups", "advertiser_id, type, publisher_id"),
        ("advertiser_type_rollups", "advertiser_id, type"),
        ("day_advertiser_country_rollups", "day, advertiser_id, country"),
        ("day_advertiser_rollups", "day, advertiser_id"),
        ("day_advertiser_type_rollups", "day, advertiser_id, type"),
        ("day_country_rollups", "day, country"),
        ("day_publisher_country_rollups", "day, publisher_id, country"),
        ("day_publisher_rollups", "day, publisher_id"),
        ("day_type_country_rollups", "day, type, country"),
        ("day_type_publisher_country_rollups", "day, type, publisher_id, country"),
        ("day_type_publisher_rollups", "day, type, publisher_id"),
        ("day_type_rollups", "day, type"),
        ("hour_country_rollups", "hour, country"),
        ("hour_day_rollups", "hour, day"),
        ("hour_type_country_rollups", "hour, type, country"),
        ("hour_type_rollups", "hour, type"),
        ("minute_country_rollups", "minute, country"),
        ("minute_type_rollups", "minute, type"),
        ("publisher_country_rollups", "publisher_id, country"),
        ("type_country_rollups", "type, country"),
        ("type_publisher_country_rollups", "type, publisher_id, country"),
        ("type_publisher_rollups", "type, publisher_id"),
        ("type_rollups", "type"),
        ("week_advertiser_country_rollups", "week, advertiser_id, country"),
        ("week_advertiser_rollups", "week, advertiser_id"),
        ("week_advertiser_type_country_rollups", "week, advertiser_id, type, country"),
        ("week_advertiser_type_rollups", "week, advertiser_id, type"),
        ("week_country_rollups", "week, country"),
        ("week_day_country_rollups", "week, day, country"),
        ("week_day_rollups", "week, day"),
        ("week_day_type_country_rollups", "week, day, type, country"),
        ("week_day_type_rollups", "week, day, type"),
        ("week_hour_country_rollups", "week, hour, country"),
        ("week_hour_rollups", "week, hour"),
        ("week_hour_type_country_rollups", "week, hour, type, country"),
        ("week_hour_type_rollups", "week, hour, type"),
        ("week_publisher_country_rollups", "week, publisher_id, country"),
        ("week_publisher_rollups", "week, publisher_id"),
        ("week_type_country_rollups", "week, type, country"),
        ("week_type_publisher_country_rollups", "week, type, publisher_id, country"),
        ("week_type_publisher_rollups", "week, type, publisher_id"),
        ("week_type_rollups", "week, type"),
    ];
    
    let aggregations = vec![
        "COUNT(*) as count",
        "SUM(bid_price) as sum_bid_price",
        "SUM(total_price) as sum_total_price",
        "AVG(bid_price) as avg_bid_price",
        "AVG(total_price) as avg_total_price",
        "MIN(bid_price) as min_bid_price",
        "MAX(bid_price) as max_bid_price",
        "MIN(total_price) as min_total_price",
        "MAX(total_price) as max_total_price"
    ];
    
    let agg_str = aggregations.join(", ");
    
    let mut tasks = Vec::new();
    for (table_name, group_cols) in rollups {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT {}, {} FROM {} GROUP BY {}",
            table_name, group_cols, agg_str, MATERIALIZED_TABLE, group_cols
        );
        tasks.push(SqlTask {
            sql,
            description: format!("create rollup table {}", table_name),
        });
    }
    
    let total = tasks.len();
    TaskBatch {
        tasks,
        description: format!("Creating {} rollup tables...", total),
    }
}

fn execute_batch(con: &Connection, batch: &TaskBatch) {
    let total = batch.tasks.len();
    println!("{}", batch.description);
    
    let mut success = 0;
    
    for (i, task) in batch.tasks.iter().enumerate() {
        print!("  [{}/{}] {}...", i + 1, total, task.description);
        io::stdout().flush().unwrap();
        
        let result = execute_with_retry(con, &task.sql, &task.description);
        
        match result {
            Ok(_) => {
                println!(" OK");
                success += 1;
            }
            Err(e) => {
                println!(" FAILED after retries: {}", e);
            }
        }
    }
    
    println!("{} / {} tasks completed successfully", success, total);
}

#[allow(dead_code)]
pub fn preprocess(con: &Connection) -> Result<()> {
    println!("Starting preprocessing...");
    
    let index_batch = build_index_batch();
    let rollup_batch = build_rollup_batch();
    
    execute_batch(con, &index_batch);
    
    println!("Updating table statistics...");
    let _ = con.execute(&format!("ANALYZE {}", MATERIALIZED_TABLE), []);
    
    execute_batch(con, &rollup_batch);
    
    println!("Preprocessing complete");
    
    Ok(())
}

#[allow(dead_code)]
pub fn create_indexes_on_all_columns(con: &Connection) -> Result<()> {
    // Fallback to sequential execution if no db_path
    let index_batch = build_index_batch();
    execute_batch(con, &index_batch);
    
    println!("Updating table statistics...");
    let _ = con.execute(&format!("ANALYZE {}", MATERIALIZED_TABLE), []);
    
    Ok(())
}

pub fn create_indexes_concurrent_file(db_path: &PathBuf) -> Result<()> {
    let total_start = std::time::Instant::now();
    
    // Define all columns for indexing
    let columns = vec![
        "week", "day", "hour", "minute", 
        "type", "advertiser_id", "publisher_id",
        "country", "user_id"
    ];
    
    // Generate all index combinations for parallel processing
    let index_combinations = generate_index_combinations(&columns);
    let total_indexes = index_combinations.len();
    
    println!("Creating {} indexes in parallel...", total_indexes);
    
    let success_counter = Arc::new(AtomicUsize::new(0));
    let db_path = db_path.clone();
    
    index_combinations
        .par_iter()
        .for_each(|(name, cols)| {
            let sql = format!("CREATE INDEX IF NOT EXISTS idx_{}_{} ON {}({})", 
                MATERIALIZED_TABLE, name, MATERIALIZED_TABLE, cols);
            
            loop {
                match Connection::open(&db_path) {
                    Ok(thread_con) => {
                        match thread_con.execute(&sql, []) {
                            Ok(_) => {
                                let current_count = success_counter.fetch_add(1, Ordering::Relaxed);
                                println!("Created index {}/{}: {}", current_count + 1, total_indexes, name);
                                return;
                            },
                            Err(e) => {
                                println!("Error creating index {}: {} - retrying...", name, e);
                                continue;
                            }
                        }
                    },
                    Err(e) => {
                        println!("Connection error for index {}: {} - retrying...", name, e);
                        continue;
                    }
                }
            }
        });
    
    let final_success = success_counter.load(Ordering::Relaxed);
    println!("All {} indexes created successfully!", final_success);
    
    let total_time = total_start.elapsed();
    println!("Index creation complete: {:.3}s", total_time.as_secs_f64());
    
    Ok(())
}

#[allow(dead_code)]
pub fn create_indexes_concurrent_imemory(_db_name: &str) -> Result<()> {
    // DEPRECATED: In-memory concurrent indexing causes segfaults
    // Use create_indexes_concurrent_file instead
    panic!("In-memory concurrent indexing is not safe. Use create_indexes_concurrent_file instead.");
}

pub fn create_rollup_tables(con: &Connection) -> Result<()> {
    let rollup_batch = build_rollup_batch();
    execute_batch(con, &rollup_batch);
    
    Ok(())
}


