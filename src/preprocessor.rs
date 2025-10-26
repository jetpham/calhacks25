use duckdb::Connection;
use anyhow::Result;
use rand::Rng;
use std::io::{self, Write};
use std::time::Duration;
use std::thread;

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
    
    
    combinations
}

fn build_index_batch() -> TaskBatch {
    let columns = vec![
        "week", "day", "hour", "minute", 
        "type", "advertiser_id", "publisher_id",
        "country", "user_id"
    ];
    
    let mut tasks = Vec::new();
    let index_combinations = generate_index_combinations(&columns);
    let total_indexes = index_combinations.len();
    
    for (name, cols) in index_combinations {
        let index_name = format!("idx_{}_{}", MATERIALIZED_TABLE, name);
        let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}({})", index_name, MATERIALIZED_TABLE, cols);
        tasks.push(SqlTask {
            sql,
            description: format!("create index {}", name),
        });
    }
    
    TaskBatch {
        tasks,
        description: format!("Creating {} indexes...", total_indexes),
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

pub fn create_indexes(con: &Connection) -> Result<()> {
    let total_start = std::time::Instant::now();
    let index_batch = build_index_batch();
    execute_batch(con, &index_batch);
    
    println!("Updating table statistics...");
    let _ = con.execute(&format!("ANALYZE {}", MATERIALIZED_TABLE), []);
    
    println!("Index creation complete: {:.3}s", total_start.elapsed().as_secs_f64());
    
    Ok(())
}



pub fn create_rollup_tables(con: &Connection) -> Result<()> {
    let total_start = std::time::Instant::now();
    let rollup_batch = build_rollup_batch();
    execute_batch(con, &rollup_batch);
    
    println!("Rollup tables creation complete: {:.3}s", total_start.elapsed().as_secs_f64());
    
    Ok(())
}



