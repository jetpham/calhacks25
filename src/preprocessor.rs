use duckdb::Connection;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};

use crate::data_loader::MATERIALIZED_TABLE;

const ENABLE_INDEX_CREATION: bool = true;

/// Create indexes on specified columns for better query performance
pub fn create_indexes_on_all_columns(con: &Connection) -> Result<()> {
    if !ENABLE_INDEX_CREATION {
        println!("Index creation disabled (ENABLE_INDEX_CREATION = false)");
        return Ok(());
    }
    
    // Only index low-cardinality columns (avoid ts and auction_id which are 2-3GB each)
    // Keep indexes under 16GB total
    let columns = vec![
        "week", "day", "hour", 
        "type", "advertiser_id", "publisher_id",
        "country"
    ];
    
    // Calculate total indexes
    let total_single = columns.len();
    let total_composite: usize = (0..columns.len()).map(|i| columns.len() - i - 1).sum();
    let total_indexes = total_single + total_composite;
    
    println!("Creating {} total indexes ({} single, {} composite)...", total_indexes, total_single, total_composite);
    
    // Create progress bar
    let pb = ProgressBar::new(total_indexes as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} ({percent}%) ETA: {eta}")?
            .progress_chars("##-")
    );
    
    // Create all single-column indexes
    pb.set_message("Creating single-column indexes...");
    for column in &columns {
        let index_name = format!("idx_{}", column);
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}({})", 
            index_name, MATERIALIZED_TABLE, column
        );
        
        if let Err(e) = con.execute(&sql, []) {
            eprintln!("Warning: Failed to create index on {}: {}", column, e);
        }
        pb.inc(1);
    }
    
    // Create all two-column composite indexes
    pb.set_message("Creating composite indexes (2 columns)...");
    for (i, col1) in columns.iter().enumerate() {
        for col2 in &columns[i + 1..] {
            let index_name = format!("idx_{}_{}", col1, col2);
            let sql = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}({}, {})",
                index_name, MATERIALIZED_TABLE, col1, col2
            );
            
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Failed to create composite index on ({}, {}): {}", col1, col2, e);
            }
            pb.inc(1);
        }
    }
    
    pb.finish_with_message("Index creation complete");
    
    // Update statistics to help query optimizer
    println!("Updating table statistics...");
    let analyze_sql = format!("ANALYZE {}", MATERIALIZED_TABLE);
    if let Err(e) = con.execute(&analyze_sql, []) {
        eprintln!("Warning: Failed to analyze table: {}", e);
    }
    
    Ok(())
}

/// Create rollup tables for common aggregations to speed up queries
pub fn create_rollup_tables(con: &Connection) -> Result<()> {
    if !ENABLE_INDEX_CREATION {
        return Ok(());
    }
    
    println!("Creating rollup tables for common aggregations...");
    
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
    
    let total = rollups.len();
    println!("Creating {} rollup tables...", total);
    
    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} ({percent}%) ETA: {eta}")?
            .progress_chars("##-")
    );
    
    // Define the aggregation functions to use in rollups
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
    
    let total_rollups = rollups.len();
    let mut success_count = 0;
    
    for (table_name, group_cols) in rollups {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT {}, {} FROM {} GROUP BY {}",
            table_name, group_cols, agg_str, MATERIALIZED_TABLE, group_cols
        );
        
        if let Err(e) = con.execute(&sql, []) {
            eprintln!("Warning: Failed to create rollup table {}: {}", table_name, e);
            // Continue with next rollup instead of crashing
        } else {
            success_count += 1;
        }
        pb.inc(1);
    }
    
    println!("\nSuccessfully created {}/{} rollup tables", success_count, total_rollups);
    
    pb.finish_with_message("Rollup creation complete");
    Ok(())
}


