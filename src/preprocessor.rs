use duckdb::Connection;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};

const MATERIALIZED_TABLE: &str = "events_table";
const ENABLE_INDEX_CREATION: bool = true;

/// Check cardinality (unique values) for each column to determine safe indexes
pub fn check_column_cardinality(con: &Connection) -> Result<()> {
    if !ENABLE_INDEX_CREATION {
        println!("Index creation disabled, skipping cardinality check");
        return Ok(());
    }
    
    let columns = vec![
        "ts", "week", "day", "hour", "minute",
        "type", "auction_id", "advertiser_id", "publisher_id",
        "bid_price", "user_id", "total_price", "country"
    ];
    
    println!("\nColumn Cardinality Analysis:");
    println!("{:-<70}", "");
    
    let mut cardinalities = Vec::new();
    
    for column in &columns {
        let sql = format!("SELECT COUNT(DISTINCT {}) as unique_count FROM {}", column, MATERIALIZED_TABLE);
        
        let mut stmt = con.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        if let Some(row) = rows.next()? {
            let unique_count: u64 = row.get(0)?;
            cardinalities.push((column.to_string(), unique_count));
            println!("{:15} : {:>10} unique values", column, unique_count);
        }
    }
    
    println!("{:-<70}", "");
    
    // Estimate memory usage for indexes
    // Rule of thumb: each unique value in an index needs ~16-24 bytes
    println!("\nEstimating index memory usage (conservative 20 bytes per unique value):");
    
    let mut total_single_memory = 0u64;
    for (col, card) in &cardinalities {
        let memory_mb = (*card as f64 * 20.0) / (1024.0 * 1024.0);
        total_single_memory += card * 20;
        println!("  {}: {:.2} MB", col, memory_mb);
    }
    
    // Estimate memory for composite indexes (product of cardinalities)
    println!("\nEstimating composite index memory (20 bytes per unique combination):");
    let mut total_composite_memory = 0u64;
    
    for (i, (col1, card1)) in cardinalities.iter().enumerate() {
        for (col2, card2) in cardinalities.iter().skip(i + 1) {
            // Estimate composite cardinality as sqrt(prod) for better accuracy
            let composite_card = ((card1 * card2) as f64).sqrt() as u64;
            let memory_mb = (composite_card as f64 * 20.0) / (1024.0 * 1024.0);
            total_composite_memory += composite_card * 20;
            println!("  ({}, {}): {:.2} MB (est. {} unique combos)", col1, col2, memory_mb, composite_card);
        }
    }
    
    println!("\nEstimated single-column indexes total: {:.2} MB", (total_single_memory as f64) / (1024.0 * 1024.0));
    println!("Estimated composite indexes total: {:.2} MB", (total_composite_memory as f64) / (1024.0 * 1024.0));
    
    let total_memory_gb = (total_single_memory as f64 + total_composite_memory as f64) / (1024.0 * 1024.0 * 1024.0);
    println!("Total estimated memory: {:.2} GB", total_memory_gb);
    
    if total_memory_gb > 8.0 {
        println!("\n⚠️  WARNING: Estimated index memory ({:.2} GB) exceeds 8GB!", total_memory_gb);
        println!("   Consider reducing number of indexes created.");
    }
    
    Ok(())
}


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
    
    for (table_name, group_cols) in rollups {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} AS SELECT {}, {} FROM {} GROUP BY {}",
            table_name, group_cols, agg_str, MATERIALIZED_TABLE, group_cols
        );
        
        if let Err(e) = con.execute(&sql, []) {
            eprintln!("Warning: Failed to create rollup table {}: {}", table_name, e);
        }
        pb.inc(1);
    }
    
    pb.finish_with_message("Rollup creation complete");
    Ok(())
}


