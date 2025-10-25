use datafusion::prelude::*;
use std::path::PathBuf;
use anyhow::Result;
use datafusion::config::ConfigOptions;

/// Create an optimized SessionContext for concurrent query execution
pub fn create_optimized_context() -> SessionContext {
    let mut config = ConfigOptions::new();
    
    // Optimize for concurrent execution
    config.execution.target_partitions = num_cpus::get(); // Use all CPU cores
    config.execution.coalesce_batches = true; // Combine small batches for efficiency
    config.execution.collect_statistics = false; // Skip stats collection for speed
    
    // Memory and I/O optimizations
    config.execution.batch_size = 8192; // Larger batch size for better vectorization
    
    SessionContext::new_with_config(config.into())
}

/// Phase 1: Load CSV files into DataFusion context
pub async fn load_csv_files(
    ctx: &SessionContext,
    input_dir: &PathBuf,
) -> Result<()> {
    // Register all CSV files in the directory as a single table
    // DataFusion will automatically discover and load all CSV files with the same schema
    ctx.register_csv("raw_events", &input_dir.to_string_lossy(), CsvReadOptions::new()).await?;
    
    // Create a view with pre-computed day and minute columns like the baseline
    let create_view_sql = r#"
        CREATE OR REPLACE VIEW events AS
        SELECT 
            to_timestamp(CAST(ts AS BIGINT) / 1000) as ts,
            type,
            auction_id,
            CAST(advertiser_id AS INTEGER) as advertiser_id,
            CAST(publisher_id AS INTEGER) as publisher_id,
            CASE WHEN bid_price = '' THEN NULL ELSE CAST(bid_price AS DOUBLE) END as bid_price,
            CAST(user_id AS BIGINT) as user_id,
            CASE WHEN total_price = '' THEN NULL ELSE CAST(total_price AS DOUBLE) END as total_price,
            country,
            date_trunc('day', to_timestamp(CAST(ts AS BIGINT) / 1000)) as day,
            date_format(to_timestamp(CAST(ts AS BIGINT) / 1000), '%Y-%m-%d %H:%M') as minute
        FROM raw_events
    "#;
    
    ctx.sql(create_view_sql).await?;
    Ok(())
}
