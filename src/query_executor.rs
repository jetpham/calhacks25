use datafusion::prelude::*;
use serde_json::Value;
use std::path::PathBuf;
use std::fs;
use std::time::Instant;

/// Phase 1: Load CSV files into DataFusion context
pub async fn load_csv_files(
    ctx: &SessionContext,
    input_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
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

/// Phase 2: Parse JSON queries to SQL
pub fn parse_queries_from_file(queries_file: &PathBuf) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let queries_content = fs::read_to_string(queries_file)?;
    let queries: Vec<Value> = serde_json::from_str(&queries_content)?;
    
    let mut sql_queries = Vec::new();
    for query in queries.iter() {
        let sql = crate::sql_converter::assemble_sql(query);
        sql_queries.push(sql);
    }
    
    Ok(sql_queries)
}

/// Phase 3: Execute SQL queries
pub async fn execute_queries(
    ctx: &SessionContext,
    sql_queries: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    for sql in sql_queries.iter() {
        let df = ctx.sql(sql).await?;
        df.collect().await?;
    }
    
    Ok(())
}

