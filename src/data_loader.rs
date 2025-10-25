use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;
use std::time::Instant;

const TABLE_NAME: &str = "events";

/// Load data from CSV files into DuckDB with aggressive preprocessing optimizations
pub fn load_data(con: &Connection, data_dir: &PathBuf) -> Result<()> {
    let total_start = Instant::now();
    
    // Step 0: Configure DuckDB for maximum performance
    let step_start = Instant::now();
    con.execute("SET enable_profiling TO 'json'", [])?; // Enable profiling
    con.execute("SET profiling_output TO 'query_profile.json'", [])?;
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

    // Step 2: Table creation and data loading
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
              'ts': 'VARCHAR',
              'type': 'VARCHAR',
              'auction_id': 'VARCHAR',
              'advertiser_id': 'VARCHAR',
              'publisher_id': 'VARCHAR',
              'bid_price': 'VARCHAR',
              'user_id': 'VARCHAR',
              'total_price': 'VARCHAR',
              'country': 'VARCHAR'
            }}
          )
        ),
        casted AS (
          SELECT
            to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
            type,
            auction_id,
            TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
            TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
            NULLIF(bid_price, '')::DOUBLE             AS bid_price,
            TRY_CAST(user_id AS BIGINT)               AS user_id,
            NULLIF(total_price, '')::DOUBLE           AS total_price,
            country
          FROM raw
        )
        SELECT
          ts,
          DATE_TRUNC('week', ts)              AS week,
          DATE(ts)                            AS day,
          DATE_TRUNC('hour', ts)              AS hour,
          STRFTIME(ts, '%Y-%m-%d %H:%M')      AS minute,
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

    // Step 3: Single column indexes
    let step_start = Instant::now();
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_type ON {}(type)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_country ON {}(country)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_day ON {}(day)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_advertiser_id ON {}(advertiser_id)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_publisher_id ON {}(publisher_id)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_minute ON {}(minute)", TABLE_NAME, TABLE_NAME), [])?;
    let single_index_time = step_start.elapsed();
    println!("Single column indexes: {:.3}s", single_index_time.as_secs_f64());
    
    // Step 4: Composite indexes
    let step_start = Instant::now();
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_type_country ON {}(type, country)", TABLE_NAME, TABLE_NAME), [])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS idx_{}_type_day ON {}(type, day)", TABLE_NAME, TABLE_NAME), [])?;
    let composite_index_time = step_start.elapsed();
    println!("Composite indexes: {:.3}s", composite_index_time.as_secs_f64());
    
    // Step 5: Pre-warm the database
    let step_start = Instant::now();
    con.execute("SELECT COUNT(*) FROM events", [])?;
    let prewarm_time = step_start.elapsed();
    println!("Database pre-warming: {:.3}s", prewarm_time.as_secs_f64());
    
    // Step 6: Analyze the table for better query planning
    let step_start = Instant::now();
    con.execute("ANALYZE events", [])?;
    let analyze_time = step_start.elapsed();
    println!("Table analysis: {:.3}s", analyze_time.as_secs_f64());

    let total_time = total_start.elapsed();
    println!("Total preprocessing: {:.3}s", total_time.as_secs_f64());
    
    Ok(())
}

