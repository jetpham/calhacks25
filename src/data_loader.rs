use duckdb::Connection;
use std::path::PathBuf;
use anyhow::Result;

pub const MATERIALIZED_TABLE: &str = "events_table";

/// Load data from CSV files into DuckDB as a table (not a view)
/// This loads all data into memory for fast query execution
pub fn load_data(con: &Connection, data_dir: &PathBuf) -> Result<()> {
    println!("Creating events_table from source data...");
    
    // Use glob pattern for DuckDB read_csv
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());
    
    // Drop existing events_table if it exists
    println!("Step 1: Dropping existing events_table if it exists...");
    let _ = con.execute(&format!("DROP TABLE IF EXISTS {}", MATERIALIZED_TABLE), []);
    
    // Try to create from existing events table first, otherwise load from CSV
    println!("Step 2: Attempting to create events_table from existing events...");
    
    let use_existing = con.execute(
        &format!("CREATE TABLE {} AS SELECT * FROM events", MATERIALIZED_TABLE),
        []
    );
    
    if use_existing.is_err() {
        // Events table doesn't exist, need to load from CSV
        println!("No existing events table found, loading from CSV files...");
        
        // Create a temporary view from CSV
        con.execute(
            &format!(
                r#"
                CREATE TEMP VIEW temp_events AS
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
                    TRY_CAST(auction_id AS UUID)              AS auction_id,
                    TRY_CAST(advertiser_id AS USMALLINT)       AS advertiser_id,
                    TRY_CAST(publisher_id AS USMALLINT)        AS publisher_id,
                    NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                    TRY_CAST(user_id AS BIGINT)               AS user_id,
                    NULLIF(total_price, '')::DOUBLE           AS total_price,
                    country
                  FROM raw
                )
                SELECT
                  ts,
                  CAST(DATE_TRUNC('week', ts) AS VARCHAR)    AS week,
                  CAST(DATE(ts) AS VARCHAR)                  AS day,
                  CAST(DATE_TRUNC('hour', ts) AS VARCHAR)    AS hour,
                  STRFTIME(ts, '%Y-%m-%d %H:%M')             AS minute,
                  type,
                  auction_id,
                  advertiser_id,
                  publisher_id,
                  bid_price,
                  user_id,
                  total_price,
                  country
                FROM casted
                "#,
                csv_pattern
            ),
            [],
        )?;
        
        // Now materialize from temp view
        println!("Step 3: Materializing from CSV into events_table...");
        con.execute(
            &format!("CREATE TABLE {} AS SELECT * FROM temp_events", MATERIALIZED_TABLE),
            [],
        )?;
    } else {
        println!("Step 3: Successfully created events_table from existing events");
    }
    
    println!("Data loaded into table '{}'", MATERIALIZED_TABLE);
    println!("Loading complete");
    
    Ok(())
}

// open_database function removed - using in-memory connection directly in main.rs