use duckdb::Connection;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;

const TABLE_NAME: &str = "events";

/// Load data from CSV files into DuckDB
pub fn load_data(con: &Connection, data_dir: &PathBuf) -> Result<()> {
    let total_start = Instant::now();
    
    println!("Loading data from CSV files...");
    
    // Configure DuckDB
    con.execute("SET memory_limit = '16GB'", [])?;
    con.execute("SET threads = 8", [])?;
    
    // Use glob pattern for DuckDB read_csv
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());

    let step_start = Instant::now();
    
    // Create table from CSV files following baseline approach
    con.execute(&format!(
        r#"
        CREATE OR REPLACE VIEW {} AS
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
    println!("Loaded data ({:.3}s)", table_creation_time.as_secs_f64());
    
    let total_time = total_start.elapsed();
    println!("Data loading complete. Total time: {:.3}s", total_time.as_secs_f64());
    
    Ok(())
}

/// Load a database from a file
pub fn load_database(db_path: &PathBuf) -> Result<Connection> {
    let con = Connection::open(db_path)?;
    Ok(con)
}