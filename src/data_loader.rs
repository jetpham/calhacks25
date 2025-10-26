use duckdb::Connection;
use std::path::PathBuf;
use anyhow::Result;

const TABLE_NAME: &str = "events";
const MATERIALIZED_TABLE: &str = "events_table";

/// Load data from CSV files into DuckDB as a table (not a view)
/// This loads all data into memory for fast query execution
pub fn load_data(con: &Connection, data_dir: &PathBuf) -> Result<()> {
    println!("Loading data from CSV files into table...");
    
    // Use glob pattern for DuckDB read_csv
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());
    
    // First, create a temporary view to transform the data
    println!("Step 1: Creating temporary view from CSV...");
    con.execute(
        &format!(
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
            FROM casted
            "#,
            TABLE_NAME, csv_pattern
        ),
        [],
    )?;
    
    println!("Step 2: Materializing table (loading into memory)...");
    
    // Now materialize the view into a persistent table
    // This will load ALL data into memory
    con.execute(
        &format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM {}",
            MATERIALIZED_TABLE, TABLE_NAME
        ),
        [],
    )?;
    
    println!("Data loaded into table '{}'", MATERIALIZED_TABLE);
    println!("Loading complete");
    
    Ok(())
}

/// Open a database connection with configuration
pub fn open_database(db_path: &PathBuf) -> Result<Connection> {
    let con = Connection::open(db_path)?;
    // con.execute("SET memory_limit = '16GB'", [])?;
    // con.execute("SET threads = 8", [])?;
    Ok(con)
}