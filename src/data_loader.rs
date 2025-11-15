use duckdb::Connection;
use std::path::PathBuf;
use anyhow::Result;

pub fn load_data(con: &Connection, data_dir: &PathBuf) -> Result<Option<PathBuf>> {
    // Determine parquet file/directory location (in data directory parent)
    let parquet_dir = data_dir.parent()
        .unwrap_or(data_dir)
        .join("events.parquet");
    
    // Check if parquet already exists
    let parquet_exists = parquet_dir.is_dir() || parquet_dir.exists();
    
    let parquet_path = if parquet_exists {
        // Parquet exists - use it directly, skip CSV entirely
        let parquet_pattern = if parquet_dir.is_dir() {
            // Directory with multiple parquet files - use glob pattern
            format!("{}/data_*.parquet", parquet_dir.to_string_lossy())
        } else {
            // Single parquet file
            parquet_dir.to_string_lossy().to_string()
        };

        // Create events view directly from Parquet
        con.execute(
            &format!(
                r#"
                CREATE OR REPLACE VIEW events AS
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
                FROM read_parquet('{}')
                "#,
                parquet_pattern
            ),
            [],
        )?;
        
        Some(parquet_dir)
    } else {
        // Parquet doesn't exist - need to generate it from CSV
        let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());
        
        // Create the events view from CSV first
        con.execute(
            &format!(
                r#"
                CREATE OR REPLACE VIEW events AS
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
                    TRY_CAST(type AS ENUM('impression','serve','click','purchase')) AS type,
                    auction_id,
                    TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                    TRY_CAST(publisher_id AS INTEGER)        AS publisher_id,
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
                csv_pattern
            ),
            [],
        )?;

        // Generate parquet from CSV view
        // Use hardware-aware Parquet generation
        use crate::hardware::get_hardware_info;
        let hw = get_hardware_info();
        
        // Estimate total rows (rough estimate: 245M for full dataset)
        let estimated_rows = 245_000_000; // Full dataset estimate
        let optimal_row_group_size = hw.optimal_row_group_size(estimated_rows);
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&parquet_dir)?;
        let parquet_file = parquet_dir.join("data.parquet");
        
        con.execute(
            &format!(
                "COPY (SELECT * FROM events) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {});",
                parquet_file.to_string_lossy(),
                optimal_row_group_size
            ),
            [],
        )?;
        
        // Replace events view to read from Parquet
        let parquet_pattern = format!("{}/data_*.parquet", parquet_dir.to_string_lossy());
        con.execute(
            &format!(
                r#"
                CREATE OR REPLACE VIEW events AS
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
                FROM read_parquet('{}')
                "#,
                parquet_pattern
            ),
            [],
        )?;

        Some(parquet_dir)
    };
    
    Ok(parquet_path)
}