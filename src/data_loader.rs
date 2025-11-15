use duckdb::Connection;
use std::path::PathBuf;
use anyhow::Result;

pub fn load_data(con: &Connection, data_dir: &PathBuf, use_parquet: bool) -> Result<Option<PathBuf>> {
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());
    
    // Create the events view from CSV
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

    let parquet_path = if use_parquet {
        // Determine parquet file/directory location (in data directory parent)
        let parquet_dir = data_dir.parent()
            .unwrap_or(data_dir)
            .join("events.parquet");
        
        // Check if it's a directory with parquet files or a single file
        let parquet_pattern = if parquet_dir.is_dir() {
            // Directory with multiple parquet files - use glob pattern
            format!("{}/data_*.parquet", parquet_dir.to_string_lossy())
        } else if parquet_dir.exists() {
            // Single parquet file
            parquet_dir.to_string_lossy().to_string()
        } else {
            // Doesn't exist - generate it
            // Use hardware-aware Parquet generation
            // Get optimal row group size based on hardware
            use crate::hardware::get_hardware_info;
            let hw = get_hardware_info();
            
            // Estimate total rows (rough estimate: 245M for full dataset)
            // For hardware-aware tuning, we'll use a reasonable default
            // Winner used 1M for 10 threads, we'll adjust for our thread count
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
            
            parquet_file.to_string_lossy().to_string()
        };

        // Replace events view to read from Parquet
        con.execute(
            &format!(
                "CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{}');",
                parquet_pattern
            ),
            [],
        )?;

        Some(parquet_dir)
    } else {
        None
    };
    
    Ok(parquet_path)
}