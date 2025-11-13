use duckdb::Connection;
use std::path::PathBuf;
use anyhow::Result;

pub fn load_data(con: &Connection, data_dir: &PathBuf, use_parquet: bool) -> Result<Option<PathBuf>> {
    println!("Creating events view from source data...");
    
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.to_string_lossy());
    
    // Create the events view from CSV
    println!("Creating events view from CSV files...");
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
        // Determine parquet file location (in data directory parent)
        let parquet_file = data_dir.parent()
            .unwrap_or(data_dir)
            .join("events.parquet");
        
        if !parquet_file.exists() {
            println!("Generating Parquet files with optimized row groups (takes ~60 seconds)...");
            let start = std::time::Instant::now();
            
            // Use hardware-aware Parquet generation
            // Get optimal row group size based on hardware
            use crate::hardware::get_hardware_info;
            let hw = get_hardware_info();
            
            // Estimate total rows (rough estimate: 245M for full dataset)
            // For hardware-aware tuning, we'll use a reasonable default
            // Winner used 1M for 10 threads, we'll adjust for our thread count
            let estimated_rows = 245_000_000; // Full dataset estimate
            let optimal_row_group_size = hw.optimal_row_group_size(estimated_rows);
            
            println!("Hardware detected: {} threads, {:.1}GB RAM available", 
                     hw.num_threads, hw.available_memory_gb);
            println!("Using ROW_GROUP_SIZE: {} (optimized for {} threads)", 
                     optimal_row_group_size, hw.num_threads);
            
            con.execute(
                &format!(
                    "COPY (SELECT * FROM events) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, PER_THREAD_OUTPUT, ROW_GROUP_SIZE {});",
                    parquet_file.to_string_lossy(),
                    optimal_row_group_size
                ),
                [],
            )?;
            
            println!("Parquet generation complete: {:.3}s", start.elapsed().as_secs_f64());
        } else {
            println!("Parquet files already exist - skipping generation");
        }

        // Replace events view to read from Parquet
        con.execute(
            &format!(
                "CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{}');",
                parquet_file.to_string_lossy()
            ),
            [],
        )?;

        Some(parquet_file)
    } else {
        None
    };
    
    println!("Data loading complete");
    
    Ok(parquet_path)
}