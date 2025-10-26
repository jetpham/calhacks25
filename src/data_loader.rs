use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;
use std::time::Instant;
use crate::profiler::{ProfilingConfig, ProfilingMode, setup_profiling};
use indicatif::{ProgressBar, ProgressStyle};

const TABLE_NAME: &str = "events";

/// Load data from CSV files into DuckDB with persistent storage
pub fn load_data(con: &Connection, data_dir: &PathBuf, db_path: &PathBuf) -> Result<()> {
    let total_start = Instant::now();
    
    // Create progress bar for data loading steps
    let pb = ProgressBar::new(45); // Total number of major steps + individual indexes
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>2}/{len:2} ETA:{eta_precise} {msg}"
        )?
        .progress_chars("##-")
    );
    pb.set_message("Starting data loading...");
    
    // Step 0: Configure DuckDB for persistent storage with memory limits
    let step_start = Instant::now();
    
    con.execute("SET memory_limit = '16GB'", [])?;
    
    // Configure temp directory for better disk management (ensure large disk space)
    con.execute("SET temp_directory = '/tmp/duckdb_temp'", [])?;
    
    con.execute("SET enable_progress_bar = false", [])?;

    // Disable insertion order preservation to save memory
    con.execute("SET preserve_insertion_order = false", [])?;

    // Optimize thread count based on CPU cores (guide suggests avoiding too many threads)
    con.execute("SET threads = 8", [])?;
    // Note: Compression and row group size settings are applied at database creation time
    // Setup profiling
    let profiling_config = ProfilingConfig {
        mode: ProfilingMode::Json,
        output_dir: PathBuf::from("profiling"),
        enable_detailed: true,
        enable_optimizer_metrics: true,
        enable_planner_metrics: true,
        enable_physical_planner_metrics: true,
    };
    
    setup_profiling(con, &profiling_config)?;
    let config_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("DuckDB configured ({:.3}s)", config_time.as_secs_f64()));
    
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
    pb.inc(1);
    pb.set_message(format!("Found {} files ({:.3}s)", csv_files.len(), file_discovery_time.as_secs_f64()));

    if csv_files.is_empty() {
        return Err(anyhow::anyhow!("No events_part_*.csv files found in {:?}", data_dir));
    }

    // Step 2: Table creation and data loading with optimized data types
    let step_start = Instant::now();
    let csv_pattern = format!("{}/events_part_*.csv", data_dir.display());
    
    con.execute(&format!(
        r#"
        -- Note: Using VARCHAR instead of custom enums for better compatibility
        
        CREATE OR REPLACE TABLE {} AS
        WITH raw AS (
          SELECT *
          FROM read_csv(
            '{}',
            AUTO_DETECT = FALSE,
            HEADER = TRUE,
            union_by_name = TRUE,
            COLUMNS = {{
              'ts': 'BIGINT',
              'type': 'VARCHAR',
              'auction_id': 'VARCHAR',
              'advertiser_id': 'INTEGER',
              'publisher_id': 'INTEGER',
              'bid_price': 'VARCHAR',  -- Keep as VARCHAR to preserve empty strings
              'user_id': 'BIGINT',
              'total_price': 'VARCHAR', -- Keep as VARCHAR to preserve empty strings
              'country': 'VARCHAR'
            }}
          )
        ),
        casted AS (
          SELECT
            to_timestamp(ts / 1000.0)                    AS ts,
            type                                         AS type,
            auction_id::UUID                             AS auction_id,
            advertiser_id,
            publisher_id,
            -- Optimize bid_price based on event type
            CASE 
              WHEN type = 'impression' THEN 
                CASE 
                  WHEN bid_price = '' OR bid_price IS NULL THEN NULL
                  ELSE bid_price::DOUBLE
                END
              ELSE NULL  -- serve, click, purchase don't have meaningful bid_price
            END AS bid_price,
            user_id,
            -- Optimize total_price based on event type  
            CASE 
              WHEN type = 'purchase' THEN 
                CASE 
                  WHEN total_price = '' OR total_price IS NULL THEN NULL
                  ELSE total_price::DOUBLE
                END
              ELSE NULL  -- serve, impression, click don't have total_price
            END AS total_price,
            country                                      AS country,
            CASE 
              WHEN ts IS NOT NULL AND ts > 0 THEN DATE(DATE_TRUNC('week', to_timestamp(ts / 1000.0)))
              ELSE NULL 
            END AS week,
            DATE(to_timestamp(ts / 1000.0))              AS day,
            DATE_TRUNC('hour', to_timestamp(ts / 1000.0)) AS hour,
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-%m-%d %H:%M') AS minute,
            -- Event-specific optimizations
            CASE WHEN type = 'serve' THEN 1 ELSE 0 END AS is_serve,
            CASE WHEN type = 'impression' THEN 1 ELSE 0 END AS is_impression,
            CASE WHEN type = 'click' THEN 1 ELSE 0 END AS is_click,
            CASE WHEN type = 'purchase' THEN 1 ELSE 0 END AS is_purchase,
            -- Revenue optimization: only calculate for purchase events
            CASE 
              WHEN type = 'purchase' THEN 
                CASE 
                  WHEN total_price = '' OR total_price IS NULL THEN 0.0
                  ELSE total_price::DOUBLE
                END
              ELSE 0.0 
            END AS revenue,
            -- Bid optimization: only calculate for impression events  
            CASE 
              WHEN type = 'impression' THEN 
                CASE 
                  WHEN bid_price = '' OR bid_price IS NULL THEN 0.0
                  ELSE bid_price::DOUBLE
                END
              ELSE 0.0 
            END AS impression_bid
          FROM raw
        )
        SELECT
          ts,
          week,
          day,
          hour,
          minute,
          type,
          auction_id,
          advertiser_id,
          publisher_id,
          bid_price,
          user_id,
          total_price,
          country,
          -- Event flags for efficient filtering
          is_serve,
          is_impression,
          is_click,
          is_purchase,
          -- Optimized metrics
          revenue,
          impression_bid
        FROM casted;
        "#,
        TABLE_NAME, csv_pattern
    ), [])?;
    let table_creation_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Table created & data loaded ({:.3}s)", table_creation_time.as_secs_f64()));

    // Step 3: Pre-warm the database
    let step_start = Instant::now();
    con.execute("SELECT COUNT(*) FROM events", [])?;
    let prewarm_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Database pre-warmed ({:.3}s)", prewarm_time.as_secs_f64()));
    
    // Step 4: Create generalized indexes for optimal overall performance
    let step_start = Instant::now();
    
    // Primary time-based index (most queries filter by time)
    pb.set_message("Creating time-based index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_primary ON events (ts)", [])?;
    pb.inc(1);
    
    // Event type index (most queries filter by type)
    pb.set_message("Creating event type index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_type ON events (type)", [])?;
    pb.inc(1);
    
    // Composite time + type index (covers most query patterns)
    pb.set_message("Creating composite time+type index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_type ON events (ts, type)", [])?;
    pb.inc(1);
    
    // Geographic index (common for country-based analysis)
    pb.set_message("Creating country index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_country ON events (country)", [])?;
    pb.inc(1);
    
    // Business entity indexes (advertiser/publisher analysis)
    pb.set_message("Creating advertiser index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_advertiser ON events (advertiser_id)", [])?;
    pb.inc(1);
    
    pb.set_message("Creating publisher index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_publisher ON events (publisher_id)", [])?;
    pb.inc(1);
    
    // Auction tracking index (for funnel analysis)
    pb.set_message("Creating auction index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_auction ON events (auction_id)", [])?;
    pb.inc(1);
    
    // Multi-column composite index for complex queries
    pb.set_message("Creating composite index...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_composite ON events (type, country, ts)", [])?;
    pb.inc(1);
    
    let index_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Indexes created ({:.3}s)", index_time.as_secs_f64()));
    
    // Step 5: Create comprehensive query-agnostic rollups (unlimited preprocessing time)
    let step_start = Instant::now();
    
    // 1. Time-based rollups (all time dimensions) - Most important first
    pb.set_message("Creating time_rollups...");
    
    
    con.execute(r#"
        CREATE TABLE time_rollups AS
        SELECT 
            week,
            day,
            hour,
            minute,
            type,
            country,
            advertiser_id,
            publisher_id,
            
            -- Event counts
            COUNT(*) as event_count,
            SUM(is_serve) as serve_count,
            SUM(is_impression) as impression_count,
            SUM(is_click) as click_count,
            SUM(is_purchase) as purchase_count,
            
            -- Financial metrics
            SUM(impression_bid) as total_bid_spend,
            SUM(revenue) as total_revenue,
            AVG(impression_bid) as avg_bid_price,
            AVG(revenue) as avg_purchase_price,
            MIN(impression_bid) as min_bid_price,
            MAX(impression_bid) as max_bid_price,
            MIN(revenue) as min_revenue,
            MAX(revenue) as max_revenue,
            
            -- Conversion metrics
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_click)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as click_through_rate,
            
            CASE 
                WHEN SUM(is_click) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_click)::DOUBLE 
                ELSE 0 
            END as conversion_rate,
            
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as impression_to_purchase_rate

        FROM events 
        GROUP BY week, day, hour, minute, type, country, advertiser_id, publisher_id
    "#, [])?;
    
    // Note: DuckDB handles memory management automatically through spilling to disk
    // No manual cleanup needed - the temp_directory setting enables this
    
    // 2. Business entity rollups (advertiser/publisher analysis) - HIGH PRIORITY
    pb.set_message("Creating business_rollups...");
    con.execute(r#"
        CREATE TABLE business_rollups AS
        SELECT 
            advertiser_id,
            publisher_id,
            type,
            country,
            
            -- Event counts
            COUNT(*) as event_count,
            SUM(is_serve) as serve_count,
            SUM(is_impression) as impression_count,
            SUM(is_click) as click_count,
            SUM(is_purchase) as purchase_count,
            
            -- Financial metrics
            SUM(impression_bid) as total_bid_spend,
            SUM(revenue) as total_revenue,
            AVG(impression_bid) as avg_bid_price,
            AVG(revenue) as avg_purchase_price,
            
            -- Performance metrics
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_click)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as click_through_rate,
            
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as impression_to_purchase_rate,
            
            -- Time range
            MIN(day) as first_day,
            MAX(day) as last_day

        FROM events 
        GROUP BY advertiser_id, publisher_id, type, country
    "#, [])?;
    
    // DuckDB automatically manages memory - no manual cleanup needed
    
    // 3. Geographic rollups (country-based analysis) - MEDIUM PRIORITY
    pb.set_message("Creating geo_rollups...");
    con.execute(r#"
        CREATE TABLE geo_rollups AS
        SELECT 
            country,
            type,
            
            -- Event counts
            COUNT(*) as event_count,
            SUM(is_serve) as serve_count,
            SUM(is_impression) as impression_count,
            SUM(is_click) as click_count,
            SUM(is_purchase) as purchase_count,
            
            -- Financial metrics
            SUM(impression_bid) as total_bid_spend,
            SUM(revenue) as total_revenue,
            AVG(impression_bid) as avg_bid_price,
            AVG(revenue) as avg_purchase_price

        FROM events 
        GROUP BY country, type
    "#, [])?;
    
    // DuckDB automatically manages memory - no manual cleanup needed
    
    // 4. Event type rollups (type-based analysis)
    pb.set_message("Creating event_type_rollups...");
    con.execute(r#"
        CREATE TABLE event_type_rollups AS
        SELECT 
            type,
            country,
            advertiser_id,
            publisher_id,
            
            -- Event counts
            COUNT(*) as event_count,
            SUM(is_serve) as serve_count,
            SUM(is_impression) as impression_count,
            SUM(is_click) as click_count,
            SUM(is_purchase) as purchase_count,
            
            -- Financial metrics
            SUM(impression_bid) as total_bid_spend,
            SUM(revenue) as total_revenue,
            AVG(impression_bid) as avg_bid_price,
            AVG(revenue) as avg_purchase_price,
            
            -- Performance metrics
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_click)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as click_through_rate,
            
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as impression_to_purchase_rate

        FROM events 
        GROUP BY type, country, advertiser_id, publisher_id
    "#, [])?;
    
    // DuckDB automatically manages memory - no manual cleanup needed
    
    // 5. Comprehensive rollups (all dimensions combined)
    pb.set_message("Creating comprehensive_rollups...");
    con.execute(r#"
        CREATE TABLE comprehensive_rollups AS
        SELECT 
            week,
            day,
            hour,
            minute,
            type,
            country,
            advertiser_id,
            publisher_id,
            
            -- Event counts
            COUNT(*) as event_count,
            SUM(is_serve) as serve_count,
            SUM(is_impression) as impression_count,
            SUM(is_click) as click_count,
            SUM(is_purchase) as purchase_count,
            
            -- Financial metrics
            SUM(impression_bid) as total_bid_spend,
            SUM(revenue) as total_revenue,
            AVG(impression_bid) as avg_bid_price,
            AVG(revenue) as avg_purchase_price,
            MIN(impression_bid) as min_bid_price,
            MAX(impression_bid) as max_bid_price,
            MIN(revenue) as min_revenue,
            MAX(revenue) as max_revenue,
            
            -- Conversion metrics
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_click)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as click_through_rate,
            
            CASE 
                WHEN SUM(is_click) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_click)::DOUBLE 
                ELSE 0 
            END as conversion_rate,
            
            CASE 
                WHEN SUM(is_impression) > 0 
                THEN SUM(is_purchase)::DOUBLE / SUM(is_impression)::DOUBLE 
                ELSE 0 
            END as impression_to_purchase_rate

        FROM events 
        GROUP BY week, day, hour, minute, type, country, advertiser_id, publisher_id
    "#, [])?;
    
    // DuckDB automatically manages memory - no manual cleanup needed
    
    let rollup_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Rollups created ({:.3}s)", rollup_time.as_secs_f64()));
    
    // Step 6: Create indexes on rollup tables for maximum query performance
    let step_start = Instant::now();
    
    // Indexes for time_rollups
    pb.set_message("Creating time_rollups indexes...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_day ON time_rollups (day)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_hour ON time_rollups (hour)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_minute ON time_rollups (minute)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_type ON time_rollups (type)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_country ON time_rollups (country)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_time_rollups_composite ON time_rollups (day, type, country)", [])?;
    pb.inc(1);
    
    // Indexes for geo_rollups
    pb.set_message("Creating geo_rollups indexes...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_geo_rollups_country ON geo_rollups (country)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_geo_rollups_type ON geo_rollups (type)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_geo_rollups_composite ON geo_rollups (country, type)", [])?;
    pb.inc(1);
    
    // Indexes for business_rollups
    pb.set_message("Creating business_rollups indexes...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_business_rollups_advertiser ON business_rollups (advertiser_id)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_business_rollups_publisher ON business_rollups (publisher_id)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_business_rollups_composite ON business_rollups (advertiser_id, publisher_id, type)", [])?;
    pb.inc(1);
    
    // Indexes for event_type_rollups
    pb.set_message("Creating event_type_rollups indexes...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_event_type_rollups_type ON event_type_rollups (type)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_event_type_rollups_country ON event_type_rollups (country)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_event_type_rollups_composite ON event_type_rollups (type, country)", [])?;
    pb.inc(1);
    
    // Indexes for comprehensive_rollups
    pb.set_message("Creating comprehensive_rollups indexes...");
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_day ON comprehensive_rollups (day)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_hour ON comprehensive_rollups (hour)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_minute ON comprehensive_rollups (minute)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_type ON comprehensive_rollups (type)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_country ON comprehensive_rollups (country)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_advertiser ON comprehensive_rollups (advertiser_id)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_publisher ON comprehensive_rollups (publisher_id)", [])?;
    pb.inc(1);
    con.execute("CREATE INDEX IF NOT EXISTS idx_comprehensive_rollups_composite ON comprehensive_rollups (day, type, country, advertiser_id, publisher_id)", [])?;
    pb.inc(1);
    
    let rollup_index_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Rollup indexes created ({:.3}s)", rollup_index_time.as_secs_f64()));
    
    // Step 7: Analyze all tables for optimal query planning
    let step_start = Instant::now();
    pb.set_message("Analyzing events table...");
    con.execute("ANALYZE events", [])?;
    pb.inc(1);
    
    pb.set_message("Analyzing time_rollups table...");
    con.execute("ANALYZE time_rollups", [])?;
    pb.inc(1);
    
    pb.set_message("Analyzing geo_rollups table...");
    con.execute("ANALYZE geo_rollups", [])?;
    pb.inc(1);
    
    pb.set_message("Analyzing business_rollups table...");
    con.execute("ANALYZE business_rollups", [])?;
    pb.inc(1);
    
    pb.set_message("Analyzing event_type_rollups table...");
    con.execute("ANALYZE event_type_rollups", [])?;
    pb.inc(1);
    
    pb.set_message("Analyzing comprehensive_rollups table...");
    con.execute("ANALYZE comprehensive_rollups", [])?;
    pb.inc(1);
    
    let analyze_time = step_start.elapsed();
    pb.set_message(format!("All tables analyzed ({:.3}s)", analyze_time.as_secs_f64()));
    
    // Step 8: DuckDB optimization settings
    let step_start = Instant::now();
    
    let optimization_time = step_start.elapsed();
    pb.inc(1);
    pb.set_message(format!("Optimization complete ({:.3}s)", optimization_time.as_secs_f64()));

    let total_time = total_start.elapsed();
    pb.finish_with_message(format!("✅ Data loading complete! Total time: {:.3}s", total_time.as_secs_f64()));
    
    println!("\n=== Optimized Data Loading Complete ===");
    println!("Database saved to: {:?}", db_path);
    println!("Ready for lightning-fast query execution! ⚡");
    
    Ok(())
}

/// Load a database from a file
pub fn load_database_from_file(db_path: &PathBuf) -> Result<Connection> {
    let start = Instant::now();
    
    // Check if the database file exists
    if !db_path.exists() {
        return Err(anyhow::anyhow!("Database file does not exist: {:?}", db_path));
    }
    
    // Open the database file
    let con = Connection::open(db_path)?;
    
    let duration = start.elapsed();
    println!("Database loaded from {:?} in {:.3}s", db_path, duration.as_secs_f64());
    
    Ok(con)
}

