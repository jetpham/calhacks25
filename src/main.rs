use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time::{Instant, Duration};
use duckdb::Connection;

mod data_loader;
mod preprocessor;
mod query_executor;
mod query_handler;
mod result_checker;

use data_loader::load_data;
use preprocessor::{create_indexes_on_all_columns, create_rollup_tables};
use query_executor::{prepare_query, write_single_result_to_csv, explain_query};
use query_handler::{parse_queries_from_file, assemble_sql};
use result_checker::compare_results;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, value_name = "DIR", default_value = "data/data")]
    input_dir: PathBuf,

    /// Run queries and save results
    #[arg(long)]
    run: bool,

    /// Output directory for query results (required with --run or --check)
    #[arg(long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        default_value = "queries.json",
        requires = "run"
    )]
    queries: PathBuf,

    /// Save the database to a file after data loading
    #[arg(
        long,
        value_name = "FILE",
        default_value = "duck.db"
    )]
    save_db: Option<PathBuf>,

    /// Load a database from a file instead of loading from CSV
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with = "input_dir"
    )]
    load_db: Option<PathBuf>,

    /// Baseline directory for comparison (if provided, compares results instead of running queries)
    #[arg(long, value_name = "DIR")]
    baseline_dir: Option<PathBuf>,

    /// Enable profiling with EXPLAIN ANALYZE
    #[arg(long)]
    profile: bool,

    /// Number of times to run each query (for averaging)
    #[arg(long, default_value = "1")]
    runs: usize,
}


fn main() -> Result<()> {
    let total_start = Instant::now();
    let args = Args::parse();

    // Check results if baseline directory is provided (and not running queries)
    if let Some(baseline_dir) = &args.baseline_dir {
        if !args.run {
            // Only check mode
            let Some(output_dir) = &args.output_dir else {
                anyhow::bail!("--output-dir required when using --baseline-dir");
            };
            return compare_results(baseline_dir, output_dir);
        }
        // If both --run and --baseline-dir, we'll check after running queries
    }

    // Start with in-memory database
    let db_path = PathBuf::from("duck.db");
    let con = Connection::open_in_memory()?;
    
    // Configure memory and threads - load entire DB into memory
    
    // Check if database file exists and load it, otherwise create from CSV
    if db_path.exists() && !db_path.is_dir() {
        println!("Loading existing database into memory...");
        let db_name = "loaded_db";
        con.execute(&format!("ATTACH '{}' AS {}", db_path.display(), db_name), [])?;
        con.execute(&format!("COPY FROM DATABASE {} TO memory", db_name), [])?;
        con.execute(&format!("DETACH {}", db_name), [])?;
        println!("Database loaded into memory");
    } else {
        // Remove broken database if it's a directory
        if db_path.exists() && db_path.is_dir() {
            println!("Removing broken database directory...");
            std::fs::remove_dir_all(&db_path)?;
        }
        
        // Prepare phase: load data from CSV, create indexes and rollups in memory
        println!("Creating database from CSV files...");
        load_data(&con, &args.input_dir)?;
        println!("Creating indexes...");
        create_indexes_on_all_columns(&con)?;
        println!("Creating rollup tables...");
        create_rollup_tables(&con)?;
        
        // Save the in-memory database to disk
        println!("Saving database to disk...");
        let db_name = "disk_db";
        con.execute(&format!("ATTACH '{}' AS {}", db_path.display(), db_name), [])?;
        con.execute(&format!("COPY FROM DATABASE memory TO {}", db_name), [])?;
        con.execute(&format!("DETACH {}", db_name), [])?;
        println!("Database saved to duck.db");
    }
    
    // Parse queries from JSON file
    let queries = parse_queries_from_file(&args.queries)?;
    println!("Parsed {} queries", queries.len());
    
    if args.run {
        let Some(output_dir) = &args.output_dir else {
            anyhow::bail!("--output-dir required with --run");
        };
        
        // Phase 1: Convert all JSON queries to SQL
        println!("Converting {} queries to SQL...", queries.len());
        let sql_queries: Vec<String> = queries.iter()
            .map(|q| assemble_sql(q))
            .collect();
        
        // Phase 2: Prepare all statements
        println!("Preparing all statements...");
        let mut prepared_statements: Vec<_> = sql_queries
            .iter()
            .map(|sql| prepare_query(&con, sql))
            .collect::<Result<Vec<_>, _>>()?;
        
        // Phase 3a: Run EXPLAIN ANALYZE if profiling (outside transaction/timing)
        if args.profile {
            println!("\n=== Running EXPLAIN ANALYZE on all queries ===");
            for (i, sql) in sql_queries.iter().enumerate() {
                println!("\n=== Query {} EXPLAIN ANALYZE ===", i + 1);
                explain_query(&con, sql, i + 1)?;
                println!();
            }
        }
        
        // Phase 3b: Execute queries with multiple runs for averaging
        println!("Executing {} queries {} time(s) each...", prepared_statements.len(), args.runs);
        let mut total_duration = Duration::ZERO;
        
        // Track per-query averages
        let num_queries = prepared_statements.len();
        let mut query_times = vec![Vec::new(); num_queries];
        
        // Execute queries multiple times, each run in its own transaction
        for run in 1..=args.runs {
            if args.runs > 1 {
                println!("\n--- Run {}/{} ---", run, args.runs);
            }
            
            let run_total = Instant::now();
            
            // Wrap entire run in a transaction for consistency
            con.execute("BEGIN TRANSACTION", [])?;
            
            for (i, stmt) in prepared_statements.iter_mut().enumerate() {
                // Run the query
                let query_start = Instant::now();
                let rows = stmt.query([])?;
                let duration = query_start.elapsed();
                
                query_times[i].push(duration.as_secs_f64());
                
                if args.runs > 1 {
                    println!("Query {} completed: {:.3}s", i + 1, duration.as_secs_f64());
                } else {
                    println!("Query {} completed: {:.3}s", i + 1, duration.as_secs_f64());
                }
                
                total_duration += duration;
                
                // Only write results from first run
                if run == 1 {
                    write_single_result_to_csv(i + 1, rows, output_dir)?;
                }
            }
            
            // Commit the transaction for this run
            con.execute("COMMIT", [])?;
            
            if args.runs > 1 {
                println!("Run {} total time: {:.3}s", run, run_total.elapsed().as_secs_f64());
            }
        }
        
        // Print averages
        if args.runs > 1 {
            println!("\n=== Average Query Times (over {} runs) ===", args.runs);
            let mut total_avg = 0.0;
            for (i, times) in query_times.iter().enumerate() {
                let avg = times.iter().sum::<f64>() / times.len() as f64;
                total_avg += avg;
                println!("Query {}: {:.3}s (min: {:.3}s, max: {:.3}s)", 
                    i + 1, 
                    avg,
                    times.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
                    times.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b))
                );
            }
            println!("Average total time: {:.3}s", total_avg);
        }
        
        println!("\nTotal query time: {:.3}s", total_duration.as_secs_f64());
        println!("Results written to {:?}", output_dir);

        // If baseline directory was provided, compare results
        if let Some(baseline_dir) = &args.baseline_dir {
            println!("\nComparing results...");
            compare_results(baseline_dir, output_dir)?;
        }
    }

    let total_time = total_start.elapsed();
    println!("Total runtime: {:.3}s", total_time.as_secs_f64());

    Ok(())
}
