use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time::{Instant, Duration};

mod data_loader;
mod preprocessor;
mod query_executor;
mod query_handler;
mod result_checker;

use data_loader::{load_data, open_database};
use preprocessor::{create_indexes_on_all_columns};
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

    // Use persistent database file like baseline (tmp/baseline.duckdb)
    let db_path = PathBuf::from("tmp/baseline.duckdb");
    
    // Create tmp directory if it doesn't exist
    std::fs::create_dir_all("tmp")?;
    
    // Create or load database
    let db_path_to_use = args.load_db.as_ref().unwrap_or(&db_path);
    
    // Delete existing database if creating new one
    if args.load_db.is_none() && db_path.exists() {
        println!("Deleting existing database at {:?}...", db_path);
        std::fs::remove_file(&db_path)?;
    }
    
    println!("Opening database at {:?}...", db_path_to_use);
    let con = open_database(db_path_to_use)?;
    
    // Load data if creating new database
    if args.load_db.is_none() {
        load_data(&con, &args.input_dir)?;
        create_indexes_on_all_columns(&con)?;
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
        
        // Phase 3: Execute queries and write results immediately
        println!("Executing {} queries...", prepared_statements.len());
        let mut total_duration = Duration::ZERO;
        
        // Execute queries and write results immediately
        for (i, stmt) in prepared_statements.iter_mut().enumerate() {
            let sql = &sql_queries[i];
            
            // Optionally print EXPLAIN ANALYZE if profiling is enabled
            if args.profile {
                println!("\n=== Query {} EXPLAIN ANALYZE ===", i + 1);
                explain_query(&con, sql, i + 1)?;
                println!();
            }
            
            // Run the query normally
            let query_start = Instant::now();
            let rows = stmt.query([])?;
            let duration = query_start.elapsed();
            println!("Query {} completed: {:.3}s", i + 1, duration.as_secs_f64());
            total_duration += duration;
            
            // Write result immediately
            write_single_result_to_csv(i + 1, rows, output_dir)?;
        }

        println!("Total query time: {:.3}s", total_duration.as_secs_f64());
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
