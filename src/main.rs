use anyhow::Result;
use clap::Parser;
use duckdb::Connection;
use std::path::PathBuf;
use std::time::Instant;

mod data_loader;
mod query_executor;
mod query_parser;
mod sql_assembler;
mod profiler;

use data_loader::{load_data, load_database_from_file};
use query_executor::{run_queries, write_results_to_csv};
use query_parser::parse_queries_from_file;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, value_name = "DIR", default_value = "data/data")]
    input_dir: PathBuf,

    /// Run queries and save results
    #[arg(long)]
    run: bool,

    #[arg(long, value_name = "DIR", requires = "run")]
    output_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        default_value = "queries.json",
        requires = "run"
    )]
    queries: PathBuf,

    #[arg(long, value_name = "DIR")]
    check_dir: Option<PathBuf>,

    /// Save the preprocessed database to a file after data loading
    #[arg(
        long,
        value_name = "FILE",
        default_value = "duck.db",
        conflicts_with = "load_db"
    )]
    save_db: Option<PathBuf>,

    /// Load a preprocessed database from a file instead of loading from CSV
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with = "save_db"
    )]
    load_db: Option<PathBuf>,
}

/// CalHacks Query Engine
/// 
/// A high-performance query engine built with Rust and DuckDB for processing large-scale event data.
/// 
/// ## How it works:
/// 
/// 1. **Database Loading**: Loads data from CSV files or a preprocessed database file
///    - If `--load-db` is specified: loads from the specified database file
///    - Otherwise: loads from CSV files in the input directory and preprocesses the data
///    - If `--save-db` is specified: saves the preprocessed database to a file
/// 
/// 2. **Query Processing**: Parses and executes queries from a JSON file
///    - Queries are parsed from the specified JSON file (default: queries.json)
///    - Only executes queries if `--run` flag is provided
///    - Results are written to CSV files in the output directory if `--output-dir` is specified
/// 
/// ## Usage Examples:
/// 
/// ```bash
/// # Just preprocess data and save to database file
/// cargo run -- --input-dir data/data
/// 
/// # Load preprocessed data and run queries
/// cargo run -- --run --load-db --queries queries.json --output-dir results/
/// 
/// # Process from CSV and run queries in one step
/// cargo run -- --run --input-dir data/data --queries queries.json --output-dir results/
/// 
/// # Use custom database file names
/// cargo run -- --input-dir data/data --save-db my_db.db
/// cargo run -- --run --load-db my_db.db --queries queries.json --output-dir results/
/// ```
/// 
/// ## Architecture:
/// 
/// - **data_loader**: Handles CSV loading, data preprocessing, and database persistence
/// - **query_parser**: Parses JSON query definitions into internal format
/// - **sql_assembler**: Converts internal query format to SQL
/// - **query_executor**: Executes SQL queries and manages results
fn main() -> Result<()> {
    let total_start = Instant::now();
    let args = Args::parse();

    // Step 2: Load database from file or create persistent database
    let con = if let Some(db_path) = &args.load_db {
        // Load database from file
        load_database_from_file(db_path)?
    } else {
        // Create persistent database file and load data from CSVs
        let db_path = args.save_db.unwrap(); // Should never be None due to default_value
        
        // Remove existing database file if it exists to avoid serialization errors
        if db_path.exists() {
            std::fs::remove_file(&db_path)?;
            println!("Removed existing database file: {:?}", db_path);
        }
        
        let con = Connection::open(&db_path)?;
        load_data(&con, &args.input_dir, &db_path)?;
        
        println!("Persistent database created at: {:?}", db_path);
        con
    };
    // Step 1: Parse queries from JSON file
    let queries = parse_queries_from_file(&args.queries)?;
    println!("Parsed {} queries", queries.len());
    if args.run {
        // Step 3: Run queries and get results
        let results = run_queries(&con, &queries)?;
        println!("Executed {} queries", results.len());

        // Step 4: Write results to disk if output directory is specified
        if let Some(output_dir) = &args.output_dir {
            write_results_to_csv(&results, output_dir)?;
            println!("Results written to {:?}", output_dir);
        }
    }

    let total_time = total_start.elapsed();
    println!("Total runtime: {:.3}s", total_time.as_secs_f64());

    Ok(())
}
