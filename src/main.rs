use anyhow::Result;
use clap::Parser;
use duckdb::Connection;
use std::path::PathBuf;
use std::time::Instant;

mod data_loader;
mod query_executor;
mod query_handler;

use data_loader::{load_data, load_database};
use query_executor::{execute_query, write_results_to_csv};
use query_handler::{parse_queries_from_file, assemble_sql};

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
}

/// CalHacks Query Engine
/// 
/// A high-performance query engine built with Rust and DuckDB for processing large-scale event data.
/// 
/// ## How it works:
/// 
/// 1. **Database Loading**: Loads data from CSV files or a database file
///    - If `--load-db` is specified: loads from the specified database file
///    - Otherwise: loads from CSV files in the input directory
///    - If `--save-db` is specified: saves the database to a file
/// 
/// 2. **Query Processing**: Parses and executes queries from a JSON file
///    - Queries are parsed from the specified JSON file (default: queries.json)
///    - Only executes queries if `--run` flag is provided
///    - Results are written to CSV files in the output directory if `--output-dir` is specified
/// 
/// ## Usage Examples:
/// 
/// ```bash
/// # Load data and save to database file
/// cargo run -- --input-dir data/data --save-db my_db.db
/// 
/// # Load from database and run queries
/// cargo run -- --run --load-db my_db.db --queries queries.json --output-dir results/
/// 
/// # Load from CSV and run queries in one step
/// cargo run -- --run --input-dir data/data --queries queries.json --output-dir results/
/// ```
fn main() -> Result<()> {
    let total_start = Instant::now();
    let args = Args::parse();

    // Load database from file or create and load from CSV
    let con = if let Some(db_path) = &args.load_db {
        println!("Loading database from {:?}...", db_path);
        load_database(db_path)?
    } else {
        println!("Loading data from CSV files...");
        let con = Connection::open(":memory:")?;
        load_data(&con, &args.input_dir)?;
        con
    };
    
    // Parse queries from JSON file
    let queries = parse_queries_from_file(&args.queries)?;
    println!("Parsed {} queries", queries.len());
    
    if args.run {
        // Convert queries to SQL and execute
        let mut results = Vec::new();
        for (i, query) in queries.iter().enumerate() {
            let sql = assemble_sql(query);
            let result = execute_query(&con, &sql, i + 1)?;
            results.push(result);
        }
        
        println!("Executed {} queries", results.len());

        // Write results to disk if output directory is specified
        if let Some(output_dir) = &args.output_dir {
            write_results_to_csv(&results, output_dir)?;
            println!("Results written to {:?}", output_dir);
        }
    }

    let total_time = total_start.elapsed();
    println!("Total runtime: {:.3}s", total_time.as_secs_f64());

    Ok(())
}
