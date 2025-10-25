use clap::Parser;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;

mod data_loader;
mod query_executor;
mod query_parser;
mod sql_assembler;

use query_executor::{run_queries, write_results_to_csv};
use query_parser::parse_queries_from_file;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, value_name = "DIR", default_value = "data/data")]
    input_dir: PathBuf,

    #[arg(long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    #[arg(long, value_name = "FILE", default_value = "queries.json")]
    queries: PathBuf,

    #[arg(long, value_name = "DIR")]
    check_dir: Option<PathBuf>,

    /// Save the preprocessed database to a file after data loading
    #[arg(long, value_name = "FILE", conflicts_with = "load_db")]
    save_db: Option<PathBuf>,

    /// Load a preprocessed database from a file instead of loading from CSV
    #[arg(long, value_name = "FILE", conflicts_with = "save_db")]
    load_db: Option<PathBuf>,
}

fn main() -> Result<()> {
    let total_start = Instant::now();
    let args = Args::parse();
    
    // Parse queries from JSON file
    let queries = parse_queries_from_file(&args.queries)?;
    
    // Run queries (includes preprocessing and benchmarking)
    let results = run_queries(&queries, &args.input_dir, &args.save_db, &args.load_db)?;
    
    // Write results to disk if output directory is specified
    if let Some(output_dir) = &args.output_dir {
        write_results_to_csv(&results, output_dir)?;
    }
    
    let total_time = total_start.elapsed();
    println!("Total runtime: {:.3}s", total_time.as_secs_f64());
    
    Ok(())
}