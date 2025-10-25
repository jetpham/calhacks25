use clap::Parser;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;

mod sql_converter;
mod data_loader;
mod query_parser;
mod query_executor;
mod result_handler;

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
}


#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    
    // Create optimized DataFusion context for concurrent execution
    let ctx = data_loader::create_optimized_context();
    
    // PHASE 1: Load CSV files
    data_loader::load_csv_files(&ctx, &args.input_dir).await?;
    
    // PHASE 2: Parse JSON queries to SQL
    let sql_queries = query_parser::parse_queries_from_file(&args.queries)?;
    
    // PHASE 3: Execute SQL queries (timing measurement)
    let start_time = Instant::now();
    let results = query_executor::execute_queries_for_timing(&ctx, &sql_queries).await?;
    let total_duration = start_time.elapsed();
    println!("{:.2}", total_duration.as_secs_f64());
    
    // PHASE 4: Save results to CSV files if output directory is specified
    if let Some(output_dir) = &args.output_dir {
        result_handler::save_results_to_csv(results, output_dir).await?;
        
        // PHASE 5: Check results against reference directory if specified
        if let Some(check_dir) = &args.check_dir {
            result_handler::check_results(output_dir, check_dir).await?;
        }
    }
    
    Ok(())
}