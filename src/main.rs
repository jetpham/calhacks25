use clap::Parser;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

mod sql_converter;
mod query_executor;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, value_name = "DIR", default_value = "data/data")]
    input_dir: PathBuf,

    #[arg(long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    #[arg(long, value_name = "FILE", default_value = "queries.json")]
    queries: PathBuf,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    
    // Create DataFusion context
    let ctx = SessionContext::new();
    
    // PHASE 1: Load CSV files
    query_executor::load_csv_files(&ctx, &args.input_dir).await?;
    
    // PHASE 2: Parse JSON queries to SQL
    let sql_queries = query_executor::parse_queries_from_file(&args.queries)?;
    
    // PHASE 3: Execute SQL queries
    let start_time = Instant::now();

    query_executor::execute_queries(&ctx, &sql_queries).await?;
    
    let total_duration = start_time.elapsed();
    println!("{:.2}", total_duration.as_secs_f64());
    
    Ok(())
}