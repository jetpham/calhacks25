use clap::Parser;
use std::path::PathBuf;

/// CalHacks Data Processing Application
/// 
/// Processes data from a specified directory and executes queries.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Input directory containing CSV files
    #[arg(long, value_name = "DIR", default_value = "/home/jet/Documents/calhacks/data/data")]
    input_dir: PathBuf,

    /// Output directory for results
    #[arg(long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    /// Path to queries.json file
    #[arg(long, value_name = "FILE", default_value = "queries.json")]
    queries: PathBuf,
}

fn get_default_output_dir() -> PathBuf {
    let base = PathBuf::from("/home/jet/Documents/calhacks/results/calhacks");
    let mut counter = 1;
    let mut output_dir = base.clone();

    while output_dir.exists() {
        counter += 1;
        output_dir = base.with_extension(counter.to_string());
    }

    output_dir
}

fn main() {
    let mut args = Args::parse();
    
    // Set default output directory if not provided
    if args.output_dir.is_none() {
        args.output_dir = Some(get_default_output_dir());
    }
    
    println!("Input Directory: {:?}", args.input_dir);
    println!("Output Directory: {:?}", args.output_dir.unwrap());
    println!("Queries File: {:?}", args.queries);
    
    // TODO: Implement the actual data processing logic here
    println!("CLI arguments parsed successfully!");
}
