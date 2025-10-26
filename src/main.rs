use anyhow::Result;
use clap::Parser;
use std::path::{PathBuf, Path};
use std::time::{Instant, Duration};
use duckdb::Connection;

mod data_loader;
mod preprocessor;
mod query_executor;
mod query_handler;
mod result_checker;

use data_loader::load_data;
use preprocessor::{create_indexes, create_rollup_tables};
use query_executor::{prepare_query, write_single_result_to_csv, explain_query};
use query_handler::{parse_queries_from_file, assemble_sql};
use result_checker::compare_results;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, value_name = "DIR", default_value = "data/data")]
    input_dir: PathBuf,

    #[arg(long)]
    run: bool,

    #[arg(long, value_name = "DIR")]
    output_dir: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        default_value = "queries.json",
        requires = "run"
    )]
    queries: PathBuf,

    #[arg(long)]
    use_existing: bool,

    #[arg(long, value_name = "DIR")]
    baseline_dir: Option<PathBuf>,

    #[arg(long)]
    profile: bool,

    #[arg(long, default_value = "1")]
    runs: usize,

    #[arg(long)]
    skip_save: bool,
}

fn find_next_db_filename() -> Result<PathBuf> {
    // First check if any duckN.db files exist
    if !Path::new("duck1.db").exists() {
        return Ok(PathBuf::from("duck1.db"));
    }
    
    // Find the highest existing number
    let mut max_num = 0;
    for i in 1..=100 {
        let path = format!("duck{}.db", i);
        if Path::new(&path).exists() {
            max_num = i;
        }
    }
    
    // Return the next available number
    Ok(PathBuf::from(format!("duck{}.db", max_num + 1)))
}

fn find_latest_db_filename() -> Result<PathBuf> {
    // Check for old database location first (backward compatibility)
    let old_db = PathBuf::from("tmp/concurrent.duckdb");
    if old_db.exists() {
        return Ok(old_db);
    }
    
    // Find the highest existing number
    let mut max_num = 0;
    for i in 1..=100 {
        let path = format!("duck{}.db", i);
        if Path::new(&path).exists() {
            max_num = i;
        }
    }
    
    if max_num == 0 {
        return Ok(PathBuf::from("duck1.db"));
    }
    
    Ok(PathBuf::from(format!("duck{}.db", max_num)))
}

fn main() -> Result<()> {
    let total_start = Instant::now();
    let args = Args::parse();

    if let Some(baseline_dir) = &args.baseline_dir {
        if !args.run {
            let Some(output_dir) = &args.output_dir else {
                anyhow::bail!("--output-dir required when using --baseline-dir");
            };
            return compare_results(baseline_dir, output_dir);
        }
    }

    let db_path = if args.use_existing {
        find_latest_db_filename()?
    } else {
        find_next_db_filename()?
    };
    
    println!("Using persistent database: {}", db_path.display());
    
    if db_path.exists() && args.use_existing {
        println!("Using existing database...");
    } else {
        println!("Creating database from CSV files...");
        
        if db_path.exists() {
            std::fs::remove_file(&db_path)?;
        }
        
        let file_con = Connection::open(&db_path)?;
        load_data(&file_con, &args.input_dir)?;
        create_rollup_tables(&file_con)?;
        
        println!("Database saved to {}", db_path.display());
        
        println!("Creating indexes on persistent database...");
        create_indexes(&file_con)?;
        
        println!("Database setup complete");
    }
    
    let con = Connection::open(&db_path)?;
    println!("Ready for query execution");
    
    let queries = parse_queries_from_file(&args.queries)?;
    println!("Parsed {} queries", queries.len());
    
    if args.run {
        let Some(output_dir) = &args.output_dir else {
            anyhow::bail!("--output-dir required with --run");
        };
        
        println!("Converting {} queries to SQL...", queries.len());
        let sql_queries: Vec<String> = queries.iter()
            .map(|q| assemble_sql(q))
            .collect();
        
        println!("Preparing all statements...");
        let mut prepared_statements: Vec<_> = sql_queries
            .iter()
            .map(|sql| prepare_query(&con, sql))
            .collect::<Result<Vec<_>, _>>()?;
        
        if args.profile {
            println!("\n=== Running EXPLAIN ANALYZE on all queries ===");
            for (i, sql) in sql_queries.iter().enumerate() {
                println!("\n=== Query {} EXPLAIN ANALYZE ===", i + 1);
                explain_query(&con, sql, i + 1)?;
                println!();
            }
        }
        
        println!("Executing {} queries {} time(s) each...", prepared_statements.len(), args.runs);
        let mut total_duration = Duration::ZERO;
        
        let num_queries = prepared_statements.len();
        let mut query_times = vec![Vec::new(); num_queries];
        
        for run in 1..=args.runs {
            if args.runs > 1 {
                println!("\n--- Run {}/{} ---", run, args.runs);
            }
            
            let run_total = Instant::now();
            
            con.execute("BEGIN TRANSACTION", [])?;
            
            for (i, stmt) in prepared_statements.iter_mut().enumerate() {
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
                
                if run == 1 {
                    write_single_result_to_csv(i + 1, rows, output_dir)?;
                }
            }
            
            con.execute("COMMIT", [])?;
            
            if args.runs > 1 {
                println!("Run {} total time: {:.3}s", run, run_total.elapsed().as_secs_f64());
            }
        }
        
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

        if let Some(baseline_dir) = &args.baseline_dir {
            println!("\nComparing results...");
            compare_results(baseline_dir, output_dir)?;
        }
    }

    let total_time = total_start.elapsed();
    println!("Total runtime: {:.3}s", total_time.as_secs_f64());

    Ok(())
}
