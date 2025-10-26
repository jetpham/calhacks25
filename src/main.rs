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

    let con = Connection::open_in_memory()?;
    
    let mut db_num = 1;
    let db_path = loop {
        let path = PathBuf::from(format!("duck{}.db", db_num));
        if !path.exists() || (args.use_existing && path.exists()) {
            break path;
        }
        db_num += 1;
    };
    
    println!("Using database: {}", db_path.display());
    
    if db_path.exists() && args.use_existing {
        println!("Loading existing database into memory...");
        let db_name = "loaded_db";
        con.execute(&format!("ATTACH '{}' AS {}", db_path.display(), db_name), [])?;
        con.execute(&format!("COPY FROM DATABASE {} TO memory", db_name), [])?;
        con.execute(&format!("DETACH {}", db_name), [])?;
        println!("Database loaded into memory");
    } else {
        println!("Creating database from CSV files...");
        load_data(&con, &args.input_dir)?;
        create_indexes_on_all_columns(&con)?;
        create_rollup_tables(&con)?;
        
        println!("Saving database to disk...");
        let db_name = "disk_db";
        con.execute(&format!("ATTACH '{}' AS {}", db_path.display(), db_name), [])?;
        con.execute(&format!("COPY FROM DATABASE memory TO {}", db_name), [])?;
        con.execute(&format!("DETACH {}", db_name), [])?;
        println!("Database saved to {}", db_path.display());
    }
    
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
