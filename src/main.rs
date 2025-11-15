use anyhow::Result;
use clap::Parser;
use std::path::{PathBuf, Path};
use std::time::{Instant, Duration};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};

fn format_duration_seconds(duration: Duration) -> String {
    let total_secs = duration.as_secs_f64();
    format!("{:.1}s", total_secs)
}

fn format_duration_ms_ns(duration: Duration) -> String {
    let total_ns = duration.as_nanos();
    let total_ms = total_ns as f64 / 1_000_000.0;
    format!("{:.2}ms", total_ms)
}

mod data_loader;
mod preprocessor;
mod query_executor;
mod query_handler;
mod result_checker;
mod mv;
mod planner;
mod hardware;

use data_loader::load_data;
use preprocessor::{create_materialized_views, compute_mv_stats, warmup_cache, create_indexes, create_type_partitioned_materialized_views, load_all_mvs_from_db};
use query_executor::{prepare_query, write_single_result_to_csv, explain_query};
use query_handler::parse_queries_from_file;
use result_checker::compare_results;
use planner::Planner;

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

    #[arg(long, value_name = "FILE")]
    use_existing: Option<PathBuf>,

    #[arg(long, value_name = "DIR")]
    baseline_dir: Option<PathBuf>,

    #[arg(long)]
    profile: bool,

    #[arg(long, default_value = "1")]
    runs: usize,
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

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(baseline_dir) = &args.baseline_dir {
        if !args.run {
            let Some(output_dir) = &args.output_dir else {
                anyhow::bail!("--output-dir required when using --baseline-dir");
            };
            return compare_results(baseline_dir, output_dir);
        }
    }

    let db_path = if let Some(existing_path) = &args.use_existing {
        existing_path.clone()
    } else {
        find_next_db_filename()?
    };
    
    // Part 1: Print DB file status
    if db_path.exists() && args.use_existing.is_some() {
        println!("Using existing database: {}", db_path.display());
    } else {
        println!("Creating new database: {}", db_path.display());
        
        let preprocess_start = Instant::now();
        
        // Part 2: Preprocessing progress bar
        let pb = ProgressBar::new(6);
        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        pb.set_message("Preprocessing database");
        
        if db_path.exists() {
            std::fs::remove_file(&db_path)?;
        }
        
        let file_con = Connection::open(&db_path)?;
        pb.set_message("Loading data...");
        load_data(&file_con, &args.input_dir)?;
        pb.inc(1);
        
        pb.set_message("Creating materialized views...");
        let mut mvs = create_materialized_views(&file_con)?;
        pb.inc(1);
        
        pb.set_message("Computing MV statistics...");
        compute_mv_stats(&file_con, &mut mvs)?;
        pb.inc(1);
        
        pb.set_message("Creating type-partitioned MVs...");
        let mut partitioned_mvs = create_type_partitioned_materialized_views(&file_con, &mvs)?;
        pb.inc(1);
        
        let partitioned_count = partitioned_mvs.len();
        
        // Combine base and partitioned MVs
        mvs.append(&mut partitioned_mvs);
        
        pb.set_message("Computing partitioned MV statistics...");
        let total_mvs = mvs.len();
        compute_mv_stats(&file_con, &mut mvs[total_mvs - partitioned_count..])?;
        pb.inc(1);
        
        pb.set_message("Creating indexes...");
        create_indexes(&file_con, &mvs)?;
        pb.inc(1);
        
        pb.finish_and_clear();
        let preprocess_duration = preprocess_start.elapsed();
        println!("Database preprocessing completed in {}", format_duration_seconds(preprocess_duration));
    }
    
    let con = Connection::open(&db_path)?;
    
    if args.run {
        let Some(output_dir) = &args.output_dir else {
            anyhow::bail!("--output-dir required with --run");
        };
        
        // Part 3: Query prep progress bar
        let prep_start = Instant::now();
        let prep_pb = ProgressBar::new(4);
        prep_pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        prep_pb.set_message("Preparing queries");
        
        prep_pb.set_message("Parsing queries...");
        let queries = parse_queries_from_file(&args.queries)?;
        prep_pb.inc(1);
        
        prep_pb.set_message("Loading materialized views...");
        // Load all MVs from database (base + type-partitioned)
        let mut mvs = load_all_mvs_from_db(&con)?;
        
        if mvs.is_empty() {
            // Fallback: create base MVs if none exist
            mvs = create_materialized_views(&con)?;
        }
        prep_pb.inc(1);
        
        prep_pb.set_message("Computing statistics...");
        compute_mv_stats(&con, &mut mvs)?;
        prep_pb.inc(1);
        
        prep_pb.set_message("Planning and preparing queries...");
        let planner = Planner::new(&con);
        
        let sql_queries: Vec<String> = queries.iter()
            .map(|q| planner.translate_query(q, &mut mvs, false).unwrap_or_else(|_| {
                // Fallback to plain SQL if planner fails
                query_handler::assemble_sql(q)
            }))
            .collect();
        
        let mut prepared_statements: Vec<_> = sql_queries
            .iter()
            .map(|sql| prepare_query(&con, sql))
            .collect::<Result<Vec<_>, _>>()?;
        
        if args.profile {
            for (i, sql) in sql_queries.iter().enumerate() {
                explain_query(&con, sql, i + 1)?;
            }
        }
        
        prep_pb.set_message("Warming up database...");
        warmup_cache(&con, &mvs)?;
        prep_pb.inc(1);
        
        prep_pb.finish_and_clear();
        let prep_duration = prep_start.elapsed();
        println!("Query preparation and warmup completed in {}", format_duration_seconds(prep_duration));
        
        // Part 4: Query execution progress bar
        let exec_start = Instant::now();
        let exec_pb = ProgressBar::new(args.runs as u64);
        exec_pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Running queries...")
                .unwrap()
                .progress_chars("#>-")
        );
        
        let mut total_duration = Duration::ZERO;
        
        let num_queries = prepared_statements.len();
        let mut query_times = vec![Vec::new(); num_queries];
        
        for run in 1..=args.runs {
            con.execute("BEGIN TRANSACTION", [])?;
            
            for (i, stmt) in prepared_statements.iter_mut().enumerate() {
                let query_start = Instant::now();
                let rows = stmt.query([])?;
                let duration = query_start.elapsed();
                
                query_times[i].push(duration.as_secs_f64());
                
                total_duration += duration;
                
                if run == 1 {
                    write_single_result_to_csv(i + 1, rows, output_dir)?;
                }
            }
            
            con.execute("COMMIT", [])?;
            exec_pb.inc(1);
        }
        
        exec_pb.finish_and_clear();
        let exec_duration = exec_start.elapsed();
        println!("Query execution completed in {}", format_duration_seconds(exec_duration));
        
        // Part 5: Summary
        println!("\n=== Query Performance Summary ===");
        let mut sum_of_averages_ns = 0u64;
        for (i, times) in query_times.iter().enumerate() {
            // Convert f64 seconds to nanoseconds for averaging
            let avg_ns = (times.iter().sum::<f64>() / times.len() as f64 * 1_000_000_000.0) as u64;
            sum_of_averages_ns = sum_of_averages_ns.saturating_add(avg_ns);
            let avg_duration = Duration::from_nanos(avg_ns);
            println!("Query {}: {} average", i + 1, format_duration_ms_ns(avg_duration));
        }
        let sum_avg_duration = Duration::from_nanos(sum_of_averages_ns);
        println!("Sum of averages: {}", format_duration_ms_ns(sum_avg_duration));

        if let Some(baseline_dir) = &args.baseline_dir {
            compare_results(baseline_dir, output_dir)?;
        }
    }

    Ok(())
}
