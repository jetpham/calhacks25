use anyhow::Result;
use duckdb::Connection;
use serde_json::Value;
use std::path::PathBuf;
use std::time::Instant;
use std::fs;
use crate::sql_assembler::assemble_sql;
use crate::profiler::{ProfilingConfig, ProfilingMode, setup_profiling, execute_with_profiling, generate_profiling_report, generate_query_graph};

/// Query result structure
#[derive(Debug)]
pub struct QueryResult {
    pub query_num: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Execute queries and return results with comprehensive profiling
pub fn run_queries(con: &Connection, queries: &[Value]) -> Result<Vec<QueryResult>> {
    let mut query_results = Vec::new();
    let mut profiling_results = Vec::new();
    let benchmark_start = Instant::now();
    
    // Setup profiling for query execution
    let profiling_config = ProfilingConfig {
        mode: ProfilingMode::Both,
        output_dir: PathBuf::from("profiling"),
        enable_detailed: true,
        enable_optimizer_metrics: true,
        enable_planner_metrics: true,
        enable_physical_planner_metrics: true,
    };
    
    setup_profiling(con, &profiling_config)?;
    
    for (i, q) in queries.iter().enumerate() {
        let sql = assemble_sql(q);
        let query_name = format!("Query {}", i + 1);
        
        println!("Executing {}...", query_name);
        
        // Execute query with profiling
        let profiling_result = execute_with_profiling(con, &sql, &query_name, &profiling_config)?;
        profiling_results.push(profiling_result);
        
        // Also get the actual results for output
        let mut stmt = con.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        let mut all_rows = Vec::new();
        let mut columns = Vec::new();
        let mut column_count = 0;
        
        // Get column info from first row
        if let Some(first_row) = rows.next()? {
            // Determine column count
            let mut i = 0;
            while let Ok(_) = first_row.get::<_, String>(i) {
                i += 1;
            }
            column_count = i;
            
            // Use generic column names for now
            columns = (0..column_count).map(|i| format!("column{}", i)).collect();
            
            // Process first row
            let mut row_data = Vec::new();
            for i in 0..column_count {
                let value: String = first_row.get(i)?;
                row_data.push(value);
            }
            all_rows.push(row_data);
        }
        
        // Collect remaining rows
        while let Some(row) = rows.next()? {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                let value: String = row.get(i)?;
                row_data.push(value);
            }
            all_rows.push(row_data);
        }
        
        println!("{} completed: {:.3}s (CPU: {:.3}s, Rows: {})", 
                query_name, profiling_results[i].total_time, 
                profiling_results[i].cpu_time, profiling_results[i].rows_returned);

        // Store result in memory
        query_results.push(QueryResult {
            query_num: i + 1,
            columns,
            rows: all_rows,
        });
    }
    
    let total_benchmark_time = benchmark_start.elapsed();
    println!("Total benchmark time: {:.3}s", total_benchmark_time.as_secs_f64());
    
    // Generate profiling report
    generate_profiling_report(&profiling_results, &profiling_config.output_dir)?;
    
    // Generate query graph if requested
    if matches!(profiling_config.mode, ProfilingMode::QueryGraph | ProfilingMode::Both) {
        let profile_file = profiling_config.output_dir.join("query_profile.json");
        generate_query_graph(&profile_file, &profiling_config.output_dir)?;
    }

    Ok(query_results)
}

/// Write query results to CSV files
pub fn write_results_to_csv(results: &[QueryResult], output_dir: &PathBuf) -> Result<()> {
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    for result in results {
        let out_path = output_dir.join(format!("q{}.csv", result.query_num));
        let mut file = std::fs::File::create(&out_path)?;
        let mut wtr = csv::Writer::from_writer(&mut file);
        
        // Write header
        wtr.write_record(&result.columns)?;
        
        // Write rows
        for row in &result.rows {
            wtr.write_record(row)?;
        }
        
        wtr.flush()?;
    }
    
    Ok(())
}

