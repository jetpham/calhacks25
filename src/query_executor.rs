use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::time::Instant;
use std::fs;

/// Query result structure
#[derive(Debug)]
pub struct QueryResult {
    pub query_num: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Execute a single SQL query and return result
pub fn execute_query(con: &Connection, sql: &str, query_num: usize) -> Result<QueryResult> {
    let query_name = format!("Query {}", query_num);
    println!("Executing {}...", query_name);
    
    let query_start = Instant::now();
    
    // Get the actual results
    let mut stmt = con.prepare(sql)?;
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
    
    let elapsed = query_start.elapsed();
    println!("{} completed: {:.3}s ({} rows)", 
            query_name, elapsed.as_secs_f64(), all_rows.len());

    Ok(QueryResult {
        query_num,
        columns,
        rows: all_rows,
    })
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

