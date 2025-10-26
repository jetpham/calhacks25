use duckdb::Connection;
use anyhow::Result;

const MATERIALIZED_TABLE: &str = "events_table";


/// Create indexes on specified columns for better query performance
pub fn create_indexes_on_all_columns(con: &Connection) -> Result<()> {
    // Based on profiling: filter columns are type, country, ts (dates)
    // Indexing these columns would help Q1-Q5 significantly
    let columns: Vec<&str> = vec![
        "type",      // Used in filters for Q1, Q2, Q3, Q5
        "day",       // Used in group by for Q1
        "country",   // Used in filters for Q2
        "ts",        // Used for date filtering in Q2, Q5
    ];
    
    // Note: Materialization is already done in data_loader.rs
    println!("Creating indexes on {} columns...", columns.len());
    
    // Create an index on each specified column
    for column in columns {
        let index_name: String = format!("idx_{}", column);
        println!("Creating index: {} ON {}({})", index_name, MATERIALIZED_TABLE, column);
        
        // Use IF NOT EXISTS to avoid errors if index already exists
        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}({})", 
            index_name, MATERIALIZED_TABLE, column
        );
        
        if let Err(e) = con.execute(&create_index_sql, []) {
            eprintln!("Warning: Failed to create index on {}: {}", column, e);
        }
    }
    
    println!("Index creation complete");
    Ok(())
}


