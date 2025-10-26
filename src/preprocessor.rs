use duckdb::Connection;
use anyhow::Result;

const TABLE_NAME: &str = "events";
const MATERIALIZED_TABLE: &str = "events_table";


/// Create indexes on specified columns (empty by default for now)
pub fn create_indexes_on_all_columns(con: &Connection) -> Result<()> {
    // Based on profiling: filter columns are type, country, ts (dates)
    let columns: Vec<&str> = vec![
        "type",      // Most queries filter on this
        "country",   // Query 2 filters
        "ts",        // Date filtering in queries
        "day",       // Grouped in queries
    ];
    
    if columns.is_empty() {
        println!("No indexes to create");
        return Ok(());
    }
    
    println!("Materializing events data into table...");
    
    // Materialize the view into a table for better performance
    con.execute(
        &format!("CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {}", MATERIALIZED_TABLE, TABLE_NAME),
        [],
    )?;
    println!("Events table materialized");
    
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


