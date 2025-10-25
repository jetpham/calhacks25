use serde_json::Value;
use std::path::PathBuf;
use std::fs;
use anyhow::Result;

/// Phase 2: Parse JSON queries to SQL
pub fn parse_queries_from_file(queries_file: &PathBuf) -> Result<Vec<String>> {
    let queries_content = fs::read_to_string(queries_file)?;
    let queries: Vec<Value> = serde_json::from_str(&queries_content)?;
    
    let mut sql_queries = Vec::new();
    for query in queries.iter() {
        let sql = crate::sql_converter::assemble_sql(query);
        sql_queries.push(sql);
    }
    
    Ok(sql_queries)
}
