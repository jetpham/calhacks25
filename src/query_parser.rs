use anyhow::Result;
use serde_json::Value;
use std::path::PathBuf;
use std::fs;

/// Parse queries from JSON file
pub fn parse_queries_from_file(queries_path: &PathBuf) -> Result<Vec<Value>> {
    let content = fs::read_to_string(queries_path)?;
    let queries: Vec<Value> = serde_json::from_str(&content)?;
    Ok(queries)
}
