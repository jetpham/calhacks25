use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;

/// Extract a value from a row as a properly formatted string
fn extract_value_as_string(row: &duckdb::Row, col_index: usize) -> String {
    let value = row.get_ref::<usize>(col_index).unwrap();
    match value {
        duckdb::types::ValueRef::Null => String::from("NULL"),
        duckdb::types::ValueRef::Boolean(b) => b.to_string(),
        duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
        duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
        duckdb::types::ValueRef::Int(i) => i.to_string(),
        duckdb::types::ValueRef::BigInt(i) => i.to_string(),
        duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
        duckdb::types::ValueRef::UTinyInt(u) => u.to_string(),
        duckdb::types::ValueRef::USmallInt(u) => u.to_string(),
        duckdb::types::ValueRef::UInt(u) => u.to_string(),
        duckdb::types::ValueRef::UBigInt(u) => u.to_string(),
        duckdb::types::ValueRef::Float(f) => trim_float(f as f64),
        duckdb::types::ValueRef::Double(d) => trim_float(d),
        duckdb::types::ValueRef::Decimal(d) => d.to_string(),
        duckdb::types::ValueRef::Timestamp(_, ts) => format!("{}", ts),
        duckdb::types::ValueRef::Text(bytes) => {
            match std::str::from_utf8(bytes) {
                Ok(s) => s.to_string(),
                Err(_) => format!("{:?}", bytes),
            }
        },
        duckdb::types::ValueRef::Blob(bytes) => format!("{:?}", bytes),
        duckdb::types::ValueRef::Date32(i) => {
            // Convert epoch days to date string
            use chrono::{NaiveDate, Datelike};
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(i as i64)) {
                format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
            } else {
                i.to_string()
            }
        },
        duckdb::types::ValueRef::Time64(_, i) => i.to_string(),
        duckdb::types::ValueRef::Interval { months, days, nanos } => format!("{}-{}-{}", months, days, nanos),
        // Handle other complex types as needed, or fallback:
        _ => "<unsupported>".to_string(),
    }
}

/// Trim unnecessary trailing zeros from float representation  
fn trim_float(v: f64) -> String {
    let s = v.to_string();
    // Remove trailing zeros and decimal point if not needed
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

/// Get the query execution plan using EXPLAIN ANALYZE (executes query with runtime stats)
pub fn _explain_query(con: &Connection, sql: &str) -> Result<String> {
    let explain_sql = format!("EXPLAIN ANALYZE {}", sql);
    let mut plan = String::new();
    
    // Execute and collect the plan (ANALYZE runs the query)
    let mut stmt = con.prepare(&explain_sql)?;
    let mut rows = stmt.query([])?;
    
    while let Some(row) = rows.next()? {
        plan.push_str(&row.get::<_, String>(1)?); // Column 1 contains the explain output
        plan.push('\n');
    }
    
    Ok(plan)
}

/// Query result structure
#[derive(Debug)]
pub struct QueryResult {
    pub query_num: usize,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Prepare a SQL statement and return it
pub fn prepare_query<'a>(con: &'a Connection, sql: &str) -> Result<duckdb::Statement<'a>> {
    let stmt = con.prepare(sql)?;
    Ok(stmt)
}

/// Write a single query result to CSV
pub fn write_single_result_to_csv(
    query_num: usize,
    mut rows: duckdb::Rows,
    output_dir: &PathBuf,
) -> Result<()> {
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    let out_path = output_dir.join(format!("q{}.csv", query_num));
    let mut file = std::fs::File::create(&out_path)?;
    let mut wtr = csv::Writer::from_writer(&mut file);
    
    // Get column names from the statement after it's been executed
    let stmt_ref = rows.as_ref().ok_or_else(|| anyhow::anyhow!("Failed to get statement reference"))?;
    let column_count = stmt_ref.column_count();
    let columns: Vec<String> = (0..column_count)
        .map(|i| stmt_ref.column_name(i).map(|s| s.to_string()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    
    // Write header
    wtr.write_record(&columns)?;
    
    // Write rows
    while let Some(row) = rows.next()? {
        let mut record = Vec::new();
        for i in 0..column_count {
            let value = extract_value_as_string(&row, i);
            record.push(value);
        }
        wtr.write_record(&record)?;
    }
    
    wtr.flush()?;
    
    Ok(())
}

