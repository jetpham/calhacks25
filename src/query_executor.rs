use anyhow::Result;
use duckdb::Connection;
use std::path::PathBuf;
use std::fs;

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
        _ => "<unsupported>".to_string(),
    }
}

fn trim_float(v: f64) -> String {
    let s = v.to_string();
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

pub fn explain_query(con: &Connection, sql: &str, query_num: usize) -> Result<()> {
    use std::path::PathBuf;
    
    let profile_dir = PathBuf::from("profiling");
    std::fs::create_dir_all(&profile_dir)?;
    let profile_file = profile_dir.join(format!("q{}.json", query_num));
    let temp_file = format!("/tmp/duckdb_profile_{}.json", query_num);
    
    con.execute("PRAGMA enable_profiling = 'json'", [])?;
    con.execute(&format!("PRAGMA profiling_output = '{}'", temp_file), [])?;
    
    con.execute(
        r#"PRAGMA custom_profiling_settings = '{"OPERATOR_TIMING": "true", "OPERATOR_CARDINALITY": "true", "CPU_TIME": "true", "EXTRA_INFO": "true"}'"#,
        [],
    )?;
    
    let mut stmt = con.prepare(sql)?;
    let _rows = stmt.query([])?;
    
    
    match std::fs::read_to_string(&temp_file) {
        Ok(json_content) => {
            std::fs::write(&profile_file, &json_content)?;
            
            let _ = std::fs::remove_file(&temp_file);
        }
        Err(e) => {
            eprintln!("Could not read profile file: {}", e);
            println!("Could not read profile file: {}", e);
        }
    }
    
    Ok(())
}

pub fn prepare_query<'a>(con: &'a Connection, sql: &str) -> Result<duckdb::Statement<'a>> {
    let stmt = con.prepare(sql)?;
    Ok(stmt)
}

pub fn write_single_result_to_csv(
    query_num: usize,
    mut rows: duckdb::Rows,
    output_dir: &PathBuf,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    
    let out_path = output_dir.join(format!("q{}.csv", query_num));
    let mut file = std::fs::File::create(&out_path)?;
    let mut wtr = csv::Writer::from_writer(&mut file);
    
    let stmt_ref = rows.as_ref().ok_or_else(|| anyhow::anyhow!("Failed to get statement reference"))?;
    let column_count = stmt_ref.column_count();
    let mut columns: Vec<String> = (0..column_count)
        .map(|i| stmt_ref.column_name(i).map(|s| s.to_string()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    
    // Fix column name normalization: DuckDB Rust bindings may return count(*) instead of count_star()
    // Check if we have COUNT(*) and normalize it
    for col in &mut columns {
        if col == "count(*)" {
            *col = "count_star()".to_string();
        }
    }
    
    wtr.write_record(&columns)?;
    
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

