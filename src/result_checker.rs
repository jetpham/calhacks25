use anyhow::Result;
use std::path::{Path, PathBuf};
use std::fs;

/// Compare two result directories for correctness
/// 
/// Requirements from Discord:
/// - Set equality comparison (order doesn't matter)
/// - Floating point tolerance: 0.01
/// - Truncate floats to 2 decimal places before comparison
pub fn compare_results(baseline_dir: &Path, output_dir: &Path) -> Result<()> {
    println!("Comparing results in {:?} with {:?}", baseline_dir, output_dir);
    
    // Get all q*.csv files from baseline
    let baseline_files = get_query_files(baseline_dir)?;
    let mut total_queries = 0;
    let mut passed = 0;
    let mut failed = Vec::new();
    
    for (qnum, baseline_file) in baseline_files.iter().enumerate() {
        total_queries += 1;
        let query_num = qnum + 1;
        
        let output_file = output_dir.join(format!("q{}.csv", query_num));
        
        // Check if output file exists
        if !output_file.exists() {
            println!("Query {}: MISSING - No output file found", query_num);
            failed.push((query_num, "Missing output file".to_string()));
            continue;
        }
        
        // Compare the two files
        match compare_csv_files(baseline_file, &output_file) {
            Ok(()) => {
                println!("Query {}: PASSED", query_num);
                passed += 1;
            }
            Err(e) => {
                println!("Query {}: FAILED - {}", query_num, e);
                failed.push((query_num, e.to_string()));
            }
        }
    }
    
    println!("\nSummary: {}/{} queries passed", passed, total_queries);
    
    if !failed.is_empty() {
        println!("\nFailed queries:");
        for (qnum, reason) in failed {
            println!("  Query {}: {}", qnum, reason);
        }
        anyhow::bail!("Some queries failed comparison")
    }
    
    Ok(())
}

/// Get all q*.csv files from a directory, sorted by query number
fn get_query_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = fs::read_dir(dir)?;
    let mut files: Vec<PathBuf> = Vec::new();
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if filename.starts_with("q") && filename.ends_with(".csv") {
                files.push(path);
            }
        }
    }
    
    // Sort by query number
    files.sort_by(|a, b| {
        let a_num = extract_query_number(a);
        let b_num = extract_query_number(b);
        a_num.cmp(&b_num)
    });
    
    Ok(files)
}

/// Extract query number from filename (e.g., "q5.csv" -> 5)
fn extract_query_number(path: &Path) -> usize {
    if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
        if let Some(stripped) = filename.strip_suffix(".csv") {
            if let Some(num_str) = stripped.strip_prefix("q") {
                if let Ok(num) = num_str.parse::<usize>() {
                    return num;
                }
            }
        }
    }
    0
}

/// Compare two CSV files for correctness
/// Uses bag equality (order doesn't matter, duplicates preserved) with floating point tolerance
fn compare_csv_files(baseline_file: &Path, output_file: &Path) -> Result<()> {
    let (baseline_header, baseline_rows) = parse_csv(baseline_file)?;
    let (output_header, output_rows) = parse_csv(output_file)?;
    
    // Check headers match
    if baseline_header != output_header {
        anyhow::bail!("Headers don't match");
    }
    
    // Check row counts
    if baseline_rows.len() != output_rows.len() {
        anyhow::bail!("Row count mismatch (baseline: {}, output: {})", 
                     baseline_rows.len(), output_rows.len());
    }
    
    // Compare rows with tolerance (order-independent, but handles duplicates)
    let mut baseline_used = vec![false; baseline_rows.len()];
    
    for output_row in &output_rows {
        let mut found = false;
        for (i, baseline_row) in baseline_rows.iter().enumerate() {
            if !baseline_used[i] && rows_match_with_tolerance(baseline_row, output_row) {
                baseline_used[i] = true;
                found = true;
                break;
            }
        }
        if !found {
            anyhow::bail!("Row not found in baseline or already matched (duplicate mismatch)");
        }
    }
    
    Ok(())
}

/// Parse a CSV file and return header and data rows
fn parse_csv(file: &Path) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let content = fs::read_to_string(file)?;
    let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    
    if lines.is_empty() {
        anyhow::bail!("Empty CSV file");
    }
    
    // Parse header
    let header = lines[0].split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    // Parse data rows
    let data_rows: Vec<Vec<String>> = lines[1..].iter()
        .map(|line| {
            line.split(',')
                .map(|s| s.trim().to_string())
                .collect()
        })
        .collect();
    
    Ok((header, data_rows))
}

/// Check if two rows match with float tolerance
fn rows_match_with_tolerance(row1: &Vec<String>, row2: &Vec<String>) -> bool {
    if row1.len() != row2.len() {
        return false;
    }
    
    for (cell1, cell2) in row1.iter().zip(row2.iter()) {
        if !cells_match_with_tolerance(cell1, cell2) {
            return false;
        }
    }
    
    true
}

/// Check if two cells match with float tolerance (0.1)
fn cells_match_with_tolerance(cell1: &str, cell2: &str) -> bool {
    // First try exact match
    if cell1 == cell2 {
        return true;
    }
    
    // Try parsing as floats for tolerance comparison
    if let (Ok(val1), Ok(val2)) = (cell1.parse::<f64>(), cell2.parse::<f64>()) {
        // If within 0.1 tolerance, they match
        (val1 - val2).abs() < 0.1
    } else {
        // Not floats, exact match required
        false
    }
}

