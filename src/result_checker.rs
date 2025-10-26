use anyhow::Result;
use std::path::{Path, PathBuf};
use std::fs;

pub fn compare_results(baseline_dir: &Path, output_dir: &Path) -> Result<()> {
    println!("Comparing results in {:?} with {:?}", baseline_dir, output_dir);
    
    let baseline_files = get_query_files(baseline_dir)?;
    let mut total_queries = 0;
    let mut passed = 0;
    let mut failed = Vec::new();
    
    for (qnum, baseline_file) in baseline_files.iter().enumerate() {
        total_queries += 1;
        let query_num = qnum + 1;
        
        let output_file = output_dir.join(format!("q{}.csv", query_num));
        
        if !output_file.exists() {
            println!("Query {}: MISSING - No output file found", query_num);
            failed.push((query_num, "Missing output file".to_string()));
            continue;
        }
        
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
    
    files.sort_by(|a, b| {
        let a_num = extract_query_number(a);
        let b_num = extract_query_number(b);
        a_num.cmp(&b_num)
    });
    
    Ok(files)
}

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

fn compare_csv_files(baseline_file: &Path, output_file: &Path) -> Result<()> {
    let (baseline_header, baseline_rows) = parse_csv(baseline_file)?;
    let (output_header, output_rows) = parse_csv(output_file)?;
    
    if baseline_header != output_header {
        anyhow::bail!("Headers don't match");
    }
    
    if baseline_rows.len() != output_rows.len() {
        anyhow::bail!("Row count mismatch (baseline: {}, output: {})", 
                     baseline_rows.len(), output_rows.len());
    }
    
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

fn parse_csv(file: &Path) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let content = fs::read_to_string(file)?;
    let lines: Vec<&str> = content.lines().filter(|l| !l.trim().is_empty()).collect();
    
    if lines.is_empty() {
        anyhow::bail!("Empty CSV file");
    }
    
    let header = lines[0].split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    let data_rows: Vec<Vec<String>> = lines[1..].iter()
        .map(|line| {
            line.split(',')
                .map(|s| s.trim().to_string())
                .collect()
        })
        .collect();
    
    Ok((header, data_rows))
}

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

fn cells_match_with_tolerance(cell1: &str, cell2: &str) -> bool {
    if cell1 == cell2 {
        return true;
    }
    
    if let (Ok(val1), Ok(val2)) = (cell1.parse::<f64>(), cell2.parse::<f64>()) {
        (val1 - val2).abs() < 0.1
    } else {
        false
    }
}

