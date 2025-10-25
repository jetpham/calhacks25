use std::path::PathBuf;
use std::fs;
use anyhow::Result;
use futures::future;

/// Save query results to CSV files using Arrow's CSV writer
/// Uses concurrent file writing for better I/O performance
pub async fn save_results_to_csv(
    results: Vec<Vec<datafusion::arrow::array::RecordBatch>>,
    output_dir: &PathBuf,
) -> Result<()> {
    use datafusion::arrow::csv::writer::Writer;
    use std::fs::File;
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Create futures for all file writes to execute them concurrently
    let futures = results.iter().enumerate().map(|(query_index, batches)| {
        let output_dir = output_dir.clone();
        let batches = batches.clone();
        async move {
            let filename = format!("q{}.csv", query_index + 1);
            let file_path = output_dir.join(&filename);
            
            // Write to CSV using Arrow's CSV writer
            let file = File::create(&file_path)?;
            let mut writer = Writer::new(file);
            
            for batch in &batches {
                writer.write(batch)?;
            }
            
            println!("Saved results to: {}", file_path.display());
            Ok::<(), anyhow::Error>(())
        }
    });
    
    // Execute all file writes concurrently
    let results = future::join_all(futures).await;
    results.into_iter().collect::<Result<Vec<_>>>()?;
    
    Ok(())
}

/// Check results against reference directory
pub async fn check_results(
    output_dir: &PathBuf,
    check_dir: &PathBuf,
) -> Result<()> {
    use std::collections::HashSet;
    
    println!("Checking results against reference directory: {}", check_dir.display());
    
    // Get all CSV files in both directories
    let output_files: HashSet<String> = fs::read_dir(output_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "csv" {
                Some(path.file_name()?.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();
    
    let check_files: HashSet<String> = fs::read_dir(check_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "csv" {
                Some(path.file_name()?.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();
    
    // Check if file sets match
    if output_files != check_files {
        println!("ERROR: File mismatch!");
        println!("Output files: {:?}", output_files);
        println!("Reference files: {:?}", check_files);
        return Ok(());
    }
    
    // Compare files concurrently for better I/O performance
    let output_dir = output_dir.clone();
    let check_dir = check_dir.clone();
    let comparison_futures = output_files.iter().map(|filename| {
        let output_dir = output_dir.clone();
        let check_dir = check_dir.clone();
        let filename = filename.clone();
        async move {
            let output_path = output_dir.join(&filename);
            let check_path = check_dir.join(&filename);
            
            if !check_path.exists() {
                println!("ERROR: Reference file not found: {}", check_path.display());
                return Ok(false);
            }
            
            let output_content = fs::read_to_string(&output_path)?;
            let check_content = fs::read_to_string(&check_path)?;
            
            if output_content.trim() == check_content.trim() {
                println!("PASS: {} - CORRECT", filename);
                Ok(true)
            } else {
                println!("FAIL: {} - INCORRECT", filename);
                Ok(false)
            }
        }
    });
    
    // Execute all file comparisons concurrently
    let comparison_results = future::join_all(comparison_futures).await;
    let comparison_results = comparison_results.into_iter().collect::<Result<Vec<_>>>()?;
    let all_correct = comparison_results.iter().all(|&correct| correct);
    
    if all_correct {
        println!("SUCCESS: All results are CORRECT!");
    } else {
        println!("FAILURE: Some results are INCORRECT!");
    }
    
    Ok(())
}
