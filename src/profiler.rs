use anyhow::Result;
use duckdb::Connection;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::Instant;
use std::fs;

/// Profiling configuration for different phases
#[derive(Debug, Clone)]
pub struct ProfilingConfig {
    pub mode: ProfilingMode,
    pub output_dir: PathBuf,
    pub enable_detailed: bool,
    pub enable_optimizer_metrics: bool,
    pub enable_planner_metrics: bool,
    pub enable_physical_planner_metrics: bool,
}

#[derive(Debug, Clone)]
pub enum ProfilingMode {
    Json,
    #[allow(dead_code)]
    QueryGraph,
    Both,
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            mode: ProfilingMode::Json,
            output_dir: PathBuf::from("profiling"),
            enable_detailed: true,
            enable_optimizer_metrics: true,
            enable_planner_metrics: true,
            enable_physical_planner_metrics: true,
        }
    }
}

/// Profiling results for analysis
#[derive(Debug)]
pub struct ProfilingResults {
    pub phase: String,
    pub total_time: f64,
    pub cpu_time: f64,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub memory_usage: u64,
    #[allow(dead_code)]
    pub temp_dir_size: u64,
    pub optimizer_timing: Option<f64>,
    pub planner_timing: Option<f64>,
    pub physical_planner_timing: Option<f64>,
    pub operator_breakdown: Vec<OperatorProfile>,
}

#[derive(Debug)]
pub struct OperatorProfile {
    pub operator_type: String,
    pub timing: f64,
    pub rows_scanned: u64,
    pub cardinality: u64,
    pub memory_usage: u64,
}

/// Initialize profiling for a connection
pub fn setup_profiling(con: &Connection, config: &ProfilingConfig) -> Result<()> {
    // Create profiling output directory
    fs::create_dir_all(&config.output_dir)?;
    
    // Enable profiling
    con.execute("SET enable_profiling = 'json'", [])?;
    
    // Set profiling output file
    let profile_file = config.output_dir.join("query_profile.json");
    con.execute(&format!("SET profiling_output = '{}'", profile_file.display()), [])?;
    
    // Configure detailed profiling if enabled
    if config.enable_detailed {
        con.execute("SET profiling_mode = 'detailed'", [])?;
        
        // Enable specific detailed metrics
        let mut custom_settings = json!({
            "CPU_TIME": "true",
            "EXTRA_INFO": "true",
            "OPERATOR_CARDINALITY": "true",
            "OPERATOR_TIMING": "true",
            "OPERATOR_ROWS_SCANNED": "true",
            "CUMULATIVE_CARDINALITY": "true",
            "CUMULATIVE_ROWS_SCANNED": "true",
            "BLOCKED_THREAD_TIME": "true",
            "SYSTEM_PEAK_BUFFER_MEMORY": "true",
            "SYSTEM_PEAK_TEMP_DIR_SIZE": "true"
        });
        
        if config.enable_optimizer_metrics {
            custom_settings["ALL_OPTIMIZERS"] = json!("true");
            custom_settings["CUMULATIVE_OPTIMIZER_TIMING"] = json!("true");
        }
        
        if config.enable_planner_metrics {
            custom_settings["PLANNER"] = json!("true");
            custom_settings["PLANNER_BINDING"] = json!("true");
        }
        
        if config.enable_physical_planner_metrics {
            custom_settings["PHYSICAL_PLANNER"] = json!("true");
            custom_settings["PHYSICAL_PLANNER_COLUMN_BINDING"] = json!("true");
            custom_settings["PHYSICAL_PLANNER_RESOLVE_TYPES"] = json!("true");
            custom_settings["PHYSICAL_PLANNER_CREATE_PLAN"] = json!("true");
        }
        
        let settings_str = serde_json::to_string(&custom_settings)?;
        con.execute(&format!("SET custom_profiling_settings = '{}'", settings_str), [])?;
    }
    
    Ok(())
}

/// Execute a query with profiling and return detailed results
pub fn execute_with_profiling(
    con: &Connection, 
    sql: &str, 
    query_name: &str,
    config: &ProfilingConfig
) -> Result<ProfilingResults> {
    let start = Instant::now();
    
    // Create unique profile file for this query
    let profile_file = config.output_dir.join(format!("query_profile_{}.json", 
        query_name.replace(" ", "_").replace("Query", "Q")));
    
    // Set profiling output to this specific file
    con.execute(&format!("SET profiling_output = '{}'", profile_file.display()), [])?;
    
    // Execute the query
    let mut stmt = con.prepare(sql)?;
    let mut rows = stmt.query([])?;
    
    // Consume all rows to ensure complete execution
    let mut row_count = 0;
    while let Some(_) = rows.next()? {
        row_count += 1;
    }
    
    let total_time = start.elapsed().as_secs_f64();
    
    // Parse profiling results from the specific file
    let profiling_data = if profile_file.exists() {
        let content = fs::read_to_string(&profile_file)?;
        serde_json::from_str::<Value>(&content)?
    } else {
        return Err(anyhow::anyhow!("Profiling file not found: {:?}", profile_file));
    };
    
    // Extract profiling metrics
    let cpu_time = profiling_data["cpu_time"].as_f64().unwrap_or(0.0);
    let rows_scanned = profiling_data["cumulative_rows_scanned"].as_u64().unwrap_or(0);
    let rows_returned = profiling_data["rows_returned"].as_u64().unwrap_or(row_count as u64);
    let memory_usage = profiling_data["system_peak_buffer_memory"].as_u64().unwrap_or(0);
    let temp_dir_size = profiling_data["system_peak_temp_dir_size"].as_u64().unwrap_or(0);
    
    let optimizer_timing = profiling_data["cumulative_optimizer_timing"].as_f64();
    let planner_timing = profiling_data["planner"].as_f64();
    let physical_planner_timing = profiling_data["physical_planner"].as_f64();
    
    // Parse operator breakdown
    let operator_breakdown = parse_operator_breakdown(&profiling_data)?;
    
    Ok(ProfilingResults {
        phase: query_name.to_string(),
        total_time,
        cpu_time,
        rows_scanned,
        rows_returned,
        memory_usage,
        temp_dir_size,
        optimizer_timing,
        planner_timing,
        physical_planner_timing,
        operator_breakdown,
    })
}

/// Parse operator breakdown from profiling data
fn parse_operator_breakdown(data: &Value) -> Result<Vec<OperatorProfile>> {
    let mut operators = Vec::new();
    
    if let Some(children) = data["children"].as_array() {
        for child in children {
            if let Some(op) = parse_operator(child) {
                operators.push(op);
            }
        }
    }
    
    Ok(operators)
}

/// Parse a single operator from profiling data
fn parse_operator(data: &Value) -> Option<OperatorProfile> {
    let operator_type = data["operator_type"].as_str()?.to_string();
    let timing = data["operator_timing"].as_f64().unwrap_or(0.0);
    let rows_scanned = data["operator_rows_scanned"].as_u64().unwrap_or(0);
    let cardinality = data["operator_cardinality"].as_u64().unwrap_or(0);
    let memory_usage = data["result_set_size"].as_u64().unwrap_or(0);
    
    Some(OperatorProfile {
        operator_type,
        timing,
        rows_scanned,
        cardinality,
        memory_usage,
    })
}

/// Generate profiling report
pub fn generate_profiling_report(results: &[ProfilingResults], output_dir: &PathBuf) -> Result<()> {
    let report_file = output_dir.join("profiling_report.md");
    let mut report = String::new();
    
    report.push_str("# DuckDB Profiling Report\n\n");
    report.push_str(&format!("Generated at: {}\n\n", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
    
    // Summary statistics
    let total_time: f64 = results.iter().map(|r| r.total_time).sum();
    let total_cpu_time: f64 = results.iter().map(|r| r.cpu_time).sum();
    let total_rows_scanned: u64 = results.iter().map(|r| r.rows_scanned).sum();
    let total_rows_returned: u64 = results.iter().map(|r| r.rows_returned).sum();
    let peak_memory: u64 = results.iter().map(|r| r.memory_usage).max().unwrap_or(0);
    
    report.push_str("## Summary Statistics\n\n");
    report.push_str(&format!("- **Total Execution Time**: {:.3}s\n", total_time));
    report.push_str(&format!("- **Total CPU Time**: {:.3}s\n", total_cpu_time));
    report.push_str(&format!("- **Total Rows Scanned**: {}\n", total_rows_scanned));
    report.push_str(&format!("- **Total Rows Returned**: {}\n", total_rows_returned));
    report.push_str(&format!("- **Peak Memory Usage**: {:.2} GB\n", peak_memory as f64 / 1_000_000_000.0));
    report.push_str(&format!("- **CPU Efficiency**: {:.1}%\n\n", (total_cpu_time / total_time) * 100.0));
    
    // Individual query analysis
    report.push_str("## Query Analysis\n\n");
    for (i, result) in results.iter().enumerate() {
        report.push_str(&format!("### Query {}: {}\n\n", i + 1, result.phase));
        report.push_str(&format!("- **Execution Time**: {:.3}s\n", result.total_time));
        report.push_str(&format!("- **CPU Time**: {:.3}s\n", result.cpu_time));
        report.push_str(&format!("- **Rows Scanned**: {}\n", result.rows_scanned));
        report.push_str(&format!("- **Rows Returned**: {}\n", result.rows_returned));
        report.push_str(&format!("- **Memory Usage**: {:.2} GB\n", result.memory_usage as f64 / 1_000_000_000.0));
        
        if let Some(opt_time) = result.optimizer_timing {
            report.push_str(&format!("- **Optimizer Time**: {:.3}s\n", opt_time));
        }
        if let Some(plan_time) = result.planner_timing {
            report.push_str(&format!("- **Planner Time**: {:.3}s\n", plan_time));
        }
        if let Some(phys_time) = result.physical_planner_timing {
            report.push_str(&format!("- **Physical Planner Time**: {:.3}s\n", phys_time));
        }
        
        // Operator breakdown
        if !result.operator_breakdown.is_empty() {
            report.push_str("\n#### Operator Breakdown\n\n");
            report.push_str("| Operator | Time (s) | Rows Scanned | Cardinality | Memory (bytes) |\n");
            report.push_str("|----------|----------|--------------|-------------|----------------|\n");
            
            for op in &result.operator_breakdown {
                report.push_str(&format!(
                    "| {} | {:.3} | {} | {} | {} |\n",
                    op.operator_type, op.timing, op.rows_scanned, op.cardinality, op.memory_usage
                ));
            }
        }
        
        report.push_str("\n");
    }
    
    // Add section about individual query profiles
    report.push_str("## Individual Query Profiles\n\n");
    report.push_str("Detailed profiling data for each query is available in separate JSON files:\n\n");
    for (i, result) in results.iter().enumerate() {
        let profile_filename = format!("query_profile_{}.json", 
            result.phase.replace(" ", "_").replace("Query", "Q"));
        report.push_str(&format!("- **Query {}**: `{}`\n", i + 1, profile_filename));
    }
    report.push_str("\nThese files contain detailed operator-level profiling data including:\n");
    report.push_str("- Complete query execution plans\n");
    report.push_str("- Individual operator timings and cardinalities\n");
    report.push_str("- Memory usage per operator\n");
    report.push_str("- Optimizer, planner, and physical planner metrics\n\n");
    
    // Optimization recommendations
    report.push_str("## Optimization Recommendations\n\n");
    generate_optimization_recommendations(&mut report, results);
    
    // Write report
    fs::write(&report_file, report)?;
    println!("Profiling report written to: {:?}", report_file);
    
    Ok(())
}

/// Generate optimization recommendations based on profiling data
fn generate_optimization_recommendations(report: &mut String, results: &[ProfilingResults]) {
    let mut recommendations = Vec::new();
    
    // Check for high CPU time vs execution time ratio
    for result in results {
        let cpu_efficiency = result.cpu_time / result.total_time;
        if cpu_efficiency < 0.7 {
            recommendations.push(format!(
                "Query '{}' has low CPU efficiency ({:.1}%). Consider optimizing I/O operations or reducing data movement.",
                result.phase, cpu_efficiency * 100.0
            ));
        }
    }
    
    // Check for high memory usage
    let peak_memory = results.iter().map(|r| r.memory_usage).max().unwrap_or(0);
    if peak_memory > 1_000_000_000 { // > 1GB
        recommendations.push(format!(
            "High memory usage detected ({:.2} GB). Consider using streaming operations or reducing working set size.",
            peak_memory as f64 / 1_000_000_000.0
        ));
    }
    
    // Check for sequential scans
    for result in results {
        for op in &result.operator_breakdown {
            if op.operator_type == "TABLE_SCAN" && op.rows_scanned > 1_000_000 {
                recommendations.push(format!(
                    "Query '{}' performs large sequential scan ({} rows). Consider adding appropriate indexes.",
                    result.phase, op.rows_scanned
                ));
            }
        }
    }
    
    // Check for expensive operations
    for result in results {
        for op in &result.operator_breakdown {
            if op.timing > result.total_time * 0.5 {
                recommendations.push(format!(
                    "Query '{}' spends {:.1}% of time in {} operation. Consider optimizing this operation.",
                    result.phase, (op.timing / result.total_time) * 100.0, op.operator_type
                ));
            }
        }
    }
    
    if recommendations.is_empty() {
        report.push_str("No specific optimization recommendations at this time. The queries appear to be well-optimized.\n");
    } else {
        for (i, rec) in recommendations.iter().enumerate() {
            report.push_str(&format!("{}. {}\n", i + 1, rec));
        }
    }
}

/// Generate query graph visualization
pub fn generate_query_graph(profile_file: &PathBuf, output_dir: &PathBuf) -> Result<()> {
    let graph_file = output_dir.join("query_graph.html");
    
    // This would typically call the DuckDB Python module to generate the graph
    // For now, we'll create a placeholder that shows the command to run
    let command = format!(
        "python -m duckdb.query_graph {}",
        profile_file.display()
    );
    
    let html_content = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>DuckDB Query Graph</title>
</head>
<body>
    <h1>DuckDB Query Graph</h1>
    <p>To generate the query graph, run the following command:</p>
    <pre>{}</pre>
    <p>This will generate an interactive HTML visualization of the query execution plan.</p>
</body>
</html>"#,
        command
    );
    
    fs::write(&graph_file, html_content)?;
    println!("Query graph placeholder written to: {:?}", graph_file);
    println!("Run '{}' to generate the actual query graph", command);
    
    Ok(())
}
