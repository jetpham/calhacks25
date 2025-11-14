use duckdb::Connection;
use anyhow::Result;
use std::time::Instant;

use crate::mv::{create_mv_registry, MaterializedView, create_type_partitioned_mvs};

pub fn create_materialized_views(con: &Connection) -> Result<Vec<MaterializedView>> {
    let total_start = Instant::now();
    let mvs = create_mv_registry();
    
    println!("Creating {} materialized views...", mvs.len());
    
    for mv in &mvs {
        let start = Instant::now();
        println!("Creating materialized view {} (takes ~10-60 seconds)", mv.name);
        
        let sql = mv.generate_create_sql();
        con.execute(&sql, [])?;
        
        println!("🟩 {} created in {:.3}s", mv.name, start.elapsed().as_secs_f64());
    }
    
    println!("Materialized views creation complete: {:.3}s", total_start.elapsed().as_secs_f64());
    
    Ok(mvs)
}

pub fn create_type_partitioned_materialized_views(con: &Connection, base_mvs: &[MaterializedView]) -> Result<Vec<MaterializedView>> {
    let total_start = Instant::now();
    
    // First compute stats on base MVs to determine which ones to partition
    let mut mvs_with_stats = base_mvs.to_vec();
    compute_mv_stats(con, &mut mvs_with_stats)?;
    
    let partitioned_mvs = create_type_partitioned_mvs(&mvs_with_stats);
    
    if partitioned_mvs.is_empty() {
        println!("No MVs eligible for type partitioning");
        return Ok(Vec::new());
    }
    
    println!("Creating {} type-partitioned materialized views...", partitioned_mvs.len());
    
    for mv in &partitioned_mvs {
        let start = Instant::now();
        // Extract type from name (format: mv_name_type_impression)
        let event_type = mv.name.split("_type_").last().unwrap_or("unknown");
        
        println!("Creating type-partitioned MV {} (takes ~5-30 seconds)", mv.name);
        
        // Create SQL that filters by type and groups by remaining columns
        // Note: We don't include 'type' in SELECT since it's constant (filtered in WHERE)
        let mut select_parts = mv.group_by.clone();
        
        for agg in &mv.aggs {
            if agg.column.is_none() {
                select_parts.push("COUNT(*) AS count_rows".to_string());
            } else {
                let col = agg.column.as_ref().unwrap();
                use crate::mv::metric_col_name;
                let metric_name = metric_col_name(&agg.op, Some(col));
                select_parts.push(format!("{}({}) AS {}", agg.op, col, metric_name));
            }
        }
        
        let group_by_positions: Vec<String> = (1..=mv.group_by.len())
            .map(|i| i.to_string())
            .collect();
        
        // Determine sort order (same logic as base MV)
        let mut order_by_cols = Vec::new();
        if mv.group_by.contains(&"day".to_string()) {
            order_by_cols.push("day".to_string());
        }
        if mv.group_by.contains(&"country".to_string()) {
            order_by_cols.push("country".to_string());
        }
        for col in &mv.group_by {
            if !order_by_cols.contains(col) {
                order_by_cols.push(col.clone());
            }
        }
        
        let order_by_clause = if !order_by_cols.is_empty() {
            format!(" ORDER BY {}", order_by_cols.join(", "))
        } else {
            String::new()
        };
        
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} AS\nSELECT\n{}\nFROM events\nWHERE type = '{}'\nGROUP BY {}{};",
            mv.name,
            select_parts.join(",\n"),
            event_type,
            group_by_positions.join(", "),
            order_by_clause
        );
        
        con.execute(&sql, [])?;
        
        println!("🟩 {} created in {:.3}s", mv.name, start.elapsed().as_secs_f64());
    }
    
    println!("Type-partitioned materialized views creation complete: {:.3}s", total_start.elapsed().as_secs_f64());
    
    Ok(partitioned_mvs)
}

pub fn compute_mv_stats(con: &Connection, mvs: &mut [MaterializedView]) -> Result<()> {
    for mv in mvs.iter_mut() {
        let start = Instant::now();
        println!("Computing stats for {} (takes ~10-60 seconds)", mv.name);
        
        // We need to compute stats, but Planner::compute_mv_stats needs mutable access
        // For now, we'll compute stats directly here
        let mut selects = vec!["COUNT(*)".to_string()];
        for col in &mv.group_by {
            selects.push(format!("COUNT(DISTINCT {})", col));
        }
        
        let sql = format!("SELECT {} FROM {}", selects.join(", "), mv.name);
        let mut stmt = con.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        if let Some(row) = rows.next()? {
            mv.num_rows = Some(row.get::<_, i64>(0)?);
            for (i, col) in mv.group_by.iter().enumerate() {
                mv.num_distinct.insert(col.clone(), row.get::<_, i64>(i + 1)?);
            }
        }

        // Compute top-k for each column
        for col in &mv.group_by {
            // Cast ENUM types to VARCHAR for compatibility with Rust bindings
            let sql = format!(
                "SELECT CAST({} AS VARCHAR) as {}, COUNT(*) as cnt FROM {} GROUP BY {} ORDER BY cnt DESC LIMIT 10",
                col, col, mv.name, col
            );
            let mut stmt = con.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            
            let mut topk = std::collections::HashMap::new();
            while let Some(row) = rows.next()? {
                let value: String = row.get(0)?;
                let count: i64 = row.get(1)?;
                topk.insert(value, count);
            }
            mv.col_to_topk.insert(col.clone(), topk);
        }
        
        println!("🟩 {} stats computed in {:.3}s", mv.name, start.elapsed().as_secs_f64());
    }
    
    Ok(())
}

pub fn create_indexes(con: &Connection, mvs: &[MaterializedView]) -> Result<()> {
    println!("Creating indexes on materialized views...");
    
    for mv in mvs {
        let start = Instant::now();
        
        // Create composite indexes based on common query patterns
        // Pattern 1: (type, day) - very common in queries
        if mv.group_by.contains(&"type".to_string()) && mv.group_by.contains(&"day".to_string()) {
            let idx_name = format!("idx_{}_type_day", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(type, day);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 2: Index on day for BETWEEN queries
        if mv.group_by.contains(&"day".to_string()) {
            let idx_name = format!("idx_{}_day", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(day);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 3: Index on type (most common filter)
        if mv.group_by.contains(&"type".to_string()) {
            let idx_name = format!("idx_{}_type", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(type);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 4: Index on country (common filter)
        if mv.group_by.contains(&"country".to_string()) {
            let idx_name = format!("idx_{}_country", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(country);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 5: Composite (type, country) - common combo
        if mv.group_by.contains(&"type".to_string()) && mv.group_by.contains(&"country".to_string()) {
            let idx_name = format!("idx_{}_type_country", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(type, country);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 6: Index on advertiser_id (for Q7, Q8, Q11)
        if mv.group_by.contains(&"advertiser_id".to_string()) {
            let idx_name = format!("idx_{}_advertiser_id", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(advertiser_id);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 7: Index on publisher_id (for Q3, Q15)
        if mv.group_by.contains(&"publisher_id".to_string()) {
            let idx_name = format!("idx_{}_publisher_id", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(publisher_id);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 8: Index on minute (for Q4, Q5 ORDER BY minute)
        if mv.group_by.contains(&"minute".to_string()) {
            let idx_name = format!("idx_{}_minute", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(minute);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        // Pattern 9: Index on week (for Q12)
        if mv.group_by.contains(&"week".to_string()) {
            let idx_name = format!("idx_{}_week", mv.name);
            let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {}(week);", idx_name, mv.name);
            if let Err(e) = con.execute(&sql, []) {
                eprintln!("Warning: Could not create index {}: {}", idx_name, e);
            } else {
                println!("  Created index {}", idx_name);
            }
        }
        
        println!("🟩 Indexes created for {} in {:.3}s", mv.name, start.elapsed().as_secs_f64());
    }
    
    println!("Index creation complete");
    Ok(())
}

pub fn load_all_mvs_from_db(con: &Connection) -> Result<Vec<MaterializedView>> {
    // Query DuckDB to get all MV tables
    let mut stmt = con.prepare(
        "SELECT table_name FROM information_schema.tables 
         WHERE table_schema = 'main' 
         AND table_name LIKE 'mv_%' 
         AND table_type = 'BASE TABLE'
         ORDER BY table_name"
    )?;
    
    let mut rows = stmt.query([])?;
    let mut mv_names = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        mv_names.push(name);
    }
    
    if mv_names.is_empty() {
        return Ok(Vec::new());
    }
    
    println!("Found {} materialized views in database", mv_names.len());
    
    // Reconstruct MV metadata from database schema
    let mut mvs = Vec::new();
    for mv_name in mv_names {
        // Get column info for this MV
        let mut col_stmt = con.prepare(
            &format!(
                "SELECT column_name, data_type 
                 FROM information_schema.columns 
                 WHERE table_schema = 'main' AND table_name = '{}' 
                 ORDER BY ordinal_position",
                mv_name
            )
        )?;
        
        let mut col_rows = col_stmt.query([])?;
        let mut group_by_cols = Vec::new();
        let mut aggs = Vec::new();
        
        while let Some(col_row) = col_rows.next()? {
            let col_name: String = col_row.get(0)?;
            let _data_type: String = col_row.get(1)?;
            
            // Determine if this is a group-by column or aggregate
            // Group-by columns are typically: type, day, country, advertiser_id, etc.
            // Aggregates have names like: sum_bid_price, count_rows, etc.
            if col_name.contains("_") && (col_name.starts_with("sum_") || 
                                          col_name.starts_with("count_") ||
                                          col_name.starts_with("min_") ||
                                          col_name.starts_with("max_")) {
                // This is an aggregate column
                let parts: Vec<&str> = col_name.splitn(2, '_').collect();
                if parts.len() == 2 {
                    let op = parts[0].to_uppercase();
                    let col = if parts[1] == "rows" {
                        None
                    } else {
                        Some(parts[1].to_string())
                    };
                    use crate::mv::Agg;
                    aggs.push(Agg::new(&op, col.as_deref()));
                }
            } else if !col_name.contains("_") || 
                      col_name == "type" || 
                      col_name == "day" || 
                      col_name == "country" || 
                      col_name == "week" ||
                      col_name == "hour" ||
                      col_name == "minute" ||
                      col_name.contains("id") {
                // This is likely a group-by column
                group_by_cols.push(col_name);
            }
        }
        
        // Remove 'type' from group_by if this is a type-partitioned MV
        // Type-partitioned MVs have format: mv_name_type_<type> (e.g., mv_advertiser_id_full_type_impression)
        // Base MVs with type in name have format: mv_type_* (e.g., mv_type_week_day)
        // We distinguish by checking if the pattern matches *_type_<type> where <type> is one of the known types
        let is_type_partitioned = mv_name.contains("_type_") && {
            let parts: Vec<&str> = mv_name.split("_type_").collect();
            if parts.len() == 2 {
                let type_part = parts[1];
                matches!(type_part, "click" | "impression" | "purchase" | "serve")
            } else {
                false
            }
        };
        if is_type_partitioned {
            group_by_cols.retain(|x| x != "type");
        }
        
        use crate::mv::MaterializedView;
        mvs.push(MaterializedView::new(
            &mv_name,
            group_by_cols.iter().map(|s| s.as_str()).collect(),
            aggs.clone(),
        ));
    }
    
    Ok(mvs)
}

pub fn warmup_cache(con: &Connection, mvs: &[MaterializedView]) -> Result<()> {
    println!("Warming up cache...");
    
    for mv in mvs {
        let start = Instant::now();
        println!("Analyzing {} (takes ~10-60 seconds)", mv.name);
        
        con.execute(&format!("ANALYZE {};", mv.name), [])?;
        con.execute(&format!("SELECT COUNT(*) FROM {}", mv.name), [])?;
        
        println!("🟩 {} analyzed in {:.3}s", mv.name, start.elapsed().as_secs_f64());
    }
    
    Ok(())
}



