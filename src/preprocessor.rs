use duckdb::Connection;
use anyhow::Result;
use std::time::Instant;

use crate::mv::{create_mv_registry, MaterializedView};

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



