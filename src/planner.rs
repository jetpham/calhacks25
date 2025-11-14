use anyhow::Result;
use duckdb::Connection;
use serde_json::Value;
use std::collections::HashSet;

use crate::mv::{Agg, MaterializedView, metric_col_name};

pub struct Planner;

impl Planner {
    pub fn new(_con: &Connection) -> Self {
        Self
    }


    fn agg_derivable(&self, agg: &Agg, mv: &MaterializedView) -> bool {
        if agg.op == "AVG" {
            return mv.aggs.contains(&Agg::new("SUM", agg.column.as_deref()))
                && mv.aggs.contains(&Agg::new("COUNT", agg.column.as_deref()));
        }

        if matches!(agg.op.as_str(), "MIN" | "MAX" | "SUM" | "COUNT") {
            return mv.aggs.contains(agg);
        }

        false
    }

    pub fn is_mv_usable(&self, query: &Value, mv: &MaterializedView) -> bool {
        // Check if this is a type-partitioned MV
        let is_type_partitioned = mv.name.contains("_type_") && {
            let parts: Vec<&str> = mv.name.split("_type_").collect();
            if parts.len() == 2 {
                let type_part = parts[1];
                matches!(type_part, "click" | "impression" | "purchase" | "serve")
            } else {
                false
            }
        };
        let query_type = self.extract_type_filter(query);
        
        // For type-partitioned MVs, check if the type matches
        if is_type_partitioned {
            if let Some(qtype) = &query_type {
                // Extract type from MV name (format: mv_name_type_impression)
                if let Some(mv_type) = mv.name.split("_type_").last() {
                    if mv_type != qtype {
                        return false; // Type doesn't match
                    }
                }
            } else {
                return false; // Query doesn't filter by type, can't use partitioned MV
            }
        }
        
        // Check group_by is subset
        let q_group_by: HashSet<String> = query
            .get("group_by")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        // For type-partitioned MVs, 'type' is not in group_by but is filtered
        let mut mv_group_by: HashSet<String> = mv.group_by.iter().cloned().collect();
        if is_type_partitioned {
            // Add 'type' back for WHERE clause checking
            mv_group_by.insert("type".to_string());
        }
        
        if !q_group_by.is_subset(&mv_group_by) {
            return false;
        }

        // Check WHERE columns exist in MV (excluding type for partitioned MVs)
        if let Some(where_arr) = query.get("where").and_then(|v| v.as_array()) {
            for pred in where_arr {
                if let Some(col) = pred.get("col").and_then(|v| v.as_str()) {
                    if col == "type" && is_type_partitioned {
                        // Type is already filtered in partitioned MV, skip
                        continue;
                    }
                    if !mv_group_by.contains(col) {
                        return false;
                    }
                }
            }
        }

        // Check SELECT columns/aggregates are derivable
        if let Some(select_arr) = query.get("select").and_then(|v| v.as_array()) {
            for item in select_arr {
                if let Some(col_str) = item.as_str() {
                    if !mv_group_by.contains(col_str) {
                        return false;
                    }
                } else if let Some(obj) = item.as_object() {
                    for (op, col_val) in obj {
                        let col = if col_val.as_str() == Some("*") {
                            None
                        } else {
                            col_val.as_str()
                        };
                        let agg = Agg::new(op, col);
                        if !self.agg_derivable(&agg, mv) {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    fn predicate_selectivity(&self, pred: &Value, mv: &MaterializedView) -> f64 {
        let col = pred.get("col").and_then(|v| v.as_str()).unwrap_or("");
        let op = pred.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let val = pred.get("val");

        if op == "eq" {
            if let Some(value_str) = val.and_then(|v| v.as_str()) {
                if let Some(topk) = mv.col_to_topk.get(col) {
                    if let Some(&count) = topk.get(value_str) {
                        return count as f64 / mv.num_rows.unwrap_or(1) as f64;
                    }
                }
                // Estimate: 1 / distinct count
                if let Some(&distinct) = mv.num_distinct.get(col) {
                    return 1.0 / distinct as f64;
                }
            }
        } else if op == "in" {
            if let Some(arr) = val.and_then(|v| v.as_array()) {
                let mut cnt = 0;
                if let Some(topk) = mv.col_to_topk.get(col) {
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            if let Some(&c) = topk.get(s) {
                                cnt += c;
                            } else {
                                cnt += 1;
                            }
                        }
                    }
                }
                return cnt as f64 / mv.num_rows.unwrap_or(1).max(1) as f64;
            }
        } else if op == "neq" {
            return 1.0 - self.predicate_selectivity(
                &serde_json::json!({"col": col, "op": "eq", "val": val}),
                mv,
            );
        } else if op == "between" {
            // Improved between selectivity estimation
            if let Some(arr) = val.and_then(|v| v.as_array()) {
                if arr.len() >= 2 {
                    let _low = arr[0].as_str().unwrap_or("");
                    let _high = arr[1].as_str().unwrap_or("");
                    
                    // For date columns, estimate based on date span
                    if col == "day" {
                        // Parse dates and estimate selectivity
                        // For now, use a heuristic: assume uniform distribution
                        // A full year (365 days) would be ~1.0 selectivity
                        // Estimate: if range spans many days, use higher selectivity
                        if let Some(&distinct) = mv.num_distinct.get(col) {
                            // Rough estimate: assume between covers ~1/3 to 2/3 of distinct days
                            // This is a heuristic - proper implementation would parse dates
                            if distinct > 100 {
                                // Large range (like full year)
                                return 0.5; // Estimate 50% selectivity
                            } else {
                                // Smaller range
                                return 0.2; // Estimate 20% selectivity
                            }
                        }
                    } else if col == "hour" || col == "minute" {
                        // For time columns, use similar heuristic
                        if let Some(&distinct) = mv.num_distinct.get(col) {
                            return (distinct as f64 / 2.0).min(0.5) / distinct as f64;
                        }
                    }
                    
                    // Generic: estimate based on distinct count
                    if let Some(&distinct) = mv.num_distinct.get(col) {
                        // Assume between covers roughly 1/3 of distinct values
                        return (distinct as f64 / 3.0).max(1.0) / distinct as f64;
                    }
                }
            }
        }

        0.1 // Default selectivity
    }

    pub fn mv_cost(&self, query: &Value, mv: &MaterializedView) -> f64 {
        // Compute selectivity from WHERE clauses
        let mut selectivity = 1.0;
        if let Some(where_arr) = query.get("where").and_then(|v| v.as_array()) {
            for pred in where_arr {
                selectivity *= self.predicate_selectivity(pred, mv);
            }
        }

        let num_rows_scanned = mv.num_rows.unwrap_or(0) as f64 * selectivity;

        // Compute rollup cost if MV is more granular than query
        let q_group_by: Vec<String> = query
            .get("group_by")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut num_groups = 1.0;
        let mut has_rollup = false;
        if !q_group_by.is_empty() && q_group_by.len() < mv.group_by.len() {
            has_rollup = true;
            for groupby in &mv.group_by {
                if !q_group_by.contains(groupby) {
                    if let Some(&distinct) = mv.num_distinct.get(groupby) {
                        num_groups *= distinct as f64;
                    }
                }
            }
        }

        // Hardware-aware cost function
        use crate::hardware::get_hardware_info;
        let hw = get_hardware_info();
        let (scan_weight, rollup_weight) = hw.cost_weights();
        
        let base_cost = scan_weight * num_rows_scanned + rollup_weight * num_groups;

        // Exact match bonus: prefer MVs with matching group-by
        let q_group_by_set: std::collections::HashSet<&String> = q_group_by.iter().collect();
        let mv_group_by_set: std::collections::HashSet<&String> = mv.group_by.iter().collect();
        
        if q_group_by_set == mv_group_by_set && !has_rollup {
            // Exact match: 20% cost reduction
            return base_cost * 0.8;
        }

        // Small MV bonus: prefer smaller MVs when selectivity is similar
        let mv_size_factor = match mv.num_rows {
            Some(rows) if rows < 10_000 => 0.9,        // Very small MV bonus
            Some(rows) if rows < 100_000 => 0.95,      // Small MV bonus
            Some(rows) if rows < 1_000_000 => 1.0,    // Medium MV
            Some(_) => 1.05,                           // Large MV penalty
            None => 1.0,
        };

        base_cost * mv_size_factor
    }

    pub fn translate_query(&self, query: &Value, mvs: &mut [MaterializedView], verbose: bool) -> Result<String> {
        // Check if query filters by type - if so, prefer type-partitioned MVs
        let query_type = self.extract_type_filter(query);
        
        let mut best_mv: Option<usize> = None;
        let mut best_cost = f64::INFINITY;

        for (i, mv) in mvs.iter().enumerate() {
            if self.is_mv_usable(query, mv) {
                if !mv.has_stats() {
                    // Compute stats on the fly (should be precomputed, but handle it)
                    if verbose {
                        println!("Computing missing stats for {}", mv.name);
                    }
                    // Note: We'd need mutable access, but for now assume stats are precomputed
                }

                let cost = self.mv_cost(query, mv);
                
                // Prefer type-partitioned MVs when query filters by type
                // Type-partitioned MVs have format: mv_name_type_<type> (e.g., mv_advertiser_id_full_type_impression)
                // Base MVs with type in name have format: mv_type_* (e.g., mv_type_week_day)
                let is_type_partitioned_mv = mv.name.contains("_type_") && {
                    let parts: Vec<&str> = mv.name.split("_type_").collect();
                    if parts.len() == 2 {
                        let type_part = parts[1];
                        matches!(type_part, "click" | "impression" | "purchase" | "serve")
                    } else {
                        false
                    }
                };
                
                let adjusted_cost = if let Some(qtype) = &query_type {
                    if is_type_partitioned_mv && mv.name.contains(&format!("_type_{}", qtype)) {
                        // Type-partitioned MV matches query type - significant cost reduction
                        cost * 0.1 // 90% cost reduction for exact type match
                    } else if is_type_partitioned_mv {
                        // Type-partitioned MV but wrong type - very high cost
                        cost * 100.0
                    } else {
                        cost
                    }
                } else {
                    cost
                };
                
                if verbose {
                    println!("Considering {}: cost {} (adjusted: {})", mv.name, cost, adjusted_cost);
                }

                if adjusted_cost < best_cost {
                    best_cost = adjusted_cost;
                    best_mv = Some(i);
                }
            }
        }

        if let Some(idx) = best_mv {
            let mv = &mvs[idx];
            if verbose {
                println!("Picking MV {} for query", mv.name);
            }
            Ok(self.assemble_sql_for_mv(query, mv))
        } else {
            if verbose {
                println!("Warning: could not find a feasible MV for the query. Using events table.");
            }
            Ok(self.assemble_sql_plain(query))
        }
    }
    
    fn extract_type_filter(&self, query: &Value) -> Option<String> {
        if let Some(where_arr) = query.get("where").and_then(|v| v.as_array()) {
            for pred in where_arr {
                if let Some(col) = pred.get("col").and_then(|v| v.as_str()) {
                    if col == "type" {
                        if let Some(op) = pred.get("op").and_then(|v| v.as_str()) {
                            if op == "eq" {
                                return pred.get("val").and_then(|v| v.as_str()).map(|s| s.to_string());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn assemble_sql_for_mv(&self, query: &Value, mv: &MaterializedView) -> String {
        let select_sql = self.select_over_mv(query.get("select"), mv);
        let from_tbl = mv.name.clone();
        
        // For type-partitioned MVs, exclude type filter from WHERE clause
        let is_type_partitioned = mv.name.contains("_type_");
        let where_clause = if is_type_partitioned {
            self.where_to_sql_excluding_type(query.get("where"))
        } else {
            self.where_to_sql(query.get("where"))
        };
        
        let group_by = self.group_by_to_sql(query.get("group_by"));
        let order_by = self.order_by_to_sql(query.get("order_by"));

        let mut sql = format!("SELECT {} FROM {}", select_sql, from_tbl);
        if !where_clause.is_empty() {
            sql.push_str(&format!(" {}", where_clause));
        }
        if !group_by.is_empty() {
            sql.push_str(&format!(" {}", group_by));
        }
        if !order_by.is_empty() {
            sql.push_str(&format!(" {}", order_by));
        }
        if let Some(limit) = query.get("limit").and_then(|v| v.as_i64()) {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        sql
    }
    
    fn where_to_sql_excluding_type(&self, where_clause: Option<&Value>) -> String {
        let Some(conditions) = where_clause.and_then(|w| w.as_array()) else {
            return String::new();
        };

        let parts: Vec<String> = conditions.iter()
            .filter_map(|cond| {
                // Skip type filters for partitioned MVs
                if let Some(col) = cond.get("col").and_then(|v| v.as_str()) {
                    if col == "type" {
                        return None;
                    }
                }
                Some(self.predicate_to_sql(cond))
            })
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", parts.join(" AND "))
        }
    }
    
    fn predicate_to_sql(&self, cond: &Value) -> String {
        let col = cond.get("col").and_then(|v| v.as_str()).unwrap_or("");
        let op = cond.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let val = cond.get("val");

        match op {
            "eq" => {
                if let Some(s) = val.and_then(|v| v.as_str()) {
                    format!("{} = '{}'", col, s)
                } else {
                    format!("{} = {}", col, val.unwrap_or(&serde_json::Value::Null))
                }
            }
            "neq" => {
                if let Some(s) = val.and_then(|v| v.as_str()) {
                    format!("{} != '{}'", col, s)
                } else {
                    format!("{} != {}", col, val.unwrap_or(&serde_json::Value::Null))
                }
            }
            "between" => {
                if let Some(arr) = val.and_then(|v| v.as_array()) {
                    let low = arr[0].as_str().unwrap_or("");
                    let high = arr[1].as_str().unwrap_or("");
                    format!("{} BETWEEN '{}' AND '{}'", col, low, high)
                } else {
                    String::new()
                }
            }
            "in" => {
                if let Some(arr) = val.and_then(|v| v.as_array()) {
                    let vals_str: Vec<String> = arr.iter()
                        .map(|v| format!("'{}'", v.as_str().unwrap_or("")))
                        .collect();
                    format!("{} IN ({})", col, vals_str.join(", "))
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    }

    fn assemble_sql_plain(&self, query: &Value) -> String {
        crate::query_handler::assemble_sql(query)
    }

    fn select_over_mv(&self, select: Option<&Value>, _mv: &MaterializedView) -> String {
        let Some(select_arr) = select.and_then(|v| v.as_array()) else {
            return "*".to_string();
        };

        let mut parts = Vec::new();
        for item in select_arr {
            if let Some(col_str) = item.as_str() {
                // Cast ENUM types to VARCHAR for compatibility with Rust bindings
                let col_expr = if col_str == "type" {
                    format!("CAST({} AS VARCHAR) AS {}", col_str, col_str)
                } else {
                    col_str.to_string()
                };
                parts.push(col_expr);
            } else if let Some(obj) = item.as_object() {
                for (op, col_val) in obj {
                    let col = if col_val.as_str() == Some("*") {
                        None
                    } else {
                        col_val.as_str()
                    };
                    let (expr, alias) = self.compute_agg_alias_expr(op, col);
                    parts.push(format!("{} AS \"{}\"", expr, alias));
                }
            }
        }

        if parts.is_empty() {
            "*".to_string()
        } else {
            parts.join(", ")
        }
    }

    fn compute_agg_alias_expr(&self, op: &str, col: Option<&str>) -> (String, String) {
        let op_upper = op.to_uppercase();
        let op_lower = op.to_lowercase();
        
        // Output format should match baseline: sum(bid_price) (lowercase)
        // Special case: COUNT(*) becomes count_star()
        if op_upper == "AVG" {
            let sum_col = metric_col_name("sum", col);
            let cnt_col = metric_col_name("count", col);
            let col_str = col.unwrap_or("*");
            let alias = format!("{}({})", op_lower, col_str);
            let expr = format!("SUM({})::DOUBLE / NULLIF(SUM({}), 0)", sum_col, cnt_col);
            return (expr, alias);
        }

        if matches!(op_upper.as_str(), "SUM" | "COUNT") {
            let mv_col = metric_col_name(&op_upper, if op_upper == "COUNT" && col == Some("*") { None } else { col });
            let expr = format!("SUM({})", mv_col);
            // For COUNT(*), use special alias format
            let alias = if op_upper == "COUNT" && col == Some("*") {
                "count_star()".to_string()
            } else {
                let col_str = col.unwrap_or("*");
                format!("{}({})", op_lower, col_str)
            };
            return (expr, alias);
        }

        if matches!(op_upper.as_str(), "MIN" | "MAX") {
            let col_str = col.unwrap_or("*");
            let alias = format!("{}({})", op_lower, col_str);
            let mv_col = metric_col_name(&op_upper, col);
            let expr = format!("{}({})", op_upper, mv_col);
            return (expr, alias);
        }

        panic!("Unsupported aggregate: {}({})", op_upper, col.unwrap_or("*"));
    }

    fn where_to_sql(&self, where_clause: Option<&Value>) -> String {
        let Some(conditions) = where_clause.and_then(|w| w.as_array()) else {
            return String::new();
        };

        let parts: Vec<String> = conditions.iter().map(|cond| {
            let col = cond.get("col").and_then(|v| v.as_str()).unwrap_or("");
            let op = cond.get("op").and_then(|v| v.as_str()).unwrap_or("");
            let val = cond.get("val");

            match op {
                "eq" => {
                    if let Some(s) = val.and_then(|v| v.as_str()) {
                        format!("{} = '{}'", col, s)
                    } else {
                        format!("{} = {}", col, val.unwrap_or(&Value::Null))
                    }
                }
                "neq" => {
                    if let Some(s) = val.and_then(|v| v.as_str()) {
                        format!("{} != '{}'", col, s)
                    } else {
                        format!("{} != {}", col, val.unwrap_or(&Value::Null))
                    }
                }
                "between" => {
                    if let Some(arr) = val.and_then(|v| v.as_array()) {
                        let low = arr[0].as_str().unwrap_or("");
                        let high = arr[1].as_str().unwrap_or("");
                        format!("{} BETWEEN '{}' AND '{}'", col, low, high)
                    } else {
                        String::new()
                    }
                }
                "in" => {
                    if let Some(arr) = val.and_then(|v| v.as_array()) {
                        let vals_str: Vec<String> = arr.iter()
                            .map(|v| format!("'{}'", v.as_str().unwrap_or("")))
                            .collect();
                        format!("{} IN ({})", col, vals_str.join(", "))
                    } else {
                        String::new()
                    }
                }
                _ => String::new(),
            }
        }).collect();

        if parts.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", parts.join(" AND "))
        }
    }

    fn group_by_to_sql(&self, group_by: Option<&Value>) -> String {
        if let Some(gb) = group_by {
            if let Some(gb_array) = gb.as_array() {
                if !gb_array.is_empty() {
                    let parts: Vec<String> = gb_array.iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.to_string())
                        .collect();
                    if !parts.is_empty() {
                        return format!("GROUP BY {}", parts.join(", "));
                    }
                }
            }
        }
        String::new()
    }

    fn order_by_to_sql(&self, order_by: Option<&Value>) -> String {
        if let Some(ob) = order_by {
            if let Some(ob_array) = ob.as_array() {
                if !ob_array.is_empty() {
                    let parts: Vec<String> = ob_array.iter().map(|o| {
                        let col = o.get("col").and_then(|v| v.as_str()).unwrap_or("");
                        let dir = o.get("dir").and_then(|d| d.as_str()).unwrap_or("asc").to_uppercase();
                        
                        // Handle aggregate functions in ORDER BY
                        if col.contains('(') && col.contains(')') {
                            // Extract function and column
                            if let Some(start) = col.find('(') {
                                let op = &col[..start];
                                let col_part = &col[start+1..col.len()-1];
                                let (expr, _) = self.compute_agg_alias_expr(op, Some(col_part));
                                format!("{} {}", expr, dir)
                            } else {
                                format!("{} {}", col, dir)
                            }
                        } else {
                            format!("{} {}", col, dir)
                        }
                    }).collect();
                    if !parts.is_empty() {
                        return format!("ORDER BY {}", parts.join(", "));
                    }
                }
            }
        }
        String::new()
    }
}

