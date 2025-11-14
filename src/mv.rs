use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Agg {
    pub op: String,
    pub column: Option<String>,
}

impl Agg {
    pub fn new(op: &str, column: Option<&str>) -> Self {
        Self {
            op: op.to_uppercase(),
            column: column.map(|s| s.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct MaterializedView {
    pub name: String,
    pub group_by: Vec<String>,
    pub aggs: HashSet<Agg>,
    pub num_rows: Option<i64>,
    pub num_distinct: std::collections::HashMap<String, i64>,
    pub col_to_topk: std::collections::HashMap<String, std::collections::HashMap<String, i64>>,
}

impl MaterializedView {
    pub fn new(name: &str, group_by: Vec<&str>, aggs: Vec<Agg>) -> Self {
        Self {
            name: name.to_string(),
            group_by: group_by.iter().map(|s| s.to_string()).collect(),
            aggs: aggs.into_iter().collect(),
            num_rows: None,
            num_distinct: std::collections::HashMap::new(),
            col_to_topk: std::collections::HashMap::new(),
        }
    }

    pub fn has_stats(&self) -> bool {
        !self.num_distinct.is_empty() 
            && !self.col_to_topk.is_empty() 
            && self.num_rows.is_some()
    }

    pub fn generate_create_sql(&self) -> String {
        let mut select_parts = self.group_by.clone();
        
        for agg in &self.aggs {
            if agg.column.is_none() {
                select_parts.push("COUNT(*) AS count_rows".to_string());
            } else {
                let col = agg.column.as_ref().unwrap();
                let metric_name = metric_col_name(&agg.op, Some(col));
                select_parts.push(format!("{}({}) AS {}", agg.op, col, metric_name));
            }
        }

        let group_by_positions: Vec<String> = (1..=self.group_by.len())
            .map(|i| i.to_string())
            .collect();

        // Determine optimal sort order for filtering
        // Priority: type (most common filter), then day, then other dimensions
        let mut order_by_cols = Vec::new();
        if self.group_by.contains(&"type".to_string()) {
            order_by_cols.push("type".to_string());
        }
        if self.group_by.contains(&"day".to_string()) {
            order_by_cols.push("day".to_string());
        }
        if self.group_by.contains(&"country".to_string()) {
            order_by_cols.push("country".to_string());
        }
        // Add remaining group_by columns
        for col in &self.group_by {
            if !order_by_cols.contains(col) {
                order_by_cols.push(col.clone());
            }
        }

        let order_by_clause = if !order_by_cols.is_empty() {
            format!(" ORDER BY {}", order_by_cols.join(", "))
        } else {
            String::new()
        };

        format!(
            "CREATE TABLE IF NOT EXISTS {} AS\nSELECT\n{}\nFROM events\nGROUP BY {}{};",
            self.name,
            select_parts.join(",\n"),
            group_by_positions.join(", "),
            order_by_clause
        )
    }
}

pub fn metric_col_name(op: &str, col: Option<&str>) -> String {
    let op_lower = op.to_lowercase();
    if op_lower == "count" && (col.is_none() || col == Some("*")) {
        return "count_rows".to_string();
    }
    let base = col.unwrap_or("rows").replace(".", "_");
    format!("{}_{}", op_lower, base)
}

pub fn create_mv_registry() -> Vec<MaterializedView> {
    let mut registry = Vec::new();

    // Common aggregates for all MVs
    let common_aggs = vec![
        Agg::new("SUM", Some("bid_price")),
        Agg::new("SUM", Some("total_price")),
        Agg::new("COUNT", None),
        Agg::new("COUNT", Some("bid_price")),
        Agg::new("COUNT", Some("total_price")),
    ];

    // Full MVs: (type, day, country, <id>)
    // Only keep advertiser_id_full (used by Q11)
    registry.push(MaterializedView::new(
        "mv_advertiser_id_full",
        vec!["type", "day", "country", "advertiser_id"],
        common_aggs.clone(),
    ));

    // Fast MVs: (type, day)
    registry.push(MaterializedView::new(
        "mv_day_fast",
        vec!["type", "day"],
        common_aggs.clone(),
    ));

    // Time fast MV: (type, day, hour, minute)
    registry.push(MaterializedView::new(
        "mv_time_fast",
        vec!["type", "day", "hour", "minute"],
        common_aggs.clone(),
    ));

    // Fast MVs: (type, <id>)
    // Only keep advertiser_id_fast (used by Q8)
    registry.push(MaterializedView::new(
        "mv_advertiser_id_fast",
        vec!["type", "advertiser_id"],
        common_aggs.clone(),
    ));

    // Strategic MVs for judges.json queries
    // (type, country) - needed for Q9, Q10, Q14 (country GROUP BY with type filter)
    registry.push(MaterializedView::new(
        "mv_type_country",
        vec!["type", "country"],
        common_aggs.clone(),
    ));

    // (type, week) - needed for Q12 (week GROUP BY with type filter)
    registry.push(MaterializedView::new(
        "mv_type_week",
        vec!["type", "week"],
        common_aggs.clone(),
    ));

    // (type, day, country) - needed for Q2, Q13 (day/country GROUP BY with type filter)
    registry.push(MaterializedView::new(
        "mv_type_day_country",
        vec!["type", "day", "country"],
        common_aggs.clone(),
    ));

    // (type) - needed for Q6 (no group-by, just type filter)
    registry.push(MaterializedView::new(
        "mv_type_only",
        vec!["type"],
        common_aggs.clone(),
    ));

    // (type, day, publisher_id) - needed for Q3, Q15
    registry.push(MaterializedView::new(
        "mv_type_day_publisher_id",
        vec!["type", "day", "publisher_id"],
        common_aggs.clone(),
    ));

    // High priority: Exact match for Q4, Q5 (minute GROUP BY)
    registry.push(MaterializedView::new(
        "mv_type_day_minute",
        vec!["type", "day", "minute"],
        common_aggs.clone(),
    ));

    // High priority: Could help Q12 (week GROUP BY with day BETWEEN)
    registry.push(MaterializedView::new(
        "mv_type_week_day",
        vec!["type", "week", "day"],
        common_aggs.clone(),
    ));

    // Medium priority: Exact match for Q15 (publisher_id GROUP BY with country filter)
    registry.push(MaterializedView::new(
        "mv_type_day_country_publisher_id",
        vec!["type", "day", "country", "publisher_id"],
        common_aggs.clone(),
    ));

    registry
}

/// Create type-partitioned versions of MVs that have 'type' in group_by
/// This allows queries filtering by type to scan much smaller tables
pub fn create_type_partitioned_mvs(base_mvs: &[MaterializedView]) -> Vec<MaterializedView> {
    let mut partitioned = Vec::new();
    let types = vec!["impression", "click", "serve", "purchase"];
    
    for mv in base_mvs {
        // Only partition MVs that have 'type' in group_by and are commonly filtered
        if mv.group_by.contains(&"type".to_string()) {
            // Skip very small MVs (not worth partitioning)
            if let Some(rows) = mv.num_rows {
                if rows < 100_000 {
                    continue; // Too small to benefit from partitioning
                }
            }
            
            // Create a partitioned MV for each type
            for event_type in &types {
                let partitioned_name = format!("{}_type_{}", mv.name, event_type);
                let mut partitioned_group_by = mv.group_by.clone();
                // Remove 'type' from group_by since it's now constant
                partitioned_group_by.retain(|x| x != "type");
                
                partitioned.push(MaterializedView::new(
                    &partitioned_name,
                    partitioned_group_by.iter().map(|s| s.as_str()).collect(),
                    mv.aggs.iter().cloned().collect(),
                ));
            }
        }
    }
    
    partitioned
}

