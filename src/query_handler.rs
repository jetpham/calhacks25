use anyhow::Result;
use serde_json::Value;
use std::path::PathBuf;
use std::fs;

/// Parse queries from JSON file
pub fn parse_queries_from_file(queries_path: &PathBuf) -> Result<Vec<Value>> {
    let content = fs::read_to_string(queries_path)?;
    let queries: Vec<Value> = serde_json::from_str(&content)?;
    Ok(queries)
}

/// Convert JSON query to SQL string (matching baseline behavior)
pub fn assemble_sql(q: &Value) -> String {
    let select = select_to_sql(q.get("select").unwrap_or(&Value::Array(vec![])));
    // Use events_table (materialized table) instead of events (view) for better performance
    let from_tbl = q["from"].as_str().unwrap_or("events_table");
    let where_clause = where_to_sql(q.get("where"));
    let group_by = group_by_to_sql(q.get("group_by"));
    let order_by = order_by_to_sql(q.get("order_by"));
    
    let mut sql = format!("SELECT {} FROM {}", select, from_tbl);
    if !where_clause.is_empty() {
        sql.push_str(&format!(" {}", where_clause));
    }
    if !group_by.is_empty() {
        sql.push_str(&format!(" {}", group_by));
    }
    if !order_by.is_empty() {
        sql.push_str(&format!(" {}", order_by));
    }
    if let Some(limit) = q.get("limit") {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    sql
}

fn where_to_sql(where_clause: Option<&Value>) -> String {
    if where_clause.is_none() {
        return String::new();
    }
    
    let Some(conditions) = where_clause.and_then(|w| w.as_array()) else {
        return String::new();
    };
    
    let parts: Vec<String> = conditions.iter().map(|cond| {
        let col = cond["col"].as_str().unwrap_or("");
        let op = cond["op"].as_str().unwrap_or("");
        let val = &cond["val"];
        
        if op == "eq" {
            format!("{} = '{}'", col, val.as_str().unwrap_or(""))
        } else if op == "neq" {
            format!("{} != '{}'", col, val.as_str().unwrap_or(""))
        } else if op == "lt" {
            format!("{} < {}", col, format_value_for_sql(val))
        } else if op == "lte" {
            format!("{} <= {}", col, format_value_for_sql(val))
        } else if op == "gt" {
            format!("{} > {}", col, format_value_for_sql(val))
        } else if op == "gte" {
            format!("{} >= {}", col, format_value_for_sql(val))
        } else if op == "between" {
            if let Some(vals) = val.as_array() {
                let low = vals[0].as_str().unwrap_or("");
                let high = vals[1].as_str().unwrap_or("");
                format!("{} BETWEEN '{}' AND '{}'", col, low, high)
            } else {
                String::new()
            }
        } else if op == "in" {
            if let Some(vals) = val.as_array() {
                let vals_str = vals.iter()
                    .map(|v| format!("'{}'", v.as_str().unwrap_or("")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} IN ({})", col, vals_str)
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    }).collect();
    
    if parts.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", parts.join(" AND "))
    }
}

fn format_value_for_sql(val: &serde_json::Value) -> String {
    // For lt/lte/gt/gte comparisons, don't add quotes around numeric values
    if let Some(num) = val.as_f64() {
        if num.fract() == 0.0 {
            format!("{}", num as i64)
        } else {
            format!("{}", num)
        }
    } else if let Some(str_val) = val.as_str() {
        // Try to parse as number
        if let Ok(num) = str_val.parse::<f64>() {
            if num.fract() == 0.0 {
                format!("{}", num as i64)
            } else {
                format!("{}", num)
            }
        } else {
            // Not a number, use quotes
            format!("'{}'", str_val)
        }
    } else if let Some(num) = val.as_u64() {
        format!("{}", num)
    } else if let Some(num) = val.as_i64() {
        format!("{}", num)
    } else {
        // Fallback - try to convert to string
        val.to_string()
    }
}

fn select_to_sql(select: &Value) -> String {
    let Some(select_array) = select.as_array() else {
        return "*".to_string();
    };
    
    let parts: Vec<String> = select_array.iter().map(|item| {
        if let Some(s) = item.as_str() {
            s.to_string()
        } else if let Some(obj) = item.as_object() {
            let mut result = String::new();
            for (func, col) in obj {
                result = format!("{}({})", func.to_uppercase(), col.as_str().unwrap_or(""));
            }
            result
        } else {
            String::new()
        }
    }).collect();
    
    if parts.is_empty() {
        "*".to_string()
    } else {
        parts.join(", ")
    }
}

fn group_by_to_sql(group_by: Option<&Value>) -> String {
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

fn order_by_to_sql(order_by: Option<&Value>) -> String {
    if let Some(ob) = order_by {
        if let Some(ob_array) = ob.as_array() {
            if !ob_array.is_empty() {
                let parts: Vec<String> = ob_array.iter().map(|o| {
                    let col = o["col"].as_str().unwrap_or("");
                    let dir = o.get("dir").and_then(|d| d.as_str()).unwrap_or("asc").to_uppercase();
                    format!("{} {}", col, dir)
                }).collect();
                if !parts.is_empty() {
                    return format!("ORDER BY {}", parts.join(", "));
                }
            }
        }
    }
    String::new()
}

