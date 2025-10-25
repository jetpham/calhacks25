use serde_json::Value;

/// Convert JSON query to SQL string
pub fn assemble_sql(q: &Value) -> String {
    let select = select_to_sql(q.get("select").unwrap_or(&Value::Array(vec![])));
    let from_tbl = q["from"].as_str().unwrap_or("events");
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
    if let Some(where_conditions) = where_clause {
        if let Some(conditions) = where_conditions.as_array() {
            let parts: Vec<String> = conditions.iter().map(|cond| {
                let col = cond["col"].as_str().unwrap_or("");
                let op = cond["op"].as_str().unwrap_or("");
                let val = &cond["val"];
                
                match op {
                    "eq" => format!("{} = '{}'", col, val.as_str().unwrap_or("")),
                    "neq" => format!("{} != '{}'", col, val.as_str().unwrap_or("")),
                    "lt" => format!("{} < {}", col, val),
                    "lte" => format!("{} <= {}", col, val),
                    "gt" => format!("{} > {}", col, val),
                    "gte" => format!("{} >= {}", col, val),
                    "between" => {
                        if let Some(vals) = val.as_array() {
                            let low = vals[0].as_str().unwrap_or("");
                            let high = vals[1].as_str().unwrap_or("");
                            format!("{} BETWEEN '{}' AND '{}'", col, low, high)
                        } else {
                            String::new()
                        }
                    },
                    "in" => {
                        if let Some(vals) = val.as_array() {
                            let vals_str = vals.iter()
                                .map(|v| format!("'{}'", v.as_str().unwrap_or("")))
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("{} IN ({})", col, vals_str)
                        } else {
                            String::new()
                        }
                    },
                    _ => String::new(),
                }
            }).collect();
            
            if !parts.is_empty() {
                return format!("WHERE {}", parts.join(" AND "));
            }
        }
    }
    String::new()
}

fn select_to_sql(select: &Value) -> String {
    if let Some(select_array) = select.as_array() {
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
        parts.join(", ")
    } else {
        "*".to_string()
    }
}

fn group_by_to_sql(group_by: Option<&Value>) -> String {
    if let Some(gb) = group_by {
        if let Some(gb_array) = gb.as_array() {
            let parts: Vec<String> = gb_array.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();
            if !parts.is_empty() {
                return format!("GROUP BY {}", parts.join(", "));
            }
        }
    }
    String::new()
}

fn order_by_to_sql(order_by: Option<&Value>) -> String {
    if let Some(ob) = order_by {
        if let Some(ob_array) = ob.as_array() {
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
    String::new()
}
