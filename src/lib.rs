pub mod parser;
pub mod storage;

pub use parser::{parse_sql, SqlStatement, Value};
pub use storage::Storage;

/// Execute a SQL string against the storage engine. Returns Ok with a description
/// of what happened, or Err with an error message. Never panics.
pub fn execute(storage: &Storage, sql: &str) -> Result<String, String> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err("empty input".to_string());
    }

    let stmt = match parse_sql(trimmed) {
        Ok((_, stmt)) => stmt,
        Err(e) => return Err(format!("Parse error: {:?}", e)),
    };

    match stmt {
        SqlStatement::CreateTable(create_stmt) => {
            let name = create_stmt.table_name.clone();
            storage.create_table(&create_stmt)
                .map(|_| format!("Created table '{}'", name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::Insert(insert_stmt) => {
            storage.insert_row(&insert_stmt)
                .map(|_| "Inserted 1 row".to_string())
                .map_err(|e| e.to_string())
        }
        SqlStatement::Select(select_stmt) => {
            execute_select_to_string(&select_stmt, storage)
        }
        SqlStatement::Update(update_stmt) => {
            storage.update_rows(&update_stmt)
                .map(|n| format!("Updated {} row(s)", n))
                .map_err(|e| e.to_string())
        }
        SqlStatement::Delete(delete_stmt) => {
            storage.delete_rows(&delete_stmt)
                .map(|n| format!("Deleted {} row(s)", n))
                .map_err(|e| e.to_string())
        }
    }
}

// Minimal select executor that loads data and applies WHERE, returning results as a string
fn execute_select_to_string(
    stmt: &parser::SelectStatement,
    storage: &Storage,
) -> Result<String, String> {
    let table_name = stmt.from.table_name().ok_or("Subquery FROM not supported here")?;
    let from_schema = storage.load_schema(table_name).map_err(|e| e.to_string())?;
    let from_rows = storage.read_rows(table_name).map_err(|e| e.to_string())?;

    let from_alias = stmt.from_alias.as_deref().unwrap_or(table_name);
    let mut combined_cols: Vec<(String, String)> = from_schema.columns.iter()
        .map(|c| (from_alias.to_string(), c.name.clone()))
        .collect();
    let mut combined_rows: Vec<Vec<Value>> = from_rows;

    // process joins
    for join in &stmt.joins {
        let join_schema = storage.load_schema(&join.table).map_err(|e| e.to_string())?;
        let join_rows = storage.read_rows(&join.table).map_err(|e| e.to_string())?;
        let join_alias = join.alias.as_deref().unwrap_or(&join.table);
        let join_cols: Vec<(String, String)> = join_schema.columns.iter()
            .map(|c| (join_alias.to_string(), c.name.clone()))
            .collect();

        let mut new_rows = Vec::new();
        let left_col_count = combined_cols.len();

        for left_row in &combined_rows {
            let mut matched = false;
            for right_row in &join_rows {
                let mut candidate = left_row.clone();
                candidate.extend(right_row.iter().cloned());
                let all_cols: Vec<(String, String)> = combined_cols.iter()
                    .chain(join_cols.iter())
                    .cloned()
                    .collect();
                if eval_condition(&join.on, &candidate, &all_cols) {
                    new_rows.push(candidate);
                    matched = true;
                }
            }
            if !matched && join.join_type == parser::JoinType::Left {
                let mut row = left_row.clone();
                row.extend(std::iter::repeat(Value::Null).take(join_cols.len()));
                new_rows.push(row);
            }
        }

        if join.join_type == parser::JoinType::Right {
            for right_row in &join_rows {
                let has_match = combined_rows.iter().any(|left_row| {
                    let mut candidate = left_row.clone();
                    candidate.extend(right_row.iter().cloned());
                    let all_cols: Vec<(String, String)> = combined_cols.iter()
                        .chain(join_cols.iter())
                        .cloned()
                        .collect();
                    eval_condition(&join.on, &candidate, &all_cols)
                });
                if !has_match {
                    let mut row: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                    row.extend(right_row.iter().cloned());
                    new_rows.push(row);
                }
            }
        }

        combined_cols.extend(join_cols);
        combined_rows = new_rows;
    }

    // apply WHERE
    let rows: Vec<Vec<Value>> = combined_rows.into_iter()
        .filter(|row| {
            match &stmt.where_clause {
                Some(wc) => eval_condition(&wc.condition, row, &combined_cols),
                None => true,
            }
        })
        .collect();

    // apply LIMIT
    let rows = if let Some(n) = stmt.limit {
        rows.into_iter().take(n as usize).collect()
    } else {
        rows
    };

    Ok(format!("({} rows)", rows.len()))
}

fn eval_condition(cond: &parser::Condition, row: &[Value], cols: &[(String, String)]) -> bool {
    let left = resolve_expr(&cond.left, row, cols);
    let right = resolve_expr(&cond.right, row, cols);
    match (left, right) {
        (Some(l), Some(r)) => compare(&l, &cond.operator, &r),
        _ => false,
    }
}

fn resolve_expr(expr: &parser::Expression, row: &[Value], cols: &[(String, String)]) -> Option<Value> {
    match expr {
        parser::Expression::Literal(v) => Some(v.clone()),
        parser::Expression::Column(name) => {
            cols.iter().position(|c| c.1 == *name).map(|i| row[i].clone())
        }
        parser::Expression::QualifiedColumn(table, col) => {
            cols.iter().position(|c| c.0 == *table && c.1 == *col).map(|i| row[i].clone())
        }
        parser::Expression::Subquery(_) => None,
    }
}

fn compare(left: &Value, op: &parser::Operator, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => match op {
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            parser::Operator::GreaterThan => l > r,
            parser::Operator::LessThan => l < r,
            parser::Operator::GreaterThanOrEqual => l >= r,
            parser::Operator::LessThanOrEqual => l <= r,
            parser::Operator::Like | parser::Operator::In | parser::Operator::NotIn
            | parser::Operator::Exists | parser::Operator::NotExists => false,
        },
        (Value::String(l), Value::String(r)) => match op {
            parser::Operator::Like => like_match(l, r),
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            parser::Operator::GreaterThan => l > r,
            parser::Operator::LessThan => l < r,
            parser::Operator::GreaterThanOrEqual => l >= r,
            parser::Operator::LessThanOrEqual => l <= r,
            parser::Operator::In | parser::Operator::NotIn
            | parser::Operator::Exists | parser::Operator::NotExists => false,
        },
        _ => false,
    }
}

fn like_match(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_recursive(&v, &p, 0, 0)
}

fn like_match_recursive(v: &[char], p: &[char], vi: usize, pi: usize) -> bool {
    if pi == p.len() {
        return vi == v.len();
    }
    match p[pi] {
        '%' => {
            for i in vi..=v.len() {
                if like_match_recursive(v, p, i, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => vi < v.len() && like_match_recursive(v, p, vi + 1, pi + 1),
        c => vi < v.len() && v[vi] == c && like_match_recursive(v, p, vi + 1, pi + 1),
    }
}
