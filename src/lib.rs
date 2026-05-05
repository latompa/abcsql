pub mod parser;
pub mod storage;

pub use parser::{parse_sql, SqlStatement, Value};
pub use storage::Storage;

/// Execute a SQL string against the storage engine. Returns Ok with a description
/// of what happened, or Err with an error message. Never panics.
pub fn execute(storage: &Storage, sql: &str) -> Result<String, String> {
    let stripped = parser::strip_sql_comments(sql.trim());
    let trimmed = stripped.trim();
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
        SqlStatement::CreateIndex(idx_stmt) => {
            let label = if idx_stmt.unique { "unique index" } else { "index" };
            storage.create_index(&idx_stmt)
                .map(|_| format!("Created {} '{}'", label, idx_stmt.index_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::DropIndex(idx_stmt) => {
            storage.drop_index(&idx_stmt.index_name)
                .map(|_| format!("Dropped index '{}'", idx_stmt.index_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::DropTable(stmt) => {
            if stmt.if_exists && !storage.table_exists(&stmt.table_name) {
                return Ok(format!("Table '{}' does not exist", stmt.table_name));
            }
            storage.drop_table(&stmt.table_name)
                .map(|_| format!("Dropped table '{}'", stmt.table_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::AlterTable(stmt) => {
            storage.alter_table(&stmt)
                .map(|_| format!("Altered table '{}'", stmt.table_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::CreateView(stmt) => {
            storage.create_view(&stmt.view_name, &stmt.select_sql)
                .map(|_| format!("Created view '{}'", stmt.view_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::DropView(stmt) => {
            if stmt.if_exists && !storage.view_exists(&stmt.view_name) {
                return Ok(format!("View '{}' does not exist", stmt.view_name));
            }
            storage.drop_view(&stmt.view_name)
                .map(|_| format!("Dropped view '{}'", stmt.view_name))
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

    // If FROM names a view, expand it by re-running the view's SELECT
    if let Ok(Some(view_sql)) = storage.load_view(table_name) {
        let inner_stmt = match parser::parse_sql(&view_sql) {
            Ok((_, parser::SqlStatement::Select(s))) => s,
            _ => return Err(format!("View '{}' contains invalid SQL", table_name)),
        };
        return execute_select_to_string(&inner_stmt, storage);
    }

    let from_schema = storage.load_schema(table_name).map_err(|e| e.to_string())?;

    // Try to use an index if WHERE is a simple column = literal equality
    let from_rows = if let Some(ref wc) = stmt.where_clause {
        let hint = if let parser::Condition::Comparison { left, operator: parser::Operator::Equals, right, .. } = &wc.condition {
            match (left, right) {
                (parser::Expression::Column(col), parser::Expression::Literal(val)) => Some((col.as_str(), val)),
                (parser::Expression::Literal(val), parser::Expression::Column(col)) => Some((col.as_str(), val)),
                _ => None,
            }
        } else {
            None
        };
        if let Some((col, val)) = hint {
            if let Ok(Some(idx_name)) = storage.find_index(table_name, col) {
                if let Ok(Some(row_nums)) = storage.lookup_index(&idx_name, val) {
                    storage.read_rows_by_numbers(table_name, &row_nums).map_err(|e| e.to_string())?
                } else {
                    storage.read_rows(table_name).map_err(|e| e.to_string())?
                }
            } else {
                storage.read_rows(table_name).map_err(|e| e.to_string())?
            }
        } else {
            storage.read_rows(table_name).map_err(|e| e.to_string())?
        }
    } else {
        storage.read_rows(table_name).map_err(|e| e.to_string())?
    };

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
                let matches = match &join.on {
                    Some(cond) => eval_condition(cond, &candidate, &all_cols, storage),
                    None => true, // CROSS JOIN — no condition
                };
                if matches {
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
                    match &join.on {
                        Some(cond) => eval_condition(cond, &candidate, &all_cols, storage),
                        None => true,
                    }
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
                Some(wc) => eval_condition(&wc.condition, row, &combined_cols, storage),
                None => true,
            }
        })
        .collect();

    // apply OFFSET then LIMIT
    let rows: Vec<_> = if let Some(off) = stmt.offset {
        rows.into_iter().skip(off as usize).collect()
    } else {
        rows
    };
    let rows = if let Some(n) = stmt.limit {
        rows.into_iter().take(n as usize).collect()
    } else {
        rows
    };

    Ok(format!("({} rows)", rows.len()))
}

fn lib_execute_scalar_subquery(stmt: &parser::SelectStatement, storage: &Storage) -> Option<Value> {
    lib_execute_correlated_subquery(stmt, storage, &[], &[])
}

/// Execute a subquery with optional outer row context for correlated references.
fn lib_execute_correlated_subquery(
    stmt: &parser::SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[(String, String)],
) -> Option<Value> {
    let table_name = stmt.from.table_name()?;
    let schema = storage.load_schema(table_name).ok()?;
    let rows = storage.read_rows(table_name).ok()?;
    let inner_cols: Vec<(String, String)> = schema.columns.iter()
        .map(|c| (table_name.to_string(), c.name.clone()))
        .collect();
    let filtered: Vec<Vec<Value>> = rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => eval_correlated_condition(&wc.condition, row, &inner_cols, storage, outer_row, outer_cols),
            None => true,
        })
        .collect();
    let first_row = filtered.into_iter().next()?;
    match stmt.columns.first() {
        Some(parser::SelectColumn::Column(name)) => {
            let idx = schema.columns.iter().position(|c| c.name == *name)?;
            first_row.get(idx).cloned()
        }
        _ => first_row.into_iter().next(),
    }
}

/// Evaluate a condition, falling back to outer_cols/outer_row for unresolved column references.
fn eval_correlated_condition(
    cond: &parser::Condition,
    row: &[Value],
    cols: &[(String, String)],
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[(String, String)],
) -> bool {
    match cond {
        parser::Condition::And(l, r) => {
            eval_correlated_condition(l, row, cols, storage, outer_row, outer_cols)
                && eval_correlated_condition(r, row, cols, storage, outer_row, outer_cols)
        }
        parser::Condition::Or(l, r) => {
            eval_correlated_condition(l, row, cols, storage, outer_row, outer_cols)
                || eval_correlated_condition(r, row, cols, storage, outer_row, outer_cols)
        }
        parser::Condition::Not(inner) => {
            !eval_correlated_condition(inner, row, cols, storage, outer_row, outer_cols)
        }
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == parser::Operator::IsNull || *operator == parser::Operator::IsNotNull {
                let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
                let is_null = matches!(lv, Some(Value::Null) | None);
                return if *operator == parser::Operator::IsNull { is_null } else { !is_null };
            }
            if *operator == parser::Operator::Between || *operator == parser::Operator::NotBetween {
                let val = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
                let low = resolve_correlated_expr(right, row, cols, storage, outer_row, outer_cols);
                let high = upper_bound.as_ref().and_then(|e| resolve_correlated_expr(e, row, cols, storage, outer_row, outer_cols));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare(v, &parser::Operator::GreaterThanOrEqual, l) && compare(v, &parser::Operator::LessThanOrEqual, h));
                return if *operator == parser::Operator::Between { in_range } else { !in_range };
            }
            if *operator == parser::Operator::In || *operator == parser::Operator::NotIn {
                let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
                let contains = match right {
                    parser::Expression::List(exprs) => lv.map_or(false, |lv| {
                        exprs.iter().any(|e| resolve_correlated_expr(e, row, cols, storage, outer_row, outer_cols).map_or(false, |rv| rv == lv))
                    }),
                    parser::Expression::Subquery(sub) => {
                        let first = lib_execute_scalar_subquery(sub, storage);
                        lv.map_or(false, |lv| first.map_or(false, |rv| rv == lv))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::NotIn { !contains } else { contains };
            }
            if *operator == parser::Operator::Exists || *operator == parser::Operator::NotExists {
                if let parser::Expression::Subquery(sub) = right {
                    let exists = lib_execute_correlated_subquery(sub, storage, row, cols).is_some();
                    return if *operator == parser::Operator::NotExists { !exists } else { exists };
                }
                return false;
            }
            let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
            let rv = resolve_correlated_expr(right, row, cols, storage, outer_row, outer_cols);
            match (lv, rv) {
                (Some(l), Some(r)) => compare(&l, operator, &r),
                _ => false,
            }
        }
    }
}

/// Resolve expression with fallback to outer_cols for correlated column references.
fn resolve_correlated_expr(
    expr: &parser::Expression,
    row: &[Value],
    cols: &[(String, String)],
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[(String, String)],
) -> Option<Value> {
    match expr {
        parser::Expression::Column(name) => {
            // Try inner cols first, then outer cols
            if let Some(i) = cols.iter().position(|c| c.1 == *name) {
                return Some(row[i].clone());
            }
            outer_cols.iter().position(|c| c.1 == *name).map(|i| outer_row[i].clone())
        }
        parser::Expression::QualifiedColumn(table, col) => {
            if let Some(i) = cols.iter().position(|c| c.0 == *table && c.1 == *col) {
                return Some(row[i].clone());
            }
            outer_cols.iter().position(|c| c.0 == *table && c.1 == *col).map(|i| outer_row[i].clone())
        }
        // For all other expression types, delegate to resolve_expr (uses inner row/cols only)
        other => resolve_expr(other, row, cols, storage),
    }
}

fn eval_condition(cond: &parser::Condition, row: &[Value], cols: &[(String, String)], storage: &Storage) -> bool {
    match cond {
        parser::Condition::And(left, right) => {
            eval_condition(left, row, cols, storage) && eval_condition(right, row, cols, storage)
        }
        parser::Condition::Or(left, right) => {
            eval_condition(left, row, cols, storage) || eval_condition(right, row, cols, storage)
        }
        parser::Condition::Not(inner) => !eval_condition(inner, row, cols, storage),
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == parser::Operator::IsNull || *operator == parser::Operator::IsNotNull {
                let lv = resolve_expr(left, row, cols, storage);
                let is_null = matches!(lv, Some(Value::Null) | None);
                return if *operator == parser::Operator::IsNull { is_null } else { !is_null };
            }
            if *operator == parser::Operator::Between || *operator == parser::Operator::NotBetween {
                let val = resolve_expr(left, row, cols, storage);
                let low = resolve_expr(right, row, cols, storage);
                let high = upper_bound.as_ref().and_then(|e| resolve_expr(e, row, cols, storage));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare(v, &parser::Operator::GreaterThanOrEqual, l) && compare(v, &parser::Operator::LessThanOrEqual, h));
                return if *operator == parser::Operator::Between { in_range } else { !in_range };
            }
            if *operator == parser::Operator::In || *operator == parser::Operator::NotIn {
                let lv = resolve_expr(left, row, cols, storage);
                let contains = match right {
                    parser::Expression::List(exprs) => {
                        lv.map_or(false, |lv| {
                            exprs.iter().any(|e| resolve_expr(e, row, cols, storage).map_or(false, |rv| rv == lv))
                        })
                    }
                    parser::Expression::Subquery(sub) => {
                        let first = lib_execute_scalar_subquery(sub, storage);
                        lv.map_or(false, |lv| first.map_or(false, |rv| rv == lv))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::NotIn { !contains } else { contains };
            }
            if *operator == parser::Operator::Exists || *operator == parser::Operator::NotExists {
                if let parser::Expression::Subquery(sub) = right {
                    let exists = lib_execute_correlated_subquery(sub, storage, row, cols).is_some();
                    return if *operator == parser::Operator::NotExists { !exists } else { exists };
                }
                return false;
            }
            let lv = resolve_expr(left, row, cols, storage);
            let rv = resolve_expr(right, row, cols, storage);
            match (lv, rv) {
                (Some(l), Some(r)) => compare(&l, operator, &r),
                _ => false,
            }
        }
    }
}

fn resolve_expr(expr: &parser::Expression, row: &[Value], cols: &[(String, String)], storage: &Storage) -> Option<Value> {
    match expr {
        parser::Expression::Literal(v) => Some(v.clone()),
        parser::Expression::Column(name) => {
            cols.iter().position(|c| c.1 == *name).map(|i| row[i].clone())
        }
        parser::Expression::QualifiedColumn(table, col) => {
            cols.iter().position(|c| c.0 == *table && c.1 == *col).map(|i| row[i].clone())
        }
        parser::Expression::Subquery(subquery) => lib_execute_scalar_subquery(subquery, storage),
        parser::Expression::List(_) => None,
        parser::Expression::ScalarFunc(func, inner) => {
            resolve_expr(inner, row, cols, storage).and_then(|v| parser::apply_scalar_func(func, v))
        }
        parser::Expression::Coalesce(exprs) => {
            exprs.iter().find_map(|e| {
                let v = resolve_expr(e, row, cols, storage);
                match v { Some(Value::Null) | None => None, other => other }
            })
        }
        parser::Expression::NullIf(a, b) => {
            let va = resolve_expr(a, row, cols, storage);
            let vb = resolve_expr(b, row, cols, storage);
            match (&va, &vb) {
                (Some(l), Some(r)) if l == r => Some(Value::Null),
                _ => va,
            }
        }
        parser::Expression::Round(val, places) => {
            let v = resolve_expr(val, row, cols, storage)?;
            let p = places.as_ref().and_then(|e| resolve_expr(e, row, cols, storage));
            parser::apply_round(v, p)
        }
        parser::Expression::Concat(exprs) => {
            let parts: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expr(e, row, cols, storage)).collect();
            parser::apply_concat(parts)
        }
        parser::Expression::Substr(s, start, len) => {
            let sv = resolve_expr(s, row, cols, storage)?;
            let startv = resolve_expr(start, row, cols, storage)?;
            let lenv = len.as_ref().and_then(|e| resolve_expr(e, row, cols, storage));
            parser::apply_substr(sv, startv, lenv)
        }
        parser::Expression::Replace(s, from, to) => {
            let sv = resolve_expr(s, row, cols, storage)?;
            let fv = resolve_expr(from, row, cols, storage)?;
            let tv = resolve_expr(to, row, cols, storage)?;
            parser::apply_replace(sv, fv, tv)
        }
        parser::Expression::LPad(s, len, pad) => {
            let sv = resolve_expr(s, row, cols, storage)?;
            let lv = resolve_expr(len, row, cols, storage)?;
            let pv = resolve_expr(pad, row, cols, storage)?;
            parser::apply_lpad(sv, lv, pv)
        }
        parser::Expression::RPad(s, len, pad) => {
            let sv = resolve_expr(s, row, cols, storage)?;
            let lv = resolve_expr(len, row, cols, storage)?;
            let pv = resolve_expr(pad, row, cols, storage)?;
            parser::apply_rpad(sv, lv, pv)
        }
        parser::Expression::Cast(inner, type_name) => {
            let v = resolve_expr(inner, row, cols, storage)?;
            parser::apply_cast(v, type_name)
        }
        parser::Expression::BinaryOp(left, op, right) => {
            let lv = resolve_expr(left, row, cols, storage)?;
            let rv = resolve_expr(right, row, cols, storage)?;
            lib_eval_arith(&lv, op, &rv)
        }
        parser::Expression::Aggregate(_, _) => None,
        parser::Expression::Window(_, _) => None,
        parser::Expression::Case(branches, else_expr) => {
            for (cond, then_expr) in branches {
                if eval_condition(cond, row, cols, storage) {
                    return resolve_expr(then_expr, row, cols, storage);
                }
            }
            else_expr.as_ref().and_then(|e| resolve_expr(e, row, cols, storage))
        }
    }
}

/// Evaluate a binary arithmetic / concatenation operation
fn lib_eval_arith(left: &Value, op: &parser::ArithOp, right: &Value) -> Option<Value> {
    if let parser::ArithOp::Concat = op {
        let ls = match left {
            Value::String(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
        };
        let rs = match right {
            Value::String(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
        };
        return Some(Value::String(ls + &rs));
    }
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            let v = match op {
                parser::ArithOp::Add => l + r,
                parser::ArithOp::Sub => l - r,
                parser::ArithOp::Mul => l * r,
                parser::ArithOp::Div => { if *r == 0 { return Some(Value::Null); } l / r }
                parser::ArithOp::Concat => unreachable!(),
            };
            Some(Value::Int(v))
        }
        (Value::Float(l), Value::Float(r)) => {
            let v = match op {
                parser::ArithOp::Add => l + r,
                parser::ArithOp::Sub => l - r,
                parser::ArithOp::Mul => l * r,
                parser::ArithOp::Div => { if *r == 0.0 { return Some(Value::Null); } l / r }
                parser::ArithOp::Concat => unreachable!(),
            };
            Some(Value::Float(v))
        }
        (Value::Int(l), Value::Float(r)) => {
            let l = *l as f64;
            let v = match op {
                parser::ArithOp::Add => l + r,
                parser::ArithOp::Sub => l - r,
                parser::ArithOp::Mul => l * r,
                parser::ArithOp::Div => { if *r == 0.0 { return Some(Value::Null); } l / r }
                parser::ArithOp::Concat => unreachable!(),
            };
            Some(Value::Float(v))
        }
        (Value::Float(l), Value::Int(r)) => {
            let r = *r as f64;
            let v = match op {
                parser::ArithOp::Add => l + r,
                parser::ArithOp::Sub => l - r,
                parser::ArithOp::Mul => l * r,
                parser::ArithOp::Div => { if r == 0.0 { return Some(Value::Null); } l / r }
                parser::ArithOp::Concat => unreachable!(),
            };
            Some(Value::Float(v))
        }
        _ => None,
    }
}

fn compare_numeric(l: f64, r: f64, op: &parser::Operator) -> bool {
    match op {
        parser::Operator::Equals => l == r,
        parser::Operator::NotEquals => l != r,
        parser::Operator::GreaterThan => l > r,
        parser::Operator::LessThan => l < r,
        parser::Operator::GreaterThanOrEqual => l >= r,
        parser::Operator::LessThanOrEqual => l <= r,
        _ => false,
    }
}

fn compare(left: &Value, op: &parser::Operator, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => compare_numeric(*l as f64, *r as f64, op),
        (Value::Float(l), Value::Float(r)) => compare_numeric(*l, *r, op),
        (Value::Int(l), Value::Float(r)) => compare_numeric(*l as f64, *r, op),
        (Value::Float(l), Value::Int(r)) => compare_numeric(*l, *r as f64, op),
        (Value::Bool(l), Value::Bool(r)) => match op {
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            _ => false,
        },
        (Value::String(l), Value::String(r)) => match op {
            parser::Operator::Like => like_match(l, r),
            parser::Operator::NotLike => !like_match(l, r),
            parser::Operator::ILike => like_match(&l.to_lowercase(), &r.to_lowercase()),
            parser::Operator::NotILike => !like_match(&l.to_lowercase(), &r.to_lowercase()),
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            parser::Operator::GreaterThan => l > r,
            parser::Operator::LessThan => l < r,
            parser::Operator::GreaterThanOrEqual => l >= r,
            parser::Operator::LessThanOrEqual => l <= r,
            _ => false,
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
