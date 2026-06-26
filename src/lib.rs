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
                .map(|(n, ret)| {
                    if let Some(rows) = ret {
                        format_returning_rows(&rows)
                    } else {
                        format!("Inserted {} row(s)", n)
                    }
                })
                .map_err(|e| e.to_string())
        }
        SqlStatement::Select(select_stmt) => {
            execute_select_to_string(&select_stmt, storage)
        }
        SqlStatement::Update(update_stmt) => {
            storage.update_rows(&update_stmt)
                .map(|(n, ret)| {
                    if let Some(rows) = ret {
                        format_returning_rows(&rows)
                    } else {
                        format!("Updated {} row(s)", n)
                    }
                })
                .map_err(|e| e.to_string())
        }
        SqlStatement::Delete(delete_stmt) => {
            storage.delete_rows(&delete_stmt)
                .map(|(n, ret)| {
                    if let Some(rows) = ret {
                        format_returning_rows(&rows)
                    } else {
                        format!("Deleted {} row(s)", n)
                    }
                })
                .map_err(|e| e.to_string())
        }
        SqlStatement::Truncate(stmt) => {
            storage.truncate_table(&stmt)
                .map(|_| format!("Truncated table '{}'", stmt.table_name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::Merge(stmt) => {
            storage.execute_merge(&stmt)
                .map(|(matched, inserted)| format!("Merged: {} matched, {} inserted", matched, inserted))
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
        SqlStatement::CreateFunction(stmt) => {
            let name = stmt.name.clone();
            storage.create_function(&stmt)
                .map(|_| format!("Created function '{}'", name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::DropFunction(stmt) => {
            if stmt.if_exists && !storage.function_exists(&stmt.name) {
                return Ok(format!("Function '{}' does not exist", stmt.name));
            }
            storage.drop_function(&stmt.name, false)
                .map(|_| format!("Dropped function '{}'", stmt.name))
                .map_err(|e| e.to_string())
        }
        SqlStatement::Begin => {
            storage.begin_transaction().map(|_| "BEGIN".to_string()).map_err(|e| e.to_string())
        }
        SqlStatement::Commit => {
            storage.commit_transaction().map(|_| "COMMIT".to_string()).map_err(|e| e.to_string())
        }
        SqlStatement::Rollback => {
            storage.rollback_transaction().map(|_| "ROLLBACK".to_string()).map_err(|e| e.to_string())
        }
        SqlStatement::Savepoint(name) => {
            storage.create_savepoint(&name).map(|_| "SAVEPOINT".to_string()).map_err(|e| e.to_string())
        }
        SqlStatement::RollbackToSavepoint(name) => {
            storage.rollback_to_savepoint(&name).map(|_| "ROLLBACK".to_string()).map_err(|e| e.to_string())
        }
        SqlStatement::ReleaseSavepoint(name) => {
            storage.release_savepoint(&name).map(|_| "RELEASE".to_string()).map_err(|e| e.to_string())
        }
    }
}

/// Format RETURNING rows as a simple newline-separated value list
fn format_returning_rows(rows: &[Vec<Value>]) -> String {
    if rows.is_empty() {
        return "(0 rows)".to_string();
    }
    let lines: Vec<String> = rows.iter().map(|row| {
        row.iter().map(|v| match v {
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Bool(b) => b.to_string(),
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Date(d) => parser::format_date(*d),
            Value::Timestamp(ts) => parser::format_timestamp(*ts),
            Value::Null => "NULL".to_string(),
        }).collect::<Vec<_>>().join(", ")
    }).collect();
    format!("({} rows)\n{}", rows.len(), lines.join("\n"))
}

// Column map type used throughout lib.rs: (table_alias, column_name)
type LibCols = Vec<(String, String)>;
// CTE map: name -> (columns, rows)
type LibCteMap = std::collections::HashMap<String, (LibCols, Vec<Vec<Value>>)>;

/// Load rows for a table name, checking the CTE map first then storage.
fn lib_load_table<'a>(
    name: &str,
    cte_map: &'a LibCteMap,
    storage: &Storage,
) -> Result<(LibCols, Vec<Vec<Value>>), String> {
    if let Some((cols, rows)) = cte_map.get(name) {
        let tagged: LibCols = cols.iter().map(|(_, c)| (name.to_string(), c.clone())).collect();
        return Ok((tagged, rows.clone()));
    }
    if let Ok(Some(view_sql)) = storage.load_view(name) {
        let inner_stmt = match parser::parse_sql(&view_sql) {
            Ok((_, parser::SqlStatement::Select(s))) => s,
            _ => return Err(format!("View '{}' has invalid SQL", name)),
        };
        return lib_materialize_select(&inner_stmt, storage, cte_map);
    }
    let schema = storage.load_schema(name).map_err(|e| e.to_string())?;
    let rows = storage.read_rows(name).map_err(|e| e.to_string())?;
    let cols: LibCols = schema.columns.iter().map(|c| (name.to_string(), c.name.clone())).collect();
    Ok((cols, rows))
}

/// Handle "SELECT expr, ..." with no FROM — returns a single synthetic row.
fn lib_materialize_no_from_select(
    stmt: &parser::SelectStatement,
    storage: &Storage,
) -> Result<(LibCols, Vec<Vec<Value>>), String> {
    let empty_row: Vec<Value> = Vec::new();
    let empty_cols: LibCols = Vec::new();
    let mut out_cols: LibCols = Vec::new();
    let mut out_vals: Vec<Value> = Vec::new();
    for col in &stmt.columns {
        let (col_name, expr) = match col {
            parser::SelectColumn::Expr(e) => (format!("{:?}", e), e.clone()),
            parser::SelectColumn::Alias(inner, alias) => {
                if let parser::SelectColumn::Expr(e) = inner.as_ref() {
                    (alias.clone(), e.clone())
                } else {
                    (alias.clone(), parser::Expression::Literal(Value::Null))
                }
            }
            parser::SelectColumn::Column(n) => (n.clone(), parser::Expression::Column(n.clone())),
            _ => ("?".to_string(), parser::Expression::Literal(Value::Null)),
        };
        let val = resolve_expr(&expr, &empty_row, &empty_cols, storage).unwrap_or(Value::Null);
        out_cols.push((String::new(), col_name));
        out_vals.push(val);
    }
    Ok((out_cols, vec![out_vals]))
}

/// Materialize a SELECT statement given an existing CTE map, returning (cols, rows).
fn lib_materialize_select(
    stmt: &parser::SelectStatement,
    storage: &Storage,
    cte_map: &LibCteMap,
) -> Result<(LibCols, Vec<Vec<Value>>), String> {
    // Handle SELECT without FROM — produces one synthetic row from expressions
    if let parser::FromClause::Table(name) = &stmt.from {
        if name == "__no_from__" {
            return lib_materialize_no_from_select(stmt, storage);
        }
    }

    // Handle VALUES inline table
    if let parser::FromClause::Values(value_rows, col_names) = &stmt.from {
        let from_alias = stmt.from_alias.as_deref().unwrap_or("_values");
        let materialized: Vec<Vec<Value>> = value_rows.iter().map(|exprs| {
            exprs.iter().map(|e| resolve_expr(e, &[], &[], storage).unwrap_or(Value::Null)).collect()
        }).collect();
        let ncols = materialized.first().map(|r| r.len()).unwrap_or(0);
        let combined_cols: LibCols = (0..ncols).map(|i| {
            let name = col_names.get(i).cloned().unwrap_or_else(|| format!("column{}", i + 1));
            (from_alias.to_string(), name)
        }).collect();
        // Apply WHERE, LIMIT, OFFSET, projection
        let rows: Vec<Vec<Value>> = materialized.into_iter()
            .filter(|row| match &stmt.where_clause {
                Some(wc) => eval_condition(&wc.condition, row, &combined_cols, storage),
                None => true,
            })
            .collect();
        let rows = if let Some(off) = stmt.offset { rows.into_iter().skip(off as usize).collect() } else { rows };
        let rows = if let Some(n) = stmt.limit { rows.into_iter().take(n as usize).collect() } else { rows };
        let (out_cols, out_rows) = lib_project_rows(stmt, &combined_cols, rows, storage);
        return Ok((out_cols, out_rows));
    }

    // Handle subquery in FROM
    if let parser::FromClause::Subquery(subquery) = &stmt.from {
        let from_alias = stmt.from_alias.as_deref().unwrap_or("_subquery");
        let (sub_cols, sub_rows) = lib_materialize_select(subquery, storage, cte_map)?;
        let combined_cols: LibCols = sub_cols.into_iter()
            .map(|(_, c)| (from_alias.to_string(), c))
            .collect();
        let rows: Vec<Vec<Value>> = sub_rows.into_iter()
            .filter(|row| match &stmt.where_clause {
                Some(wc) => eval_condition(&wc.condition, row, &combined_cols, storage),
                None => true,
            })
            .collect();
        let rows = if let Some(off) = stmt.offset { rows.into_iter().skip(off as usize).collect() } else { rows };
        let rows = if let Some(n) = stmt.limit { rows.into_iter().take(n as usize).collect() } else { rows };
        let (out_cols, out_rows) = lib_project_rows(stmt, &combined_cols, rows, storage);
        return Ok((out_cols, out_rows));
    }

    let table_name = stmt.from.table_name().ok_or("FROM clause not supported here")?;
    let from_alias = stmt.from_alias.as_deref().unwrap_or(table_name);

    let (from_cols_raw, from_rows) = lib_load_table(table_name, cte_map, storage)?;
    let mut combined_cols: LibCols = from_cols_raw.into_iter()
        .map(|(_, c)| (from_alias.to_string(), c))
        .collect();
    let mut combined_rows: Vec<Vec<Value>> = from_rows;

    // Process joins (simplified: only INNER/LEFT supported here)
    for join in &stmt.joins {
        let (join_cols_raw, join_rows) = lib_load_table(&join.table, cte_map, storage)?;
        let join_alias = join.alias.as_deref().unwrap_or(&join.table);
        let join_cols: LibCols = join_cols_raw.into_iter().map(|(_, c)| (join_alias.to_string(), c)).collect();

        let mut new_rows = Vec::new();
        for left_row in &combined_rows {
            let mut matched = false;
            for right_row in &join_rows {
                let mut candidate = left_row.clone();
                candidate.extend(right_row.iter().cloned());
                let all_cols: LibCols = combined_cols.iter().chain(join_cols.iter()).cloned().collect();
                let matches = match &join.on {
                    Some(cond) => eval_condition(cond, &candidate, &all_cols, storage),
                    None => true,
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
        combined_cols.extend(join_cols);
        combined_rows = new_rows;
    }

    // Apply WHERE
    let rows: Vec<Vec<Value>> = combined_rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => eval_condition(&wc.condition, row, &combined_cols, storage),
            None => true,
        })
        .collect();

    // Apply OFFSET then LIMIT
    let rows: Vec<_> = if let Some(off) = stmt.offset {
        rows.into_iter().skip(off as usize).collect()
    } else { rows };
    let rows = if let Some(n) = stmt.limit {
        rows.into_iter().take(n as usize).collect()
    } else { rows };

    // Project columns
    let (out_cols, out_rows) = lib_project_rows(stmt, &combined_cols, rows, storage);
    Ok((out_cols, out_rows))
}

/// Project a set of rows according to the SELECT column list.
fn lib_project_rows(
    stmt: &parser::SelectStatement,
    combined_cols: &LibCols,
    rows: Vec<Vec<Value>>,
    storage: &Storage,
) -> (LibCols, Vec<Vec<Value>>) {
    match stmt.columns.as_slice() {
        [parser::SelectColumn::All] => {
            // * — return all columns as-is, but strip table prefix from names
            let out_cols: LibCols = combined_cols.iter().map(|(_, c)| (String::new(), c.clone())).collect();
            (out_cols, rows)
        }
        cols => {
            let mut out_col_names: LibCols = Vec::new();
            let mut col_sources: Vec<Option<usize>> = Vec::new(); // index into combined_cols or None for expr
            let mut col_exprs: Vec<Option<parser::Expression>> = Vec::new();
            for col in cols {
                match col {
                    parser::SelectColumn::Column(name) => {
                        let idx = combined_cols.iter().position(|(_, c)| c == name);
                        out_col_names.push((String::new(), name.clone()));
                        col_sources.push(idx);
                        col_exprs.push(None);
                    }
                    // table.column — look up by both table alias and column name
                    parser::SelectColumn::QualifiedColumn(tbl, name) => {
                        let idx = combined_cols.iter().position(|(t, c)| t == tbl && c == name);
                        out_col_names.push((String::new(), name.clone()));
                        col_sources.push(idx);
                        col_exprs.push(None);
                    }
                    parser::SelectColumn::Alias(inner, alias) => {
                        if let parser::SelectColumn::Expr(expr) = inner.as_ref() {
                            out_col_names.push((String::new(), alias.clone()));
                            col_sources.push(None);
                            col_exprs.push(Some(expr.clone()));
                        } else if let parser::SelectColumn::Column(name) = inner.as_ref() {
                            let idx = combined_cols.iter().position(|(_, c)| c == name);
                            out_col_names.push((String::new(), alias.clone()));
                            col_sources.push(idx);
                            col_exprs.push(None);
                        } else if let parser::SelectColumn::QualifiedColumn(tbl, name) = inner.as_ref() {
                            let idx = combined_cols.iter().position(|(t, c)| t == tbl && c == name);
                            out_col_names.push((String::new(), alias.clone()));
                            col_sources.push(idx);
                            col_exprs.push(None);
                        } else {
                            out_col_names.push((String::new(), alias.clone()));
                            col_sources.push(None);
                            col_exprs.push(None);
                        }
                    }
                    parser::SelectColumn::Expr(expr) => {
                        out_col_names.push((String::new(), format!("{:?}", expr)));
                        col_sources.push(None);
                        col_exprs.push(Some(expr.clone()));
                    }
                    _ => {} // skip aggregates etc. in this simplified path
                }
            }
            let out_rows: Vec<Vec<Value>> = rows.into_iter().map(|row| {
                col_sources.iter().zip(col_exprs.iter()).map(|(src, expr)| {
                    if let Some(idx) = src {
                        row.get(*idx).cloned().unwrap_or(Value::Null)
                    } else if let Some(e) = expr {
                        resolve_expr(e, &row, combined_cols, storage).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }).collect()
            }).collect();
            (out_col_names, out_rows)
        }
    }
}

/// Fixpoint algorithm for a recursive CTE. Returns (cols, rows).
fn lib_materialize_recursive_cte(
    cte: &parser::CteDefinition,
    storage: &Storage,
    existing_ctes: &LibCteMap,
) -> Result<(LibCols, Vec<Vec<Value>>), String> {
    let query = &cte.query;

    let (union_type, recursive_query) = match &query.union {
        Some((ut, rq)) => (ut.clone(), rq.as_ref()),
        None => {
            // No UNION — just materialize normally
            return lib_materialize_select(query, storage, existing_ctes);
        }
    };

    // Anchor: left side of UNION
    let mut anchor_stmt = query.clone();
    anchor_stmt.union = None;
    let (anchor_cols_raw, anchor_rows) = lib_materialize_select(&anchor_stmt, storage, existing_ctes)?;

    // Determine output column names from CTE column list or anchor headers
    let output_col_names: Vec<String> = if !cte.columns.is_empty() {
        cte.columns.clone()
    } else {
        anchor_cols_raw.iter().map(|(_, c)| c.clone()).collect()
    };
    let output_cols: LibCols = output_col_names.iter().map(|n| (String::new(), n.clone())).collect();

    let mut accumulated: Vec<Vec<Value>> = anchor_rows.clone();
    let mut current_rows = anchor_rows;
    let mut seen: Vec<Vec<Value>> = accumulated.clone();

    let max_iterations = 10_000usize;
    for _ in 0..max_iterations {
        if current_rows.is_empty() {
            break;
        }

        // Expose current rows under the CTE name
        let mut iter_ctes = existing_ctes.clone();
        iter_ctes.insert(cte.name.clone(), (output_cols.clone(), current_rows.clone()));

        let (_, next_rows) = lib_materialize_select(recursive_query, storage, &iter_ctes)?;

        let new_rows: Vec<Vec<Value>> = match union_type {
            parser::UnionType::UnionAll | parser::UnionType::IntersectAll | parser::UnionType::ExceptAll => next_rows,
            _ => next_rows.into_iter().filter(|r| !seen.contains(r)).collect(),
        };

        if new_rows.is_empty() {
            break;
        }

        seen.extend(new_rows.clone());
        accumulated.extend(new_rows.clone());
        current_rows = new_rows;
    }

    Ok((output_cols, accumulated))
}

// Minimal select executor that loads data and applies WHERE, returning results as a string
fn execute_select_to_string(
    stmt: &parser::SelectStatement,
    storage: &Storage,
) -> Result<String, String> {
    // Handle FOR UPDATE: acquire lock (requires active transaction)
    if stmt.for_update {
        let table_name = stmt.from.table_name()
            .ok_or_else(|| "FOR UPDATE requires a table reference".to_string())?;
        storage.lock_for_update(table_name).map_err(|e| e.to_string())?;
    }

    // If there are CTEs, materialize them first (including recursive)
    if !stmt.ctes.is_empty() {
        let mut cte_map: LibCteMap = std::collections::HashMap::new();
        for cte in &stmt.ctes {
            let result = if cte.recursive {
                lib_materialize_recursive_cte(cte, storage, &cte_map)?
            } else {
                lib_materialize_select(&cte.query, storage, &cte_map)?
            };
            // Apply column rename if CTE column list is provided
            let (mut cols, rows) = result;
            if !cte.columns.is_empty() {
                for (i, col) in cols.iter_mut().enumerate() {
                    if let Some(name) = cte.columns.get(i) {
                        col.1 = name.clone();
                    }
                }
            }
            cte_map.insert(cte.name.clone(), (cols, rows));
        }
        // Now execute the main query using the CTE map
        let (_, rows) = lib_materialize_select(stmt, storage, &cte_map)?;
        return Ok(format!("({} rows)", rows.len()));
    }

    // Delegate non-table FROM clauses (subquery, values) to lib_materialize_select
    if stmt.from.table_name().is_none() {
        let empty_ctes: LibCteMap = std::collections::HashMap::new();
        let (_, rows) = lib_materialize_select(stmt, storage, &empty_ctes)?;
        return Ok(format!("({} rows)", rows.len()));
    }

    let table_name = stmt.from.table_name().ok_or("FROM clause not supported here")?;

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
        // Handle LATERAL (SELECT ...) joins
        if let Some(lateral_query) = &join.lateral {
            let lateral_alias = join.alias.as_deref().unwrap_or("lateral");
            // Determine lateral output column names from a dry run on the first outer row
            let lat_col_names: Vec<String> = if let Some(_outer_row) = combined_rows.first() {
                let table_name = lateral_query.from.table_name().unwrap_or("__unknown__");
                let schema = storage.load_schema(table_name).ok();
                let inner_cols: Vec<(String, String)> = schema.as_ref()
                    .map(|s| s.columns.iter().map(|c| (table_name.to_string(), c.name.clone())).collect())
                    .unwrap_or_default();
                // Collect col names from lateral_query SELECT list
                lateral_query.columns.iter().filter_map(|c| match c {
                    parser::SelectColumn::Column(n) => Some(n.clone()),
                    parser::SelectColumn::Alias(_, a) => Some(a.clone()),
                    parser::SelectColumn::QualifiedColumn(_, n) => Some(n.clone()),
                    _ => inner_cols.first().map(|(_, n)| n.clone()),
                }).collect()
            } else {
                vec!["col1".to_string()]
            };
            let lat_ncols = lat_col_names.len().max(1);
            let new_lat_cols: Vec<(String, String)> = lat_col_names.into_iter()
                .map(|n| (lateral_alias.to_string(), n))
                .collect();

            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for outer_row in &combined_rows {
                // Execute the lateral subquery for each outer row using correlated eval
                let table_name = lateral_query.from.table_name().unwrap_or("__unknown__");
                let schema = storage.load_schema(table_name).ok();
                let inner_cols: Vec<(String, String)> = schema.as_ref()
                    .map(|s| s.columns.iter().map(|c| (table_name.to_string(), c.name.clone())).collect())
                    .unwrap_or_default();
                let all_inner_rows = schema.as_ref()
                    .and_then(|_| storage.read_rows(table_name).ok())
                    .unwrap_or_default();
                let lat_rows: Vec<Vec<Value>> = all_inner_rows.into_iter()
                    .filter(|row| match &lateral_query.where_clause {
                        Some(wc) => eval_correlated_condition(&wc.condition, row, &inner_cols, storage, outer_row, &combined_cols),
                        None => true,
                    })
                    .collect();
                let lat_rows: Vec<Vec<Value>> = if let Some(n) = lateral_query.limit {
                    lat_rows.into_iter().take(n as usize).collect()
                } else { lat_rows };

                if lat_rows.is_empty() {
                    if join.join_type == parser::JoinType::Left {
                        let mut row = outer_row.clone();
                        row.extend(std::iter::repeat(Value::Null).take(lat_ncols));
                        new_rows.push(row);
                    }
                    // INNER/CROSS: omit outer row if no match
                } else {
                    for lat_row in &lat_rows {
                        // Project lateral columns from inner row
                        let lat_vals: Vec<Value> = lateral_query.columns.iter().filter_map(|c| match c {
                            parser::SelectColumn::Column(n) | parser::SelectColumn::QualifiedColumn(_, n) => {
                                inner_cols.iter().position(|(_, cn)| cn == n).and_then(|i| lat_row.get(i).cloned())
                            }
                            _ => lat_row.first().cloned(),
                        }).collect();
                        let lat_vals = if lat_vals.is_empty() { lat_row.clone() } else { lat_vals };
                        let mut new_row = outer_row.clone();
                        new_row.extend(lat_vals);
                        new_rows.push(new_row);
                    }
                }
            }
            combined_cols.extend(new_lat_cols);
            combined_rows = new_rows;
            continue;
        }

        let join_schema = storage.load_schema(&join.table).map_err(|e| e.to_string())?;
        let join_rows = storage.read_rows(&join.table).map_err(|e| e.to_string())?;
        let join_alias = join.alias.as_deref().unwrap_or(&join.table);
        let join_cols: Vec<(String, String)> = join_schema.columns.iter()
            .map(|c| (join_alias.to_string(), c.name.clone()))
            .collect();

        let mut new_rows = Vec::new();
        let left_col_count = combined_cols.len();

        // Determine shared columns for NATURAL JOIN or JOIN USING
        let shared_cols: Vec<String> = if matches!(join.join_type, parser::JoinType::Natural) {
            combined_cols.iter()
                .filter(|(_, lname)| join_cols.iter().any(|(_, rname)| rname.eq_ignore_ascii_case(lname)))
                .map(|(_, name)| name.clone())
                .collect()
        } else if let Some(using) = &join.using {
            using.clone()
        } else {
            Vec::new()
        };

        // Check equality of shared columns in a candidate row
        let check_shared = |candidate: &Vec<Value>, all_cols: &Vec<(String, String)>| -> bool {
            for col_name in &shared_cols {
                let li = all_cols[..left_col_count].iter().position(|(_, n)| n.eq_ignore_ascii_case(col_name));
                let ri = all_cols[left_col_count..].iter().position(|(_, n)| n.eq_ignore_ascii_case(col_name))
                    .map(|i| i + left_col_count);
                match (li, ri) {
                    (Some(l), Some(r)) => { if candidate[l] != candidate[r] { return false; } }
                    _ => return false,
                }
            }
            true
        };

        for left_row in &combined_rows {
            let mut matched = false;
            for right_row in &join_rows {
                let mut candidate = left_row.clone();
                candidate.extend(right_row.iter().cloned());
                let all_cols: Vec<(String, String)> = combined_cols.iter()
                    .chain(join_cols.iter())
                    .cloned()
                    .collect();
                let matches = if !shared_cols.is_empty() {
                    check_shared(&candidate, &all_cols)
                } else {
                    match &join.on {
                        Some(cond) => eval_condition(cond, &candidate, &all_cols, storage),
                        None => true, // CROSS JOIN — no condition
                    }
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
                    if !shared_cols.is_empty() {
                        check_shared(&candidate, &all_cols)
                    } else {
                        match &join.on {
                            Some(cond) => eval_condition(cond, &candidate, &all_cols, storage),
                            None => true,
                        }
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

/// Execute a subquery and return all first-column values (for ANY/ALL evaluation).
fn lib_execute_subquery_all_values(stmt: &parser::SelectStatement, storage: &Storage) -> Vec<Value> {
    let table_name = match stmt.from.table_name() { Some(t) => t, None => return vec![] };
    let schema = match storage.load_schema(table_name) { Ok(s) => s, Err(_) => return vec![] };
    let rows = match storage.read_rows(table_name) { Ok(r) => r, Err(_) => return vec![] };
    let inner_cols: Vec<(String, String)> = schema.columns.iter()
        .map(|c| (table_name.to_string(), c.name.clone()))
        .collect();
    rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => eval_correlated_condition(&wc.condition, row, &inner_cols, storage, &[], &[]),
            None => true,
        })
        .filter_map(|row| {
            match stmt.columns.first() {
                Some(parser::SelectColumn::Column(name)) => {
                    schema.columns.iter().position(|c| c.name == *name).and_then(|i| row.get(i).cloned())
                }
                _ => row.into_iter().next(),
            }
        })
        .collect()
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
            if *operator == parser::Operator::Similar || *operator == parser::Operator::NotSimilar {
                let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
                let rv = resolve_correlated_expr(right, row, cols, storage, outer_row, outer_cols);
                let escape = upper_bound.as_ref().and_then(|e| resolve_correlated_expr(e, row, cols, storage, outer_row, outer_cols));
                let similar = match (&lv, &rv) {
                    (Some(parser::Value::String(s)), Some(parser::Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let parser::Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = crate::storage::similar_to_regex(p, escape_char);
                        regex::Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::Similar { similar } else { !similar };
            }
            let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
            let rv = resolve_correlated_expr(right, row, cols, storage, outer_row, outer_cols);
            match (lv, rv) {
                (Some(l), Some(r)) => compare(&l, operator, &r),
                _ => false,
            }
        }
        parser::Condition::Unique(_) | parser::Condition::NotUnique(_) | parser::Condition::Overlaps(..) => false,
        parser::Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols) { Some(v) => v, None => return false };
            let values = lib_execute_subquery_all_values(subquery, storage);
            values.iter().any(|rv| compare(&lv, op, rv))
        }
        parser::Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols) { Some(v) => v, None => return false };
            let values = lib_execute_subquery_all_values(subquery, storage);
            // Vacuously true if no rows
            values.iter().all(|rv| compare(&lv, op, rv))
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
            if *operator == parser::Operator::Similar || *operator == parser::Operator::NotSimilar {
                let lv = resolve_expr(left, row, cols, storage);
                let rv = resolve_expr(right, row, cols, storage);
                let escape = upper_bound.as_ref().and_then(|e| resolve_expr(e, row, cols, storage));
                let similar = match (&lv, &rv) {
                    (Some(parser::Value::String(s)), Some(parser::Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let parser::Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = crate::storage::similar_to_regex(p, escape_char);
                        regex::Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::Similar { similar } else { !similar };
            }
            let lv = resolve_expr(left, row, cols, storage);
            let rv = resolve_expr(right, row, cols, storage);
            match (lv, rv) {
                (Some(l), Some(r)) => compare(&l, operator, &r),
                _ => false,
            }
        }
        parser::Condition::Unique(_) | parser::Condition::NotUnique(_) | parser::Condition::Overlaps(..) => false,
        parser::Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_expr(left, row, cols, storage) { Some(v) => v, None => return false };
            let values = lib_execute_subquery_all_values(subquery, storage);
            values.iter().any(|rv| compare(&lv, op, rv))
        }
        parser::Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_expr(left, row, cols, storage) { Some(v) => v, None => return false };
            let values = lib_execute_subquery_all_values(subquery, storage);
            values.iter().all(|rv| compare(&lv, op, rv))
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
        parser::Expression::Greatest(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expr(e, row, cols, storage)).collect();
            parser::apply_greatest(args)
        }
        parser::Expression::Least(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expr(e, row, cols, storage)).collect();
            parser::apply_least(args)
        }
        parser::Expression::Power(base, exp) => {
            let b = resolve_expr(base, row, cols, storage)?;
            let e = resolve_expr(exp, row, cols, storage)?;
            parser::apply_power(b, e)
        }
        parser::Expression::Position(needle, haystack) => {
            let n = resolve_expr(needle, row, cols, storage)?;
            let h = resolve_expr(haystack, row, cols, storage)?;
            parser::apply_position(n, h)
        }
        parser::Expression::Repeat(s, n) => {
            let sv = resolve_expr(s, row, cols, storage)?;
            let nv = resolve_expr(n, row, cols, storage)?;
            parser::apply_repeat(sv, nv)
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
        // Date/time expressions
        parser::Expression::CurrentDate => Some(Value::Date(parser::current_epoch_days())),
        parser::Expression::CurrentTimestamp => Some(Value::Timestamp(parser::current_epoch_secs())),
        parser::Expression::Extract(field, expr) => {
            let v = resolve_expr(expr, row, cols, storage)?;
            lib_eval_extract(field, v)
        }
        parser::Expression::DateTrunc(field, expr) => {
            let v = resolve_expr(expr, row, cols, storage)?;
            lib_eval_date_trunc(field, v)
        }
        parser::Expression::DateDiff(unit, e1, e2) => {
            let v1 = resolve_expr(e1, row, cols, storage)?;
            let v2 = resolve_expr(e2, row, cols, storage)?;
            lib_eval_datediff(unit, v1, v2)
        }
        parser::Expression::DateAdd(date_expr, n, unit) => {
            let v = resolve_expr(date_expr, row, cols, storage)?;
            lib_eval_dateadd(v, *n, unit)
        }
        parser::Expression::JsonTypeOf(inner) => {
            let v = resolve_expr(inner, row, cols, storage)?;
            parser::apply_json_typeof(&v)
        }
        parser::Expression::JsonArrayLength(inner) => {
            let v = resolve_expr(inner, row, cols, storage)?;
            parser::apply_json_array_length(&v)
        }
        parser::Expression::JsonBuildObject(pairs) => {
            let resolved: Vec<(Value, Value)> = pairs.iter()
                .filter_map(|(k, v)| {
                    let kv = resolve_expr(k, row, cols, storage)?;
                    let vv = resolve_expr(v, row, cols, storage)?;
                    Some((kv, vv))
                })
                .collect();
            parser::apply_json_build_object(&resolved)
        }
        parser::Expression::JsonBuildArray(vals) => {
            let resolved: Vec<Value> = vals.iter()
                .filter_map(|v| resolve_expr(v, row, cols, storage))
                .collect();
            parser::apply_json_build_array(&resolved)
        }
        parser::Expression::UserFunc(name, args) => {
            let func_def = match storage.load_function(name) {
                Ok(Some(f)) => f,
                _ => return None,
            };
            if func_def.params.len() != args.len() {
                return None;
            }
            let arg_vals: Vec<Value> = args.iter()
                .filter_map(|a| resolve_expr(a, row, cols, storage))
                .collect();
            if arg_vals.len() != args.len() {
                return None;
            }
            let func_cols: Vec<(String, String)> = func_def.params.iter()
                .map(|(n, _)| ("".to_string(), n.clone()))
                .collect();
            resolve_expr(&func_def.body, &arg_vals, &func_cols, storage)
        }
    }
}

/// Evaluate EXTRACT(field FROM value) — mirrors main.rs version
fn lib_eval_extract(field: &str, v: Value) -> Option<Value> {
    let days = match &v {
        Value::Date(d) => *d,
        Value::Timestamp(ts) => (*ts / 86400) as i32,
        Value::String(s) => parser::parse_date_str(s)
            .or_else(|| parser::parse_timestamp_str(s).map(|ts| (ts / 86400) as i32))?,
        _ => return None,
    };
    let (y, m, d) = parser::epoch_days_to_date(days);
    match field {
        "YEAR"  => Some(Value::Int(y as i64)),
        "MONTH" => Some(Value::Int(m as i64)),
        "DAY" | "DAY_OF_MONTH" => Some(Value::Int(d as i64)),
        "HOUR"   => if let Value::Timestamp(ts) = v { Some(Value::Int((ts % 86400) / 3600)) } else { Some(Value::Int(0)) },
        "MINUTE" => if let Value::Timestamp(ts) = v { Some(Value::Int((ts % 86400 % 3600) / 60)) } else { Some(Value::Int(0)) },
        "SECOND" => if let Value::Timestamp(ts) = v { Some(Value::Int(ts % 60)) } else { Some(Value::Int(0)) },
        "DOW" | "DAY_OF_WEEK" => Some(Value::Int(((days % 7 + 4) % 7) as i64)),
        "DOY" | "DAY_OF_YEAR" => {
            let jan1 = parser::date_to_epoch_days(y, 1, 1);
            Some(Value::Int((days - jan1 + 1) as i64))
        }
        "QUARTER" => Some(Value::Int(((m - 1) / 3 + 1) as i64)),
        "WEEK" => {
            let jan1 = parser::date_to_epoch_days(y, 1, 1);
            Some(Value::Int(((days - jan1) / 7 + 1) as i64))
        }
        _ => None,
    }
}

fn lib_eval_date_trunc(field: &str, v: Value) -> Option<Value> {
    let days = match &v {
        Value::Date(d) => *d,
        Value::Timestamp(ts) => (*ts / 86400) as i32,
        Value::String(s) => parser::parse_date_str(s)?,
        _ => return None,
    };
    let (y, m, _) = parser::epoch_days_to_date(days);
    let truncated_days = match field {
        "YEAR"    => parser::date_to_epoch_days(y, 1, 1),
        "MONTH"   => parser::date_to_epoch_days(y, m, 1),
        "DAY"     => days,
        "WEEK"    => { let dow = ((days % 7 + 4) % 7) as i32; days - dow }
        "QUARTER" => { let q_month = ((m - 1) / 3) * 3 + 1; parser::date_to_epoch_days(y, q_month, 1) }
        _ => return None,
    };
    match &v {
        Value::Date(_)      => Some(Value::Date(truncated_days)),
        Value::Timestamp(_) => Some(Value::Timestamp(truncated_days as i64 * 86400)),
        _                   => Some(Value::Date(truncated_days)),
    }
}

fn lib_eval_datediff(unit: &str, v1: Value, v2: Value) -> Option<Value> {
    let d1 = match &v1 { Value::Date(d) => *d, Value::Timestamp(ts) => (*ts / 86400) as i32, Value::String(s) => parser::parse_date_str(s)?, _ => return None };
    let d2 = match &v2 { Value::Date(d) => *d, Value::Timestamp(ts) => (*ts / 86400) as i32, Value::String(s) => parser::parse_date_str(s)?, _ => return None };
    let diff = (d1 - d2) as i64;
    let result = match unit.to_uppercase().as_str() {
        "DAY" | "DD" | "DAYS" => diff,
        "WEEK" | "WEEKS" => diff / 7,
        "MONTH" | "MONTHS" => { let (y1,m1,_) = parser::epoch_days_to_date(d1); let (y2,m2,_) = parser::epoch_days_to_date(d2); ((y1-y2)*12 + (m1-m2)) as i64 }
        "YEAR" | "YY" | "YEARS" => { let (y1,_,_) = parser::epoch_days_to_date(d1); let (y2,_,_) = parser::epoch_days_to_date(d2); (y1-y2) as i64 }
        "HOUR" | "HOURS" => diff * 24,
        "MINUTE" | "MINUTES" => diff * 1440,
        "SECOND" | "SECONDS" => diff * 86400,
        _ => return None,
    };
    Some(Value::Int(result))
}

fn lib_eval_dateadd(v: Value, n: i64, unit: &str) -> Option<Value> {
    let spu: i64 = match unit.to_uppercase().as_str() {
        "SECOND"|"SECONDS" => 1, "MINUTE"|"MINUTES" => 60, "HOUR"|"HOURS" => 3600,
        "DAY"|"DAYS" => 86400, "WEEK"|"WEEKS" => 604800,
        "MONTH"|"MONTHS" => 2592000, "YEAR"|"YEARS" => 31536000,
        _ => return None,
    };
    match v {
        Value::Date(d)      => Some(Value::Date((d as i64 + n * spu / 86400) as i32)),
        Value::Timestamp(ts)=> Some(Value::Timestamp(ts + n * spu)),
        _ => None,
    }
}

/// Evaluate a binary arithmetic / concatenation operation
fn lib_eval_arith(left: &Value, op: &parser::ArithOp, right: &Value) -> Option<Value> {
    // JSON field access operators
    if matches!(op, parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText) {
        return parser::apply_json_op(left, op, right);
    }
    if let parser::ArithOp::Concat = op {
        let ls = match left {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => parser::format_date(*d),
            Value::Timestamp(ts) => parser::format_timestamp(*ts),
        };
        let rs = match right {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => parser::format_date(*d),
            Value::Timestamp(ts) => parser::format_timestamp(*ts),
        };
        return Some(Value::String(ls + &rs));
    }
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            match op {
                parser::ArithOp::Add => Some(Value::Int(l + r)),
                parser::ArithOp::Sub => Some(Value::Int(l - r)),
                parser::ArithOp::Mul => Some(Value::Int(l * r)),
                parser::ArithOp::Div => { if *r == 0 { Some(Value::Null) } else { Some(Value::Int(l / r)) } }
                parser::ArithOp::Mod => { if *r == 0 { Some(Value::Null) } else { Some(Value::Int(l % r)) } }
                parser::ArithOp::Concat | parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText => unreachable!(),
            }
        }
        (Value::Float(l), Value::Float(r)) => {
            match op {
                parser::ArithOp::Add => Some(Value::Float(l + r)),
                parser::ArithOp::Sub => Some(Value::Float(l - r)),
                parser::ArithOp::Mul => Some(Value::Float(l * r)),
                parser::ArithOp::Div => { if *r == 0.0 { Some(Value::Null) } else { Some(Value::Float(l / r)) } }
                parser::ArithOp::Mod => Some(Value::Float(l % r)),
                parser::ArithOp::Concat | parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText => unreachable!(),
            }
        }
        (Value::Int(l), Value::Float(r)) => {
            let l = *l as f64;
            match op {
                parser::ArithOp::Add => Some(Value::Float(l + r)),
                parser::ArithOp::Sub => Some(Value::Float(l - r)),
                parser::ArithOp::Mul => Some(Value::Float(l * r)),
                parser::ArithOp::Div => { if *r == 0.0 { Some(Value::Null) } else { Some(Value::Float(l / r)) } }
                parser::ArithOp::Mod => Some(Value::Float(l % r)),
                parser::ArithOp::Concat | parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText => unreachable!(),
            }
        }
        (Value::Float(l), Value::Int(r)) => {
            let r = *r as f64;
            match op {
                parser::ArithOp::Add => Some(Value::Float(l + r)),
                parser::ArithOp::Sub => Some(Value::Float(l - r)),
                parser::ArithOp::Mul => Some(Value::Float(l * r)),
                parser::ArithOp::Div => { if r == 0.0 { Some(Value::Null) } else { Some(Value::Float(l / r)) } }
                parser::ArithOp::Mod => Some(Value::Float(l % r)),
                parser::ArithOp::Concat | parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText => unreachable!(),
            }
        }
        // Date + Int / Date - Int → shift by days
        (Value::Date(d), Value::Int(n)) => match op {
            parser::ArithOp::Add => Some(Value::Date(d + *n as i32)),
            parser::ArithOp::Sub => Some(Value::Date(d - *n as i32)),
            _ => None,
        },
        // Date - Date → difference in days
        (Value::Date(a), Value::Date(b)) => match op {
            parser::ArithOp::Sub => Some(Value::Int((a - b) as i64)),
            _ => None,
        },
        // Timestamp + Int / Timestamp - Int → shift by seconds
        (Value::Timestamp(ts), Value::Int(n)) => match op {
            parser::ArithOp::Add => Some(Value::Timestamp(ts + n)),
            parser::ArithOp::Sub => Some(Value::Timestamp(ts - n)),
            _ => None,
        },
        // Timestamp - Timestamp → difference in seconds
        (Value::Timestamp(a), Value::Timestamp(b)) => match op {
            parser::ArithOp::Sub => Some(Value::Int(a - b)),
            _ => None,
        },
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
    // IS DISTINCT FROM / IS NOT DISTINCT FROM: NULL is comparable
    if *op == parser::Operator::IsDistinctFrom || *op == parser::Operator::IsNotDistinctFrom {
        let distinct = match (left, right) {
            (Value::Null, Value::Null) => false,
            (Value::Null, _) | (_, Value::Null) => true,
            _ => compare(left, &parser::Operator::NotEquals, right),
        };
        return if *op == parser::Operator::IsDistinctFrom { distinct } else { !distinct };
    }

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
        // Date comparisons
        (Value::Date(a), Value::Date(b)) => compare_numeric(*a as f64, *b as f64, op),
        (Value::Timestamp(a), Value::Timestamp(b)) => compare_numeric(*a as f64, *b as f64, op),
        (Value::Date(a), Value::Timestamp(b)) => compare_numeric((*a as i64 * 86400) as f64, *b as f64, op),
        (Value::Timestamp(a), Value::Date(b)) => compare_numeric(*a as f64, (*b as i64 * 86400) as f64, op),
        (Value::Date(d), Value::String(s)) => {
            if let Some(rd) = parser::parse_date_str(s) { compare_numeric(*d as f64, rd as f64, op) } else { false }
        }
        (Value::String(s), Value::Date(d)) => {
            if let Some(ld) = parser::parse_date_str(s) { compare_numeric(ld as f64, *d as f64, op) } else { false }
        }
        (Value::Timestamp(ts), Value::String(s)) => {
            if let Some(rts) = parser::parse_timestamp_str(s) { compare_numeric(*ts as f64, rts as f64, op) } else { false }
        }
        (Value::String(s), Value::Timestamp(ts)) => {
            if let Some(lts) = parser::parse_timestamp_str(s) { compare_numeric(lts as f64, *ts as f64, op) } else { false }
        }
        // JSON comparisons
        (Value::Json(l), Value::Json(r)) | (Value::Json(l), Value::String(r)) | (Value::String(l), Value::Json(r)) => match op {
            parser::Operator::JsonContains => parser::json_contains(l, r),
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
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
