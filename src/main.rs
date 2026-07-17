mod parser;
mod storage;

use std::collections::HashMap;
use parser::{parse_sql, SqlStatement, Value};
use storage::Storage;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

fn main() {
    let data_dir = std::env::args().nth(1).unwrap_or_else(|| "./data".to_string());

    let storage = match Storage::new(&data_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize storage: {}", e);
            std::process::exit(1);
        }
    };

    let history_file = dirs_or_default(&data_dir, ".abcsql_history");

    let mut rl = match DefaultEditor::new() {
        Ok(rl) => rl,
        Err(e) => {
            eprintln!("Failed to initialize REPL: {}", e);
            std::process::exit(1);
        }
    };

    if rl.load_history(&history_file).is_err() {
        // No previous history — ok
    }

    println!("abcsql v0.1.0");
    println!("Data directory: {}", data_dir);
    println!("Type .help for help, .quit to exit\n");

    loop {
        let prompt = "abcsql> ";
        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        };

        let trimmed = line.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }

        // Meta-commands are single-line
        if trimmed.starts_with('.') {
            let _ = rl.add_history_entry(trimmed.as_str());
            handle_meta_command(&trimmed, &storage);
            continue;
        }

        // Handle multi-line SQL: keep reading with continuation prompt
        // until we have balanced parens/quotes and non-empty input
        let mut buffer = trimmed;
        loop {
            let sql = buffer.trim();
            if sql.is_empty() {
                break;
            }
            if is_sql_complete(sql) {
                let _ = rl.add_history_entry(sql);
                execute_sql(sql, &storage);
                break;
            }
            // Read continuation line
            let cont = match rl.readline("  ...> ") {
                Ok(l) => l,
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    buffer.clear();
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!();
                    buffer.clear();
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    buffer.clear();
                    break;
                }
            };
            buffer.push('\n');
            buffer.push_str(&cont);
        }
    }

    let _ = rl.save_history(&history_file);
    println!("\nGoodbye!");
}

/// Return a full path for a file inside the data directory.
fn dirs_or_default(data_dir: &str, file: &str) -> String {
    let path = std::path::Path::new(data_dir).join(file);
    path.to_string_lossy().into_owned()
}

/// Simple heuristic: SQL is "complete" when parentheses and string literals
/// are balanced, so the parser can attempt to parse the whole buffer.
fn is_sql_complete(s: &str) -> bool {
    let mut depth: i32 = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut prev_bs = false;
    for c in s.chars() {
        if prev_bs {
            prev_bs = false;
            continue;
        }
        if c == '\\' { prev_bs = true; continue; }
        if in_single {
            if c == '\'' { in_single = false; }
            continue;
        }
        if in_double {
            if c == '"' { in_double = false; }
            continue;
        }
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            '\'' => in_single = true,
            '"' => in_double = true,
            _ => {}
        }
    }
    depth == 0 && !in_single && !in_double
}

fn handle_meta_command(cmd: &str, storage: &Storage) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let command = parts[0].to_lowercase();

    match command.as_str() {
        ".quit" | ".exit" => {
            println!("Goodbye!");
            std::process::exit(0);
        }
        ".help" => {
            println!("Meta-commands:");
            println!("  .help              Show this help");
            println!("  .quit,.exit        Exit the REPL");
            println!("  .tables            List all tables");
            println!("  .views             List all views");
            println!("  .indices           List all indexes");
            println!("  .functions         List all user-defined functions");
            println!("  .schema <table>    Show table schema");
            println!("  .databases         Show current database path");
            println!("\nSQL statements (multi-line supported):");
            println!("  CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, MERGE");
            println!("  CREATE/DROP VIEW, CREATE/DROP INDEX, CREATE/DROP FUNCTION");
            println!("  BEGIN, COMMIT, ROLLBACK, SAVEPOINT, TRUNCATE, ALTER TABLE");
        }
        ".tables" => {
            match storage.list_tables() {
                Ok(tables) => {
                    if tables.is_empty() {
                        println!("(no tables)");
                    } else {
                        for table in tables {
                            println!("{}", table);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".views" => {
            match storage.list_views() {
                Ok(views) => {
                    if views.is_empty() {
                        println!("(no views)");
                    } else {
                        for v in views {
                            println!("{}", v);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".indices" => {
            match storage.list_indexes() {
                Ok(indexes) => {
                    if indexes.is_empty() {
                        println!("(no indexes)");
                    } else {
                        for idx in indexes {
                            println!("{}", idx);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".functions" => {
            match storage.list_functions() {
                Ok(funcs) => {
                    if funcs.is_empty() {
                        println!("(no functions)");
                    } else {
                        for f in funcs {
                            println!("{}", f);
                        }
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        ".databases" => {
            println!("{}", storage.data_dir().display());
        }
        ".schema" => {
            if parts.len() < 2 {
                println!("Usage: .schema <table_name>");
                return;
            }
            let table_name = parts[1];
            if Storage::is_metadata_table(table_name) {
                if let Some(schema) = Storage::metadata_schema(table_name) {
                    println!("CREATE TABLE {} (", schema.table_name);
                    for (i, col) in schema.columns.iter().enumerate() {
                        let type_str = data_type_display(&col.data_type);
                        let nn = if col.not_null { " NOT NULL" } else { "" };
                        let comma = if i < schema.columns.len() - 1 { "," } else { "" };
                        println!("  {} {}{}{}", col.name, type_str, nn, comma);
                    }
                    println!(");");
                } else {
                    eprintln!("Metadata table '{}' not found", table_name);
                }
                return;
            }
            match storage.load_schema(table_name) {
                Ok(schema) => {
                    println!("CREATE TABLE {} (", schema.table_name);
                    for (i, col) in schema.columns.iter().enumerate() {
                        let type_str = data_type_display(&col.data_type);
                        let nn = if col.not_null { " NOT NULL" } else { "" };
                        let uq = if col.unique { " UNIQUE" } else { "" };
                        let auto_inc = if col.auto_increment { " AUTO_INCREMENT" } else { "" };
                        let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
                        let fk = col.references.as_ref()
                            .map(|r| format!(" REFERENCES {}({})", r.table, r.column))
                            .unwrap_or_default();
                        let comma = if i < schema.columns.len() - 1 { "," } else { "" };
                        println!("  {} {}{}{}{}{}{}{}", col.name, type_str, nn, uq, auto_inc, pk, fk, comma);
                    }
                    println!(");");
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        _ => {
            println!("Unknown command: {}. Type .help for help.", command);
        }
    }
}

fn data_type_display(dt: &parser::DataType) -> String {
    match dt {
        parser::DataType::Int => "INT".into(),
        parser::DataType::SmallInt => "SMALLINT".into(),
        parser::DataType::BigInt => "BIGINT".into(),
        parser::DataType::Float => "FLOAT".into(),
        parser::DataType::Real => "REAL".into(),
        parser::DataType::Double => "DOUBLE".into(),
        parser::DataType::Boolean => "BOOLEAN".into(),
        parser::DataType::Date => "DATE".into(),
        parser::DataType::Timestamp => "TIMESTAMP".into(),
        parser::DataType::Varchar(Some(n)) => format!("VARCHAR({})", n),
        parser::DataType::Varchar(None) => "VARCHAR".into(),
        parser::DataType::Char(Some(n)) => format!("CHAR({})", n),
        parser::DataType::Char(None) => "CHAR".into(),
        parser::DataType::Text => "TEXT".into(),
        parser::DataType::Decimal(Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
        parser::DataType::Decimal(Some(p), None) => format!("DECIMAL({})", p),
        parser::DataType::Decimal(None, _) => "DECIMAL".into(),
        parser::DataType::Uuid => "UUID".into(),
        parser::DataType::Json => "JSON".into(),
        parser::DataType::Jsonb => "JSONB".into(),
    }
}

fn execute_sql(sql: &str, storage: &Storage) {
    let stripped = parser::strip_sql_comments(sql);
    let sql = stripped.trim();
    let stmt = match parse_sql(sql) {
        Ok((remaining, stmt)) => {
            if !remaining.trim().is_empty() {
                eprintln!("Warning: unparsed input: '{}'", remaining.trim());
            }
            stmt
        }
        Err(e) => {
            eprintln!("Parse error: {:?}", e);
            return;
        }
    };

    match stmt {
        SqlStatement::CreateTable(create_stmt) => {
            let table_name = create_stmt.table_name.clone();
            match storage.create_table(&create_stmt) {
                Ok(_) => println!("Created table '{}'", table_name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Insert(insert_stmt) => {
            match &insert_stmt.source {
                parser::InsertSource::Values(_) | parser::InsertSource::DefaultValues => {
                    match storage.insert_row(&insert_stmt) {
                        Ok((n, _)) => println!("Inserted {} row(s)", n),
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
                parser::InsertSource::Select(select_stmt) => {
                    execute_insert_select(&insert_stmt.table_name, select_stmt, storage);
                }
            }
        }
        SqlStatement::Select(select_stmt) => {
            let (headers, rows) = execute_select(&select_stmt, storage);
            print_table(&headers, &rows);
        }
        SqlStatement::Update(update_stmt) => {
            match storage.update_rows(&update_stmt) {
                Ok((count, _)) => println!("Updated {} row(s)", count),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Delete(delete_stmt) => {
            match storage.delete_rows(&delete_stmt) {
                Ok((count, _)) => println!("Deleted {} row(s)", count),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::CreateIndex(idx_stmt) => {
            let name = idx_stmt.index_name.clone();
            let unique = idx_stmt.unique;
            match storage.create_index(&idx_stmt) {
                Ok(_) => println!("Created{} index '{}'", if unique { " unique" } else { "" }, name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::DropIndex(idx_stmt) => {
            let name = idx_stmt.index_name.clone();
            match storage.drop_index(&name) {
                Ok(_) => println!("Dropped index '{}'", name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::DropTable(drop_stmt) => {
            if drop_stmt.if_exists && !storage.table_exists(&drop_stmt.table_name) {
                println!("Table '{}' does not exist", drop_stmt.table_name);
                return;
            }
            let name = drop_stmt.table_name.clone();
            match storage.drop_table(&name) {
                Ok(_) => println!("Dropped table '{}'", name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::AlterTable(alter_stmt) => {
            let name = alter_stmt.table_name.clone();
            match storage.alter_table(&alter_stmt) {
                Ok(_) => println!("Altered table '{}'", name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::CreateView(stmt) => {
            match storage.create_view(&stmt.view_name, &stmt.select_sql) {
                Ok(_) => println!("Created view '{}'", stmt.view_name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::DropView(stmt) => {
            if stmt.if_exists && !storage.view_exists(&stmt.view_name) {
                println!("View '{}' does not exist", stmt.view_name);
                return;
            }
            match storage.drop_view(&stmt.view_name) {
                Ok(_) => println!("Dropped view '{}'", stmt.view_name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::CreateFunction(stmt) => {
            let name = stmt.name.clone();
            match storage.create_function(&stmt) {
                Ok(_) => println!("Created function '{}'", name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::DropFunction(stmt) => {
            if stmt.if_exists && !storage.function_exists(&stmt.name) {
                println!("Function '{}' does not exist", stmt.name);
                return;
            }
            match storage.drop_function(&stmt.name, false) {
                Ok(_) => println!("Dropped function '{}'", stmt.name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Truncate(stmt) => {
            let name = stmt.table_name.clone();
            match storage.truncate_table(&stmt) {
                Ok(_) => println!("Truncated table '{}'", name),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Merge(stmt) => {
            match storage.execute_merge(&stmt) {
                Ok((matched, inserted)) => println!("Merged: {} matched, {} inserted", matched, inserted),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Begin => {
            match storage.begin_transaction() {
                Ok(()) => println!("BEGIN"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Commit => {
            match storage.commit_transaction() {
                Ok(()) => println!("COMMIT"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Rollback => {
            match storage.rollback_transaction() {
                Ok(()) => println!("ROLLBACK"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Savepoint(name) => {
            match storage.create_savepoint(&name) {
                Ok(()) => println!("SAVEPOINT"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::RollbackToSavepoint(name) => {
            match storage.rollback_to_savepoint(&name) {
                Ok(()) => println!("ROLLBACK"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::ReleaseSavepoint(name) => {
            match storage.release_savepoint(&name) {
                Ok(()) => println!("RELEASE"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

/// A column in the combined result set, tracked by table name and column name
#[derive(Clone)]
struct ResultColumn {
    table: String,
    name: String,
}

/// Materialized CTE: column definitions + row data
#[derive(Clone)]
struct CteData {
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<Value>>,
}

/// Load a table's schema and rows from CTEs first, falling back to storage
fn load_table(
    name: &str,
    ctes: &HashMap<String, CteData>,
    storage: &Storage,
) -> Result<(Vec<ResultColumn>, Vec<Vec<Value>>), String> {
    load_table_with_index(name, ctes, storage, None)
}

// Load a table, optionally using an index for a WHERE column = value condition
fn load_table_with_index(
    name: &str,
    ctes: &HashMap<String, CteData>,
    storage: &Storage,
    index_hint: Option<(&str, &parser::Value)>,
) -> Result<(Vec<ResultColumn>, Vec<Vec<Value>>), String> {
    if let Some(cte) = ctes.get(name) {
        let cols = cte.columns.iter()
            .map(|c| ResultColumn { table: name.to_string(), name: c.name.clone() })
            .collect();
        return Ok((cols, cte.rows.clone()));
    }

    // Expand view if name refers to one
    if let Ok(Some(view_sql)) = storage.load_view(name) {
        let view_stmt = match parser::parse_sql(&view_sql) {
            Ok((_, parser::SqlStatement::Select(s))) => s,
            _ => return Err(format!("View '{}' contains invalid SQL", name)),
        };
        let (headers, string_rows) = execute_select(&view_stmt, storage);
        // Re-materialise as Value rows using the string representation
        let cols: Vec<ResultColumn> = headers.iter()
            .map(|h| ResultColumn { table: name.to_string(), name: h.clone() })
            .collect();
        let rows: Vec<Vec<Value>> = string_rows.iter()
            .map(|row| row.iter().map(|cell| {
                if cell == "NULL" { Value::Null }
                else if let Ok(i) = cell.parse::<i64>() { Value::Int(i) }
                else if let Ok(f) = cell.parse::<f64>() { Value::Float(f) }
                else if cell == "true" || cell == "false" { Value::Bool(cell == "true") }
                else { Value::String(cell.clone()) }
            }).collect())
            .collect();
        return Ok((cols, rows));
    }

    // Check for information_schema metadata tables
    if Storage::is_metadata_table(name) {
        if let Some(rows) = storage.read_metadata_rows(name) {
            if let Some(schema) = Storage::metadata_schema(name) {
                let cols: Vec<ResultColumn> = schema.columns.iter()
                    .map(|c| ResultColumn { table: name.to_string(), name: c.name.clone() })
                    .collect();
                return Ok((cols, rows));
            }
        }
        return Err(format!("Metadata table '{}' not found", name));
    }

    let schema = storage.load_schema(name).map_err(|e| e.to_string())?;

    // Try index lookup if we have a hint
    let rows = if let Some((col_name, value)) = index_hint {
        if let Ok(Some(idx_name)) = storage.find_index(name, col_name) {
            if let Ok(Some(row_nums)) = storage.lookup_index(&idx_name, value) {
                storage.read_rows_by_numbers(name, &row_nums).map_err(|e| e.to_string())?
            } else {
                storage.read_rows(name).map_err(|e| e.to_string())?
            }
        } else {
            storage.read_rows(name).map_err(|e| e.to_string())?
        }
    } else {
        storage.read_rows(name).map_err(|e| e.to_string())?
    };

    let cols = schema.columns.iter()
        .map(|c| ResultColumn { table: name.to_string(), name: c.name.clone() })
        .collect();
    Ok((cols, rows))
}

/// Load from a FromClause — handles both table names and subqueries
fn load_from(
    from: &parser::FromClause,
    alias: &str,
    ctes: &HashMap<String, CteData>,
    storage: &Storage,
) -> Result<(Vec<ResultColumn>, Vec<Vec<Value>>), String> {
    load_from_with_index(from, alias, ctes, storage, None)
}

// Load from a FromClause, optionally using an index for equality lookups
fn load_from_with_index(
    from: &parser::FromClause,
    alias: &str,
    ctes: &HashMap<String, CteData>,
    storage: &Storage,
    index_hint: Option<(&str, &parser::Value)>,
) -> Result<(Vec<ResultColumn>, Vec<Vec<Value>>), String> {
    match from {
        parser::FromClause::Table(name) => load_table_with_index(name, ctes, storage, index_hint),
        parser::FromClause::Subquery(subquery) => {
            let cte_data = materialize_cte_inner(subquery, storage, ctes);
            let cols = cte_data.columns.iter()
                .map(|c| ResultColumn { table: alias.to_string(), name: c.name.clone() })
                .collect();
            Ok((cols, cte_data.rows))
        }
        parser::FromClause::Values(value_rows, col_names) => {
            // Evaluate each expression row against an empty context
            let empty_cols: Vec<ResultColumn> = Vec::new();
            let empty_row: Vec<Value> = Vec::new();
            let materialized: Vec<Vec<Value>> = value_rows.iter().map(|exprs| {
                exprs.iter()
                    .map(|e| resolve_join_expression(e, &empty_row, &empty_cols, storage).unwrap_or(Value::Null))
                    .collect()
            }).collect();
            let ncols = materialized.first().map(|r| r.len()).unwrap_or(0);
            let result_cols: Vec<ResultColumn> = (0..ncols).map(|i| {
                let name = col_names.get(i).cloned()
                    .unwrap_or_else(|| format!("column{}", i + 1));
                ResultColumn { table: alias.to_string(), name }
            }).collect();
            Ok((result_cols, materialized))
        }
    }
}

// Extract a simple (column_name, literal_value) from a WHERE column = literal condition
fn extract_index_hint(where_clause: &Option<parser::WhereClause>) -> Option<(String, parser::Value)> {
    let wc = where_clause.as_ref()?;
    if let parser::Condition::Comparison { left, operator: parser::Operator::Equals, right, .. } = &wc.condition {
        match (left, right) {
            (parser::Expression::Column(col), parser::Expression::Literal(val)) => {
                return Some((col.clone(), val.clone()));
            }
            (parser::Expression::Literal(val), parser::Expression::Column(col)) => {
                return Some((col.clone(), val.clone()));
            }
            _ => {}
        }
    }
    None
}

/// Get the effective name for a FROM clause (table name or alias)
fn from_name(from: &parser::FromClause, alias: &Option<String>) -> String {
    match (from, alias) {
        (_, Some(a)) => a.clone(),
        (parser::FromClause::Table(name), None) => name.clone(),
        (parser::FromClause::Subquery(_), None) => "_subquery".to_string(),
        (parser::FromClause::Values(_, _), None) => "_values".to_string(),
    }
}

/// Get the output column name for a SelectColumn, respecting aliases
fn select_column_name(col: &parser::SelectColumn) -> String {
    match col {
        parser::SelectColumn::Alias(_, alias) => alias.clone(),
        parser::SelectColumn::Column(name) => name.clone(),
        parser::SelectColumn::QualifiedColumn(_, name) => name.clone(),
        parser::SelectColumn::Aggregate(_, _) => column_header(col),
        parser::SelectColumn::AggregateFiltered(_, _, _) => column_header(col),
        parser::SelectColumn::Expr(expr) => format_expr(expr),
        parser::SelectColumn::All => "*".to_string(),
        parser::SelectColumn::StarFromTable(tbl) => format!("{}.*", tbl),
    }
}

/// Execute a CTE definition, dispatching to the recursive path if needed
fn materialize_cte(
    cte: &parser::CteDefinition,
    storage: &Storage,
    existing_ctes: &HashMap<String, CteData>,
) -> CteData {
    if cte.recursive {
        return materialize_recursive_cte(cte, storage, existing_ctes);
    }
    let mut data = materialize_cte_inner(&cte.query, storage, existing_ctes);
    // Apply optional column rename from the CTE column list
    if !cte.columns.is_empty() {
        for (i, col) in data.columns.iter_mut().enumerate() {
            if let Some(name) = cte.columns.get(i) {
                col.name = name.clone();
            }
        }
    }
    data
}

/// Fixpoint evaluation for recursive CTEs
fn materialize_recursive_cte(
    cte: &parser::CteDefinition,
    storage: &Storage,
    existing_ctes: &HashMap<String, CteData>,
) -> CteData {
    let query = &cte.query;

    // Split UNION into anchor (left) and recursive term (right)
    let (union_type, recursive_query) = match &query.union {
        Some((ut, rq)) => (ut.clone(), rq.as_ref()),
        None => {
            // No UNION — execute normally as a non-recursive CTE
            return materialize_cte_inner(query, storage, existing_ctes);
        }
    };

    // Anchor: the left side of the UNION (strip the union field)
    let mut anchor_stmt = query.clone();
    anchor_stmt.union = None;

    let anchor_data = materialize_cte_inner(&anchor_stmt, storage, existing_ctes);

    // Determine output column names: use CTE column list if provided, else anchor's headers
    let output_columns: Vec<ResultColumn> = if !cte.columns.is_empty() {
        cte.columns.iter().map(|name| ResultColumn { table: String::new(), name: name.clone() }).collect()
    } else {
        anchor_data.columns.clone()
    };

    let mut accumulated: Vec<Vec<Value>> = anchor_data.rows.clone();
    let mut current_rows = anchor_data.rows;
    // Track seen rows for UNION (dedup) mode
    let mut seen: Vec<Vec<Value>> = accumulated.clone();

    let max_iterations = 10_000usize;
    for _ in 0..max_iterations {
        if current_rows.is_empty() {
            break;
        }

        // Expose current rows under the CTE name so the recursive term can reference them
        let mut iter_ctes = existing_ctes.clone();
        iter_ctes.insert(cte.name.clone(), CteData {
            columns: output_columns.clone(),
            rows: current_rows.clone(),
        });

        let next_data = materialize_cte_inner(recursive_query, storage, &iter_ctes);

        // UNION deduplicates; UNION ALL keeps everything
        let new_rows: Vec<Vec<Value>> = match union_type {
            parser::UnionType::UnionAll | parser::UnionType::IntersectAll | parser::UnionType::ExceptAll => {
                next_data.rows
            }
            _ => next_data.rows.into_iter().filter(|r| !seen.contains(r)).collect(),
        };

        if new_rows.is_empty() {
            break;
        }

        seen.extend(new_rows.clone());
        accumulated.extend(new_rows.clone());
        current_rows = new_rows;
    }

    CteData { columns: output_columns, rows: accumulated }
}

/// Handle "SELECT expr, expr, ..." with no FROM clause — produces exactly one row.
fn materialize_no_from_select(
    query: &parser::SelectStatement,
    _storage: &Storage,
    _existing_ctes: &HashMap<String, CteData>,
) -> CteData {
    let empty_row: Vec<Value> = Vec::new();
    let empty_cols: Vec<ResultColumn> = Vec::new();
    let empty_storage = Storage::new("/dev/null").unwrap();
    let mut result_cols: Vec<ResultColumn> = Vec::new();
    let mut result_vals: Vec<Value> = Vec::new();
    for col in &query.columns {
        let name = select_column_name(col);
        let expr = match col {
            parser::SelectColumn::Expr(e) => e.clone(),
            parser::SelectColumn::Alias(inner, _) => {
                if let parser::SelectColumn::Expr(e) = inner.as_ref() { e.clone() }
                else { parser::Expression::Literal(Value::Null) }
            }
            parser::SelectColumn::Column(n) => parser::Expression::Literal(Value::String(n.clone())),
            _ => parser::Expression::Literal(Value::Null),
        };
        let val = resolve_join_expression(&expr, &empty_row, &empty_cols, &empty_storage)
            .unwrap_or(Value::Null);
        result_cols.push(ResultColumn { table: String::new(), name });
        result_vals.push(val);
    }
    CteData { columns: result_cols, rows: vec![result_vals] }
}

/// Execute a SELECT query and capture its result as columns + rows (non-recursive inner path)
fn materialize_cte_inner(
    query: &parser::SelectStatement,
    storage: &Storage,
    existing_ctes: &HashMap<String, CteData>,
) -> CteData {
    // Handle SELECT without FROM (e.g. "SELECT 1, 2+3") — produces one synthetic row
    if let parser::FromClause::Table(name) = &query.from {
        if name == "__no_from__" {
            return materialize_no_from_select(query, storage, existing_ctes);
        }
    }

    let effective_name = from_name(&query.from, &query.from_alias);

    // Load FROM table
    let (from_cols, from_rows) = match load_from(&query.from, &effective_name, existing_ctes, storage) {
        Ok(r) => r,
        Err(_) => return CteData { columns: Vec::new(), rows: Vec::new() },
    };

    let combined_cols: Vec<ResultColumn> = from_cols.into_iter()
        .map(|c| ResultColumn { table: effective_name.clone(), name: c.name })
        .collect();

    // Filter by WHERE
    let filtered: Vec<Vec<Value>> = from_rows.into_iter()
        .filter(|row| {
            match &query.where_clause {
                Some(wc) => evaluate_join_condition(&wc.condition, row, &combined_cols, storage),
                None => true,
            }
        })
        .collect();

    // Check for aggregates / GROUP BY
    let has_aggregates = query.columns.iter().any(|c| {
        matches!(c, parser::SelectColumn::Aggregate(_, _))
            || matches!(c, parser::SelectColumn::Alias(inner, _) if matches!(inner.as_ref(), parser::SelectColumn::Aggregate(_, _)))
    });

    if has_aggregates || !query.group_by.is_empty() {
        return materialize_aggregate_cte(&query.columns, &filtered, &combined_cols, &query.group_by, query.having.as_ref(), storage);
    }

    // Determine output columns and their source indices
    let (result_cols, display_indices): (Vec<ResultColumn>, Vec<usize>) = match &query.columns[..] {
        [parser::SelectColumn::All] => {
            let cols = combined_cols.iter()
                .map(|c| ResultColumn { table: String::new(), name: c.name.clone() })
                .collect();
            let idxs = (0..combined_cols.len()).collect();
            (cols, idxs)
        }
        query_cols => {
            let mut cols = Vec::new();
            let mut idxs = Vec::new();
            for col in query_cols {
                match col {
                    parser::SelectColumn::StarFromTable(tbl) => {
                        for (i, c) in combined_cols.iter().enumerate() {
                            if c.table.eq_ignore_ascii_case(tbl) {
                                cols.push(ResultColumn { table: String::new(), name: c.name.clone() });
                                idxs.push(i);
                            }
                        }
                    }
                    parser::SelectColumn::All => {
                        for (i, c) in combined_cols.iter().enumerate() {
                            cols.push(ResultColumn { table: String::new(), name: c.name.clone() });
                            idxs.push(i);
                        }
                    }
                    _ => {
                        let inner = match col {
                            parser::SelectColumn::Alias(inner, _) => inner.as_ref(),
                            other => other,
                        };
                        if let Some(idx) = resolve_column_index(inner, &combined_cols) {
                            cols.push(ResultColumn { table: String::new(), name: select_column_name(col) });
                            idxs.push(idx);
                        }
                    }
                }
            }
            (cols, idxs)
        }
    };

    let mut result_rows: Vec<Vec<Value>> = filtered.iter()
        .map(|row| display_indices.iter().map(|&i| row[i].clone()).collect())
        .collect();

    // Apply DISTINCT
    if query.distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new();
        result_rows.retain(|row| {
            if seen.contains(row) {
                false
            } else {
                seen.push(row.clone());
                true
            }
        });
    }

    CteData { columns: result_cols, rows: result_rows }
}

/// Materialize an aggregate CTE (GROUP BY or aggregate functions, with optional HAVING)
fn materialize_aggregate_cte(
    columns: &[parser::SelectColumn],
    rows: &[Vec<Value>],
    combined_cols: &[ResultColumn],
    group_by: &[parser::SelectColumn],
    having: Option<&parser::WhereClause>,
    storage: &Storage,
) -> CteData {
    let group_indices: Vec<usize> = group_by.iter()
        .filter_map(|c| resolve_column_index(c, combined_cols))
        .collect();

    // Group rows
    let mut group_keys: Vec<Vec<Value>> = Vec::new();
    let mut groups: Vec<Vec<&Vec<Value>>> = Vec::new();
    for row in rows {
        let key: Vec<Value> = group_indices.iter().map(|&i| row[i].clone()).collect();
        if let Some(pos) = group_keys.iter().position(|k| k == &key) {
            groups[pos].push(row);
        } else {
            group_keys.push(key);
            groups.push(vec![row]);
        }
    }
    if group_by.is_empty() {
        groups = vec![rows.iter().collect()];
    }

    // Apply HAVING filter on groups
    if let Some(wc) = having {
        groups.retain(|g| {
            let owned: Vec<Vec<Value>> = g.iter().map(|r| (*r).clone()).collect();
            evaluate_having_condition(&wc.condition, &owned, combined_cols, storage)
        });
    }

    let active_columns: Vec<&parser::SelectColumn> = columns.iter()
        .filter(|c| !matches!(c, parser::SelectColumn::All))
        .collect();

    let result_cols: Vec<ResultColumn> = active_columns.iter()
        .map(|col| ResultColumn { table: String::new(), name: select_column_name(col) })
        .collect();

    let result_rows: Vec<Vec<Value>> = groups.iter().map(|group| {
        let owned: Vec<Vec<Value>> = group.iter().map(|r| (*r).clone()).collect();
        active_columns.iter().map(|col| {
            let inner = match col {
                parser::SelectColumn::Alias(inner, _) => inner.as_ref(),
                other => *other,
            };
            let val_str = compute_column_value(inner, &owned, combined_cols);
            // Parse back to Value
            if val_str == "NULL" {
                Value::Null
            } else if let Ok(n) = val_str.parse::<i64>() {
                Value::Int(n)
            } else {
                Value::String(val_str)
            }
        }).collect()
    }).collect();

    CteData { columns: result_cols, rows: result_rows }
}

fn execute_insert_select(table_name: &str, select: &parser::SelectStatement, storage: &Storage) {
    let mut cte_map: HashMap<String, CteData> = HashMap::new();
    for cte in &select.ctes {
        let cte_data = materialize_cte(cte, storage, &cte_map);
        cte_map.insert(cte.name.clone(), cte_data);
    }

    let (combined_cols, filtered_rows) = match prepare_rows(select, storage, &cte_map) {
        Some(r) => r,
        None => return,
    };

    // Project each row according to the SELECT columns
    let empty_storage = Storage::new("/dev/null").unwrap();
    let project = |row: &Vec<Value>| -> Vec<Value> {
        match select.columns.as_slice() {
            [parser::SelectColumn::All] => row.clone(),
            cols => cols.iter().filter_map(|col| {
                match col {
                    parser::SelectColumn::Column(_) | parser::SelectColumn::QualifiedColumn(_, _) => {
                        resolve_column_index(col, &combined_cols).map(|i| row[i].clone())
                    }
                    parser::SelectColumn::Alias(inner, _) => {
                        resolve_column_index(inner, &combined_cols).map(|i| row[i].clone())
                    }
                    parser::SelectColumn::Expr(expr) => {
                        Some(resolve_join_expression(expr, row, &combined_cols, &empty_storage)
                            .unwrap_or(Value::Null))
                    }
                    parser::SelectColumn::Aggregate(_, _) | parser::SelectColumn::AggregateFiltered(_, _, _) | parser::SelectColumn::All | parser::SelectColumn::StarFromTable(_) => None,
                }
            }).collect(),
        }
    };

    let mut count = 0usize;
    for row in &filtered_rows {
        let values = project(row);
        let stmt = parser::InsertStatement {
            table_name: table_name.to_string(),
            columns: Vec::new(),
            source: parser::InsertSource::Values(vec![values]),
            on_conflict: None,
            returning: None,
        };
        match storage.insert_row(&stmt) {
            Ok(_) => count += 1,
            Err(e) => { eprintln!("Error: {}", e); return; }
        }
    }
    println!("Inserted {} row(s)", count);
}

/// Load, join, and filter rows for a SELECT statement.
/// Returns (combined_cols, filtered_rows) or None on error.
fn prepare_rows(
    stmt: &parser::SelectStatement,
    storage: &Storage,
    cte_map: &HashMap<String, CteData>,
) -> Option<(Vec<ResultColumn>, Vec<Vec<Value>>)> {
    let effective_from = from_name(&stmt.from, &stmt.from_alias);
    let hint = extract_index_hint(&stmt.where_clause);
    let hint_ref = hint.as_ref().map(|(c, v)| (c.as_str(), v));
    let (from_cols, from_rows) = match load_from_with_index(&stmt.from, &effective_from, cte_map, storage, hint_ref) {
        Ok(r) => r,
        Err(e) => { eprintln!("Error: {}", e); return None; }
    };

    let from_alias = &effective_from;
    let mut combined_cols: Vec<ResultColumn> = from_cols.into_iter()
        .map(|c| ResultColumn { table: from_alias.to_string(), name: c.name })
        .collect();
    let mut combined_rows: Vec<Vec<Value>> = from_rows;

    for join in &stmt.joins {
        // Handle LATERAL (SELECT ...) joins
        if let Some(lateral_query) = &join.lateral {
            let lateral_alias = join.alias.as_deref().unwrap_or("lateral");
            // Get column schema from first outer row, or empty if no rows
            let schema_row = combined_rows.first().cloned().unwrap_or_default();
            let (lat_schema_cols, _) = execute_lateral_subquery(lateral_query, &schema_row, &combined_cols, storage, cte_map);
            let lateral_result_cols: Vec<ResultColumn> = lat_schema_cols.iter()
                .map(|c| ResultColumn { table: lateral_alias.to_string(), name: c.name.clone() })
                .collect();
            let lat_col_count = lateral_result_cols.len();
            let new_combined_cols: Vec<ResultColumn> = combined_cols.iter()
                .chain(lateral_result_cols.iter())
                .cloned()
                .collect();
            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for outer_row in &combined_rows {
                let (_, lat_rows) = execute_lateral_subquery(lateral_query, outer_row, &combined_cols, storage, cte_map);
                match join.join_type {
                    parser::JoinType::Left => {
                        if lat_rows.is_empty() {
                            let mut row = outer_row.clone();
                            row.extend(std::iter::repeat(Value::Null).take(lat_col_count));
                            new_rows.push(row);
                        } else {
                            for lat_row in lat_rows {
                                let mut row = outer_row.clone();
                                row.extend(lat_row);
                                if join.on.as_ref().map_or(true, |c| evaluate_join_condition(c, &row, &new_combined_cols, storage)) {
                                    new_rows.push(row);
                                }
                            }
                        }
                    }
                    _ => { // INNER / CROSS
                        for lat_row in lat_rows {
                            let mut row = outer_row.clone();
                            row.extend(lat_row);
                            if join.on.as_ref().map_or(true, |c| evaluate_join_condition(c, &row, &new_combined_cols, storage)) {
                                new_rows.push(row);
                            }
                        }
                    }
                }
            }
            combined_cols = new_combined_cols;
            combined_rows = new_rows;
            continue;
        }

        let (join_cols, join_rows) = match load_table(&join.table, cte_map, storage) {
            Ok(r) => r,
            Err(e) => { eprintln!("Error: {}", e); return None; }
        };

        let join_alias = join.alias.as_deref().unwrap_or(&join.table);
        let join_result_cols: Vec<ResultColumn> = join_cols.into_iter()
            .map(|c| ResultColumn { table: join_alias.to_string(), name: c.name })
            .collect();

        let mut new_rows: Vec<Vec<Value>> = Vec::new();
        let left_col_count = combined_cols.len();

        // Determine shared column names for NATURAL JOIN or JOIN USING
        let shared_cols: Vec<String> = if matches!(join.join_type, parser::JoinType::Natural) {
            // Natural join: find columns present in both sides (case-insensitive)
            combined_cols.iter()
                .filter(|lc| join_result_cols.iter().any(|rc| rc.name.eq_ignore_ascii_case(&lc.name)))
                .map(|lc| lc.name.clone())
                .collect()
        } else if let Some(using) = &join.using {
            using.clone()
        } else {
            Vec::new()
        };

        // Build a row-level equality check for shared columns
        let check_shared = |candidate: &Vec<Value>, all_cols: &Vec<ResultColumn>| -> bool {
            for col_name in &shared_cols {
                // Find left-side index (first occurrence)
                let left_idx = all_cols[..left_col_count].iter().position(|c| c.name.eq_ignore_ascii_case(col_name));
                // Find right-side index (in the joined portion)
                let right_idx = all_cols[left_col_count..].iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .map(|i| i + left_col_count);
                match (left_idx, right_idx) {
                    (Some(li), Some(ri)) => {
                        if candidate[li] != candidate[ri] { return false; }
                    }
                    _ => return false,
                }
            }
            true
        };

        for left_row in &combined_rows {
            let mut matched = false;
            for right_row in &join_rows {
                let mut candidate: Vec<Value> = left_row.clone();
                candidate.extend(right_row.iter().cloned());

                let all_cols: Vec<ResultColumn> = combined_cols.iter()
                    .chain(join_result_cols.iter())
                    .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                    .collect();

                let matches = if !shared_cols.is_empty() {
                    // NATURAL or USING: match on shared column equality
                    check_shared(&candidate, &all_cols)
                } else {
                    match &join.on {
                        Some(cond) => evaluate_join_condition(cond, &candidate, &all_cols, storage),
                        None => true, // CROSS JOIN — no condition
                    }
                };
                if matches {
                    new_rows.push(candidate);
                    matched = true;
                }
            }
            if !matched && matches!(join.join_type, parser::JoinType::Left | parser::JoinType::Full) {
                let mut row = left_row.clone();
                row.extend(std::iter::repeat(Value::Null).take(join_result_cols.len()));
                new_rows.push(row);
            }
        }

        if matches!(join.join_type, parser::JoinType::Right | parser::JoinType::Full) {
            for right_row in &join_rows {
                let has_match = combined_rows.iter().any(|left_row| {
                    let mut candidate: Vec<Value> = left_row.clone();
                    candidate.extend(right_row.iter().cloned());
                    let all_cols: Vec<ResultColumn> = combined_cols.iter()
                        .chain(join_result_cols.iter())
                        .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                        .collect();
                    if !shared_cols.is_empty() {
                        check_shared(&candidate, &all_cols)
                    } else {
                        match &join.on {
                            Some(cond) => evaluate_join_condition(cond, &candidate, &all_cols, storage),
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

        // For NATURAL JOIN / USING, deduplicate shared columns by removing right-side duplicates
        let (final_cols, final_rows) = if !shared_cols.is_empty() {
            // Indices in the combined layout to keep (drop right-side shared cols)
            let all_cols_tmp: Vec<ResultColumn> = combined_cols.iter()
                .chain(join_result_cols.iter())
                .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                .collect();
            let drop_indices: std::collections::HashSet<usize> = (left_col_count..all_cols_tmp.len())
                .filter(|&i| shared_cols.iter().any(|s| s.eq_ignore_ascii_case(&all_cols_tmp[i].name)))
                .collect();
            let kept_cols: Vec<ResultColumn> = all_cols_tmp.iter().enumerate()
                .filter(|(i, _)| !drop_indices.contains(i))
                .map(|(_, c)| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                .collect();
            let kept_rows: Vec<Vec<Value>> = new_rows.into_iter()
                .map(|row| row.into_iter().enumerate()
                    .filter(|(i, _)| !drop_indices.contains(i))
                    .map(|(_, v)| v)
                    .collect())
                .collect();
            (kept_cols, kept_rows)
        } else {
            let all_cols_extended: Vec<ResultColumn> = combined_cols.iter()
                .chain(join_result_cols.iter())
                .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                .collect();
            (all_cols_extended, new_rows)
        };

        combined_cols = final_cols;
        combined_rows = final_rows;
    }

    let filtered_rows: Vec<Vec<Value>> = combined_rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => evaluate_join_condition(&wc.condition, row, &combined_cols, storage),
            None => true,
        })
        .collect();

    Some((combined_cols, filtered_rows))
}

/// Resolve a window spec by merging with its base named window (if any).
fn resolve_window_spec(
    spec: &parser::WindowSpec,
    window_defs: &[(String, parser::WindowSpec)],
) -> parser::WindowSpec {
    let base = spec.base_window.as_ref().and_then(|name| {
        window_defs.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, s)| s)
    });
    match base {
        None => spec.clone(),
        Some(base_spec) => {
            // Merge: spec's own clauses override/extend the base
            let partition_by = if spec.partition_by.is_empty() {
                base_spec.partition_by.clone()
            } else {
                spec.partition_by.clone()
            };
            let order_by = if spec.order_by.is_empty() {
                base_spec.order_by.clone()
            } else {
                spec.order_by.clone()
            };
            let frame = spec.frame.clone().or_else(|| base_spec.frame.clone());
            parser::WindowSpec {
                base_window: None,
                partition_by,
                order_by,
                frame,
            }
        }
    }
}

/// Compute all window function expressions in `columns`, appending results as virtual
/// columns to each row. Returns updated (select_columns, rows, combined_cols).
fn materialize_window_functions(
    columns: &[parser::SelectColumn],
    mut rows: Vec<Vec<Value>>,
    mut combined_cols: Vec<ResultColumn>,
    storage: &Storage,
    window_defs: &[(String, parser::WindowSpec)],
) -> (Vec<parser::SelectColumn>, Vec<Vec<Value>>, Vec<ResultColumn>) {
    // Collect unique window expressions with their position
    let mut win_exprs: Vec<parser::Expression> = Vec::new();
    for col in columns {
        let expr = match col {
            parser::SelectColumn::Expr(e) => Some(e.clone()),
            parser::SelectColumn::Alias(inner, _) => {
                if let parser::SelectColumn::Expr(e) = inner.as_ref() { Some(e.clone()) } else { None }
            }
            _ => None,
        };
        if let Some(e @ parser::Expression::Window(_, _)) = expr {
            if !win_exprs.contains(&e) {
                win_exprs.push(e);
            }
        }
    }

    if win_exprs.is_empty() {
        return (columns.to_vec(), rows, combined_cols);
    }

    // Compute values for each window expression and append as virtual columns
    for (win_idx, win_expr) in win_exprs.iter().enumerate() {
        if let parser::Expression::Window(func, spec) = win_expr {
            let resolved = resolve_window_spec(spec, window_defs);
            let values = compute_window_values(func, &resolved, &rows, &combined_cols, storage);
            combined_cols.push(ResultColumn {
                table: String::new(),
                name: format!("__win_{}", win_idx),
            });
            for (row, val) in rows.iter_mut().zip(values.into_iter()) {
                row.push(val);
            }
        }
    }

    // Replace Window expressions in select columns with a column reference to the virtual column
    let new_columns: Vec<parser::SelectColumn> = columns.iter().map(|col| {
        match col {
            parser::SelectColumn::Expr(e) => {
                if let Some(idx) = win_exprs.iter().position(|w| w == e) {
                    let header = format_expr(e);
                    let virtual_col = parser::SelectColumn::Column(format!("__win_{}", idx));
                    parser::SelectColumn::Alias(Box::new(virtual_col), header)
                } else {
                    col.clone()
                }
            }
            parser::SelectColumn::Alias(inner, alias) => {
                if let parser::SelectColumn::Expr(e) = inner.as_ref() {
                    if let Some(idx) = win_exprs.iter().position(|w| w == e) {
                        let virtual_col = parser::SelectColumn::Column(format!("__win_{}", idx));
                        parser::SelectColumn::Alias(Box::new(virtual_col), alias.clone())
                    } else {
                        col.clone()
                    }
                } else {
                    col.clone()
                }
            }
            _ => col.clone(),
        }
    }).collect();

    (new_columns, rows, combined_cols)
}

/// Compute frame bounds [start, end] (inclusive) for a given position in a sorted partition.
/// `sorted` is the sorted array of row indices. `pos` is the index into `sorted` (0-based).
/// Default (no frame spec) = whole partition: (0, sorted.len()-1).
#[allow(dead_code)]
fn compute_frame_bounds(
    spec: &parser::WindowSpec,
    pos: usize,
    sorted: &[usize],
    rows: &[Vec<Value>],
    cols: &[ResultColumn],
    _storage: &Storage,
) -> (usize, usize) {
    let n = sorted.len();
    let default = || (0, n.saturating_sub(1));

    let frame = match spec.frame {
        Some(ref f) => f,
        None => return default(),
    };

    // Helper: get the ORDER BY value for a given row index (or indices for multi-column ORDER BY).
    // Returns a Vec so that multi-column ORDER BY can be compared element-wise.
    let get_order_values = |row_idx: usize| -> Vec<Value> {
        spec.order_by.iter()
            .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[row_idx][i].clone()))
            .collect()
    };

    // Helper: check if `a` is within `k` units of `b` (for the first ORDER BY column only).
    // For numeric types, uses subtraction. For other types, falls back to equality.
    let within_range = |a: &Value, b: &Value, k: u64| -> bool {
        let k = k as f64;
        match (a, b) {
            (Value::Int(ai), Value::Int(bi)) => (*ai as f64 - *bi as f64).abs() <= k,
            (Value::Int(ai), Value::Float(bf)) => (*ai as f64 - *bf).abs() <= k,
            (Value::Float(af), Value::Int(bi)) => (*af - *bi as f64).abs() <= k,
            (Value::Float(af), Value::Float(bf)) => (*af - *bf).abs() <= k,
            _ => a == b,
        }
    };

    match frame.mode {
        parser::FrameMode::Rows => {
            let fs = match &frame.start {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => pos.saturating_sub(*k as usize),
                parser::FrameBound::CurrentRow => pos,
                parser::FrameBound::Following(k) => (pos + *k as usize).min(n.saturating_sub(1)),
                parser::FrameBound::UnboundedFollowing => n.saturating_sub(1),
            };
            let fe = match &frame.end {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => pos.saturating_sub(*k as usize),
                parser::FrameBound::CurrentRow => pos,
                parser::FrameBound::Following(k) => (pos + *k as usize).min(n.saturating_sub(1)),
                parser::FrameBound::UnboundedFollowing => n.saturating_sub(1),
            };
            (fs, fe)
        }
        parser::FrameMode::Range => {
            let current_vals = get_order_values(sorted[pos]);
            // For RANGE, PRECEDING/FOLLOWING offset applies to the ORDER BY value,
            // not to the row position. Compare by the first ORDER BY column.
            let fs = match &frame.start {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => {
                    let mut start = pos;
                    while start > 0 {
                        let candidate = get_order_values(sorted[start - 1]);
                        let in_range = current_vals.iter().zip(candidate.iter()).all(|(c, cand)| {
                            within_range(cand, c, *k)
                        });
                        if in_range {
                            start -= 1;
                        } else {
                            break;
                        }
                    }
                    start
                }
                parser::FrameBound::CurrentRow => pos,
                parser::FrameBound::Following(k) => {
                    let mut start = pos;
                    while start > 0 {
                        let candidate = get_order_values(sorted[start - 1]);
                        let in_range = current_vals.iter().zip(candidate.iter()).all(|(c, cand)| {
                            within_range(cand, c, *k)
                        });
                        if in_range {
                            start -= 1;
                        } else {
                            break;
                        }
                    }
                    start
                }
                parser::FrameBound::UnboundedFollowing => n.saturating_sub(1),
            };
            let fe = match &frame.end {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => {
                    let mut end = pos;
                    while end + 1 < n {
                        let candidate = get_order_values(sorted[end + 1]);
                        let in_range = current_vals.iter().zip(candidate.iter()).all(|(c, cand)| {
                            within_range(cand, c, *k)
                        });
                        if in_range {
                            end += 1;
                        } else {
                            break;
                        }
                    }
                    end
                }
                parser::FrameBound::CurrentRow => pos,
                parser::FrameBound::Following(k) => {
                    let mut end = pos;
                    while end + 1 < n {
                        let candidate = get_order_values(sorted[end + 1]);
                        let in_range = current_vals.iter().zip(candidate.iter()).all(|(c, cand)| {
                            within_range(cand, c, *k)
                        });
                        if in_range {
                            end += 1;
                        } else {
                            break;
                        }
                    }
                    end
                }
                parser::FrameBound::UnboundedFollowing => n.saturating_sub(1),
            };
            (fs, fe)
        }
        parser::FrameMode::Groups => {
            // Build peer group boundaries: a list of (start, end) indices into `sorted`.
            let mut groups: Vec<(usize, usize)> = Vec::new();
            if n > 0 {
                let mut g_start = 0;
                for i in 1..=n {
                    if i == n || get_order_values(sorted[i]) != get_order_values(sorted[i - 1]) {
                        groups.push((g_start, i - 1));
                        g_start = i;
                    }
                }
            }
            // Find which group the current row belongs to
            let current_group = groups.iter().position(|&(s, e)| s <= pos && pos <= e);
            let group_idx = match current_group {
                Some(idx) => idx,
                None => return default(),
            };
            let n_groups = groups.len();

            let gs = match &frame.start {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => {
                    let g = group_idx.saturating_sub(*k as usize);
                    groups[g].0
                }
                parser::FrameBound::CurrentRow => groups[group_idx].0,
                parser::FrameBound::Following(k) => {
                    let g = (group_idx + *k as usize).min(n_groups.saturating_sub(1));
                    groups[g].0
                }
                parser::FrameBound::UnboundedFollowing => groups[n_groups - 1].0,
            };
            let ge = match &frame.end {
                parser::FrameBound::UnboundedPreceding => 0,
                parser::FrameBound::Preceding(k) => {
                    let g = group_idx.saturating_sub(*k as usize);
                    groups[g].1
                }
                parser::FrameBound::CurrentRow => groups[group_idx].1,
                parser::FrameBound::Following(k) => {
                    let g = (group_idx + *k as usize).min(n_groups.saturating_sub(1));
                    groups[g].1
                }
                parser::FrameBound::UnboundedFollowing => groups[n_groups - 1].1,
            };
            (gs, ge)
        }
    }
}

fn compute_window_values(
    func: &parser::WindowFunc,
    spec: &parser::WindowSpec,
    rows: &[Vec<Value>],
    cols: &[ResultColumn],
    storage: &Storage,
) -> Vec<Value> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Build partition groups: map from partition key → list of original row indices
    let mut partition_keys: Vec<Vec<Value>> = Vec::new();
    let mut partitions: Vec<Vec<usize>> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let key: Vec<Value> = spec.partition_by.iter()
            .map(|e| resolve_join_expression(e, row, cols, storage).unwrap_or(Value::Null))
            .collect();
        if let Some(pos) = partition_keys.iter().position(|k| k == &key) {
            partitions[pos].push(i);
        } else {
            partition_keys.push(key);
            partitions.push(vec![i]);
        }
    }

    let mut result = vec![Value::Null; rows.len()];

    for partition_indices in &partitions {
        // Sort the indices within this partition by the ORDER BY spec
        let mut sorted = partition_indices.clone();
        sorted.sort_by(|&a, &b| {
            for ob in &spec.order_by {
                if let Some(idx) = resolve_column_index(&ob.column, cols) {
                    let ord = cmp_values(&rows[a][idx], &rows[b][idx]);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        match func {
            parser::WindowFunc::RowNumber => {
                for (rank, &orig_idx) in sorted.iter().enumerate() {
                    result[orig_idx] = Value::Int((rank + 1) as i64);
                }
            }
            parser::WindowFunc::Rank => {
                let mut cur_rank = 1usize;
                let mut prev_order_vals: Option<Vec<Value>> = None;
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let order_vals: Vec<Value> = spec.order_by.iter()
                        .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[orig_idx][i].clone()))
                        .collect();
                    if prev_order_vals.as_ref().map_or(false, |p| p == &order_vals) {
                        // tie — keep same rank
                    } else {
                        cur_rank = pos + 1;
                    }
                    result[orig_idx] = Value::Int(cur_rank as i64);
                    prev_order_vals = Some(order_vals);
                }
            }
            parser::WindowFunc::DenseRank => {
                let mut cur_rank = 0i64;
                let mut prev_order_vals: Option<Vec<Value>> = None;
                for &orig_idx in &sorted {
                    let order_vals: Vec<Value> = spec.order_by.iter()
                        .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[orig_idx][i].clone()))
                        .collect();
                    if prev_order_vals.as_ref().map_or(true, |p| p != &order_vals) {
                        cur_rank += 1;
                    }
                    result[orig_idx] = Value::Int(cur_rank);
                    prev_order_vals = Some(order_vals);
                }
            }
            parser::WindowFunc::Lag(expr, offset) => {
                let n = (*offset).max(0) as usize;
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let val = if pos >= n {
                        let src_idx = sorted[pos - n];
                        resolve_join_expression(expr, &rows[src_idx], cols, storage).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    result[orig_idx] = val;
                }
            }
            parser::WindowFunc::Lead(expr, offset) => {
                let n = (*offset).max(0) as usize;
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let val = if pos + n < sorted.len() {
                        let src_idx = sorted[pos + n];
                        resolve_join_expression(expr, &rows[src_idx], cols, storage).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    result[orig_idx] = val;
                }
            }
            parser::WindowFunc::Agg(agg_func, inner_col) => {
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let (frame_start, frame_end) = compute_frame_bounds(spec, pos, &sorted, rows, cols, storage);
                    let frame_rows: Vec<Vec<Value>> = sorted[frame_start..=frame_end]
                        .iter().map(|&i| rows[i].clone()).collect();
                    let agg_str = compute_aggregate(agg_func, inner_col, &frame_rows, cols);
                    let agg_val = if agg_str == "NULL" {
                        Value::Null
                    } else if let Ok(n) = agg_str.parse::<i64>() {
                        Value::Int(n)
                    } else if let Ok(f) = agg_str.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        Value::String(agg_str)
                    };
                    result[orig_idx] = agg_val;
                }
            }
            parser::WindowFunc::Ntile(n_expr) => {
                let n = resolve_join_expression(n_expr, &rows[sorted[0]], cols, storage)
                    .and_then(|v| if let Value::Int(n) = v { Some(n.max(1) as usize) } else { None })
                    .unwrap_or(1);
                let count = sorted.len();
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    // 1-based bucket, distributed as evenly as possible
                    let bucket = (pos * n / count) + 1;
                    result[orig_idx] = Value::Int(bucket as i64);
                }
            }
            parser::WindowFunc::PercentRank => {
                let count = sorted.len();
                let mut cur_rank = 1usize;
                let mut prev_order_vals: Option<Vec<Value>> = None;
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let order_vals: Vec<Value> = spec.order_by.iter()
                        .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[orig_idx][i].clone()))
                        .collect();
                    if prev_order_vals.as_ref().map_or(false, |p| p == &order_vals) {
                        // tie — keep same rank
                    } else {
                        cur_rank = pos + 1;
                    }
                    let pct = if count <= 1 { 0.0 } else { (cur_rank - 1) as f64 / (count - 1) as f64 };
                    result[orig_idx] = Value::Float(pct);
                    prev_order_vals = Some(order_vals);
                }
            }
            parser::WindowFunc::CumeDist => {
                let count = sorted.len();
                // For each row, cume_dist = (# rows with order_val <= this row's) / count
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let order_vals: Vec<Value> = spec.order_by.iter()
                        .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[orig_idx][i].clone()))
                        .collect();
                    // Find last position with the same order key (handles ties)
                    let last_pos = sorted.iter().rposition(|&idx| {
                        let ov: Vec<Value> = spec.order_by.iter()
                            .filter_map(|ob| resolve_column_index(&ob.column, cols).map(|i| rows[idx][i].clone()))
                            .collect();
                        ov == order_vals
                    }).unwrap_or(pos);
                    result[orig_idx] = Value::Float((last_pos + 1) as f64 / count as f64);
                }
            }
            parser::WindowFunc::FirstValue(expr) => {
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let (frame_start, _) = compute_frame_bounds(spec, pos, &sorted, rows, cols, storage);
                    let src_idx = sorted[frame_start];
                    result[orig_idx] = resolve_join_expression(expr, &rows[src_idx], cols, storage)
                        .unwrap_or(Value::Null);
                }
            }
            parser::WindowFunc::LastValue(expr) => {
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let (_, frame_end) = compute_frame_bounds(spec, pos, &sorted, rows, cols, storage);
                    let src_idx = sorted[frame_end];
                    result[orig_idx] = resolve_join_expression(expr, &rows[src_idx], cols, storage)
                        .unwrap_or(Value::Null);
                }
            }
            parser::WindowFunc::NthValue(expr, n_expr) => {
                let n = resolve_join_expression(n_expr, &rows[sorted[0]], cols, storage)
                    .and_then(|v| if let Value::Int(n) = v { Some((n - 1).max(0) as usize) } else { None })
                    .unwrap_or(0);
                for (pos, &orig_idx) in sorted.iter().enumerate() {
                    let (frame_start, frame_end) = compute_frame_bounds(spec, pos, &sorted, rows, cols, storage);
                    let frame_len = frame_end - frame_start + 1;
                    let val = if n < frame_len {
                        let src_idx = sorted[frame_start + n];
                        resolve_join_expression(expr, &rows[src_idx], cols, storage).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    };
                    result[orig_idx] = val;
                }
            }
        }
    }

    result
}

fn execute_select(stmt: &parser::SelectStatement, storage: &Storage) -> (Vec<String>, Vec<Vec<String>>) {
    // Handle FOR UPDATE: acquire lock (requires active transaction)
    if stmt.for_update {
        if let Some(table_name) = stmt.from.table_name() {
            if let Err(e) = storage.lock_for_update(table_name) {
                eprintln!("Error: {}", e);
                return (Vec::new(), Vec::new());
            }
        } else {
            eprintln!("Error: FOR UPDATE requires a table reference");
            return (Vec::new(), Vec::new());
        }
    }

    // Materialize CTEs
    let mut cte_map: HashMap<String, CteData> = HashMap::new();
    for cte in &stmt.ctes {
        let cte_data = materialize_cte(cte, storage, &cte_map);
        cte_map.insert(cte.name.clone(), cte_data);
    }

    let (combined_cols, filtered_rows) = match prepare_rows(stmt, storage, &cte_map) {
        Some(r) => r,
        None => return (Vec::new(), Vec::new()),
    };

    // Materialize window functions before projection, passing named window defs
    let (stmt_columns, filtered_rows, combined_cols) =
        materialize_window_functions(&stmt.columns, filtered_rows, combined_cols, storage, &stmt.window_defs);

    // Check if any column is an aggregate or GROUP BY is present
    let has_aggregates = stmt_columns.iter().any(|c| matches!(c, parser::SelectColumn::Aggregate(_, _)));
    let has_group_by = !stmt.group_by.is_empty();

    let (headers, mut rows) = if has_aggregates || has_group_by {
        if let Some(ref sets) = stmt.grouping_sets {
            // ROLLUP/CUBE/GROUPING SETS: run aggregation for each set and union results
            collect_grouping_sets_rows(&stmt_columns, &filtered_rows, &combined_cols, sets, stmt.having.as_ref(), &stmt.order_by, stmt.limit, stmt.offset, stmt.distinct, storage)
        } else {
            collect_aggregate_rows(&stmt_columns, &filtered_rows, &combined_cols, &stmt.group_by, stmt.having.as_ref(), &stmt.order_by, stmt.limit, stmt.offset, stmt.distinct, storage)
        }
    } else {
        collect_normal_rows(&stmt_columns, filtered_rows, &combined_cols, &stmt.order_by, stmt.limit, stmt.offset, stmt.distinct, storage)
    };

    // Handle UNION / UNION ALL / INTERSECT / EXCEPT
    if let Some((union_type, right_stmt)) = &stmt.union {
        let (_, right_rows) = execute_select(right_stmt, storage);
        match union_type {
            parser::UnionType::Union => {
                rows.extend(right_rows);
                let mut seen: Vec<Vec<String>> = Vec::new();
                rows.retain(|row| {
                    if seen.contains(row) { false } else { seen.push(row.clone()); true }
                });
            }
            parser::UnionType::UnionAll => {
                rows.extend(right_rows);
            }
            parser::UnionType::Intersect => {
                rows.retain(|r| right_rows.contains(r));
                // deduplicate
                let mut seen: Vec<Vec<String>> = Vec::new();
                rows.retain(|row| {
                    if seen.contains(row) { false } else { seen.push(row.clone()); true }
                });
            }
            parser::UnionType::IntersectAll => {
                rows.retain(|r| right_rows.contains(r));
            }
            parser::UnionType::Except => {
                rows.retain(|r| !right_rows.contains(r));
                let mut seen: Vec<Vec<String>> = Vec::new();
                rows.retain(|row| {
                    if seen.contains(row) { false } else { seen.push(row.clone()); true }
                });
            }
            parser::UnionType::ExceptAll => {
                rows.retain(|r| !right_rows.contains(r));
            }
        }
    }

    (headers, rows)
}

/// Resolve a SelectColumn to a column index in the combined result set
fn resolve_column_index(col: &parser::SelectColumn, combined_cols: &[ResultColumn]) -> Option<usize> {
    match col {
        parser::SelectColumn::Column(name) => {
            combined_cols.iter().position(|c| c.name == *name)
        }
        parser::SelectColumn::QualifiedColumn(table, name) => {
            combined_cols.iter().position(|c| c.table == *table && c.name == *name)
        }
        parser::SelectColumn::Alias(inner, _) => resolve_column_index(inner, combined_cols),
        _ => None,
    }
}

/// Build the header name for a select column
fn column_header(col: &parser::SelectColumn) -> String {
    match col {
        parser::SelectColumn::Aggregate(func, inner) => {
            let func_name = match func {
                parser::AggregateFunc::Count => "COUNT",
                parser::AggregateFunc::CountDistinct => "COUNT",
                parser::AggregateFunc::Sum => "SUM",
                parser::AggregateFunc::Avg => "AVG",
                parser::AggregateFunc::Min => "MIN",
                parser::AggregateFunc::Max => "MAX",
            };
            let inner_name = match inner.as_ref() {
                parser::SelectColumn::All => "*".to_string(),
                parser::SelectColumn::Column(n) => n.clone(),
                parser::SelectColumn::QualifiedColumn(t, n) => format!("{}.{}", t, n),
                _ => "?".to_string(),
            };
            if *func == parser::AggregateFunc::CountDistinct {
                format!("COUNT(DISTINCT {})", inner_name)
            } else {
                format!("{}({})", func_name, inner_name)
            }
        }
        parser::SelectColumn::Column(name) => name.clone(),
        parser::SelectColumn::QualifiedColumn(_, name) => name.clone(),
        parser::SelectColumn::Alias(_, alias) => alias.clone(),
        parser::SelectColumn::Expr(expr) => format_expr(expr),
        parser::SelectColumn::AggregateFiltered(func, inner, _) => {
            // Reuse same naming as Aggregate but call into column_header for the inner part
            let func_name = match func {
                parser::AggregateFunc::Count => "COUNT",
                parser::AggregateFunc::CountDistinct => "COUNT",
                parser::AggregateFunc::Sum => "SUM",
                parser::AggregateFunc::Avg => "AVG",
                parser::AggregateFunc::Min => "MIN",
                parser::AggregateFunc::Max => "MAX",
            };
            let inner_name = match inner.as_ref() {
                parser::SelectColumn::All => "*".to_string(),
                parser::SelectColumn::Column(n) => n.clone(),
                parser::SelectColumn::QualifiedColumn(t, n) => format!("{}.{}", t, n),
                _ => "?".to_string(),
            };
            format!("{}({}) FILTER", func_name, inner_name)
        }
        parser::SelectColumn::All => "*".to_string(),
        parser::SelectColumn::StarFromTable(tbl) => format!("{}.*", tbl),
    }
}

/// Compute one result value for a column given a group of rows
fn compute_column_value(
    col: &parser::SelectColumn,
    group: &[Vec<Value>],
    combined_cols: &[ResultColumn],
) -> String {
    match col {
        parser::SelectColumn::Aggregate(func, inner) => {
            compute_aggregate(func, inner, group, combined_cols)
        }
        parser::SelectColumn::Alias(inner, _) => {
            compute_column_value(inner, group, combined_cols)
        }
        parser::SelectColumn::Column(_) | parser::SelectColumn::QualifiedColumn(_, _) => {
            if let Some(idx) = resolve_column_index(col, combined_cols) {
                group.first().map(|r| format_value(&r[idx])).unwrap_or_else(|| "NULL".to_string())
            } else {
                "NULL".to_string()
            }
        }
        parser::SelectColumn::Expr(expr) => {
            if let Some(row) = group.first() {
                let empty_storage = Storage::new("/dev/null").unwrap();
                resolve_join_expression(expr, row, combined_cols, &empty_storage)
                    .map(|v| format_value(&v))
                    .unwrap_or_else(|| "NULL".to_string())
            } else {
                "NULL".to_string()
            }
        }
        parser::SelectColumn::AggregateFiltered(func, inner, filter_cond) => {
            // Filter the group rows by the FILTER (WHERE ...) condition, then aggregate
            let empty_storage = Storage::new("/dev/null").unwrap();
            let filtered_group: Vec<Vec<Value>> = group.iter()
                .filter(|row| {
                    // Wrap single row as a one-element slice to satisfy evaluate_having_condition's signature
                    evaluate_having_condition(filter_cond, std::slice::from_ref(*row), combined_cols, &empty_storage)
                })
                .cloned()
                .collect();
            let agg_col = parser::SelectColumn::Aggregate(func.clone(), inner.clone());
            compute_column_value(&agg_col, &filtered_group, combined_cols)
        }
        parser::SelectColumn::All | parser::SelectColumn::StarFromTable(_) => "".to_string(),
    }
}

/// Execute aggregation for ROLLUP/CUBE/GROUPING SETS: one pass per grouping set, union all results.
/// Columns not in the current set are replaced with NULL in the output.
fn collect_grouping_sets_rows(
    columns: &[parser::SelectColumn],
    rows: &[Vec<Value>],
    combined_cols: &[ResultColumn],
    sets: &[Vec<parser::SelectColumn>],
    having: Option<&parser::WhereClause>,
    order_by: &[parser::OrderByClause],
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
    storage: &Storage,
) -> (Vec<String>, Vec<Vec<String>>) {
    let header_names: Vec<String> = columns.iter()
        .filter(|c| !matches!(c, parser::SelectColumn::All))
        .map(|c| column_header(c))
        .collect();

    let mut all_result_rows: Vec<Vec<String>> = Vec::new();

    for set in sets {
        // For this grouping set, build a patched column list: GROUP BY columns not in set → NULL
        let set_col_names: Vec<String> = set.iter().map(|c| match c {
            parser::SelectColumn::Column(n) => n.clone(),
            parser::SelectColumn::QualifiedColumn(_, n) => n.clone(),
            _ => String::new(),
        }).collect();

        // Run aggregation for this set
        let (_, set_rows) = collect_aggregate_rows(
            columns, rows, combined_cols, set, having, &[], None, None, false, storage,
        );

        // For each result row, null out group-by columns not in this set
        let patched: Vec<Vec<String>> = set_rows.into_iter().map(|row| {
            row.into_iter().enumerate().map(|(i, val)| {
                // Find if this header corresponds to a group-by column not in this set
                let header = header_names.get(i).map(|s| s.as_str()).unwrap_or("");
                // Check if it's a simple column that's a group-by candidate
                let is_group_col = columns.iter().filter(|c| !matches!(c, parser::SelectColumn::All))
                    .nth(i)
                    .map(|c| {
                        let col_name = match c {
                            parser::SelectColumn::Column(n) => Some(n.clone()),
                            parser::SelectColumn::QualifiedColumn(_, n) => Some(n.clone()),
                            parser::SelectColumn::Alias(inner, _) => match inner.as_ref() {
                                parser::SelectColumn::Column(n) => Some(n.clone()),
                                parser::SelectColumn::QualifiedColumn(_, n) => Some(n.clone()),
                                _ => None,
                            },
                            _ => None,
                        };
                        // It's a group column if it matches any of the all-sets columns
                        col_name.map(|n| {
                            // Check if this column appears in any grouping set
                            sets.iter().any(|s| s.iter().any(|sc| match sc {
                                parser::SelectColumn::Column(sn) => sn.eq_ignore_ascii_case(&n),
                                parser::SelectColumn::QualifiedColumn(_, sn) => sn.eq_ignore_ascii_case(&n),
                                _ => false,
                            }))
                        }).unwrap_or(false)
                    })
                    .unwrap_or(false);

                if is_group_col {
                    // Check if this column is in the current set
                    let in_set = columns.iter().filter(|c| !matches!(c, parser::SelectColumn::All))
                        .nth(i)
                        .and_then(|c| match c {
                            parser::SelectColumn::Column(n) => Some(n.clone()),
                            parser::SelectColumn::QualifiedColumn(_, n) => Some(n.clone()),
                            parser::SelectColumn::Alias(inner, _) => match inner.as_ref() {
                                parser::SelectColumn::Column(n) => Some(n.clone()),
                                parser::SelectColumn::QualifiedColumn(_, n) => Some(n.clone()),
                                _ => None,
                            },
                            _ => None,
                        })
                        .map(|n| set_col_names.iter().any(|sn| sn.eq_ignore_ascii_case(&n)))
                        .unwrap_or(true);
                    if !in_set { "NULL".to_string() } else { val }
                } else {
                    let _ = header; // suppress unused warning
                    val
                }
            }).collect()
        }).collect();

        all_result_rows.extend(patched);
    }

    // Apply ORDER BY, DISTINCT, OFFSET, LIMIT
    if !order_by.is_empty() {
        all_result_rows.sort_by(|a, b| {
            for ob in order_by {
                let col_name = column_header(&ob.column);
                if let Some(idx) = header_names.iter().position(|h| *h == col_name) {
                    let ord = a[idx].cmp(&b[idx]);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if distinct {
        let mut seen: Vec<Vec<String>> = Vec::new();
        all_result_rows.retain(|row| {
            if seen.contains(row) { false } else { seen.push(row.clone()); true }
        });
    }
    if let Some(off) = offset {
        let off = off as usize;
        if off >= all_result_rows.len() { all_result_rows.clear(); } else { all_result_rows.drain(..off); }
    }
    if let Some(n) = limit { all_result_rows.truncate(n as usize); }

    (header_names, all_result_rows)
}

/// Execute a SELECT with aggregate functions, with optional GROUP BY and HAVING
fn collect_aggregate_rows(
    columns: &[parser::SelectColumn],
    rows: &[Vec<Value>],
    combined_cols: &[ResultColumn],
    group_by: &[parser::SelectColumn],
    having: Option<&parser::WhereClause>,
    order_by: &[parser::OrderByClause],
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
    storage: &Storage,
) -> (Vec<String>, Vec<Vec<String>>) {
    // Build header
    let header_names: Vec<String> = columns.iter()
        .filter(|c| !matches!(c, parser::SelectColumn::All))
        .map(|c| column_header(c))
        .collect();

    // Group the rows
    let groups: Vec<Vec<&Vec<Value>>> = if group_by.is_empty() {
        // No GROUP BY: all rows are one group
        vec![rows.iter().collect()]
    } else {
        // Resolve GROUP BY column indices (with ordinal support: GROUP BY 1, 2, ...)
        let group_indices: Vec<usize> = group_by.iter()
            .filter_map(|c| {
                if let parser::SelectColumn::Expr(parser::Expression::Literal(parser::Value::Int(n))) = c {
                    if *n >= 1 {
                        let ord = (*n - 1) as usize;
                        return columns.iter()
                            .filter(|sc| !matches!(sc, parser::SelectColumn::All))
                            .nth(ord)
                            .and_then(|inner| resolve_column_index(inner, combined_cols));
                    }
                }
                resolve_column_index(c, combined_cols)
            })
            .collect();
        // Build groups preserving insertion order
        let mut group_keys: Vec<Vec<Value>> = Vec::new();
        let mut group_map: Vec<Vec<&Vec<Value>>> = Vec::new();
        for row in rows {
            let key: Vec<Value> = group_indices.iter().map(|&i| row[i].clone()).collect();
            if let Some(pos) = group_keys.iter().position(|k| k == &key) {
                group_map[pos].push(row);
            } else {
                group_keys.push(key);
                group_map.push(vec![row]);
            }
        }
        group_map
    };

    // Apply HAVING filter on groups (post-aggregation)
    let groups: Vec<Vec<&Vec<Value>>> = match having {
        Some(wc) => groups.into_iter()
            .filter(|g| {
                let owned: Vec<Vec<Value>> = g.iter().map(|r| (*r).clone()).collect();
                evaluate_having_condition(&wc.condition, &owned, combined_cols, storage)
            })
            .collect(),
        None => groups,
    };

    // Compute result rows from groups
    let active_columns: Vec<&parser::SelectColumn> = columns.iter()
        .filter(|c| !matches!(c, parser::SelectColumn::All))
        .collect();

    let mut result_rows: Vec<Vec<String>> = groups.iter().map(|group| {
        // Convert &Vec<&Vec<Value>> to &[Vec<Value>] by collecting owned copies
        let owned: Vec<Vec<Value>> = group.iter().map(|r| (*r).clone()).collect();
        active_columns.iter()
            .map(|col| compute_column_value(col, &owned, combined_cols))
            .collect()
    }).collect();

    // Apply ORDER BY on result rows using header names to find sort column
    if !order_by.is_empty() {
        result_rows.sort_by(|a, b| {
            for ob in order_by {
                let col_name = column_header(&ob.column);
                if let Some(idx) = header_names.iter().position(|h| *h == col_name) {
                    let ord = a[idx].cmp(&b[idx]);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Apply DISTINCT
    if distinct {
        let mut seen: Vec<Vec<String>> = Vec::new();
        result_rows.retain(|row| {
            if seen.contains(row) {
                false
            } else {
                seen.push(row.clone());
                true
            }
        });
    }

    // Apply OFFSET then LIMIT
    if let Some(off) = offset {
        let off = off as usize;
        if off >= result_rows.len() {
            result_rows.clear();
        } else {
            result_rows.drain(..off);
        }
    }
    if let Some(n) = limit {
        result_rows.truncate(n as usize);
    }

    (header_names, result_rows)
}

/// Compute a single aggregate value
fn compute_aggregate(
    func: &parser::AggregateFunc,
    inner: &parser::SelectColumn,
    rows: &[Vec<Value>],
    combined_cols: &[ResultColumn],
) -> String {
    // COUNT(*) counts all rows
    if *func == parser::AggregateFunc::Count && *inner == parser::SelectColumn::All {
        return rows.len().to_string();
    }

    let col_idx = match resolve_column_index(inner, combined_cols) {
        Some(idx) => idx,
        None => return "NULL".to_string(),
    };

    // COUNT(DISTINCT col) — count unique non-null values
    if *func == parser::AggregateFunc::CountDistinct {
        let mut seen: Vec<&Value> = Vec::new();
        for row in rows {
            let v = &row[col_idx];
            if !matches!(v, Value::Null) && !seen.contains(&v) {
                seen.push(v);
            }
        }
        return seen.len().to_string();
    }

    // Collect non-null values
    let values: Vec<&Value> = rows.iter()
        .map(|r| &r[col_idx])
        .filter(|v| !matches!(v, Value::Null))
        .collect();

    match func {
        parser::AggregateFunc::Count => values.len().to_string(),
        parser::AggregateFunc::CountDistinct => unreachable!(), // handled above
        parser::AggregateFunc::Sum => {
            let has_float = values.iter().any(|v| matches!(v, Value::Float(_)));
            if has_float {
                let sum: f64 = values.iter().filter_map(|v| match v {
                    Value::Float(n) => Some(*n),
                    Value::Int(n) => Some(*n as f64),
                    _ => None,
                }).sum();
                format_value(&Value::Float(sum))
            } else {
                let sum: i64 = values.iter().filter_map(|v| match v {
                    Value::Int(n) => Some(*n),
                    _ => None,
                }).sum();
                sum.to_string()
            }
        }
        parser::AggregateFunc::Avg => {
            let nums: Vec<f64> = values.iter().filter_map(|v| match v {
                Value::Int(n) => Some(*n as f64),
                Value::Float(n) => Some(*n),
                _ => None,
            }).collect();
            if nums.is_empty() {
                "NULL".to_string()
            } else {
                let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                if avg == avg.floor() && avg.abs() < 1e15 {
                    format!("{}", avg as i64)
                } else {
                    format!("{:.2}", avg)
                }
            }
        }
        parser::AggregateFunc::Min => {
            values.iter().min_by(|a, b| cmp_values(a, b)).map(|v| format_value(v)).unwrap_or_else(|| "NULL".to_string())
        }
        parser::AggregateFunc::Max => {
            values.iter().max_by(|a, b| cmp_values(a, b)).map(|v| format_value(v)).unwrap_or_else(|| "NULL".to_string())
        }
    }
}

/// Compare two Values for ordering with NULL handling per NULLS FIRST/LAST spec
fn cmp_values_nulls(a: &Value, b: &Value, nulls_first: Option<bool>, descending: bool) -> std::cmp::Ordering {
    let null_is_first = nulls_first.unwrap_or(descending); // SQL default: NULLs first for DESC, last for ASC
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => if null_is_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
        (_, Value::Null) => if null_is_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
        _ => cmp_values(a, b),
    }
}

/// Compare two Values for ordering
fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        // Cross-type date comparisons
        (Value::Date(a), Value::Timestamp(b)) => (*a as i64 * 86400).cmp(b),
        (Value::Timestamp(a), Value::Date(b)) => a.cmp(&(*b as i64 * 86400)),
        // Allow date/string comparison via coercion
        (Value::Date(d), Value::String(s)) => {
            if let Some(sd) = parser::parse_date_str(s) { d.cmp(&sd) } else { std::cmp::Ordering::Less }
        }
        (Value::String(s), Value::Date(d)) => {
            if let Some(sd) = parser::parse_date_str(s) { sd.cmp(d) } else { std::cmp::Ordering::Greater }
        }
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

/// Execute a normal (non-aggregate) SELECT with optional ORDER BY
fn collect_normal_rows(
    columns: &[parser::SelectColumn],
    mut rows: Vec<Vec<Value>>,
    combined_cols: &[ResultColumn],
    order_by: &[parser::OrderByClause],
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
    storage: &Storage,
) -> (Vec<String>, Vec<Vec<String>>) {
    // Build alias map: alias name -> index in the projected output (for ORDER BY alias)
    let alias_map: std::collections::HashMap<String, usize> = columns.iter().enumerate()
        .filter_map(|(i, col)| {
            if let parser::SelectColumn::Alias(_, alias) = col {
                Some((alias.to_lowercase(), i))
            } else {
                None
            }
        })
        .collect();

    // Apply ORDER BY — first try raw column index, then alias map on projected rows, then expression eval
    if !order_by.is_empty() {
        // Project rows once for alias-based ordering
        let projected: Vec<Vec<Value>> = rows.iter().map(|row| {
            columns.iter().map(|col| {
                match col {
                    parser::SelectColumn::Column(_) | parser::SelectColumn::QualifiedColumn(_, _) => {
                        resolve_column_index(col, combined_cols).map(|i| row[i].clone()).unwrap_or(Value::Null)
                    }
                    parser::SelectColumn::Alias(inner, _) => {
                        resolve_column_index(inner, combined_cols).map(|i| row[i].clone()).unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }
            }).collect()
        }).collect();

        let indices: Vec<usize> = (0..rows.len()).collect();
        let mut sorted_indices = indices;
        sorted_indices.sort_by(|&ai, &bi| {
            for ob in order_by {
                // Try raw column index
                if let Some(idx) = resolve_column_index(&ob.column, combined_cols) {
                    let av = rows[ai][idx].clone();
                    let bv = rows[bi][idx].clone();
                    let ord = cmp_values_nulls(&av, &bv, ob.nulls_first, ob.descending);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                    continue;
                }
                // Try alias match on projected output
                if let parser::SelectColumn::Column(name) = &ob.column {
                    let key = name.to_lowercase();
                    if let Some(&pidx) = alias_map.get(&key) {
                        let av = projected[ai][pidx].clone();
                        let bv = projected[bi][pidx].clone();
                        let ord = cmp_values_nulls(&av, &bv, ob.nulls_first, ob.descending);
                        let ord = if ob.descending { ord.reverse() } else { ord };
                        if ord != std::cmp::Ordering::Equal { return ord; }
                        continue;
                    }
                }
                // Try expression evaluation
                if let parser::SelectColumn::Expr(expr) = &ob.column {
                    let av = resolve_join_expression(expr, &rows[ai], combined_cols, storage).unwrap_or(Value::Null);
                    let bv = resolve_join_expression(expr, &rows[bi], combined_cols, storage).unwrap_or(Value::Null);
                    let ord = cmp_values_nulls(&av, &bv, ob.nulls_first, ob.descending);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
            }
            std::cmp::Ordering::Equal
        });
        rows = sorted_indices.into_iter().map(|i| rows[i].clone()).collect();
    }

    // Build display column definitions: header name + how to get the value
    enum ColSource {
        Index(usize),
        Expr(parser::Expression),
    }
    let display_columns: Vec<(ColSource, String)> = match columns {
        [parser::SelectColumn::All] => {
            combined_cols.iter().enumerate()
                .map(|(i, c)| (ColSource::Index(i), c.name.clone()))
                .collect()
        }
        cols => {
            let mut result: Vec<(ColSource, String)> = Vec::new();
            for col in cols {
                match col {
                    parser::SelectColumn::Column(name) => {
                        if let Some(idx) = resolve_column_index(col, combined_cols) {
                            result.push((ColSource::Index(idx), name.clone()));
                        }
                    }
                    parser::SelectColumn::QualifiedColumn(_, name) => {
                        if let Some(idx) = resolve_column_index(col, combined_cols) {
                            result.push((ColSource::Index(idx), name.clone()));
                        }
                    }
                    parser::SelectColumn::Alias(inner, alias) => {
                        match inner.as_ref() {
                            parser::SelectColumn::Expr(expr) => {
                                result.push((ColSource::Expr(expr.clone()), alias.clone()));
                            }
                            _ => {
                                if let Some(idx) = resolve_column_index(inner, combined_cols) {
                                    result.push((ColSource::Index(idx), alias.clone()));
                                }
                            }
                        }
                    }
                    parser::SelectColumn::Expr(expr) => {
                        result.push((ColSource::Expr(expr.clone()), format_expr(expr)));
                    }
                    parser::SelectColumn::StarFromTable(tbl) => {
                        // Expand t.* to all columns from that table
                        for (i, c) in combined_cols.iter().enumerate() {
                            if c.table.eq_ignore_ascii_case(tbl) {
                                result.push((ColSource::Index(i), c.name.clone()));
                            }
                        }
                    }
                    parser::SelectColumn::All | parser::SelectColumn::Aggregate(_, _) | parser::SelectColumn::AggregateFiltered(_, _, _) => {}
                }
            }
            result
        }
    };

    // Helper to get a display value for a row
    let empty_storage = Storage::new("/dev/null").unwrap();
    let get_val = |row: &Vec<Value>, src: &ColSource| -> Value {
        match src {
            ColSource::Index(idx) => row[*idx].clone(),
            ColSource::Expr(expr) => {
                resolve_join_expression(expr, row, combined_cols, &empty_storage)
                    .unwrap_or(Value::Null)
            }
        }
    };

    // Apply DISTINCT
    if distinct {
        let mut seen: Vec<Vec<Value>> = Vec::new();
        rows.retain(|row| {
            let projected: Vec<Value> = display_columns.iter().map(|(src, _)| get_val(row, src)).collect();
            if seen.contains(&projected) {
                false
            } else {
                seen.push(projected);
                true
            }
        });
    }

    // Apply OFFSET then LIMIT
    if let Some(off) = offset {
        let off = off as usize;
        if off >= rows.len() {
            rows.clear();
        } else {
            rows.drain(..off);
        }
    }
    if let Some(n) = limit {
        rows.truncate(n as usize);
    }

    let headers: Vec<String> = display_columns.iter().map(|(_, name)| name.clone()).collect();
    let result_rows: Vec<Vec<String>> = rows.iter()
        .map(|row| display_columns.iter().map(|(src, _)| format_value(&get_val(row, src))).collect())
        .collect();

    (headers, result_rows)
}

/// Print a query result table to stdout
fn print_table(headers: &[String], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let header: Vec<String> = headers.iter().enumerate()
        .map(|(i, name)| format!("{:width$}", name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    for row in rows {
        let values: Vec<String> = row.iter().enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        println!("{}", values.join(" | "));
    }

    println!("({} rows)", rows.len());
}

/// Format an expression for display as a column header
fn format_expr(expr: &parser::Expression) -> String {
    match expr {
        parser::Expression::Column(name) => name.clone(),
        parser::Expression::QualifiedColumn(t, c) => format!("{}.{}", t, c),
        parser::Expression::Literal(v) => format_value(v),
        parser::Expression::BinaryOp(l, op, r) => {
            let op_str = match op {
                parser::ArithOp::Add => "+",
                parser::ArithOp::Sub => "-",
                parser::ArithOp::Mul => "*",
                parser::ArithOp::Div => "/",
                parser::ArithOp::Mod => "%",
                parser::ArithOp::Concat => "||",
                parser::ArithOp::JsonGet => "->",
                parser::ArithOp::JsonGetText => "->>",
            };
            format!("{} {} {}", format_expr(l), op_str, format_expr(r))
        }
        parser::Expression::Subquery(_) => "(subquery)".to_string(),
        parser::Expression::List(_) => "(list)".to_string(),
        parser::Expression::ScalarFunc(func, inner) => {
            let name = match func {
                parser::ScalarFunc::Upper => "upper",
                parser::ScalarFunc::Lower => "lower",
                parser::ScalarFunc::Length => "length",
                parser::ScalarFunc::Trim => "trim",
                parser::ScalarFunc::Abs => "abs",
                parser::ScalarFunc::Ceil => "ceil",
                parser::ScalarFunc::Floor => "floor",
                parser::ScalarFunc::LTrim => "ltrim",
                parser::ScalarFunc::RTrim => "rtrim",
                parser::ScalarFunc::Sqrt => "sqrt",
                parser::ScalarFunc::Sign => "sign",
                parser::ScalarFunc::Trunc => "trunc",
                parser::ScalarFunc::Reverse => "reverse",
                parser::ScalarFunc::Year => "year",
                parser::ScalarFunc::Month => "month",
                parser::ScalarFunc::Day => "day",
                parser::ScalarFunc::Hour => "hour",
                parser::ScalarFunc::Minute => "minute",
                parser::ScalarFunc::Second => "second",
                parser::ScalarFunc::DayOfWeek => "dayofweek",
                parser::ScalarFunc::DayOfYear => "dayofyear",
            };
            format!("{}({})", name, format_expr(inner))
        }
        parser::Expression::Coalesce(exprs) => {
            let args: Vec<String> = exprs.iter().map(format_expr).collect();
            format!("coalesce({})", args.join(", "))
        }
        parser::Expression::NullIf(a, b) => format!("nullif({}, {})", format_expr(a), format_expr(b)),
        parser::Expression::Round(val, places) => match places {
            Some(p) => format!("round({}, {})", format_expr(val), format_expr(p)),
            None => format!("round({})", format_expr(val)),
        },
        parser::Expression::Concat(exprs) => {
            let args: Vec<String> = exprs.iter().map(format_expr).collect();
            format!("concat({})", args.join(", "))
        }
        parser::Expression::Substr(s, start, len) => match len {
            Some(l) => format!("substr({}, {}, {})", format_expr(s), format_expr(start), format_expr(l)),
            None => format!("substr({}, {})", format_expr(s), format_expr(start)),
        },
        parser::Expression::Replace(s, from, to) => format!("replace({}, {}, {})", format_expr(s), format_expr(from), format_expr(to)),
        parser::Expression::LPad(s, len, pad) => format!("lpad({}, {}, {})", format_expr(s), format_expr(len), format_expr(pad)),
        parser::Expression::RPad(s, len, pad) => format!("rpad({}, {}, {})", format_expr(s), format_expr(len), format_expr(pad)),
        parser::Expression::Cast(inner, type_name) => format!("cast({} as {})", format_expr(inner), type_name.to_lowercase()),
        parser::Expression::Window(func, spec) => {
            let func_str = match func {
                parser::WindowFunc::RowNumber => "row_number()".to_string(),
                parser::WindowFunc::Rank => "rank()".to_string(),
                parser::WindowFunc::DenseRank => "dense_rank()".to_string(),
                parser::WindowFunc::Lag(expr, n) => format!("lag({}, {})", format_expr(expr), n),
                parser::WindowFunc::Lead(expr, n) => format!("lead({}, {})", format_expr(expr), n),
                parser::WindowFunc::Agg(agg, col) => {
                    let agg_name = match agg {
                        parser::AggregateFunc::Count => "count",
                        parser::AggregateFunc::CountDistinct => "count",
                        parser::AggregateFunc::Sum => "sum",
                        parser::AggregateFunc::Avg => "avg",
                        parser::AggregateFunc::Min => "min",
                        parser::AggregateFunc::Max => "max",
                    };
                    format!("{}({})", agg_name, column_header(col))
                }
                parser::WindowFunc::Ntile(n) => format!("ntile({})", format_expr(n)),
                parser::WindowFunc::PercentRank => "percent_rank()".to_string(),
                parser::WindowFunc::CumeDist => "cume_dist()".to_string(),
                parser::WindowFunc::FirstValue(e) => format!("first_value({})", format_expr(e)),
                parser::WindowFunc::LastValue(e) => format!("last_value({})", format_expr(e)),
                parser::WindowFunc::NthValue(e, n) => format!("nth_value({}, {})", format_expr(e), format_expr(n)),
            };
            // If it's a bare named window reference with no inline spec, format as "func OVER name"
            if let Some(ref base_name) = spec.base_window {
                if spec.partition_by.is_empty() && spec.order_by.is_empty() && spec.frame.is_none() {
                    return format!("{} over {}", func_str, base_name);
                }
            }
            let part_str = if spec.partition_by.is_empty() {
                String::new()
            } else {
                let parts: Vec<String> = spec.partition_by.iter().map(format_expr).collect();
                format!("partition by {}", parts.join(", "))
            };
            let ord_str = if spec.order_by.is_empty() {
                String::new()
            } else {
                let ords: Vec<String> = spec.order_by.iter()
                    .map(|o| format!("{}{}", column_header(&o.column), if o.descending { " desc" } else { "" }))
                    .collect();
                format!("order by {}", ords.join(", "))
            };
            let spec_str = match (part_str.is_empty(), ord_str.is_empty()) {
                (true, true) => String::new(),
                (true, false) => ord_str,
                (false, true) => part_str,
                (false, false) => format!("{} {}", part_str, ord_str),
            };
            format!("{} over ({})", func_str, spec_str)
        }
        parser::Expression::Greatest(exprs) => {
            let args: Vec<String> = exprs.iter().map(format_expr).collect();
            format!("greatest({})", args.join(", "))
        }
        parser::Expression::Least(exprs) => {
            let args: Vec<String> = exprs.iter().map(format_expr).collect();
            format!("least({})", args.join(", "))
        }
        parser::Expression::Power(base, exp) => format!("power({}, {})", format_expr(base), format_expr(exp)),
        parser::Expression::Position(needle, haystack) => format!("position({} in {})", format_expr(needle), format_expr(haystack)),
        parser::Expression::Repeat(s, n) => format!("repeat({}, {})", format_expr(s), format_expr(n)),
        parser::Expression::CurrentDate => "CURRENT_DATE".to_string(),
        parser::Expression::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
        parser::Expression::Extract(field, expr) => format!("EXTRACT({} FROM {})", field, format_expr(expr)),
        parser::Expression::DateTrunc(field, expr) => format!("DATE_TRUNC('{}', {})", field.to_lowercase(), format_expr(expr)),
        parser::Expression::DateDiff(unit, e1, e2) => format!("DATEDIFF({}, {}, {})", unit.to_lowercase(), format_expr(e1), format_expr(e2)),
        parser::Expression::DateAdd(expr, n, unit) => format!("DATEADD({}, {}, {})", unit.to_lowercase(), n, format_expr(expr)),
        parser::Expression::JsonTypeOf(inner) => format!("json_typeof({})", format_expr(inner)),
        parser::Expression::JsonArrayLength(inner) => format!("json_array_length({})", format_expr(inner)),
        parser::Expression::JsonBuildObject(pairs) => {
            let args: Vec<String> = pairs.iter()
                .flat_map(|(k, v)| vec![format_expr(k), format_expr(v)])
                .collect();
            format!("json_build_object({})", args.join(", "))
        }
        parser::Expression::JsonBuildArray(vals) => {
            let args: Vec<String> = vals.iter().map(format_expr).collect();
            format!("json_build_array({})", args.join(", "))
        }
        parser::Expression::UserFunc(name, args) => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            format!("{}({})", name, args_str.join(", "))
        }
        parser::Expression::Case(_, _) => "case".to_string(),
        parser::Expression::Aggregate(func, inner) => {
            let func_name = match func {
                parser::AggregateFunc::Count => "COUNT",
                parser::AggregateFunc::CountDistinct => "COUNT",
                parser::AggregateFunc::Sum => "SUM",
                parser::AggregateFunc::Avg => "AVG",
                parser::AggregateFunc::Min => "MIN",
                parser::AggregateFunc::Max => "MAX",
            };
            let inner_name = match inner.as_ref() {
                parser::SelectColumn::All => "*".to_string(),
                parser::SelectColumn::Column(n) => n.clone(),
                parser::SelectColumn::QualifiedColumn(t, n) => format!("{}.{}", t, n),
                _ => "?".to_string(),
            };
            format!("{}({})", func_name, inner_name)
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(n) => {
            // Use up to 6 significant decimal places, trim trailing zeros
            let s = format!("{:.6}", n);
            let s = s.trim_end_matches('0');
            let s = s.trim_end_matches('.');
            if s.contains('.') { s.to_string() } else { format!("{}.0", s) }
        }
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::String(s) | Value::Json(s) => s.clone(),
        Value::Date(d) => parser::format_date(*d),
        Value::Timestamp(ts) => parser::format_timestamp(*ts),
        Value::Null | Value::Default => "NULL".to_string(),
    }
}

fn evaluate_join_condition(
    condition: &parser::Condition,
    row: &[Value],
    cols: &[ResultColumn],
    storage: &Storage,
) -> bool {
    match condition {
        parser::Condition::And(left, right) => {
            evaluate_join_condition(left, row, cols, storage) && evaluate_join_condition(right, row, cols, storage)
        }
        parser::Condition::Or(left, right) => {
            evaluate_join_condition(left, row, cols, storage) || evaluate_join_condition(right, row, cols, storage)
        }
        parser::Condition::Not(inner) => !evaluate_join_condition(inner, row, cols, storage),
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == parser::Operator::IsNull || *operator == parser::Operator::IsNotNull {
                let left_val = resolve_join_expression(left, row, cols, storage);
                let is_null = matches!(left_val, Some(Value::Null) | None);
                return if *operator == parser::Operator::IsNull { is_null } else { !is_null };
            }

            if *operator == parser::Operator::Between || *operator == parser::Operator::NotBetween {
                let val = resolve_join_expression(left, row, cols, storage);
                let low = resolve_join_expression(right, row, cols, storage);
                let high = upper_bound.as_ref().and_then(|e| resolve_join_expression(e, row, cols, storage));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &parser::Operator::GreaterThanOrEqual, l) && compare_values(v, &parser::Operator::LessThanOrEqual, h));
                return if *operator == parser::Operator::Between { in_range } else { !in_range };
            }

            if *operator == parser::Operator::Exists || *operator == parser::Operator::NotExists {
                if let parser::Expression::Subquery(subquery) = right {
                    let subquery_values = execute_correlated_subquery(subquery, storage, row, cols);
                    let exists = !subquery_values.is_empty();
                    return if *operator == parser::Operator::NotExists { !exists } else { exists };
                }
                return false;
            }

            if *operator == parser::Operator::In || *operator == parser::Operator::NotIn {
                let left_val = resolve_join_expression(left, row, cols, storage);
                let contains = match right {
                    parser::Expression::Subquery(subquery) => {
                        left_val.map_or(false, |lv| execute_correlated_subquery(subquery, storage, row, cols).contains(&lv))
                    }
                    parser::Expression::List(exprs) => {
                        left_val.map_or(false, |lv| {
                            exprs.iter().any(|e| resolve_join_expression(e, row, cols, storage).map_or(false, |rv| rv == lv))
                        })
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::NotIn { !contains } else { contains };
            }

            if *operator == parser::Operator::Similar || *operator == parser::Operator::NotSimilar {
                let lv = resolve_join_expression(left, row, cols, storage);
                let rv = resolve_join_expression(right, row, cols, storage);
                let escape = upper_bound.as_ref().and_then(|e| resolve_join_expression(e, row, cols, storage));
                let similar = match (&lv, &rv) {
                    (Some(Value::String(s)), Some(Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = crate::storage::similar_to_regex(p, escape_char);
                        regex::Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::Similar { similar } else { !similar };
            }

            let left_val = resolve_join_expression(left, row, cols, storage);
            let right_val = resolve_join_expression(right, row, cols, storage);
            match (&left_val, &right_val) {
                (Some(l), Some(r)) => compare_values(l, operator, r),
                _ => false,
            }
        }
        parser::Condition::Unique(_) | parser::Condition::NotUnique(_) | parser::Condition::Overlaps(..) => false,
        parser::Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_join_expression(left, row, cols, storage) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            vals.iter().any(|rv| compare_values(&lv, op, rv))
        }
        parser::Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_join_expression(left, row, cols, storage) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            if vals.is_empty() { return true; }
            vals.iter().all(|rv| compare_values(&lv, op, rv))
        }
    }
}

/// Evaluate a HAVING condition over a group of rows. Aggregates are computed
/// across the whole group; bare columns resolve from the first row (assumes the
/// column is part of the GROUP BY key, like standard SQL).
fn evaluate_having_condition(
    condition: &parser::Condition,
    group: &[Vec<Value>],
    cols: &[ResultColumn],
    storage: &Storage,
) -> bool {
    match condition {
        parser::Condition::And(left, right) => {
            evaluate_having_condition(left, group, cols, storage) && evaluate_having_condition(right, group, cols, storage)
        }
        parser::Condition::Or(left, right) => {
            evaluate_having_condition(left, group, cols, storage) || evaluate_having_condition(right, group, cols, storage)
        }
        parser::Condition::Not(inner) => !evaluate_having_condition(inner, group, cols, storage),
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == parser::Operator::IsNull || *operator == parser::Operator::IsNotNull {
                let left_val = resolve_having_expression(left, group, cols, storage);
                let is_null = matches!(left_val, Some(Value::Null) | None);
                return if *operator == parser::Operator::IsNull { is_null } else { !is_null };
            }

            if *operator == parser::Operator::Between || *operator == parser::Operator::NotBetween {
                let val = resolve_having_expression(left, group, cols, storage);
                let low = resolve_having_expression(right, group, cols, storage);
                let high = upper_bound.as_ref().and_then(|e| resolve_having_expression(e, group, cols, storage));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &parser::Operator::GreaterThanOrEqual, l) && compare_values(v, &parser::Operator::LessThanOrEqual, h));
                return if *operator == parser::Operator::Between { in_range } else { !in_range };
            }

            let left_val = resolve_having_expression(left, group, cols, storage);
            let right_val = resolve_having_expression(right, group, cols, storage);
            match (&left_val, &right_val) {
                (Some(l), Some(r)) => compare_values(l, operator, r),
                _ => false,
            }
        }
        parser::Condition::Unique(_) | parser::Condition::NotUnique(_) | parser::Condition::Overlaps(..) => false,
        parser::Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_having_expression(left, group, cols, storage) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            vals.iter().any(|rv| compare_values(&lv, op, rv))
        }
        parser::Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_having_expression(left, group, cols, storage) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            if vals.is_empty() { return true; }
            vals.iter().all(|rv| compare_values(&lv, op, rv))
        }
    }
}

/// Resolve an expression in HAVING context. Aggregates compute over the group;
/// bare columns resolve from the first row of the group.
fn resolve_having_expression(
    expr: &parser::Expression,
    group: &[Vec<Value>],
    cols: &[ResultColumn],
    storage: &Storage,
) -> Option<Value> {
    match expr {
        parser::Expression::Aggregate(func, inner) => {
            let result_str = compute_aggregate(func, inner, group, cols);
            if result_str == "NULL" {
                Some(Value::Null)
            } else if let Ok(n) = result_str.parse::<i64>() {
                Some(Value::Int(n))
            } else if let Ok(f) = result_str.parse::<f64>() {
                Some(Value::Float(f))
            } else {
                Some(Value::String(result_str))
            }
        }
        parser::Expression::BinaryOp(left, op, right) => {
            let l = resolve_having_expression(left, group, cols, storage)?;
            let r = resolve_having_expression(right, group, cols, storage)?;
            eval_arith(&l, op, &r)
        }
        // For non-aggregate atoms, fall back to row-level resolution against the first row.
        _ => {
            let row = group.first()?;
            resolve_join_expression(expr, row, cols, storage)
        }
    }
}

/// Execute a subquery and return the first column's values as a list
fn execute_subquery(stmt: &parser::SelectStatement, storage: &Storage) -> Vec<Value> {
    let effective_name = from_name(&stmt.from, &stmt.from_alias);
    let empty_ctes = HashMap::new();
    let (from_cols, rows) = match load_from(&stmt.from, &effective_name, &empty_ctes, storage) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let combined_cols: Vec<ResultColumn> = from_cols.into_iter()
        .map(|c| ResultColumn { table: effective_name.clone(), name: c.name })
        .collect();

    // Filter by WHERE
    let filtered: Vec<Vec<Value>> = rows.into_iter()
        .filter(|row| {
            match &stmt.where_clause {
                Some(wc) => evaluate_join_condition(&wc.condition, row, &combined_cols, storage),
                None => true,
            }
        })
        .collect();

    // Handle aggregate subqueries (e.g. SELECT MAX(id) FROM ...)
    let has_aggregates = stmt.columns.iter().any(|c| matches!(c, parser::SelectColumn::Aggregate(_, _)));
    if has_aggregates {
        // Compute aggregate and parse the result string back into a Value
        if let parser::SelectColumn::Aggregate(func, inner) = &stmt.columns[0] {
            let result_str = compute_aggregate(func, inner, &filtered, &combined_cols);
            if result_str == "NULL" {
                return vec![Value::Null];
            }
            // Try parsing as integer first, then treat as string
            if let Ok(n) = result_str.parse::<i64>() {
                return vec![Value::Int(n)];
            }
            return vec![Value::String(result_str)];
        }
        return Vec::new();
    }

    // Extract the first selected column's values
    let col_idx = match &stmt.columns[0] {
        parser::SelectColumn::All => Some(0),
        other => resolve_column_index(other, &combined_cols),
    };

    match col_idx {
        Some(idx) => filtered.iter().map(|row| row[idx].clone()).collect(),
        None => Vec::new(),
    }
}

/// Substitute outer column references in an expression with literal values
fn substitute_outer_refs_expr(
    expr: &parser::Expression,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> parser::Expression {
    match expr {
        parser::Expression::Column(name) => {
            if let Some(idx) = outer_cols.iter().position(|c| c.name.eq_ignore_ascii_case(name)) {
                return parser::Expression::Literal(outer_row[idx].clone());
            }
            expr.clone()
        }
        parser::Expression::QualifiedColumn(table, name) => {
            if let Some(idx) = outer_cols.iter().position(|c| c.table.eq_ignore_ascii_case(table) && c.name.eq_ignore_ascii_case(name)) {
                return parser::Expression::Literal(outer_row[idx].clone());
            }
            expr.clone()
        }
        parser::Expression::BinaryOp(l, op, r) => parser::Expression::BinaryOp(
            Box::new(substitute_outer_refs_expr(l, outer_row, outer_cols)),
            op.clone(),
            Box::new(substitute_outer_refs_expr(r, outer_row, outer_cols)),
        ),
        other => other.clone(),
    }
}

/// Substitute outer column references in a condition
fn substitute_outer_refs_cond(
    cond: &parser::Condition,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> parser::Condition {
    match cond {
        parser::Condition::And(l, r) => parser::Condition::And(
            Box::new(substitute_outer_refs_cond(l, outer_row, outer_cols)),
            Box::new(substitute_outer_refs_cond(r, outer_row, outer_cols)),
        ),
        parser::Condition::Or(l, r) => parser::Condition::Or(
            Box::new(substitute_outer_refs_cond(l, outer_row, outer_cols)),
            Box::new(substitute_outer_refs_cond(r, outer_row, outer_cols)),
        ),
        parser::Condition::Not(inner) => parser::Condition::Not(
            Box::new(substitute_outer_refs_cond(inner, outer_row, outer_cols))
        ),
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            parser::Condition::Comparison {
                left: substitute_outer_refs_expr(left, outer_row, outer_cols),
                operator: operator.clone(),
                right: substitute_outer_refs_expr(right, outer_row, outer_cols),
                upper_bound: upper_bound.as_ref().map(|e| substitute_outer_refs_expr(e, outer_row, outer_cols)),
            }
        }
        other => other.clone(),
    }
}

/// Patch a lateral subquery by substituting outer row references in its WHERE clause
fn substitute_outer_refs_in_query(
    query: &parser::SelectStatement,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> parser::SelectStatement {
    let mut q = query.clone();
    if let Some(ref wc) = q.where_clause {
        q.where_clause = Some(parser::WhereClause {
            condition: substitute_outer_refs_cond(&wc.condition, outer_row, outer_cols),
        });
    }
    q
}

/// Execute a lateral subquery for a single outer row, returning (cols, rows)
fn execute_lateral_subquery(
    query: &parser::SelectStatement,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
    storage: &Storage,
    cte_map: &HashMap<String, CteData>,
) -> (Vec<ResultColumn>, Vec<Vec<Value>>) {
    let patched = substitute_outer_refs_in_query(query, outer_row, outer_cols);
    let cte_data = materialize_cte_inner(&patched, storage, cte_map);
    (cte_data.columns, cte_data.rows)
}

/// Execute a subquery with outer row context for correlated subqueries.
/// Column references not found in inner cols fall back to outer_row/outer_cols.
fn execute_correlated_subquery(
    stmt: &parser::SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> Vec<Value> {
    let effective_name = from_name(&stmt.from, &stmt.from_alias);
    let empty_ctes = HashMap::new();
    let (from_cols, rows) = match load_from(&stmt.from, &effective_name, &empty_ctes, storage) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let combined_cols: Vec<ResultColumn> = from_cols.into_iter()
        .map(|c| ResultColumn { table: effective_name.clone(), name: c.name })
        .collect();

    // Filter by WHERE, falling back to outer context for unresolved columns
    let filtered: Vec<Vec<Value>> = rows.into_iter()
        .filter(|row| {
            match &stmt.where_clause {
                Some(wc) => evaluate_correlated_condition(&wc.condition, row, &combined_cols, storage, outer_row, outer_cols),
                None => true,
            }
        })
        .collect();

    let has_aggregates = stmt.columns.iter().any(|c| matches!(c, parser::SelectColumn::Aggregate(_, _)));
    if has_aggregates {
        if let parser::SelectColumn::Aggregate(func, inner) = &stmt.columns[0] {
            let result_str = compute_aggregate(func, inner, &filtered, &combined_cols);
            if result_str == "NULL" {
                return vec![Value::Null];
            }
            if let Ok(n) = result_str.parse::<i64>() {
                return vec![Value::Int(n)];
            }
            return vec![Value::String(result_str)];
        }
        return Vec::new();
    }

    let col_idx = match &stmt.columns[0] {
        parser::SelectColumn::All => Some(0),
        other => resolve_column_index(other, &combined_cols),
    };
    match col_idx {
        Some(idx) => filtered.iter().map(|row| row[idx].clone()).collect(),
        None => Vec::new(),
    }
}

/// Evaluate a condition with outer-query row context for correlated subqueries
fn evaluate_correlated_condition(
    condition: &parser::Condition,
    row: &[Value],
    cols: &[ResultColumn],
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> bool {
    match condition {
        parser::Condition::And(left, right) => {
            evaluate_correlated_condition(left, row, cols, storage, outer_row, outer_cols)
                && evaluate_correlated_condition(right, row, cols, storage, outer_row, outer_cols)
        }
        parser::Condition::Or(left, right) => {
            evaluate_correlated_condition(left, row, cols, storage, outer_row, outer_cols)
                || evaluate_correlated_condition(right, row, cols, storage, outer_row, outer_cols)
        }
        parser::Condition::Not(inner) => !evaluate_correlated_condition(inner, row, cols, storage, outer_row, outer_cols),
        parser::Condition::Comparison { left, operator, right, upper_bound } => {
            let lv = resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols);
            let rv = resolve_correlated_expr(right, row, cols, storage, outer_row, outer_cols);
            // handle IS NULL / IS NOT NULL
            if *operator == parser::Operator::IsNull || *operator == parser::Operator::IsNotNull {
                let is_null = matches!(lv, Some(Value::Null) | None);
                return if *operator == parser::Operator::IsNull { is_null } else { !is_null };
            }
            if *operator == parser::Operator::Between || *operator == parser::Operator::NotBetween {
                let high = upper_bound.as_ref().and_then(|e| resolve_correlated_expr(e, row, cols, storage, outer_row, outer_cols));
                let in_range = matches!((&lv, &rv, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &parser::Operator::GreaterThanOrEqual, l) && compare_values(v, &parser::Operator::LessThanOrEqual, h));
                return if *operator == parser::Operator::Between { in_range } else { !in_range };
            }
            if *operator == parser::Operator::Similar || *operator == parser::Operator::NotSimilar {
                let escape = upper_bound.as_ref().and_then(|e| resolve_correlated_expr(e, row, cols, storage, outer_row, outer_cols));
                let similar = match (&lv, &rv) {
                    (Some(Value::String(s)), Some(Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = crate::storage::similar_to_regex(p, escape_char);
                        regex::Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == parser::Operator::Similar { similar } else { !similar };
            }
            match (&lv, &rv) {
                (Some(l), Some(r)) => compare_values(l, operator, r),
                _ => false,
            }
        }
        parser::Condition::Unique(_) | parser::Condition::NotUnique(_) | parser::Condition::Overlaps(..) => false,
        parser::Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            vals.iter().any(|rv| compare_values(&lv, op, rv))
        }
        parser::Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr(left, row, cols, storage, outer_row, outer_cols) { Some(v) => v, None => return false };
            let vals = execute_subquery(subquery, storage);
            if vals.is_empty() { return true; }
            vals.iter().all(|rv| compare_values(&lv, op, rv))
        }
    }
}

/// Resolve expression in correlated subquery context, falling back to outer row if column not found
fn resolve_correlated_expr(
    expr: &parser::Expression,
    row: &[Value],
    cols: &[ResultColumn],
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[ResultColumn],
) -> Option<Value> {
    match expr {
        parser::Expression::Column(name) => {
            // Try inner row first
            if let Some(idx) = cols.iter().position(|c| c.name == *name) {
                return Some(row[idx].clone());
            }
            // Fall back to outer context
            outer_cols.iter().position(|c| c.name == *name).map(|idx| outer_row[idx].clone())
        }
        parser::Expression::QualifiedColumn(table, col) => {
            // Try inner row first (qualified)
            if let Some(idx) = cols.iter().position(|c| c.table == *table && c.name == *col) {
                return Some(row[idx].clone());
            }
            // Fall back to outer context
            outer_cols.iter().position(|c| c.table == *table && c.name == *col)
                .map(|idx| outer_row[idx].clone())
        }
        // For all other expressions, delegate to resolve_join_expression using inner row
        // (simple — correlated refs are usually just column lookups)
        _ => resolve_join_expression(expr, row, cols, storage),
    }
}

/// Convert a Value to epoch days (for date arithmetic helpers)
pub(crate) fn val_to_epoch_days(v: &Value) -> Option<i32> {
    match v {
        Value::Date(d) => Some(*d),
        Value::Timestamp(ts) => Some((*ts / 86400) as i32),
        Value::String(s) => parser::parse_date_str(s),
        _ => None,
    }
}

/// Evaluate EXTRACT(field FROM value)
pub(crate) fn eval_extract(field: &str, v: Value) -> Option<Value> {
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

/// Evaluate DATE_TRUNC(unit, value)
pub(crate) fn eval_date_trunc(field: &str, v: Value) -> Option<Value> {
    let days = match &v {
        Value::Date(d) => *d,
        Value::Timestamp(ts) => (*ts / 86400) as i32,
        Value::String(s) => parser::parse_date_str(s)?,
        _ => return None,
    };
    let (y, m, _d) = parser::epoch_days_to_date(days);
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

/// Evaluate DATEDIFF(unit, v1, v2) — returns v1 - v2 in the given unit
pub(crate) fn eval_datediff(unit: &str, v1: Value, v2: Value) -> Option<Value> {
    let days1 = val_to_epoch_days(&v1)?;
    let days2 = val_to_epoch_days(&v2)?;
    let diff_days = (days1 - days2) as i64;
    let result = match unit.to_uppercase().as_str() {
        "DAY" | "DD" | "DAYS" => diff_days,
        "WEEK" | "WEEKS" => diff_days / 7,
        "MONTH" | "MONTHS" => {
            let (y1, m1, _) = parser::epoch_days_to_date(days1);
            let (y2, m2, _) = parser::epoch_days_to_date(days2);
            ((y1 - y2) * 12 + (m1 - m2)) as i64
        }
        "YEAR" | "YY" | "YEARS" => {
            let (y1, _, _) = parser::epoch_days_to_date(days1);
            let (y2, _, _) = parser::epoch_days_to_date(days2);
            (y1 - y2) as i64
        }
        "HOUR" | "HOURS" => diff_days * 24,
        "MINUTE" | "MINUTES" => diff_days * 1440,
        "SECOND" | "SECONDS" => diff_days * 86400,
        _ => return None,
    };
    Some(Value::Int(result))
}

/// Evaluate DATEADD(date, n, unit) — shift a date/timestamp by n units
pub(crate) fn eval_dateadd(v: Value, n: i64, unit: &str) -> Option<Value> {
    let secs_per_unit: i64 = match unit.to_uppercase().as_str() {
        "SECOND" | "SECONDS" => 1,
        "MINUTE" | "MINUTES" => 60,
        "HOUR" | "HOURS" => 3600,
        "DAY" | "DAYS" => 86400,
        "WEEK" | "WEEKS" => 604800,
        "MONTH" | "MONTHS" => 2592000,
        "YEAR" | "YEARS" => 31536000,
        _ => return None,
    };
    match v {
        Value::Date(d) => Some(Value::Date((d as i64 + n * secs_per_unit / 86400) as i32)),
        Value::Timestamp(ts) => Some(Value::Timestamp(ts + n * secs_per_unit)),
        _ => None,
    }
}

fn resolve_join_expression(
    expr: &parser::Expression,
    row: &[Value],
    cols: &[ResultColumn],
    storage: &Storage,
) -> Option<Value> {
    match expr {
        parser::Expression::Literal(v) => Some(v.clone()),
        parser::Expression::Column(name) => {
            cols.iter()
                .position(|c| c.name == *name)
                .map(|idx| row[idx].clone())
        }
        parser::Expression::QualifiedColumn(table, col) => {
            cols.iter()
                .position(|c| c.table == *table && c.name == *col)
                .map(|idx| row[idx].clone())
        }
        parser::Expression::Subquery(subquery) => {
            // Scalar subquery: execute and return first value
            let values = execute_subquery(subquery, storage);
            values.into_iter().next()
        }
        parser::Expression::BinaryOp(left, op, right) => {
            let left_val = resolve_join_expression(left, row, cols, storage)?;
            let right_val = resolve_join_expression(right, row, cols, storage)?;
            eval_arith(&left_val, op, &right_val)
        }
        parser::Expression::List(_) => None,
        parser::Expression::ScalarFunc(func, inner) => {
            resolve_join_expression(inner, row, cols, storage).and_then(|v| parser::apply_scalar_func(func, v))
        }
        parser::Expression::Coalesce(exprs) => {
            exprs.iter().find_map(|e| {
                let v = resolve_join_expression(e, row, cols, storage);
                match v { Some(Value::Null) | None => None, other => other }
            })
        }
        parser::Expression::NullIf(a, b) => {
            let va = resolve_join_expression(a, row, cols, storage);
            let vb = resolve_join_expression(b, row, cols, storage);
            match (&va, &vb) {
                (Some(l), Some(r)) if l == r => Some(Value::Null),
                _ => va,
            }
        }
        parser::Expression::Round(val, places) => {
            let v = resolve_join_expression(val, row, cols, storage)?;
            let p = places.as_ref().and_then(|e| resolve_join_expression(e, row, cols, storage));
            parser::apply_round(v, p)
        }
        parser::Expression::Concat(exprs) => {
            let parts: Vec<Option<Value>> = exprs.iter().map(|e| resolve_join_expression(e, row, cols, storage)).collect();
            parser::apply_concat(parts)
        }
        parser::Expression::Substr(s, start, len) => {
            let sv = resolve_join_expression(s, row, cols, storage)?;
            let startv = resolve_join_expression(start, row, cols, storage)?;
            let lenv = len.as_ref().and_then(|e| resolve_join_expression(e, row, cols, storage));
            parser::apply_substr(sv, startv, lenv)
        }
        parser::Expression::Replace(s, from, to) => {
            let sv = resolve_join_expression(s, row, cols, storage)?;
            let fv = resolve_join_expression(from, row, cols, storage)?;
            let tv = resolve_join_expression(to, row, cols, storage)?;
            parser::apply_replace(sv, fv, tv)
        }
        parser::Expression::LPad(s, len, pad) => {
            let sv = resolve_join_expression(s, row, cols, storage)?;
            let lv = resolve_join_expression(len, row, cols, storage)?;
            let pv = resolve_join_expression(pad, row, cols, storage)?;
            parser::apply_lpad(sv, lv, pv)
        }
        parser::Expression::RPad(s, len, pad) => {
            let sv = resolve_join_expression(s, row, cols, storage)?;
            let lv = resolve_join_expression(len, row, cols, storage)?;
            let pv = resolve_join_expression(pad, row, cols, storage)?;
            parser::apply_rpad(sv, lv, pv)
        }
        // Window values are precomputed before row-level resolution is called.
        parser::Expression::Window(_, _) => None,
        // Aggregates aren't valid in row-level (WHERE/JOIN ON) contexts; HAVING uses its own evaluator.
        parser::Expression::Cast(inner, type_name) => {
            let v = resolve_join_expression(inner, row, cols, storage)?;
            parser::apply_cast(v, type_name)
        }
        parser::Expression::Aggregate(_, _) => None,
        parser::Expression::Case(branches, else_expr) => {
            for (condition, result) in branches {
                if evaluate_join_condition(condition, row, cols, storage) {
                    return resolve_join_expression(result, row, cols, storage);
                }
            }
            else_expr.as_ref().and_then(|e| resolve_join_expression(e, row, cols, storage))
        }
        parser::Expression::Greatest(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_join_expression(e, row, cols, storage)).collect();
            parser::apply_greatest(args)
        }
        parser::Expression::Least(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_join_expression(e, row, cols, storage)).collect();
            parser::apply_least(args)
        }
        parser::Expression::Power(base, exp) => {
            let b = resolve_join_expression(base, row, cols, storage)?;
            let e = resolve_join_expression(exp, row, cols, storage)?;
            parser::apply_power(b, e)
        }
        parser::Expression::Position(needle, haystack) => {
            let n = resolve_join_expression(needle, row, cols, storage)?;
            let h = resolve_join_expression(haystack, row, cols, storage)?;
            parser::apply_position(n, h)
        }
        parser::Expression::Repeat(s, n) => {
            let sv = resolve_join_expression(s, row, cols, storage)?;
            let nv = resolve_join_expression(n, row, cols, storage)?;
            parser::apply_repeat(sv, nv)
        }
        // Date/time expressions
        parser::Expression::CurrentDate => Some(Value::Date(parser::current_epoch_days())),
        parser::Expression::CurrentTimestamp => Some(Value::Timestamp(parser::current_epoch_secs())),
        parser::Expression::Extract(field, expr) => {
            let v = resolve_join_expression(expr, row, cols, storage)?;
            eval_extract(field, v)
        }
        parser::Expression::DateTrunc(field, expr) => {
            let v = resolve_join_expression(expr, row, cols, storage)?;
            eval_date_trunc(field, v)
        }
        parser::Expression::DateDiff(unit, e1, e2) => {
            let v1 = resolve_join_expression(e1, row, cols, storage)?;
            let v2 = resolve_join_expression(e2, row, cols, storage)?;
            eval_datediff(unit, v1, v2)
        }
        parser::Expression::DateAdd(date_expr, n, unit) => {
            let v = resolve_join_expression(date_expr, row, cols, storage)?;
            eval_dateadd(v, *n, unit)
        }
        parser::Expression::JsonTypeOf(inner) => {
            let v = resolve_join_expression(inner, row, cols, storage)?;
            parser::apply_json_typeof(&v)
        }
        parser::Expression::JsonArrayLength(inner) => {
            let v = resolve_join_expression(inner, row, cols, storage)?;
            parser::apply_json_array_length(&v)
        }
        parser::Expression::JsonBuildObject(pairs) => {
            let resolved: Vec<(Value, Value)> = pairs.iter()
                .filter_map(|(k, v)| {
                    let kv = resolve_join_expression(k, row, cols, storage)?;
                    let vv = resolve_join_expression(v, row, cols, storage)?;
                    Some((kv, vv))
                })
                .collect();
            parser::apply_json_build_object(&resolved)
        }
        parser::Expression::JsonBuildArray(vals) => {
            let resolved: Vec<Value> = vals.iter()
                .filter_map(|v| resolve_join_expression(v, row, cols, storage))
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
                .filter_map(|a| resolve_join_expression(a, row, cols, storage))
                .collect();
            if arg_vals.len() != args.len() {
                return None;
            }
            let func_cols: Vec<ResultColumn> = func_def.params.iter()
                .map(|(n, _)| ResultColumn { table: String::new(), name: n.clone() })
                .collect();
            resolve_join_expression(&func_def.body, &arg_vals, &func_cols, storage)
        }
    }
}

/// Evaluate arithmetic on f64
fn arith_f64(l: f64, op: &parser::ArithOp, r: f64) -> Option<Value> {
    let result = match op {
        parser::ArithOp::Add => l + r,
        parser::ArithOp::Sub => l - r,
        parser::ArithOp::Mul => l * r,
        parser::ArithOp::Div => {
            if r == 0.0 { return Some(Value::Null); }
            l / r
        }
        parser::ArithOp::Mod => l % r,
        parser::ArithOp::Concat => return Some(Value::String(format!("{}{}", l, r))),
        parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText => return None,
    };
    Some(Value::Float(result))
}

/// Evaluate arithmetic operation on two Values
fn eval_arith(left: &Value, op: &parser::ArithOp, right: &Value) -> Option<Value> {
    // JSON field access operators
    if matches!(op, parser::ArithOp::JsonGet | parser::ArithOp::JsonGetText) {
        return parser::apply_json_op(left, op, right);
    }
    // Handle || concatenation across all type combinations
    if let parser::ArithOp::Concat = op {
        let ls = match left {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => parser::format_date(*d),
            Value::Timestamp(ts) => parser::format_timestamp(*ts),
            Value::Default => return None,
        };
        let rs = match right {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => parser::format_date(*d),
            Value::Timestamp(ts) => parser::format_timestamp(*ts),
            Value::Default => return None,
        };
        return Some(Value::String(ls + &rs));
    }
    match (left, right) {
        // Date +/- Int: treat int as days
        (Value::Date(d), Value::Int(n)) => match op {
            parser::ArithOp::Add => Some(Value::Date(d + *n as i32)),
            parser::ArithOp::Sub => Some(Value::Date(d - *n as i32)),
            _ => None,
        },
        // Date - Date: days between
        (Value::Date(d1), Value::Date(d2)) => match op {
            parser::ArithOp::Sub => Some(Value::Int((*d1 - *d2) as i64)),
            _ => None,
        },
        // Timestamp +/- Int: treat int as seconds
        (Value::Timestamp(ts), Value::Int(n)) => match op {
            parser::ArithOp::Add => Some(Value::Timestamp(ts + n)),
            parser::ArithOp::Sub => Some(Value::Timestamp(ts - n)),
            _ => None,
        },
        // Timestamp - Timestamp: seconds between
        (Value::Timestamp(ts1), Value::Timestamp(ts2)) => match op {
            parser::ArithOp::Sub => Some(Value::Int(ts1 - ts2)),
            _ => None,
        },
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
        (Value::Float(l), Value::Float(r)) => arith_f64(*l, op, *r),
        (Value::Int(l), Value::Float(r)) => arith_f64(*l as f64, op, *r),
        (Value::Float(l), Value::Int(r)) => arith_f64(*l, op, *r as f64),
        _ => Some(Value::Null),
    }
}

fn compare_values(left: &Value, op: &parser::Operator, right: &Value) -> bool {
    // IS DISTINCT FROM / IS NOT DISTINCT FROM: NULL is comparable
    if *op == parser::Operator::IsDistinctFrom || *op == parser::Operator::IsNotDistinctFrom {
        let distinct = match (left, right) {
            (Value::Null, Value::Null) => false,
            (Value::Null, _) | (_, Value::Null) => true,
            _ => compare_values(left, &parser::Operator::NotEquals, right),
        };
        return if *op == parser::Operator::IsDistinctFrom { distinct } else { !distinct };
    }

    // Use cmp_values for ordering-based comparisons between comparable types
    let ordering = cmp_values(left, right);
    // For types that have natural ordering, use cmp_values
    match (left, right) {
        (Value::Int(_), Value::Int(_))
        | (Value::Float(_), Value::Float(_))
        | (Value::Int(_), Value::Float(_))
        | (Value::Float(_), Value::Int(_))
        | (Value::Date(_), Value::Date(_))
        | (Value::Timestamp(_), Value::Timestamp(_))
        | (Value::Date(_), Value::Timestamp(_))
        | (Value::Timestamp(_), Value::Date(_))
        | (Value::Date(_), Value::String(_))
        | (Value::String(_), Value::Date(_)) => {
            match op {
                parser::Operator::Equals            => ordering == std::cmp::Ordering::Equal,
                parser::Operator::NotEquals         => ordering != std::cmp::Ordering::Equal,
                parser::Operator::GreaterThan       => ordering == std::cmp::Ordering::Greater,
                parser::Operator::LessThan          => ordering == std::cmp::Ordering::Less,
                parser::Operator::GreaterThanOrEqual=> ordering != std::cmp::Ordering::Less,
                parser::Operator::LessThanOrEqual   => ordering != std::cmp::Ordering::Greater,
                _ => false,
            }
        }
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
        (Value::Null, Value::Null) => match op {
            parser::Operator::Equals => true,
            parser::Operator::NotEquals => false,
            _ => false,
        },
        _ => false,
    }
}

/// SQL LIKE pattern matching: % matches any sequence, _ matches any single char
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
            // % matches zero or more characters
            for i in vi..=v.len() {
                if like_match_recursive(v, p, i, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            vi < v.len() && like_match_recursive(v, p, vi + 1, pi + 1)
        }
        c => {
            vi < v.len() && v[vi] == c && like_match_recursive(v, p, vi + 1, pi + 1)
        }
    }
}
