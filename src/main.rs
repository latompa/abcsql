mod parser;
mod storage;

use std::io::{self, Write};
use parser::{parse_sql, SqlStatement, Value};
use storage::Storage;

fn main() {
    let data_dir = std::env::args().nth(1).unwrap_or_else(|| "./data".to_string());

    let storage = match Storage::new(&data_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize storage: {}", e);
            std::process::exit(1);
        }
    };

    println!("abcsql v0.1.0");
    println!("Data directory: {}", data_dir);
    println!("Type .help for help, .quit to exit\n");

    let mut input = String::new();

    loop {
        print!("abcsql> ");
        io::stdout().flush().unwrap();

        input.clear();
        match io::stdin().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                continue;
            }
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Handle meta-commands
        if trimmed.starts_with('.') {
            handle_meta_command(trimmed, &storage);
            continue;
        }

        // Parse and execute SQL
        execute_sql(trimmed, &storage);
    }

    println!("\nGoodbye!");
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
            println!("  .quit              Exit the REPL");
            println!("  .tables            List all tables");
            println!("  .schema <table>    Show table schema");
            println!("\nSQL statements:");
            println!("  CREATE TABLE name (col TYPE, ...)");
            println!("  INSERT INTO table VALUES (val, ...)");
            println!("  SELECT * FROM table [WHERE cond]");
            println!("  UPDATE table SET col = val [WHERE cond]");
            println!("  DELETE FROM table [WHERE cond]");
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
        ".schema" => {
            if parts.len() < 2 {
                println!("Usage: .schema <table_name>");
                return;
            }
            let table_name = parts[1];
            match storage.load_schema(table_name) {
                Ok(schema) => {
                    println!("CREATE TABLE {} (", schema.table_name);
                    for (i, col) in schema.columns.iter().enumerate() {
                        let type_str = match &col.data_type {
                            parser::DataType::Int => "INT".to_string(),
                            parser::DataType::Varchar(Some(n)) => format!("VARCHAR({})", n),
                            parser::DataType::Varchar(None) => "VARCHAR".to_string(),
                        };
                        let comma = if i < schema.columns.len() - 1 { "," } else { "" };
                        println!("  {} {}{}", col.name, type_str, comma);
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

fn execute_sql(sql: &str, storage: &Storage) {
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
            match storage.insert_row(&insert_stmt) {
                Ok(_) => println!("Inserted 1 row"),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Select(select_stmt) => {
            execute_select(&select_stmt, storage);
        }
        SqlStatement::Update(update_stmt) => {
            match storage.update_rows(&update_stmt) {
                Ok(count) => println!("Updated {} row(s)", count),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
        SqlStatement::Delete(delete_stmt) => {
            match storage.delete_rows(&delete_stmt) {
                Ok(count) => println!("Deleted {} row(s)", count),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

/// A column in the combined result set, tracked by table name and column name
struct ResultColumn {
    table: String,
    name: String,
}

fn execute_select(stmt: &parser::SelectStatement, storage: &Storage) {
    // Load the FROM table
    let from_schema = match storage.load_schema(&stmt.from) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };
    let from_rows = match storage.read_rows(&stmt.from) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };

    // Build the combined column list and row set, starting with the FROM table
    let from_alias = stmt.from_alias.as_deref().unwrap_or(&stmt.from);
    let mut combined_cols: Vec<ResultColumn> = from_schema.columns.iter()
        .map(|c| ResultColumn { table: from_alias.to_string(), name: c.name.clone() })
        .collect();
    let mut combined_rows: Vec<Vec<Value>> = from_rows;

    // Process each JOIN
    for join in &stmt.joins {
        let join_schema = match storage.load_schema(&join.table) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error: {}", e);
                return;
            }
        };
        let join_rows = match storage.read_rows(&join.table) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {}", e);
                return;
            }
        };

        let join_alias = join.alias.as_deref().unwrap_or(&join.table);
        let join_result_cols: Vec<ResultColumn> = join_schema.columns.iter()
            .map(|c| ResultColumn { table: join_alias.to_string(), name: c.name.clone() })
            .collect();

        let mut new_rows: Vec<Vec<Value>> = Vec::new();
        let left_col_count = combined_cols.len();

        for left_row in &combined_rows {
            let mut matched = false;
            for right_row in &join_rows {
                // Build a candidate combined row to evaluate the ON condition
                let mut candidate: Vec<Value> = left_row.clone();
                candidate.extend(right_row.iter().cloned());

                let all_cols: Vec<ResultColumn> = combined_cols.iter()
                    .chain(join_result_cols.iter())
                    .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                    .collect();

                if evaluate_join_condition(&join.on, &candidate, &all_cols) {
                    new_rows.push(candidate);
                    matched = true;
                }
            }
            // LEFT JOIN: include unmatched left rows with NULLs for right side
            if !matched && join.join_type == parser::JoinType::Left {
                let mut row = left_row.clone();
                row.extend(std::iter::repeat(Value::Null).take(join_result_cols.len()));
                new_rows.push(row);
            }
        }

        // RIGHT JOIN: include unmatched right rows with NULLs for left side
        if join.join_type == parser::JoinType::Right {
            for right_row in &join_rows {
                let has_match = combined_rows.iter().any(|left_row| {
                    let mut candidate: Vec<Value> = left_row.clone();
                    candidate.extend(right_row.iter().cloned());
                    let all_cols: Vec<ResultColumn> = combined_cols.iter()
                        .chain(join_result_cols.iter())
                        .map(|c| ResultColumn { table: c.table.clone(), name: c.name.clone() })
                        .collect();
                    evaluate_join_condition(&join.on, &candidate, &all_cols)
                });
                if !has_match {
                    let mut row: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                    row.extend(right_row.iter().cloned());
                    new_rows.push(row);
                }
            }
        }

        combined_cols.extend(join_result_cols);
        combined_rows = new_rows;
    }

    // Filter by WHERE clause
    let filtered_rows: Vec<Vec<Value>> = combined_rows.into_iter()
        .filter(|row| {
            match &stmt.where_clause {
                Some(wc) => evaluate_join_condition(&wc.condition, row, &combined_cols),
                None => true,
            }
        })
        .collect();

    // Check if any column is an aggregate or GROUP BY is present
    let has_aggregates = stmt.columns.iter().any(|c| matches!(c, parser::SelectColumn::Aggregate(_, _)));
    let has_group_by = !stmt.group_by.is_empty();

    if has_aggregates || has_group_by {
        execute_aggregate(&stmt.columns, &filtered_rows, &combined_cols, &stmt.group_by, &stmt.order_by, stmt.limit);
    } else {
        execute_normal_select(&stmt.columns, filtered_rows, &combined_cols, &stmt.order_by, stmt.limit);
    }
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
        _ => None,
    }
}

/// Build the header name for a select column
fn column_header(col: &parser::SelectColumn) -> String {
    match col {
        parser::SelectColumn::Aggregate(func, inner) => {
            let func_name = match func {
                parser::AggregateFunc::Count => "COUNT",
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
        parser::SelectColumn::Column(name) => name.clone(),
        parser::SelectColumn::QualifiedColumn(_, name) => name.clone(),
        parser::SelectColumn::All => "*".to_string(),
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
        parser::SelectColumn::Column(_) | parser::SelectColumn::QualifiedColumn(_, _) => {
            if let Some(idx) = resolve_column_index(col, combined_cols) {
                group.first().map(|r| format_value(&r[idx])).unwrap_or_else(|| "NULL".to_string())
            } else {
                "NULL".to_string()
            }
        }
        parser::SelectColumn::All => "".to_string(),
    }
}

/// Execute a SELECT with aggregate functions, with optional GROUP BY
fn execute_aggregate(
    columns: &[parser::SelectColumn],
    rows: &[Vec<Value>],
    combined_cols: &[ResultColumn],
    group_by: &[parser::SelectColumn],
    order_by: &[parser::OrderByClause],
    limit: Option<u64>,
) {
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
        // Resolve GROUP BY column indices
        let group_indices: Vec<usize> = group_by.iter()
            .filter_map(|c| resolve_column_index(c, combined_cols))
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

    // Apply LIMIT
    if let Some(n) = limit {
        result_rows.truncate(n as usize);
    }

    if result_rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Calculate column widths and print
    let mut widths: Vec<usize> = header_names.iter().map(|h| h.len()).collect();
    for row in &result_rows {
        for (i, val) in row.iter().enumerate() {
            if val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let header: Vec<String> = header_names.iter().enumerate()
        .map(|(i, name)| format!("{:width$}", name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    for row in &result_rows {
        let vals: Vec<String> = row.iter().enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        println!("{}", vals.join(" | "));
    }

    println!("({} rows)", result_rows.len());
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

    // Collect non-null values
    let values: Vec<&Value> = rows.iter()
        .map(|r| &r[col_idx])
        .filter(|v| !matches!(v, Value::Null))
        .collect();

    match func {
        parser::AggregateFunc::Count => values.len().to_string(),
        parser::AggregateFunc::Sum => {
            let sum: i64 = values.iter().filter_map(|v| match v {
                Value::Int(n) => Some(*n),
                _ => None,
            }).sum();
            sum.to_string()
        }
        parser::AggregateFunc::Avg => {
            let nums: Vec<i64> = values.iter().filter_map(|v| match v {
                Value::Int(n) => Some(*n),
                _ => None,
            }).collect();
            if nums.is_empty() {
                "NULL".to_string()
            } else {
                let avg = nums.iter().sum::<i64>() as f64 / nums.len() as f64;
                // Show integer if whole number, otherwise 2 decimal places
                if avg == avg.floor() {
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

/// Compare two Values for ordering
fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        // Mixed types: ints before strings
        (Value::Int(_), Value::String(_)) => std::cmp::Ordering::Less,
        (Value::String(_), Value::Int(_)) => std::cmp::Ordering::Greater,
    }
}

/// Execute a normal (non-aggregate) SELECT with optional ORDER BY
fn execute_normal_select(
    columns: &[parser::SelectColumn],
    mut rows: Vec<Vec<Value>>,
    combined_cols: &[ResultColumn],
    order_by: &[parser::OrderByClause],
    limit: Option<u64>,
) {
    // Apply ORDER BY
    if !order_by.is_empty() {
        rows.sort_by(|a, b| {
            for ob in order_by {
                if let Some(idx) = resolve_column_index(&ob.column, combined_cols) {
                    let ord = cmp_values(&a[idx], &b[idx]);
                    let ord = if ob.descending { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Apply LIMIT
    if let Some(n) = limit {
        rows.truncate(n as usize);
    }

    // Determine which columns to display
    let display_columns: Vec<(usize, String)> = match columns {
        [parser::SelectColumn::All] => {
            combined_cols.iter().enumerate()
                .map(|(i, c)| (i, c.name.clone()))
                .collect()
        }
        cols => {
            cols.iter().filter_map(|col| {
                match col {
                    parser::SelectColumn::Column(name) => {
                        resolve_column_index(col, combined_cols)
                            .map(|idx| (idx, name.clone()))
                    }
                    parser::SelectColumn::QualifiedColumn(_, name) => {
                        resolve_column_index(col, combined_cols)
                            .map(|idx| (idx, name.clone()))
                    }
                    parser::SelectColumn::All | parser::SelectColumn::Aggregate(_, _) => None,
                }
            }).collect()
        }
    };

    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = display_columns.iter().map(|(_, name)| name.len()).collect();
    for row in &rows {
        for (i, (col_idx, _)) in display_columns.iter().enumerate() {
            let val_len = format_value(&row[*col_idx]).len();
            if val_len > widths[i] {
                widths[i] = val_len;
            }
        }
    }

    // Print header
    let header: Vec<String> = display_columns.iter()
        .enumerate()
        .map(|(i, (_, name))| format!("{:width$}", name, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    // Print rows
    for row in &rows {
        let values: Vec<String> = display_columns.iter()
            .enumerate()
            .map(|(i, (col_idx, _))| {
                format!("{:width$}", format_value(&row[*col_idx]), width = widths[i])
            })
            .collect();
        println!("{}", values.join(" | "));
    }

    println!("({} rows)", rows.len());
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Null => "NULL".to_string(),
    }
}

fn evaluate_join_condition(
    condition: &parser::Condition,
    row: &[Value],
    cols: &[ResultColumn],
) -> bool {
    let left_val = resolve_join_expression(&condition.left, row, cols);
    let right_val = resolve_join_expression(&condition.right, row, cols);

    match (&left_val, &right_val) {
        (Some(l), Some(r)) => compare_values(l, &condition.operator, r),
        _ => false,
    }
}

fn resolve_join_expression(
    expr: &parser::Expression,
    row: &[Value],
    cols: &[ResultColumn],
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
    }
}

fn compare_values(left: &Value, op: &parser::Operator, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => match op {
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            parser::Operator::GreaterThan => l > r,
            parser::Operator::LessThan => l < r,
            parser::Operator::GreaterThanOrEqual => l >= r,
            parser::Operator::LessThanOrEqual => l <= r,
        },
        (Value::String(l), Value::String(r)) => match op {
            parser::Operator::Equals => l == r,
            parser::Operator::NotEquals => l != r,
            parser::Operator::GreaterThan => l > r,
            parser::Operator::LessThan => l < r,
            parser::Operator::GreaterThanOrEqual => l >= r,
            parser::Operator::LessThanOrEqual => l <= r,
        },
        (Value::Null, Value::Null) => match op {
            parser::Operator::Equals => true,
            parser::Operator::NotEquals => false,
            _ => false,
        },
        _ => false,
    }
}
