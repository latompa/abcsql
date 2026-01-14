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

fn execute_select(stmt: &parser::SelectStatement, storage: &Storage) {
    // Load schema to get column names
    let schema = match storage.load_schema(&stmt.from) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };

    // Read all rows
    let rows = match storage.read_rows(&stmt.from) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };

    // Filter rows by WHERE clause if present
    let filtered_rows: Vec<&Vec<Value>> = rows.iter()
        .filter(|row| {
            match &stmt.where_clause {
                Some(wc) => evaluate_condition(&wc.condition, row, &schema.columns),
                None => true,
            }
        })
        .collect();

    // Determine which columns to display
    let display_columns: Vec<(usize, &str)> = match &stmt.columns[..] {
        [parser::SelectColumn::All] => {
            schema.columns.iter().enumerate().map(|(i, c)| (i, c.name.as_str())).collect()
        }
        cols => {
            cols.iter().filter_map(|col| {
                match col {
                    parser::SelectColumn::Column(name) |
                    parser::SelectColumn::QualifiedColumn(_, name) => {
                        schema.columns.iter().position(|c| &c.name == name)
                            .map(|idx| (idx, name.as_str()))
                    }
                    parser::SelectColumn::All => None,
                }
            }).collect()
        }
    };

    if filtered_rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = display_columns.iter().map(|(_, name)| name.len()).collect();
    for row in &filtered_rows {
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
    for row in &filtered_rows {
        let values: Vec<String> = display_columns.iter()
            .enumerate()
            .map(|(i, (col_idx, _))| {
                format!("{:width$}", format_value(&row[*col_idx]), width = widths[i])
            })
            .collect();
        println!("{}", values.join(" | "));
    }

    println!("({} rows)", filtered_rows.len());
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Null => "NULL".to_string(),
    }
}

fn evaluate_condition(
    condition: &parser::Condition,
    row: &[Value],
    schema: &[parser::ColumnDefinition],
) -> bool {
    let left_val = resolve_expression(&condition.left, row, schema);
    let right_val = resolve_expression(&condition.right, row, schema);

    match (&left_val, &right_val) {
        (Some(l), Some(r)) => compare_values(l, &condition.operator, r),
        _ => false,
    }
}

fn resolve_expression(
    expr: &parser::Expression,
    row: &[Value],
    schema: &[parser::ColumnDefinition],
) -> Option<Value> {
    match expr {
        parser::Expression::Literal(v) => Some(v.clone()),
        parser::Expression::Column(name) => {
            schema.iter()
                .position(|c| c.name == *name)
                .map(|idx| row[idx].clone())
        }
        parser::Expression::QualifiedColumn(_, col) => {
            schema.iter()
                .position(|c| c.name == *col)
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
