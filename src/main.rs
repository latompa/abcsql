mod parser;
mod storage;

use parser::{parse_sql, SqlStatement};
use storage::Storage;

fn main() {
    println!("ABCSQL - A lightweight SQL database\n");

    // Initialize storage in ./data directory
    let storage = match Storage::new("./data") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize storage: {}", e);
            return;
        }
    };

    println!("=== Testing Parser ===\n");

    // Example: Parse SQL statements
    let test_cases = vec![
        "CREATE TABLE users (id INT, name VARCHAR(255), email VARCHAR(255));",
        "CREATE TABLE orders (id INT, user_id INT, product VARCHAR(100));",
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');",
        "SELECT * FROM users",
    ];

    for sql in test_cases {
        println!("Testing: {}", sql);
        match parse_sql(sql) {
            Ok((remaining, stmt)) => {
                println!("✓ Parsed successfully!");
                if !remaining.is_empty() {
                    println!("  Remaining: '{}'", remaining);
                }
            }
            Err(e) => {
                println!("✗ Parse error: {:?}", e);
            }
        }
        println!();
    }

    println!("\n=== Testing Storage Layer ===\n");

    // Test CREATE TABLE with storage
    let create_statements = vec![
        "CREATE TABLE users (id INT, name VARCHAR(255), email VARCHAR(255));",
        "CREATE TABLE products (id INT, name VARCHAR(100), price INT);",
    ];

    for sql in create_statements {
        println!("Executing: {}", sql);
        match parse_sql(sql) {
            Ok((_, stmt)) => {
                match stmt {
                    SqlStatement::CreateTable(create_stmt) => {
                        match storage.create_table(&create_stmt) {
                            Ok(_) => {
                                println!("✓ Table '{}' created successfully!", create_stmt.table_name);
                                println!("  Schema file: ./data/{}.schema", create_stmt.table_name);
                                println!("  Data file: ./data/{}.data", create_stmt.table_name);
                            }
                            Err(e) => {
                                println!("✗ Storage error: {:?}", e);
                            }
                        }
                    }
                    _ => println!("  (Not a CREATE TABLE statement)"),
                }
            }
            Err(e) => {
                println!("✗ Parse error: {:?}", e);
            }
        }
        println!();
    }

    // Test INSERT statements
    println!("\n=== Testing INSERT Statements ===\n");

    let insert_statements = vec![
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');",
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');",
        "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');",
        "INSERT INTO products VALUES (101, 'Laptop', 999);",
        "INSERT INTO products VALUES (102, 'Mouse', 25);",
    ];

    for sql in insert_statements {
        println!("Executing: {}", sql);
        match parse_sql(sql) {
            Ok((_, stmt)) => {
                match stmt {
                    SqlStatement::Insert(insert_stmt) => {
                        match storage.insert_row(&insert_stmt) {
                            Ok(_) => {
                                println!("✓ Inserted row into '{}' table", insert_stmt.table_name);
                            }
                            Err(e) => {
                                println!("✗ Insert error: {:?}", e);
                            }
                        }
                    }
                    _ => println!("  (Not an INSERT statement)"),
                }
            }
            Err(e) => {
                println!("✗ Parse error: {:?}", e);
            }
        }
        println!();
    }

    // List all tables
    println!("\n=== Listing Tables and Data ===\n");
    match storage.list_tables() {
        Ok(tables) => {
            if tables.is_empty() {
                println!("No tables found.");
            } else {
                println!("Tables in database:");
                for table in &tables {
                    println!("\n  Table: {}", table);

                    // Load and display schema
                    if let Ok(schema) = storage.load_schema(table) {
                        println!("    Columns:");
                        for col in &schema.columns {
                            println!("      {} {:?}", col.name, col.data_type);
                        }
                    }

                    // Load and display row data
                    match storage.read_rows(table) {
                        Ok(rows) => {
                            if rows.is_empty() {
                                println!("    (No data)");
                            } else {
                                println!("    Data ({} rows):", rows.len());
                                for (i, row) in rows.iter().enumerate() {
                                    let row_str = row.iter()
                                        .map(|v| match v {
                                            parser::Value::Int(n) => n.to_string(),
                                            parser::Value::String(s) => format!("'{}'", s),
                                            parser::Value::Null => "NULL".to_string(),
                                        })
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    println!("      Row {}: [{}]", i + 1, row_str);
                                }
                            }
                        }
                        Err(e) => {
                            println!("    Error reading data: {:?}", e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error listing tables: {}", e);
        }
    }
}
