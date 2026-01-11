use std::fs;
use std::io::{self, Write as IoWrite, BufWriter, BufRead, BufReader};
use std::path::{Path, PathBuf};
use crate::parser::{CreateTableStatement, ColumnDefinition, DataType, InsertStatement, Value};

/// Storage engine for persisting tables to disk
pub struct Storage {
    data_dir: PathBuf,
}

#[derive(Debug)]
pub enum StorageError {
    IoError(io::Error),
    TableAlreadyExists(String),
    TableNotFound(String),
    InvalidSchema(String),
    ColumnCountMismatch { expected: usize, got: usize },
    TypeMismatch { column: String, expected: String, got: String },
    InvalidData(String),
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        StorageError::IoError(error)
    }
}

impl Storage {
    /// Create a new Storage instance with the specified data directory
    pub fn new<P: AsRef<Path>>(data_dir: P) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create the data directory if it doesn't exist
        if !data_dir.exists() {
            fs::create_dir_all(&data_dir)?;
        }

        Ok(Storage { data_dir })
    }

    /// Create a new table by persisting its schema to disk
    pub fn create_table(&self, stmt: &CreateTableStatement) -> Result<(), StorageError> {
        let schema_path = self.schema_path(&stmt.table_name);

        // Check if table already exists
        if schema_path.exists() {
            return Err(StorageError::TableAlreadyExists(stmt.table_name.clone()));
        }

        // Write schema file
        let mut file = fs::File::create(schema_path)?;

        // First line: table name
        writeln!(file, "{}", stmt.table_name)?;

        // Subsequent lines: column definitions
        for col in &stmt.columns {
            let type_str = data_type_to_string(&col.data_type);
            writeln!(file, "{}:{}", col.name, type_str)?;
        }

        // Create empty data file
        let data_path = self.data_path(&stmt.table_name);
        fs::File::create(data_path)?;

        Ok(())
    }

    /// Insert a row of data into a table
    pub fn insert_row(&self, stmt: &InsertStatement) -> Result<(), StorageError> {
        // Load schema to validate the insert
        let schema = self.load_schema(&stmt.table_name)?;

        // Validate column count
        if stmt.values.len() != schema.columns.len() {
            return Err(StorageError::ColumnCountMismatch {
                expected: schema.columns.len(),
                got: stmt.values.len(),
            });
        }

        // Validate types (basic validation)
        for (i, (value, col_def)) in stmt.values.iter().zip(schema.columns.iter()).enumerate() {
            validate_value_type(value, &col_def.data_type, &col_def.name)?;
        }

        // Serialize row and append to data file
        let data_path = self.data_path(&stmt.table_name);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_path)?;

        let mut writer = BufWriter::new(file);
        let row_str = serialize_row(&stmt.values);
        writeln!(writer, "{}", row_str)?;
        writer.flush()?;

        Ok(())
    }

    /// Read all rows from a table
    pub fn read_rows(&self, table_name: &str) -> Result<Vec<Vec<Value>>, StorageError> {
        if !self.table_exists(table_name) {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }

        let data_path = self.data_path(table_name);

        // If file doesn't exist or is empty, return empty vec
        if !data_path.exists() {
            return Ok(Vec::new());
        }

        let file = fs::File::open(data_path)?;
        let reader = BufReader::new(file);
        let mut rows = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row = deserialize_row(&line)?;
            rows.push(row);
        }

        Ok(rows)
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schema_path(table_name).exists()
    }

    /// Load a table's schema from disk
    pub fn load_schema(&self, table_name: &str) -> Result<CreateTableStatement, StorageError> {
        let schema_path = self.schema_path(table_name);

        if !schema_path.exists() {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }

        let content = fs::read_to_string(schema_path)?;
        let mut lines = content.lines();

        // First line should be table name
        let stored_table_name = lines.next()
            .ok_or_else(|| StorageError::InvalidSchema("Empty schema file".to_string()))?;

        if stored_table_name != table_name {
            return Err(StorageError::InvalidSchema(
                format!("Table name mismatch: expected {}, got {}", table_name, stored_table_name)
            ));
        }

        // Parse column definitions
        let mut columns = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() != 2 {
                return Err(StorageError::InvalidSchema(
                    format!("Invalid column definition: {}", line)
                ));
            }

            let col_name = parts[0].to_string();
            let data_type = parse_data_type(parts[1])?;

            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
            });
        }

        Ok(CreateTableStatement {
            table_name: table_name.to_string(),
            columns,
        })
    }

    /// List all tables in the database
    pub fn list_tables(&self) -> io::Result<Vec<String>> {
        let mut tables = Vec::new();

        if !self.data_dir.exists() {
            return Ok(tables);
        }

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(extension) = path.extension() {
                if extension == "schema" {
                    if let Some(stem) = path.file_stem() {
                        if let Some(table_name) = stem.to_str() {
                            tables.push(table_name.to_string());
                        }
                    }
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    /// Delete a table (removes both schema and data files)
    pub fn drop_table(&self, table_name: &str) -> Result<(), StorageError> {
        let schema_path = self.schema_path(table_name);
        let data_path = self.data_path(table_name);

        if !schema_path.exists() {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }

        fs::remove_file(schema_path)?;

        if data_path.exists() {
            fs::remove_file(data_path)?;
        }

        Ok(())
    }

    /// Get the path to a table's schema file
    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.schema", table_name))
    }

    /// Get the path to a table's data file
    fn data_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.data", table_name))
    }
}

/// Convert a DataType to its string representation
fn data_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Int => "INT".to_string(),
        DataType::Varchar(Some(size)) => format!("VARCHAR({})", size),
        DataType::Varchar(None) => "VARCHAR".to_string(),
    }
}

/// Parse a data type from string representation
fn parse_data_type(s: &str) -> Result<DataType, StorageError> {
    if s == "INT" {
        Ok(DataType::Int)
    } else if s == "VARCHAR" {
        Ok(DataType::Varchar(None))
    } else if s.starts_with("VARCHAR(") && s.ends_with(')') {
        let size_str = &s[8..s.len()-1];
        let size = size_str.parse::<usize>()
            .map_err(|_| StorageError::InvalidSchema(format!("Invalid VARCHAR size: {}", size_str)))?;
        Ok(DataType::Varchar(Some(size)))
    } else {
        Err(StorageError::InvalidSchema(format!("Unknown data type: {}", s)))
    }
}

/// Validate that a value matches the expected data type
fn validate_value_type(value: &Value, data_type: &DataType, column_name: &str) -> Result<(), StorageError> {
    match (value, data_type) {
        (Value::Null, _) => Ok(()), // NULL is valid for any type
        (Value::Int(_), DataType::Int) => Ok(()),
        (Value::String(_), DataType::Varchar(_)) => Ok(()),
        _ => Err(StorageError::TypeMismatch {
            column: column_name.to_string(),
            expected: format!("{:?}", data_type),
            got: format!("{:?}", value),
        }),
    }
}

/// Serialize a row to string format: TYPE:value|TYPE:value|...
/// Format: INT:123|STRING:Alice|NULL
fn serialize_row(values: &[Value]) -> String {
    values
        .iter()
        .map(|v| match v {
            Value::Int(n) => format!("INT:{}", n),
            Value::String(s) => {
                // Escape pipe and newline characters
                let escaped = s.replace('\\', "\\\\")
                    .replace('|', "\\|")
                    .replace('\n', "\\n");
                format!("STRING:{}", escaped)
            }
            Value::Null => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Deserialize a row from string format
fn deserialize_row(s: &str) -> Result<Vec<Value>, StorageError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = s.chars().peekable();
    let mut parts = Vec::new();

    // Split by unescaped pipes
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            // Escaped character - add both backslash and next char to current part
            current.push(ch);
            if let Some(next_ch) = chars.next() {
                current.push(next_ch);
            }
        } else if ch == '|' {
            // Unescaped pipe - this is a delimiter
            parts.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    // Don't forget the last part
    if !current.is_empty() || !parts.is_empty() {
        parts.push(current);
    }

    // Parse each part
    for part in parts {
        if part == "NULL" {
            values.push(Value::Null);
        } else if let Some(int_str) = part.strip_prefix("INT:") {
            let n = int_str.parse::<i64>()
                .map_err(|_| StorageError::InvalidData(format!("Invalid integer: {}", int_str)))?;
            values.push(Value::Int(n));
        } else if let Some(string_val) = part.strip_prefix("STRING:") {
            // Unescape special characters
            let unescaped = string_val
                .replace("\\n", "\n")
                .replace("\\|", "|")
                .replace("\\\\", "\\");
            values.push(Value::String(unescaped));
        } else {
            return Err(StorageError::InvalidData(format!("Invalid value format: {}", part)));
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::DataType;
    use std::fs;

    #[test]
    fn test_create_table() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_create");

        // Clean up if exists
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                },
            ],
        };

        storage.create_table(&stmt).unwrap();

        assert!(storage.table_exists("users"));

        // Clean up
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_table_already_exists() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_exists");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        };

        storage.create_table(&stmt).unwrap();

        // Try to create again
        let result = storage.create_table(&stmt);
        assert!(matches!(result, Err(StorageError::TableAlreadyExists(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_load_schema() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_load");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let stmt = CreateTableStatement {
            table_name: "products".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(100)),
                },
                ColumnDefinition {
                    name: "description".to_string(),
                    data_type: DataType::Varchar(None),
                },
            ],
        };

        storage.create_table(&stmt).unwrap();

        let loaded = storage.load_schema("products").unwrap();
        assert_eq!(loaded.table_name, "products");
        assert_eq!(loaded.columns.len(), 3);
        assert_eq!(loaded.columns[0].name, "id");
        assert_eq!(loaded.columns[1].name, "name");
        assert_eq!(loaded.columns[2].name, "description");

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_list_tables() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_list");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let users = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        };

        let orders = CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        };

        storage.create_table(&users).unwrap();
        storage.create_table(&orders).unwrap();

        let tables = storage.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_drop_table() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_drop");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let stmt = CreateTableStatement {
            table_name: "temp_table".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        };

        storage.create_table(&stmt).unwrap();
        assert!(storage.table_exists("temp_table"));

        storage.drop_table("temp_table").unwrap();
        assert!(!storage.table_exists("temp_table"));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_insert_and_read() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_insert");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        // Create table
        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                },
                ColumnDefinition {
                    name: "email".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                },
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Insert data
        let insert_stmt = InsertStatement {
            table_name: "users".to_string(),
            values: vec![
                Value::Int(1),
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
            ],
        };
        storage.insert_row(&insert_stmt).unwrap();

        // Insert more data
        let insert_stmt2 = InsertStatement {
            table_name: "users".to_string(),
            values: vec![
                Value::Int(2),
                Value::String("Bob".to_string()),
                Value::String("bob@example.com".to_string()),
            ],
        };
        storage.insert_row(&insert_stmt2).unwrap();

        // Read rows
        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 2);

        // Check first row
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));
        assert_eq!(rows[0][2], Value::String("alice@example.com".to_string()));

        // Check second row
        assert_eq!(rows[1][0], Value::Int(2));
        assert_eq!(rows[1][1], Value::String("Bob".to_string()));
        assert_eq!(rows[1][2], Value::String("bob@example.com".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_insert_with_null() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_insert_null");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "products".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(100)),
                },
                ColumnDefinition {
                    name: "description".to_string(),
                    data_type: DataType::Varchar(None),
                },
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert_stmt = InsertStatement {
            table_name: "products".to_string(),
            values: vec![
                Value::Int(1),
                Value::String("Widget".to_string()),
                Value::Null,
            ],
        };
        storage.insert_row(&insert_stmt).unwrap();

        let rows = storage.read_rows("products").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][2], Value::Null);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_insert_column_count_mismatch() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_insert_mismatch");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "test".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                },
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert with wrong number of columns
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            values: vec![Value::Int(1)], // Missing one column
        };

        let result = storage.insert_row(&insert_stmt);
        assert!(matches!(result, Err(StorageError::ColumnCountMismatch { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_insert_type_mismatch() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_insert_type");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "test".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(Some(255)),
                },
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert string into int column
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            values: vec![
                Value::String("not a number".to_string()),
                Value::String("Alice".to_string()),
            ],
        };

        let result = storage.insert_row(&insert_stmt);
        assert!(matches!(result, Err(StorageError::TypeMismatch { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_serialize_deserialize_row() {
        let values = vec![
            Value::Int(42),
            Value::String("Hello World".to_string()),
            Value::Null,
            Value::Int(-100),
        ];

        let serialized = serialize_row(&values);
        let deserialized = deserialize_row(&serialized).unwrap();

        assert_eq!(values, deserialized);
    }

    #[test]
    fn test_serialize_with_special_chars() {
        let values = vec![
            Value::String("Hello|World".to_string()), // Contains pipe
            Value::String("Line1\nLine2".to_string()), // Contains newline
            Value::String("Back\\slash".to_string()),  // Contains backslash
        ];

        let serialized = serialize_row(&values);
        let deserialized = deserialize_row(&serialized).unwrap();

        assert_eq!(values, deserialized);
    }
}
