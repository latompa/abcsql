use std::fs;
use std::io::{self, Write as IoWrite, BufWriter, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::fmt;
use std::collections::HashMap;
use crate::parser::{CreateTableStatement, CreateIndexStatement, ColumnDefinition, DataType, ForeignKeyRef, InsertStatement, UpdateStatement, DeleteStatement, AlterTableStatement, AlterAction, Value, Condition, Expression, Operator};

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
    ColumnNotFound(String),
    DuplicateKey { column: String, value: String },
    NullConstraint { column: String },
    ForeignKeyViolation { column: String, ref_table: String, ref_column: String },
    IndexAlreadyExists(String),
    IndexNotFound(String),
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        StorageError::IoError(error)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::IoError(e) => write!(f, "IO error: {}", e),
            StorageError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            StorageError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            StorageError::InvalidSchema(msg) => write!(f, "Invalid schema: {}", msg),
            StorageError::ColumnCountMismatch { expected, got } => {
                write!(f, "Column count mismatch: expected {}, got {}", expected, got)
            }
            StorageError::TypeMismatch { column, expected, got } => {
                write!(f, "Type mismatch in column '{}': expected {}, got {}", column, expected, got)
            }
            StorageError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            StorageError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            StorageError::DuplicateKey { column, value } => {
                write!(f, "Duplicate key in column '{}': {}", column, value)
            }
            StorageError::NullConstraint { column } => {
                write!(f, "NULL not allowed in PRIMARY KEY column '{}'", column)
            }
            StorageError::ForeignKeyViolation { column, ref_table, ref_column } => {
                write!(f, "Foreign key violation: '{}' references {}.{}", column, ref_table, ref_column)
            }
            StorageError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            StorageError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::IoError(e) => Some(e),
            _ => None,
        }
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

        self.write_schema_file(&stmt.table_name, &stmt.columns)?;

        // Create empty data file
        let data_path = self.data_path(&stmt.table_name);
        fs::File::create(data_path)?;

        // Initialize sequence file for auto_increment columns
        if stmt.columns.iter().any(|c| c.auto_increment) {
            let seq_path = self.seq_path(&stmt.table_name);
            fs::write(seq_path, "0")?;
        }

        Ok(())
    }

    /// Write (or overwrite) a schema file for a table
    fn write_schema_file(&self, table_name: &str, columns: &[ColumnDefinition]) -> Result<(), StorageError> {
        let schema_path = self.schema_path(table_name);
        let mut file = fs::File::create(schema_path)?;
        writeln!(file, "{}", table_name)?;
        for col in columns {
            let type_str = data_type_to_string(&col.data_type);
            let mut parts = vec![col.name.as_str(), type_str.as_str()];
            let ai = "AUTO_INCREMENT".to_string();
            let pk = "PRIMARY_KEY".to_string();
            let nn = "NOT_NULL".to_string();
            let fk = col.references.as_ref().map(|r| format!("FK={}.{}", r.table, r.column));
            let uq = "UNIQUE".to_string();
            if col.not_null { parts.push(&nn); }
            if col.unique { parts.push(&uq); }
            if col.auto_increment { parts.push(&ai); }
            if col.primary_key { parts.push(&pk); }
            if let Some(ref fk_str) = fk { parts.push(fk_str); }
            writeln!(file, "{}", parts.join(":"))?;
        }
        Ok(())
    }

    /// Insert a row of data into a table
    pub fn insert_row(&self, stmt: &InsertStatement) -> Result<(), StorageError> {
        let values = match &stmt.source {
            crate::parser::InsertSource::Values(v) => v,
            crate::parser::InsertSource::Select(_) => panic!("insert_row called with Select source — caller must resolve to values first"),
        };

        // Load schema to validate the insert
        let schema = self.load_schema(&stmt.table_name)?;

        // Validate column count
        if values.len() != schema.columns.len() {
            return Err(StorageError::ColumnCountMismatch {
                expected: schema.columns.len(),
                got: values.len(),
            });
        }

        // Build final values, filling in auto_increment where NULL is provided
        let mut final_values = values.clone();
        for (i, col_def) in schema.columns.iter().enumerate() {
            if col_def.auto_increment && final_values[i] == Value::Null {
                let next_val = self.next_auto_increment(&stmt.table_name)?;
                final_values[i] = Value::Int(next_val);
            }
        }

        // Validate types
        for (value, col_def) in final_values.iter().zip(schema.columns.iter()) {
            validate_value_type(value, &col_def.data_type, &col_def.name)?;
        }

        // Enforce NOT NULL constraints
        for (value, col_def) in final_values.iter().zip(schema.columns.iter()) {
            if col_def.not_null && *value == Value::Null {
                return Err(StorageError::NullConstraint { column: col_def.name.clone() });
            }
        }

        // Enforce primary key constraints (NOT NULL + unique)
        for (i, col_def) in schema.columns.iter().enumerate() {
            if col_def.primary_key && final_values[i] == Value::Null {
                return Err(StorageError::NullConstraint { column: col_def.name.clone() });
            }
        }

        // Enforce uniqueness for PRIMARY KEY and UNIQUE columns
        let unique_columns: Vec<(usize, &ColumnDefinition)> = schema.columns.iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key || c.unique)
            .collect();
        if !unique_columns.is_empty() {
            let existing_rows = self.read_rows(&stmt.table_name)?;
            for row in &existing_rows {
                for &(i, col_def) in &unique_columns {
                    // NULL values don't violate uniqueness
                    if final_values[i] != Value::Null && row[i] == final_values[i] {
                        return Err(StorageError::DuplicateKey {
                            column: col_def.name.clone(),
                            value: format!("{:?}", final_values[i]),
                        });
                    }
                }
            }
        }

        // Enforce unique index constraints
        self.check_unique_indexes(&stmt.table_name, &final_values)?;

        // Enforce foreign key constraints
        for (i, col_def) in schema.columns.iter().enumerate() {
            if let Some(ref fk) = col_def.references {
                if final_values[i] != Value::Null {
                    self.validate_foreign_key(&final_values[i], fk, &col_def.name)?;
                }
            }
        }

        // Serialize row and append to data file
        let data_path = self.data_path(&stmt.table_name);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(data_path)?;

        let mut writer = BufWriter::new(file);
        let row_str = serialize_row(&final_values);
        writeln!(writer, "{}", row_str)?;
        writer.flush()?;

        // Rebuild any indexes on this table
        self.rebuild_indexes_for_table(&stmt.table_name)?;

        Ok(())
    }

    /// Update rows in a table matching the WHERE condition
    pub fn update_rows(&self, stmt: &UpdateStatement) -> Result<usize, StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;

        // Validate that all columns in assignments exist and have correct types
        for assignment in &stmt.assignments {
            let col_def = schema.columns.iter()
                .find(|c| c.name == assignment.column)
                .ok_or_else(|| StorageError::ColumnNotFound(assignment.column.clone()))?;
            validate_value_type(&assignment.value, &col_def.data_type, &col_def.name)?;
            // Prevent setting NOT NULL or primary key columns to NULL
            if (col_def.not_null || col_def.primary_key) && assignment.value == Value::Null {
                return Err(StorageError::NullConstraint { column: col_def.name.clone() });
            }
        }

        // Read all existing rows
        let mut rows = self.read_rows(&stmt.table_name)?;
        let mut updated_count = 0;

        // Update matching rows
        for row in &mut rows {
            let matches = match &stmt.where_clause {
                Some(wc) => evaluate_condition(&wc.condition, row, &schema.columns),
                None => true, // No WHERE clause means update all rows
            };

            if matches {
                // Apply assignments
                for assignment in &stmt.assignments {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == assignment.column) {
                        row[col_idx] = assignment.value.clone();
                    }
                }
                updated_count += 1;
            }
        }

        // Write all rows back to file (overwrite)
        let data_path = self.data_path(&stmt.table_name);
        let file = fs::File::create(data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            let row_str = serialize_row(row);
            writeln!(writer, "{}", row_str)?;
        }
        writer.flush()?;

        self.rebuild_indexes_for_table(&stmt.table_name)?;
        Ok(updated_count)
    }

    /// Delete rows from a table matching the WHERE condition
    pub fn delete_rows(&self, stmt: &DeleteStatement) -> Result<usize, StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;

        // Read all existing rows
        let rows = self.read_rows(&stmt.table_name)?;

        // Split into rows to keep and rows to delete
        let (remaining_rows, deleted_rows): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .partition(|row| {
                match &stmt.where_clause {
                    Some(wc) => !evaluate_condition(&wc.condition, row, &schema.columns),
                    None => false,
                }
            });

        let deleted_count = deleted_rows.len();

        // Check FK constraints on deleted rows — are any referenced by child tables?
        for (i, col) in schema.columns.iter().enumerate() {
            if col.primary_key {
                let deleted_values: Vec<Value> = deleted_rows.iter().map(|r| r[i].clone()).collect();
                if !deleted_values.is_empty() {
                    self.check_fk_references(&stmt.table_name, &col.name, &deleted_values)?;
                }
            }
        }

        // Write remaining rows back to file
        let data_path = self.data_path(&stmt.table_name);
        let file = fs::File::create(data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &remaining_rows {
            let row_str = serialize_row(row);
            writeln!(writer, "{}", row_str)?;
        }
        writer.flush()?;

        self.rebuild_indexes_for_table(&stmt.table_name)?;
        Ok(deleted_count)
    }

    /// Read specific rows by row numbers (used with index lookups)
    pub fn read_rows_by_numbers(&self, table_name: &str, row_nums: &[usize]) -> Result<Vec<Vec<Value>>, StorageError> {
        if !self.table_exists(table_name) {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        let data_path = self.data_path(table_name);
        if !data_path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(data_path)?;
        let reader = BufReader::new(file);
        let mut rows = Vec::new();
        for (i, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            if row_nums.contains(&i) {
                rows.push(deserialize_row(&line)?);
            }
        }
        Ok(rows)
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
            if parts.len() < 2 {
                return Err(StorageError::InvalidSchema(
                    format!("Invalid column definition: {}", line)
                ));
            }

            let col_name = parts[0].to_string();
            let data_type = parse_data_type(parts[1])?;
            let flags: Vec<&str> = parts[2..].to_vec();
            let auto_increment = flags.contains(&"AUTO_INCREMENT");
            let primary_key = flags.contains(&"PRIMARY_KEY");
            let not_null = flags.contains(&"NOT_NULL");
            let unique = flags.contains(&"UNIQUE");
            let references = flags.iter()
                .find(|f| f.starts_with("FK="))
                .map(|f| {
                    let fk = &f[3..];
                    let dot = fk.find('.').unwrap();
                    ForeignKeyRef { table: fk[..dot].to_string(), column: fk[dot+1..].to_string() }
                });

            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
                auto_increment,
                primary_key,
                not_null,
                unique,
                references,
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
    #[allow(dead_code)]
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

        let seq_path = self.seq_path(table_name);
        if seq_path.exists() {
            fs::remove_file(seq_path)?;
        }

        // Drop all indexes for this table
        let meta = self.load_index_meta()?;
        for (idx_name, t, _, _) in &meta {
            if t == table_name {
                let idx_path = self.index_data_path(idx_name);
                if idx_path.exists() {
                    fs::remove_file(&idx_path)?;
                }
            }
        }
        // Rewrite metadata without this table's indexes
        let remaining: Vec<_> = meta.iter().filter(|(_, t, _, _)| t != table_name).collect();
        let meta_path = self.index_meta_path();
        if meta_path.exists() {
            let mut file = fs::File::create(meta_path)?;
            for (name, table, col, unique) in remaining {
                if *unique {
                    writeln!(file, "{}:{}:{}:UNIQUE", name, table, col)?;
                } else {
                    writeln!(file, "{}:{}:{}", name, table, col)?;
                }
            }
        }

        Ok(())
    }

    /// Apply an ALTER TABLE statement
    pub fn alter_table(&self, stmt: &AlterTableStatement) -> Result<(), StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;
        match &stmt.action {
            AlterAction::AddColumn(col) => self.alter_add_column(&schema, col),
            AlterAction::DropColumn(name) => self.alter_drop_column(&schema, name),
            AlterAction::RenameColumn { from, to } => self.alter_rename_column(&schema, from, to),
            AlterAction::RenameTable(new_name) => self.alter_rename_table(&stmt.table_name, new_name),
        }
    }

    fn alter_add_column(&self, schema: &CreateTableStatement, col: &ColumnDefinition) -> Result<(), StorageError> {
        if schema.columns.iter().any(|c| c.name == col.name) {
            return Err(StorageError::InvalidSchema(
                format!("column '{}' already exists in table '{}'", col.name, schema.table_name)
            ));
        }

        let rows = self.read_rows(&schema.table_name)?;

        // Can't add NOT NULL to an existing non-empty table without a default
        if col.not_null && !rows.is_empty() {
            return Err(StorageError::InvalidSchema(
                format!("cannot add NOT NULL column '{}' to non-empty table", col.name)
            ));
        }

        // Adding a UNIQUE column to a non-empty table with existing NULLs is fine
        // (NULLs don't violate uniqueness). With multiple non-NULL values we'd
        // already need defaults to populate, so this only matters once defaults exist.

        let mut new_columns = schema.columns.clone();
        new_columns.push(col.clone());

        // Rewrite data: append Null to each row
        let data_path = self.data_path(&schema.table_name);
        let file = fs::File::create(data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            let mut new_row = row.clone();
            new_row.push(Value::Null);
            writeln!(writer, "{}", serialize_row(&new_row))?;
        }
        writer.flush()?;

        self.write_schema_file(&schema.table_name, &new_columns)?;

        // Initialize sequence file if this is the first auto_increment column
        if col.auto_increment && !schema.columns.iter().any(|c| c.auto_increment) {
            let seq_path = self.seq_path(&schema.table_name);
            fs::write(seq_path, "0")?;
        }

        self.rebuild_indexes_for_table(&schema.table_name)?;
        Ok(())
    }

    fn alter_drop_column(&self, schema: &CreateTableStatement, col_name: &str) -> Result<(), StorageError> {
        let col_idx = schema.columns.iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| StorageError::ColumnNotFound(col_name.to_string()))?;

        if schema.columns.len() == 1 {
            return Err(StorageError::InvalidSchema(
                format!("cannot drop last column '{}' from table '{}'", col_name, schema.table_name)
            ));
        }

        // Block drop if another table FK-references this column
        let tables = self.list_tables().map_err(StorageError::IoError)?;
        for t in &tables {
            if t == &schema.table_name { continue; }
            let other = self.load_schema(t)?;
            for other_col in &other.columns {
                if let Some(ref fk) = other_col.references {
                    if fk.table == schema.table_name && fk.column == col_name {
                        return Err(StorageError::InvalidSchema(
                            format!("cannot drop '{}.{}': referenced by '{}.{}'", schema.table_name, col_name, t, other_col.name)
                        ));
                    }
                }
            }
        }

        // Drop indexes on this column
        let meta = self.load_index_meta()?;
        for (idx_name, t, c, _) in &meta {
            if t == &schema.table_name && c == col_name {
                self.drop_index(idx_name)?;
            }
        }

        // Rewrite data without the dropped column
        let rows = self.read_rows(&schema.table_name)?;
        let data_path = self.data_path(&schema.table_name);
        let file = fs::File::create(data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            let new_row: Vec<Value> = row.iter().enumerate()
                .filter(|(i, _)| *i != col_idx)
                .map(|(_, v)| v.clone())
                .collect();
            writeln!(writer, "{}", serialize_row(&new_row))?;
        }
        writer.flush()?;

        let new_columns: Vec<ColumnDefinition> = schema.columns.iter()
            .filter(|c| c.name != col_name)
            .cloned()
            .collect();
        self.write_schema_file(&schema.table_name, &new_columns)?;

        // Remove sequence file if no auto_increment columns remain
        let dropped_col = &schema.columns[col_idx];
        if dropped_col.auto_increment && !new_columns.iter().any(|c| c.auto_increment) {
            let seq_path = self.seq_path(&schema.table_name);
            if seq_path.exists() {
                fs::remove_file(seq_path)?;
            }
        }

        self.rebuild_indexes_for_table(&schema.table_name)?;
        Ok(())
    }

    fn alter_rename_column(&self, schema: &CreateTableStatement, from: &str, to: &str) -> Result<(), StorageError> {
        if !schema.columns.iter().any(|c| c.name == from) {
            return Err(StorageError::ColumnNotFound(from.to_string()));
        }
        if schema.columns.iter().any(|c| c.name == to) {
            return Err(StorageError::InvalidSchema(
                format!("column '{}' already exists in table '{}'", to, schema.table_name)
            ));
        }

        // Rewrite this table's schema with the renamed column
        let new_columns: Vec<ColumnDefinition> = schema.columns.iter()
            .map(|c| if c.name == from {
                let mut nc = c.clone();
                nc.name = to.to_string();
                nc
            } else {
                c.clone()
            })
            .collect();
        self.write_schema_file(&schema.table_name, &new_columns)?;

        // Update FK references in other tables
        let tables = self.list_tables().map_err(StorageError::IoError)?;
        for t in &tables {
            if t == &schema.table_name { continue; }
            let other = self.load_schema(t)?;
            let mut changed = false;
            let updated: Vec<ColumnDefinition> = other.columns.iter()
                .map(|c| {
                    if let Some(ref fk) = c.references {
                        if fk.table == schema.table_name && fk.column == from {
                            let mut nc = c.clone();
                            nc.references = Some(ForeignKeyRef {
                                table: fk.table.clone(),
                                column: to.to_string(),
                            });
                            changed = true;
                            return nc;
                        }
                    }
                    c.clone()
                })
                .collect();
            if changed {
                self.write_schema_file(t, &updated)?;
            }
        }

        // Update index metadata column entries
        let meta = self.load_index_meta()?;
        let updated_meta: Vec<_> = meta.iter()
            .map(|(name, t, c, u)| {
                if t == &schema.table_name && c == from {
                    (name.clone(), t.clone(), to.to_string(), *u)
                } else {
                    (name.clone(), t.clone(), c.clone(), *u)
                }
            })
            .collect();
        self.write_index_meta(&updated_meta)?;

        Ok(())
    }

    fn alter_rename_table(&self, old_name: &str, new_name: &str) -> Result<(), StorageError> {
        if old_name == new_name {
            return Ok(());
        }
        if self.table_exists(new_name) {
            return Err(StorageError::TableAlreadyExists(new_name.to_string()));
        }

        // Rewrite schema with new table name (first line) at the new path
        let schema = self.load_schema(old_name)?;
        self.write_schema_file(new_name, &schema.columns)?;
        fs::remove_file(self.schema_path(old_name))?;

        // Rename data file
        let old_data = self.data_path(old_name);
        let new_data = self.data_path(new_name);
        if old_data.exists() {
            fs::rename(old_data, new_data)?;
        }

        // Rename sequence file
        let old_seq = self.seq_path(old_name);
        let new_seq = self.seq_path(new_name);
        if old_seq.exists() {
            fs::rename(old_seq, new_seq)?;
        }

        // Update index metadata: any index entries owned by old_name now belong to new_name
        let meta = self.load_index_meta()?;
        let updated: Vec<_> = meta.iter()
            .map(|(name, t, c, u)| {
                let new_t = if t == old_name { new_name.to_string() } else { t.clone() };
                (name.clone(), new_t, c.clone(), *u)
            })
            .collect();
        self.write_index_meta(&updated)?;

        // Update FK references in other tables
        let tables = self.list_tables().map_err(StorageError::IoError)?;
        for t in &tables {
            if t == new_name { continue; }
            let other = self.load_schema(t)?;
            let mut changed = false;
            let updated_cols: Vec<ColumnDefinition> = other.columns.iter()
                .map(|c| {
                    if let Some(ref fk) = c.references {
                        if fk.table == old_name {
                            let mut nc = c.clone();
                            nc.references = Some(ForeignKeyRef {
                                table: new_name.to_string(),
                                column: fk.column.clone(),
                            });
                            changed = true;
                            return nc;
                        }
                    }
                    c.clone()
                })
                .collect();
            if changed {
                self.write_schema_file(t, &updated_cols)?;
            }
        }

        Ok(())
    }

    fn write_index_meta(&self, entries: &[(String, String, String, bool)]) -> Result<(), StorageError> {
        let path = self.index_meta_path();
        if entries.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        let mut file = fs::File::create(path)?;
        for (name, table, col, unique) in entries {
            if *unique {
                writeln!(file, "{}:{}:{}:UNIQUE", name, table, col)?;
            } else {
                writeln!(file, "{}:{}:{}", name, table, col)?;
            }
        }
        Ok(())
    }

    fn schema_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.schema", table_name))
    }

    fn data_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.data", table_name))
    }

    fn seq_path(&self, table_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.seq", table_name))
    }

    /// Read and increment the auto_increment counter
    fn next_auto_increment(&self, table_name: &str) -> Result<i64, StorageError> {
        let seq_path = self.seq_path(table_name);
        let current: i64 = fs::read_to_string(&seq_path)
            .map_err(|_| StorageError::InvalidData("Missing sequence file".to_string()))?
            .trim()
            .parse()
            .map_err(|_| StorageError::InvalidData("Invalid sequence value".to_string()))?;
        let next = current + 1;
        fs::write(&seq_path, next.to_string())?;
        Ok(next)
    }

    /// Check that a value exists in the referenced table's column
    fn validate_foreign_key(&self, value: &Value, fk: &ForeignKeyRef, col_name: &str) -> Result<(), StorageError> {
        let ref_schema = self.load_schema(&fk.table)?;
        let ref_col_idx = ref_schema.columns.iter()
            .position(|c| c.name == fk.column)
            .ok_or_else(|| StorageError::InvalidSchema(
                format!("FK references unknown column {}.{}", fk.table, fk.column)
            ))?;
        let ref_rows = self.read_rows(&fk.table)?;
        let exists = ref_rows.iter().any(|row| row[ref_col_idx] == *value);
        if !exists {
            return Err(StorageError::ForeignKeyViolation {
                column: col_name.to_string(),
                ref_table: fk.table.clone(),
                ref_column: fk.column.clone(),
            });
        }
        Ok(())
    }

    /// Check if any table has a FK referencing the given table+column with the given values
    fn check_fk_references(&self, table_name: &str, col_name: &str, values: &[Value]) -> Result<(), StorageError> {
        let tables = self.list_tables().map_err(StorageError::IoError)?;
        for t in &tables {
            if t == table_name { continue; }
            let schema = self.load_schema(t)?;
            for (i, col) in schema.columns.iter().enumerate() {
                if let Some(ref fk) = col.references {
                    if fk.table == table_name && fk.column == col_name {
                        let rows = self.read_rows(t)?;
                        for val in values {
                            if rows.iter().any(|row| row[i] == *val) {
                                return Err(StorageError::ForeignKeyViolation {
                                    column: col.name.clone(),
                                    ref_table: table_name.to_string(),
                                    ref_column: col_name.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // --- Index operations ---

    fn index_meta_path(&self) -> PathBuf {
        self.data_dir.join("_indexes.meta")
    }

    fn index_data_path(&self, index_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.idx", index_name))
    }

    /// Load all index metadata entries
    pub fn load_index_meta(&self) -> Result<Vec<(String, String, String, bool)>, StorageError> {
        let path = self.index_meta_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(path)?;
        let mut entries = Vec::new();
        for line in content.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 3 {
                let unique = parts.get(3) == Some(&"UNIQUE");
                entries.push((parts[0].to_string(), parts[1].to_string(), parts[2].to_string(), unique));
            }
        }
        Ok(entries)
    }

    /// Create an index, building it from existing data
    pub fn create_index(&self, stmt: &CreateIndexStatement) -> Result<(), StorageError> {
        // Check table and column exist
        let schema = self.load_schema(&stmt.table_name)?;
        let col_idx = schema.columns.iter()
            .position(|c| c.name == stmt.column_name)
            .ok_or_else(|| StorageError::ColumnNotFound(stmt.column_name.clone()))?;

        // Check index doesn't already exist
        let meta = self.load_index_meta()?;
        if meta.iter().any(|(name, _, _, _)| name == &stmt.index_name) {
            return Err(StorageError::IndexAlreadyExists(stmt.index_name.clone()));
        }

        // Build index from existing rows
        let rows = self.read_rows(&stmt.table_name)?;
        let mut index: HashMap<String, Vec<usize>> = HashMap::new();
        for (row_num, row) in rows.iter().enumerate() {
            let key = serialize_value(&row[col_idx]);
            index.entry(key).or_default().push(row_num);
        }

        // For unique indexes, check no duplicates exist in current data
        if stmt.unique {
            for (key, row_nums) in &index {
                if key != "NULL" && row_nums.len() > 1 {
                    return Err(StorageError::DuplicateKey {
                        column: stmt.column_name.clone(),
                        value: key.clone(),
                    });
                }
            }
        }

        // Write index data
        self.write_index_data(&stmt.index_name, &index)?;

        // Append to metadata
        let meta_path = self.index_meta_path();
        let mut file = fs::OpenOptions::new().create(true).append(true).open(meta_path)?;
        if stmt.unique {
            writeln!(file, "{}:{}:{}:UNIQUE", stmt.index_name, stmt.table_name, stmt.column_name)?;
        } else {
            writeln!(file, "{}:{}:{}", stmt.index_name, stmt.table_name, stmt.column_name)?;
        }

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, index_name: &str) -> Result<(), StorageError> {
        let meta = self.load_index_meta()?;
        if !meta.iter().any(|(name, _, _, _)| name == index_name) {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }

        // Remove index data file
        let idx_path = self.index_data_path(index_name);
        if idx_path.exists() {
            fs::remove_file(idx_path)?;
        }

        // Rewrite metadata without this index
        let remaining: Vec<_> = meta.iter().filter(|(name, _, _, _)| name != index_name).collect();
        let meta_path = self.index_meta_path();
        let mut file = fs::File::create(meta_path)?;
        for (name, table, col, unique) in remaining {
            if *unique {
                writeln!(file, "{}:{}:{}:UNIQUE", name, table, col)?;
            } else {
                writeln!(file, "{}:{}:{}", name, table, col)?;
            }
        }

        Ok(())
    }

    /// Write index data to disk
    fn write_index_data(&self, index_name: &str, index: &HashMap<String, Vec<usize>>) -> Result<(), StorageError> {
        let path = self.index_data_path(index_name);
        let mut file = fs::File::create(path)?;
        for (key, row_nums) in index {
            let nums: Vec<String> = row_nums.iter().map(|n| n.to_string()).collect();
            writeln!(file, "{}|{}", key, nums.join(","))?;
        }
        Ok(())
    }

    /// Look up row numbers from an index for a given value
    pub fn lookup_index(&self, index_name: &str, value: &Value) -> Result<Option<Vec<usize>>, StorageError> {
        let path = self.index_data_path(index_name);
        if !path.exists() {
            return Ok(None);
        }
        let key = serialize_value(value);
        let content = fs::read_to_string(path)?;
        for line in content.lines() {
            // Format: serialized_value|row_num1,row_num2,...
            if let Some((line_key, nums_str)) = line.split_once('|') {
                if line_key == key {
                    let nums: Vec<usize> = nums_str.split(',')
                        .filter_map(|s| s.parse().ok())
                        .collect();
                    return Ok(Some(nums));
                }
            }
        }
        Ok(None)
    }

    /// Find an index for a given table and column
    pub fn find_index(&self, table_name: &str, column_name: &str) -> Result<Option<String>, StorageError> {
        let meta = self.load_index_meta()?;
        Ok(meta.iter()
            .find(|(_, t, c, _)| t == table_name && c == column_name)
            .map(|(name, _, _, _)| name.clone()))
    }

    // Check unique index constraints for a table before inserting a value
    fn check_unique_indexes(&self, table_name: &str, values: &[Value]) -> Result<(), StorageError> {
        let meta = self.load_index_meta()?;
        let schema = self.load_schema(table_name)?;
        for (idx_name, t, col_name, unique) in &meta {
            if !unique || t != table_name {
                continue;
            }
            let col_idx = schema.columns.iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| StorageError::ColumnNotFound(col_name.clone()))?;
            let val = &values[col_idx];
            if *val == Value::Null {
                continue; // NULL doesn't violate uniqueness
            }
            if let Some(row_nums) = self.lookup_index(idx_name, val)? {
                if !row_nums.is_empty() {
                    return Err(StorageError::DuplicateKey {
                        column: col_name.clone(),
                        value: format!("{:?}", val),
                    });
                }
            }
        }
        Ok(())
    }

    /// Rebuild all indexes for a table (called after insert/update/delete)
    fn rebuild_indexes_for_table(&self, table_name: &str) -> Result<(), StorageError> {
        let meta = self.load_index_meta()?;
        let table_indexes: Vec<_> = meta.iter()
            .filter(|(_, t, _, _)| t == table_name)
            .collect();
        if table_indexes.is_empty() {
            return Ok(());
        }

        let schema = self.load_schema(table_name)?;
        let rows = self.read_rows(table_name)?;

        for (idx_name, _, col_name, _) in &table_indexes {
            let col_idx = schema.columns.iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| StorageError::ColumnNotFound(col_name.clone()))?;
            let mut index: HashMap<String, Vec<usize>> = HashMap::new();
            for (row_num, row) in rows.iter().enumerate() {
                let key = serialize_value(&row[col_idx]);
                index.entry(key).or_default().push(row_num);
            }
            self.write_index_data(idx_name, &index)?;
        }
        Ok(())
    }
}

/// Convert a DataType to its string representation
fn data_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Int => "INT".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Varchar(Some(size)) => format!("VARCHAR({})", size),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Varchar(None) => "VARCHAR".to_string(),
    }
}

/// Parse a data type from string representation
fn parse_data_type(s: &str) -> Result<DataType, StorageError> {
    if s == "INT" {
        Ok(DataType::Int)
    } else if s == "FLOAT" {
        Ok(DataType::Float)
    } else if s == "DOUBLE" {
        Ok(DataType::Double)
    } else if s == "BOOLEAN" || s == "BOOL" {
        Ok(DataType::Boolean)
    } else if s == "DATE" {
        Ok(DataType::Date)
    } else if s == "TIMESTAMP" {
        Ok(DataType::Timestamp)
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
        (Value::Float(_), DataType::Float) => Ok(()),
        (Value::Float(_), DataType::Double) => Ok(()),
        (Value::Bool(_), DataType::Boolean) => Ok(()),
        (Value::String(s), DataType::Date) => {
            validate_date_format(s, column_name)
        }
        (Value::String(s), DataType::Timestamp) => {
            validate_timestamp_format(s, column_name)
        }
        (Value::Int(_), DataType::Float) => Ok(()),
        (Value::Int(_), DataType::Double) => Ok(()),
        (Value::String(_), DataType::Varchar(_)) => Ok(()),
        _ => Err(StorageError::TypeMismatch {
            column: column_name.to_string(),
            expected: format!("{:?}", data_type),
            got: format!("{:?}", value),
        }),
    }
}

// Validate YYYY-MM-DD format with valid ranges
fn validate_date_format(s: &str, column_name: &str) -> Result<(), StorageError> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() == 3
        && parts[0].len() == 4 && parts[1].len() == 2 && parts[2].len() == 2
        && parts[0].parse::<u16>().is_ok()
        && parts[1].parse::<u8>().map_or(false, |m| (1..=12).contains(&m))
        && parts[2].parse::<u8>().map_or(false, |d| (1..=31).contains(&d))
    {
        Ok(())
    } else {
        Err(StorageError::TypeMismatch {
            column: column_name.to_string(),
            expected: "DATE (YYYY-MM-DD)".to_string(),
            got: s.to_string(),
        })
    }
}

// Validate YYYY-MM-DD HH:MM:SS format
fn validate_timestamp_format(s: &str, column_name: &str) -> Result<(), StorageError> {
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Err(StorageError::TypeMismatch {
            column: column_name.to_string(),
            expected: "TIMESTAMP (YYYY-MM-DD HH:MM:SS)".to_string(),
            got: s.to_string(),
        });
    }
    validate_date_format(parts[0], column_name)?;
    let time_parts: Vec<&str> = parts[1].split(':').collect();
    if time_parts.len() == 3
        && time_parts[0].len() == 2 && time_parts[1].len() == 2 && time_parts[2].len() == 2
        && time_parts[0].parse::<u8>().map_or(false, |h| h < 24)
        && time_parts[1].parse::<u8>().map_or(false, |m| m < 60)
        && time_parts[2].parse::<u8>().map_or(false, |s| s < 60)
    {
        Ok(())
    } else {
        Err(StorageError::TypeMismatch {
            column: column_name.to_string(),
            expected: "TIMESTAMP (YYYY-MM-DD HH:MM:SS)".to_string(),
            got: s.to_string(),
        })
    }
}

/// Evaluate a WHERE condition against a row
fn evaluate_condition(condition: &Condition, row: &[Value], schema: &[ColumnDefinition]) -> bool {
    if condition.operator == Operator::IsNull || condition.operator == Operator::IsNotNull {
        let left_val = resolve_expression(&condition.left, row, schema);
        let is_null = matches!(left_val, Some(Value::Null) | None);
        return if condition.operator == Operator::IsNull { is_null } else { !is_null };
    }

    if condition.operator == Operator::Between || condition.operator == Operator::NotBetween {
        let val = resolve_expression(&condition.left, row, schema);
        let low = resolve_expression(&condition.right, row, schema);
        let high = condition.upper_bound.as_ref().and_then(|e| resolve_expression(e, row, schema));
        let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
            if compare_values(v, &Operator::GreaterThanOrEqual, l) && compare_values(v, &Operator::LessThanOrEqual, h));
        return if condition.operator == Operator::Between { in_range } else { !in_range };
    }

    let left_val = resolve_expression(&condition.left, row, schema);
    let right_val = resolve_expression(&condition.right, row, schema);

    match (&left_val, &right_val) {
        (Some(l), Some(r)) => compare_values(l, &condition.operator, r),
        _ => false,
    }
}

/// Resolve an expression to a Value
fn resolve_expression(expr: &Expression, row: &[Value], schema: &[ColumnDefinition]) -> Option<Value> {
    match expr {
        Expression::Literal(v) => Some(v.clone()),
        Expression::Column(name) => {
            schema.iter()
                .position(|c| c.name == *name)
                .map(|idx| row[idx].clone())
        }
        Expression::QualifiedColumn(_, col) => {
            // For now, ignore table qualifier and just match column name
            schema.iter()
                .position(|c| c.name == *col)
                .map(|idx| row[idx].clone())
        }
        Expression::Subquery(_) => None,
        Expression::BinaryOp(_, _, _) => None,
        Expression::Aggregate(_, _) => None,
        Expression::Case(branches, else_expr) => {
            for (condition, result) in branches {
                if evaluate_condition(condition, row, schema) {
                    return resolve_expression(result, row, schema);
                }
            }
            else_expr.as_ref().and_then(|e| resolve_expression(e, row, schema))
        }
    }
}

fn compare_numeric(l: f64, r: f64, op: &Operator) -> bool {
    match op {
        Operator::Equals => l == r,
        Operator::NotEquals => l != r,
        Operator::GreaterThan => l > r,
        Operator::LessThan => l < r,
        Operator::GreaterThanOrEqual => l >= r,
        Operator::LessThanOrEqual => l <= r,
        _ => false,
    }
}

/// Compare two values using the given operator
fn compare_values(left: &Value, op: &Operator, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => compare_numeric(*l as f64, *r as f64, op),
        (Value::Float(l), Value::Float(r)) => compare_numeric(*l, *r, op),
        (Value::Int(l), Value::Float(r)) => compare_numeric(*l as f64, *r, op),
        (Value::Float(l), Value::Int(r)) => compare_numeric(*l, *r as f64, op),
        (Value::Bool(l), Value::Bool(r)) => match op {
            Operator::Equals => l == r,
            Operator::NotEquals => l != r,
            _ => false,
        },
        (Value::String(l), Value::String(r)) => match op {
            Operator::Like => like_match(l, r),
            Operator::Equals => l == r,
            Operator::NotEquals => l != r,
            Operator::GreaterThan => l > r,
            Operator::LessThan => l < r,
            Operator::GreaterThanOrEqual => l >= r,
            Operator::LessThanOrEqual => l <= r,
            _ => false,
        },
        (Value::Null, Value::Null) => match op {
            Operator::Equals => true,
            Operator::NotEquals => false,
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
            for i in vi..=v.len() {
                if like_match_recursive(v, p, i, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            vi < v.len() && like_match_recursive(v, p, vi + 1, pi + 1)
        }
        c => {
            vi < v.len() && v[vi] == c && like_match_recursive(v, p, vi + 1, pi + 1)
        }
    }
}

/// Serialize a row to string format: TYPE:value|TYPE:value|...
/// Format: INT:123|STRING:Alice|NULL
fn serialize_value(v: &Value) -> String {
    match v {
        Value::Int(n) => format!("INT:{}", n),
        Value::Float(n) => format!("FLOAT:{}", n),
        Value::Bool(b) => format!("BOOL:{}", b),
        Value::String(s) => {
            let escaped = s.replace('\\', "\\\\")
                .replace('|', "\\|")
                .replace('\n', "\\n");
            format!("STRING:{}", escaped)
        }
        Value::Null => "NULL".to_string(),
    }
}

fn serialize_row(values: &[Value]) -> String {
    values.iter().map(serialize_value).collect::<Vec<_>>().join("|")
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
        } else if let Some(float_str) = part.strip_prefix("FLOAT:") {
            let n = float_str.parse::<f64>()
                .map_err(|_| StorageError::InvalidData(format!("Invalid float: {}", float_str)))?;
            values.push(Value::Float(n));
        } else if let Some(bool_str) = part.strip_prefix("BOOL:") {
            let b = bool_str.parse::<bool>()
                .map_err(|_| StorageError::InvalidData(format!("Invalid boolean: {}", bool_str)))?;
            values.push(Value::Bool(b));
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
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
                ColumnDefinition::new("id", DataType::Int),
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(100))),
                ColumnDefinition::new("description", DataType::Varchar(None)),
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
                ColumnDefinition::new("id", DataType::Int),
            ],
        };

        let orders = CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
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
                ColumnDefinition::new("id", DataType::Int),
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
                ColumnDefinition::new("email", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Insert data
        let insert_stmt = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![
                Value::Int(1),
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
            ]),
        };
        storage.insert_row(&insert_stmt).unwrap();

        // Insert more data
        let insert_stmt2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![
                Value::Int(2),
                Value::String("Bob".to_string()),
                Value::String("bob@example.com".to_string()),
            ]),
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(100))),
                ColumnDefinition::new("description", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert_stmt = InsertStatement {
            table_name: "products".to_string(),
            source: crate::parser::InsertSource::Values(vec![
                Value::Int(1),
                Value::String("Widget".to_string()),
                Value::Null,
            ]),
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert with wrong number of columns
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1)]), // Missing one column
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
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert string into int column
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            source: crate::parser::InsertSource::Values(vec![
                Value::String("not a number".to_string()),
                Value::String("Alice".to_string()),
            ]),
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

    #[test]
    fn test_update_single_row() {
        use crate::parser::{UpdateStatement, Assignment, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_single");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        // Create table and insert data
        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert1 = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        };
        let insert2 = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        };
        storage.insert_row(&insert1).unwrap();
        storage.insert_row(&insert2).unwrap();

        // Update single row
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Value::String("Alice Updated".to_string()),
            }],
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(1)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated, 1);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[0][1], Value::String("Alice Updated".to_string()));
        assert_eq!(rows[1][1], Value::String("Bob".to_string())); // Unchanged

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_update_multiple_rows() {
        use crate::parser::{UpdateStatement, Assignment, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_multi");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("active", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Insert 3 rows with active = 1
        for i in 1..=3 {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![Value::Int(i), Value::Int(1)]),
            };
            storage.insert_row(&insert).unwrap();
        }

        // Update all rows where active = 1
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "active".to_string(),
                value: Value::Int(0),
            }],
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("active".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(1)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated, 3);

        let rows = storage.read_rows("users").unwrap();
        for row in rows {
            assert_eq!(row[1], Value::Int(0));
        }

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_update_all_rows_no_where() {
        use crate::parser::{UpdateStatement, Assignment};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_all");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("status", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        for i in 1..=3 {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![Value::Int(i), Value::String("old".to_string())]),
            };
            storage.insert_row(&insert).unwrap();
        }

        // Update all rows (no WHERE clause)
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "status".to_string(),
                value: Value::String("new".to_string()),
            }],
            where_clause: None,
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated, 3);

        let rows = storage.read_rows("users").unwrap();
        for row in rows {
            assert_eq!(row[1], Value::String("new".to_string()));
        }

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_update_no_matches() {
        use crate::parser::{UpdateStatement, Assignment, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_none");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1)]),
        };
        storage.insert_row(&insert).unwrap();

        // Update with non-matching condition
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "id".to_string(),
                value: Value::Int(99),
            }],
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(999)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated, 0);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[0][0], Value::Int(1)); // Unchanged

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_update_invalid_column() {
        use crate::parser::{UpdateStatement, Assignment};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_invalid_col");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "nonexistent".to_string(),
                value: Value::Int(1),
            }],
            where_clause: None,
        };

        let result = storage.update_rows(&update_stmt);
        assert!(matches!(result, Err(StorageError::ColumnNotFound(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_update_type_mismatch() {
        use crate::parser::{UpdateStatement, Assignment};

        let temp_dir = std::env::temp_dir().join("abcsql_test_update_type");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to update INT column with STRING value
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "id".to_string(),
                value: Value::String("not a number".to_string()),
            }],
            where_clause: None,
        };

        let result = storage.update_rows(&update_stmt);
        assert!(matches!(result, Err(StorageError::TypeMismatch { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_delete_single_row() {
        use crate::parser::{DeleteStatement, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_delete_single");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Insert 3 rows
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![Value::Int(id), Value::String(name.to_string())]),
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete where id = 2
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(2)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted, 1);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(3));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_delete_multiple_rows() {
        use crate::parser::{DeleteStatement, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_delete_multi");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("active", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Insert rows: 1-active, 2-inactive, 3-active, 4-inactive
        for (id, active) in [(1, 1), (2, 0), (3, 1), (4, 0)] {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![Value::Int(id), Value::Int(active)]),
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete inactive users (active = 0)
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("active".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(0)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted, 2);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 2);
        // Only active users remain
        for row in rows {
            assert_eq!(row[1], Value::Int(1));
        }

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_delete_all_rows() {
        use crate::parser::DeleteStatement;

        let temp_dir = std::env::temp_dir().join("abcsql_test_delete_all");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        for i in 1..=5 {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![Value::Int(i)]),
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete all (no WHERE clause)
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            where_clause: None,
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted, 5);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 0);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_delete_no_matches() {
        use crate::parser::{DeleteStatement, WhereClause, Condition, Expression, Operator};

        let temp_dir = std::env::temp_dir().join("abcsql_test_delete_none");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let create_stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1)]),
        };
        storage.insert_row(&insert).unwrap();

        // Delete with non-matching condition
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(999)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted, 0);

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 1);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_delete_table_not_found() {
        use crate::parser::DeleteStatement;

        let temp_dir = std::env::temp_dir().join("abcsql_test_delete_notfound");
        let _ = fs::remove_dir_all(&temp_dir);

        let storage = Storage::new(&temp_dir).unwrap();

        let delete_stmt = DeleteStatement {
            table_name: "nonexistent".to_string(),
            where_clause: None,
        };

        let result = storage.delete_rows(&delete_stmt);
        assert!(matches!(result, Err(StorageError::TableNotFound(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_date_insert_and_read() {
        let temp_dir = format!("/tmp/abcsql_test_date_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "events".to_string(),
            columns: vec![
                ColumnDefinition::new("name", DataType::Varchar(None)),
                ColumnDefinition::new("event_date", DataType::Date),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert = InsertStatement {
            table_name: "events".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::String("launch".to_string()), Value::String("2024-03-15".to_string())]),
        };
        storage.insert_row(&insert).unwrap();

        let rows = storage.read_rows("events").unwrap();
        assert_eq!(rows[0][1], Value::String("2024-03-15".to_string()));

        // invalid date should fail
        let bad_insert = InsertStatement {
            table_name: "events".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::String("oops".to_string()), Value::String("not-a-date".to_string())]),
        };
        assert!(bad_insert.values().len() == 2);
        assert!(storage.insert_row(&bad_insert).is_err());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_timestamp_insert_and_read() {
        let temp_dir = format!("/tmp/abcsql_test_ts_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "logs".to_string(),
            columns: vec![
                ColumnDefinition::new("msg", DataType::Varchar(None)),
                ColumnDefinition::new("created_at", DataType::Timestamp),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert = InsertStatement {
            table_name: "logs".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::String("hello".to_string()), Value::String("2024-03-15 14:30:00".to_string())]),
        };
        storage.insert_row(&insert).unwrap();

        let rows = storage.read_rows("logs").unwrap();
        assert_eq!(rows[0][1], Value::String("2024-03-15 14:30:00".to_string()));

        // invalid timestamp should fail
        let bad_insert = InsertStatement {
            table_name: "logs".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::String("bad".to_string()), Value::String("2024-03-15".to_string())]),
        };
        assert!(storage.insert_row(&bad_insert).is_err());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_auto_increment() {
        let temp_dir = format!("/tmp/abcsql_test_autoinc_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: true, primary_key: false, not_null: false, unique: false, references: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        // Insert with NULL for auto_increment column
        let insert1 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Null, Value::String("Alice".to_string())]),
        };
        storage.insert_row(&insert1).unwrap();

        let insert2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Null, Value::String("Bob".to_string())]),
        };
        storage.insert_row(&insert2).unwrap();

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(2));

        // Can also supply an explicit value
        let insert3 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(10), Value::String("Charlie".to_string())]),
        };
        storage.insert_row(&insert3).unwrap();

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[2][0], Value::Int(10));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_primary_key_unique() {
        let temp_dir = format!("/tmp/abcsql_test_pk_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert1 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        };
        storage.insert_row(&insert1).unwrap();

        // Duplicate key should fail
        let insert2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Bob".to_string())]),
        };
        assert!(matches!(storage.insert_row(&insert2), Err(StorageError::DuplicateKey { .. })));

        // Different key should succeed
        let insert3 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        };
        storage.insert_row(&insert3).unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_primary_key_not_null() {
        let temp_dir = format!("/tmp/abcsql_test_pknull_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        // NULL primary key should fail
        let insert = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Null, Value::String("Alice".to_string())]),
        };
        assert!(matches!(storage.insert_row(&insert), Err(StorageError::NullConstraint { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_foreign_key_insert() {
        let temp_dir = format!("/tmp/abcsql_test_fk_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        // Parent table
        let create_users = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_users).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();

        // Child table with FK
        let create_orders = CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false,
                    references: Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() }) },
            ],
        };
        storage.create_table(&create_orders).unwrap();

        // Valid FK reference
        storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::Int(1)]),
        }).unwrap();

        // Invalid FK reference should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::Int(999)]),
        });
        assert!(matches!(result, Err(StorageError::ForeignKeyViolation { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_foreign_key_delete_parent() {
        let temp_dir = format!("/tmp/abcsql_test_fkdel_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        // Parent table
        let create_users = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_users).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        }).unwrap();

        // Child table with FK
        let create_orders = CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false,
                    references: Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() }) },
            ],
        };
        storage.create_table(&create_orders).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::Int(1)]),
        }).unwrap();

        // Deleting referenced parent should fail
        let result = storage.delete_rows(&DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(crate::parser::WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(1)),
                },
            }),
        });
        assert!(matches!(result, Err(StorageError::ForeignKeyViolation { .. })));

        // Deleting non-referenced parent should succeed
        let result = storage.delete_rows(&DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(crate::parser::WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(2)),
                },
            }),
        });
        assert!(result.is_ok());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_not_null_constraint() {
        let temp_dir = format!("/tmp/abcsql_test_nn_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "name".to_string(), data_type: DataType::Varchar(None),
                    auto_increment: false, primary_key: false, not_null: true, unique: false, references: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Valid insert
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();

        // NULL in NOT NULL column should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::Null]),
        });
        assert!(matches!(result, Err(StorageError::NullConstraint { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_unique_constraint() {
        let temp_dir = format!("/tmp/abcsql_test_uq_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "email".to_string(), data_type: DataType::Varchar(None),
                    auto_increment: false, primary_key: false, not_null: false, unique: true, references: None },
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("a@b.com".to_string())]),
        }).unwrap();

        // Duplicate unique value should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("a@b.com".to_string())]),
        });
        assert!(matches!(result, Err(StorageError::DuplicateKey { .. })));

        // NULL values don't violate uniqueness
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(3), Value::Null]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(4), Value::Null]),
        }).unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_create_and_lookup_index() {
        let temp_dir = format!("/tmp/abcsql_test_idx_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(3), Value::String("Alice".to_string())]),
        }).unwrap();

        // Create index on name column
        storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        }).unwrap();

        // Lookup should find matching rows
        let result = storage.lookup_index("idx_name", &Value::String("Alice".to_string())).unwrap();
        assert!(result.is_some());
        let row_nums = result.unwrap();
        assert_eq!(row_nums.len(), 2);

        // Lookup non-existent value
        let result = storage.lookup_index("idx_name", &Value::String("Charlie".to_string())).unwrap();
        assert!(result.is_none());

        // find_index should locate it
        let found = storage.find_index("users", "name").unwrap();
        assert_eq!(found, Some("idx_name".to_string()));

        // find_index for non-indexed column
        let found = storage.find_index("users", "id").unwrap();
        assert_eq!(found, None);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_index_rebuild_after_insert() {
        let temp_dir = format!("/tmp/abcsql_test_idx_ins_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();

        storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        }).unwrap();

        // Insert another row — index should be rebuilt
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        }).unwrap();

        let result = storage.lookup_index("idx_name", &Value::String("Bob".to_string())).unwrap();
        assert!(result.is_some());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_index_rebuild_after_delete() {
        use crate::parser::{DeleteStatement, WhereClause, Condition, Expression, Operator};

        let temp_dir = format!("/tmp/abcsql_test_idx_del_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        }).unwrap();

        storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        }).unwrap();

        // Delete Alice
        storage.delete_rows(&DeleteStatement {
            table_name: "users".to_string(),
            where_clause: Some(WhereClause {
                condition: Condition { upper_bound: None,
                    left: Expression::Column("name".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::String("Alice".to_string())),
                },
            }),
        }).unwrap();

        // Alice should no longer be in the index
        let result = storage.lookup_index("idx_name", &Value::String("Alice".to_string())).unwrap();
        assert!(result.is_none());

        // Bob should still be there
        let result = storage.lookup_index("idx_name", &Value::String("Bob".to_string())).unwrap();
        assert!(result.is_some());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_drop_index() {
        let temp_dir = format!("/tmp/abcsql_test_idx_drop_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        }).unwrap();

        // Drop the index
        storage.drop_index("idx_name").unwrap();

        // Should no longer be findable
        let found = storage.find_index("users", "name").unwrap();
        assert_eq!(found, None);

        // Dropping again should fail
        let result = storage.drop_index("idx_name");
        assert!(matches!(result, Err(StorageError::IndexNotFound(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_duplicate_index_name() {
        let temp_dir = format!("/tmp/abcsql_test_idx_dup_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        }).unwrap();

        // Creating an index with the same name should fail
        let result = storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: false,
        });
        assert!(matches!(result, Err(StorageError::IndexAlreadyExists(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_read_rows_by_numbers() {
        let temp_dir = format!("/tmp/abcsql_test_idx_rbn_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Bob".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(3), Value::String("Charlie".to_string())]),
        }).unwrap();

        // Read only rows 0 and 2
        let rows = storage.read_rows_by_numbers("users", &[0, 2]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::Int(1), Value::String("Alice".to_string())]);
        assert_eq!(rows[1], vec![Value::Int(3), Value::String("Charlie".to_string())]);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_unique_index_enforced_on_insert() {
        let temp_dir = format!("/tmp/abcsql_test_uidx_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("email", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("a@b.com".to_string())]),
        }).unwrap();

        // Create unique index on email
        storage.create_index(&CreateIndexStatement {
            index_name: "idx_email".to_string(),
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            unique: true,
        }).unwrap();

        // Inserting a duplicate email should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("a@b.com".to_string())]),
        });
        assert!(matches!(result, Err(StorageError::DuplicateKey { .. })));

        // Inserting a different email should succeed
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(3), Value::String("c@d.com".to_string())]),
        }).unwrap();

        // NULL should not violate unique index
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(4), Value::Null]),
        }).unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_unique_index_rejects_existing_duplicates() {
        let temp_dir = format!("/tmp/abcsql_test_uidx_dup_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(2), Value::String("Alice".to_string())]),
        }).unwrap();

        // Creating a unique index should fail because duplicates exist
        let result = storage.create_index(&CreateIndexStatement {
            index_name: "idx_name".to_string(),
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            unique: true,
        });
        assert!(matches!(result, Err(StorageError::DuplicateKey { .. })));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_add_column() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_add");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::AddColumn(ColumnDefinition::new("email", DataType::Varchar(None))),
        }).unwrap();

        let schema = storage.load_schema("users").unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[2].name, "email");

        // Existing row should now have NULL in the new column
        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert_eq!(rows[0][2], Value::Null);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_add_not_null_to_nonempty_fails() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_add_nn");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "t".to_string(),
            columns: vec![ColumnDefinition::new("id", DataType::Int)],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "t".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1)]),
        }).unwrap();

        let mut col = ColumnDefinition::new("required", DataType::Int);
        col.not_null = true;
        let result = storage.alter_table(&AlterTableStatement {
            table_name: "t".to_string(),
            action: AlterAction::AddColumn(col),
        });
        assert!(matches!(result, Err(StorageError::InvalidSchema(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_drop_column() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_drop");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
                ColumnDefinition::new("temp", DataType::Int),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string()), Value::Int(99)]),
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::DropColumn("temp".to_string()),
        }).unwrap();

        let schema = storage.load_schema("users").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert!(!schema.columns.iter().any(|c| c.name == "temp"));

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_drop_column_referenced_by_fk_fails() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_drop_fk");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        let mut id_col = ColumnDefinition::new("id", DataType::Int);
        id_col.primary_key = true;
        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![id_col],
        }).unwrap();

        let mut fk_col = ColumnDefinition::new("user_id", DataType::Int);
        fk_col.references = Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() });
        storage.create_table(&CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![ColumnDefinition::new("oid", DataType::Int), fk_col],
        }).unwrap();

        let result = storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::DropColumn("id".to_string()),
        });
        assert!(matches!(result, Err(StorageError::InvalidSchema(_))));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_rename_column_updates_fk() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_rename_col");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        let mut id_col = ColumnDefinition::new("id", DataType::Int);
        id_col.primary_key = true;
        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![id_col],
        }).unwrap();

        let mut fk_col = ColumnDefinition::new("user_id", DataType::Int);
        fk_col.references = Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() });
        storage.create_table(&CreateTableStatement {
            table_name: "orders".to_string(),
            columns: vec![ColumnDefinition::new("oid", DataType::Int), fk_col],
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::RenameColumn { from: "id".to_string(), to: "user_id".to_string() },
        }).unwrap();

        let users = storage.load_schema("users").unwrap();
        assert_eq!(users.columns[0].name, "user_id");

        let orders = storage.load_schema("orders").unwrap();
        let fk = orders.columns[1].references.as_ref().unwrap();
        assert_eq!(fk.column, "user_id");

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_rename_table() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_rename_tbl");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![Value::Int(1), Value::String("Alice".to_string())]),
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::RenameTable("members".to_string()),
        }).unwrap();

        assert!(!storage.table_exists("users"));
        assert!(storage.table_exists("members"));

        let rows = storage.read_rows("members").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_drop_column_drops_index() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_drop_idx");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("email", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.create_index(&CreateIndexStatement {
            index_name: "idx_email".to_string(),
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            unique: false,
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::DropColumn("email".to_string()),
        }).unwrap();

        assert!(storage.find_index("users", "email").unwrap().is_none());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_alter_rename_column_updates_index_meta() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_alter_rename_col_idx");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        storage.create_table(&CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("email", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.create_index(&CreateIndexStatement {
            index_name: "idx_email".to_string(),
            table_name: "users".to_string(),
            column_name: "email".to_string(),
            unique: false,
        }).unwrap();

        storage.alter_table(&AlterTableStatement {
            table_name: "users".to_string(),
            action: AlterAction::RenameColumn { from: "email".to_string(), to: "addr".to_string() },
        }).unwrap();

        assert!(storage.find_index("users", "email").unwrap().is_none());
        assert_eq!(storage.find_index("users", "addr").unwrap().as_deref(), Some("idx_email"));

        fs::remove_dir_all(&temp_dir).unwrap();
    }
}
