use std::fs;
use std::io::{self, Write as IoWrite, BufWriter, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::fmt;
use std::collections::HashMap;
use regex::Regex;
use crate::parser::{CreateTableStatement, CreateIndexStatement, CreateFunctionStatement, ColumnDefinition, DataType, ForeignKeyRef, InsertStatement, InsertSource, OnConflict, UpdateStatement, DeleteStatement, TruncateStatement, MergeStatement, MergeSource, MergeAction, AlterTableStatement, AlterAction, Value, Condition, Expression, Operator, ArithOp, SelectStatement, SelectColumn, FromClause, TableConstraint, TableConstraintKind, apply_scalar_func, apply_round, apply_concat, apply_substr, apply_replace, apply_lpad, apply_rpad, apply_cast, apply_greatest, apply_least, apply_power, apply_position, apply_repeat, apply_json_typeof, apply_json_array_length, apply_json_build_object, apply_json_build_array};

/// Before-image snapshot for a single transaction
struct TransactionState {
    // None = file didn't exist before the transaction (delete on rollback)
    before_images: HashMap<PathBuf, Option<Vec<u8>>>,
    // Named savepoints in creation order; each stores current on-disk bytes of modified files
    savepoints: Vec<(String, HashMap<PathBuf, Option<Vec<u8>>>)>,
}

/// Storage engine for persisting tables to disk
pub struct Storage {
    data_dir: PathBuf,
    txn: std::sync::Mutex<Option<TransactionState>>,
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
    CheckConstraintViolation { column: String, reason: String },
    IndexAlreadyExists(String),
    IndexNotFound(String),
    TransactionError(String),
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
            StorageError::CheckConstraintViolation { column, reason } => {
                write!(f, "CHECK constraint violation on '{}': {}", column, reason)
            }
            StorageError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            StorageError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            StorageError::TransactionError(msg) => write!(f, "Transaction error: {}", msg),
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
    /// Create a no-op Storage for expression evaluation contexts that don't touch files
    fn noop() -> Self {
        Storage { data_dir: PathBuf::new(), txn: std::sync::Mutex::new(None) }
    }

    /// Create a new Storage instance with the specified data directory
    pub fn new<P: AsRef<Path>>(data_dir: P) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create the data directory if it doesn't exist
        if !data_dir.exists() {
            fs::create_dir_all(&data_dir)?;
        }

        Ok(Storage { data_dir, txn: std::sync::Mutex::new(None) })
    }

    /// Return a reference to the data directory path
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Capture the before-image of a file once per transaction (idempotent)
    fn snapshot_before_write(&self, path: &Path) {
        let mut guard = self.txn.lock().unwrap();
        if let Some(txn) = guard.as_mut() {
            txn.before_images.entry(path.to_path_buf()).or_insert_with(|| fs::read(path).ok());
        }
    }

    /// Snapshot the index meta file and all existing index data files for a table
    fn snapshot_index_files(&self, table_name: &str) {
        let meta = self.index_meta_path();
        self.snapshot_before_write(&meta);
        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let s = name.to_string_lossy();
                if s.starts_with(&format!("{}.idx.", table_name)) {
                    self.snapshot_before_write(&entry.path());
                }
            }
        }
    }

    // --- Transaction control ---

    pub fn begin_transaction(&self) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        if guard.is_some() {
            return Err(StorageError::TransactionError("already in a transaction".into()));
        }
        *guard = Some(TransactionState { before_images: HashMap::new(), savepoints: Vec::new() });
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        if guard.is_none() {
            return Err(StorageError::TransactionError("no active transaction".into()));
        }
        *guard = None; // writes are already on disk — discard before-images
        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        let txn = guard.take().ok_or_else(|| StorageError::TransactionError("no active transaction".into()))?;
        restore_files(&txn.before_images)?;
        Ok(())
    }

    pub fn create_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        let txn = guard.as_mut().ok_or_else(|| StorageError::TransactionError("no active transaction".into()))?;
        // Snapshot current on-disk state of every file touched so far
        let snapshot: HashMap<PathBuf, Option<Vec<u8>>> = txn.before_images.keys()
            .map(|path| (path.clone(), fs::read(path).ok()))
            .collect();
        // Replace any existing savepoint with the same name
        txn.savepoints.retain(|(n, _)| n != name);
        txn.savepoints.push((name.to_string(), snapshot));
        Ok(())
    }

    pub fn rollback_to_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        let txn = guard.as_mut().ok_or_else(|| StorageError::TransactionError("no active transaction".into()))?;
        let pos = txn.savepoints.iter().rposition(|(n, _)| n == name)
            .ok_or_else(|| StorageError::TransactionError(format!("savepoint '{}' does not exist", name)))?;
        let (_, snapshot) = &txn.savepoints[pos];
        restore_files(snapshot)?;
        // Truncate savepoints created after this one
        txn.savepoints.truncate(pos + 1);
        Ok(())
    }

    pub fn release_savepoint(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.txn.lock().unwrap();
        let txn = guard.as_mut().ok_or_else(|| StorageError::TransactionError("no active transaction".into()))?;
        let pos = txn.savepoints.iter().rposition(|(n, _)| n == name)
            .ok_or_else(|| StorageError::TransactionError(format!("savepoint '{}' does not exist", name)))?;
        txn.savepoints.remove(pos);
        Ok(())
    }

    /// Check if a transaction is active
    pub fn is_in_transaction(&self) -> bool {
        self.txn.lock().unwrap().is_some()
    }

    /// Acquire a FOR UPDATE lock on a table (requires active transaction).
    /// In this single-user engine, we verify a transaction is active but don't need
    /// actual concurrency locks. The lock ensures the caller has explicitly started
    /// a transaction before attempting FOR UPDATE.
    pub fn lock_for_update(&self, table_name: &str) -> Result<(), StorageError> {
        if !self.is_in_transaction() {
            return Err(StorageError::TransactionError(
                "SELECT ... FOR UPDATE requires an active transaction (use BEGIN first)".into()
            ));
        }
        // Verify the table exists
        if !self.table_exists(table_name) {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    /// Create a new table by persisting its schema to disk
    pub fn create_table(&self, stmt: &CreateTableStatement) -> Result<(), StorageError> {
        let schema_path = self.schema_path(&stmt.table_name);

        // Check if table already exists
        if schema_path.exists() {
            return Err(StorageError::TableAlreadyExists(stmt.table_name.clone()));
        }

        // Snapshot before creating (all three paths don't exist yet → None)
        self.snapshot_before_write(&schema_path);
        let data_path = self.data_path(&stmt.table_name);
        self.snapshot_before_write(&data_path);
        let seq_path = self.seq_path(&stmt.table_name);
        self.snapshot_before_write(&seq_path);

        // Validate that table constraints reference known columns
        for tc in &stmt.constraints {
            let cols: &[String] = match &tc.kind {
                TableConstraintKind::PrimaryKey(c) | TableConstraintKind::Unique(c) => c,
                TableConstraintKind::ForeignKey { columns, .. } => columns,
                TableConstraintKind::Check(_) => &[],
            };
            for col in cols {
                if !stmt.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col)) {
                    return Err(StorageError::InvalidSchema(
                        format!("constraint references unknown column '{}'", col)
                    ));
                }
            }
        }

        self.write_schema_file(&stmt.table_name, &stmt.columns, &stmt.constraints)?;

        // Create empty data file
        fs::File::create(&data_path)?;

        // Initialize sequence file for auto_increment columns
        if stmt.columns.iter().any(|c| c.auto_increment) {
            fs::write(&seq_path, "0")?;
        }

        Ok(())
    }

    /// Write (or overwrite) a schema file for a table
    fn write_schema_file(&self, table_name: &str, columns: &[ColumnDefinition], constraints: &[TableConstraint]) -> Result<(), StorageError> {
        let schema_path = self.schema_path(table_name);
        self.snapshot_before_write(&schema_path);
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
            let check_str = col.check_constraint_text.as_ref().map(|t| {
                let escaped = t.replace('\\', "\\\\").replace(':', "\\:");
                format!("CK={}", escaped)
            });
            if let Some(ref ck) = check_str { parts.push(ck); }
            let default_str = col.default_text.as_ref().map(|t| {
                let escaped = t.replace('\\', "\\\\").replace(':', "\\:");
                format!("DF={}", escaped)
            });
            if let Some(ref df) = default_str { parts.push(df); }
            writeln!(file, "{}", parts.join(":"))?;
        }
        for tc in constraints {
            let escaped = tc.raw.replace('\\', "\\\\").replace('\n', " ");
            writeln!(file, "!TC={}", escaped)?;
        }
        Ok(())
    }

/// Regenerate the SQL text of a table constraint from its parsed form.
/// CHECK constraints keep their original text since the condition isn't re-serialized.
fn constraint_to_sql(tc: &TableConstraint) -> String {
    let body = match &tc.kind {
        TableConstraintKind::PrimaryKey(c) => format!("PRIMARY KEY ({})", c.join(", ")),
        TableConstraintKind::Unique(c) => format!("UNIQUE ({})", c.join(", ")),
        TableConstraintKind::ForeignKey { columns, ref_table, ref_columns } => {
            if ref_columns.is_empty() {
                format!("FOREIGN KEY ({}) REFERENCES {}", columns.join(", "), ref_table)
            } else {
                format!("FOREIGN KEY ({}) REFERENCES {} ({})", columns.join(", "), ref_table, ref_columns.join(", "))
            }
        }
        TableConstraintKind::Check(_) => return tc.raw.clone(),
    };
    match &tc.name {
        Some(n) => format!("CONSTRAINT {} {}", n, body),
        None => body,
    }
}

/// Decode a CHECK constraint text that was escaped for schema file storage.
fn decode_check_text(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(':') => result.push(':'),
                Some('\\') => result.push('\\'),
                Some(other) => { result.push('\\'); result.push(other); }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

    /// Insert row(s) into a table. Returns (rows_inserted, returning_rows).
    pub fn insert_row(&self, stmt: &InsertStatement) -> Result<(usize, Option<Vec<Vec<Value>>>), StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;
        let rows: Vec<Vec<Value>> = match &stmt.source {
            InsertSource::Values(rows) => rows.clone(),
            InsertSource::DefaultValues => vec![vec![Value::Default; schema.columns.len()]],
            InsertSource::Select(_) => panic!("insert_row called with Select source — caller must resolve to values first"),
        };

        let mut count = 0usize;
        let mut returning_rows: Vec<Vec<Value>> = Vec::new();

        for values in &rows {
            // Validate column count (only when no explicit column list)
            if stmt.columns.is_empty() && values.len() != schema.columns.len() {
                return Err(StorageError::ColumnCountMismatch {
                    expected: schema.columns.len(),
                    got: values.len(),
                });
            }

            // Map values to schema positions when a column list is provided;
            // omitted columns take their DEFAULT (Null when none is defined)
            let mapped_values: Vec<Value> = if stmt.columns.is_empty() {
                values.clone()
            } else {
                schema.columns.iter().map(|col_def| {
                    stmt.columns.iter().position(|c| c.eq_ignore_ascii_case(&col_def.name))
                        .and_then(|i| values.get(i).cloned())
                        .unwrap_or(Value::Default)
                }).collect()
            };

            // Resolve DEFAULT markers against column defaults
            let mapped_values: Vec<Value> = mapped_values.into_iter()
                .zip(schema.columns.iter())
                .map(|(v, col_def)| {
                    if v == Value::Default { self.default_value_for(col_def) } else { v }
                })
                .collect();

            let result = self.insert_single_row(&stmt.table_name, mapped_values, &schema, &stmt.on_conflict);
            match result {
                Ok(Some(final_values)) => {
                    if let Some(ref ret_cols) = stmt.returning {
                        let ret_row = project_returning(&final_values, ret_cols, &schema.columns);
                        returning_rows.push(ret_row);
                    }
                    count += 1;
                }
                Ok(None) => {
                    // ON CONFLICT DO NOTHING — skip silently
                }
                Err(StorageError::DuplicateKey { ref column, ref value }) => {
                    // Re-check for on_conflict handler
                    if stmt.on_conflict.is_none() {
                        return Err(StorageError::DuplicateKey { column: column.clone(), value: value.clone() });
                    }
                    // ON CONFLICT DO NOTHING or DO UPDATE already handled inside insert_single_row
                    unreachable!("on_conflict should be handled in insert_single_row");
                }
                Err(e) => return Err(e),
            }
        }

        let returning = if stmt.returning.is_some() { Some(returning_rows) } else { None };
        Ok((count, returning))
    }

    /// Value used for a column when INSERT/UPDATE specifies DEFAULT or omits the column
    fn default_value_for(&self, col: &ColumnDefinition) -> Value {
        col.default.as_ref()
            .and_then(|e| resolve_expression(e, &[], &[], self))
            .unwrap_or(Value::Null)
    }

    /// Insert a single row; handles ON CONFLICT. Returns Some(final_values) on success, None for DO NOTHING.
    fn insert_single_row(
        &self,
        table_name: &str,
        values: Vec<Value>,
        schema: &CreateTableStatement,
        on_conflict: &Option<OnConflict>,
    ) -> Result<Option<Vec<Value>>, StorageError> {
        // Build final values, filling in auto_increment where NULL is provided
        let mut final_values = values;
        for (i, col_def) in schema.columns.iter().enumerate() {
            if col_def.auto_increment && final_values[i] == Value::Null {
                let next_val = self.next_auto_increment(table_name)?;
                final_values[i] = Value::Int(next_val);
            }
        }

        // Coerce string literals into Date/Timestamp values
        for (i, col_def) in schema.columns.iter().enumerate() {
            match (&final_values[i].clone(), &col_def.data_type) {
                (Value::String(s), DataType::Date) => {
                    if let Some(days) = crate::parser::parse_date_str(s) {
                        final_values[i] = Value::Date(days);
                    }
                }
                (Value::String(s), DataType::Timestamp) => {
                    if let Some(secs) = crate::parser::parse_timestamp_str(s) {
                        final_values[i] = Value::Timestamp(secs);
                    }
                }
                (Value::String(s), DataType::Json | DataType::Jsonb) => {
                    if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                        final_values[i] = Value::Json(s.clone());
                    }
                }
                _ => {}
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

        // Enforce primary key NOT NULL
        for (i, col_def) in schema.columns.iter().enumerate() {
            if col_def.primary_key && final_values[i] == Value::Null {
                return Err(StorageError::NullConstraint { column: col_def.name.clone() });
            }
        }

        // Check uniqueness — handle ON CONFLICT if there's a duplicate
        let unique_columns: Vec<(usize, &ColumnDefinition)> = schema.columns.iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key || c.unique)
            .collect();

        if !unique_columns.is_empty() {
            let existing_rows = self.read_rows(table_name)?;
            for row in &existing_rows {
                for &(i, col_def) in &unique_columns {
                    if final_values[i] != Value::Null && row[i] == final_values[i] {
                        // Conflict detected — check ON CONFLICT handler
                        match on_conflict {
                            Some(OnConflict::DoNothing) => return Ok(None),
                            Some(OnConflict::DoUpdate { assignments, .. }) => {
                                return self.apply_conflict_update(table_name, schema, &final_values, assignments);
                            }
                            None => {
                                return Err(StorageError::DuplicateKey {
                                    column: col_def.name.clone(),
                                    value: format!("{:?}", final_values[i]),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Enforce table-level PRIMARY KEY / UNIQUE constraints (composite keys)
        for tc in &schema.constraints {
            let (cols, is_pk) = match &tc.kind {
                TableConstraintKind::PrimaryKey(c) => (c, true),
                TableConstraintKind::Unique(c) => (c, false),
                _ => continue,
            };
            let idxs: Vec<usize> = cols.iter()
                .filter_map(|c| schema.columns.iter().position(|col| col.name.eq_ignore_ascii_case(c)))
                .collect();
            if idxs.len() != cols.len() { continue; }

            if is_pk {
                for &i in &idxs {
                    if final_values[i] == Value::Null {
                        return Err(StorageError::NullConstraint { column: schema.columns[i].name.clone() });
                    }
                }
            } else if idxs.iter().any(|&i| final_values[i] == Value::Null) {
                // UNIQUE tuples containing NULL never conflict
                continue;
            }

            let existing_rows = self.read_rows(table_name)?;
            for row in &existing_rows {
                if idxs.iter().all(|&i| row[i] == final_values[i]) {
                    match on_conflict {
                        Some(OnConflict::DoNothing) => return Ok(None),
                        Some(OnConflict::DoUpdate { assignments, .. }) => {
                            return self.apply_conflict_update(table_name, schema, &final_values, assignments);
                        }
                        None => {
                            let key = tc.name.clone().unwrap_or_else(|| cols.join(", "));
                            let vals: Vec<String> = idxs.iter().map(|&i| format!("{:?}", final_values[i])).collect();
                            return Err(StorageError::DuplicateKey {
                                column: key,
                                value: vals.join(", "),
                            });
                        }
                    }
                }
            }
        }

        // Check unique index constraints
        if let Err(e) = self.check_unique_indexes_conflict(table_name, &final_values, on_conflict, schema) {
            return Err(e);
        }

        // Enforce foreign key constraints
        for (i, col_def) in schema.columns.iter().enumerate() {
            if let Some(ref fk) = col_def.references {
                if final_values[i] != Value::Null {
                    self.validate_foreign_key(&final_values[i], fk, &col_def.name)?;
                }
            }
        }

        // Enforce table-level FOREIGN KEY constraints (composite references)
        for tc in &schema.constraints {
            if let TableConstraintKind::ForeignKey { columns, ref_table, ref_columns } = &tc.kind {
                self.validate_composite_foreign_key(&final_values, schema, columns, ref_table, ref_columns)?;
            }
        }

        // Enforce CHECK constraints
        for (_i, col_def) in schema.columns.iter().enumerate() {
            if let Some(ref check) = col_def.check_constraint {
                if !evaluate_condition(check, &final_values, &schema.columns, self) {
                    return Err(StorageError::CheckConstraintViolation {
                        column: col_def.name.clone(),
                        reason: format!("CHECK constraint failed: {:?}", check),
                    });
                }
            }
        }

        // Enforce table-level CHECK constraints
        for tc in &schema.constraints {
            if let TableConstraintKind::Check(cond) = &tc.kind {
                if !evaluate_condition(cond, &final_values, &schema.columns, self) {
                    return Err(StorageError::CheckConstraintViolation {
                        column: tc.name.clone().unwrap_or_else(|| "table check".to_string()),
                        reason: format!("CHECK constraint failed: {}", tc.raw),
                    });
                }
            }
        }

        // Append row to data file
        let data_path = self.data_path(table_name);
        self.snapshot_before_write(&data_path);
        let file = fs::OpenOptions::new().create(true).append(true).open(&data_path)?;
        let mut writer = BufWriter::new(file);
        let row_str = serialize_row(&final_values);
        writeln!(writer, "{}", row_str)?;
        writer.flush()?;

        // Also snapshot seq file if it was just incremented
        self.snapshot_index_files(table_name);
        self.rebuild_indexes_for_table(table_name)?;
        Ok(Some(final_values))
    }

    /// Apply DO UPDATE assignments when a conflict is detected; returns the final updated row.
    fn apply_conflict_update(
        &self,
        table_name: &str,
        schema: &CreateTableStatement,
        excluded_values: &[Value],  // the incoming (conflicting) row values
        assignments: &[crate::parser::Assignment],
    ) -> Result<Option<Vec<Value>>, StorageError> {
        // Build column context for EXCLUDED.col resolution
        let excluded_cols: Vec<(String, String)> = schema.columns.iter()
            .map(|c| ("EXCLUDED".to_string(), c.name.clone()))
            .collect();

        // Read all rows to find the conflicting one and update it
        let mut rows = self.read_rows(table_name)?;
        let unique_columns: Vec<(usize, &ColumnDefinition)> = schema.columns.iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key || c.unique)
            .collect();

        let mut updated_row: Option<Vec<Value>> = None;
        for row in &mut rows {
            // Find conflicting row
            let is_conflict = unique_columns.iter().any(|(i, _)| {
                excluded_values[*i] != Value::Null && row[*i] == excluded_values[*i]
            });
            if is_conflict {
                // Build combined context: target row cols + EXCLUDED cols
                let target_cols: Vec<(String, String)> = schema.columns.iter()
                    .map(|c| (table_name.to_string(), c.name.clone()))
                    .collect();
                let mut combined_cols: Vec<(String, String)> = target_cols;
                combined_cols.extend(excluded_cols.clone());
                let mut combined_row: Vec<Value> = row.clone();
                combined_row.extend(excluded_values.iter().cloned());

                for assignment in assignments {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == assignment.column) {
                        let new_val = if matches!(&assignment.value, Expression::Literal(Value::Default)) {
                            Some(self.default_value_for(&schema.columns[col_idx]))
                        } else {
                            resolve_expr_with_excluded(&assignment.value, &combined_row, &combined_cols)
                        };
                        row[col_idx] = new_val.unwrap_or(Value::Null);
                    }
                }
                updated_row = Some(row.clone());
                break;
            }
        }

        // Write rows back
        let data_path = self.data_path(table_name);
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(table_name);
        let file = fs::File::create(&data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            writeln!(writer, "{}", serialize_row(row))?;
        }
        writer.flush()?;
        self.rebuild_indexes_for_table(table_name)?;

        Ok(updated_row.map(Some).unwrap_or(None))
    }

    /// Like check_unique_indexes but aware of ON CONFLICT
    fn check_unique_indexes_conflict(
        &self,
        table_name: &str,
        values: &[Value],
        on_conflict: &Option<OnConflict>,
        schema: &CreateTableStatement,
    ) -> Result<(), StorageError> {
        let meta = self.load_index_meta()?;
        for (idx_name, t, col_name, unique) in &meta {
            if !unique || t != table_name { continue; }
            let col_idx = schema.columns.iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| StorageError::ColumnNotFound(col_name.clone()))?;
            let val = &values[col_idx];
            if *val == Value::Null { continue; }
            if let Some(row_nums) = self.lookup_index(idx_name, val)? {
                if !row_nums.is_empty() {
                    match on_conflict {
                        Some(OnConflict::DoNothing) => return Err(StorageError::DuplicateKey {
                            column: col_name.clone(),
                            value: format!("{:?}", val),
                        }),
                        _ => {
                            return Err(StorageError::DuplicateKey {
                                column: col_name.clone(),
                                value: format!("{:?}", val),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Update rows in a table matching the WHERE condition. Returns (count, returning_rows).
    pub fn update_rows(&self, stmt: &UpdateStatement) -> Result<(usize, Option<Vec<Vec<Value>>>), StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;

        // Build column context for the target table
        let target_cols: Vec<(String, String)> = schema.columns.iter()
            .map(|c| (stmt.table_name.clone(), c.name.clone()))
            .collect();

        // If FROM clause present, load the join table
        let (from_cols, from_rows): (Vec<(String, String)>, Vec<Vec<Value>>) =
            if let Some((from_table, from_alias)) = &stmt.from {
                let from_schema = self.load_schema(from_table)?;
                let alias = from_alias.as_deref().unwrap_or(from_table.as_str());
                let cols = from_schema.columns.iter()
                    .map(|c| (alias.to_string(), c.name.clone()))
                    .collect();
                let rows = self.read_rows(from_table)?;
                (cols, rows)
            } else {
                (Vec::new(), vec![Vec::new()])  // single empty join row when no FROM
            };

        // Validate assignment columns exist and literal types match upfront
        for assignment in &stmt.assignments {
            let col_def = schema.columns.iter().find(|c| c.name == assignment.column)
                .ok_or_else(|| StorageError::ColumnNotFound(assignment.column.clone()))?;
            // Check type immediately when the RHS is a plain literal
            if let Expression::Literal(val) = &assignment.value {
                if *val != Value::Default {
                    validate_value_type(val, &col_def.data_type, &assignment.column)?;
                }
            }
        }

        let mut rows = self.read_rows(&stmt.table_name)?;
        let mut updated_count = 0;
        let mut returning_rows: Vec<Vec<Value>> = Vec::new();

        // Determine which target row indices to update (handles FROM cross-join)
        let mut rows_to_update: Vec<bool> = vec![false; rows.len()];
        for (row_idx, row) in rows.iter().enumerate() {
            for from_row in &from_rows {
                // Build combined column context (target + from)
                let mut combined_row: Vec<Value> = row.clone();
                combined_row.extend(from_row.iter().cloned());
                let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                combined_cols.extend(from_cols.iter().cloned());

                let matches = match &stmt.where_clause {
                    Some(wc) => eval_condition_cols(&wc.condition, &combined_row, &combined_cols, self),
                    None => true,
                };
                if matches {
                    rows_to_update[row_idx] = true;
                    break;
                }
            }
        }

        // Apply assignments to matched rows
        for (row_idx, row) in rows.iter_mut().enumerate() {
            if !rows_to_update[row_idx] { continue; }

            // Build combined context using an empty from_row for expression resolution
            let mut combined_row: Vec<Value> = row.clone();
            let first_from = from_rows.first().cloned().unwrap_or_default();
            combined_row.extend(first_from.iter().cloned());
            let mut combined_cols: Vec<(String, String)> = target_cols.clone();
            combined_cols.extend(from_cols.iter().cloned());

            for assignment in &stmt.assignments {
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name == assignment.column) {
                    let new_val = if matches!(&assignment.value, Expression::Literal(Value::Default)) {
                        self.default_value_for(&schema.columns[col_idx])
                    } else {
                        resolve_expr_cols(&assignment.value, &combined_row, &combined_cols, self)
                            .unwrap_or(Value::Null)
                    };
                    // Type-check the resolved value
                    validate_value_type(&new_val, &schema.columns[col_idx].data_type, &assignment.column)?;
                    row[col_idx] = new_val;
                }
            }

            // Enforce CHECK constraints after applying updates
            for (_i, col_def) in schema.columns.iter().enumerate() {
                if let Some(ref check) = col_def.check_constraint {
                    if !evaluate_condition(check, row, &schema.columns, self) {
                        return Err(StorageError::CheckConstraintViolation {
                            column: col_def.name.clone(),
                            reason: format!("CHECK constraint failed: {:?}", check),
                        });
                    }
                }
            }
            for tc in &schema.constraints {
                if let TableConstraintKind::Check(cond) = &tc.kind {
                    if !evaluate_condition(cond, row, &schema.columns, self) {
                        return Err(StorageError::CheckConstraintViolation {
                            column: tc.name.clone().unwrap_or_else(|| "table check".to_string()),
                            reason: format!("CHECK constraint failed: {}", tc.raw),
                        });
                    }
                }
            }

            if stmt.returning.is_some() {
                let ret_row = project_returning(row, stmt.returning.as_ref().unwrap(), &schema.columns);
                returning_rows.push(ret_row);
            }
            updated_count += 1;
        }

        // Write all rows back
        let data_path = self.data_path(&stmt.table_name);
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&stmt.table_name);
        let file = fs::File::create(&data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            writeln!(writer, "{}", serialize_row(row))?;
        }
        writer.flush()?;

        self.rebuild_indexes_for_table(&stmt.table_name)?;
        let returning = if stmt.returning.is_some() { Some(returning_rows) } else { None };
        Ok((updated_count, returning))
    }

    /// Delete rows from a table matching the WHERE condition. Returns (count, returning_rows).
    pub fn delete_rows(&self, stmt: &DeleteStatement) -> Result<(usize, Option<Vec<Vec<Value>>>), StorageError> {
        let schema = self.load_schema(&stmt.table_name)?;
        let target_cols: Vec<(String, String)> = schema.columns.iter()
            .map(|c| (stmt.table_name.clone(), c.name.clone()))
            .collect();

        // If USING clause present, load the join table
        let (using_cols, using_rows): (Vec<(String, String)>, Vec<Vec<Value>>) =
            if let Some((using_table, using_alias)) = &stmt.using {
                let using_schema = self.load_schema(using_table)?;
                let alias = using_alias.as_deref().unwrap_or(using_table.as_str());
                let cols = using_schema.columns.iter()
                    .map(|c| (alias.to_string(), c.name.clone()))
                    .collect();
                let rows = self.read_rows(using_table)?;
                (cols, rows)
            } else {
                (Vec::new(), vec![Vec::new()])
            };

        let rows = self.read_rows(&stmt.table_name)?;
        let mut remaining_rows: Vec<Vec<Value>> = Vec::new();
        let mut deleted_rows: Vec<Vec<Value>> = Vec::new();

        for row in rows {
            let mut should_delete = false;
            for using_row in &using_rows {
                let mut combined_row: Vec<Value> = row.clone();
                combined_row.extend(using_row.iter().cloned());
                let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                combined_cols.extend(using_cols.iter().cloned());

                let matches = match &stmt.where_clause {
                    Some(wc) => eval_condition_cols(&wc.condition, &combined_row, &combined_cols, self),
                    None => true,
                };
                if matches {
                    should_delete = true;
                    break;
                }
            }
            if should_delete {
                deleted_rows.push(row);
            } else {
                remaining_rows.push(row);
            }
        }

        // Check FK constraints on deleted rows
        for (i, col) in schema.columns.iter().enumerate() {
            if col.primary_key {
                let deleted_values: Vec<Value> = deleted_rows.iter().map(|r| r[i].clone()).collect();
                if !deleted_values.is_empty() {
                    self.check_fk_references(&stmt.table_name, &col.name, &deleted_values)?;
                }
            }
        }

        // Build RETURNING rows before writing
        let returning_rows: Vec<Vec<Value>> = if let Some(ref ret_cols) = stmt.returning {
            deleted_rows.iter()
                .map(|row| project_returning(row, ret_cols, &schema.columns))
                .collect()
        } else {
            Vec::new()
        };

        let deleted_count = deleted_rows.len();

        // Write remaining rows back
        let data_path = self.data_path(&stmt.table_name);
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&stmt.table_name);
        let file = fs::File::create(&data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &remaining_rows {
            writeln!(writer, "{}", serialize_row(row))?;
        }
        writer.flush()?;

        self.rebuild_indexes_for_table(&stmt.table_name)?;
        let returning = if stmt.returning.is_some() { Some(returning_rows) } else { None };
        Ok((deleted_count, returning))
    }

    /// Truncate a table (delete all rows, keep schema)
    pub fn truncate_table(&self, stmt: &TruncateStatement) -> Result<(), StorageError> {
        if !self.table_exists(&stmt.table_name) {
            return Err(StorageError::TableNotFound(stmt.table_name.clone()));
        }
        let data_path = self.data_path(&stmt.table_name);
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&stmt.table_name);
        fs::write(&data_path, "")?;
        self.rebuild_indexes_for_table(&stmt.table_name)?;
        Ok(())
    }

    /// Execute a MERGE statement. Returns (matched_count, inserted_count).
    pub fn execute_merge(&self, stmt: &MergeStatement) -> Result<(usize, usize), StorageError> {
        let target_schema = self.load_schema(&stmt.target)?;
        let target_alias = stmt.target_alias.as_deref().unwrap_or(&stmt.target);

        // Materialize source rows
        let (source_cols, source_rows): (Vec<(String, String)>, Vec<Vec<Value>>) = match &stmt.source {
            MergeSource::Table(tbl) => {
                let schema = self.load_schema(tbl)?;
                let alias = stmt.source_alias.as_deref().unwrap_or(tbl.as_str());
                let cols = schema.columns.iter().map(|c| (alias.to_string(), c.name.clone())).collect();
                let rows = self.read_rows(tbl)?;
                (cols, rows)
            }
            MergeSource::Values(rows_exprs, col_names) => {
                let alias = stmt.source_alias.as_deref().unwrap_or("src");
                let mat_rows: Vec<Vec<Value>> = rows_exprs.iter().map(|exprs| {
                    exprs.iter().map(|e| {
                        resolve_expression(e, &[], &[], self).unwrap_or(Value::Null)
                    }).collect()
                }).collect();
                let ncols = mat_rows.first().map(|r| r.len()).unwrap_or(0);
                let cols: Vec<(String, String)> = (0..ncols).map(|i| {
                    let name = col_names.get(i).cloned().unwrap_or_else(|| format!("column{}", i + 1));
                    (alias.to_string(), name)
                }).collect();
                (cols, mat_rows)
            }
            MergeSource::Subquery(_) => return Err(StorageError::InvalidData("MERGE with subquery source not yet supported".to_string())),
        };

        let target_cols: Vec<(String, String)> = target_schema.columns.iter()
            .map(|c| (target_alias.to_string(), c.name.clone()))
            .collect();

        let mut target_rows = self.read_rows(&stmt.target)?;
        let mut matched_count = 0usize;
        let mut inserted_count = 0usize;

        for src_row in &source_rows {
            // Build combined context for ON condition evaluation
            let mut matched_idx: Option<usize> = None;
            for (i, tgt_row) in target_rows.iter().enumerate() {
                let mut combined_row: Vec<Value> = tgt_row.clone();
                combined_row.extend(src_row.iter().cloned());
                let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                combined_cols.extend(source_cols.iter().cloned());
                if eval_condition_cols(&stmt.on, &combined_row, &combined_cols, self) {
                    matched_idx = Some(i);
                    break;
                }
            }

            if let Some(idx) = matched_idx {
                // WHEN MATCHED – try clauses in order
                let tgt_row = &mut target_rows[idx];
                for clause in &stmt.when_clauses {
                    if !clause.is_matched { continue; }
                    if let Some(ref cond) = clause.condition {
                        let mut combined_row: Vec<Value> = tgt_row.clone();
                        combined_row.extend(src_row.iter().cloned());
                        let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                        combined_cols.extend(source_cols.iter().cloned());
                        if !eval_condition_cols(cond, &combined_row, &combined_cols, self) {
                            continue;
                        }
                    }
                    match &clause.action {
                        MergeAction::Update(assignments) => {
                            let mut combined_row: Vec<Value> = tgt_row.clone();
                            combined_row.extend(src_row.iter().cloned());
                            let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                            combined_cols.extend(source_cols.iter().cloned());
                            for assignment in assignments {
                                if let Some(col_idx) = target_schema.columns.iter().position(|c| c.name == assignment.column) {
                                    let new_val = if matches!(&assignment.value, Expression::Literal(Value::Default)) {
                                        self.default_value_for(&target_schema.columns[col_idx])
                                    } else {
                                        resolve_expr_cols(&assignment.value, &combined_row, &combined_cols, self)
                                            .unwrap_or(Value::Null)
                                    };
                                    tgt_row[col_idx] = new_val;
                                }
                            }
                            matched_count += 1;
                        }
                        MergeAction::Delete => {
                            target_rows.remove(idx);
                            matched_count += 1;
                        }
                        MergeAction::DoNothing => {}
                        _ => {}
                    }
                    break; // first matching clause executed
                }
            } else {
                // WHEN NOT MATCHED – try clauses in order
                for clause in &stmt.when_clauses {
                    if clause.is_matched { continue; }
                    if let Some(ref cond) = clause.condition {
                        let mut combined_row: Vec<Value> = vec![Value::Null; target_schema.columns.len()];
                        combined_row.extend(src_row.iter().cloned());
                        let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                        combined_cols.extend(source_cols.iter().cloned());
                        if !eval_condition_cols(cond, &combined_row, &combined_cols, self) {
                            continue;
                        }
                    }
                    match &clause.action {
                        MergeAction::Insert(col_names, exprs) => {
                            let mut combined_row: Vec<Value> = vec![Value::Null; target_schema.columns.len()];
                            combined_row.extend(src_row.iter().cloned());
                            let mut combined_cols: Vec<(String, String)> = target_cols.clone();
                            combined_cols.extend(source_cols.iter().cloned());

                            let vals: Vec<Value> = exprs.iter()
                                .map(|e| resolve_expr_cols(e, &combined_row, &combined_cols, self).unwrap_or(Value::Null))
                                .collect();

                            let new_row = if col_names.is_empty() {
                                vals
                            } else {
                                target_schema.columns.iter().map(|col| {
                                    col_names.iter().position(|cn| cn.eq_ignore_ascii_case(&col.name))
                                        .and_then(|i| vals.get(i).cloned())
                                        .unwrap_or(Value::Null)
                                }).collect()
                            };

                            target_rows.push(new_row);
                            inserted_count += 1;
                        }
                        MergeAction::DoNothing => {}
                        _ => {}
                    }
                    break; // first matching clause executed
                }
            }
        }

        // Write all target rows back
        let data_path = self.data_path(&stmt.target);
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&stmt.target);
        let file = fs::File::create(&data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &target_rows {
            writeln!(writer, "{}", serialize_row(row))?;
        }
        writer.flush()?;
        self.rebuild_indexes_for_table(&stmt.target)?;

        Ok((matched_count, inserted_count))
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

        // Parse column definitions and table-level constraints
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if let Some(rest) = line.strip_prefix("!TC=") {
                let raw = Self::decode_check_text(rest);
                match crate::parser::parse_table_constraint(&raw) {
                    Ok((_, tc)) => constraints.push(tc),
                    Err(_) => return Err(StorageError::InvalidSchema(
                        format!("Invalid table constraint: {}", raw)
                    )),
                }
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

            let check_text = flags.iter()
                .find(|f| f.starts_with("CK="))
                .map(|f| Self::decode_check_text(&f[3..]));
            let check_constraint = match &check_text {
                Some(text) => {
                    match crate::parser::parse_condition(text.trim()) {
                        Ok(("", cond)) => Some(cond),
                        _ => return Err(StorageError::InvalidSchema(
                            format!("Invalid CHECK constraint: {}", text)
                        )),
                    }
                }
                None => None,
            };

            let default_text = flags.iter()
                .find(|f| f.starts_with("DF="))
                .map(|f| Self::decode_check_text(&f[3..]));
            let default = match &default_text {
                Some(text) => {
                    match crate::parser::parse_expression(text.trim()) {
                        Ok(("", expr)) => Some(expr),
                        _ => return Err(StorageError::InvalidSchema(
                            format!("Invalid DEFAULT expression: {}", text)
                        )),
                    }
                }
                None => None,
            };

            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
                auto_increment,
                primary_key,
                not_null,
                unique,
                references,
                check_constraint,
                check_constraint_text: check_text,
                default,
                default_text,
        });
        }

        Ok(CreateTableStatement {
            table_name: table_name.to_string(),
            constraints,
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

        // Snapshot everything before deletion
        self.snapshot_before_write(&schema_path);
        self.snapshot_before_write(&data_path);
        let seq_path = self.seq_path(table_name);
        self.snapshot_before_write(&seq_path);
        self.snapshot_index_files(table_name);

        fs::remove_file(&schema_path)?;

        if data_path.exists() {
            fs::remove_file(&data_path)?;
        }

        if seq_path.exists() {
            fs::remove_file(&seq_path)?;
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
            let mut file = fs::File::create(&meta_path)?;
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
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&schema.table_name);
        let file = fs::File::create(&data_path)?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            let mut new_row = row.clone();
            new_row.push(Value::Null);
            writeln!(writer, "{}", serialize_row(&new_row))?;
        }
        writer.flush()?;

        self.write_schema_file(&schema.table_name, &new_columns, &schema.constraints)?;

        // Initialize sequence file if this is the first auto_increment column
        if col.auto_increment && !schema.columns.iter().any(|c| c.auto_increment) {
            let seq_path = self.seq_path(&schema.table_name);
            self.snapshot_before_write(&seq_path);
            fs::write(&seq_path, "0")?;
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
        self.snapshot_before_write(&data_path);
        self.snapshot_index_files(&schema.table_name);
        let file = fs::File::create(&data_path)?;
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
        // Drop table constraints that reference the removed column
        let kept_constraints: Vec<TableConstraint> = schema.constraints.iter()
            .filter(|tc| {
                let cols: &[String] = match &tc.kind {
                    TableConstraintKind::PrimaryKey(c) | TableConstraintKind::Unique(c) => c,
                    TableConstraintKind::ForeignKey { columns, .. } => columns,
                    TableConstraintKind::Check(_) => return true,
                };
                !cols.iter().any(|c| c.eq_ignore_ascii_case(col_name))
            })
            .cloned()
            .collect();
        self.write_schema_file(&schema.table_name, &new_columns, &kept_constraints)?;

        // Remove sequence file if no auto_increment columns remain
        let dropped_col = &schema.columns[col_idx];
        if dropped_col.auto_increment && !new_columns.iter().any(|c| c.auto_increment) {
            let seq_path = self.seq_path(&schema.table_name);
            self.snapshot_before_write(&seq_path);
            if seq_path.exists() {
                fs::remove_file(&seq_path)?;
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
        // Rewrite column names inside table constraints (raw text is regenerated
        // from the parsed form so the schema file stays consistent)
        let renamed_constraints: Vec<TableConstraint> = schema.constraints.iter()
            .map(|tc| {
                let mut tc = tc.clone();
                let rename = |cols: &mut Vec<String>| {
                    for c in cols.iter_mut() {
                        if c.eq_ignore_ascii_case(from) { *c = to.to_string(); }
                    }
                };
                match &mut tc.kind {
                    TableConstraintKind::PrimaryKey(c) | TableConstraintKind::Unique(c) => rename(c),
                    TableConstraintKind::ForeignKey { columns, .. } => rename(columns),
                    TableConstraintKind::Check(_) => {}
                }
                tc.raw = Self::constraint_to_sql(&tc);
                tc
            })
            .collect();
        self.write_schema_file(&schema.table_name, &new_columns, &renamed_constraints)?;

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
                self.write_schema_file(t, &updated, &other.constraints)?;
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

        // Snapshot old files and new destinations before touching anything
        let old_schema = self.schema_path(old_name);
        let new_schema = self.schema_path(new_name);
        self.snapshot_before_write(&old_schema);
        self.snapshot_before_write(&new_schema);
        let old_data = self.data_path(old_name);
        let new_data = self.data_path(new_name);
        self.snapshot_before_write(&old_data);
        self.snapshot_before_write(&new_data);
        let old_seq = self.seq_path(old_name);
        let new_seq = self.seq_path(new_name);
        self.snapshot_before_write(&old_seq);
        self.snapshot_before_write(&new_seq);
        self.snapshot_index_files(old_name);

        // Rewrite schema with new table name (first line) at the new path
        let schema = self.load_schema(old_name)?;
        self.write_schema_file(new_name, &schema.columns, &schema.constraints)?;
        fs::remove_file(&old_schema)?;

        // Rename data file
        if old_data.exists() {
            fs::rename(&old_data, &new_data)?;
        }

        // Rename sequence file
        if old_seq.exists() {
            fs::rename(&old_seq, &new_seq)?;
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
                self.write_schema_file(t, &updated_cols, &other.constraints)?;
            }
        }

        Ok(())
    }

    fn write_index_meta(&self, entries: &[(String, String, String, bool)]) -> Result<(), StorageError> {
        let path = self.index_meta_path();
        self.snapshot_before_write(&path);
        if entries.is_empty() {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            return Ok(());
        }
        let mut file = fs::File::create(&path)?;
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

    fn view_path(&self, view_name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.view", view_name))
    }

    /// Create a view by persisting its SELECT SQL to disk
    pub fn create_view(&self, view_name: &str, select_sql: &str) -> Result<(), StorageError> {
        let path = self.view_path(view_name);
        if path.exists() {
            return Err(StorageError::InvalidSchema(format!("View '{}' already exists", view_name)));
        }
        self.snapshot_before_write(&path);
        fs::write(&path, select_sql).map_err(StorageError::IoError)
    }

    /// Load a view's SELECT SQL from disk
    pub fn load_view(&self, view_name: &str) -> Result<Option<String>, StorageError> {
        let path = self.view_path(view_name);
        if !path.exists() {
            return Ok(None);
        }
        fs::read_to_string(path).map(Some).map_err(StorageError::IoError)
    }

    /// Drop a view
    pub fn drop_view(&self, view_name: &str) -> Result<(), StorageError> {
        let path = self.view_path(view_name);
        if !path.exists() {
            return Err(StorageError::TableNotFound(format!("View '{}' not found", view_name)));
        }
        self.snapshot_before_write(&path);
        fs::remove_file(&path).map_err(StorageError::IoError)
    }

    pub fn view_exists(&self, view_name: &str) -> bool {
        self.view_path(view_name).exists()
    }

    /// List all views in the database
    pub fn list_views(&self) -> io::Result<Vec<String>> {
        let mut views = Vec::new();
        if !self.data_dir.exists() {
            return Ok(views);
        }
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "view" {
                    if let Some(stem) = path.file_stem() {
                        if let Some(name) = stem.to_str() {
                            views.push(name.to_string());
                        }
                    }
                }
            }
        }
        views.sort();
        Ok(views)
    }

    // ---- User-defined functions ----

    fn function_path(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!("{}.func", name))
    }

    /// Create a user-defined function by persisting its SQL definition to disk
    pub fn create_function(&self, stmt: &CreateFunctionStatement) -> Result<(), StorageError> {
        let path = self.function_path(&stmt.name);
        if path.exists() {
            return Err(StorageError::InvalidSchema(format!("Function '{}' already exists", stmt.name)));
        }
        self.snapshot_before_write(&path);
        // Serialize as: name|param1:type1,param2:type2|return_type|body_expression_sql
        let params_str = stmt.params.iter()
            .map(|(n, t)| format!("{}:{}", n, t))
            .collect::<Vec<_>>()
            .join(",");
        let return_str = stmt.return_type.as_deref().unwrap_or("");
        let serialized = format!("{}\n{}\n{}\n", params_str, return_str, stmt.name);
        fs::write(&path, serialized).map_err(StorageError::IoError)
    }

    /// Load a user-defined function definition from disk
    pub fn load_function(&self, name: &str) -> Result<Option<CreateFunctionStatement>, StorageError> {
        let path = self.function_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read_to_string(&path).map_err(StorageError::IoError)?;
        let mut lines = data.lines();
        let params_str = lines.next().unwrap_or("");
        let return_str = lines.next().unwrap_or("");
        let body_sql = lines.next().unwrap_or("");
        let params: Vec<(String, String)> = if params_str.is_empty() {
            Vec::new()
        } else {
            params_str.split(',')
                .map(|p| {
                    let mut parts = p.splitn(2, ':');
                    let pname = parts.next().unwrap_or("").to_string();
                    let ptype = parts.next().unwrap_or("").to_string();
                    (pname, ptype)
                })
                .collect()
        };
        let return_type = if return_str.is_empty() { None } else { Some(return_str.to_string()) };
        let (_, body) = crate::parser::parse_expression(body_sql)
            .map_err(|e| StorageError::InvalidSchema(format!("Invalid function body: {:?}", e)))?;
        Ok(Some(CreateFunctionStatement {
            name: name.to_string(),
            params,
            return_type,
            body: Box::new(body),
        }))
    }

    /// Drop a function
    pub fn drop_function(&self, name: &str, if_exists: bool) -> Result<(), StorageError> {
        let path = self.function_path(name);
        if !path.exists() {
            if if_exists {
                return Ok(());
            }
            return Err(StorageError::TableNotFound(format!("Function '{}' not found", name)));
        }
        self.snapshot_before_write(&path);
        fs::remove_file(&path).map_err(StorageError::IoError)
    }

    pub fn function_exists(&self, name: &str) -> bool {
        self.function_path(name).exists()
    }

    /// List all user-defined functions
    pub fn list_functions(&self) -> io::Result<Vec<String>> {
        let mut funcs = Vec::new();
        if !self.data_dir.exists() {
            return Ok(funcs);
        }
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "func" {
                    if let Some(stem) = path.file_stem() {
                        if let Some(name) = stem.to_str() {
                            funcs.push(name.to_string());
                        }
                    }
                }
            }
        }
        funcs.sort();
        Ok(funcs)
    }

    /// Read and increment the auto_increment counter
    fn next_auto_increment(&self, table_name: &str) -> Result<i64, StorageError> {
        let seq_path = self.seq_path(table_name);
        self.snapshot_before_write(&seq_path);
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
    /// Validate a table-level FOREIGN KEY: the tuple of values must exist in the
    /// referenced table. Empty ref_columns means the referenced table's PRIMARY KEY.
    fn validate_composite_foreign_key(
        &self,
        values: &[Value],
        schema: &CreateTableStatement,
        columns: &[String],
        ref_table: &str,
        ref_columns: &[String],
    ) -> Result<(), StorageError> {
        let idxs: Vec<usize> = columns.iter()
            .filter_map(|c| schema.columns.iter().position(|col| col.name.eq_ignore_ascii_case(c)))
            .collect();
        if idxs.len() != columns.len() { return Ok(()); }
        // Tuples containing NULL are exempt (spec MATCH SIMPLE behaviour)
        if idxs.iter().any(|&i| values[i] == Value::Null) { return Ok(()); }

        let ref_schema = self.load_schema(ref_table)?;
        let ref_cols: Vec<String> = if ref_columns.is_empty() {
            let mut pk: Vec<String> = ref_schema.columns.iter()
                .filter(|c| c.primary_key)
                .map(|c| c.name.clone())
                .collect();
            for tc in &ref_schema.constraints {
                if let TableConstraintKind::PrimaryKey(cols) = &tc.kind {
                    pk = cols.clone();
                }
            }
            pk
        } else {
            ref_columns.to_vec()
        };
        let ref_idxs: Vec<usize> = ref_cols.iter()
            .filter_map(|c| ref_schema.columns.iter().position(|col| col.name.eq_ignore_ascii_case(c)))
            .collect();
        if ref_idxs.len() != idxs.len() {
            return Err(StorageError::InvalidSchema(
                format!("FOREIGN KEY column count mismatch referencing {}", ref_table)
            ));
        }

        let ref_rows = self.read_rows(ref_table)?;
        let exists = ref_rows.iter().any(|row| {
            idxs.iter().zip(ref_idxs.iter()).all(|(&i, &ri)| row[ri] == values[i])
        });
        if !exists {
            return Err(StorageError::ForeignKeyViolation {
                column: columns.join(", "),
                ref_table: ref_table.to_string(),
                ref_column: ref_cols.join(", "),
            });
        }
        Ok(())
    }

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

        // Write index data (snapshot the new idx file — it doesn't exist yet)
        let idx_path = self.index_data_path(&stmt.index_name);
        self.snapshot_before_write(&idx_path);
        self.write_index_data(&stmt.index_name, &index)?;

        // Append to metadata (snapshot meta before first write)
        let meta_path = self.index_meta_path();
        self.snapshot_before_write(&meta_path);
        let mut file = fs::OpenOptions::new().create(true).append(true).open(&meta_path)?;
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
        self.snapshot_before_write(&idx_path);
        if idx_path.exists() {
            fs::remove_file(&idx_path)?;
        }

        // Rewrite metadata without this index
        let remaining: Vec<_> = meta.iter().filter(|(name, _, _, _)| name != index_name).collect();
        let meta_path = self.index_meta_path();
        self.snapshot_before_write(&meta_path);
        let mut file = fs::File::create(&meta_path)?;
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
        self.snapshot_before_write(&path);
        let mut file = fs::File::create(&path)?;
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

    /// List all index names
    pub fn list_indexes(&self) -> io::Result<Vec<String>> {
        let meta = match self.load_index_meta() {
            Ok(m) => m,
            Err(_) => return Ok(Vec::new()),
        };
        let mut names: Vec<String> = meta.into_iter().map(|(name, _, _, _)| name).collect();
        names.sort();
        Ok(names)
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

/// Restore files from a before-image snapshot (used by rollback / rollback to savepoint)
fn restore_files(images: &HashMap<PathBuf, Option<Vec<u8>>>) -> Result<(), StorageError> {
    for (path, maybe_bytes) in images {
        match maybe_bytes {
            Some(bytes) => fs::write(path, bytes)?,
            None => { let _ = fs::remove_file(path); } // file didn't exist — remove it
        }
    }
    Ok(())
}

/// Convert a DataType to its string representation
fn data_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Int => "INT".to_string(),
        DataType::SmallInt => "SMALLINT".to_string(),
        DataType::BigInt => "BIGINT".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Varchar(Some(size)) => format!("VARCHAR({})", size),
        DataType::Varchar(None) => "VARCHAR".to_string(),
        DataType::Char(Some(size)) => format!("CHAR({})", size),
        DataType::Char(None) => "CHAR".to_string(),
        DataType::Text => "TEXT".to_string(),
        DataType::Decimal(Some(p), Some(s)) => format!("DECIMAL({},{})", p, s),
        DataType::Decimal(Some(p), None) => format!("DECIMAL({})", p),
        DataType::Decimal(None, _) => "DECIMAL".to_string(),
        DataType::Uuid => "UUID".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Jsonb => "JSONB".to_string(),
    }
}

/// Parse a data type from string representation (schema file format)
fn parse_data_type(s: &str) -> Result<DataType, StorageError> {
    if s == "INT" || s == "INTEGER" { return Ok(DataType::Int); }
    if s == "SMALLINT" { return Ok(DataType::SmallInt); }
    if s == "BIGINT" { return Ok(DataType::BigInt); }
    if s == "FLOAT" { return Ok(DataType::Float); }
    if s == "REAL" { return Ok(DataType::Real); }
    if s == "DOUBLE" { return Ok(DataType::Double); }
    if s == "BOOLEAN" || s == "BOOL" { return Ok(DataType::Boolean); }
    if s == "DATE" { return Ok(DataType::Date); }
    if s == "TIMESTAMP" { return Ok(DataType::Timestamp); }
    if s == "TEXT" { return Ok(DataType::Text); }
    if s == "UUID" { return Ok(DataType::Uuid); }
    if s == "JSON" { return Ok(DataType::Json); }
    if s == "JSONB" { return Ok(DataType::Jsonb); }
    if s == "VARCHAR" { return Ok(DataType::Varchar(None)); }
    if s.starts_with("VARCHAR(") && s.ends_with(')') {
        let size_str = &s[8..s.len()-1];
        let size = size_str.parse::<usize>()
            .map_err(|_| StorageError::InvalidSchema(format!("Invalid VARCHAR size: {}", size_str)))?;
        return Ok(DataType::Varchar(Some(size)));
    }
    if s == "CHAR" { return Ok(DataType::Char(None)); }
    if s.starts_with("CHAR(") && s.ends_with(')') {
        let size_str = &s[5..s.len()-1];
        let size = size_str.parse::<usize>()
            .map_err(|_| StorageError::InvalidSchema(format!("Invalid CHAR size: {}", size_str)))?;
        return Ok(DataType::Char(Some(size)));
    }
    if s == "DECIMAL" || s == "NUMERIC" { return Ok(DataType::Decimal(None, None)); }
    if s.starts_with("DECIMAL(") || s.starts_with("NUMERIC(") {
        let inner_start = s.find('(').unwrap() + 1;
        let inner = &s[inner_start..s.len()-1];
        let parts: Vec<&str> = inner.split(',').collect();
        let p = parts[0].trim().parse::<u8>().ok();
        let sc = parts.get(1).and_then(|x| x.trim().parse::<u8>().ok());
        return Ok(DataType::Decimal(p, sc));
    }
    Err(StorageError::InvalidSchema(format!("Unknown data type: {}", s)))
}

/// Validate that a value matches the expected data type
fn validate_value_type(value: &Value, data_type: &DataType, column_name: &str) -> Result<(), StorageError> {
    match (value, data_type) {
        (Value::Null, _) => Ok(()), // NULL is valid for any type
        (Value::Int(_), DataType::Int | DataType::SmallInt | DataType::BigInt) => Ok(()),
        (Value::Float(_), DataType::Float | DataType::Real | DataType::Double) => Ok(()),
        (Value::Float(_), DataType::Decimal(_, _)) => Ok(()),
        (Value::Int(_), DataType::Float | DataType::Real | DataType::Double) => Ok(()),
        (Value::Int(_), DataType::Decimal(_, _)) => Ok(()),
        (Value::Bool(_), DataType::Boolean) => Ok(()),
        (Value::Date(_), DataType::Date) => Ok(()),
        (Value::Timestamp(_), DataType::Timestamp) => Ok(()),
        (Value::String(s), DataType::Date) => validate_date_format(s, column_name),
        (Value::String(s), DataType::Timestamp) => validate_timestamp_format(s, column_name),
        (Value::String(_), DataType::Varchar(_) | DataType::Char(_) | DataType::Text) => Ok(()),
        (Value::Json(_) | Value::String(_), DataType::Json | DataType::Jsonb) => Ok(()),
        (Value::String(_), DataType::Uuid) => Ok(()), // UUID stored as string
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
/// Execute a scalar subquery and return the first column of the first row
fn execute_scalar_subquery(stmt: &SelectStatement, storage: &Storage) -> Option<Value> {
    let table_name = match &stmt.from {
        FromClause::Table(t) => t.clone(),
        _ => return None, // nested subquery FROM not supported here
    };
    let (schema, rows) = if Storage::is_metadata_table(&table_name) {
        let schema = Storage::metadata_schema(&table_name)?;
        let rows = storage.read_metadata_rows(&table_name)?;
        (schema, rows)
    } else {
        let schema = storage.load_schema(&table_name).ok()?;
        let rows = storage.read_rows(&table_name).ok()?;
        (schema, rows)
    };

    let filtered: Vec<Vec<Value>> = rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => evaluate_condition(&wc.condition, row, &schema.columns, storage),
            None => true,
        })
        .collect();

    // Return first column of first row
    let first_row = filtered.into_iter().next()?;
    match stmt.columns.first() {
        Some(SelectColumn::Column(name)) => {
            let idx = schema.columns.iter().position(|c| c.name == *name)?;
            first_row.get(idx).cloned()
        }
        Some(SelectColumn::Expr(_)) | Some(SelectColumn::All) => first_row.into_iter().next(),
        _ => first_row.into_iter().next(),
    }
}

/// Execute a correlated subquery; unresolved columns in WHERE fall back to the outer row/schema.
fn execute_correlated_scalar_subquery(
    stmt: &SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_schema: &[ColumnDefinition],
) -> Option<Value> {
    let table_name = match &stmt.from {
        FromClause::Table(t) => t.clone(),
        _ => return None,
    };
    let (schema, rows) = if Storage::is_metadata_table(&table_name) {
        let schema = Storage::metadata_schema(&table_name)?;
        let rows = storage.read_metadata_rows(&table_name)?;
        (schema, rows)
    } else {
        let schema = storage.load_schema(&table_name).ok()?;
        let rows = storage.read_rows(&table_name).ok()?;
        (schema, rows)
    };

    let filtered: Vec<Vec<Value>> = rows.into_iter()
        .filter(|row| match &stmt.where_clause {
            Some(wc) => evaluate_correlated_condition_storage(
                &wc.condition, row, &schema.columns, storage, outer_row, outer_schema
            ),
            None => true,
        })
        .collect();

    let first_row = filtered.into_iter().next()?;
    match stmt.columns.first() {
        Some(SelectColumn::Column(name)) => {
            let idx = schema.columns.iter().position(|c| c.name == *name)?;
            first_row.get(idx).cloned()
        }
        _ => first_row.into_iter().next(),
    }
}

/// Evaluate condition with fallback to outer row context for correlated references
fn evaluate_correlated_condition_storage(
    condition: &Condition,
    row: &[Value],
    schema: &[ColumnDefinition],
    storage: &Storage,
    outer_row: &[Value],
    outer_schema: &[ColumnDefinition],
) -> bool {
    match condition {
        Condition::And(l, r) => {
            evaluate_correlated_condition_storage(l, row, schema, storage, outer_row, outer_schema)
                && evaluate_correlated_condition_storage(r, row, schema, storage, outer_row, outer_schema)
        }
        Condition::Or(l, r) => {
            evaluate_correlated_condition_storage(l, row, schema, storage, outer_row, outer_schema)
                || evaluate_correlated_condition_storage(r, row, schema, storage, outer_row, outer_schema)
        }
        Condition::Not(inner) => !evaluate_correlated_condition_storage(inner, row, schema, storage, outer_row, outer_schema),
        Condition::Comparison { left, operator, right, upper_bound } => {
            let lv = resolve_correlated_expr_storage(left, row, schema, storage, outer_row, outer_schema);
            let rv = resolve_correlated_expr_storage(right, row, schema, storage, outer_row, outer_schema);
            if *operator == Operator::IsNull || *operator == Operator::IsNotNull {
                let is_null = matches!(lv, Some(Value::Null) | None);
                return if *operator == Operator::IsNull { is_null } else { !is_null };
            }
            if *operator == Operator::Between || *operator == Operator::NotBetween {
                let high = upper_bound.as_ref().and_then(|e| resolve_correlated_expr_storage(e, row, schema, storage, outer_row, outer_schema));
                let in_range = matches!((&lv, &rv, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &Operator::GreaterThanOrEqual, l) && compare_values(v, &Operator::LessThanOrEqual, h));
                return if *operator == Operator::Between { in_range } else { !in_range };
            }
            if *operator == Operator::Similar || *operator == Operator::NotSimilar {
                let escape = upper_bound.as_ref().and_then(|e| resolve_correlated_expr_storage(e, row, schema, storage, outer_row, outer_schema));
                let similar = match (&lv, &rv) {
                    (Some(Value::String(s)), Some(Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = similar_to_regex(p, escape_char);
                        Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == Operator::Similar { similar } else { !similar };
            }
            match (&lv, &rv) {
                (Some(l), Some(r)) => compare_values(l, operator, r),
                _ => false,
            }
        }
        Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr_storage(left, row, schema, storage, outer_row, outer_schema) { Some(v) => v, None => return false };
            match execute_scalar_subquery(subquery, storage) {
                Some(rv) => compare_values(&lv, op, &rv),
                None => false,
            }
        }
        Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_correlated_expr_storage(left, row, schema, storage, outer_row, outer_schema) { Some(v) => v, None => return false };
            match execute_scalar_subquery(subquery, storage) {
                Some(rv) => compare_values(&lv, op, &rv),
                None => true,
            }
        }
        Condition::Unique(_) | Condition::NotUnique(_) | Condition::Overlaps(..) => false,
    }
}

/// Resolve expression with fallback to outer schema for correlated column references
fn resolve_correlated_expr_storage(
    expr: &Expression,
    row: &[Value],
    schema: &[ColumnDefinition],
    storage: &Storage,
    outer_row: &[Value],
    outer_schema: &[ColumnDefinition],
) -> Option<Value> {
    match expr {
        Expression::Column(name) => {
            // Try inner schema first
            if let Some(idx) = schema.iter().position(|c| c.name == *name) {
                return Some(row[idx].clone());
            }
            // Fall back to outer schema
            outer_schema.iter().position(|c| c.name == *name).map(|idx| outer_row[idx].clone())
        }
        Expression::QualifiedColumn(_, col) => {
            if let Some(idx) = schema.iter().position(|c| c.name == *col) {
                return Some(row[idx].clone());
            }
            outer_schema.iter().position(|c| c.name == *col).map(|idx| outer_row[idx].clone())
        }
        _ => resolve_expression(expr, row, schema, storage),
    }
}

/// Project RETURNING columns from a row using schema column names
fn project_returning(row: &[Value], ret_cols: &[SelectColumn], schema_cols: &[ColumnDefinition]) -> Vec<Value> {
    ret_cols.iter().map(|col| {
        match col {
            SelectColumn::All => row.first().cloned().unwrap_or(Value::Null),
            SelectColumn::Column(name) => {
                schema_cols.iter().position(|c| c.name.eq_ignore_ascii_case(name))
                    .and_then(|i| row.get(i).cloned())
                    .unwrap_or(Value::Null)
            }
            SelectColumn::Alias(inner, _) => {
                if let SelectColumn::Column(name) = inner.as_ref() {
                    schema_cols.iter().position(|c| c.name.eq_ignore_ascii_case(name))
                        .and_then(|i| row.get(i).cloned())
                        .unwrap_or(Value::Null)
                } else { Value::Null }
            }
            SelectColumn::Expr(e) => resolve_expression(e, row, schema_cols, &Storage::noop())
                .unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }).collect()
}

/// Evaluate a condition using (table_alias, column_name) column context
fn eval_condition_cols(condition: &Condition, row: &[Value], cols: &[(String, String)], storage: &Storage) -> bool {
    // Convert (alias, col) context to a fake schema so we can reuse resolve_expr_cols
    match condition {
        Condition::And(l, r) => eval_condition_cols(l, row, cols, storage) && eval_condition_cols(r, row, cols, storage),
        Condition::Or(l, r) => eval_condition_cols(l, row, cols, storage) || eval_condition_cols(r, row, cols, storage),
        Condition::Not(inner) => !eval_condition_cols(inner, row, cols, storage),
        Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == Operator::IsNull || *operator == Operator::IsNotNull {
                let lv = resolve_expr_cols(left, row, cols, storage);
                let is_null = matches!(lv, Some(Value::Null) | None);
                return if *operator == Operator::IsNull { is_null } else { !is_null };
            }
            if *operator == Operator::Between || *operator == Operator::NotBetween {
                let val = resolve_expr_cols(left, row, cols, storage);
                let low = resolve_expr_cols(right, row, cols, storage);
                let high = upper_bound.as_ref().and_then(|e| resolve_expr_cols(e, row, cols, storage));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &Operator::GreaterThanOrEqual, l) && compare_values(v, &Operator::LessThanOrEqual, h));
                return if *operator == Operator::Between { in_range } else { !in_range };
            }
            if *operator == Operator::Exists || *operator == Operator::NotExists {
                if let Expression::Subquery(subquery) = right {
                    // Build a fake schema from the (alias, col) context for correlated lookup
                    let fake_schema: Vec<ColumnDefinition> = cols.iter().map(|(_, name)| {
                        ColumnDefinition {
                            name: name.clone(),
                            data_type: DataType::Varchar(None),
                            auto_increment: false, primary_key: false,
                            not_null: false, unique: false, references: None,
                            check_constraint: None, check_constraint_text: None, default: None, default_text: None,
                        }
                    }).collect();
                    let exists = execute_correlated_scalar_subquery(subquery, storage, row, &fake_schema).is_some();
                    return if *operator == Operator::NotExists { !exists } else { exists };
                }
                return false;
            }
            if *operator == Operator::In || *operator == Operator::NotIn {
                let lv = resolve_expr_cols(left, row, cols, storage);
                let contains = match right {
                    Expression::List(exprs) => lv.map_or(false, |lv| {
                        exprs.iter().any(|e| resolve_expr_cols(e, row, cols, storage).map_or(false, |rv| rv == lv))
                    }),
                    _ => false,
                };
                return if *operator == Operator::NotIn { !contains } else { contains };
            }
            if *operator == Operator::Similar || *operator == Operator::NotSimilar {
                let lv = resolve_expr_cols(left, row, cols, storage);
                let rv = resolve_expr_cols(right, row, cols, storage);
                let escape = upper_bound.as_ref().and_then(|e| resolve_expr_cols(e, row, cols, storage));
                let similar = match (&lv, &rv) {
                    (Some(Value::String(s)), Some(Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = similar_to_regex(p, escape_char);
                        Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == Operator::Similar { similar } else { !similar };
            }
            let lv = resolve_expr_cols(left, row, cols, storage);
            let rv = resolve_expr_cols(right, row, cols, storage);
            match (lv, rv) {
                (Some(l), Some(r)) => compare_values(&l, operator, &r),
                _ => false,
            }
        }
        Condition::Unique(subquery) => {
            is_unique_subquery(subquery, storage, row, cols)
        }
        Condition::NotUnique(subquery) => {
            !is_unique_subquery(subquery, storage, row, cols)
        }
        Condition::Overlaps(a, b, c, d) => {
            eval_overlaps_cols(a, b, c, d, row, cols, storage)
        }
        _ => false,
    }
}

/// Resolve an expression using (table_alias, column_name) column context
fn resolve_expr_cols(expr: &Expression, row: &[Value], cols: &[(String, String)], storage: &Storage) -> Option<Value> {
    match expr {
        Expression::Literal(v) => Some(v.clone()),
        Expression::Column(name) => {
            // Match by column name (last match wins when ambiguous, but first is more typical)
            cols.iter().position(|c| c.1.eq_ignore_ascii_case(name)).map(|i| row[i].clone())
        }
        Expression::QualifiedColumn(table, col) => {
            cols.iter().position(|c| c.0.eq_ignore_ascii_case(table) && c.1.eq_ignore_ascii_case(col))
                .map(|i| row[i].clone())
        }
        Expression::BinaryOp(left, op, right) => {
            let lv = resolve_expr_cols(left, row, cols, storage)?;
            let rv = resolve_expr_cols(right, row, cols, storage)?;
            storage_eval_arith(&lv, op, &rv)
        }
        Expression::Coalesce(exprs) => {
            exprs.iter().find_map(|e| {
                let v = resolve_expr_cols(e, row, cols, storage);
                match v { Some(Value::Null) | None => None, other => other }
            })
        }
        Expression::Case(branches, else_expr) => {
            for (cond, then_expr) in branches {
                if eval_condition_cols(cond, row, cols, storage) {
                    return resolve_expr_cols(then_expr, row, cols, storage);
                }
            }
            else_expr.as_ref().and_then(|e| resolve_expr_cols(e, row, cols, storage))
        }
        // For all others, build a fake flat schema and delegate to resolve_expression
        other => {
            // Build a fake schema with columns in order
            let fake_schema: Vec<ColumnDefinition> = cols.iter().enumerate().map(|(_, (_, name))| {
                ColumnDefinition {
                    name: name.clone(),
                    data_type: crate::parser::DataType::Varchar(None),
                    auto_increment: false,
                    primary_key: false,
                    not_null: false,
                    unique: false,
                    references: None,
                    check_constraint: None, check_constraint_text: None, default: None, default_text: None,
                }
            }).collect();
            resolve_expression(other, row, &fake_schema, storage)
        }
    }
}

/// Resolve expression where EXCLUDED.col refers to excluded_row values
fn resolve_expr_with_excluded(expr: &Expression, combined_row: &[Value], combined_cols: &[(String, String)]) -> Option<Value> {
    resolve_expr_cols(expr, combined_row, combined_cols, &Storage::noop())
}

fn evaluate_condition(condition: &Condition, row: &[Value], schema: &[ColumnDefinition], storage: &Storage) -> bool {
    match condition {
        Condition::And(left, right) => {
            evaluate_condition(left, row, schema, storage) && evaluate_condition(right, row, schema, storage)
        }
        Condition::Or(left, right) => {
            evaluate_condition(left, row, schema, storage) || evaluate_condition(right, row, schema, storage)
        }
        Condition::Not(inner) => !evaluate_condition(inner, row, schema, storage),
        Condition::Comparison { left, operator, right, upper_bound } => {
            if *operator == Operator::IsNull || *operator == Operator::IsNotNull {
                let left_val = resolve_expression(left, row, schema, storage);
                let is_null = matches!(left_val, Some(Value::Null) | None);
                return if *operator == Operator::IsNull { is_null } else { !is_null };
            }

            if *operator == Operator::Between || *operator == Operator::NotBetween {
                let val = resolve_expression(left, row, schema, storage);
                let low = resolve_expression(right, row, schema, storage);
                let high = upper_bound.as_ref().and_then(|e| resolve_expression(e, row, schema, storage));
                let in_range = matches!((&val, &low, &high), (Some(v), Some(l), Some(h))
                    if compare_values(v, &Operator::GreaterThanOrEqual, l) && compare_values(v, &Operator::LessThanOrEqual, h));
                return if *operator == Operator::Between { in_range } else { !in_range };
            }

            if *operator == Operator::Exists || *operator == Operator::NotExists {
                if let Expression::Subquery(subquery) = right {
                    let exists = execute_correlated_scalar_subquery(subquery, storage, row, schema).is_some();
                    return if *operator == Operator::NotExists { !exists } else { exists };
                }
                return false;
            }

            if *operator == Operator::In || *operator == Operator::NotIn {
                let left_val = resolve_expression(left, row, schema, storage);
                let contains = match right {
                    Expression::List(exprs) => {
                        left_val.map_or(false, |lv| {
                            exprs.iter().any(|e| resolve_expression(e, row, schema, storage).map_or(false, |rv| rv == lv))
                        })
                    }
                    Expression::Subquery(subquery) => {
                        let first = execute_scalar_subquery(subquery, storage);
                        left_val.map_or(false, |lv| first.map_or(false, |rv| rv == lv))
                    }
                    _ => false,
                };
                return if *operator == Operator::In { contains } else { !contains };
            }

            if *operator == Operator::Similar || *operator == Operator::NotSimilar {
                let lv = resolve_expression(left, row, schema, storage);
                let rv = resolve_expression(right, row, schema, storage);
                let escape = upper_bound.as_ref().and_then(|e| resolve_expression(e, row, schema, storage));
                let similar = match (&lv, &rv) {
                    (Some(Value::String(s)), Some(Value::String(p))) => {
                        let escape_char = escape.and_then(|v| if let Value::String(c) = v { c.chars().next() } else { None });
                        let pattern = similar_to_regex(p, escape_char);
                        Regex::new(&format!("^(?:{})$", pattern)).map_or(false, |re| re.is_match(s))
                    }
                    _ => false,
                };
                return if *operator == Operator::Similar { similar } else { !similar };
            }

            let left_val = resolve_expression(left, row, schema, storage);
            let right_val = resolve_expression(right, row, schema, storage);
            match (&left_val, &right_val) {
                (Some(l), Some(r)) => compare_values(l, operator, r),
                _ => false,
            }
        }
        Condition::Unique(subquery) => {
            is_unique_subquery_expr(subquery, storage, row, schema)
        }
        Condition::NotUnique(subquery) => {
            !is_unique_subquery_expr(subquery, storage, row, schema)
        }
        Condition::Overlaps(a, b, c, d) => {
            eval_overlaps_expr(a, b, c, d, row, schema, storage)
        }
        Condition::AnyComparison { left, op, subquery } => {
            let lv = match resolve_expression(left, row, schema, storage) { Some(v) => v, None => return false };
            match execute_scalar_subquery(subquery, storage) {
                Some(rv) => compare_values(&lv, op, &rv),
                None => false,
            }
        }
        Condition::AllComparison { left, op, subquery } => {
            let lv = match resolve_expression(left, row, schema, storage) { Some(v) => v, None => return false };
            match execute_scalar_subquery(subquery, storage) {
                Some(rv) => compare_values(&lv, op, &rv),
                None => true,
            }
        }
    }
}

// ── Helpers for UNIQUE predicate: check if subquery returns all distinct rows ──

/// Execute a correlated subquery and collect all matching rows (for UNIQUE predicate)
fn collect_correlated_rows(
    subquery: &SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_schema: &[ColumnDefinition],
) -> Vec<Vec<Value>> {
    let table_name = match &subquery.from {
        crate::parser::FromClause::Table(name) => name.clone(),
        _ => return vec![],
    };
    let create_stmt = match storage.load_schema(&table_name) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let all_rows = match storage.read_rows(&table_name) {
        Ok(rows) => rows,
        Err(_) => return vec![],
    };
    let mut result = vec![];
    for row in &all_rows {
        if let Some(ref wc) = subquery.where_clause {
            if !evaluate_correlated_condition_storage(&wc.condition, row, &create_stmt.columns, storage, outer_row, outer_schema) {
                continue;
            }
        }
        let projected = match &subquery.columns[..] {
            [SelectColumn::All] => row.clone(),
            [SelectColumn::StarFromTable(_)] => row.clone(),
            _ => {
                let mut vals = Vec::new();
                for col in &subquery.columns {
                    let name = match col {
                        SelectColumn::Column(n) => n,
                        SelectColumn::QualifiedColumn(_, n) => n,
                        _ => continue,
                    };
                    if let Some(pos) = create_stmt.columns.iter().position(|c| c.name == *name) {
                        vals.push(row[pos].clone());
                    }
                }
                vals
            }
        };
        result.push(projected);
    }
    result
}

/// Check if all rows from a subquery are distinct (cols context)
fn is_unique_subquery(
    subquery: &SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_cols: &[(String, String)],
) -> bool {
    let fake_schema: Vec<ColumnDefinition> = outer_cols.iter().map(|(_, name)| {
        ColumnDefinition {
            name: name.clone(),
            data_type: DataType::Varchar(None),
            auto_increment: false, primary_key: false,
            not_null: false, unique: false, references: None,
            check_constraint: None, check_constraint_text: None, default: None, default_text: None,
        }
    }).collect();
    let rows = collect_correlated_rows(subquery, storage, outer_row, &fake_schema);
    for i in 0..rows.len() {
        for j in (i + 1)..rows.len() {
            if rows[i].len() == rows[j].len() && rows[i].iter().zip(rows[j].iter()).all(|(a, b)| a == b) {
                return false;
            }
        }
    }
    true
}

/// Check if all rows from a subquery are distinct (schema context)
fn is_unique_subquery_expr(
    subquery: &SelectStatement,
    storage: &Storage,
    outer_row: &[Value],
    outer_schema: &[ColumnDefinition],
) -> bool {
    let rows = collect_correlated_rows(subquery, storage, outer_row, outer_schema);
    for i in 0..rows.len() {
        for j in (i + 1)..rows.len() {
            if rows[i].len() == rows[j].len() && rows[i].iter().zip(rows[j].iter()).all(|(a, b)| a == b) {
                return false;
            }
        }
    }
    true
}

// ── Helpers for OVERLAPS predicate ──

/// Evaluate OVERLAPS using (table_alias, column_name) context
fn eval_overlaps_cols(
    a: &Expression, b: &Expression, c: &Expression, d: &Expression,
    row: &[Value], cols: &[(String, String)], storage: &Storage,
) -> bool {
    let va = resolve_expr_cols(a, row, cols, storage);
    let vb = resolve_expr_cols(b, row, cols, storage);
    let vc = resolve_expr_cols(c, row, cols, storage);
    let vd = resolve_expr_cols(d, row, cols, storage);
    eval_overlaps_values(&va, &vb, &vc, &vd)
}

/// Evaluate OVERLAPS using ColumnDefinition schema
fn eval_overlaps_expr(
    a: &Expression, b: &Expression, c: &Expression, d: &Expression,
    row: &[Value], schema: &[ColumnDefinition], storage: &Storage,
) -> bool {
    let va = resolve_expression(a, row, schema, storage);
    let vb = resolve_expression(b, row, schema, storage);
    let vc = resolve_expression(c, row, schema, storage);
    let vd = resolve_expression(d, row, schema, storage);
    eval_overlaps_values(&va, &vb, &vc, &vd)
}

/// Core OVERLAPS logic: two periods overlap if s1 < e2 AND s2 < e1
fn eval_overlaps_values(s1: &Option<Value>, e1: &Option<Value>, s2: &Option<Value>, e2: &Option<Value>) -> bool {
    match (s1, e1, s2, e2) {
        (Some(s1), Some(e1), Some(s2), Some(e2)) => {
            // Convert non-temporal types to numeric f64 for comparison
            let to_num = |v: &Value| -> Option<f64> {
                match v {
                    Value::Int(n) => Some(*n as f64),
                    Value::Float(f) => Some(*f),
                    Value::Date(d) => Some(*d as f64),
                    Value::Timestamp(ts) => Some(*ts as f64),
                    _ => None,
                }
            };
            match (to_num(s1), to_num(e1), to_num(s2), to_num(e2)) {
                (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                    compare_values(&Value::Float(s1), &Operator::LessThan, &Value::Float(e2)) &&
                    compare_values(&Value::Float(s2), &Operator::LessThan, &Value::Float(e1))
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// Resolve an expression to a Value
fn resolve_expression(expr: &Expression, row: &[Value], schema: &[ColumnDefinition], storage: &Storage) -> Option<Value> {
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
        Expression::Subquery(subquery) => execute_scalar_subquery(subquery, storage),
        Expression::List(_) => None,
        Expression::ScalarFunc(func, inner) => {
            resolve_expression(inner, row, schema, storage).and_then(|v| apply_scalar_func(func, v))
        }
        Expression::Coalesce(exprs) => {
            exprs.iter().find_map(|e| {
                let v = resolve_expression(e, row, schema, storage);
                match v { Some(Value::Null) | None => None, other => other }
            })
        }
        Expression::NullIf(a, b) => {
            let va = resolve_expression(a, row, schema, storage);
            let vb = resolve_expression(b, row, schema, storage);
            match (&va, &vb) {
                (Some(l), Some(r)) if l == r => Some(Value::Null),
                _ => va,
            }
        }
        Expression::Round(val, places) => {
            let v = resolve_expression(val, row, schema, storage)?;
            let p = places.as_ref().and_then(|e| resolve_expression(e, row, schema, storage));
            apply_round(v, p)
        }
        Expression::Concat(exprs) => {
            let parts: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expression(e, row, schema, storage)).collect();
            apply_concat(parts)
        }
        Expression::Substr(s, start, len) => {
            let sv = resolve_expression(s, row, schema, storage)?;
            let startv = resolve_expression(start, row, schema, storage)?;
            let lenv = len.as_ref().and_then(|e| resolve_expression(e, row, schema, storage));
            apply_substr(sv, startv, lenv)
        }
        Expression::Replace(s, from, to) => {
            let sv = resolve_expression(s, row, schema, storage)?;
            let fv = resolve_expression(from, row, schema, storage)?;
            let tv = resolve_expression(to, row, schema, storage)?;
            apply_replace(sv, fv, tv)
        }
        Expression::LPad(s, len, pad) => {
            let sv = resolve_expression(s, row, schema, storage)?;
            let lv = resolve_expression(len, row, schema, storage)?;
            let pv = resolve_expression(pad, row, schema, storage)?;
            apply_lpad(sv, lv, pv)
        }
        Expression::RPad(s, len, pad) => {
            let sv = resolve_expression(s, row, schema, storage)?;
            let lv = resolve_expression(len, row, schema, storage)?;
            let pv = resolve_expression(pad, row, schema, storage)?;
            apply_rpad(sv, lv, pv)
        }
        Expression::Cast(inner, type_name) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            apply_cast(v, type_name)
        }
        Expression::BinaryOp(left, op, right) => {
            let lv = resolve_expression(left, row, schema, storage)?;
            let rv = resolve_expression(right, row, schema, storage)?;
            storage_eval_arith(&lv, op, &rv)
        }
        Expression::Aggregate(_, _) => None,
        Expression::Window(_, _) => None,
        Expression::Case(branches, else_expr) => {
            for (condition, result) in branches {
                if evaluate_condition(condition, row, schema, storage) {
                    return resolve_expression(result, row, schema, storage);
                }
            }
            else_expr.as_ref().and_then(|e| resolve_expression(e, row, schema, storage))
        }
        Expression::Greatest(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expression(e, row, schema, storage)).collect();
            apply_greatest(args)
        }
        Expression::Least(exprs) => {
            let args: Vec<Option<Value>> = exprs.iter().map(|e| resolve_expression(e, row, schema, storage)).collect();
            apply_least(args)
        }
        Expression::Power(base, exp) => {
            let b = resolve_expression(base, row, schema, storage)?;
            let e = resolve_expression(exp, row, schema, storage)?;
            apply_power(b, e)
        }
        Expression::Position(needle, haystack) => {
            let n = resolve_expression(needle, row, schema, storage)?;
            let h = resolve_expression(haystack, row, schema, storage)?;
            apply_position(n, h)
        }
        Expression::Repeat(s, n) => {
            let sv = resolve_expression(s, row, schema, storage)?;
            let nv = resolve_expression(n, row, schema, storage)?;
            apply_repeat(sv, nv)
        }
        Expression::CurrentDate => Some(Value::Date(crate::parser::current_epoch_days())),
        Expression::CurrentTimestamp => Some(Value::Timestamp(crate::parser::current_epoch_secs())),
        Expression::Extract(field, inner) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            storage_eval_extract(field, v)
        }
        Expression::DateTrunc(unit, inner) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            storage_eval_date_trunc(unit, v)
        }
        Expression::DateDiff(unit, e1, e2) => {
            let v1 = resolve_expression(e1, row, schema, storage)?;
            let v2 = resolve_expression(e2, row, schema, storage)?;
            storage_eval_datediff(unit, v1, v2)
        }
        Expression::DateAdd(inner, n, unit) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            storage_eval_dateadd(v, *n, unit)
        }
        Expression::JsonTypeOf(inner) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            apply_json_typeof(&v)
        }
        Expression::JsonArrayLength(inner) => {
            let v = resolve_expression(inner, row, schema, storage)?;
            apply_json_array_length(&v)
        }
        Expression::JsonBuildObject(pairs) => {
            let resolved: Vec<(Value, Value)> = pairs.iter()
                .filter_map(|(k, v)| {
                    let kv = resolve_expression(k, row, schema, storage)?;
                    let vv = resolve_expression(v, row, schema, storage)?;
                    Some((kv, vv))
                })
                .collect();
            apply_json_build_object(&resolved)
        }
        Expression::JsonBuildArray(vals) => {
            let resolved: Vec<Value> = vals.iter()
                .filter_map(|v| resolve_expression(v, row, schema, storage))
                .collect();
            apply_json_build_array(&resolved)
        }
        Expression::UserFunc(name, args) => {
            let func_def = match storage.load_function(name) {
                Ok(Some(f)) => f,
                _ => return None,
            };
            if func_def.params.len() != args.len() {
                return None;
            }
            let arg_vals: Vec<Value> = args.iter()
                .filter_map(|a| resolve_expression(a, row, schema, storage))
                .collect();
            if arg_vals.len() != args.len() {
                return None;
            }
            let func_schema: Vec<ColumnDefinition> = func_def.params.iter()
                .map(|(n, t)| {
                    let dt = match t.to_uppercase().as_str() {
                        "INT" | "INTEGER" => DataType::Int,
                        "TEXT" | "VARCHAR" => DataType::Varchar(None),
                        "FLOAT" | "DOUBLE" => DataType::Double,
                        "BOOLEAN" => DataType::Boolean,
                        _ => DataType::Text,
                    };
                    ColumnDefinition {
                        name: n.clone(),
                        data_type: dt,
                        auto_increment: false,
                        primary_key: false,
                        not_null: false,
                        unique: false,
                        references: None,
                        check_constraint: None,
                        check_constraint_text: None,
                        default: None,
                        default_text: None,
                    }
                })
                .collect();
            resolve_expression(&func_def.body, &arg_vals, &func_schema, storage)
        }
    }
}

// Extract a date/time field from a Date or Timestamp value
fn storage_eval_extract(field: &str, v: Value) -> Option<Value> {
    let f = field.to_uppercase();
    let (days, secs_in_day) = match &v {
        Value::Date(d) => (*d, 0i64),
        Value::Timestamp(ts) => {
            let d = (*ts / 86400) as i32;
            let s = ts.rem_euclid(86400);
            (d, s)
        }
        Value::String(s) => {
            if let Some(d) = crate::parser::parse_date_str(s) { (d, 0) }
            else if let Some(ts) = crate::parser::parse_timestamp_str(s) {
                let d = (ts / 86400) as i32;
                let s2 = ts.rem_euclid(86400);
                (d, s2)
            } else { return None; }
        }
        _ => return None,
    };
    let (year, month, day) = crate::parser::epoch_days_to_date(days);
    let hour = secs_in_day / 3600;
    let minute = (secs_in_day % 3600) / 60;
    let second = secs_in_day % 60;
    match f.as_str() {
        "YEAR" => Some(Value::Int(year as i64)),
        "MONTH" => Some(Value::Int(month as i64)),
        "DAY" => Some(Value::Int(day as i64)),
        "HOUR" => Some(Value::Int(hour)),
        "MINUTE" => Some(Value::Int(minute)),
        "SECOND" => Some(Value::Int(second)),
        "DOW" | "DAYOFWEEK" => {
            // day of week: 0=Sunday using Tomohiko Sakamoto-style; epoch day 0 = Thursday
            let dow = ((days as i64 + 4).rem_euclid(7)) as i64;
            Some(Value::Int(dow))
        }
        "DOY" | "DAYOFYEAR" => {
            let jan1 = crate::parser::date_to_epoch_days(year, 1, 1);
            Some(Value::Int((days - jan1 + 1) as i64))
        }
        _ => None,
    }
}

// Truncate a date/timestamp to the given unit
fn storage_eval_date_trunc(unit: &str, v: Value) -> Option<Value> {
    let u = unit.to_uppercase();
    let (days, secs_in_day) = match &v {
        Value::Date(d) => (*d, 0i64),
        Value::Timestamp(ts) => {
            let d = (*ts / 86400) as i32;
            let s = ts.rem_euclid(86400);
            (d, s)
        }
        _ => return None,
    };
    let (year, month, _day) = crate::parser::epoch_days_to_date(days);
    match u.as_str() {
        "YEAR" => Some(Value::Date(crate::parser::date_to_epoch_days(year, 1, 1))),
        "MONTH" => Some(Value::Date(crate::parser::date_to_epoch_days(year, month, 1))),
        "DAY" => Some(Value::Date(days)),
        "HOUR" => {
            let base = days as i64 * 86400;
            Some(Value::Timestamp(base + (secs_in_day / 3600) * 3600))
        }
        "MINUTE" => {
            let base = days as i64 * 86400;
            Some(Value::Timestamp(base + (secs_in_day / 60) * 60))
        }
        _ => None,
    }
}

// Compute difference between two dates/timestamps in the given unit
fn storage_eval_datediff(unit: &str, v1: Value, v2: Value) -> Option<Value> {
    let u = unit.to_uppercase();
    let to_days = |v: Value| -> Option<i32> {
        match v {
            Value::Date(d) => Some(d),
            Value::Timestamp(ts) => Some((ts / 86400) as i32),
            Value::String(s) => crate::parser::parse_date_str(&s)
                .or_else(|| crate::parser::parse_timestamp_str(&s).map(|ts| (ts / 86400) as i32)),
            _ => None,
        }
    };
    let to_secs = |v: Value| -> Option<i64> {
        match v {
            Value::Date(d) => Some(d as i64 * 86400),
            Value::Timestamp(ts) => Some(ts),
            Value::String(s) => crate::parser::parse_timestamp_str(&s)
                .or_else(|| crate::parser::parse_date_str(&s).map(|d| d as i64 * 86400)),
            _ => None,
        }
    };
    match u.as_str() {
        "DAY" => Some(Value::Int((to_days(v1)? - to_days(v2)?) as i64)),
        "HOUR" => Some(Value::Int((to_secs(v1)? - to_secs(v2)?) / 3600)),
        "MINUTE" => Some(Value::Int((to_secs(v1)? - to_secs(v2)?) / 60)),
        "SECOND" => Some(Value::Int(to_secs(v1)? - to_secs(v2)?)),
        "MONTH" => {
            let d1 = to_days(v1)?;
            let d2 = to_days(v2)?;
            let (y1, m1, _) = crate::parser::epoch_days_to_date(d1);
            let (y2, m2, _) = crate::parser::epoch_days_to_date(d2);
            Some(Value::Int(((y1 - y2) * 12 + (m1 - m2)) as i64))
        }
        "YEAR" => {
            let d1 = to_days(v1)?;
            let d2 = to_days(v2)?;
            let (y1, _, _) = crate::parser::epoch_days_to_date(d1);
            let (y2, _, _) = crate::parser::epoch_days_to_date(d2);
            Some(Value::Int((y1 - y2) as i64))
        }
        _ => None,
    }
}

// Add an interval (n units) to a date/timestamp
fn storage_eval_dateadd(v: Value, n: i64, unit: &str) -> Option<Value> {
    let secs = crate::parser::interval_unit_secs(unit)?;
    let total = secs * n;
    match v {
        Value::Date(d) => {
            // If unit is days or coarser, keep as Date; otherwise promote to Timestamp
            let u = unit.to_uppercase();
            if matches!(u.as_str(), "HOUR" | "MINUTE" | "SECOND") {
                Some(Value::Timestamp(d as i64 * 86400 + total))
            } else {
                Some(Value::Date((d as i64 + total / 86400) as i32))
            }
        }
        Value::Timestamp(ts) => Some(Value::Timestamp(ts + total)),
        Value::String(s) => {
            if let Some(d) = crate::parser::parse_date_str(&s) {
                storage_eval_dateadd(Value::Date(d), n, unit)
            } else if let Some(ts) = crate::parser::parse_timestamp_str(&s) {
                Some(Value::Timestamp(ts + total))
            } else { None }
        }
        _ => None,
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

/// Convert a SQL SIMILAR TO pattern to a Rust regex pattern.
/// SIMILAR TO uses: % (any sequence), _ (any char), |, *, +, (), []
pub fn similar_to_regex(pattern: &str, escape_char: Option<char>) -> String {
    let mut re = String::new();
    let mut chars = pattern.chars();
    let escape = escape_char.unwrap_or('\\');
    let mut prev_was_escape = false;

    while let Some(c) = chars.next() {
        if prev_was_escape {
            re.push_str(&regex::escape(&c.to_string()));
            prev_was_escape = false;
            continue;
        }
        if c == escape {
            prev_was_escape = true;
            continue;
        }
        match c {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            // These are special in both SIMILAR TO and regex — keep as-is
            '|' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}' => re.push(c),
            // '.' is literal in SIMILAR TO — escape it
            '.' => re.push_str("\\."),
            // These regex special chars are literal in SIMILAR TO
            '^' => re.push_str("\\^"),
            '$' => re.push_str("\\$"),
            '?' => re.push_str("\\?"),
            '-' => re.push_str("\\-"),
            '\\' => re.push_str("\\\\"),
            // '?' is special in regex but not in SIMILAR TO
            _ => re.push(c),
        }
    }
    // If pattern ends with escape char, treat it literally
    if prev_was_escape {
        re.push_str(&regex::escape(&escape.to_string()));
    }
    re
}

/// Compare two values using the given operator
fn compare_values(left: &Value, op: &Operator, right: &Value) -> bool {
    // IS DISTINCT FROM / IS NOT DISTINCT FROM: NULL is comparable
    if *op == Operator::IsDistinctFrom || *op == Operator::IsNotDistinctFrom {
        let distinct = match (left, right) {
            (Value::Null, Value::Null) => false,
            (Value::Null, _) | (_, Value::Null) => true,
            _ => compare_values(left, &Operator::NotEquals, right),
        };
        return if *op == Operator::IsDistinctFrom { distinct } else { !distinct };
    }

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
            Operator::NotLike => !like_match(l, r),
            Operator::ILike => like_match(&l.to_lowercase(), &r.to_lowercase()),
            Operator::NotILike => !like_match(&l.to_lowercase(), &r.to_lowercase()),
            Operator::Equals => l == r,
            Operator::NotEquals => l != r,
            Operator::GreaterThan => l > r,
            Operator::LessThan => l < r,
            Operator::GreaterThanOrEqual => l >= r,
            Operator::LessThanOrEqual => l <= r,
            _ => false,
        },
        // Date comparisons
        (Value::Date(a), Value::Date(b)) => compare_numeric(*a as f64, *b as f64, op),
        (Value::Timestamp(a), Value::Timestamp(b)) => compare_numeric(*a as f64, *b as f64, op),
        (Value::Date(a), Value::Timestamp(b)) => compare_numeric((*a as i64 * 86400) as f64, *b as f64, op),
        (Value::Timestamp(a), Value::Date(b)) => compare_numeric(*a as f64, (*b as i64 * 86400) as f64, op),
        (Value::Date(d), Value::String(s)) => {
            if let Some(rd) = crate::parser::parse_date_str(s) {
                compare_numeric(*d as f64, rd as f64, op)
            } else { false }
        }
        (Value::String(s), Value::Date(d)) => {
            if let Some(ld) = crate::parser::parse_date_str(s) {
                compare_numeric(ld as f64, *d as f64, op)
            } else { false }
        }
        (Value::Timestamp(ts), Value::String(s)) => {
            if let Some(rts) = crate::parser::parse_timestamp_str(s) {
                compare_numeric(*ts as f64, rts as f64, op)
            } else { false }
        }
        (Value::String(s), Value::Timestamp(ts)) => {
            if let Some(lts) = crate::parser::parse_timestamp_str(s) {
                compare_numeric(lts as f64, *ts as f64, op)
            } else { false }
        }
        (Value::Json(l), Value::Json(r)) | (Value::String(l), Value::Json(r)) | (Value::Json(l), Value::String(r)) => match op {
            Operator::JsonContains => crate::parser::json_contains(l, r),
            Operator::Equals => l == r,
            Operator::NotEquals => l != r,
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

/// Evaluate a binary arithmetic / concatenation operation on two Values
fn storage_eval_arith(left: &Value, op: &ArithOp, right: &Value) -> Option<Value> {
    // JSON field access operators
    if matches!(op, ArithOp::JsonGet | ArithOp::JsonGetText) {
        return crate::parser::apply_json_op(left, op, right);
    }
    if let ArithOp::Concat = op {
        let ls = match left {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => crate::parser::format_date(*d),
            Value::Timestamp(ts) => crate::parser::format_timestamp(*ts),
            Value::Default => return None,
        };
        let rs = match right {
            Value::String(s) | Value::Json(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => format!("{}", f),
            Value::Null => return Some(Value::Null),
            Value::Bool(b) => b.to_string(),
            Value::Date(d) => crate::parser::format_date(*d),
            Value::Timestamp(ts) => crate::parser::format_timestamp(*ts),
            Value::Default => return None,
        };
        return Some(Value::String(ls + &rs));
    }
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => {
            match op {
                ArithOp::Add => Some(Value::Int(l + r)),
                ArithOp::Sub => Some(Value::Int(l - r)),
                ArithOp::Mul => Some(Value::Int(l * r)),
                ArithOp::Div => { if *r == 0 { Some(Value::Null) } else { Some(Value::Int(l / r)) } }
                ArithOp::Mod => { if *r == 0 { Some(Value::Null) } else { Some(Value::Int(l % r)) } }
                ArithOp::Concat | ArithOp::JsonGet | ArithOp::JsonGetText => unreachable!(),
            }
        }
        (Value::Float(l), Value::Float(r)) => storage_arith_f64(*l, op, *r),
        (Value::Int(l), Value::Float(r)) => storage_arith_f64(*l as f64, op, *r),
        (Value::Float(l), Value::Int(r)) => storage_arith_f64(*l, op, *r as f64),
        // Date + Int / Date - Int → shift by days
        (Value::Date(d), Value::Int(n)) => match op {
            ArithOp::Add => Some(Value::Date(d + *n as i32)),
            ArithOp::Sub => Some(Value::Date(d - *n as i32)),
            _ => Some(Value::Null),
        },
        // Date - Date → difference in days
        (Value::Date(a), Value::Date(b)) => match op {
            ArithOp::Sub => Some(Value::Int((a - b) as i64)),
            _ => Some(Value::Null),
        },
        // Timestamp + Int / Timestamp - Int → shift by seconds
        (Value::Timestamp(ts), Value::Int(n)) => match op {
            ArithOp::Add => Some(Value::Timestamp(ts + n)),
            ArithOp::Sub => Some(Value::Timestamp(ts - n)),
            _ => Some(Value::Null),
        },
        // Timestamp - Timestamp → difference in seconds
        (Value::Timestamp(a), Value::Timestamp(b)) => match op {
            ArithOp::Sub => Some(Value::Int(a - b)),
            _ => Some(Value::Null),
        },
        _ => Some(Value::Null),
    }
}

fn storage_arith_f64(l: f64, op: &ArithOp, r: f64) -> Option<Value> {
    let v = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => { if r == 0.0 { return Some(Value::Null); } l / r }
        ArithOp::Mod => l % r,
        ArithOp::Concat => return Some(Value::String(format!("{}{}", l, r))),
        ArithOp::JsonGet | ArithOp::JsonGetText => return None,
    };
    Some(Value::Float(v))
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
        Value::Json(s) => {
            let escaped = s.replace('\\', "\\\\")
                .replace('|', "\\|")
                .replace('\n', "\\n");
            format!("JSON:{}", escaped)
        }
        Value::Date(d) => format!("DATE:{}", d),
        Value::Timestamp(ts) => format!("TIMESTAMP:{}", ts),
        // Default markers are resolved before rows are written; store as NULL if one leaks
        Value::Null | Value::Default => "NULL".to_string(),
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
            let unescaped = string_val
                .replace("\\n", "\n")
                .replace("\\|", "|")
                .replace("\\\\", "\\");
            values.push(Value::String(unescaped));
        } else if let Some(json_val) = part.strip_prefix("JSON:") {
            let unescaped = json_val
                .replace("\\n", "\n")
                .replace("\\|", "|")
                .replace("\\\\", "\\");
            values.push(Value::Json(unescaped));
        } else if let Some(date_str) = part.strip_prefix("DATE:") {
            let d = date_str.parse::<i32>()
                .map_err(|_| StorageError::InvalidData(format!("Invalid date epoch: {}", date_str)))?;
            values.push(Value::Date(d));
        } else if let Some(ts_str) = part.strip_prefix("TIMESTAMP:") {
            let ts = ts_str.parse::<i64>()
                .map_err(|_| StorageError::InvalidData(format!("Invalid timestamp epoch: {}", ts_str)))?;
            values.push(Value::Timestamp(ts));
        } else {
            return Err(StorageError::InvalidData(format!("Invalid value format: {}", part)));
        }
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// Metadata (information_schema) tables
// ---------------------------------------------------------------------------

const METADATA_TABLES: &[&str] = &[
    "information_schema.schemata",
    "information_schema.tables",
    "information_schema.columns",
    "information_schema.views",
    "information_schema.table_constraints",
    "information_schema.key_column_usage",
    "information_schema.referential_constraints",
    "information_schema.check_constraints",
    "information_schema.routines",
];

impl Storage {
    pub fn is_metadata_table(name: &str) -> bool {
        METADATA_TABLES.contains(&name)
    }

    /// Return synthetic column definitions for a metadata table.
    pub fn metadata_schema(name: &str) -> Option<CreateTableStatement> {
        let columns = match name {
            "information_schema.schemata" => vec![
                ColumnDefinition { name: "catalog_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "schema_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "default_character_set_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "default_collation_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.tables" => vec![
                ColumnDefinition { name: "table_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_type".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.columns" => vec![
                ColumnDefinition { name: "table_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "column_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "ordinal_position".into(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "column_default".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "is_nullable".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "data_type".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.views" => vec![
                ColumnDefinition { name: "table_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "view_definition".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.table_constraints" => vec![
                ColumnDefinition { name: "constraint_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_type".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.key_column_usage" => vec![
                ColumnDefinition { name: "constraint_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "table_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "column_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "ordinal_position".into(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.referential_constraints" => vec![
                ColumnDefinition { name: "constraint_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "unique_constraint_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "unique_constraint_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "unique_constraint_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "delete_rule".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "update_rule".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.check_constraints" => vec![
                ColumnDefinition { name: "constraint_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "constraint_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "check_clause".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            "information_schema.routines" => vec![
                ColumnDefinition { name: "specific_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "routine_catalog".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "routine_schema".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "routine_name".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "routine_type".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "data_type".into(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "created".into(), data_type: DataType::Timestamp, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
            _ => return None,
        };
        Some(CreateTableStatement { table_name: name.to_string(), columns, constraints: vec![] })
    }

    /// Generate synthetic rows for a metadata table by querying live storage state.
    pub fn read_metadata_rows(&self, name: &str) -> Option<Vec<Vec<Value>>> {
        match name {
            "information_schema.schemata" => {
                Some(vec![vec![
                    Value::String("default".into()),
                    Value::String("public".into()),
                    Value::String("UTF8".into()),
                    Value::String("en_US.UTF-8".into()),
                ]])
            }
            "information_schema.tables" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                // User tables
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        rows.push(vec![
                            Value::String("default".into()),
                            Value::String("public".into()),
                            Value::String(t.clone()),
                            Value::String("BASE TABLE".into()),
                        ]);
                    }
                }
                // Views
                if let Ok(views) = self.list_views() {
                    for v in &views {
                        rows.push(vec![
                            Value::String("default".into()),
                            Value::String("public".into()),
                            Value::String(v.clone()),
                            Value::String("VIEW".into()),
                        ]);
                    }
                }
                Some(rows)
            }
            "information_schema.columns" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                // User tables
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        if let Ok(schema) = self.load_schema(t) {
                            for (i, col) in schema.columns.iter().enumerate() {
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String(col.name.clone()),
                                    Value::Int((i + 1) as i64),
                                    col.default_text.as_ref().map(|t| Value::String(t.clone())).unwrap_or(Value::Null),
                                    Value::String(if col.not_null || col.primary_key { "NO".into() } else { "YES".into() }),
                                    Value::String(data_type_to_string(&col.data_type)),
                                ]);
                            }
                        }
                    }
                }
                // Views
                if let Ok(views) = self.list_views() {
                    for v in &views {
                        if let Ok(Some(_view_sql)) = self.load_view(v) {
                            rows.push(vec![
                                Value::String("default".into()),
                                Value::String("public".into()),
                                Value::String(v.clone()),
                                Value::String("?".into()),
                                Value::Int(1),
                                Value::Null,
                                Value::String("YES".into()),
                                Value::String("VARCHAR".into()),
                            ]);
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.views" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(views) = self.list_views() {
                    for v in &views {
                        if let Ok(Some(sql)) = self.load_view(v) {
                            rows.push(vec![
                                Value::String("default".into()),
                                Value::String("public".into()),
                                Value::String(v.clone()),
                                Value::String(sql),
                            ]);
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.table_constraints" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        if let Ok(schema) = self.load_schema(t) {
                            let has_pk = schema.columns.iter().any(|c| c.primary_key);
                            if has_pk {
                                let constraint_name = format!("{}_pkey", t);
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(constraint_name),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String("PRIMARY KEY".into()),
                                ]);
                            }
                            let has_unique = schema.columns.iter().any(|c| c.unique);
                            if has_unique {
                                let constraint_name = format!("{}_unique", t);
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(constraint_name),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String("UNIQUE".into()),
                                ]);
                            }
                            let has_fk = schema.columns.iter().any(|c| c.references.is_some());
                            if has_fk {
                                let constraint_name = format!("{}_fkey", t);
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(constraint_name),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String("FOREIGN KEY".into()),
                                ]);
                            }
                            for (i, tc) in schema.constraints.iter().enumerate() {
                                let kind = match &tc.kind {
                                    TableConstraintKind::PrimaryKey(_) => "PRIMARY KEY",
                                    TableConstraintKind::Unique(_) => "UNIQUE",
                                    TableConstraintKind::ForeignKey { .. } => "FOREIGN KEY",
                                    TableConstraintKind::Check(_) => "CHECK",
                                };
                                let constraint_name = tc.name.clone()
                                    .unwrap_or_else(|| format!("{}_constraint_{}", t, i + 1));
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(constraint_name),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String(kind.into()),
                                ]);
                            }
                            let has_ck = schema.columns.iter().any(|c| c.check_constraint.is_some());
                            if has_ck {
                                let constraint_name = format!("{}_check", t);
                                rows.push(vec![
                                    Value::String("default".into()),
                                    Value::String("public".into()),
                                    Value::String(constraint_name),
                                    Value::String("public".into()),
                                    Value::String(t.clone()),
                                    Value::String("CHECK".into()),
                                ]);
                            }
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.key_column_usage" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        if let Ok(schema) = self.load_schema(t) {
                            for (i, col) in schema.columns.iter().enumerate() {
                                if col.primary_key {
                                    rows.push(vec![
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_pkey", t)),
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(t.clone()),
                                        Value::String(col.name.clone()),
                                        Value::Int((i + 1) as i64),
                                    ]);
                                }
                                if col.unique && !col.primary_key {
                                    rows.push(vec![
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_unique", t)),
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(t.clone()),
                                        Value::String(col.name.clone()),
                                        Value::Int((i + 1) as i64),
                                    ]);
                                }
                                if col.references.is_some() {
                                    rows.push(vec![
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_fkey", t)),
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(t.clone()),
                                        Value::String(col.name.clone()),
                                        Value::Int((i + 1) as i64),
                                    ]);
                                }
                            }
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.referential_constraints" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        if let Ok(schema) = self.load_schema(t) {
                            for col in &schema.columns {
                                if let Some(ref fk) = col.references {
                                    rows.push(vec![
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_fkey", t)),
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_{}_fkey", fk.table, fk.column)),
                                        Value::String("NO ACTION".into()),
                                        Value::String("NO ACTION".into()),
                                    ]);
                                }
                            }
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.check_constraints" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(tables) = self.list_tables() {
                    for t in &tables {
                        if let Ok(schema) = self.load_schema(t) {
                            for col in &schema.columns {
                                if let Some(ref ck) = col.check_constraint_text {
                                    rows.push(vec![
                                        Value::String("default".into()),
                                        Value::String("public".into()),
                                        Value::String(format!("{}_check", t)),
                                        Value::String(ck.clone()),
                                    ]);
                                }
                            }
                        }
                    }
                }
                Some(rows)
            }
            "information_schema.routines" => {
                let mut rows: Vec<Vec<Value>> = Vec::new();
                if let Ok(functions) = self.list_functions() {
                    for f in &functions {
                        if let Ok(Some(sig)) = self.load_function(f) {
                            let return_type_str = sig.return_type.as_deref().unwrap_or("").to_string();
                            rows.push(vec![
                                Value::String(f.clone()),
                                Value::String("default".into()),
                                Value::String("public".into()),
                                Value::String(f.clone()),
                                Value::String("FUNCTION".into()),
                                Value::String(return_type_str),
                                Value::Null, // created
                            ]);
                        }
                    }
                }
                Some(rows)
            }
            _ => None,
        }
    }
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
            constraints: vec![],
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
            constraints: vec![],
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
            constraints: vec![],
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };

        let orders = CreateTableStatement {
            table_name: "orders".to_string(),
            constraints: vec![],
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
            constraints: vec![],
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
            constraints: vec![],
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
            source: crate::parser::InsertSource::Values(vec![vec![
                Value::Int(1),
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
            ]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert_stmt).unwrap();

        // Insert more data
        let insert_stmt2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![
                Value::Int(2),
                Value::String("Bob".to_string()),
                Value::String("bob@example.com".to_string()),
            ]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(100))),
                ColumnDefinition::new("description", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert_stmt = InsertStatement {
            table_name: "products".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![
                Value::Int(1),
                Value::String("Widget".to_string()),
                Value::Null,
            ]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert with wrong number of columns
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1)]]), // Missing one column
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        // Try to insert string into int column
        let insert_stmt = InsertStatement {
            table_name: "test".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![
                Value::String("not a number".to_string()),
                Value::String("Alice".to_string()),
            ]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(Some(255))),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert1 = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        let insert2 = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert1).unwrap();
        storage.insert_row(&insert2).unwrap();

        // Update single row
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Expression::Literal(Value::String("Alice Updated".to_string())),
            }],
            from: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(1)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated.0, 1);

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
            constraints: vec![],
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
                source: crate::parser::InsertSource::Values(vec![vec![Value::Int(i), Value::Int(1)]]),
            
                columns: Vec::new(),
                on_conflict: None,
                returning: None,
            };
            storage.insert_row(&insert).unwrap();
        }

        // Update all rows where active = 1
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "active".to_string(),
                value: Expression::Literal(Value::Int(0)),
            }],
            from: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("active".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(1)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated.0, 3);

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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("status", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        for i in 1..=3 {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![vec![Value::Int(i), Value::String("old".to_string())]]),
            
                columns: Vec::new(),
                on_conflict: None,
                returning: None,
            };
            storage.insert_row(&insert).unwrap();
        }

        // Update all rows (no WHERE clause)
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "status".to_string(),
                value: Expression::Literal(Value::String("new".to_string())),
            }],
            from: None,
            returning: None,
            where_clause: None,
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated.0, 3);

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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert).unwrap();

        // Update with non-matching condition
        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "id".to_string(),
                value: Expression::Literal(Value::Int(99)),
            }],
            from: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(999)),
                },
            }),
        };

        let updated = storage.update_rows(&update_stmt).unwrap();
        assert_eq!(updated.0, 0);

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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let update_stmt = UpdateStatement {
            table_name: "users".to_string(),
            assignments: vec![Assignment {
                column: "nonexistent".to_string(),
                value: Expression::Literal(Value::Int(1)),
            }],
            from: None,
            returning: None,
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
            constraints: vec![],
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
                value: Expression::Literal(Value::String("not a number".to_string())),
            }],
            from: None,
            returning: None,
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
            constraints: vec![],
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
                source: crate::parser::InsertSource::Values(vec![vec![Value::Int(id), Value::String(name.to_string())]]),
            
                columns: Vec::new(),
                on_conflict: None,
                returning: None,
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete where id = 2
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            using: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(2)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted.0, 1);

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
            constraints: vec![],
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
                source: crate::parser::InsertSource::Values(vec![vec![Value::Int(id), Value::Int(active)]]),
            
                columns: Vec::new(),
                on_conflict: None,
                returning: None,
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete inactive users (active = 0)
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            using: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("active".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(0)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted.0, 2);

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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        for i in 1..=5 {
            let insert = crate::parser::InsertStatement {
                table_name: "users".to_string(),
                source: crate::parser::InsertSource::Values(vec![vec![Value::Int(i)]]),
            
                columns: Vec::new(),
                on_conflict: None,
                returning: None,
            };
            storage.insert_row(&insert).unwrap();
        }

        // Delete all (no WHERE clause)
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            using: None,
            returning: None,
            where_clause: None,
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted.0, 5);

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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
            ],
        };
        storage.create_table(&create_stmt).unwrap();

        let insert = crate::parser::InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert).unwrap();

        // Delete with non-matching condition
        let delete_stmt = DeleteStatement {
            table_name: "users".to_string(),
            using: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
                    left: Expression::Column("id".to_string()),
                    operator: Operator::Equals,
                    right: Expression::Literal(Value::Int(999)),
                },
            }),
        };

        let deleted = storage.delete_rows(&delete_stmt).unwrap();
        assert_eq!(deleted.0, 0);

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
            using: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("name", DataType::Varchar(None)),
                ColumnDefinition::new("event_date", DataType::Date),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert = InsertStatement {
            table_name: "events".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::String("launch".to_string()), Value::String("2024-03-15".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert).unwrap();

        let rows = storage.read_rows("events").unwrap();
        // String "2024-03-15" is coerced to Date on insert; 2024-03-15 = 19797 days since epoch
        assert_eq!(rows[0][1], Value::Date(19797));

        // invalid date should fail
        let bad_insert = InsertStatement {
            table_name: "events".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::String("oops".to_string()), Value::String("not-a-date".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("msg", DataType::Varchar(None)),
                ColumnDefinition::new("created_at", DataType::Timestamp),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert = InsertStatement {
            table_name: "logs".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::String("hello".to_string()), Value::String("2024-03-15 14:30:00".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert).unwrap();

        let rows = storage.read_rows("logs").unwrap();
        // String is coerced to Timestamp on insert; 2024-03-15 14:30:00 UTC = 1710513000 secs
        assert_eq!(rows[0][1], Value::Timestamp(1710513000));

        // a completely invalid timestamp string should fail
        let bad_insert = InsertStatement {
            table_name: "logs".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::String("bad".to_string()), Value::String("not-a-timestamp".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: true, primary_key: false, not_null: false, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        // Insert with NULL for auto_increment column
        let insert1 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Null, Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert1).unwrap();

        let insert2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Null, Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert2).unwrap();

        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows[0][0], Value::Int(1));
        assert_eq!(rows[1][0], Value::Int(2));

        // Can also supply an explicit value
        let insert3 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(10), Value::String("Charlie".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        let insert1 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&insert1).unwrap();

        // Duplicate key should fail
        let insert2 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        };
        assert!(matches!(storage.insert_row(&insert2), Err(StorageError::DuplicateKey { .. })));

        // Different key should succeed
        let insert3 = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        // NULL primary key should fail
        let insert = InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Null, Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_users).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Child table with FK
        let create_orders = CreateTableStatement {
            table_name: "orders".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false,
                    references: Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() }), check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
        };
        storage.create_table(&create_orders).unwrap();

        // Valid FK reference
        storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(1)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Invalid FK reference should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::Int(999)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create_users).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Child table with FK
        let create_orders = CreateTableStatement {
            table_name: "orders".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false,
                    references: Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() }), check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
        };
        storage.create_table(&create_orders).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "orders".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(1)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Deleting referenced parent should fail
        let result = storage.delete_rows(&DeleteStatement {
            table_name: "users".to_string(),
            using: None,
            returning: None,
            where_clause: Some(crate::parser::WhereClause {
                condition: Condition::Comparison { upper_bound: None,
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
            using: None,
            returning: None,
            where_clause: Some(crate::parser::WhereClause {
                condition: Condition::Comparison { upper_bound: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "name".to_string(), data_type: DataType::Varchar(None),
                    auto_increment: false, primary_key: false, not_null: true, unique: false, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Valid insert
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // NULL in NOT NULL column should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::Null]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition { name: "email".to_string(), data_type: DataType::Varchar(None),
                    auto_increment: false, primary_key: false, not_null: false, unique: true, references: None , check_constraint: None, check_constraint_text: None, default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("a@b.com".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Duplicate unique value should fail
        let result = storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("a@b.com".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        });
        assert!(matches!(result, Err(StorageError::DuplicateKey { .. })));

        // NULL values don't violate uniqueness
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(3), Value::Null]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(4), Value::Null]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_create_and_lookup_index() {
        let temp_dir = format!("/tmp/abcsql_test_idx_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(3), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            using: None,
            returning: None,
            where_clause: Some(WhereClause {
                condition: Condition::Comparison { upper_bound: None,
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
            constraints: vec![],
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
            constraints: vec![],
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Bob".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(3), Value::String("Charlie".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("email", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("a@b.com".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("a@b.com".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        });
        assert!(matches!(result, Err(StorageError::DuplicateKey { .. })));

        // Inserting a different email should succeed
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(3), Value::String("c@d.com".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // NULL should not violate unique index
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(4), Value::Null]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_unique_index_rejects_existing_duplicates() {
        let temp_dir = format!("/tmp/abcsql_test_uidx_dup_{}", std::process::id());
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "users".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&create).unwrap();

        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![ColumnDefinition::new("id", DataType::Int)],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "t".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
                ColumnDefinition::new("temp", DataType::Int),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string()), Value::Int(99)]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
            columns: vec![id_col],
        }).unwrap();

        let mut fk_col = ColumnDefinition::new("user_id", DataType::Int);
        fk_col.references = Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() });
        storage.create_table(&CreateTableStatement {
            table_name: "orders".to_string(),
            constraints: vec![],
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
            constraints: vec![],
            columns: vec![id_col],
        }).unwrap();

        let mut fk_col = ColumnDefinition::new("user_id", DataType::Int);
        fk_col.references = Some(ForeignKeyRef { table: "users".to_string(), column: "id".to_string() });
        storage.create_table(&CreateTableStatement {
            table_name: "orders".to_string(),
            constraints: vec![],
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
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        }).unwrap();
        storage.insert_row(&InsertStatement {
            table_name: "users".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::String("Alice".to_string())]]),
        
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
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
            constraints: vec![],
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
            constraints: vec![],
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

    #[test]
    fn test_scalar_subquery_in_where() {
        use crate::parser::parse_sql;
        // Use lib::execute to run a full SQL roundtrip including scalar subquery
        let temp_dir = std::env::temp_dir().join("abcsql_test_scalar_subq");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        // Create two tables
        let create_users = "CREATE TABLE users (id INT, name VARCHAR)";
        let create_limits = "CREATE TABLE limits (max_id INT)";
        if let Ok((_, stmt)) = parse_sql(create_users) {
            if let crate::parser::SqlStatement::CreateTable(s) = stmt { storage.create_table(&s).unwrap(); }
        }
        if let Ok((_, stmt)) = parse_sql(create_limits) {
            if let crate::parser::SqlStatement::CreateTable(s) = stmt { storage.create_table(&s).unwrap(); }
        }

        // Insert rows
        for sql in ["INSERT INTO users VALUES (1, 'Alice')", "INSERT INTO users VALUES (2, 'Bob')", "INSERT INTO limits VALUES (1)"] {
            if let Ok((_, stmt)) = parse_sql(sql) {
                if let crate::parser::SqlStatement::Insert(s) = stmt { storage.insert_row(&s).unwrap(); }
            }
        }

        // Execute scalar subquery via evaluate_condition path (UPDATE WHERE)
        let update_sql = "UPDATE users SET name = 'Updated' WHERE id = (SELECT max_id FROM limits)";
        if let Ok((_, stmt)) = parse_sql(update_sql) {
            if let crate::parser::SqlStatement::Update(s) = stmt {
                let n = storage.update_rows(&s).unwrap();
                assert_eq!(n.0, 1);
            }
        }

        let rows = storage.read_rows("users").unwrap();
        // Row 0 (id=1) should now be 'Updated', row 1 (id=2) unchanged
        assert_eq!(rows[0][1], Value::String("Updated".to_string()));
        assert_eq!(rows[1][1], Value::String("Bob".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    // --- Transaction tests ---

    fn make_txn_storage(suffix: &str) -> (Storage, std::path::PathBuf) {
        let temp_dir = std::env::temp_dir().join(format!("abcsql_txn_{}", suffix));
        let _ = fs::remove_dir_all(&temp_dir);
        let s = Storage::new(&temp_dir).unwrap();
        (s, temp_dir)
    }

    fn make_users_table(storage: &Storage) {
        let stmt = CreateTableStatement {
            table_name: "users".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::Int),
                ColumnDefinition::new("name", DataType::Varchar(None)),
            ],
        };
        storage.create_table(&stmt).unwrap();
    }

    fn insert_user(storage: &Storage, id: i64, name: &str) {
        use crate::parser::{InsertStatement, InsertSource};
        let stmt = InsertStatement {
            table_name: "users".to_string(),
            columns: vec![],
            source: InsertSource::Values(vec![vec![Value::Int(id), Value::String(name.to_string())]]),
            on_conflict: None,
            returning: None,
        };
        storage.insert_row(&stmt).unwrap();
    }

    #[test]
    fn test_txn_basic_commit() {
        let (storage, temp_dir) = make_txn_storage("commit");
        make_users_table(&storage);

        storage.begin_transaction().unwrap();
        insert_user(&storage, 1, "Alice");
        storage.commit_transaction().unwrap();

        // Re-open storage and verify data persists
        let storage2 = Storage::new(&temp_dir).unwrap();
        let rows = storage2.read_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_basic_rollback() {
        let (storage, temp_dir) = make_txn_storage("rollback");
        make_users_table(&storage);

        storage.begin_transaction().unwrap();
        insert_user(&storage, 1, "Alice");
        // Verify row is present before rollback
        assert_eq!(storage.read_rows("users").unwrap().len(), 1);
        storage.rollback_transaction().unwrap();

        // Table should be empty again
        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 0);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_rollback_ddl() {
        let (storage, temp_dir) = make_txn_storage("ddl");

        storage.begin_transaction().unwrap();
        make_users_table(&storage);
        assert!(storage.table_exists("users"));
        storage.rollback_transaction().unwrap();

        // Table should no longer exist
        assert!(!storage.table_exists("users"));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_savepoint_rollback() {
        let (storage, temp_dir) = make_txn_storage("savepoint");
        make_users_table(&storage);

        storage.begin_transaction().unwrap();
        insert_user(&storage, 1, "Alice");
        storage.create_savepoint("sp1").unwrap();
        insert_user(&storage, 2, "Bob");

        // Both rows visible
        assert_eq!(storage.read_rows("users").unwrap().len(), 2);

        storage.rollback_to_savepoint("sp1").unwrap();

        // Only Alice should remain
        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));

        storage.commit_transaction().unwrap();
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_release_savepoint_then_rollback_fails() {
        let (storage, temp_dir) = make_txn_storage("release");
        make_users_table(&storage);

        storage.begin_transaction().unwrap();
        insert_user(&storage, 1, "Alice");
        storage.create_savepoint("sp1").unwrap();
        storage.release_savepoint("sp1").unwrap();

        // Rollback to released savepoint should error
        let result = storage.rollback_to_savepoint("sp1");
        assert!(result.is_err());

        storage.rollback_transaction().unwrap();
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_nested_savepoints() {
        let (storage, temp_dir) = make_txn_storage("nested");
        make_users_table(&storage);

        storage.begin_transaction().unwrap();
        insert_user(&storage, 1, "Alice");
        storage.create_savepoint("sp1").unwrap();
        insert_user(&storage, 2, "Bob");
        storage.create_savepoint("sp2").unwrap();
        insert_user(&storage, 3, "Carol");

        // All three visible
        assert_eq!(storage.read_rows("users").unwrap().len(), 3);

        // Roll back to sp1 — should be back to just Alice
        storage.rollback_to_savepoint("sp1").unwrap();
        let rows = storage.read_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::String("Alice".to_string()));

        // sp2 should also be gone now
        let result = storage.rollback_to_savepoint("sp2");
        assert!(result.is_err());

        storage.commit_transaction().unwrap();
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_no_transaction_errors() {
        let (storage, temp_dir) = make_txn_storage("notxn");

        // COMMIT without BEGIN
        assert!(storage.commit_transaction().is_err());
        // ROLLBACK without BEGIN
        assert!(storage.rollback_transaction().is_err());
        // SAVEPOINT without BEGIN
        assert!(storage.create_savepoint("sp1").is_err());

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_autoincrement_rolled_back() {
        let (storage, temp_dir) = make_txn_storage("autoinc");

        // Create table with auto-increment id
        let stmt = CreateTableStatement {
            table_name: "items".to_string(),
            constraints: vec![],
            columns: vec![{
                let mut c = ColumnDefinition::new("id", DataType::Int);
                c.auto_increment = true;
                c.primary_key = true;
                c
            }, ColumnDefinition::new("name", DataType::Varchar(None))],
        };
        storage.create_table(&stmt).unwrap();

        use crate::parser::{InsertStatement, InsertSource};
        let ins = || InsertStatement {
            table_name: "items".to_string(),
            columns: vec!["name".to_string()],
            source: InsertSource::Values(vec![vec![Value::String("thing".to_string())]]),
            on_conflict: None,
            returning: None,
        };

        // BEGIN → insert (gets id=1) → ROLLBACK
        storage.begin_transaction().unwrap();
        let (_, _) = storage.insert_row(&ins()).unwrap();
        storage.rollback_transaction().unwrap();

        // After rollback, seq should be back to 0; next insert should get id=1 again
        storage.begin_transaction().unwrap();
        let (_, _) = storage.insert_row(&ins()).unwrap();
        storage.commit_transaction().unwrap();

        let rows = storage.read_rows("items").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int(1));

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_txn_double_begin_error() {
        let (storage, temp_dir) = make_txn_storage("dbl_begin");
        storage.begin_transaction().unwrap();
        assert!(storage.begin_transaction().is_err());
        storage.rollback_transaction().unwrap();
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    // --- Type system extension tests ---

    #[test]
    fn test_serialize_deserialize_json_value() {
        let val = Value::Json(r#"{"key":"value"}"#.to_string());
        let s = serialize_value(&val);
        assert!(s.starts_with("JSON:"));
        let row = deserialize_row(&s).unwrap();
        assert_eq!(row[0], Value::Json(r#"{"key":"value"}"#.to_string()));
    }

    #[test]
    fn test_serialize_json_escaping() {
        // pipes in JSON must be escaped
        let val = Value::Json(r#"{"a|b":1}"#.to_string());
        let s = serialize_value(&val);
        assert!(!s.contains('|') || s.contains("\\|"));
        let row = deserialize_row(&s).unwrap();
        assert_eq!(row[0], Value::Json(r#"{"a|b":1}"#.to_string()));
    }

    #[test]
    fn test_data_type_to_string_new_types() {
        assert_eq!(data_type_to_string(&DataType::SmallInt), "SMALLINT");
        assert_eq!(data_type_to_string(&DataType::BigInt), "BIGINT");
        assert_eq!(data_type_to_string(&DataType::Real), "REAL");
        assert_eq!(data_type_to_string(&DataType::Char(Some(5))), "CHAR(5)");
        assert_eq!(data_type_to_string(&DataType::Char(None)), "CHAR");
        assert_eq!(data_type_to_string(&DataType::Text), "TEXT");
        assert_eq!(data_type_to_string(&DataType::Decimal(Some(10), Some(2))), "DECIMAL(10,2)");
        assert_eq!(data_type_to_string(&DataType::Uuid), "UUID");
        assert_eq!(data_type_to_string(&DataType::Json), "JSON");
        assert_eq!(data_type_to_string(&DataType::Jsonb), "JSONB");
    }

    #[test]
    fn test_parse_data_type_roundtrip() {
        let types = [
            "INT", "SMALLINT", "BIGINT", "FLOAT", "REAL", "DOUBLE",
            "BOOLEAN", "DATE", "TIMESTAMP", "TEXT", "UUID", "JSON", "JSONB",
            "VARCHAR", "VARCHAR(255)", "CHAR", "CHAR(10)",
            "DECIMAL", "DECIMAL(10,2)",
        ];
        for t in &types {
            let dt = parse_data_type(t).expect(t);
            let back = data_type_to_string(&dt);
            // roundtrip: re-parse should succeed
            parse_data_type(&back).expect(&format!("roundtrip failed for {}", t));
        }
    }

    #[test]
    fn test_create_table_new_types() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_new_types");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();
        let stmt = CreateTableStatement {
            table_name: "things".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition::new("id", DataType::BigInt),
                ColumnDefinition::new("data", DataType::Json),
                ColumnDefinition::new("name", DataType::Text),
            ],
        };
        storage.create_table(&stmt).unwrap();
        let schema = storage.load_schema("things").unwrap();
        assert_eq!(schema.columns[0].data_type, DataType::BigInt);
        assert_eq!(schema.columns[1].data_type, DataType::Json);
        assert_eq!(schema.columns[2].data_type, DataType::Text);
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    #[test]
    fn test_check_constraint_insert_valid() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_check_insert_valid");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        // Create table with a CHECK constraint
        let create = CreateTableStatement {
            table_name: "products".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "price".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: Some(crate::parser::parse_condition("price > 0").unwrap().1), check_constraint_text: Some("price > 0".to_string()), default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Valid insert should succeed
        storage.insert_row(&InsertStatement {
            table_name: "products".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(100)]]),
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Read back
        let rows = storage.read_rows("products").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Int(100));
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_check_constraint_insert_invalid() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_check_insert_invalid");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "products".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "price".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: Some(crate::parser::parse_condition("price > 0").unwrap().1), check_constraint_text: Some("price > 0".to_string()), default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Invalid insert (price <= 0) should fail
        let err = storage.insert_row(&InsertStatement {
            table_name: "products".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(0)]]),
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap_err();
        match err {
            StorageError::CheckConstraintViolation { column, .. } => {
                assert_eq!(column, "price");
            }
            _ => panic!("Expected CheckConstraintViolation, got: {:?}", err),
        }
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_check_constraint_update_invalid() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_check_update_invalid");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "products".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "price".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: Some(crate::parser::parse_condition("price > 0").unwrap().1), check_constraint_text: Some("price > 0".to_string()), default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Insert a valid row
        storage.insert_row(&InsertStatement {
            table_name: "products".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(100)]]),
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Update to invalid value should fail
        let err = storage.update_rows(&UpdateStatement {
            table_name: "products".to_string(),
            assignments: vec![crate::parser::Assignment { column: "price".to_string(), value: Expression::Literal(Value::Int(-5)) }],
            where_clause: None,
            from: None,
            returning: None,
        }).unwrap_err();
        match err {
            StorageError::CheckConstraintViolation { column, .. } => {
                assert_eq!(column, "price");
            }
            _ => panic!("Expected CheckConstraintViolation, got: {:?}", err),
        }
        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn test_check_constraint_schema_roundtrip() {
        let temp_dir = std::env::temp_dir().join("abcsql_test_check_schema_rt");
        let _ = fs::remove_dir_all(&temp_dir);
        let storage = Storage::new(&temp_dir).unwrap();

        let create = CreateTableStatement {
            table_name: "items".to_string(),
            constraints: vec![],
            columns: vec![
                ColumnDefinition { name: "id".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: true, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None },
                ColumnDefinition { name: "qty".to_string(), data_type: DataType::Int, auto_increment: false, primary_key: false, not_null: true, unique: false, references: None, check_constraint: Some(crate::parser::parse_condition("qty >= 0 AND qty < 1000").unwrap().1), check_constraint_text: Some("qty >= 0 AND qty < 1000".to_string()), default: None, default_text: None },
                ColumnDefinition { name: "status".to_string(), data_type: DataType::Varchar(None), auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: Some(crate::parser::parse_condition("status IN ('active', 'inactive')").unwrap().1), check_constraint_text: Some("status IN ('active', 'inactive')".to_string()), default: None, default_text: None },
            ],
        };
        storage.create_table(&create).unwrap();

        // Load schema back and verify CHECK constraints survived
        let loaded = storage.load_schema("items").unwrap();
        assert_eq!(loaded.columns[1].check_constraint.is_some(), true);
        assert_eq!(loaded.columns[1].check_constraint_text.as_deref(), Some("qty >= 0 AND qty < 1000"));
        assert_eq!(loaded.columns[2].check_constraint.is_some(), true);
        assert_eq!(loaded.columns[2].check_constraint_text.as_deref(), Some("status IN ('active', 'inactive')"));

        // Verify constraints are enforced after reload
        storage.insert_row(&InsertStatement {
            table_name: "items".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(1), Value::Int(50), Value::String("active".to_string())]]),
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap();

        // Violation after reload
        let err = storage.insert_row(&InsertStatement {
            table_name: "items".to_string(),
            source: crate::parser::InsertSource::Values(vec![vec![Value::Int(2), Value::Int(9999), Value::String("active".to_string())]]),
            columns: Vec::new(),
            on_conflict: None,
            returning: None,
        }).unwrap_err();
        match err {
            StorageError::CheckConstraintViolation { column, .. } => {
                assert_eq!(column, "qty");
            }
            _ => panic!("Expected CheckConstraintViolation, got: {:?}", err),
        }
        fs::remove_dir_all(&temp_dir).unwrap();
    }

}
