

/// SQL AST (Abstract Syntax Tree) nodes

#[derive(Debug, PartialEq, Clone)]
pub enum SqlStatement {
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    CreateFunction(CreateFunctionStatement),
    DropIndex(DropIndexStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
    DropFunction(DropFunctionStatement),
    AlterTable(AlterTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Truncate(TruncateStatement),
    Merge(MergeStatement),
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct TruncateStatement {
    pub table_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MergeStatement {
    pub target: String,
    pub target_alias: Option<String>,
    pub source: MergeSource,
    pub source_alias: Option<String>,
    pub on: Condition,
    pub when_clauses: Vec<WhenClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhenClause {
    pub is_matched: bool,
    pub condition: Option<Condition>,
    pub action: MergeAction,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MergeSource {
    Table(String),
    Subquery(Box<SelectStatement>),
    Values(Vec<Vec<Expression>>, Vec<String>),  // rows and optional column names
}

#[derive(Debug, PartialEq, Clone)]
pub enum MergeAction {
    Update(Vec<Assignment>),
    Delete,
    Insert(Vec<String>, Vec<Expression>),  // (column_names, values)
    DoNothing,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
    /// Raw SQL text of the whole constraint, preserved for schema file round-trips.
    pub raw: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableConstraintKind {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        /// Empty means "the referenced table's PRIMARY KEY columns"
        ref_columns: Vec<String>,
        on_delete: RefAction,
        on_update: RefAction,
    },
    Check(Condition),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropIndexStatement {
    pub index_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateViewStatement {
    pub view_name: String,
    pub select_sql: String, // stored as raw SQL for persistence
    pub select: SelectStatement,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropViewStatement {
    pub view_name: String,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateFunctionStatement {
    pub name: String,
    pub params: Vec<(String, String)>,  // (param_name, param_type)
    pub return_type: Option<String>,
    pub body: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropFunctionStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub action: AlterAction,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    RenameColumn { from: String, to: String },
    RenameTable(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub auto_increment: bool,
    pub primary_key: bool,
    pub not_null: bool,
    pub unique: bool,
    pub references: Option<ForeignKeyRef>,
    pub check_constraint: Option<Condition>,
    /// Raw SQL text of the CHECK condition (without CHECK(...) wrapper), preserved for
    /// round-trip serialization to/from the schema file.
    pub check_constraint_text: Option<String>,
    pub default: Option<Expression>,
    /// Raw SQL text of the DEFAULT expression, preserved for schema file round-trips.
    pub default_text: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
    pub on_delete: RefAction,
    pub on_update: RefAction,
}

/// Referential action for ON DELETE / ON UPDATE
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RefAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl RefAction {
    pub fn as_sql(&self) -> &'static str {
        match self {
            RefAction::NoAction => "NO ACTION",
            RefAction::Restrict => "RESTRICT",
            RefAction::Cascade => "CASCADE",
            RefAction::SetNull => "SET NULL",
            RefAction::SetDefault => "SET DEFAULT",
        }
    }

    pub fn from_sql(s: &str) -> Option<RefAction> {
        match s.to_uppercase().as_str() {
            "NO ACTION" => Some(RefAction::NoAction),
            "RESTRICT" => Some(RefAction::Restrict),
            "CASCADE" => Some(RefAction::Cascade),
            "SET NULL" => Some(RefAction::SetNull),
            "SET DEFAULT" => Some(RefAction::SetDefault),
            _ => None,
        }
    }
}

#[cfg(test)]
impl ColumnDefinition {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self { name: name.to_string(), data_type, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None, check_constraint: None, check_constraint_text: None, default: None, default_text: None }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    SmallInt,
    BigInt,
    Float,
    Real,                              // alias for FLOAT
    Double,
    Boolean,
    Date,
    Timestamp,
    Time,                              // TIME — stored as 'HH:MM:SS' string
    Interval,                          // INTERVAL — stored as integer seconds
    Bit(Option<usize>),                // BIT(n) — fixed-length string of 0/1
    BitVarying(Option<usize>),         // BIT VARYING(n) — variable-length string of 0/1
    Varchar(Option<usize>),            // VARCHAR(255) or VARCHAR
    Char(Option<usize>),               // CHAR(n) or CHAR
    Text,                              // unlimited text
    Decimal(Option<u8>, Option<u8>),   // DECIMAL(p, s) / NUMERIC
    Uuid,
    Json,
    Jsonb,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,   // empty means "all columns in schema order"
    pub source: InsertSource,
    pub on_conflict: Option<OnConflict>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Value>>),    // multi-row: each inner Vec is one row
    Select(Box<SelectStatement>),
    DefaultValues,              // INSERT INTO t DEFAULT VALUES
}

#[derive(Debug, PartialEq, Clone)]
pub enum OnConflict {
    DoNothing,
    DoUpdate {
        conflict_columns: Vec<String>,
        assignments: Vec<Assignment>,
    },
}

#[cfg(test)]
impl InsertStatement {
    // Backwards-compat helper: returns first row's values
    pub fn values(&self) -> &[Value] {
        match &self.source {
            InsertSource::Values(rows) => rows.first().map(|r| r.as_slice()).unwrap_or(&[]),
            InsertSource::Select(_) | InsertSource::DefaultValues => &[],
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub from: Option<(String, Option<String>)>,  // (table_name, alias) for UPDATE ... FROM
    pub where_clause: Option<WhereClause>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,   // supports column references and arithmetic
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub using: Option<(String, Option<String>)>,  // (table_name, alias) for DELETE ... USING
    pub where_clause: Option<WhereClause>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub ctes: Vec<CteDefinition>,
    pub columns: Vec<SelectColumn>,
    pub distinct: bool,
    pub from: FromClause,
    pub from_alias: Option<String>,
    pub where_clause: Option<WhereClause>,
    pub joins: Vec<JoinClause>,
    pub group_by: Vec<SelectColumn>,
    // None = regular GROUP BY, Some = ROLLUP/CUBE/GROUPING SETS expansion
    pub grouping_sets: Option<Vec<Vec<SelectColumn>>>,
    pub having: Option<WhereClause>,
    pub window_defs: Vec<(String, WindowSpec)>,  // WINDOW clause named specs
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub union: Option<(UnionType, Box<SelectStatement>)>,
    pub for_update: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum UnionType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FromClause {
    Table(String),
    Subquery(Box<SelectStatement>),
    Values(Vec<Vec<Expression>>, Vec<String>), // inline VALUES rows, optional column names
}

impl FromClause {
    /// Get the table name, or None for subqueries/values
    #[allow(dead_code)]
    pub fn table_name(&self) -> Option<&str> {
        match self {
            FromClause::Table(name) => Some(name),
            FromClause::Subquery(_) | FromClause::Values(_, _) => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CteDefinition {
    pub name: String,
    pub columns: Vec<String>, // optional column name list: counter(n, m)
    pub query: Box<SelectStatement>,
    pub recursive: bool, // true when declared inside WITH RECURSIVE
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    All, // *
    StarFromTable(String), // t.*
    Column(String),
    QualifiedColumn(String, String), // table.column
    Aggregate(AggregateFunc, Box<SelectColumn>), // COUNT(*), SUM(col), etc.
    AggregateFiltered(AggregateFunc, Box<SelectColumn>, Box<Condition>), // COUNT(*) FILTER (WHERE ...)
    Alias(Box<SelectColumn>, String), // expr AS name
    Expr(Expression), // arithmetic expression like price * 2
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunc {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FrameMode { Rows, Range, Groups }

#[derive(Debug, PartialEq, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FrameSpec {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WindowSpec {
    pub base_window: Option<String>,   // e.g. "w" in OVER w or OVER (w ORDER BY ...)
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByClause>,
    pub frame: Option<FrameSpec>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Lag(Box<Expression>, i64),  // expression, offset (default 1)
    Lead(Box<Expression>, i64), // expression, offset (default 1)
    Agg(AggregateFunc, Box<SelectColumn>), // e.g. SUM(col) OVER (...)
    Ntile(Box<Expression>),        // NTILE(n) — n is the bucket count
    PercentRank,                   // PERCENT_RANK()
    CumeDist,                      // CUME_DIST()
    FirstValue(Box<Expression>),   // FIRST_VALUE(expr)
    LastValue(Box<Expression>),    // LAST_VALUE(expr)
    NthValue(Box<Expression>, Box<Expression>), // NTH_VALUE(expr, n)
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TrimMode { Leading, Trailing, Both }

#[derive(Debug, PartialEq, Clone)]
pub enum ScalarFunc {
    Upper,
    Lower,
    Length,
    CharLength,   // CHAR_LENGTH / CHARACTER_LENGTH — counts characters
    OctetLength,  // OCTET_LENGTH — counts bytes
    Trim,
    // Spec-form TRIM([LEADING|TRAILING|BOTH] [chars] FROM str); None = whitespace
    TrimChars(TrimMode, Option<String>),
    LTrim,
    RTrim,
    Abs,
    Ceil,
    Floor,
    Sqrt,
    Sign,
    Trunc,
    Reverse,
    // Date extraction functions
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DayOfWeek,
    DayOfYear,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause {
    pub column: SelectColumn,
    pub descending: bool,
    pub nulls_first: Option<bool>, // None = default (NULLs last for ASC, first for DESC)
}
#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub on: Option<Condition>,
    pub using: Option<Vec<String>>, // USING (col1, col2, ...)
    pub lateral: Option<Box<SelectStatement>>, // Some if LATERAL (SELECT ...)
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    Comparison {
        left: Expression,
        operator: Operator,
        right: Expression,
        // upper bound for BETWEEN / NOT BETWEEN
        upper_bound: Option<Expression>,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    // expr op ANY (SELECT ...) — true if any row satisfies
    AnyComparison { left: Expression, op: Operator, subquery: Box<SelectStatement> },
    // expr op ALL (SELECT ...) — true if all rows satisfy
    AllComparison { left: Expression, op: Operator, subquery: Box<SelectStatement> },
    // UNIQUE (SELECT ...) — true if all rows are distinct
    Unique(Box<SelectStatement>),
    // NOT UNIQUE (SELECT ...) — true if any duplicate rows exist
    NotUnique(Box<SelectStatement>),
    // (start1, end1) OVERLAPS (start2, end2) — true if time periods overlap
    Overlaps(Box<Expression>, Box<Expression>, Box<Expression>, Box<Expression>),
}

#[cfg(test)]
impl Condition {
    pub fn left(&self) -> Expression {
        if let Condition::Comparison { left, .. } = self { left.clone() } else { panic!("not a comparison") }
    }
    pub fn right(&self) -> Expression {
        if let Condition::Comparison { right, .. } = self { right.clone() } else { panic!("not a comparison") }
    }
    pub fn operator(&self) -> Operator {
        if let Condition::Comparison { operator, .. } = self { operator.clone() } else { panic!("not a comparison") }
    }
    pub fn upper_bound(&self) -> Option<Expression> {
        if let Condition::Comparison { upper_bound, .. } = self { upper_bound.clone() } else { None }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Column(String),
    QualifiedColumn(String, String), // table.column
    BinaryOp(Box<Expression>, ArithOp, Box<Expression>),
    Literal(Value),
    Subquery(Box<SelectStatement>),
    // Aggregate function call (used in HAVING clauses): COUNT(*), SUM(col), etc.
    Aggregate(AggregateFunc, Box<SelectColumn>),
    // CASE WHEN cond THEN expr ... [ELSE expr] END
    Case(Vec<(Condition, Expression)>, Option<Box<Expression>>),
    // Expression list for IN (expr, expr, ...) — items can be any scalar expressions
    List(Vec<Expression>),
    // Scalar string function: UPPER(expr), LOWER(expr), etc.
    ScalarFunc(ScalarFunc, Box<Expression>),
    // COALESCE(expr, expr, ...) — first non-NULL value
    Coalesce(Vec<Expression>),
    // NULLIF(expr, expr) — NULL if both args are equal, else first arg
    NullIf(Box<Expression>, Box<Expression>),
    // ROUND(expr [, places])
    Round(Box<Expression>, Option<Box<Expression>>),
    // CONCAT(expr, expr, ...)
    Concat(Vec<Expression>),
    // SUBSTR(str, start [, len]) — 1-indexed
    Substr(Box<Expression>, Box<Expression>, Option<Box<Expression>>),
    // CAST(expr AS type)
    Cast(Box<Expression>, String),
    // REPLACE(str, from, to)
    Replace(Box<Expression>, Box<Expression>, Box<Expression>),
    // TRANSLATE(str, from_chars, to_chars)
    Translate(Box<Expression>, Box<Expression>, Box<Expression>),
    // LPAD(str, len, pad) / RPAD(str, len, pad)
    LPad(Box<Expression>, Box<Expression>, Box<Expression>),
    RPad(Box<Expression>, Box<Expression>, Box<Expression>),
    // GREATEST(expr, ...) / LEAST(expr, ...) — return max/min ignoring NULLs
    Greatest(Vec<Expression>),
    Least(Vec<Expression>),
    // POWER(base, exp) / POW(base, exp)
    Power(Box<Expression>, Box<Expression>),
    // POSITION(needle IN haystack) — 1-based index or 0 if not found
    Position(Box<Expression>, Box<Expression>),
    // REPEAT(str, n)
    Repeat(Box<Expression>, Box<Expression>),
    // Window function: func() OVER (PARTITION BY ... ORDER BY ...)
    Window(WindowFunc, WindowSpec),
    // Date/time expressions
    CurrentDate,
    CurrentTimestamp,
    CurrentTime,  // CURRENT_TIME / LOCALTIME — evaluates to an 'HH:MM:SS' string
    CurrentUser,  // CURRENT_USER / SESSION_USER / USER
    // expr AT TIME ZONE 'UTC'/'+HH:MM' — shifts a timestamp by a fixed offset in seconds
    AtTimeZone(Box<Expression>, i64),
    // EXTRACT(field FROM expr) or DATE_PART('field', expr)
    Extract(String, Box<Expression>),
    // DATE_TRUNC('unit', expr)
    DateTrunc(String, Box<Expression>),
    // DATEDIFF(unit, date1, date2) → integer difference
    DateDiff(String, Box<Expression>, Box<Expression>),
    // DATEADD(unit, n, date) → shifted date/timestamp
    DateAdd(Box<Expression>, i64, String),
    // JSON functions
    JsonTypeOf(Box<Expression>),                           // JSON_TYPEOF(expr)
    JsonArrayLength(Box<Expression>),                      // JSON_ARRAY_LENGTH(expr)
    JsonBuildObject(Vec<(Expression, Expression)>),      // JSON_BUILD_OBJECT(key, val, ...)
    JsonBuildArray(Vec<Expression>),
    UserFunc(String, Vec<Expression>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,      // % operator
    Concat,   // || operator
    JsonGet,  // -> returns JSON
    JsonGetText, // ->> returns text
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Like,
    NotLike,
    ILike,
    NotILike,
    In,
    NotIn,
    Exists,
    NotExists,
    IsNull,
    IsNotNull,
    Between,
    NotBetween,
    JsonContains, // @>
    IsDistinctFrom,
    IsNotDistinctFrom,
    Similar,
    NotSimilar,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Date(i32),       // days since 1970-01-01
    Timestamp(i64),  // seconds since 1970-01-01 00:00:00 UTC
    Json(String),    // JSON value stored as raw text
    Null,
    /// Marker for the DEFAULT keyword in INSERT/UPDATE; resolved against the
    /// column's default before any value is stored.
    Default,
}
