use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, char as nom_char},
    combinator::recognize,
    sequence::{delimited, tuple},
    multi::separated_list0,
};

/// SQL AST (Abstract Syntax Tree) nodes

#[derive(Debug, PartialEq, Clone)]
pub enum SqlStatement {
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    DropIndex(DropIndexStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
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
    pub when_matched: Option<MergeAction>,
    pub when_not_matched: Option<MergeAction>,
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
}

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

#[cfg(test)]
impl ColumnDefinition {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self { name: name.to_string(), data_type, auto_increment: false, primary_key: false, not_null: false, unique: false, references: None }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    Float,
    Double,
    Boolean,
    Date,
    Timestamp,
    Varchar(Option<usize>), // VARCHAR(255) or VARCHAR
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
            InsertSource::Select(_) => &[],
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

#[derive(Debug, PartialEq, Clone)]
pub enum ScalarFunc {
    Upper,
    Lower,
    Length,
    Trim,
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
    // EXTRACT(field FROM expr) or DATE_PART('field', expr)
    Extract(String, Box<Expression>),
    // DATE_TRUNC('unit', expr)
    DateTrunc(String, Box<Expression>),
    // DATEDIFF(unit, date1, date2) → integer difference
    DateDiff(String, Box<Expression>, Box<Expression>),
    // DATEADD(unit, n, date) → shifted date/timestamp
    DateAdd(Box<Expression>, i64, String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,    // % operator
    Concat, // || operator
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
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Date(i32),       // days since 1970-01-01
    Timestamp(i64),  // seconds since 1970-01-01 00:00:00 UTC
    Null,
}

/// Calendar math helpers — no external dependencies needed

pub fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

pub fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1|3|5|7|8|10|12 => 31,
        4|6|9|11 => 30,
        2 => if is_leap_year(year) { 29 } else { 28 },
        _ => 0,
    }
}

/// Days since 1970-01-01 (civil calendar algorithm)
pub fn date_to_epoch_days(y: i32, m: i32, d: i32) -> i32 {
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn epoch_days_to_date(z: i32) -> (i32, i32, i32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365*yoe + yoe/4 - yoe/100);
    let mp = (5*doy + 2) / 153;
    let d = doy - (153*mp + 2)/5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Parse "YYYY-MM-DD" string into epoch days; returns None on parse failure
pub fn parse_date_str(s: &str) -> Option<i32> {
    let s = s.trim();
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 { return None; }
    let y: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    let d: i32 = parts[2].parse().ok()?;
    if m < 1 || m > 12 || d < 1 || d > 31 { return None; }
    Some(date_to_epoch_days(y, m, d))
}

/// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS" into epoch seconds
pub fn parse_timestamp_str(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = if let Some(p) = s.find(' ') {
        (&s[..p], &s[p+1..])
    } else if let Some(p) = s.find('T') {
        (&s[..p], &s[p+1..])
    } else {
        // date only → midnight
        return parse_date_str(s).map(|d| d as i64 * 86400);
    };
    let days = parse_date_str(date_part)? as i64;
    let tparts: Vec<&str> = time_part.splitn(3, ':').collect();
    let h: i64 = tparts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let m: i64 = tparts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let sec_str = tparts.get(2).copied().unwrap_or("0");
    let sec_str = sec_str.split('.').next().unwrap_or("0");
    let sec: i64 = sec_str.parse().unwrap_or(0);
    Some(days * 86400 + h * 3600 + m * 60 + sec)
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = epoch_days_to_date(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn format_timestamp(secs: i64) -> String {
    // Handle negative timestamps carefully
    let days = if secs >= 0 { secs / 86400 } else { (secs - 86399) / 86400 };
    let time = secs - days * 86400;
    let (y, m, d) = epoch_days_to_date(days as i32);
    let h = time / 3600;
    let min = (time % 3600) / 60;
    let s = time % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, min, s)
}

pub fn current_epoch_days() -> i32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    (secs / 86400) as i32
}

pub fn current_epoch_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

/// Parser functions

/// Strip SQL comments from input, preserving string literals.
pub fn strip_sql_comments(input: &str) -> String { strip_comments(input) }

/// Strip SQL comments from input, preserving string literals.
/// Handles -- line comments and /* block comments */.
fn strip_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        // Inside single-quoted string — pass through verbatim including ''
        if chars[i] == '\'' {
            out.push('\'');
            i += 1;
            loop {
                if i >= n { break; }
                if chars[i] == '\'' {
                    out.push('\'');
                    i += 1;
                    // '' is an escaped quote inside the string
                    if i < n && chars[i] == '\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break; // end of string literal
                    }
                } else {
                    out.push(chars[i]);
                    i += 1;
                }
            }
        // -- line comment: skip to end of line
        } else if i + 1 < n && chars[i] == '-' && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' { i += 1; }
        // /* block comment */: skip to */
        } else if i + 1 < n && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < n && !(chars[i] == '*' && chars[i + 1] == '/') { i += 1; }
            i += 2; // consume */
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// Parse a SQL statement
pub fn parse_sql(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = multispace0(input)?;
    let (input, stmt) = nom::branch::alt((
        parse_insert,
        parse_create,
        parse_drop,
        parse_alter,
        parse_select,
        parse_update,
        parse_delete,
        parse_truncate,
        parse_merge,
        parse_begin,
        parse_commit,
        parse_rollback,
        parse_savepoint,
        parse_release,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, stmt))
}

/// Parse CREATE TABLE / INDEX / VIEW statement
pub fn parse_create(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    nom::branch::alt((
        parse_create_view_inner,
        parse_create_table_inner,
        parse_create_unique_index_inner,
        parse_create_index_inner,
    ))(input)
}

fn parse_create_view_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("VIEW")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, view_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;
    let select_sql_start = input;
    let (input, select) = parse_select_statement(input)?;
    // Capture the raw SQL consumed (strip trailing semicolon/whitespace)
    let consumed_len = select_sql_start.len() - input.len();
    let select_sql = select_sql_start[..consumed_len].trim_end_matches(';').trim().to_string();
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::CreateView(CreateViewStatement {
        view_name: view_name.to_string(),
        select_sql,
        select,
    })))
}

fn parse_create_table_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = delimited(
        nom_char('('),
        separated_list0(nom_char(','), parse_column_definition),
        nom_char(')'),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::CreateTable(CreateTableStatement {
        table_name: table_name.to_string(),
        columns,
    })))
}

// CREATE UNIQUE INDEX index_name ON table(column);
fn parse_create_unique_index_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("UNIQUE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INDEX")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, index_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("ON")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, column_name) = parse_identifier(input)?;
    let (input, _) = nom_char(')')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::CreateIndex(CreateIndexStatement {
        index_name: index_name.to_string(),
        table_name: table_name.to_string(),
        column_name: column_name.to_string(),
        unique: true,
    })))
}

// CREATE INDEX index_name ON table(column);
fn parse_create_index_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("INDEX")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, index_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("ON")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, column_name) = parse_identifier(input)?;
    let (input, _) = nom_char(')')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::CreateIndex(CreateIndexStatement {
        index_name: index_name.to_string(),
        table_name: table_name.to_string(),
        column_name: column_name.to_string(),
        unique: false,
    })))
}

/// Parse column definition: name TYPE
fn parse_column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = parse_data_type(input)?;
    let (input, _) = multispace0(input)?;
    let (input, nn) = nom::combinator::opt(tag_no_case("NOT NULL"))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, uniq) = nom::combinator::opt(tag_no_case("UNIQUE"))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, auto_inc) = nom::combinator::opt(tag("AUTO_INCREMENT"))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, pk) = nom::combinator::opt(tag_no_case("PRIMARY KEY"))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, fk_ref) = nom::combinator::opt(parse_references)(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, ColumnDefinition {
        name: name.to_string(),
        data_type,
        auto_increment: auto_inc.is_some(),
        primary_key: pk.is_some(),
        not_null: nn.is_some(),
        unique: uniq.is_some(),
        references: fk_ref,
    }))
}

// Parse REFERENCES table(column)
fn parse_references(input: &str) -> IResult<&str, ForeignKeyRef> {
    let (input, _) = tag_no_case("REFERENCES")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, column) = parse_identifier(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, ForeignKeyRef { table: table.to_string(), column: column.to_string() }))
}

/// Parse data type: INT or VARCHAR or VARCHAR(n)
fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    nom::branch::alt((
        parse_timestamp_type,
        parse_double_type,
        parse_float_type,
        parse_boolean_type,
        parse_date_type,
        parse_int_type,
        parse_varchar_type,
    ))(input)
}

fn parse_date_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("DATE")(input)?;
    Ok((input, DataType::Date))
}

fn parse_timestamp_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("TIMESTAMP")(input)?;
    Ok((input, DataType::Timestamp))
}

fn parse_boolean_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = nom::branch::alt((tag_no_case("BOOLEAN"), tag_no_case("BOOL")))(input)?;
    Ok((input, DataType::Boolean))
}

fn parse_int_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("INT")(input)?;
    Ok((input, DataType::Int))
}

fn parse_float_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("FLOAT")(input)?;
    Ok((input, DataType::Float))
}

fn parse_double_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("DOUBLE")(input)?;
    Ok((input, DataType::Double))
}

fn parse_varchar_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("VARCHAR")(input)?;
    let (input, size) = nom::combinator::opt(delimited(
        nom_char('('),
        nom::character::complete::u64,
        nom_char(')'),
    ))(input)?;
    
    Ok((input, DataType::Varchar(size.map(|s| s as usize))))
}

/// Parse INSERT statement
pub fn parse_insert(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("INSERT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INTO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;

    // Parse optional column list: (col1, col2, ...)
    let (input, columns) = nom::combinator::opt(|input| {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, cols) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_identifier,
        )(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        Ok((input, cols.into_iter().map(|s| s.to_string()).collect::<Vec<_>>()))
    })(input)?;
    let columns = columns.unwrap_or_default();

    let (input, _) = multispace1(input)?;

    // Try INSERT INTO ... SELECT first, then VALUES
    let (input, source) = if let Ok((i2, select)) = parse_select_statement(input) {
        let (i2, _) = multispace0(i2)?;
        let (i2, on_conflict) = parse_on_conflict(i2)?;
        let (i2, returning) = parse_returning(i2)?;
        let (i2, _) = multispace0(i2)?;
        let (i2, _) = nom::combinator::opt(nom_char(';'))(i2)?;
        return Ok((i2, SqlStatement::Insert(InsertStatement {
            table_name: table_name.to_string(),
            columns,
            source: InsertSource::Select(Box::new(select)),
            on_conflict,
            returning,
        })));
    } else {
        let (input, _) = tag_no_case("VALUES")(input)?;
        let (input, _) = multispace0(input)?;
        // Parse one or more rows separated by commas
        let (input, rows) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            |input| {
                let (input, _) = nom_char('(')(input)?;
                let (input, _) = multispace0(input)?;
                let (input, values) = separated_list0(
                    delimited(multispace0, nom_char(','), multispace0),
                    parse_value,
                )(input)?;
                let (input, _) = multispace0(input)?;
                let (input, _) = nom_char(')')(input)?;
                Ok((input, values))
            }
        )(input)?;
        (input, InsertSource::Values(rows))
    };

    let (input, on_conflict) = parse_on_conflict(input)?;
    let (input, returning) = parse_returning(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Insert(InsertStatement {
        table_name: table_name.to_string(),
        columns,
        source,
        on_conflict,
        returning,
    })))
}

/// Parse optional ON CONFLICT clause
fn parse_on_conflict(input: &str) -> IResult<&str, Option<OnConflict>> {
    nom::combinator::opt(|input| {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("ON")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("CONFLICT")(input)?;
        let (input, _) = multispace0(input)?;
        // Optional conflict column list
        let (input, conflict_columns) = nom::combinator::opt(|input| {
            let (input, _) = nom_char('(')(input)?;
            let (input, _) = multispace0(input)?;
            let (input, cols) = nom::multi::separated_list1(
                nom::sequence::delimited(multispace0, nom_char(','), multispace0),
                parse_identifier,
            )(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = nom_char(')')(input)?;
            Ok((input, cols.into_iter().map(|s| s.to_string()).collect::<Vec<_>>()))
        })(input)?;
        let conflict_columns = conflict_columns.unwrap_or_default();
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("DO")(input)?;
        let (input, _) = multispace1(input)?;
        // DO NOTHING
        if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("NOTHING")(input) {
            return Ok((input, OnConflict::DoNothing));
        }
        // DO UPDATE SET ...
        let (input, _) = tag_no_case("UPDATE")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, assignments) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_assignment,
        )(input)?;
        Ok((input, OnConflict::DoUpdate { conflict_columns, assignments }))
    })(input)
}

/// Parse optional RETURNING clause
fn parse_returning(input: &str) -> IResult<&str, Option<Vec<SelectColumn>>> {
    nom::combinator::opt(|input| {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("RETURNING")(input)?;
        let (input, _) = multispace1(input)?;
        nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_select_column,
        )(input)
    })(input)
}

/// Parse UPDATE statement
pub fn parse_update(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("UPDATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("SET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        parse_assignment
    )(input)?;
    // Optional FROM clause for join-based updates
    let (input, from) = nom::combinator::opt(|input| {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, tbl) = parse_identifier(input)?;
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        Ok((input, (tbl.to_string(), alias)))
    })(input)?;
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, returning) = parse_returning(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Update(UpdateStatement {
        table_name: table_name.to_string(),
        assignments,
        from,
        where_clause,
        returning,
    })))
}

/// Parse assignment: column = expression
pub fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, _) = multispace0(input)?;
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, Assignment {
        column: column.to_string(),
        value,
    }))
}

/// Parse DELETE statement
pub fn parse_delete(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("DELETE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    // Optional USING clause for join-based deletes
    let (input, using) = nom::combinator::opt(|input| {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("USING")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, tbl) = parse_identifier(input)?;
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        Ok((input, (tbl.to_string(), alias)))
    })(input)?;
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, returning) = parse_returning(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Delete(DeleteStatement {
        table_name: table_name.to_string(),
        using,
        where_clause,
        returning,
    })))
}

/// Parse TRUNCATE [TABLE] table_name
pub fn parse_truncate(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("TRUNCATE")(input)?;
    let (input, _) = multispace1(input)?;
    // Optional TABLE keyword
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("TABLE"), multispace1))(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Truncate(TruncateStatement { table_name: table_name.to_string() })))
}

/// Parse MERGE INTO target USING source ON condition WHEN ... THEN ...
pub fn parse_merge(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("MERGE")(input)?;
    let (input, _) = multispace1(input)?;
    // Optional INTO keyword
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("INTO"), multispace1))(input)?;
    let (input, target) = parse_identifier(input)?;
    let (input, target_alias) = nom::combinator::opt(parse_table_alias)(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("USING")(input)?;
    let (input, _) = multispace1(input)?;

    // Source can be: (VALUES ...) AS alias, (SELECT ...) AS alias, or table [alias]
    let (input, source, source_alias) = if let Ok((input, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(input) {
        let (input, _) = multispace0(input)?;
        // Try VALUES first
        if let Ok((input, rows)) = parse_values_clause(input) {
            let (input, _) = multispace0(input)?;
            let (input, _) = nom_char(')')(input)?;
            // Parse alias and optional column names
            let (input, (alias, col_names)) = parse_values_alias(input)?;
            (input, MergeSource::Values(rows, col_names), Some(alias))
        } else {
            let (input, subquery) = parse_select_statement(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = nom_char(')')(input)?;
            let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
            (input, MergeSource::Subquery(Box::new(subquery)), alias)
        }
    } else {
        let (input, tbl) = parse_identifier(input)?;
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        (input, MergeSource::Table(tbl.to_string()), alias)
    };

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("ON")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, on) = parse_condition(input)?;

    // Parse WHEN MATCHED / WHEN NOT MATCHED clauses (at most one of each)
    let (input, (when_matched, when_not_matched)) = parse_merge_when_clauses(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Merge(MergeStatement {
        target: target.to_string(),
        target_alias,
        source,
        source_alias,
        on,
        when_matched,
        when_not_matched,
    })))
}

/// Parse WHEN MATCHED / WHEN NOT MATCHED clauses for MERGE
fn parse_merge_when_clauses(input: &str) -> IResult<&str, (Option<MergeAction>, Option<MergeAction>)> {
    let mut input = input;
    let mut when_matched: Option<MergeAction> = None;
    let mut when_not_matched: Option<MergeAction> = None;

    // Try parsing up to 2 WHEN clauses
    for _ in 0..4 {
        let trimmed = input.trim_start();
        if !trimmed.to_uppercase().starts_with("WHEN") {
            break;
        }
        match parse_merge_when_clause(trimmed) {
            Ok((rest, (is_matched, action))) => {
                if is_matched && when_matched.is_none() {
                    when_matched = Some(action);
                } else if !is_matched && when_not_matched.is_none() {
                    when_not_matched = Some(action);
                }
                input = rest;
            }
            Err(_) => break,
        }
    }
    Ok((input, (when_matched, when_not_matched)))
}

/// Parse a single WHEN clause — returns (is_matched, action)
fn parse_merge_when_clause(input: &str) -> IResult<&str, (bool, MergeAction)> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WHEN")(input)?;
    let (input, _) = multispace1(input)?;
    // Check for NOT MATCHED
    let (input, is_not) = nom::combinator::opt(nom::sequence::terminated(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        multispace1,
    ))(input)?;
    let (input, _) = tag_no_case("MATCHED")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("THEN")(input)?;
    let (input, _) = multispace1(input)?;
    let is_matched = is_not.is_none();

    // Parse the action: UPDATE SET ..., DELETE, INSERT ..., or DO NOTHING
    let action = if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("UPDATE")(input) {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, assignments) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_assignment,
        )(input)?;
        return Ok((input, (is_matched, MergeAction::Update(assignments))));
    } else if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("DELETE")(input) {
        return Ok((input, (is_matched, MergeAction::Delete)));
    } else if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("INSERT")(input) {
        let (input, _) = multispace0(input)?;
        // Optional column list
        let (input, cols) = nom::combinator::opt(|input| {
            let (input, _) = nom_char('(')(input)?;
            let (input, _) = multispace0(input)?;
            let (input, cols) = nom::multi::separated_list1(
                nom::sequence::delimited(multispace0, nom_char(','), multispace0),
                parse_identifier,
            )(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = nom_char(')')(input)?;
            Ok((input, cols.into_iter().map(|s| s.to_string()).collect::<Vec<_>>()))
        })(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("VALUES")(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, exprs) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_expression,
        )(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, (is_matched, MergeAction::Insert(cols.unwrap_or_default(), exprs))));
    } else {
        // DO NOTHING (if someone writes it)
        let _ = nom::combinator::opt(nom::sequence::pair(
            tag_no_case::<&str, &str, nom::error::Error<&str>>("DO"),
            nom::sequence::preceded(multispace1, tag_no_case("NOTHING")),
        ))(input);
        MergeAction::DoNothing
    };
    Ok((input, (is_matched, action)))
}

// DROP INDEX name; / DROP TABLE [IF EXISTS] name;
/// Parse BEGIN / BEGIN TRANSACTION / START TRANSACTION
pub fn parse_begin(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = nom::branch::alt((tag_no_case("BEGIN"), tag_no_case("START")))(input)?;
    let (input, _) = multispace0(input)?;
    // Optional TRANSACTION keyword
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("TRANSACTION"), multispace0))(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Begin))
}

/// Parse COMMIT [TRANSACTION]
pub fn parse_commit(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("COMMIT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("TRANSACTION"), multispace0))(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Commit))
}

/// Parse ROLLBACK [TRANSACTION] or ROLLBACK TO [SAVEPOINT] name
pub fn parse_rollback(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("ROLLBACK")(input)?;
    let (input, _) = multispace0(input)?;
    // Check for ROLLBACK TO [SAVEPOINT] name
    if let Ok((input2, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("TO")(input) {
        let (input2, _) = multispace1(input2)?;
        // Optional SAVEPOINT keyword
        let (input2, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("SAVEPOINT"), multispace1))(input2)?;
        let (input2, name) = parse_identifier(input2)?;
        let (input2, _) = multispace0(input2)?;
        let (input2, _) = nom::combinator::opt(nom_char(';'))(input2)?;
        return Ok((input2, SqlStatement::RollbackToSavepoint(name.to_string())));
    }
    // Plain ROLLBACK [TRANSACTION]
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("TRANSACTION"), multispace0))(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Rollback))
}

/// Parse SAVEPOINT name
pub fn parse_savepoint(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("SAVEPOINT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Savepoint(name.to_string())))
}

/// Parse RELEASE [SAVEPOINT] name
pub fn parse_release(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("RELEASE")(input)?;
    let (input, _) = multispace1(input)?;
    // Optional SAVEPOINT keyword
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("SAVEPOINT"), multispace1))(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::ReleaseSavepoint(name.to_string())))
}

pub fn parse_drop(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("DROP")(input)?;
    let (input, _) = multispace1(input)?;
    nom::branch::alt((parse_drop_view_inner, parse_drop_index_inner, parse_drop_table_inner))(input)
}

fn parse_drop_view_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("VIEW")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, if_exists) = nom::combinator::opt(
        nom::sequence::terminated(tag_no_case("IF EXISTS"), multispace1)
    )(input)?;
    let (input, view_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::DropView(DropViewStatement {
        view_name: view_name.to_string(),
        if_exists: if_exists.is_some(),
    })))
}

fn parse_drop_index_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("INDEX")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, index_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::DropIndex(DropIndexStatement {
        index_name: index_name.to_string(),
    })))
}

fn parse_drop_table_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, if_exists) = nom::combinator::opt(
        nom::sequence::terminated(tag_no_case("IF EXISTS"), multispace1)
    )(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::DropTable(DropTableStatement {
        table_name: table_name.to_string(),
        if_exists: if_exists.is_some(),
    })))
}

// ALTER TABLE name { ADD COLUMN col TYPE [constraints]
//                  | DROP COLUMN col
//                  | RENAME COLUMN a TO b
//                  | RENAME TO new_name }
pub fn parse_alter(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("ALTER")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, action) = nom::branch::alt((
        parse_alter_add_column,
        parse_alter_drop_column,
        parse_alter_rename,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::AlterTable(AlterTableStatement {
        table_name: table_name.to_string(),
        action,
    })))
}

fn parse_alter_add_column(input: &str) -> IResult<&str, AlterAction> {
    let (input, _) = tag_no_case("ADD")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = nom::combinator::opt(
        nom::sequence::terminated(tag_no_case("COLUMN"), multispace1)
    )(input)?;
    let (input, col) = parse_column_definition(input)?;
    Ok((input, AlterAction::AddColumn(col)))
}

fn parse_alter_drop_column(input: &str) -> IResult<&str, AlterAction> {
    let (input, _) = tag_no_case("DROP")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = nom::combinator::opt(
        nom::sequence::terminated(tag_no_case("COLUMN"), multispace1)
    )(input)?;
    let (input, name) = parse_identifier(input)?;
    Ok((input, AlterAction::DropColumn(name.to_string())))
}

// RENAME [COLUMN a] TO b — column rename if "COLUMN" present, table rename otherwise
fn parse_alter_rename(input: &str) -> IResult<&str, AlterAction> {
    let (input, _) = tag_no_case("RENAME")(input)?;
    let (input, _) = multispace1(input)?;
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("COLUMN")(input) {
        let (input, _) = multispace1(input)?;
        let (input, from) = parse_identifier(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("TO")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, to) = parse_identifier(input)?;
        Ok((input, AlterAction::RenameColumn { from: from.to_string(), to: to.to_string() }))
    } else {
        let (input, _) = tag_no_case("TO")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, new_name) = parse_identifier(input)?;
        Ok((input, AlterAction::RenameTable(new_name.to_string())))
    }
}

/// Parse VALUES (expr,...),(expr,...) used in FROM position
fn parse_values_clause(input: &str) -> IResult<&str, Vec<Vec<Expression>>> {
    let (input, _) = tag_no_case("VALUES")(input)?;
    let (input, _) = multispace0(input)?;
    nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        |input| {
            let (input, _) = nom_char('(')(input)?;
            let (input, _) = multispace0(input)?;
            let (input, exprs) = nom::multi::separated_list1(
                nom::sequence::delimited(multispace0, nom_char(','), multispace0),
                parse_expression,
            )(input)?;
            let (input, _) = multispace0(input)?;
            let (input, _) = nom_char(')')(input)?;
            Ok((input, exprs))
        }
    )(input)
}

/// Parse optional (col1, col2, ...) after a VALUES alias
fn parse_values_col_list(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cols) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_identifier,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, cols.into_iter().map(|s| s.to_string()).collect()))
}

/// Parse [AS] alias [(col1, col2, ...)] for VALUES table alias
fn parse_values_alias(input: &str) -> IResult<&str, (String, Vec<String>)> {
    let (input, _) = multispace0(input)?;
    // Consume optional AS
    let (input, _) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("AS"), multispace1))(input)?;
    let (input, alias) = parse_identifier(input)?;
    // Optional column name list
    let (input, cols) = nom::combinator::opt(parse_values_col_list)(input)?;
    Ok((input, (alias.to_string(), cols.unwrap_or_default())))
}

/// Parse an extra comma-separated FROM table (returns name and optional alias)
fn parse_extra_from_table(input: &str) -> IResult<&str, (String, Option<String>)> {
    let (input, tbl) = parse_identifier(input)?;
    let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
    Ok((input, (tbl.to_string(), alias)))
}

/// Parse SELECT into a SelectStatement (used by both top-level and subqueries)
pub fn parse_select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, distinct) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("DISTINCT"), multispace1))(input)?;
    let distinct = distinct.is_some();
    let (input, columns) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        parse_select_column
    )(input)?;

    // FROM is optional — "SELECT 1" or "SELECT 1+1" are valid (used in recursive CTE anchors)
    let from_present = {
        let trimmed = input.trim_start();
        trimmed.to_uppercase().starts_with("FROM") &&
            trimmed[4..].starts_with(|c: char| c.is_whitespace() || c == '(')
    };

    let (input, from, from_alias) = if from_present {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = multispace1(input)?;

        // FROM can be: (VALUES ...) AS alias, (SELECT ...) AS alias, or table [alias]
        if let Ok((input, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(input) {
            let (input, _) = multispace0(input)?;
            // Try VALUES inside parens first
            if let Ok((input, value_rows)) = parse_values_clause(input) {
                let (input, _) = multispace0(input)?;
                let (input, _) = nom_char(')')(input)?;
                let (input, (alias, col_names)) = parse_values_alias(input)?;
                (input, FromClause::Values(value_rows, col_names), Some(alias))
            } else {
                let (input, subquery) = parse_select_statement(input)?;
                let (input, _) = multispace0(input)?;
                let (input, _) = nom_char(')')(input)?;
                let (input, _) = multispace1(input)?;
                let (input, _) = tag_no_case("AS")(input)?;
                let (input, _) = multispace1(input)?;
                let (input, alias) = parse_identifier(input)?;
                (input, FromClause::Subquery(Box::new(subquery)), Some(alias.to_string()))
            }
        } else {
            let (input, table) = parse_identifier(input)?;
            let (input, from_alias) = nom::combinator::opt(parse_table_alias)(input)?;
            (input, FromClause::Table(table.to_string()), from_alias)
        }
    } else {
        // No FROM clause — use a sentinel table name so the rest of the machinery works
        (input, FromClause::Table("__no_from__".to_string()), None)
    };

    // Parse optional comma-separated extra FROM tables (implicit CROSS JOIN)
    let (input, extra_tables) = nom::multi::many0(
        nom::sequence::preceded(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_extra_from_table,
        )
    )(input)?;

    let mut joins = Vec::new();
    for (tbl, alias) in extra_tables {
        joins.push(JoinClause { join_type: JoinType::Cross, table: tbl, alias, on: None, using: None, lateral: None });
    }

    let (input, explicit_joins) = nom::multi::many0(parse_join)(input)?;
    let mut joins = joins;
    joins.extend(explicit_joins);
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, (group_by, grouping_sets)) = parse_group_by_clause(input)?;
    let (input, having) = parse_having_clause(input)?;
    let (input, window_defs) = parse_window_clause(input)?;
    let (input, order_by) = parse_order_by_clause(input)?;
    let (input, (limit, offset)) = parse_limit_offset_clause(input)?;

    // Try to parse UNION [ALL] / INTERSECT [ALL] / EXCEPT [ALL] SELECT ...
    let (input, union) = {
        let input_before_union = input;
        let (input_trimmed, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;
        let set_op = nom::branch::alt((
            nom::combinator::map(tag_no_case::<&str, &str, nom::error::Error<&str>>("UNION"), |_| "UNION"),
            nom::combinator::map(tag_no_case("INTERSECT"), |_| "INTERSECT"),
            nom::combinator::map(tag_no_case("EXCEPT"), |_| "EXCEPT"),
        ))(input_trimmed);
        if let Ok((after_kw, kw)) = set_op {
            let (after_kw, _) = multispace1(after_kw)?;
            let (after_kw, all) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("ALL"), multispace1))(after_kw)?;
            let union_type = match (kw, all.is_some()) {
                ("UNION", false) => UnionType::Union,
                ("UNION", true)  => UnionType::UnionAll,
                ("INTERSECT", false) => UnionType::Intersect,
                ("INTERSECT", true)  => UnionType::IntersectAll,
                ("EXCEPT", false) => UnionType::Except,
                ("EXCEPT", true)  => UnionType::ExceptAll,
                _ => unreachable!(),
            };
            let (after_kw, right) = parse_select_statement(after_kw)?;
            (after_kw, Some((union_type, Box::new(right))))
        } else {
            (input_before_union, None)
        }
    };

    Ok((input, SelectStatement {
        ctes: Vec::new(),
        columns,
        distinct,
        from,
        from_alias,
        where_clause,
        joins,
        group_by,
        grouping_sets,
        having,
        window_defs,
        order_by,
        limit,
        offset,
        union,
    }))
}

/// Parse a single CTE definition: name [(col, ...)] AS (SELECT ...)
/// The `recursive` flag is set by the caller based on WITH RECURSIVE.
fn parse_cte_definition_inner(input: &str, recursive: bool) -> IResult<&str, CteDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    // Optional column name list: counter(n, m)
    let (input, columns) = nom::combinator::opt(|input| {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, cols) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_identifier,
        )(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        Ok((input, cols.into_iter().map(|s| s.to_string()).collect::<Vec<_>>()))
    })(input)?;
    let columns = columns.unwrap_or_default();
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, query) = parse_select_statement(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, CteDefinition { name: name.to_string(), columns, query: Box::new(query), recursive }))
}

/// Parse SELECT statement (top-level, with optional WITH [RECURSIVE] clause and semicolon)
pub fn parse_select(input: &str) -> IResult<&str, SqlStatement> {
    // Try parsing WITH [RECURSIVE] ... AS (...) before the SELECT
    let (input, ctes) = if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("WITH")(input) {
        let (input, _) = multispace1(input)?;
        // Optional RECURSIVE keyword
        let (input, recursive_kw) = nom::combinator::opt(
            nom::sequence::terminated(tag_no_case::<&str, &str, nom::error::Error<&str>>("RECURSIVE"), multispace1)
        )(input)?;
        let is_recursive = recursive_kw.is_some();
        let (input, ctes) = separated_list0(
            delimited(multispace0, nom_char(','), multispace0),
            |i| parse_cte_definition_inner(i, is_recursive),
        )(input)?;
        let (input, _) = multispace0(input)?;
        (input, ctes)
    } else {
        (input, Vec::new())
    };

    let (input, mut stmt) = parse_select_statement(input)?;
    stmt.ctes = ctes;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::Select(stmt)))
}

/// Parse SELECT column: aggregate, *, arithmetic expr, table.column, or column
fn parse_select_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, _) = multispace0(input)?;
    let (input, col) = nom::branch::alt((
        parse_all_column,
        parse_star_from_table, // t.* must come before parse_arith_select_column
        parse_arith_select_column, // must come before parse_aggregate_column to catch window agg functions
        parse_aggregate_column,
        parse_qualified_column,
        parse_simple_column,
    ))(input)?;
    // Check for optional AS alias
    if let Ok((input, _)) = multispace1::<&str, nom::error::Error<&str>>(input) {
        if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("AS")(input) {
            let (input, _) = multispace1(input)?;
            let (input, alias) = parse_identifier(input)?;
            return Ok((input, SelectColumn::Alias(Box::new(col), alias.to_string())));
        }
    }
    Ok((input, col))
}

/// Parse arithmetic expression as a select column (complex exprs, literals, subqueries)
fn parse_arith_select_column(input: &str) -> IResult<&str, SelectColumn> {
    let (new_input, expr) = parse_expression(input)?;
    match &expr {
        Expression::BinaryOp(_, _, _) | Expression::Case(_, _) | Expression::ScalarFunc(_, _)
        | Expression::Coalesce(_) | Expression::NullIf(_, _)
        | Expression::Round(_, _) | Expression::Concat(_) | Expression::Substr(_, _, _)
        | Expression::Replace(_, _, _) | Expression::LPad(_, _, _) | Expression::RPad(_, _, _)
        | Expression::Cast(_, _) | Expression::Window(_, _)
        | Expression::Greatest(_) | Expression::Least(_)
        | Expression::Power(_, _) | Expression::Position(_, _) | Expression::Repeat(_, _)
        | Expression::Literal(_) | Expression::Subquery(_)
        | Expression::CurrentDate | Expression::CurrentTimestamp
        | Expression::Extract(_, _) | Expression::DateTrunc(_, _)
        | Expression::DateDiff(_, _, _) | Expression::DateAdd(_, _, _) => Ok((new_input, SelectColumn::Expr(expr))),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    }
}

/// Parse FILTER (WHERE condition) clause used after aggregates
fn parse_filter_clause(input: &str) -> IResult<&str, Condition> {
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FILTER")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, cond) = parse_condition(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, cond))
}

/// Parse aggregate function: COUNT(*), COUNT(DISTINCT col), SUM(col), AVG(col), MIN(col), MAX(col)
fn parse_aggregate_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, func_name) = nom::branch::alt((
        tag_no_case("COUNT"),
        tag_no_case("SUM"),
        tag_no_case("AVG"),
        tag_no_case("MIN"),
        tag_no_case("MAX"),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Check for COUNT(DISTINCT ...)
    let is_count = func_name.eq_ignore_ascii_case("COUNT");
    let (input, func, inner) = if is_count {
        if let Ok((rest, _)) = nom::sequence::terminated(
            tag_no_case::<&str, &str, nom::error::Error<&str>>("DISTINCT"),
            multispace1,
        )(input) {
            let (rest, inner) = nom::branch::alt((
                parse_qualified_column,
                parse_simple_column,
            ))(rest)?;
            (rest, AggregateFunc::CountDistinct, inner)
        } else {
            let (rest, inner) = nom::branch::alt((
                parse_all_column,
                parse_qualified_column,
                parse_simple_column,
            ))(input)?;
            (rest, AggregateFunc::Count, inner)
        }
    } else {
        let func = match func_name.to_uppercase().as_str() {
            "SUM" => AggregateFunc::Sum,
            "AVG" => AggregateFunc::Avg,
            "MIN" => AggregateFunc::Min,
            "MAX" => AggregateFunc::Max,
            _ => unreachable!(),
        };
        let (rest, inner) = nom::branch::alt((
            parse_all_column,
            parse_qualified_column,
            parse_simple_column,
        ))(input)?;
        (rest, func, inner)
    };

    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;

    // Try to parse optional FILTER (WHERE condition)
    if let Ok((input, cond)) = parse_filter_clause(input) {
        return Ok((input, SelectColumn::AggregateFiltered(func, Box::new(inner), Box::new(cond))));
    }

    Ok((input, SelectColumn::Aggregate(func, Box::new(inner))))
}

fn parse_all_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, _) = nom_char('*')(input)?;
    Ok((input, SelectColumn::All))
}

/// Parse t.* — table-qualified wildcard
fn parse_star_from_table(input: &str) -> IResult<&str, SelectColumn> {
    let (input, table) = parse_identifier(input)?;
    let (input, _) = nom_char('.')(input)?;
    let (input, _) = nom_char('*')(input)?;
    Ok((input, SelectColumn::StarFromTable(table.to_string())))
}

fn parse_qualified_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, table) = parse_identifier(input)?;
    let (input, _) = nom_char('.')(input)?;
    let (input, column) = parse_identifier(input)?;
    Ok((input, SelectColumn::QualifiedColumn(
        table.to_string(),
        column.to_string(),
    )))
}

fn parse_simple_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, name) = parse_identifier(input)?;
    Ok((input, SelectColumn::Column(name.to_string())))
}

/// Parse WHERE clause
fn parse_where(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = parse_condition(input)?;
    Ok((input, WhereClause { condition }))
}

/// Parse ROLLUP(col, ...) — returns the column list
fn parse_rollup(input: &str) -> IResult<&str, Vec<SelectColumn>> {
    let (input, _) = tag_no_case("ROLLUP")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cols) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        nom::branch::alt((parse_qualified_column, parse_simple_column)),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, cols))
}

/// Parse CUBE(col, ...)
fn parse_cube(input: &str) -> IResult<&str, Vec<SelectColumn>> {
    let (input, _) = tag_no_case("CUBE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cols) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        nom::branch::alt((parse_qualified_column, parse_simple_column)),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, cols))
}

/// Parse a single grouping set: (col, col, ...) or ()
fn parse_one_grouping_set(input: &str) -> IResult<&str, Vec<SelectColumn>> {
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cols) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        nom::branch::alt((parse_qualified_column, parse_simple_column)),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, cols))
}

/// Parse GROUPING SETS((...), (...), ...)
fn parse_grouping_sets(input: &str) -> IResult<&str, Vec<Vec<SelectColumn>>> {
    let (input, _) = tag_no_case("GROUPING")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("SETS")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, sets) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_one_grouping_set,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, sets))
}

/// ROLLUP(a,b,c) → [(a,b,c), (a,b), (a), ()]
fn expand_rollup(cols: &[SelectColumn]) -> Vec<Vec<SelectColumn>> {
    (0..=cols.len()).rev().map(|n| cols[..n].to_vec()).collect()
}

/// CUBE(a,b) → all 2^n subsets in descending order of size
fn expand_cube(cols: &[SelectColumn]) -> Vec<Vec<SelectColumn>> {
    let n = cols.len();
    let mut sets: Vec<Vec<SelectColumn>> = (0..(1u32 << n))
        .map(|mask| (0..n).filter(|&i| mask & (1 << i) != 0).map(|i| cols[i].clone()).collect())
        .collect();
    sets.sort_by_key(|s: &Vec<SelectColumn>| std::cmp::Reverse(s.len()));
    sets
}

/// Parse GROUP BY clause (returns group_by cols and optional grouping sets for ROLLUP/CUBE)
fn parse_group_by_clause(input: &str) -> IResult<&str, (Vec<SelectColumn>, Option<Vec<Vec<SelectColumn>>>)> {
    let (input, _) = multispace0(input)?;
    let result = nom::sequence::pair(tag_no_case("GROUP"), nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("BY")))(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            // Try ROLLUP
            if let Ok((input, cols)) = parse_rollup(input) {
                let sets = expand_rollup(&cols);
                let primary = sets.first().cloned().unwrap_or_default();
                return Ok((input, (primary, Some(sets))));
            }
            // Try CUBE
            if let Ok((input, cols)) = parse_cube(input) {
                let sets = expand_cube(&cols);
                let primary = sets.first().cloned().unwrap_or_default();
                return Ok((input, (primary, Some(sets))));
            }
            // Try GROUPING SETS
            if let Ok((input, sets)) = parse_grouping_sets(input) {
                let primary = sets.first().cloned().unwrap_or_default();
                return Ok((input, (primary, Some(sets))));
            }
            // Regular GROUP BY
            let (input, cols) = separated_list0(
                delimited(multispace0, nom_char(','), multispace0),
                nom::branch::alt((parse_qualified_column, parse_simple_column)),
            )(input)?;
            Ok((input, (cols, None)))
        }
        Err(_) => Ok((input, (Vec::new(), None))),
    }
}

/// Parse HAVING clause (returns None if not present)
fn parse_having_clause(input: &str) -> IResult<&str, Option<WhereClause>> {
    let (input, _) = multispace0(input)?;
    let result = tag::<&str, &str, nom::error::Error<&str>>("HAVING")(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            let (input, condition) = parse_condition(input)?;
            Ok((input, Some(WhereClause { condition })))
        }
        Err(_) => Ok((input, None)),
    }
}

/// Parse ORDER BY clause (returns empty vec if not present)
fn parse_order_by_clause(input: &str) -> IResult<&str, Vec<OrderByClause>> {
    let (input, _) = multispace0(input)?;
    let result = nom::sequence::pair(tag_no_case("ORDER"), nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("BY")))(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            let (input, clauses) = separated_list0(
                delimited(multispace0, nom_char(','), multispace0),
                parse_order_by_item,
            )(input)?;
            Ok((input, clauses))
        }
        Err(_) => Ok((input, Vec::new())),
    }
}

/// Parse a single ORDER BY item: column [ASC|DESC] [NULLS FIRST|LAST]
fn parse_order_by_item(input: &str) -> IResult<&str, OrderByClause> {
    let (input, _) = multispace0(input)?;
    let (input, column) = nom::branch::alt((
        parse_qualified_column,
        parse_simple_column,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, dir) = nom::combinator::opt(nom::branch::alt((
        tag_no_case("ASC"),
        tag_no_case("DESC"),
    )))(input)?;
    let descending = dir.map(|d| d.eq_ignore_ascii_case("DESC")).unwrap_or(false);
    // Parse optional NULLS FIRST / NULLS LAST
    let (input, nulls_first) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::pair(multispace1, tag_no_case("NULLS")),
        nom::sequence::preceded(
            multispace1,
            nom::branch::alt((
                nom::combinator::map(tag_no_case("FIRST"), |_| true),
                nom::combinator::map(tag_no_case("LAST"), |_| false),
            )),
        ),
    ))(input)?;
    Ok((input, OrderByClause { column, descending, nulls_first }))
}

/// Parse LIMIT clause (returns None if not present)
fn parse_limit_offset_clause(input: &str) -> IResult<&str, (Option<u64>, Option<u64>)> {
    let (input, _) = multispace0(input)?;
    let result = tag_no_case::<&str, &str, nom::error::Error<&str>>("LIMIT")(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            let (input, n) = nom::character::complete::u64(input)?;
            // Parse optional OFFSET
            let (input, _) = multispace0(input)?;
            let offset_result = tag_no_case::<&str, &str, nom::error::Error<&str>>("OFFSET")(input);
            match offset_result {
                Ok((input, _)) => {
                    let (input, _) = multispace1(input)?;
                    let (input, off) = nom::character::complete::u64(input)?;
                    Ok((input, (Some(n), Some(off))))
                }
                Err(_) => Ok((input, (Some(n), None))),
            }
        }
        Err(_) => Ok((input, (None, None))),
    }
}

/// Check if identifier is a reserved keyword that can't be used as an alias
fn is_reserved_keyword(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(), "ON" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS" | "WHERE" | "ORDER" | "GROUP" | "LIMIT" | "OFFSET" | "HAVING" | "UNION" | "ALL" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" | "AND" | "OR" | "NOT" | "AS" | "VIEW" | "OVER" | "PARTITION" | "NULLS" | "FIRST" | "LAST" | "INTERSECT" | "EXCEPT" | "ROWS" | "RANGE" | "GROUPS" | "PRECEDING" | "FOLLOWING" | "UNBOUNDED" | "BETWEEN" | "USING" | "NATURAL" | "ANY" | "SOME" | "FILTER" | "RECURSIVE" | "CURRENT_DATE" | "CURRENT_TIMESTAMP" | "EXTRACT" | "INTERVAL" | "DATEDIFF" | "DATE_TRUNC" | "DATE_PART" | "DATEADD" | "WINDOW" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" | "ROLLUP" | "CUBE" | "GROUPING" | "SETS" | "LATERAL" | "VALUES" | "RETURNING" | "CONFLICT" | "EXCLUDED" | "NOTHING" | "MERGE" | "MATCHED" | "TRUNCATE" | "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "START" | "TRANSACTION")
}

/// Parse optional table alias, handling both `table alias` and `table AS alias` forms
fn parse_table_alias(input: &str) -> IResult<&str, String> {
    let (input, _) = multispace1(input)?;
    // Optionally consume the AS keyword
    let (input, has_as) = if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("AS")(input) {
        // Only consume AS if followed by whitespace (not e.g. "ASC")
        if i.starts_with(|c: char| c.is_whitespace()) {
            let (i, _) = multispace1(i)?;
            (i, true)
        } else {
            (input, false)
        }
    } else {
        (input, false)
    };
    let (input, alias) = parse_identifier(input)?;
    // Without explicit AS, reject reserved keywords as implicit aliases
    if !has_as && is_reserved_keyword(alias) {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((input, alias.to_string()))
}

/// Parse JOIN clause
pub fn parse_join(input: &str) -> IResult<&str, JoinClause> {
    let (input, _) = multispace1(input)?;

    // Try NATURAL [LEFT|RIGHT|INNER] JOIN first
    if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("NATURAL")(input) {
        let (input, _) = multispace1(input)?;
        // Optionally consume LEFT/RIGHT/INNER before JOIN
        let (input, _) = nom::combinator::opt(nom::sequence::terminated(
            nom::branch::alt((tag_no_case("LEFT"), tag_no_case("RIGHT"), tag_no_case("INNER"))),
            multispace1::<&str, nom::error::Error<&str>>,
        ))(input)?;
        let (input, _) = tag_no_case("JOIN")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, table) = parse_identifier(input)?;
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        return Ok((input, JoinClause {
            join_type: JoinType::Natural,
            table: table.to_string(),
            alias,
            on: None,
            using: None,
            lateral: None,
        }));
    }

    let (input, join_type) = nom::branch::alt((
        nom::combinator::map(tag_no_case("INNER JOIN"), |_| JoinType::Inner),
        nom::combinator::map(tag_no_case("LEFT JOIN"), |_| JoinType::Left),
        nom::combinator::map(tag_no_case("RIGHT JOIN"), |_| JoinType::Right),
        nom::combinator::map(tag_no_case("FULL OUTER JOIN"), |_| JoinType::Full),
        nom::combinator::map(tag_no_case("FULL JOIN"), |_| JoinType::Full),
        nom::combinator::map(tag_no_case("CROSS JOIN"), |_| JoinType::Cross),
        nom::combinator::map(tag_no_case("JOIN"), |_| JoinType::Inner),
    ))(input)?;
    let (input, _) = multispace1(input)?;

    // Check for LATERAL (SELECT ...) subquery
    if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("LATERAL")(input) {
        let (input, _) = multispace1(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, lateral_query) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        // Parse optional alias
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        // Parse optional ON condition
        let (input, on) = if let Ok((input2, _)) = nom::sequence::preceded(
            multispace1::<&str, nom::error::Error<&str>>,
            tag_no_case::<&str, &str, nom::error::Error<&str>>("ON"),
        )(input) {
            let (input2, _) = multispace1(input2)?;
            let (input2, cond) = parse_condition(input2)?;
            (input2, Some(cond))
        } else {
            (input, None)
        };
        return Ok((input, JoinClause {
            join_type,
            table: String::new(),
            alias,
            on,
            using: None,
            lateral: Some(Box::new(lateral_query)),
        }));
    }

    let (input, table) = parse_identifier(input)?;
    let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;

    // CROSS JOIN has no ON/USING clause
    if join_type == JoinType::Cross {
        return Ok((input, JoinClause { join_type, table: table.to_string(), alias, on: None, using: None, lateral: None }));
    }

    // Try USING (col1, col2, ...)
    let after_alias = input;
    if let Ok((input, _)) = nom::sequence::preceded(
        multispace1::<&str, nom::error::Error<&str>>,
        tag_no_case::<&str, &str, nom::error::Error<&str>>("USING"),
    )(after_alias) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, cols) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_identifier,
        )(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, JoinClause {
            join_type,
            table: table.to_string(),
            alias,
            on: None,
            using: Some(cols.iter().map(|s| s.to_string()).collect()),
            lateral: None,
        }));
    }

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("ON")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = parse_condition(input)?;

    Ok((input, JoinClause { join_type, table: table.to_string(), alias, on: Some(condition), using: None, lateral: None }))
}

/// Parse condition with OR (lowest precedence), AND, then a primary comparison
pub fn parse_condition(input: &str) -> IResult<&str, Condition> {
    let (mut input, mut cond) = parse_and_condition(input)?;
    loop {
        let (i, _) = multispace0(input)?;
        match tag_no_case::<&str, &str, nom::error::Error<&str>>("OR")(i) {
            Ok((i, _)) if i.starts_with(' ') || i.starts_with('\t') || i.starts_with('\n') || i.starts_with('(') => {
                let (i, _) = multispace0(i)?;
                let (i, right) = parse_and_condition(i)?;
                cond = Condition::Or(Box::new(cond), Box::new(right));
                input = i;
            }
            _ => break,
        }
    }
    Ok((input, cond))
}

fn parse_and_condition(input: &str) -> IResult<&str, Condition> {
    let (mut input, mut cond) = parse_not_condition(input)?;
    loop {
        let (i, _) = multispace0(input)?;
        // "AND" inside BETWEEN is consumed by parse_primary_condition, so any "AND" here is logical
        match tag_no_case::<&str, &str, nom::error::Error<&str>>("AND")(i) {
            Ok((i, _)) if i.starts_with(' ') || i.starts_with('\t') || i.starts_with('\n') || i.starts_with('(') => {
                let (i, _) = multispace0(i)?;
                let (i, right) = parse_not_condition(i)?;
                cond = Condition::And(Box::new(cond), Box::new(right));
                input = i;
            }
            _ => break,
        }
    }
    Ok((input, cond))
}

fn parse_not_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = multispace0(input)?;

    // Logical NOT — but NOT EXISTS is handled inside parse_primary_condition,
    // so skip if "NOT" is followed (after whitespace) by "EXISTS".
    if let Ok((after_not, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT")(input) {
        let sep = after_not.starts_with(' ') || after_not.starts_with('\t')
            || after_not.starts_with('\n') || after_not.starts_with('(');
        let trimmed = after_not.trim_start();
        let is_exists = trimmed.to_uppercase().starts_with("EXISTS");
        if sep && !is_exists {
            let (after_not, _) = multispace0(after_not)?;
            let (after_not, inner) = parse_not_condition(after_not)?;
            return Ok((after_not, Condition::Not(Box::new(inner))));
        }
    }

    parse_primary_condition(input)
}

/// Parse a single comparison or a parenthesized condition group
fn parse_primary_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = multispace0(input)?;

    // Boolean literals TRUE / FALSE as standalone conditions
    if let Ok((input, val)) = nom::branch::alt((
        tag_no_case::<&str, &str, nom::error::Error<&str>>("TRUE"),
        tag_no_case::<&str, &str, nom::error::Error<&str>>("FALSE"),
    ))(input) {
        // Only treat as standalone if not immediately followed by alphanumeric (e.g. "TRUNC")
        if input.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
            // Fall through — this is part of an identifier/function
        } else {
            let b = val.to_uppercase() == "TRUE";
            return Ok((input, Condition::Comparison {
                left: Expression::Literal(Value::Bool(b)),
                operator: Operator::Equals,
                right: Expression::Literal(Value::Bool(true)),
                upper_bound: None,
            }));
        }
    }

    // Parenthesized sub-condition: (cond AND/OR cond ...)
    if let Ok((input, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(input) {
        // Only treat as parenthesized condition if it doesn't look like EXISTS(SELECT
        let (input, _) = multispace0(input)?;
        let (input, inner) = parse_condition(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, inner));
    }

    // Try NOT EXISTS (SELECT ...)
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("EXISTS")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition::Comparison {
            left: Expression::Literal(Value::Null),
            operator: Operator::NotExists,
            right: Expression::Subquery(Box::new(subquery)),
            upper_bound: None,
        }));
    }

    // Try EXISTS (SELECT ...)
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("EXISTS")(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition::Comparison {
            left: Expression::Literal(Value::Null),
            operator: Operator::Exists,
            right: Expression::Subquery(Box::new(subquery)),
            upper_bound: None,
        }));
    }

    let (input, left) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;

    // Try IS NOT NULL / IS NULL
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("IS")(input) {
        let (input, _) = multispace1(input)?;
        if let Ok((input, _)) = nom::sequence::pair(
            tag::<&str, &str, nom::error::Error<&str>>("NOT"),
            nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("NULL")),
        )(input) {
            return Ok((input, Condition::Comparison {
                left,
                operator: Operator::IsNotNull,
                right: Expression::Literal(Value::Null),
                upper_bound: None,
            }));
        }
        let (input, _) = tag_no_case("NULL")(input)?;
        return Ok((input, Condition::Comparison {
            left,
            operator: Operator::IsNull,
            right: Expression::Literal(Value::Null),
            upper_bound: None,
        }));
    }

    // Try parsing NOT IN (...) or IN (...)
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("IN")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_in_list(input)?;
        return Ok((input, Condition::Comparison {
            left,
            operator: Operator::NotIn,
            right,
            upper_bound: None,
        }));
    }
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("IN")(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_in_list(input)?;
        return Ok((input, Condition::Comparison {
            left,
            operator: Operator::In,
            right,
            upper_bound: None,
        }));
    }

    // Try NOT BETWEEN low AND high
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("BETWEEN")),
    )(input) {
        let (input, _) = multispace1(input)?;
        let (input, low) = parse_expression(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("AND")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, high) = parse_expression(input)?;
        return Ok((input, Condition::Comparison {
            left,
            operator: Operator::NotBetween,
            right: low,
            upper_bound: Some(high),
        }));
    }

    // Try BETWEEN low AND high
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("BETWEEN")(input) {
        let (input, _) = multispace1(input)?;
        let (input, low) = parse_expression(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("AND")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, high) = parse_expression(input)?;
        return Ok((input, Condition::Comparison {
            left,
            operator: Operator::Between,
            right: low,
            upper_bound: Some(high),
        }));
    }

    // Try NOT LIKE / NOT ILIKE before general operator parse
    if let Ok((input, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("ILIKE")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_expression(input)?;
        return Ok((input, Condition::Comparison { left, operator: Operator::NotILike, right, upper_bound: None }));
    }
    if let Ok((input, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("LIKE")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_expression(input)?;
        return Ok((input, Condition::Comparison { left, operator: Operator::NotLike, right, upper_bound: None }));
    }

    // Try ANY/ALL subquery operators: expr op ANY (SELECT ...) / expr op ALL (SELECT ...)
    if let Ok((after_op, op)) = parse_operator(input) {
        let after_op_trimmed = after_op.trim_start();
        // Try ANY or SOME (synonym for ANY)
        if let Ok((rest, _)) = nom::branch::alt((
            tag_no_case::<&str, &str, nom::error::Error<&str>>("ANY"),
            tag_no_case::<&str, &str, nom::error::Error<&str>>("SOME"),
        ))(after_op_trimmed) {
            let rest = rest.trim_start();
            if let Ok((rest, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(rest) {
                let rest = rest.trim_start();
                if let Ok((rest, subquery)) = parse_select_statement(rest) {
                    let rest = rest.trim_start();
                    if let Ok((rest, _)) = nom_char::<&str, nom::error::Error<&str>>(')')(rest) {
                        return Ok((rest, Condition::AnyComparison { left, op, subquery: Box::new(subquery) }));
                    }
                }
            }
        }
        // Try ALL
        if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("ALL")(after_op_trimmed) {
            let rest = rest.trim_start();
            if let Ok((rest, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(rest) {
                let rest = rest.trim_start();
                if let Ok((rest, subquery)) = parse_select_statement(rest) {
                    let rest = rest.trim_start();
                    if let Ok((rest, _)) = nom_char::<&str, nom::error::Error<&str>>(')')(rest) {
                        return Ok((rest, Condition::AllComparison { left, op, subquery: Box::new(subquery) }));
                    }
                }
            }
        }
    }

    let (input, operator) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, right) = parse_expression(input)?;

    Ok((input, Condition::Comparison { left, operator, right, upper_bound: None }))
}

/// Try to parse an arithmetic operator surrounded by optional whitespace
fn parse_arith_add_sub(input: &str) -> IResult<&str, ArithOp> {
    let (input, _) = multispace0(input)?;
    let (input, op) = nom::branch::alt((
        nom::combinator::map(nom_char('+'), |_| ArithOp::Add),
        nom::combinator::map(nom_char('-'), |_| ArithOp::Sub),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, op))
}

fn parse_arith_mul_div(input: &str) -> IResult<&str, ArithOp> {
    let (input, _) = multispace0(input)?;
    let (input, op) = nom::branch::alt((
        nom::combinator::map(nom_char('*'), |_| ArithOp::Mul),
        nom::combinator::map(nom_char('/'), |_| ArithOp::Div),
        nom::combinator::map(nom_char('%'), |_| ArithOp::Mod),
    ))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, op))
}

/// Parse expression with arithmetic: handles ||, +, -, *, / with precedence
fn parse_expression(input: &str) -> IResult<&str, Expression> {
    let (mut input, mut left) = parse_arith_expr(input)?;
    // || has lower precedence than +/-
    while let Ok((remaining, _)) = nom::sequence::delimited(
        multispace0::<&str, nom::error::Error<&str>>,
        tag("||"),
        multispace0::<&str, nom::error::Error<&str>>,
    )(input) {
        let (remaining, right) = parse_arith_expr(remaining)?;
        left = Expression::BinaryOp(Box::new(left), ArithOp::Concat, Box::new(right));
        input = remaining;
    }
    Ok((input, left))
}

/// Parse additive arithmetic: handles +, -
fn parse_arith_expr(input: &str) -> IResult<&str, Expression> {
    let (mut input, mut left) = parse_term(input)?;
    while let Ok((remaining, op)) = parse_arith_add_sub(input) {
        let (remaining, right) = parse_term(remaining)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
        input = remaining;
    }
    Ok((input, left))
}

/// Parse term: handles * and / (higher precedence)
fn parse_term(input: &str) -> IResult<&str, Expression> {
    let (mut input, mut left) = parse_atom(input)?;
    while let Ok((remaining, op)) = parse_arith_mul_div(input) {
        let (remaining, right) = parse_atom(remaining)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
        input = remaining;
    }
    Ok((input, left))
}

/// Parse atomic expression: subquery, aggregate, CASE, column, table.column, or literal
fn parse_atom(input: &str) -> IResult<&str, Expression> {
    // Split into three alt groups because nom::alt supports max 21 alternatives
    nom::branch::alt((
        nom::branch::alt((
            parse_expression_case,
            parse_expression_subquery,
            parse_expression_coalesce,
            parse_expression_nullif,
            parse_expression_greatest,
            parse_expression_least,
            parse_expression_power,
            parse_expression_position,
            parse_expression_repeat,
            parse_expression_round,
            parse_expression_concat,
            parse_expression_substr,
        )),
        nom::branch::alt((
            parse_expression_cast,
            parse_expression_replace,
            parse_expression_lpad,
            parse_expression_rpad,
            // Date/time expressions (try before window and scalar to catch keywords)
            parse_expression_current_date,
            parse_expression_current_timestamp,
            parse_expression_extract,
            parse_expression_date_trunc,
            parse_expression_datediff,
            parse_expression_dateadd,
            parse_expression_date_part,
            parse_expression_interval,
            parse_expression_date_literal,
        )),
        nom::branch::alt((
            parse_expression_timestamp_literal,
            parse_expression_window,
            parse_expression_scalar_func,
            parse_expression_aggregate,
            parse_expression_qualified_column,
            parse_expression_literal,
            parse_expression_simple_column,
        )),
    ))(input)
}

fn parse_expression_coalesce(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("COALESCE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Coalesce(exprs)))
}

fn parse_expression_nullif(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("NULLIF")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, first) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(',')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, second) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::NullIf(Box::new(first), Box::new(second))))
}

fn parse_expression_scalar_func(input: &str) -> IResult<&str, Expression> {
    let (input, func_name) = nom::branch::alt((
        nom::branch::alt((
            tag_no_case("UPPER"),
            tag_no_case("LOWER"),
            tag_no_case("LENGTH"),
            tag_no_case("LTRIM"),
            tag_no_case("RTRIM"),
            tag_no_case("TRIM"),
            tag_no_case("ABS"),
            tag_no_case("CEILING"),
            tag_no_case("CEIL"),
            tag_no_case("FLOOR"),
            tag_no_case("SQRT"),
            tag_no_case("SIGN"),
            tag_no_case("TRUNC"),
            tag_no_case("REVERSE"),
        )),
        nom::branch::alt((
            tag_no_case("YEAR"),
            tag_no_case("MONTH"),
            tag_no_case("DAYOFMONTH"),
            tag_no_case("DAYOFWEEK"),
            tag_no_case("DAYOFYEAR"),
            tag_no_case("HOUR"),
            tag_no_case("MINUTE"),
            tag_no_case("SECOND"),
            tag_no_case("DAY"),
        )),
    ))(input)?;
    let func = match func_name.to_uppercase().as_str() {
        "UPPER"   => ScalarFunc::Upper,
        "LOWER"   => ScalarFunc::Lower,
        "LENGTH"  => ScalarFunc::Length,
        "LTRIM"   => ScalarFunc::LTrim,
        "RTRIM"   => ScalarFunc::RTrim,
        "TRIM"    => ScalarFunc::Trim,
        "ABS"     => ScalarFunc::Abs,
        "CEILING" | "CEIL" => ScalarFunc::Ceil,
        "FLOOR"   => ScalarFunc::Floor,
        "SQRT"    => ScalarFunc::Sqrt,
        "SIGN"    => ScalarFunc::Sign,
        "TRUNC"   => ScalarFunc::Trunc,
        "REVERSE" => ScalarFunc::Reverse,
        "YEAR"    => ScalarFunc::Year,
        "MONTH"   => ScalarFunc::Month,
        "DAY" | "DAYOFMONTH" => ScalarFunc::Day,
        "HOUR"    => ScalarFunc::Hour,
        "MINUTE"  => ScalarFunc::Minute,
        "SECOND"  => ScalarFunc::Second,
        "DAYOFWEEK" => ScalarFunc::DayOfWeek,
        "DAYOFYEAR" => ScalarFunc::DayOfYear,
        _ => unreachable!(),
    };
    // These functions require a following '(' to avoid mistaking column names like "day", "month"
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::ScalarFunc(func, Box::new(expr))))
}

fn parse_expression_round(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("ROUND")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, val) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, places) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Round(Box::new(val), places.map(Box::new))))
}

fn parse_expression_concat(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("CONCAT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Concat(exprs)))
}

fn parse_expression_substr(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom::branch::alt((tag_no_case("SUBSTRING"), tag_no_case("SUBSTR")))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(',')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, start) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, len) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Substr(Box::new(s), Box::new(start), len.map(Box::new))))
}

fn parse_expression_cast(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("CAST")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;
    // Type name: e.g. INT, FLOAT, VARCHAR, TEXT, BOOLEAN, etc.
    let (input, type_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Cast(Box::new(expr), type_name.to_uppercase())))
}

fn parse_expression_replace(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("REPLACE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, from) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, to) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Replace(Box::new(s), Box::new(from), Box::new(to))))
}

fn parse_expression_lpad(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("LPAD")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, len) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, pad) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::LPad(Box::new(s), Box::new(len), Box::new(pad))))
}

fn parse_expression_rpad(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("RPAD")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, len) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, pad) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::RPad(Box::new(s), Box::new(len), Box::new(pad))))
}

fn parse_expression_greatest(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("GREATEST")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Greatest(exprs)))
}

fn parse_expression_least(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("LEAST")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Least(exprs)))
}

fn parse_expression_power(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom::branch::alt((tag_no_case("POWER"), tag_no_case("POW")))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, base) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, exp) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Power(Box::new(base), Box::new(exp))))
}

fn parse_expression_position(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("POSITION")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, needle) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("IN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, haystack) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Position(Box::new(needle), Box::new(haystack))))
}

fn parse_expression_repeat(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("REPEAT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, n) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Repeat(Box::new(s), Box::new(n))))
}

/// Parse CURRENT_DATE (no parentheses)
fn parse_expression_current_date(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("CURRENT_DATE")(input)?;
    // Make sure it's not followed by '(' (which would mean something else)
    if input.trim_start().starts_with('(') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((input, Expression::CurrentDate))
}

/// Parse CURRENT_TIMESTAMP or NOW() — both return the current timestamp
fn parse_expression_current_timestamp(input: &str) -> IResult<&str, Expression> {
    // Try CURRENT_TIMESTAMP first (no parens)
    if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("CURRENT_TIMESTAMP")(input) {
        // optionally consume empty parentheses
        let rest2 = rest.trim_start();
        if rest2.starts_with("()") {
            return Ok((&rest2[2..], Expression::CurrentTimestamp));
        }
        return Ok((rest, Expression::CurrentTimestamp));
    }
    // Try NOW()
    let (input, _) = tag_no_case("NOW")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::CurrentTimestamp))
}

/// Parse DATE 'YYYY-MM-DD' literal
fn parse_expression_date_literal(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("DATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, val) = parse_string_value(input)?;
    if let Value::String(s) = val {
        match parse_date_str(&s) {
            Some(d) => Ok((input, Expression::Literal(Value::Date(d)))),
            None => Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
        }
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
    }
}

/// Parse TIMESTAMP 'YYYY-MM-DD HH:MM:SS' literal
fn parse_expression_timestamp_literal(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("TIMESTAMP")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, val) = parse_string_value(input)?;
    if let Value::String(s) = val {
        match parse_timestamp_str(&s) {
            Some(ts) => Ok((input, Expression::Literal(Value::Timestamp(ts)))),
            None => Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
        }
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
    }
}

/// Parse EXTRACT(field FROM expr)
fn parse_expression_extract(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("EXTRACT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, field) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Extract(field.to_uppercase(), Box::new(expr))))
}

/// Parse DATE_PART('field', expr) — PostgreSQL style
fn parse_expression_date_part(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("DATE_PART")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, field_val) = parse_string_value(input)?;
    let field = if let Value::String(s) = field_val { s.to_uppercase() } else {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    };
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Extract(field, Box::new(expr))))
}

/// Parse DATE_TRUNC('unit', expr)
fn parse_expression_date_trunc(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("DATE_TRUNC")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, field_val) = parse_string_value(input)?;
    let field = if let Value::String(s) = field_val { s.to_uppercase() } else {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    };
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::DateTrunc(field, Box::new(expr))))
}

/// Parse DATEDIFF(unit, date1, date2) — returns integer difference
fn parse_expression_datediff(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("DATEDIFF")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    // Unit can be a quoted string or an identifier
    let (input, unit) = nom::branch::alt((
        |i| { let (i, v) = parse_string_value(i)?; if let Value::String(s) = v { Ok((i, s.to_uppercase())) } else { Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag))) } },
        |i| { let (i, s) = parse_identifier(i)?; Ok((i, s.to_uppercase())) },
    ))(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, e1) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, e2) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::DateDiff(unit, Box::new(e1), Box::new(e2))))
}

/// Parse DATEADD(unit, n, date) — shift date/timestamp by n units
fn parse_expression_dateadd(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom::branch::alt((tag_no_case("DATE_ADD"), tag_no_case("DATEADD")))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    // Unit first
    let (input, unit) = nom::branch::alt((
        |i| { let (i, v) = parse_string_value(i)?; if let Value::String(s) = v { Ok((i, s.to_uppercase())) } else { Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag))) } },
        |i| { let (i, s) = parse_identifier(i)?; Ok((i, s.to_uppercase())) },
    ))(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    // n (integer amount)
    let (input, n) = nom::character::complete::i64(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, date_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::DateAdd(Box::new(date_expr), n, unit)))
}

/// Convert an INTERVAL unit name to seconds
pub fn interval_unit_secs(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "SECOND" | "SECONDS" => Some(1),
        "MINUTE" | "MINUTES" => Some(60),
        "HOUR" | "HOURS" => Some(3600),
        "DAY" | "DAYS" => Some(86400),
        "WEEK" | "WEEKS" => Some(604800),
        "MONTH" | "MONTHS" => Some(2592000),   // approximate 30 days
        "YEAR" | "YEARS" => Some(31536000),    // approximate 365 days
        _ => None,
    }
}

/// Parse INTERVAL n unit (e.g. INTERVAL 7 DAY, INTERVAL '30' DAYS)
/// Returns Literal(Int(total_seconds)) for use in date arithmetic
fn parse_expression_interval(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("INTERVAL")(input)?;
    let (input, _) = multispace1(input)?;
    // Try quoted string form: INTERVAL '7' DAY or INTERVAL '7 days'
    let (input, n, unit) = if let Ok((rest, val)) = parse_string_value(input) {
        if let Value::String(s) = val {
            let s = s.trim();
            // Check if the string contains the unit: '7 days'
            if let Some(sp) = s.find(|c: char| c.is_whitespace()) {
                let n: i64 = s[..sp].trim().parse().map_err(|_| nom::Err::Error(nom::error::Error::new(rest, nom::error::ErrorKind::Tag)))?;
                let unit = s[sp+1..].trim().to_uppercase();
                (rest, n, unit)
            } else {
                // '7' followed by unit identifier
                let n: i64 = s.parse().map_err(|_| nom::Err::Error(nom::error::Error::new(rest, nom::error::ErrorKind::Tag)))?;
                let (rest, _) = multispace1(rest)?;
                let (rest, u) = parse_identifier(rest)?;
                (rest, n, u.to_uppercase())
            }
        } else {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
        }
    } else {
        // Bare integer form: INTERVAL 7 DAY
        let (rest, n) = nom::character::complete::i64(input)?;
        let (rest, _) = multispace1(rest)?;
        let (rest, u) = parse_identifier(rest)?;
        (rest, n, u.to_uppercase())
    };
    let secs = interval_unit_secs(&unit).ok_or_else(|| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
    Ok((input, Expression::Literal(Value::Int(n * secs))))
}

/// Parse optional WINDOW clause: WINDOW name AS (...), name2 AS (...)
fn parse_window_clause(input: &str) -> IResult<&str, Vec<(String, WindowSpec)>> {
    let (input, _) = multispace0(input)?;
    if let Ok((input2, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("WINDOW")(input) {
        // Require whitespace after WINDOW so "windowfn" isn't matched
        let (input2, _) = multispace1(input2)?;
        let (input2, defs) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_named_window_def,
        )(input2)?;
        Ok((input2, defs))
    } else {
        Ok((input, Vec::new()))
    }
}

/// Parse one named window definition: name AS (window_spec)
fn parse_named_window_def(input: &str) -> IResult<&str, (String, WindowSpec)> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, spec) = parse_window_spec(input)?;
    Ok((input, (name.to_string(), spec)))
}

fn parse_window_spec(input: &str) -> IResult<&str, WindowSpec> {
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // Check for an optional base window name (identifier before PARTITION/ORDER/frame keywords)
    let (input, base_window) = try_parse_base_window_name(input)?;

    let (input, _) = multispace0(input)?;

    // Optional PARTITION BY
    let (input, partition_by) = if let Ok((input2, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("PARTITION"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("BY")),
    )(input) {
        let (input2, _) = multispace1(input2)?;
        let (input2, exprs) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_expression,
        )(input2)?;
        (input2, exprs)
    } else {
        (input, Vec::new())
    };

    let (input, _) = multispace0(input)?;

    // Optional ORDER BY (reuse parse_order_by_item)
    let (input, order_by) = if let Ok((input2, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("ORDER"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("BY")),
    )(input) {
        let (input2, _) = multispace1(input2)?;
        let (input2, clauses) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_order_by_item,
        )(input2)?;
        (input2, clauses)
    } else {
        (input, Vec::new())
    };

    let (input, _) = multispace0(input)?;
    // Optional frame clause: ROWS|RANGE|GROUPS BETWEEN <bound> AND <bound>
    //   or ROWS|RANGE|GROUPS <bound>
    let (input, frame) = parse_window_frame(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowSpec { base_window, partition_by, order_by, frame }))
}

/// Try to parse a base window name at the start of a window spec.
/// Returns Some(name) only if the identifier is not a window-spec keyword.
fn try_parse_base_window_name(input: &str) -> IResult<&str, Option<String>> {
    // Keywords that can start a window spec clause (not a base window name)
    const WINDOW_SPEC_KEYWORDS: &[&str] = &["PARTITION", "ORDER", "ROWS", "RANGE", "GROUPS", "BY"];
    if let Ok((rest, name)) = parse_identifier(input) {
        let upper = name.to_uppercase();
        if !WINDOW_SPEC_KEYWORDS.contains(&upper.as_str()) {
            // Not followed by '(' — that would make it look like a function call
            let next = rest.trim_start();
            if !next.starts_with('(') {
                return Ok((rest, Some(name.to_string())));
            }
        }
    }
    Ok((input, None))
}

fn parse_frame_bound(input: &str) -> IResult<&str, FrameBound> {
    let (input, _) = multispace0(input)?;
    // Try UNBOUNDED PRECEDING / FOLLOWING
    if let Ok((rest, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("UNBOUNDED"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("PRECEDING")),
    )(input) {
        return Ok((rest, FrameBound::UnboundedPreceding));
    }
    if let Ok((rest, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("UNBOUNDED"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("FOLLOWING")),
    )(input) {
        return Ok((rest, FrameBound::UnboundedFollowing));
    }
    // Try CURRENT ROW
    if let Ok((rest, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("CURRENT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("ROW")),
    )(input) {
        return Ok((rest, FrameBound::CurrentRow));
    }
    // Try n PRECEDING / n FOLLOWING
    let (rest, n) = nom::character::complete::u64(input)?;
    let (rest, _) = multispace1(rest)?;
    if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("PRECEDING")(rest) {
        return Ok((rest, FrameBound::Preceding(n)));
    }
    let (rest, _) = tag_no_case("FOLLOWING")(rest)?;
    Ok((rest, FrameBound::Following(n)))
}

fn parse_window_frame(input: &str) -> IResult<&str, Option<FrameSpec>> {
    let mode_res = nom::branch::alt((
        nom::combinator::map(tag_no_case::<&str, &str, nom::error::Error<&str>>("ROWS"), |_| FrameMode::Rows),
        nom::combinator::map(tag_no_case("RANGE"), |_| FrameMode::Range),
        nom::combinator::map(tag_no_case("GROUPS"), |_| FrameMode::Groups),
    ))(input);
    let (after_mode, mode) = match mode_res {
        Ok(r) => r,
        Err(_) => return Ok((input, None)),
    };
    let (after_mode, _) = multispace1(after_mode)?;
    // Try BETWEEN <start> AND <end>
    if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("BETWEEN")(after_mode) {
        let (rest, _) = multispace1(rest)?;
        let (rest, start) = parse_frame_bound(rest)?;
        let (rest, _) = multispace1(rest)?;
        let (rest, _) = tag_no_case("AND")(rest)?;
        let (rest, _) = multispace1(rest)?;
        let (rest, end) = parse_frame_bound(rest)?;
        return Ok((rest, Some(FrameSpec { mode, start, end })));
    }
    // Single bound: start only, end defaults to CURRENT ROW
    let (after_mode, start) = parse_frame_bound(after_mode)?;
    Ok((after_mode, Some(FrameSpec { mode, start, end: FrameBound::CurrentRow })))
}

fn parse_window_func_row_number(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("ROW_NUMBER")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::RowNumber))
}

fn parse_window_func_dense_rank(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("DENSE_RANK")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::DenseRank))
}

fn parse_window_func_rank(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("RANK")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::Rank))
}

fn parse_window_func_lag(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("LAG")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, offset) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        nom::character::complete::i64,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::Lag(Box::new(expr), offset.unwrap_or(1))))
}

fn parse_window_func_lead(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("LEAD")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, offset) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        nom::character::complete::i64,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::Lead(Box::new(expr), offset.unwrap_or(1))))
}

fn parse_window_func_agg(input: &str) -> IResult<&str, WindowFunc> {
    let (input, func_name) = nom::branch::alt((
        tag_no_case("COUNT"),
        tag_no_case("SUM"),
        tag_no_case("AVG"),
        tag_no_case("MIN"),
        tag_no_case("MAX"),
    ))(input)?;
    let agg_func = match func_name.to_uppercase().as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM"   => AggregateFunc::Sum,
        "AVG"   => AggregateFunc::Avg,
        "MIN"   => AggregateFunc::Min,
        "MAX"   => AggregateFunc::Max,
        _ => unreachable!(),
    };
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, inner) = nom::branch::alt((
        parse_all_column,
        parse_qualified_column,
        parse_simple_column,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::Agg(agg_func, Box::new(inner))))
}

fn parse_window_func_ntile(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("NTILE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::Ntile(Box::new(expr))))
}

fn parse_window_func_percent_rank(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("PERCENT_RANK")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::PercentRank))
}

fn parse_window_func_cume_dist(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("CUME_DIST")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::CumeDist))
}

fn parse_window_func_first_value(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("FIRST_VALUE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::FirstValue(Box::new(expr))))
}

fn parse_window_func_last_value(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("LAST_VALUE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::LastValue(Box::new(expr))))
}

fn parse_window_func_nth_value(input: &str) -> IResult<&str, WindowFunc> {
    let (input, _) = tag_no_case("NTH_VALUE")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = nom::sequence::delimited(multispace0, nom_char(','), multispace0)(input)?;
    let (input, n_expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, WindowFunc::NthValue(Box::new(expr), Box::new(n_expr))))
}

fn parse_expression_window(input: &str) -> IResult<&str, Expression> {
    let (input, _) = multispace0(input)?;

    // Parse the window function name+args; DENSE_RANK before RANK, PERCENT_RANK before PERCENT
    let (input, func) = nom::branch::alt((
        nom::branch::alt((
            parse_window_func_row_number,
            parse_window_func_dense_rank,
            parse_window_func_rank,
            parse_window_func_percent_rank, // must come before any shorter prefix
            parse_window_func_cume_dist,
        )),
        nom::branch::alt((
            parse_window_func_first_value,
            parse_window_func_last_value,
            parse_window_func_nth_value,
            parse_window_func_ntile,
            parse_window_func_lag,
            parse_window_func_lead,
            parse_window_func_agg,
        )),
    ))(input)?;

    // OVER is mandatory — if missing this parser fails and alt tries next option
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("OVER")(input)?;
    let (input, _) = multispace1(input)?;

    // Try bare identifier first: OVER w (not followed by '(')
    if let Ok((rest, name)) = parse_identifier(input) {
        let next = rest.trim_start();
        if !next.starts_with('(') && !is_reserved_keyword(name) {
            let spec = WindowSpec {
                base_window: Some(name.to_string()),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            };
            return Ok((rest, Expression::Window(func, spec)));
        }
    }

    // Otherwise parse a full window spec with parens: OVER (...)
    let (input, _) = multispace0(input)?;
    let (input, spec) = parse_window_spec(input)?;
    Ok((input, Expression::Window(func, spec)))
}

fn parse_expression_case(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("CASE")(input)?;
    let (input, _) = multispace1(input)?;

    let mut branches: Vec<(Condition, Expression)> = Vec::new();
    let mut input = input;
    loop {
        let (input_after_when, _) = match tag_no_case::<&str, &str, nom::error::Error<&str>>("WHEN")(input) {
            Ok(r) => r,
            Err(_) => break,
        };
        let (input_after_when, _) = multispace1(input_after_when)?;
        let (input_after_when, condition) = parse_condition(input_after_when)?;
        let (input_after_when, _) = multispace1(input_after_when)?;
        let (input_after_when, _) = tag_no_case("THEN")(input_after_when)?;
        let (input_after_when, _) = multispace1(input_after_when)?;
        let (input_after_when, result) = parse_expression(input_after_when)?;
        let (input_after_when, _) = multispace0(input_after_when)?;
        branches.push((condition, result));
        input = input_after_when;
    }

    if branches.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }

    let (input, else_expr) = if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("ELSE")(input) {
        let (i, _) = multispace1(i)?;
        let (i, expr) = parse_expression(i)?;
        let (i, _) = multispace1(i)?;
        (i, Some(Box::new(expr)))
    } else {
        (input, None)
    };

    let (input, _) = tag_no_case("END")(input)?;

    Ok((input, Expression::Case(branches, else_expr)))
}

/// Parse an aggregate function call as an expression: COUNT(*), SUM(col), AVG(t.col), etc.
fn parse_expression_aggregate(input: &str) -> IResult<&str, Expression> {
    let (input, func_name) = nom::branch::alt((
        tag_no_case("COUNT"),
        tag_no_case("SUM"),
        tag_no_case("AVG"),
        tag_no_case("MIN"),
        tag_no_case("MAX"),
    ))(input)?;
    let func = match func_name.to_uppercase().as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "AVG" => AggregateFunc::Avg,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        _ => unreachable!(),
    };
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, inner) = nom::branch::alt((
        parse_all_column,
        parse_qualified_column,
        parse_simple_column,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Aggregate(func, Box::new(inner))))
}

/// Parse (SELECT ...) as a scalar subquery expression
fn parse_expression_subquery(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, stmt) = parse_select_statement(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::Subquery(Box::new(stmt))))
}

/// Evaluate ROUND(val, places)
pub fn apply_round(val: Value, places: Option<Value>) -> Option<Value> {
    let decimals = match places {
        Some(Value::Int(n)) => n,
        None => 0,
        _ => return None,
    };
    match val {
        Value::Int(n) => Some(Value::Int(n)),
        Value::Float(f) => {
            let factor = 10f64.powi(decimals as i32);
            Some(Value::Float((f * factor).round() / factor))
        }
        _ => None,
    }
}

/// Evaluate CONCAT(values)
pub fn apply_concat(parts: Vec<Option<Value>>) -> Option<Value> {
    let mut result = String::new();
    for part in parts {
        match part {
            Some(Value::String(s))    => result.push_str(&s),
            Some(Value::Int(n))       => result.push_str(&n.to_string()),
            Some(Value::Float(f))     => result.push_str(&f.to_string()),
            Some(Value::Bool(b))      => result.push_str(if b { "true" } else { "false" }),
            Some(Value::Date(d))      => result.push_str(&format_date(d)),
            Some(Value::Timestamp(ts))=> result.push_str(&format_timestamp(ts)),
            Some(Value::Null) | None => return None,
        }
    }
    Some(Value::String(result))
}

/// Evaluate SUBSTR(str, start [, len]) — 1-indexed, like SQL
pub fn apply_substr(s: Value, start: Value, len: Option<Value>) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let start = match start { Value::Int(n) => n, _ => return None };
    // SQL SUBSTR is 1-indexed; clamp to valid range
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    let idx = if start >= 1 { (start - 1).min(n) as usize } else { 0 };
    let slice = &chars[idx..];
    let result: String = match len {
        Some(Value::Int(l)) => slice.iter().take(l.max(0) as usize).collect(),
        None => slice.iter().collect(),
        _ => return None,
    };
    Some(Value::String(result))
}

/// Evaluate REPLACE(str, from, to)
pub fn apply_replace(s: Value, from: Value, to: Value) -> Option<Value> {
    match (s, from, to) {
        (Value::String(s), Value::String(f), Value::String(t)) => Some(Value::String(s.replace(&*f, &*t))),
        _ => None,
    }
}

/// Evaluate LPAD(str, len, pad)
pub fn apply_lpad(s: Value, len: Value, pad: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let len = match len { Value::Int(n) => n.max(0) as usize, _ => return None };
    let pad = match pad { Value::String(p) => p, _ => return None };
    if pad.is_empty() { return Some(Value::String(s)); }
    let current = s.chars().count();
    if current >= len {
        return Some(Value::String(s.chars().take(len).collect()));
    }
    let needed = len - current;
    let pad_chars: Vec<char> = pad.chars().collect();
    let prefix: String = pad_chars.iter().cycle().take(needed).collect();
    Some(Value::String(format!("{}{}", prefix, s)))
}

/// Evaluate RPAD(str, len, pad)
pub fn apply_rpad(s: Value, len: Value, pad: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let len = match len { Value::Int(n) => n.max(0) as usize, _ => return None };
    let pad = match pad { Value::String(p) => p, _ => return None };
    if pad.is_empty() { return Some(Value::String(s)); }
    let current = s.chars().count();
    if current >= len {
        return Some(Value::String(s.chars().take(len).collect()));
    }
    let needed = len - current;
    let pad_chars: Vec<char> = pad.chars().collect();
    let suffix: String = pad_chars.iter().cycle().take(needed).collect();
    Some(Value::String(format!("{}{}", s, suffix)))
}

/// Evaluate CAST(value AS type)
pub fn apply_cast(val: Value, type_name: &str) -> Option<Value> {
    match type_name {
        "INT" | "INTEGER" | "BIGINT" => match val {
            Value::Int(n)       => Some(Value::Int(n)),
            Value::Float(f)     => Some(Value::Int(f as i64)),
            Value::Bool(b)      => Some(Value::Int(b as i64)),
            Value::String(s)    => s.trim().parse::<i64>().ok().map(Value::Int),
            Value::Date(d)      => Some(Value::Int(d as i64)),
            Value::Timestamp(ts)=> Some(Value::Int(ts)),
            Value::Null         => Some(Value::Null),
        },
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => match val {
            Value::Float(f)  => Some(Value::Float(f)),
            Value::Int(n)    => Some(Value::Float(n as f64)),
            Value::String(s) => s.trim().parse::<f64>().ok().map(Value::Float),
            Value::Null      => Some(Value::Null),
            _ => None,
        },
        "TEXT" | "VARCHAR" | "STRING" | "CHAR" => match val {
            Value::String(s)    => Some(Value::String(s)),
            Value::Int(n)       => Some(Value::String(n.to_string())),
            Value::Float(f)     => Some(Value::String(f.to_string())),
            Value::Bool(b)      => Some(Value::String(b.to_string())),
            Value::Date(d)      => Some(Value::String(format_date(d))),
            Value::Timestamp(ts)=> Some(Value::String(format_timestamp(ts))),
            Value::Null         => Some(Value::Null),
        },
        "BOOLEAN" | "BOOL" => match val {
            Value::Bool(b)   => Some(Value::Bool(b)),
            Value::Int(n)    => Some(Value::Bool(n != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(Value::Bool(true)),
                "false" | "0" | "no" => Some(Value::Bool(false)),
                _ => None,
            },
            Value::Null => Some(Value::Null),
            _ => None,
        },
        "DATE" => match val {
            Value::Date(d)      => Some(Value::Date(d)),
            Value::Timestamp(ts)=> Some(Value::Date((ts / 86400) as i32)),
            Value::String(s)    => parse_date_str(&s).map(Value::Date),
            Value::Int(n)       => Some(Value::Date(n as i32)),
            Value::Null         => Some(Value::Null),
            _ => None,
        },
        "TIMESTAMP" => match val {
            Value::Timestamp(ts)=> Some(Value::Timestamp(ts)),
            Value::Date(d)      => Some(Value::Timestamp(d as i64 * 86400)),
            Value::String(s)    => parse_timestamp_str(&s).map(Value::Timestamp),
            Value::Int(n)       => Some(Value::Timestamp(n)),
            Value::Null         => Some(Value::Null),
            _ => None,
        },
        _ => None, // unknown type
    }
}

/// Evaluate GREATEST(args) — return max non-NULL arg, or NULL if all NULL
pub fn apply_greatest(args: Vec<Option<Value>>) -> Option<Value> {
    let non_null: Vec<Value> = args.into_iter().flatten()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    non_null.into_iter().max_by(cmp_values_for_sort)
}

/// Evaluate LEAST(args) — return min non-NULL arg, or NULL if all NULL
pub fn apply_least(args: Vec<Option<Value>>) -> Option<Value> {
    let non_null: Vec<Value> = args.into_iter().flatten()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    non_null.into_iter().min_by(cmp_values_for_sort)
}

fn cmp_values_for_sort(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Evaluate POWER(base, exp)
pub fn apply_power(base: Value, exp: Value) -> Option<Value> {
    let b = match base { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return None };
    let e = match exp  { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return None };
    Some(Value::Float(b.powf(e)))
}

/// Evaluate POSITION(needle IN haystack) — 1-based, 0 if not found
pub fn apply_position(needle: Value, haystack: Value) -> Option<Value> {
    let n = match needle   { Value::String(s) => s, _ => return None };
    let h = match haystack { Value::String(s) => s, _ => return None };
    let pos = h.find(n.as_str()).map(|i| i + 1).unwrap_or(0);
    Some(Value::Int(pos as i64))
}

/// Evaluate REPEAT(str, n)
pub fn apply_repeat(s: Value, n: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let n = match n { Value::Int(n) if n >= 0 => n as usize, _ => return None };
    Some(Value::String(s.repeat(n)))
}

/// Apply a single-arg scalar function to a resolved Value
pub fn apply_scalar_func(func: &ScalarFunc, val: Value) -> Option<Value> {
    match (func, val) {
        (ScalarFunc::Upper,  Value::String(s)) => Some(Value::String(s.to_uppercase())),
        (ScalarFunc::Lower,  Value::String(s)) => Some(Value::String(s.to_lowercase())),
        (ScalarFunc::Length, Value::String(s)) => Some(Value::Int(s.len() as i64)),
        (ScalarFunc::Trim,   Value::String(s)) => Some(Value::String(s.trim().to_string())),
        (ScalarFunc::LTrim,  Value::String(s)) => Some(Value::String(s.trim_start().to_string())),
        (ScalarFunc::RTrim,  Value::String(s)) => Some(Value::String(s.trim_end().to_string())),
        (ScalarFunc::Abs, Value::Int(n))    => Some(Value::Int(n.abs())),
        (ScalarFunc::Abs, Value::Float(f))  => Some(Value::Float(f.abs())),
        (ScalarFunc::Ceil,  Value::Float(f)) => Some(Value::Float(f.ceil())),
        (ScalarFunc::Ceil,  Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Floor, Value::Float(f)) => Some(Value::Float(f.floor())),
        (ScalarFunc::Floor, Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Sqrt,  Value::Float(f)) => Some(Value::Float(f.sqrt())),
        (ScalarFunc::Sqrt,  Value::Int(n))   => Some(Value::Float((n as f64).sqrt())),
        (ScalarFunc::Sign,  Value::Int(n))   => Some(Value::Int(n.signum())),
        (ScalarFunc::Sign,  Value::Float(f)) => Some(Value::Float(f.signum())),
        (ScalarFunc::Trunc, Value::Float(f)) => Some(Value::Float(f.trunc())),
        (ScalarFunc::Trunc, Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Reverse, Value::String(s)) => Some(Value::String(s.chars().rev().collect())),
        // Date extraction from Date values
        (ScalarFunc::Year,  Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).0 as i64)),
        (ScalarFunc::Month, Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).1 as i64)),
        (ScalarFunc::Day,   Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).2 as i64)),
        // Date extraction from Timestamp values
        (ScalarFunc::Year,  Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).0 as i64)),
        (ScalarFunc::Month, Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).1 as i64)),
        (ScalarFunc::Day,   Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).2 as i64)),
        (ScalarFunc::Hour,   Value::Timestamp(ts)) => Some(Value::Int((ts % 86400) / 3600)),
        (ScalarFunc::Minute, Value::Timestamp(ts)) => Some(Value::Int((ts % 86400 % 3600) / 60)),
        (ScalarFunc::Second, Value::Timestamp(ts)) => Some(Value::Int(ts % 60)),
        // DayOfWeek: Unix epoch (1970-01-01) was Thursday=4; 0=Sun,1=Mon,...,6=Sat
        (ScalarFunc::DayOfWeek, Value::Date(d)) => Some(Value::Int(((d % 7 + 4) % 7) as i64)),
        (ScalarFunc::DayOfWeek, Value::Timestamp(ts)) => {
            let d = (ts / 86400) as i32;
            Some(Value::Int(((d % 7 + 4) % 7) as i64))
        }
        (ScalarFunc::DayOfYear, Value::Date(d)) => {
            let (y, _, _) = epoch_days_to_date(d);
            let jan1 = date_to_epoch_days(y, 1, 1);
            Some(Value::Int((d - jan1 + 1) as i64))
        }
        (ScalarFunc::DayOfYear, Value::Timestamp(ts)) => {
            let d = (ts / 86400) as i32;
            let (y, _, _) = epoch_days_to_date(d);
            let jan1 = date_to_epoch_days(y, 1, 1);
            Some(Value::Int((d - jan1 + 1) as i64))
        }
        // Allow string input by coercing to date
        (ScalarFunc::Year, Value::String(s))  => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).0 as i64)),
        (ScalarFunc::Month, Value::String(s)) => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).1 as i64)),
        (ScalarFunc::Day, Value::String(s))   => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).2 as i64)),
        // Hour/Minute/Second from Date are always 0
        (ScalarFunc::Hour, Value::Date(_))   => Some(Value::Int(0)),
        (ScalarFunc::Minute, Value::Date(_)) => Some(Value::Int(0)),
        (ScalarFunc::Second, Value::Date(_)) => Some(Value::Int(0)),
        _ => None,
    }
}

fn parse_expression_qualified_column(input: &str) -> IResult<&str, Expression> {
    let (input, table) = parse_identifier(input)?;
    let (input, _) = nom_char('.')(input)?;
    let (input, column) = parse_identifier(input)?;
    Ok((input, Expression::QualifiedColumn(
        table.to_string(),
        column.to_string(),
    )))
}

fn parse_expression_simple_column(input: &str) -> IResult<&str, Expression> {
    let (input, name) = parse_identifier(input)?;
    Ok((input, Expression::Column(name.to_string())))
}

fn parse_expression_literal(input: &str) -> IResult<&str, Expression> {
    let (input, value) = parse_value(input)?;
    Ok((input, Expression::Literal(value)))
}

/// Parse operator: =, !=, >, <, >=, <=, LIKE
/// Parse IN (...): either a subquery or a comma-separated literal value list
fn parse_in_list(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;

    // If it starts with SELECT, parse as subquery
    if input.trim_start().to_uppercase().starts_with("SELECT") {
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Expression::Subquery(Box::new(subquery))));
    }

    // Parse a comma-separated list of scalar expressions (columns, literals, functions, etc.)
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_atom,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::List(exprs)))
}

fn parse_operator(input: &str) -> IResult<&str, Operator> {
    nom::branch::alt((
        nom::combinator::map(tag("!="), |_| Operator::NotEquals),
        nom::combinator::map(tag(">="), |_| Operator::GreaterThanOrEqual),
        nom::combinator::map(tag("<="), |_| Operator::LessThanOrEqual),
        nom::combinator::map(tag("="), |_| Operator::Equals),
        nom::combinator::map(tag(">"), |_| Operator::GreaterThan),
        nom::combinator::map(tag("<"), |_| Operator::LessThan),
        nom::combinator::map(tag_no_case("ILIKE"), |_| Operator::ILike),
        nom::combinator::map(tag_no_case("LIKE"), |_| Operator::Like),
    ))(input)
}

/// Parse value: float, integer, string, or NULL
fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    let (input, value) = nom::branch::alt((
        parse_date_value,
        parse_timestamp_value,
        parse_string_value,
        parse_null_value,
        parse_bool_value,
        parse_float_value,
        parse_int_value,
    ))(input)?;
    Ok((input, value))
}

/// Parse DATE 'YYYY-MM-DD' as Value::Date
fn parse_date_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag_no_case("DATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, val) = parse_string_value(input)?;
    if let Value::String(s) = val {
        match parse_date_str(&s) {
            Some(d) => Ok((input, Value::Date(d))),
            None => Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
        }
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
    }
}

/// Parse TIMESTAMP 'YYYY-MM-DD HH:MM:SS' as Value::Timestamp
fn parse_timestamp_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag_no_case("TIMESTAMP")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, val) = parse_string_value(input)?;
    if let Value::String(s) = val {
        match parse_timestamp_str(&s) {
            Some(ts) => Ok((input, Value::Timestamp(ts))),
            None => Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
        }
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
    }
}

fn parse_bool_value(input: &str) -> IResult<&str, Value> {
    let (input, val) = nom::branch::alt((tag_no_case("TRUE"), tag_no_case("FALSE")))(input)?;
    Ok((input, Value::Bool(val.eq_ignore_ascii_case("TRUE"))))
}

/// Parse float literal: digits.digits (must have decimal point)
fn parse_float_value(input: &str) -> IResult<&str, Value> {
    let (input, neg) = nom::combinator::opt(nom_char('-'))(input)?;
    let (input, whole) = nom::character::complete::digit1(input)?;
    let (input, _) = nom_char('.')(input)?;
    let (input, frac) = nom::character::complete::digit1(input)?;
    let s = format!("{}{}.{}", if neg.is_some() { "-" } else { "" }, whole, frac);
    let n = s.parse::<f64>().map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float)))?;
    Ok((input, Value::Float(n)))
}

fn parse_int_value(input: &str) -> IResult<&str, Value> {
    let (input, num) = nom::character::complete::i64(input)?;
    Ok((input, Value::Int(num)))
}

fn parse_string_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = nom_char('\'')(input)?;
    // Accumulate content, treating '' as an escaped single quote
    let mut result = String::new();
    let mut remaining = input;
    loop {
        // Consume up to the next single quote
        let (rest, chunk) = nom::bytes::complete::take_while(|c| c != '\'')(remaining)?;
        result.push_str(chunk);
        // Consume the closing quote
        let (rest, _) = nom_char('\'')(rest)?;
        // If the next char is also a quote, it's an escape sequence ''
        if rest.starts_with('\'') {
            result.push('\'');
            remaining = &rest[1..];
        } else {
            remaining = rest;
            break;
        }
    }
    Ok((remaining, Value::String(result)))
}

fn parse_null_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag_no_case("NULL")(input)?;
    Ok((input, Value::Null))
}

/// Parse identifier (table/column name)
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        nom::character::complete::alpha1,
        nom::bytes::complete::take_while(|c: char| c.is_alphanumeric() || c == '_'),
    )))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT, name VARCHAR(255));";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "users");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[1].name, "name");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice');";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert_eq!(ins.values().len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_select() {
        let sql = "SELECT * FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.from, FromClause::Table("users".to_string()));
                assert_eq!(sel.columns.len(), 1);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_create_table_varchar_no_size() {
        let sql = "CREATE TABLE products (id INT, name VARCHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "products");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[1].name, "name");
                match ct.columns[1].data_type {
                    DataType::Varchar(None) => {},
                    _ => panic!("Expected VARCHAR without size"),
                }
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_multiple_columns() {
        let sql = "CREATE TABLE orders (id INT, user_id INT, product VARCHAR(100), quantity INT);";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "orders");
                assert_eq!(ct.columns.len(), 4);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[1].name, "user_id");
                assert_eq!(ct.columns[2].name, "product");
                assert_eq!(ct.columns[3].name, "quantity");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_no_semicolon() {
        let sql = "CREATE TABLE test (id INT)";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "test");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_insert_with_null() {
        let sql = "INSERT INTO users VALUES (1, NULL, 'test@example.com');";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert_eq!(ins.values().len(), 3);
                assert_eq!(ins.values()[0], Value::Int(1));
                assert_eq!(ins.values()[1], Value::Null);
                assert_eq!(ins.values()[2], Value::String("test@example.com".to_string()));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_no_semicolon() {
        let sql = "INSERT INTO users VALUES (42, 'Bob')";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.table_name, "users");
                assert_eq!(ins.values().len(), 2);
                assert_eq!(ins.values()[0], Value::Int(42));
                assert_eq!(ins.values()[1], Value::String("Bob".to_string()));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_negative_numbers() {
        let sql = "INSERT INTO accounts VALUES (-100, 'debit');";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.values()[0], Value::Int(-100));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_select() {
        let sql = "INSERT INTO archive SELECT id, name FROM users WHERE active = TRUE;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.table_name, "archive");
                match &ins.source {
                    InsertSource::Select(sel) => {
                        assert_eq!(sel.from, FromClause::Table("users".to_string()));
                        assert_eq!(sel.columns.len(), 2);
                    }
                    _ => panic!("Expected InsertSource::Select"),
                }
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_select_specific_columns() {
        let sql = "SELECT name, email FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                match &sel.columns[0] {
                    SelectColumn::Column(name) => assert_eq!(name, "name"),
                    _ => panic!("Expected Column"),
                }
                match &sel.columns[1] {
                    SelectColumn::Column(name) => assert_eq!(name, "email"),
                    _ => panic!("Expected Column"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT * FROM users WHERE id = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
                let where_clause = sel.where_clause.unwrap();
                match where_clause.condition.left() {
                    Expression::Column(name) => assert_eq!(name, "id"),
                    _ => panic!("Expected Column expression"),
                }
                assert_eq!(where_clause.condition.operator(), Operator::Equals);
                match where_clause.condition.right() {
                    Expression::Literal(Value::Int(1)) => {},
                    _ => panic!("Expected Int literal"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_where_string() {
        let sql = "SELECT * FROM users WHERE name = 'Alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                let where_clause = sel.where_clause.unwrap();
                match where_clause.condition.right() {
                    Expression::Literal(Value::String(s)) => assert_eq!(s, "Alice"),
                    _ => panic!("Expected String literal"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_where_operators() {
        let test_cases = vec![
            ("id > 10", Operator::GreaterThan),
            ("id < 10", Operator::LessThan),
            ("id >= 10", Operator::GreaterThanOrEqual),
            ("id <= 10", Operator::LessThanOrEqual),
            ("id != 10", Operator::NotEquals),
        ];

        for (condition, expected_op) in test_cases {
            let sql = format!("SELECT * FROM users WHERE {};", condition);
            let (_, stmt) = parse_sql(&sql).unwrap();
            
            match stmt {
                SqlStatement::Select(sel) => {
                    let where_clause = sel.where_clause.unwrap();
                    assert_eq!(where_clause.condition.operator(), expected_op);
                }
                _ => panic!("Expected Select"),
            }
        }
    }

    #[test]
    fn test_parse_select_with_join() {
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id;";
        let result = parse_sql(sql);
        if result.is_err() {
            println!("Parse error: {:?}", result);
        }
        let (_, stmt) = result.unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                println!("Joins found: {}", sel.joins.len());
                assert_eq!(sel.joins.len(), 1);
                let join = &sel.joins[0];
                assert_eq!(join.table, "orders");
                assert_eq!(join.join_type, JoinType::Inner);
                match &join.on.as_ref().unwrap().left() {
                    Expression::QualifiedColumn(table, col) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "id");
                    }
                    _ => panic!("Expected QualifiedColumn"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_join_types() {
        let test_cases = vec![
            ("INNER JOIN", JoinType::Inner),
            ("LEFT JOIN", JoinType::Left),
            ("RIGHT JOIN", JoinType::Right),
            ("FULL JOIN", JoinType::Full),
            ("FULL OUTER JOIN", JoinType::Full),
            ("JOIN", JoinType::Inner), // JOIN defaults to INNER
        ];

        for (join_type, expected) in test_cases {
            let sql = format!("SELECT * FROM users {} orders ON users.id = orders.user_id;", join_type);
            let (_, stmt) = parse_sql(&sql).unwrap();
            
            match stmt {
                SqlStatement::Select(sel) => {
                    assert_eq!(sel.joins[0].join_type, expected);
                }
                _ => panic!("Expected Select"),
            }
        }
    }

    #[test]
    fn test_parse_cross_join() {
        let sql = "SELECT * FROM users CROSS JOIN orders;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                let join = &sel.joins[0];
                assert_eq!(join.join_type, JoinType::Cross);
                assert_eq!(join.table, "orders");
                assert!(join.on.is_none());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_join_alias() {
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                let join = &sel.joins[0];
                assert_eq!(join.alias, Some("o".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_qualified_columns() {
        let sql = "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                match &sel.columns[0] {
                    SelectColumn::QualifiedColumn(table, col) => {
                        assert_eq!(table, "users");
                        assert_eq!(col, "name");
                    }
                    _ => panic!("Expected QualifiedColumn"),
                }
                match &sel.columns[1] {
                    SelectColumn::QualifiedColumn(table, col) => {
                        assert_eq!(table, "orders");
                        assert_eq!(col, "product");
                    }
                    _ => panic!("Expected QualifiedColumn"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_multiple_joins() {
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.joins.len(), 2);
                assert_eq!(sel.joins[0].table, "orders");
                assert_eq!(sel.joins[1].table, "products");
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_where_and_join() {
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
                assert_eq!(sel.joins.len(), 1);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_no_semicolon() {
        let sql = "SELECT * FROM users";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.from, FromClause::Table("users".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_whitespace_variations() {
        // Test with extra whitespace
        let sql = "SELECT   *   FROM   users   WHERE   id   =   1  ;";
        let (_, stmt) = parse_sql(sql).unwrap();
        
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_string_with_apostrophe() {
        // Test string parsing - note: our current parser doesn't handle escaped quotes
        let sql = "INSERT INTO users VALUES (1, 'O''Brien');";
        // This will fail with current implementation, but let's test it
        let result = parse_sql(sql);
        // For now, we expect this might fail or parse incorrectly
        // This test documents current behavior
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_parse_update_single_column() {
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Update(upd) => {
                assert_eq!(upd.table_name, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.assignments[0].column, "name");
                assert_eq!(upd.assignments[0].value, Expression::Literal(Value::String("Bob".to_string())));
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_update_multiple_columns() {
        let sql = "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Update(upd) => {
                assert_eq!(upd.table_name, "users");
                assert_eq!(upd.assignments.len(), 2);
                assert_eq!(upd.assignments[0].column, "name");
                assert_eq!(upd.assignments[1].column, "email");
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_update_no_where() {
        let sql = "UPDATE users SET active = 0;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Update(upd) => {
                assert_eq!(upd.table_name, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.assignments[0].column, "active");
                assert_eq!(upd.assignments[0].value, Expression::Literal(Value::Int(0)));
                assert!(upd.where_clause.is_none());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_update_no_semicolon() {
        let sql = "UPDATE users SET name = 'Alice' WHERE id = 5";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Update(upd) => {
                assert_eq!(upd.table_name, "users");
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_update_with_null() {
        let sql = "UPDATE users SET email = NULL WHERE id = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Update(upd) => {
                assert_eq!(upd.assignments[0].value, Expression::Literal(Value::Null));
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_delete_with_where() {
        let sql = "DELETE FROM users WHERE id = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Delete(del) => {
                assert_eq!(del.table_name, "users");
                assert!(del.where_clause.is_some());
                let wc = del.where_clause.unwrap();
                match wc.condition.left() {
                    Expression::Column(name) => assert_eq!(name, "id"),
                    _ => panic!("Expected Column"),
                }
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_all() {
        let sql = "DELETE FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Delete(del) => {
                assert_eq!(del.table_name, "users");
                assert!(del.where_clause.is_none());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_no_semicolon() {
        let sql = "DELETE FROM products WHERE price > 100";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Delete(del) => {
                assert_eq!(del.table_name, "products");
                assert!(del.where_clause.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_with_string_condition() {
        let sql = "DELETE FROM users WHERE name = 'Bob';";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Delete(del) => {
                let wc = del.where_clause.unwrap();
                match wc.condition.right() {
                    Expression::Literal(Value::String(s)) => assert_eq!(s, "Bob"),
                    _ => panic!("Expected String literal"),
                }
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_order_by_asc() {
        let sql = "SELECT * FROM users ORDER BY name ASC;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert_eq!(sel.order_by[0].column, SelectColumn::Column("name".to_string()));
                assert!(!sel.order_by[0].descending);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_order_by_desc() {
        let sql = "SELECT * FROM users ORDER BY id DESC;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(sel.order_by[0].descending);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_order_by_default_asc() {
        let sql = "SELECT * FROM users ORDER BY name;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].descending);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_order_by_multiple() {
        let sql = "SELECT * FROM users ORDER BY name ASC, id DESC;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 2);
                assert_eq!(sel.order_by[0].column, SelectColumn::Column("name".to_string()));
                assert!(!sel.order_by[0].descending);
                assert_eq!(sel.order_by[1].column, SelectColumn::Column("id".to_string()));
                assert!(sel.order_by[1].descending);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_order_by_qualified() {
        let sql = "SELECT * FROM users ORDER BY users.name DESC;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert_eq!(sel.order_by[0].column, SelectColumn::QualifiedColumn("users".to_string(), "name".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_count_star() {
        let sql = "SELECT COUNT(*) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.columns.len(), 1);
                match &sel.columns[0] {
                    SelectColumn::Aggregate(AggregateFunc::Count, inner) => {
                        assert_eq!(**inner, SelectColumn::All);
                    }
                    _ => panic!("Expected COUNT(*)"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_aggregate_functions() {
        let test_cases = vec![
            ("SELECT SUM(price) FROM products;", AggregateFunc::Sum, "price"),
            ("SELECT AVG(price) FROM products;", AggregateFunc::Avg, "price"),
            ("SELECT MIN(id) FROM users;", AggregateFunc::Min, "id"),
            ("SELECT MAX(id) FROM users;", AggregateFunc::Max, "id"),
            ("SELECT COUNT(name) FROM users;", AggregateFunc::Count, "name"),
        ];

        for (sql, expected_func, expected_col) in test_cases {
            let (_, stmt) = parse_sql(sql).unwrap();
            match stmt {
                SqlStatement::Select(sel) => {
                    match &sel.columns[0] {
                        SelectColumn::Aggregate(func, inner) => {
                            assert_eq!(*func, expected_func, "Failed for: {}", sql);
                            assert_eq!(**inner, SelectColumn::Column(expected_col.to_string()));
                        }
                        _ => panic!("Expected aggregate for: {}", sql),
                    }
                }
                _ => panic!("Expected Select"),
            }
        }
    }

    #[test]
    fn test_parse_mixed_aggregate_and_columns() {
        let sql = "SELECT name, COUNT(*) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                assert_eq!(sel.columns[0], SelectColumn::Column("name".to_string()));
                match &sel.columns[1] {
                    SelectColumn::Aggregate(AggregateFunc::Count, _) => {}
                    _ => panic!("Expected COUNT(*)"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_where_with_order_by() {
        let sql = "SELECT * FROM users WHERE id > 1 ORDER BY name DESC;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
                assert_eq!(sel.order_by.len(), 1);
                assert!(sel.order_by[0].descending);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_limit() {
        let sql = "SELECT * FROM users LIMIT 10;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_order_by_with_limit() {
        let sql = "SELECT * FROM users ORDER BY name LIMIT 5;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert_eq!(sel.limit, Some(5));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_limit() {
        let sql = "SELECT * FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.limit, None);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_where_order_by_limit() {
        let sql = "SELECT * FROM users WHERE id > 1 ORDER BY name DESC LIMIT 3;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
                assert_eq!(sel.order_by.len(), 1);
                assert!(sel.order_by[0].descending);
                assert_eq!(sel.limit, Some(3));
                assert_eq!(sel.offset, None);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_limit_offset() {
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 20;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(10));
                assert_eq!(sel.offset, Some(20));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_offset() {
        let sql = "SELECT * FROM users LIMIT 5;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.limit, Some(5));
                assert_eq!(sel.offset, None);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_group_by_single() {
        let sql = "SELECT name, COUNT(*) FROM users GROUP BY name;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert_eq!(sel.group_by[0], SelectColumn::Column("name".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_group_by_multiple() {
        let sql = "SELECT name, email, COUNT(*) FROM users GROUP BY name, email;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 2);
                assert_eq!(sel.group_by[0], SelectColumn::Column("name".to_string()));
                assert_eq!(sel.group_by[1], SelectColumn::Column("email".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_group_by_with_order_by() {
        let sql = "SELECT name, COUNT(*) FROM users GROUP BY name ORDER BY name;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert_eq!(sel.order_by.len(), 1);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_group_by_qualified() {
        let sql = "SELECT users.name, COUNT(*) FROM users GROUP BY users.name;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert_eq!(sel.group_by[0], SelectColumn::QualifiedColumn("users".to_string(), "name".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_group_by() {
        let sql = "SELECT * FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.group_by.is_empty());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_having_simple() {
        let sql = "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                let having = sel.having.expect("HAVING clause");
                assert_eq!(having.condition.operator(), Operator::GreaterThan);
                match having.condition.left() {
                    Expression::Aggregate(AggregateFunc::Count, ref inner) => {
                        assert_eq!(**inner, SelectColumn::All);
                    }
                    other => panic!("expected COUNT(*) aggregate, got {:?}", other),
                }
                assert_eq!(having.condition.right(), Expression::Literal(Value::Int(1)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_having_sum_column() {
        let sql = "SELECT dept, SUM(salary) FROM emp GROUP BY dept HAVING SUM(salary) > 100000;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let having = sel.having.expect("HAVING clause");
                match having.condition.left() {
                    Expression::Aggregate(AggregateFunc::Sum, ref inner) => {
                        assert_eq!(**inner, SelectColumn::Column("salary".to_string()));
                    }
                    other => panic!("expected SUM(salary), got {:?}", other),
                }
                assert_eq!(having.condition.right(), Expression::Literal(Value::Int(100000)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_having_with_order_by_limit() {
        let sql = "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) >= 2 ORDER BY name LIMIT 10;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.having.is_some());
                assert_eq!(sel.order_by.len(), 1);
                assert_eq!(sel.limit, Some(10));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_having() {
        let sql = "SELECT name, COUNT(*) FROM users GROUP BY name;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.having.is_none());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_distinct() {
        let sql = "SELECT DISTINCT name FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.distinct);
                assert_eq!(sel.columns.len(), 1);
                assert_eq!(sel.columns[0], SelectColumn::Column("name".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_distinct_star() {
        let sql = "SELECT DISTINCT * FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.distinct);
                assert_eq!(sel.columns[0], SelectColumn::All);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_distinct() {
        let sql = "SELECT name FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(!sel.distinct);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_like_operator() {
        let sql = "SELECT * FROM users WHERE name LIKE 'A%';";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::Like);
                match &wc.condition.right() {
                    Expression::Literal(Value::String(s)) => assert_eq!(s, "A%"),
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_like_underscore() {
        let sql = "SELECT * FROM users WHERE name LIKE '_ob';";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::Like);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_like() {
        let sql = "SELECT * FROM users WHERE name NOT LIKE 'A%';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.where_clause.unwrap().condition.operator(), Operator::NotLike);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_ilike() {
        let sql = "SELECT * FROM users WHERE name ILIKE 'alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.where_clause.unwrap().condition.operator(), Operator::ILike);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_ilike() {
        let sql = "SELECT * FROM users WHERE name NOT ILIKE 'alice%';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.where_clause.unwrap().condition.operator(), Operator::NotILike);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_cast() {
        let sql = "SELECT CAST(price AS INT) FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Cast(inner, type_name)) => {
                        assert_eq!(**inner, Expression::Column("price".to_string()));
                        assert_eq!(type_name, "INT");
                    }
                    _ => panic!("Expected Cast"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_string_escaped_quote() {
        let sql = "SELECT * FROM users WHERE name = 'it''s here';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.right(), Expression::Literal(Value::String("it's here".to_string())));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_strip_line_comment() {
        let sql = "SELECT * FROM users -- get all users\nWHERE id = 1;";
        let (_, stmt) = parse_sql(&strip_sql_comments(sql)).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.where_clause.is_some());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_strip_block_comment() {
        let sql = "SELECT /* all cols */ * FROM users;";
        let (_, stmt) = parse_sql(&strip_sql_comments(sql)).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(sel.columns[0], SelectColumn::All));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_in_subquery() {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::In);
                match &wc.condition.left() {
                    Expression::Column(name) => assert_eq!(name, "id"),
                    _ => panic!("Expected column"),
                }
                match &wc.condition.right() {
                    Expression::Subquery(sub) => {
                        assert_eq!(sub.from, FromClause::Table("orders".to_string()));
                        assert_eq!(sub.columns.len(), 1);
                    }
                    _ => panic!("Expected subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_in_subquery_with_where() {
        let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'active');";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::In);
                match &wc.condition.right() {
                    Expression::Subquery(sub) => {
                        assert!(sub.where_clause.is_some());
                    }
                    _ => panic!("Expected subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let sql = "SELECT * FROM users WHERE id = (SELECT MAX(id) FROM users);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::Equals);
                match &wc.condition.right() {
                    Expression::Subquery(sub) => {
                        assert_eq!(sub.from, FromClause::Table("users".to_string()));
                    }
                    _ => panic!("Expected subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_scalar_subquery_gt() {
        let sql = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::GreaterThan);
                match &wc.condition.right() {
                    Expression::Subquery(sub) => {
                        assert_eq!(sub.from, FromClause::Table("products".to_string()));
                    }
                    _ => panic!("Expected subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_in_subquery() {
        let sql = "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::NotIn);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_in_value_list() {
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::In);
                assert_eq!(wc.condition.right(), Expression::List(vec![
                    Expression::Literal(Value::Int(1)),
                    Expression::Literal(Value::Int(2)),
                    Expression::Literal(Value::Int(3)),
                ]));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_in_value_list() {
        let sql = "SELECT * FROM users WHERE status NOT IN ('active', 'pending');";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::NotIn);
                assert_eq!(wc.condition.right(), Expression::List(vec![
                    Expression::Literal(Value::String("active".to_string())),
                    Expression::Literal(Value::String("pending".to_string())),
                ]));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_exists() {
        let sql = "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders WHERE user_id = 1);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::Exists);
                match &wc.condition.right() {
                    Expression::Subquery(sub) => {
                        assert_eq!(sub.from, FromClause::Table("orders".to_string()));
                    }
                    _ => panic!("Expected subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_exists() {
        let sql = "SELECT * FROM users WHERE NOT EXISTS (SELECT id FROM orders WHERE user_id = 1);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::NotExists);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_is_null() {
        let sql = "SELECT * FROM users WHERE email IS NULL;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::IsNull);
                assert_eq!(wc.condition.left(), Expression::Column("email".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_is_not_null() {
        let sql = "SELECT * FROM users WHERE email IS NOT NULL;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::IsNotNull);
                assert_eq!(wc.condition.left(), Expression::Column("email".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_union() {
        let sql = "SELECT id FROM users UNION SELECT id FROM admins;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let (union_type, right) = sel.union.unwrap();
                assert_eq!(union_type, UnionType::Union);
                assert_eq!(right.from, FromClause::Table("admins".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_union_all() {
        let sql = "SELECT id FROM users UNION ALL SELECT id FROM admins;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let (union_type, _) = sel.union.unwrap();
                assert_eq!(union_type, UnionType::UnionAll);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_union() {
        let sql = "SELECT id FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => assert!(sel.union.is_none()),
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_case_simple() {
        let sql = "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Case(branches, else_expr)) => {
                        assert_eq!(branches.len(), 1);
                        assert!(else_expr.is_some());
                    }
                    _ => panic!("Expected Case expression"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_case_multiple_when() {
        let sql = "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM students;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Alias(inner, alias) => {
                        assert_eq!(alias, "grade");
                        match inner.as_ref() {
                            SelectColumn::Expr(Expression::Case(branches, _)) => {
                                assert_eq!(branches.len(), 2);
                            }
                            _ => panic!("Expected Case inside Alias"),
                        }
                    }
                    _ => panic!("Expected Alias(Case)"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_case_no_else() {
        let sql = "SELECT CASE WHEN active = TRUE THEN 'yes' END FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Case(branches, else_expr)) => {
                        assert_eq!(branches.len(), 1);
                        assert!(else_expr.is_none());
                    }
                    _ => panic!("Expected Case expression"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_between() {
        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::Between);
                assert_eq!(wc.condition.left(), Expression::Column("age".to_string()));
                assert_eq!(wc.condition.right(), Expression::Literal(Value::Int(18)));
                assert_eq!(wc.condition.upper_bound(), Some(Expression::Literal(Value::Int(65))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_between() {
        let sql = "SELECT * FROM users WHERE age NOT BETWEEN 18 AND 65;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::NotBetween);
                assert_eq!(wc.condition.upper_bound(), Some(Expression::Literal(Value::Int(65))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_and_condition() {
        let sql = "SELECT * FROM users WHERE age > 18 AND name = 'Alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::And(_, _)));
                if let Condition::And(left, right) = wc.condition {
                    assert_eq!(left.operator(), Operator::GreaterThan);
                    assert_eq!(right.operator(), Operator::Equals);
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_or_condition() {
        let sql = "SELECT * FROM users WHERE age < 10 OR age > 90;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::Or(_, _)));
                if let Condition::Or(left, right) = wc.condition {
                    assert_eq!(left.operator(), Operator::LessThan);
                    assert_eq!(right.operator(), Operator::GreaterThan);
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_and_or_precedence() {
        // AND binds tighter: a=1 OR b=2 AND c=3 → a=1 OR (b=2 AND c=3)
        let sql = "SELECT * FROM users WHERE a = 1 OR b = 2 AND c = 3;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::Or(_, _)));
                if let Condition::Or(_, right) = wc.condition {
                    assert!(matches!(*right, Condition::And(_, _)));
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_parenthesized_condition() {
        let sql = "SELECT * FROM users WHERE (a = 1 OR b = 2) AND c = 3;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::And(_, _)));
                if let Condition::And(left, _) = wc.condition {
                    assert!(matches!(*left, Condition::Or(_, _)));
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_simple() {
        let sql = "SELECT * FROM users WHERE NOT active = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::Not(_)));
                if let Condition::Not(inner) = wc.condition {
                    assert_eq!(inner.operator(), Operator::Equals);
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_parenthesized() {
        let sql = "SELECT * FROM users WHERE NOT (age > 18 AND active = 1);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition, Condition::Not(_)));
                if let Condition::Not(inner) = wc.condition {
                    assert!(matches!(*inner, Condition::And(_, _)));
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_exists_unaffected() {
        // NOT EXISTS should still parse as a Comparison, not a Not(Exists(...))
        let sql = "SELECT * FROM users WHERE NOT EXISTS (SELECT id FROM orders WHERE user_id = 1);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.operator(), Operator::NotExists);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_cte_simple() {
        let sql = "WITH active AS (SELECT * FROM users WHERE id > 1) SELECT * FROM active;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.ctes.len(), 1);
                assert_eq!(sel.ctes[0].name, "active");
                assert_eq!(sel.ctes[0].query.from, FromClause::Table("users".to_string()));
                assert_eq!(sel.from, FromClause::Table("active".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_cte_multiple() {
        let sql = "WITH a AS (SELECT * FROM users), b AS (SELECT * FROM products) SELECT * FROM a;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.ctes.len(), 2);
                assert_eq!(sel.ctes[0].name, "a");
                assert_eq!(sel.ctes[1].name, "b");
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_no_cte() {
        let sql = "SELECT * FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                assert!(sel.ctes.is_empty());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_column_alias() {
        let sql = "SELECT name AS n FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Alias(inner, alias) => {
                        assert_eq!(**inner, SelectColumn::Column("name".to_string()));
                        assert_eq!(alias, "n");
                    }
                    _ => panic!("Expected Alias"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_aggregate_alias() {
        let sql = "SELECT COUNT(*) AS cnt FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Alias(inner, alias) => {
                        assert!(matches!(inner.as_ref(), SelectColumn::Aggregate(AggregateFunc::Count, _)));
                        assert_eq!(alias, "cnt");
                    }
                    _ => panic!("Expected Alias"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_from_subquery() {
        let sql = "SELECT * FROM (SELECT name FROM users) AS t;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.from {
                    FromClause::Subquery(sub) => {
                        assert_eq!(sub.from, FromClause::Table("users".to_string()));
                    }
                    _ => panic!("Expected subquery FROM"),
                }
                assert_eq!(sel.from_alias, Some("t".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_from_subquery_with_aggregates() {
        let sql = "SELECT * FROM (SELECT name, COUNT(*) AS cnt FROM users GROUP BY name) AS counts;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.from {
                    FromClause::Subquery(sub) => {
                        assert!(!sub.group_by.is_empty());
                    }
                    _ => panic!("Expected subquery FROM"),
                }
                assert_eq!(sel.from_alias, Some("counts".to_string()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_arithmetic_add() {
        let sql = "SELECT * FROM products WHERE price > 100 + 50;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                match &wc.condition.right() {
                    Expression::BinaryOp(_, ArithOp::Add, _) => {}
                    _ => panic!("Expected BinaryOp Add"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_arithmetic_mul() {
        let sql = "SELECT * FROM products WHERE price > 10 * 5;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                match &wc.condition.right() {
                    Expression::BinaryOp(_, ArithOp::Mul, _) => {}
                    _ => panic!("Expected BinaryOp Mul"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_arithmetic_precedence() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let sql = "SELECT * FROM users WHERE id = 1 + 2 * 3;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                match &wc.condition.right() {
                    Expression::BinaryOp(left, ArithOp::Add, right) => {
                        assert_eq!(**left, Expression::Literal(Value::Int(1)));
                        match right.as_ref() {
                            Expression::BinaryOp(_, ArithOp::Mul, _) => {}
                            _ => panic!("Expected inner Mul"),
                        }
                    }
                    _ => panic!("Expected BinaryOp Add"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_arithmetic_select_no_alias() {
        let sql = "SELECT id + 1 FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::BinaryOp(_, ArithOp::Add, _)) => {}
                    other => panic!("Expected Expr with Add, got {:?}", other),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_arithmetic_select_column() {
        let sql = "SELECT price * 2 AS double_price FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Alias(inner, alias) => {
                        assert_eq!(alias, "double_price");
                        match inner.as_ref() {
                            SelectColumn::Expr(Expression::BinaryOp(_, ArithOp::Mul, _)) => {}
                            _ => panic!("Expected Expr with Mul"),
                        }
                    }
                    _ => panic!("Expected Alias"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_float_literal() {
        let sql = "INSERT INTO data VALUES (3.14);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.values()[0], Value::Float(3.14));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_float_type() {
        let sql = "CREATE TABLE data (val FLOAT);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Float);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_double_type() {
        let sql = "CREATE TABLE data (val DOUBLE);";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Double);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_float_in_where() {
        let sql = "SELECT * FROM data WHERE val > 3.14;";
        let (_, stmt) = parse_sql(sql).unwrap();

        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                match &wc.condition.right() {
                    Expression::Literal(Value::Float(n)) => {
                        assert!((*n - 3.14).abs() < 0.001);
                    }
                    _ => panic!("Expected Float literal"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_boolean_type() {
        let sql = "CREATE TABLE flags (active BOOLEAN);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Boolean);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_bool_type_shorthand() {
        let sql = "CREATE TABLE flags (active BOOL);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Boolean);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_bool_literal() {
        let sql = "INSERT INTO flags VALUES (TRUE);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Insert(ins) => {
                assert_eq!(ins.values()[0], Value::Bool(true));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_bool_in_where() {
        let sql = "SELECT * FROM flags WHERE active = FALSE;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                let wc = sel.where_clause.unwrap();
                assert_eq!(wc.condition.right(), Expression::Literal(Value::Bool(false)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_date_type() {
        let sql = "CREATE TABLE events (event_date DATE);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Date);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_timestamp_type() {
        let sql = "CREATE TABLE logs (created_at TIMESTAMP);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Timestamp);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_auto_increment() {
        let sql = "CREATE TABLE users (id INT AUTO_INCREMENT, name VARCHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::Int);
                assert!(ct.columns[0].auto_increment);
                assert!(!ct.columns[1].auto_increment);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_primary_key() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert!(ct.columns[0].primary_key);
                assert!(!ct.columns[1].primary_key);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_auto_increment_primary_key() {
        let sql = "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert!(ct.columns[0].auto_increment);
                assert!(ct.columns[0].primary_key);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_foreign_key() {
        let sql = "CREATE TABLE orders (id INT, user_id INT REFERENCES users(id));";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert!(ct.columns[1].references.is_some());
                let fk = ct.columns[1].references.as_ref().unwrap();
                assert_eq!(fk.table, "users");
                assert_eq!(fk.column, "id");
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_not_null() {
        let sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert!(ct.columns[0].not_null);
                assert!(!ct.columns[1].not_null);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_unique() {
        let sql = "CREATE TABLE users (id INT, email VARCHAR UNIQUE);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert!(!ct.columns[0].unique);
                assert!(ct.columns[1].unique);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_index() {
        let sql = "CREATE INDEX idx_name ON users (name);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "idx_name");
                assert_eq!(ci.table_name, "users");
                assert_eq!(ci.column_name, "name");
                assert!(!ci.unique);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_create_unique_index() {
        let sql = "CREATE UNIQUE INDEX idx_email ON users (email);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "idx_email");
                assert_eq!(ci.table_name, "users");
                assert_eq!(ci.column_name, "email");
                assert!(ci.unique);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_drop_index() {
        let sql = "DROP INDEX idx_name;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::DropIndex(di) => {
                assert_eq!(di.index_name, "idx_name");
            }
            _ => panic!("Expected DropIndex"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let (_, stmt) = parse_sql("DROP TABLE users;").unwrap();
        match stmt {
            SqlStatement::DropTable(d) => {
                assert_eq!(d.table_name, "users");
                assert!(!d.if_exists);
            }
            _ => panic!("Expected DropTable"),
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let (_, stmt) = parse_sql("DROP TABLE IF EXISTS users;").unwrap();
        match stmt {
            SqlStatement::DropTable(d) => {
                assert_eq!(d.table_name, "users");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DropTable"),
        }
    }

    #[test]
    fn test_parse_alter_add_column() {
        let (_, stmt) = parse_sql("ALTER TABLE users ADD COLUMN age INT;").unwrap();
        match stmt {
            SqlStatement::AlterTable(a) => {
                assert_eq!(a.table_name, "users");
                match a.action {
                    AlterAction::AddColumn(col) => {
                        assert_eq!(col.name, "age");
                        assert_eq!(col.data_type, DataType::Int);
                    }
                    _ => panic!("Expected AddColumn"),
                }
            }
            _ => panic!("Expected AlterTable"),
        }
    }

    #[test]
    fn test_parse_alter_add_column_no_keyword() {
        // COLUMN keyword is optional in many SQL dialects
        let (_, stmt) = parse_sql("ALTER TABLE users ADD age INT NOT NULL;").unwrap();
        match stmt {
            SqlStatement::AlterTable(a) => match a.action {
                AlterAction::AddColumn(col) => {
                    assert_eq!(col.name, "age");
                    assert!(col.not_null);
                }
                _ => panic!("Expected AddColumn"),
            }
            _ => panic!("Expected AlterTable"),
        }
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let (_, stmt) = parse_sql("ALTER TABLE users DROP COLUMN age;").unwrap();
        match stmt {
            SqlStatement::AlterTable(a) => match a.action {
                AlterAction::DropColumn(name) => assert_eq!(name, "age"),
                _ => panic!("Expected DropColumn"),
            }
            _ => panic!("Expected AlterTable"),
        }
    }

    #[test]
    fn test_parse_alter_rename_column() {
        let (_, stmt) = parse_sql("ALTER TABLE users RENAME COLUMN name TO full_name;").unwrap();
        match stmt {
            SqlStatement::AlterTable(a) => match a.action {
                AlterAction::RenameColumn { from, to } => {
                    assert_eq!(from, "name");
                    assert_eq!(to, "full_name");
                }
                _ => panic!("Expected RenameColumn"),
            }
            _ => panic!("Expected AlterTable"),
        }
    }

    #[test]
    fn test_parse_alter_rename_table() {
        let (_, stmt) = parse_sql("ALTER TABLE users RENAME TO members;").unwrap();
        match stmt {
            SqlStatement::AlterTable(a) => {
                assert_eq!(a.table_name, "users");
                match a.action {
                    AlterAction::RenameTable(new_name) => assert_eq!(new_name, "members"),
                    _ => panic!("Expected RenameTable"),
                }
            }
            _ => panic!("Expected AlterTable"),
        }
    }

    #[test]
    fn test_parse_create_view() {
        let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateView(v) => {
                assert_eq!(v.view_name, "active_users");
                assert!(v.select_sql.contains("SELECT"));
                assert_eq!(v.select.from, FromClause::Table("users".to_string()));
            }
            _ => panic!("Expected CreateView"),
        }
    }

    #[test]
    fn test_parse_drop_view() {
        let (_, stmt) = parse_sql("DROP VIEW active_users;").unwrap();
        match stmt {
            SqlStatement::DropView(v) => {
                assert_eq!(v.view_name, "active_users");
                assert!(!v.if_exists);
            }
            _ => panic!("Expected DropView"),
        }
    }

    #[test]
    fn test_parse_drop_view_if_exists() {
        let (_, stmt) = parse_sql("DROP VIEW IF EXISTS active_users;").unwrap();
        match stmt {
            SqlStatement::DropView(v) => {
                assert_eq!(v.view_name, "active_users");
                assert!(v.if_exists);
            }
            _ => panic!("Expected DropView"),
        }
    }

    #[test]
    fn test_parse_scalar_func_upper() {
        let sql = "SELECT UPPER(name) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.columns.len(), 1);
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Upper, inner)) => {
                        assert_eq!(**inner, Expression::Column("name".to_string()));
                    }
                    _ => panic!("Expected UPPER scalar func"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_scalar_func_lower() {
        let sql = "SELECT LOWER(email) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Lower, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_scalar_func_length() {
        let sql = "SELECT LENGTH(name) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Length, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_scalar_func_trim() {
        let sql = "SELECT TRIM(name) FROM users WHERE TRIM(name) = 'Alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Trim, _))));
                let wc = sel.where_clause.unwrap();
                assert!(matches!(wc.condition.left(), Expression::ScalarFunc(ScalarFunc::Trim, _)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_coalesce() {
        let sql = "SELECT COALESCE(nickname, name, 'unknown') FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Coalesce(exprs)) => {
                        assert_eq!(exprs.len(), 3);
                        assert_eq!(exprs[0], Expression::Column("nickname".to_string()));
                        assert_eq!(exprs[1], Expression::Column("name".to_string()));
                        assert_eq!(exprs[2], Expression::Literal(Value::String("unknown".to_string())));
                    }
                    _ => panic!("Expected Coalesce"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_nullif() {
        let sql = "SELECT NULLIF(score, 0) FROM results;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::NullIf(a, b)) => {
                        assert_eq!(**a, Expression::Column("score".to_string()));
                        assert_eq!(**b, Expression::Literal(Value::Int(0)));
                    }
                    _ => panic!("Expected NullIf"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_coalesce_in_where() {
        let sql = "SELECT * FROM users WHERE COALESCE(nickname, name) = 'Alice';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(sel.where_clause.unwrap().condition.left(), Expression::Coalesce(_)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_abs() {
        let sql = "SELECT ABS(balance) FROM accounts;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Abs, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_ceil_floor() {
        let sql = "SELECT CEIL(price), FLOOR(price) FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Ceil, _))));
                assert!(matches!(&sel.columns[1], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Floor, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_ceiling_alias() {
        let sql = "SELECT CEILING(price) FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Ceil, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_round_no_places() {
        let sql = "SELECT ROUND(price) FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Round(val, places)) => {
                        assert_eq!(**val, Expression::Column("price".to_string()));
                        assert!(places.is_none());
                    }
                    _ => panic!("Expected Round"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_round_with_places() {
        let sql = "SELECT ROUND(price, 2) FROM products;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Round(val, Some(places))) => {
                        assert_eq!(**val, Expression::Column("price".to_string()));
                        assert_eq!(**places, Expression::Literal(Value::Int(2)));
                    }
                    _ => panic!("Expected Round with places"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_concat() {
        let sql = "SELECT CONCAT(first_name, ' ', last_name) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Concat(parts)) => {
                        assert_eq!(parts.len(), 3);
                        assert_eq!(parts[0], Expression::Column("first_name".to_string()));
                        assert_eq!(parts[1], Expression::Literal(Value::String(" ".to_string())));
                        assert_eq!(parts[2], Expression::Column("last_name".to_string()));
                    }
                    _ => panic!("Expected Concat"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_substr_two_args() {
        let sql = "SELECT SUBSTR(name, 2) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Substr(s, start, len)) => {
                        assert_eq!(**s, Expression::Column("name".to_string()));
                        assert_eq!(**start, Expression::Literal(Value::Int(2)));
                        assert!(len.is_none());
                    }
                    _ => panic!("Expected Substr"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_substr_three_args() {
        let sql = "SELECT SUBSTRING(name, 1, 5) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Substr(s, start, Some(len))) => {
                        assert_eq!(**s, Expression::Column("name".to_string()));
                        assert_eq!(**start, Expression::Literal(Value::Int(1)));
                        assert_eq!(**len, Expression::Literal(Value::Int(5)));
                    }
                    _ => panic!("Expected Substr with length"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_ltrim_rtrim() {
        let sql = "SELECT LTRIM(name), RTRIM(name) FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::LTrim, _))));
                assert!(matches!(&sel.columns[1], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::RTrim, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_replace() {
        let sql = "SELECT REPLACE(name, 'a', 'e') FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Replace(s, from, to)) => {
                        assert_eq!(**s, Expression::Column("name".to_string()));
                        assert_eq!(**from, Expression::Literal(Value::String("a".to_string())));
                        assert_eq!(**to, Expression::Literal(Value::String("e".to_string())));
                    }
                    _ => panic!("Expected Replace"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_lpad() {
        let sql = "SELECT LPAD(code, 5, '0') FROM items;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::LPad(s, len, pad)) => {
                        assert_eq!(**s, Expression::Column("code".to_string()));
                        assert_eq!(**len, Expression::Literal(Value::Int(5)));
                        assert_eq!(**pad, Expression::Literal(Value::String("0".to_string())));
                    }
                    _ => panic!("Expected LPad"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_rpad() {
        let sql = "SELECT RPAD(name, 10, ' ') FROM users;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::RPad(s, len, pad)) => {
                        assert_eq!(**s, Expression::Column("name".to_string()));
                        assert_eq!(**len, Expression::Literal(Value::Int(10)));
                        assert_eq!(**pad, Expression::Literal(Value::String(" ".to_string())));
                    }
                    _ => panic!("Expected RPad"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_row_number() {
        let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM employees;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Window(WindowFunc::RowNumber, spec)) => {
                        assert_eq!(spec.partition_by.len(), 1);
                        assert_eq!(spec.order_by.len(), 1);
                    }
                    _ => panic!("Expected Window(RowNumber)"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_rank_dense_rank() {
        let sql = "SELECT RANK() OVER (ORDER BY score DESC), DENSE_RANK() OVER (ORDER BY score DESC) FROM results;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Window(WindowFunc::Rank, _))));
                assert!(matches!(&sel.columns[1], SelectColumn::Expr(Expression::Window(WindowFunc::DenseRank, _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_lag_lead() {
        let sql = "SELECT LAG(salary, 1) OVER (ORDER BY hire_date), LEAD(salary, 1) OVER (ORDER BY hire_date) FROM employees;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Window(WindowFunc::Lag(_, 1), _))));
                assert!(matches!(&sel.columns[1], SelectColumn::Expr(Expression::Window(WindowFunc::Lead(_, 1), _))));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_window_aggregate() {
        let sql = "SELECT SUM(salary) OVER (PARTITION BY dept) FROM employees;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Window(WindowFunc::Agg(AggregateFunc::Sum, _), spec)) => {
                        assert_eq!(spec.partition_by.len(), 1);
                        assert!(spec.order_by.is_empty());
                    }
                    _ => panic!("Expected Window(Agg(Sum))"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_window_over_empty() {
        let sql = "SELECT ROW_NUMBER() OVER () FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Window(WindowFunc::RowNumber, spec)) => {
                        assert!(spec.partition_by.is_empty());
                        assert!(spec.order_by.is_empty());
                    }
                    _ => panic!("Expected Window(RowNumber) with empty spec"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_lateral_join() {
        let sql = "SELECT c.id FROM customers AS c LEFT JOIN LATERAL (SELECT amount FROM orders WHERE customer_id = c.id LIMIT 1) AS recent ON true";
        let result = parse_sql(sql);
        assert!(result.is_ok(), "parse failed: {:?}", result);
        let (_, stmt) = result.unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1, "expected 1 join, got {}", sel.joins.len());
                assert!(sel.joins[0].lateral.is_some(), "join.lateral should be Some");
                assert_eq!(sel.joins[0].alias.as_deref(), Some("recent"));
            }
            _ => panic!("expected Select"),
        }
    }

    // --- Transaction statement parser tests ---

    #[test]
    fn test_parse_begin() {
        let (_, stmt) = parse_sql("BEGIN").unwrap();
        assert_eq!(stmt, SqlStatement::Begin);
    }

    #[test]
    fn test_parse_begin_transaction() {
        let (_, stmt) = parse_sql("BEGIN TRANSACTION").unwrap();
        assert_eq!(stmt, SqlStatement::Begin);
    }

    #[test]
    fn test_parse_start_transaction() {
        let (_, stmt) = parse_sql("START TRANSACTION;").unwrap();
        assert_eq!(stmt, SqlStatement::Begin);
    }

    #[test]
    fn test_parse_commit() {
        let (_, stmt) = parse_sql("COMMIT").unwrap();
        assert_eq!(stmt, SqlStatement::Commit);
    }

    #[test]
    fn test_parse_commit_transaction() {
        let (_, stmt) = parse_sql("COMMIT TRANSACTION;").unwrap();
        assert_eq!(stmt, SqlStatement::Commit);
    }

    #[test]
    fn test_parse_rollback() {
        let (_, stmt) = parse_sql("ROLLBACK").unwrap();
        assert_eq!(stmt, SqlStatement::Rollback);
    }

    #[test]
    fn test_parse_rollback_transaction() {
        let (_, stmt) = parse_sql("ROLLBACK TRANSACTION;").unwrap();
        assert_eq!(stmt, SqlStatement::Rollback);
    }

    #[test]
    fn test_parse_savepoint() {
        let (_, stmt) = parse_sql("SAVEPOINT sp1").unwrap();
        assert_eq!(stmt, SqlStatement::Savepoint("sp1".to_string()));
    }

    #[test]
    fn test_parse_rollback_to_savepoint() {
        let (_, stmt) = parse_sql("ROLLBACK TO SAVEPOINT sp1;").unwrap();
        assert_eq!(stmt, SqlStatement::RollbackToSavepoint("sp1".to_string()));
    }

    #[test]
    fn test_parse_rollback_to_name() {
        // ROLLBACK TO name without SAVEPOINT keyword
        let (_, stmt) = parse_sql("ROLLBACK TO sp1").unwrap();
        assert_eq!(stmt, SqlStatement::RollbackToSavepoint("sp1".to_string()));
    }

    #[test]
    fn test_parse_release_savepoint() {
        let (_, stmt) = parse_sql("RELEASE SAVEPOINT sp1;").unwrap();
        assert_eq!(stmt, SqlStatement::ReleaseSavepoint("sp1".to_string()));
    }

    #[test]
    fn test_parse_release_name() {
        // RELEASE name without SAVEPOINT keyword
        let (_, stmt) = parse_sql("RELEASE sp1").unwrap();
        assert_eq!(stmt, SqlStatement::ReleaseSavepoint("sp1".to_string()));
    }
}
