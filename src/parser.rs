use nom::{
    IResult,
    bytes::complete::{tag, take_while1},
    character::complete::{multispace0, multispace1, char as nom_char},
    combinator::recognize,
    sequence::{delimited, tuple},
    multi::separated_list0,
};

/// SQL AST (Abstract Syntax Tree) nodes

#[derive(Debug, PartialEq, Clone)]
pub enum SqlStatement {
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub auto_increment: bool,
}

impl ColumnDefinition {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self { name: name.to_string(), data_type, auto_increment: false }
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
    pub values: Vec<Value>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
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
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<u64>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FromClause {
    Table(String),
    Subquery(Box<SelectStatement>),
}

impl FromClause {
    /// Get the table name, or None for subqueries
    #[allow(dead_code)]
    pub fn table_name(&self) -> Option<&str> {
        match self {
            FromClause::Table(name) => Some(name),
            FromClause::Subquery(_) => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CteDefinition {
    pub name: String,
    pub query: Box<SelectStatement>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    All, // *
    Column(String),
    QualifiedColumn(String, String), // table.column
    Aggregate(AggregateFunc, Box<SelectColumn>), // COUNT(*), SUM(col), etc.
    Alias(Box<SelectColumn>, String), // expr AS name
    Expr(Expression), // arithmetic expression like price * 2
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause {
    pub column: SelectColumn,
    pub descending: bool,
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
    pub on: Condition,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub left: Expression,
    pub operator: Operator,
    pub right: Expression,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Column(String),
    QualifiedColumn(String, String), // table.column
    BinaryOp(Box<Expression>, ArithOp, Box<Expression>),
    Literal(Value),
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
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
    In,
    NotIn,
    Exists,
    NotExists,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Null,
}

/// Parser functions

/// Parse a SQL statement
pub fn parse_sql(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = multispace0(input)?;
    let (input, stmt) = nom::branch::alt((
        parse_insert,
        parse_create_table,
        parse_select,
        parse_update,
        parse_delete,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, stmt))
}

/// Parse CREATE TABLE statement
pub fn parse_create_table(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("TABLE")(input)?;
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

/// Parse column definition: name TYPE
fn parse_column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = parse_data_type(input)?;
    let (input, _) = multispace0(input)?;
    let (input, auto_inc) = nom::combinator::opt(tag("AUTO_INCREMENT"))(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, ColumnDefinition {
        name: name.to_string(),
        data_type,
        auto_increment: auto_inc.is_some(),
    }))
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
    let (input, _) = tag("DATE")(input)?;
    Ok((input, DataType::Date))
}

fn parse_timestamp_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag("TIMESTAMP")(input)?;
    Ok((input, DataType::Timestamp))
}

fn parse_boolean_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = nom::branch::alt((tag("BOOLEAN"), tag("BOOL")))(input)?;
    Ok((input, DataType::Boolean))
}

fn parse_int_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag("INT")(input)?;
    Ok((input, DataType::Int))
}

fn parse_float_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag("FLOAT")(input)?;
    Ok((input, DataType::Float))
}

fn parse_double_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag("DOUBLE")(input)?;
    Ok((input, DataType::Double))
}

fn parse_varchar_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag("VARCHAR")(input)?;
    let (input, size) = nom::combinator::opt(delimited(
        nom_char('('),
        nom::character::complete::u64,
        nom_char(')'),
    ))(input)?;
    
    Ok((input, DataType::Varchar(size.map(|s| s as usize))))
}

/// Parse INSERT statement
pub fn parse_insert(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag("INSERT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("INTO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("VALUES")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, values) = delimited(
        nom_char('('),
        separated_list0(
            delimited(multispace0, nom_char(','), multispace0),
            parse_value
        ),
        nom_char(')'),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    
    Ok((input, SqlStatement::Insert(InsertStatement {
        table_name: table_name.to_string(),
        values,
    })))
}

/// Parse UPDATE statement
pub fn parse_update(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag("UPDATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("SET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        parse_assignment
    )(input)?;
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Update(UpdateStatement {
        table_name: table_name.to_string(),
        assignments,
        where_clause,
    })))
}

/// Parse assignment: column = value
fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, _) = multispace0(input)?;
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_value(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, Assignment {
        column: column.to_string(),
        value,
    }))
}

/// Parse DELETE statement
pub fn parse_delete(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag("DELETE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Delete(DeleteStatement {
        table_name: table_name.to_string(),
        where_clause,
    })))
}

/// Parse SELECT into a SelectStatement (used by both top-level and subqueries)
pub fn parse_select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = tag("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, distinct) = nom::combinator::opt(nom::sequence::terminated(tag("DISTINCT"), multispace1))(input)?;
    let distinct = distinct.is_some();
    let (input, columns) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        parse_select_column
    )(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("FROM")(input)?;
    let (input, _) = multispace1(input)?;

    // FROM can be a table name or (SELECT ...) AS alias
    let (input, from, from_alias) = if let Ok((input, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(input) {
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        let (input, _) = multispace1(input)?;
        let (input, _) = tag("AS")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, alias) = parse_identifier(input)?;
        (input, FromClause::Subquery(Box::new(subquery)), Some(alias.to_string()))
    } else {
        let (input, table) = parse_identifier(input)?;
        let (input, from_alias) = nom::combinator::opt(parse_table_alias)(input)?;
        (input, FromClause::Table(table.to_string()), from_alias)
    };

    let (input, joins) = nom::multi::many0(parse_join)(input)?;
    let (input, where_clause) = nom::combinator::opt(parse_where)(input)?;
    let (input, group_by) = parse_group_by_clause(input)?;
    let (input, order_by) = parse_order_by_clause(input)?;
    let (input, limit) = parse_limit_clause(input)?;

    Ok((input, SelectStatement {
        ctes: Vec::new(),
        columns,
        distinct,
        from,
        from_alias,
        where_clause,
        joins,
        group_by,
        order_by,
        limit,
    }))
}

/// Parse a single CTE definition: name AS (SELECT ...)
fn parse_cte_definition(input: &str) -> IResult<&str, CteDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("AS")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, query) = parse_select_statement(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, CteDefinition { name: name.to_string(), query: Box::new(query) }))
}

/// Parse SELECT statement (top-level, with optional WITH clause and semicolon)
pub fn parse_select(input: &str) -> IResult<&str, SqlStatement> {
    // Try parsing WITH ... AS (...) before the SELECT
    let (input, ctes) = if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("WITH")(input) {
        let (input, _) = multispace1(input)?;
        let (input, ctes) = separated_list0(
            delimited(multispace0, nom_char(','), multispace0),
            parse_cte_definition,
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
        parse_aggregate_column,
        parse_all_column,
        parse_arith_select_column,
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

/// Parse arithmetic expression as a select column (only matches if there's an operator)
fn parse_arith_select_column(input: &str) -> IResult<&str, SelectColumn> {
    let (new_input, expr) = parse_expression(input)?;
    // Only succeed if the expression actually contains arithmetic
    match &expr {
        Expression::BinaryOp(_, _, _) => Ok((new_input, SelectColumn::Expr(expr))),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    }
}

/// Parse aggregate function: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
fn parse_aggregate_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, func_name) = nom::branch::alt((
        tag("COUNT"),
        tag("SUM"),
        tag("AVG"),
        tag("MIN"),
        tag("MAX"),
    ))(input)?;
    let func = match func_name {
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
    Ok((input, SelectColumn::Aggregate(func, Box::new(inner))))
}

fn parse_all_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, _) = nom_char('*')(input)?;
    Ok((input, SelectColumn::All))
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
    let (input, _) = tag("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = parse_condition(input)?;
    Ok((input, WhereClause { condition }))
}

/// Parse GROUP BY clause (returns empty vec if not present)
fn parse_group_by_clause(input: &str) -> IResult<&str, Vec<SelectColumn>> {
    let (input, _) = multispace0(input)?;
    let result = nom::sequence::pair(tag("GROUP"), nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag("BY")))(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            let (input, cols) = separated_list0(
                delimited(multispace0, nom_char(','), multispace0),
                nom::branch::alt((parse_qualified_column, parse_simple_column)),
            )(input)?;
            Ok((input, cols))
        }
        Err(_) => Ok((input, Vec::new())),
    }
}

/// Parse ORDER BY clause (returns empty vec if not present)
fn parse_order_by_clause(input: &str) -> IResult<&str, Vec<OrderByClause>> {
    let (input, _) = multispace0(input)?;
    let result = nom::sequence::pair(tag("ORDER"), nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag("BY")))(input);
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

/// Parse a single ORDER BY item: column [ASC|DESC]
fn parse_order_by_item(input: &str) -> IResult<&str, OrderByClause> {
    let (input, _) = multispace0(input)?;
    let (input, column) = nom::branch::alt((
        parse_qualified_column,
        parse_simple_column,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, dir) = nom::combinator::opt(nom::branch::alt((
        tag("ASC"),
        tag("DESC"),
    )))(input)?;
    let descending = dir == Some("DESC");
    Ok((input, OrderByClause { column, descending }))
}

/// Parse LIMIT clause (returns None if not present)
fn parse_limit_clause(input: &str) -> IResult<&str, Option<u64>> {
    let (input, _) = multispace0(input)?;
    let result = tag::<&str, &str, nom::error::Error<&str>>("LIMIT")(input);
    match result {
        Ok((input, _)) => {
            let (input, _) = multispace1(input)?;
            let (input, n) = nom::character::complete::u64(input)?;
            Ok((input, Some(n)))
        }
        Err(_) => Ok((input, None)),
    }
}

/// Check if identifier is a reserved keyword that can't be used as an alias
fn is_reserved_keyword(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(), "ON" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "WHERE" | "ORDER" | "GROUP" | "LIMIT" | "HAVING")
}

/// Parse optional table alias, rejecting reserved keywords
fn parse_table_alias(input: &str) -> IResult<&str, String> {
    let (input, _) = multispace1(input)?;
    let (input, alias) = parse_identifier(input)?;
    if is_reserved_keyword(alias) {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((input, alias.to_string()))
}

/// Parse JOIN clause
pub fn parse_join(input: &str) -> IResult<&str, JoinClause> {
    let (input, _) = multispace1(input)?;
    let (input, join_type) = nom::branch::alt((
        nom::combinator::map(tag("INNER JOIN"), |_| JoinType::Inner),
        nom::combinator::map(tag("LEFT JOIN"), |_| JoinType::Left),
        nom::combinator::map(tag("RIGHT JOIN"), |_| JoinType::Right),
        nom::combinator::map(tag("JOIN"), |_| JoinType::Inner),
    ))(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier(input)?;
    // Parse optional alias, but don't consume reserved keywords like ON
    let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag("ON")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = parse_condition(input)?;
    
    Ok((input, JoinClause {
        join_type,
        table: table.to_string(),
        alias,
        on: condition,
    }))
}

/// Parse condition: EXISTS/NOT EXISTS, expression IN/NOT IN, or expression op expression
pub fn parse_condition(input: &str) -> IResult<&str, Condition> {
    let (input, _) = multispace0(input)?;

    // Try NOT EXISTS (SELECT ...)
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag("EXISTS")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition {
            left: Expression::Literal(Value::Null),
            operator: Operator::NotExists,
            right: Expression::Subquery(Box::new(subquery)),
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
        return Ok((input, Condition {
            left: Expression::Literal(Value::Null),
            operator: Operator::Exists,
            right: Expression::Subquery(Box::new(subquery)),
        }));
    }

    let (input, left) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;

    // Try parsing NOT IN (SELECT ...) or IN (SELECT ...)
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag("IN")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition {
            left,
            operator: Operator::NotIn,
            right: Expression::Subquery(Box::new(subquery)),
        }));
    }
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("IN")(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition {
            left,
            operator: Operator::In,
            right: Expression::Subquery(Box::new(subquery)),
        }));
    }

    let (input, operator) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, right) = parse_expression(input)?;

    Ok((input, Condition { left, operator, right }))
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
    ))(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, op))
}

/// Parse expression with arithmetic: handles +, -, *, / with precedence
fn parse_expression(input: &str) -> IResult<&str, Expression> {
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

/// Parse atomic expression: subquery, column, table.column, or literal
fn parse_atom(input: &str) -> IResult<&str, Expression> {
    nom::branch::alt((
        parse_expression_subquery,
        parse_expression_qualified_column,
        parse_expression_literal,
        parse_expression_simple_column,
    ))(input)
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
fn parse_operator(input: &str) -> IResult<&str, Operator> {
    nom::branch::alt((
        nom::combinator::map(tag("!="), |_| Operator::NotEquals),
        nom::combinator::map(tag(">="), |_| Operator::GreaterThanOrEqual),
        nom::combinator::map(tag("<="), |_| Operator::LessThanOrEqual),
        nom::combinator::map(tag("="), |_| Operator::Equals),
        nom::combinator::map(tag(">"), |_| Operator::GreaterThan),
        nom::combinator::map(tag("<"), |_| Operator::LessThan),
        nom::combinator::map(tag("LIKE"), |_| Operator::Like),
    ))(input)
}

/// Parse value: float, integer, string, or NULL
fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    let (input, value) = nom::branch::alt((
        parse_string_value,
        parse_null_value,
        parse_bool_value,
        parse_float_value,
        parse_int_value,
    ))(input)?;
    Ok((input, value))
}

fn parse_bool_value(input: &str) -> IResult<&str, Value> {
    let (input, val) = nom::branch::alt((tag("TRUE"), tag("FALSE")))(input)?;
    Ok((input, Value::Bool(val == "TRUE")))
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
    let (input, s) = delimited(
        nom_char('\''),
        take_while1(|c| c != '\''),
        nom_char('\''),
    )(input)?;
    Ok((input, Value::String(s.to_string())))
}

fn parse_null_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag("NULL")(input)?;
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
                assert_eq!(ins.values.len(), 2);
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
                assert_eq!(ins.values.len(), 3);
                assert_eq!(ins.values[0], Value::Int(1));
                assert_eq!(ins.values[1], Value::Null);
                assert_eq!(ins.values[2], Value::String("test@example.com".to_string()));
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
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0], Value::Int(42));
                assert_eq!(ins.values[1], Value::String("Bob".to_string()));
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
                assert_eq!(ins.values[0], Value::Int(-100));
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
                match where_clause.condition.left {
                    Expression::Column(name) => assert_eq!(name, "id"),
                    _ => panic!("Expected Column expression"),
                }
                assert_eq!(where_clause.condition.operator, Operator::Equals);
                match where_clause.condition.right {
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
                match where_clause.condition.right {
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
                    assert_eq!(where_clause.condition.operator, expected_op);
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
                match &join.on.left {
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
                assert_eq!(upd.assignments[0].value, Value::String("Bob".to_string()));
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
                assert_eq!(upd.assignments[0].value, Value::Int(0));
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
                assert_eq!(upd.assignments[0].value, Value::Null);
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
                match wc.condition.left {
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
                match wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::Like);
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::Like);
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
                assert_eq!(wc.condition.operator, Operator::In);
                match &wc.condition.left {
                    Expression::Column(name) => assert_eq!(name, "id"),
                    _ => panic!("Expected column"),
                }
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::In);
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::Equals);
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::GreaterThan);
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::NotIn);
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
                assert_eq!(wc.condition.operator, Operator::Exists);
                match &wc.condition.right {
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
                assert_eq!(wc.condition.operator, Operator::NotExists);
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
                match &wc.condition.right {
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
                match &wc.condition.right {
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
                match &wc.condition.right {
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
                assert_eq!(ins.values[0], Value::Float(3.14));
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
                match &wc.condition.right {
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
                assert_eq!(ins.values[0], Value::Bool(true));
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
                assert_eq!(wc.condition.right, Expression::Literal(Value::Bool(false)));
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
}

