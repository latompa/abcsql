use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, char as nom_char},
    sequence::delimited,
    multi::separated_list0,
};

use super::ast::*;
use super::cond::{parse_condition, parse_expression, parse_value, parse_identifier, parse_window_clause, parse_table_name};
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
        parse_create_function_inner,
        parse_create_view_inner,
        parse_create_table_inner,
        parse_create_unique_index_inner,
        parse_create_index_inner,
    ))(input)
}

fn parse_create_function_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("FUNCTION")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    // Parse parameter list: (param1 type1, param2 type2, ...)
    let (input, _) = nom_char('(')(input)?;
    let (input, params) = separated_list0(
        delimited(multispace0, nom_char(','), multispace0),
        |i| {
            let (i, _) = multispace0(i)?;
            let (i, pname) = parse_identifier(i)?;
            let (i, _) = multispace1(i)?;
            let (i, ptype) = parse_identifier(i)?;
            Ok((i, (pname.to_string(), ptype.to_string())))
        },
    )(input)?;
    let (input, _) = nom_char(')')(input)?;
    // Consume whitespace before optional RETURNS / AS
    let (input, _) = multispace0(input)?;
    // Try RETURNS type (if present), then require AS
    let (input, return_type) = if input.trim_start().to_uppercase().starts_with("RETURNS") {
        let (input, _) = tag_no_case("RETURNS")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, rt) = parse_identifier(input)?;
        let (input, _) = multispace1(input)?;
        (input, Some(rt.to_string()))
    } else {
        (input, None)
    };
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, body) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::CreateFunction(CreateFunctionStatement {
        name: name.to_string(),
        params: params.into_iter().map(|(n, t)| (n.to_string(), t.to_string())).collect(),
        return_type: return_type.map(|s| s.to_string()),
        body: Box::new(body),
    })))
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

enum TableItem {
    Col(ColumnDefinition),
    Constraint(TableConstraint),
}

fn parse_create_table_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table_name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, items) = delimited(
        nom_char('('),
        separated_list0(nom_char(','), nom::branch::alt((
            nom::combinator::map(parse_table_constraint, TableItem::Constraint),
            nom::combinator::map(parse_column_definition, TableItem::Col),
        ))),
        nom_char(')'),
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    for item in items {
        match item {
            TableItem::Col(c) => columns.push(c),
            TableItem::Constraint(tc) => constraints.push(tc),
        }
    }

    Ok((input, SqlStatement::CreateTable(CreateTableStatement {
        table_name: table_name.to_string(),
        columns,
        constraints,
    })))
}

/// Parse (col1, col2, ...) — a parenthesized column name list
fn parse_paren_column_list(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0(input)?;
    let (input, cols) = delimited(
        nom_char('('),
        nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            nom::sequence::preceded(multispace0, parse_identifier),
        ),
        nom::sequence::preceded(multispace0, nom_char(')')),
    )(input)?;
    Ok((input, cols.into_iter().map(|s| s.to_string()).collect()))
}

/// Parse a table-level constraint: [CONSTRAINT name] PRIMARY KEY (...) | UNIQUE (...)
/// | FOREIGN KEY (...) REFERENCES t [(...)] | CHECK (...)
pub fn parse_table_constraint(input: &str) -> IResult<&str, TableConstraint> {
    let (input, _) = multispace0(input)?;
    let raw_start = input;
    let (input, name) = nom::combinator::opt(|i| {
        let (i, _) = tag_no_case("CONSTRAINT")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, n) = parse_identifier(i)?;
        let (i, _) = multispace1(i)?;
        Ok((i, n.to_string()))
    })(input)?;

    let (input, kind) = if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("PRIMARY KEY")(input) {
        let (i, cols) = parse_paren_column_list(i)?;
        (i, TableConstraintKind::PrimaryKey(cols))
    } else if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("UNIQUE")(input) {
        let (i, cols) = parse_paren_column_list(i)?;
        (i, TableConstraintKind::Unique(cols))
    } else if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("FOREIGN KEY")(input) {
        let (i, cols) = parse_paren_column_list(i)?;
        let (i, _) = multispace0(i)?;
        let (i, _) = tag_no_case("REFERENCES")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, ref_table) = parse_identifier(i)?;
        let (i, ref_cols) = nom::combinator::opt(parse_paren_column_list)(i)?;
        (i, TableConstraintKind::ForeignKey {
            columns: cols,
            ref_table: ref_table.to_string(),
            ref_columns: ref_cols.unwrap_or_default(),
        })
    } else {
        // CHECK only matches when followed by '(' so a column named "check"
        // still parses as a column definition
        let (i, (cond, _)) = parse_check_constraint(input)?;
        (i, TableConstraintKind::Check(cond))
    };

    let raw = raw_start[..raw_start.len() - input.len()].trim().to_string();
    Ok((input, TableConstraint { name, kind, raw }))
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

/// Parse column definition: name TYPE followed by constraints in any order
fn parse_column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, _) = multispace0(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = parse_data_type(input)?;

    let mut col = ColumnDefinition {
        name: name.to_string(),
        data_type,
        auto_increment: false,
        primary_key: false,
        not_null: false,
        unique: false,
        references: None,
        check_constraint: None,
        check_constraint_text: None,
        default: None,
        default_text: None,
    };

    let mut input = input;
    loop {
        let (rest, _) = multispace0(input)?;
        if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT NULL")(rest) {
            col.not_null = true;
            input = rest;
        } else if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("UNIQUE")(rest) {
            col.unique = true;
            input = rest;
        } else if let Ok((rest, _)) = tag::<&str, &str, nom::error::Error<&str>>("AUTO_INCREMENT")(rest) {
            col.auto_increment = true;
            input = rest;
        } else if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("PRIMARY KEY")(rest) {
            col.primary_key = true;
            input = rest;
        } else if let Ok((rest, fk)) = parse_references(rest) {
            col.references = Some(fk);
            input = rest;
        } else if let Ok((rest, (cond, text))) = parse_check_constraint(rest) {
            col.check_constraint = Some(cond);
            col.check_constraint_text = Some(text);
            input = rest;
        } else if let Ok((rest, (expr, text))) = parse_default_clause(rest) {
            col.default = Some(expr);
            col.default_text = Some(text);
            input = rest;
        } else {
            let (rest, _) = multispace0(input)?;
            return Ok((rest, col));
        }
    }
}

/// Parse DEFAULT expr — returns the parsed expression and its raw SQL text.
fn parse_default_clause(input: &str) -> IResult<&str, (Expression, String)> {
    let (input, _) = tag_no_case("DEFAULT")(input)?;
    let (input, _) = multispace1(input)?;
    let expr_start = input;
    let (input, expr) = parse_expression(input)?;
    let text = expr_start[..expr_start.len() - input.len()].trim().to_string();
    Ok((input, (expr, text)))
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

/// Parse CHECK (condition) — returns the parsed Condition and the raw inner SQL text.
fn parse_check_constraint(input: &str) -> IResult<&str, (Condition, String)> {
    let start = input;
    let (input, _) = tag_no_case("CHECK")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, cond) = parse_condition(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    // Extract the raw inner condition text (the SQL between CHECK and (...))
    let consumed = &start[..start.len() - input.len()];
    // Strip 'CHECK (' prefix and trailing ')'
    let inner = consumed.trim().strip_prefix("CHECK").unwrap_or(consumed).trim();
    let inner = inner.strip_prefix('(').unwrap_or(inner).trim();
    let inner = inner.strip_suffix(')').unwrap_or(inner).trim();
    Ok((input, (cond, inner.to_string())))
}

/// Parse data type: INT, VARCHAR, SMALLINT, BIGINT, TEXT, DECIMAL, UUID, JSON, etc.
fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    nom::branch::alt((
        nom::branch::alt((
            parse_timestamp_type,  // before DATE
            parse_double_type,     // before generic identifiers
            parse_smallint_type,   // before INT
            parse_bigint_type,     // before INT
            parse_integer_type,    // INTEGER alias for INT
            parse_int_type,
            parse_float_type,
            parse_real_type,
        )),
        nom::branch::alt((
            parse_boolean_type,
            parse_date_type,
            parse_varchar_type,
            parse_char_type,
            parse_text_type,
            parse_numeric_type,    // NUMERIC before anything shorter
            parse_decimal_type,
            parse_uuid_type,
            parse_jsonb_type,      // JSONB before JSON
            parse_json_type,
        )),
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

fn parse_smallint_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("SMALLINT")(input)?;
    Ok((input, DataType::SmallInt))
}

fn parse_bigint_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("BIGINT")(input)?;
    Ok((input, DataType::BigInt))
}

fn parse_integer_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("INTEGER")(input)?;
    Ok((input, DataType::Int))
}

fn parse_int_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("INT")(input)?;
    Ok((input, DataType::Int))
}

fn parse_float_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("FLOAT")(input)?;
    Ok((input, DataType::Float))
}

fn parse_real_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("REAL")(input)?;
    Ok((input, DataType::Real))
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

fn parse_char_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("CHAR")(input)?;
    let (input, size) = nom::combinator::opt(delimited(
        nom_char('('),
        nom::character::complete::u64,
        nom_char(')'),
    ))(input)?;
    Ok((input, DataType::Char(size.map(|s| s as usize))))
}

fn parse_text_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("TEXT")(input)?;
    Ok((input, DataType::Text))
}

/// Parse DECIMAL(p, s) / DECIMAL(p) / DECIMAL or NUMERIC variants
fn parse_decimal_prec(input: &str) -> IResult<&str, DataType> {
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, p) = nom::character::complete::u8(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = nom::combinator::opt(nom::sequence::preceded(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        nom::character::complete::u8,
    ))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, DataType::Decimal(Some(p), s)))
}

fn parse_decimal_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("DECIMAL")(input)?;
    if let Ok((rest, dt)) = parse_decimal_prec(input) {
        return Ok((rest, dt));
    }
    Ok((input, DataType::Decimal(None, None)))
}

fn parse_numeric_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("NUMERIC")(input)?;
    if let Ok((rest, dt)) = parse_decimal_prec(input) {
        return Ok((rest, dt));
    }
    Ok((input, DataType::Decimal(None, None)))
}

fn parse_uuid_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("UUID")(input)?;
    Ok((input, DataType::Uuid))
}

fn parse_jsonb_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("JSONB")(input)?;
    Ok((input, DataType::Jsonb))
}

fn parse_json_type(input: &str) -> IResult<&str, DataType> {
    let (input, _) = tag_no_case("JSON")(input)?;
    Ok((input, DataType::Json))
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

    // INSERT INTO t DEFAULT VALUES
    if let Ok((i2, _)) = nom::sequence::separated_pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("DEFAULT"),
        multispace1,
        tag_no_case("VALUES"),
    )(input) {
        let (i2, on_conflict) = parse_on_conflict(i2)?;
        let (i2, returning) = parse_returning(i2)?;
        let (i2, _) = multispace0(i2)?;
        let (i2, _) = nom::combinator::opt(nom_char(';'))(i2)?;
        return Ok((i2, SqlStatement::Insert(InsertStatement {
            table_name: table_name.to_string(),
            columns,
            source: InsertSource::DefaultValues,
            on_conflict,
            returning,
        })));
    }

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
                    nom::branch::alt((parse_default_marker, parse_value)),
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
        // Optional conflict column list
        let (input, conflict_columns) = nom::combinator::opt(|input| {
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

/// Parse the bare DEFAULT keyword (not part of a longer identifier) as a marker value.
fn parse_default_marker(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("DEFAULT")(input)?;
    // Reject if DEFAULT is a prefix of a longer identifier
    if input.chars().next().is_some_and(|c| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((input, Value::Default))
}

/// Parse assignment: column = expression (or the DEFAULT keyword)
pub fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, _) = multispace0(input)?;
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = nom::branch::alt((
        nom::combinator::map(parse_default_marker, |v| Expression::Literal(v)),
        parse_expression,
    ))(input)?;
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

    // Parse WHEN MATCHED / WHEN NOT MATCHED clauses
    let (input, when_clauses) = parse_merge_when_clauses(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;

    Ok((input, SqlStatement::Merge(MergeStatement {
        target: target.to_string(),
        target_alias,
        source,
        source_alias,
        on,
        when_clauses,
    })))
}

/// Parse all WHEN MATCHED / WHEN NOT MATCHED clauses for MERGE
fn parse_merge_when_clauses(input: &str) -> IResult<&str, Vec<WhenClause>> {
    let mut input = input;
    let mut clauses = Vec::new();

    loop {
        let trimmed = input.trim_start();
        if !trimmed.to_uppercase().starts_with("WHEN") {
            break;
        }
        match parse_merge_when_clause(trimmed) {
            Ok((rest, clause)) => {
                clauses.push(clause);
                input = rest;
            }
            Err(_) => break,
        }
    }
    Ok((input, clauses))
}

/// Parse a single WHEN clause
fn parse_merge_when_clause(input: &str) -> IResult<&str, WhenClause> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("WHEN")(input)?;
    let (input, _) = multispace1(input)?;
    // Check for NOT MATCHED
    let (input, is_not) = nom::combinator::opt(nom::sequence::terminated(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        multispace1,
    ))(input)?;
    let (input, _) = tag_no_case("MATCHED")(input)?;
    let is_matched = is_not.is_none();

    // Parse optional AND <condition>
    let (input, condition) = nom::combinator::opt(nom::sequence::pair(
        nom::sequence::preceded(multispace1, tag_no_case("AND")),
        nom::sequence::preceded(multispace0, parse_condition),
    ))(input).map(|(i, opt)| {
        (i, opt.map(|(_, cond)| cond))
    })?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("THEN")(input)?;
    let (input, _) = multispace1(input)?;

    // Parse the action: UPDATE SET ..., DELETE, INSERT ..., or DO NOTHING
    let (input, action) = if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("UPDATE")(input) {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = multispace1(input)?;
        let (input, assignments) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_assignment,
        )(input)?;
        (input, MergeAction::Update(assignments))
    } else if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("DELETE")(input) {
        (input, MergeAction::Delete)
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
        (input, MergeAction::Insert(cols.unwrap_or_default(), exprs))
    } else {
        // DO NOTHING (if someone writes it)
        let (input, _) = nom::combinator::opt(nom::sequence::pair(
            tag_no_case::<&str, &str, nom::error::Error<&str>>("DO"),
            nom::sequence::preceded(multispace1, tag_no_case("NOTHING")),
        ))(input)?;
        (input, MergeAction::DoNothing)
    };

    Ok((input, WhenClause { is_matched, condition, action }))
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
    nom::branch::alt((parse_drop_function_inner, parse_drop_view_inner, parse_drop_index_inner, parse_drop_table_inner))(input)
}

fn parse_drop_function_inner(input: &str) -> IResult<&str, SqlStatement> {
    let (input, _) = tag_no_case("FUNCTION")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, if_exists) = nom::combinator::opt(
        nom::sequence::terminated(tag_no_case("IF EXISTS"), multispace1)
    )(input)?;
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom::combinator::opt(nom_char(';'))(input)?;
    Ok((input, SqlStatement::DropFunction(DropFunctionStatement {
        name: name.to_string(),
        if_exists: if_exists.is_some(),
    })))
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
    let (input, tbl) = parse_table_name(input)?;
    let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
    Ok((input, (tbl, alias)))
}

/// Parse SELECT into a SelectStatement (used by both top-level and subqueries)
pub fn parse_select_statement(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _all) = nom::combinator::opt(nom::sequence::terminated(tag_no_case("ALL"), multispace1))(input)?;
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
            let (input, table) = parse_table_name(input)?;
            let (input, from_alias) = nom::combinator::opt(parse_table_alias)(input)?;
            (input, FromClause::Table(table), from_alias)
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

    // Parse FOR UPDATE [NOWAIT | SKIP LOCKED]
    let (input, for_update) = {
        let saved = input;
        match nom::combinator::opt::<_, _, nom::error::Error<&str>, _>(nom::sequence::preceded(
            multispace1,
            tag_no_case("FOR UPDATE"),
        ))(input) {
            Ok((after_fu, Some(_))) => {
                // Consume optional NOWAIT or SKIP LOCKED (accepted but not enforced)
                let (after_fu, _) = nom::combinator::opt::<_, _, nom::error::Error<&str>, _>(nom::sequence::preceded(
                    multispace1,
                    nom::branch::alt((
                        tag_no_case("NOWAIT"),
                        tag_no_case("SKIP LOCKED"),
                    )),
                ))(after_fu).unwrap_or((after_fu, None));
                (after_fu, true)
            }
            Ok((_, None)) => (saved, false),
            Err(_) => (saved, false),
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
        for_update,
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
        | Expression::DateDiff(_, _, _) | Expression::DateAdd(_, _, _)
        | Expression::JsonTypeOf(_) | Expression::JsonArrayLength(_)
        | Expression::JsonBuildObject(_) | Expression::JsonBuildArray(_) => Ok((new_input, SelectColumn::Expr(expr))),
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

pub(crate) fn parse_all_column(input: &str) -> IResult<&str, SelectColumn> {
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

pub(crate) fn parse_qualified_column(input: &str) -> IResult<&str, SelectColumn> {
    let (input, table) = parse_identifier(input)?;
    let (input, _) = nom_char('.')(input)?;
    let (input, column) = parse_identifier(input)?;
    Ok((input, SelectColumn::QualifiedColumn(
        table.to_string(),
        column.to_string(),
    )))
}

pub(crate) fn parse_simple_column(input: &str) -> IResult<&str, SelectColumn> {
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
            // Regular GROUP BY (with ordinal support)
            let (input, cols) = separated_list0(
                delimited(multispace0, nom_char(','), multispace0),
                nom::branch::alt((
                    nom::combinator::map(
                        nom::character::complete::u64,
                        |n| SelectColumn::Expr(Expression::Literal(Value::Int(n as i64))),
                    ),
                    parse_qualified_column,
                    parse_simple_column,
                )),
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
pub(crate) fn parse_order_by_item(input: &str) -> IResult<&str, OrderByClause> {
    let (input, _) = multispace0(input)?;
    let (input, column) = {
        // Try expression first, then fall back to simple/qualified column
        if let Ok((rest, expr)) = parse_expression(input) {
            match expr {
                Expression::Column(name) => (rest, SelectColumn::Column(name)),
                Expression::QualifiedColumn(table, col) => (rest, SelectColumn::QualifiedColumn(table, col)),
                other => (rest, SelectColumn::Expr(other)),
            }
        } else {
            nom::branch::alt((
                parse_qualified_column,
                parse_simple_column,
            ))(input)?
        }
    };
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
    // Try LIMIT n [OFFSET m] first
    let limit_res = tag_no_case::<&str, &str, nom::error::Error<&str>>("LIMIT")(input);
    if let Ok((after_limit, _)) = limit_res {
        let (after_limit, _) = multispace1(after_limit)?;
        let (after_limit, n) = nom::character::complete::u64(after_limit)?;
        let (after_limit, _) = multispace0(after_limit)?;
        let offset_res = tag_no_case::<&str, &str, nom::error::Error<&str>>("OFFSET")(after_limit);
        return match offset_res {
            Ok((after_offset, _)) => {
                let (after_offset, _) = multispace1(after_offset)?;
                let (after_offset, off) = nom::character::complete::u64(after_offset)?;
                Ok((after_offset, (Some(n), Some(off))))
            }
            Err(_) => Ok((after_limit, (Some(n), None))),
        };
    }
    // Try OFFSET m [LIMIT n]
    let offset_res = tag_no_case::<&str, &str, nom::error::Error<&str>>("OFFSET")(input);
    if let Ok((after_offset, _)) = offset_res {
        let (after_offset, _) = multispace1(after_offset)?;
        let (after_offset, off) = nom::character::complete::u64(after_offset)?;
        let (after_offset, _) = multispace0(after_offset)?;
        let limit_res = tag_no_case::<&str, &str, nom::error::Error<&str>>("LIMIT")(after_offset);
        return match limit_res {
            Ok((after_limit, _)) => {
                let (after_limit, _) = multispace1(after_limit)?;
                let (after_limit, n) = nom::character::complete::u64(after_limit)?;
                Ok((after_limit, (Some(n), Some(off))))
            }
            Err(_) => Ok((after_offset, (None, Some(off)))),
        };
    }
    Ok((input, (None, None)))
}

/// Check if identifier is a reserved keyword that can't be used as an alias
pub(crate) fn is_reserved_keyword(s: &str) -> bool {
    matches!(s.to_uppercase().as_str(), "ON" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS" | "WHERE" | "ORDER" | "GROUP" | "LIMIT" | "OFFSET" | "HAVING" | "UNION" | "ALL" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" | "AND" | "OR" | "NOT" | "AS" | "VIEW" | "OVER" | "PARTITION" | "NULLS" | "FIRST" | "LAST" | "INTERSECT" | "EXCEPT" | "ROWS" | "RANGE" | "GROUPS" | "PRECEDING" | "FOLLOWING" | "UNBOUNDED" | "BETWEEN" | "USING" | "NATURAL" | "ANY" | "SOME" | "FILTER" | "RECURSIVE" | "CURRENT_DATE" | "CURRENT_TIMESTAMP" | "EXTRACT" | "INTERVAL" | "DATEDIFF" | "DATE_TRUNC" | "DATE_PART" | "DATEADD" | "WINDOW" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" | "ROLLUP" | "CUBE" | "GROUPING" | "SETS" | "LATERAL" | "VALUES" | "RETURNING" | "CONFLICT" | "EXCLUDED" | "NOTHING" | "MERGE" | "MATCHED" | "TRUNCATE" | "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "START" | "TRANSACTION" | "DISTINCT" | "UNIQUE" | "SIMILAR" | "OVERLAPS" | "ESCAPE")
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
        let (input, table) = parse_table_name(input)?;
        let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;
        return Ok((input, JoinClause {
            join_type: JoinType::Natural,
            table,
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

    let (input, table) = parse_table_name(input)?;
    let (input, alias) = nom::combinator::opt(parse_table_alias)(input)?;

    // CROSS JOIN has no ON/USING clause
    if join_type == JoinType::Cross {
        return Ok((input, JoinClause { join_type, table, alias, on: None, using: None, lateral: None }));
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
            table,
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

    Ok((input, JoinClause { join_type, table, alias, on: Some(condition), using: None, lateral: None }))
}

