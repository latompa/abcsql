use nom::{
    IResult,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1, char as nom_char},
    combinator::recognize,
    sequence::tuple,
};

use super::ast::*;
use super::stmt::{parse_select_statement, parse_order_by_item, parse_all_column, parse_qualified_column, parse_simple_column, is_reserved_keyword};
use super::datetime::{parse_date_str, parse_timestamp_str, interval_unit_secs};
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

/// Parse a parenthesized row of >= 2 expressions: (expr, expr, ...)
fn parse_row_constructor(input: &str) -> IResult<&str, Vec<Expression>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    if exprs.len() < 2 {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }
    Ok((input, exprs))
}

/// Build the AND chain of pairwise equalities for row equality
fn row_equality(left: &[Expression], right: &[Expression]) -> Condition {
    let mut iter = left.iter().zip(right.iter()).map(|(l, r)| Condition::Comparison {
        left: l.clone(),
        operator: Operator::Equals,
        right: r.clone(),
        upper_bound: None,
    });
    let first = iter.next().expect("row constructors are non-empty");
    iter.fold(first, |acc, c| Condition::And(Box::new(acc), Box::new(c)))
}

/// Build the lexicographic ordering condition for row comparisons.
/// (a, b) < (x, y)  =>  a < x OR (a = x AND b < y)
fn row_lexicographic(left: &[Expression], right: &[Expression], op: &Operator, or_equal: bool) -> Condition {
    let strict = Condition::Comparison {
        left: left[0].clone(),
        operator: op.clone(),
        right: right[0].clone(),
        upper_bound: None,
    };
    if left.len() == 1 {
        if or_equal {
            let eq = Condition::Comparison {
                left: left[0].clone(),
                operator: Operator::Equals,
                right: right[0].clone(),
                upper_bound: None,
            };
            return Condition::Or(Box::new(strict), Box::new(eq));
        }
        return strict;
    }
    let head_eq = Condition::Comparison {
        left: left[0].clone(),
        operator: Operator::Equals,
        right: right[0].clone(),
        upper_bound: None,
    };
    let tail = row_lexicographic(&left[1..], &right[1..], op, or_equal);
    Condition::Or(
        Box::new(strict),
        Box::new(Condition::And(Box::new(head_eq), Box::new(tail))),
    )
}

/// Parse row-constructor comparisons and IN lists, desugaring to column-wise conditions
fn parse_row_comparison(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_row_constructor(input)?;
    let (input, _) = multispace0(input)?;

    // (a, b) [NOT] IN ((1, 2), (3, 4))
    let (input, negated) = match tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT")(input) {
        Ok((i, _)) => {
            let (i, _) = multispace1(i)?;
            (i, true)
        }
        Err(_) => (input, false),
    };
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("IN")(input) {
        let (i, _) = multispace0(i)?;
        let (i, _) = nom_char('(')(i)?;
        let (i, rows) = nom::multi::separated_list1(
            nom::sequence::delimited(multispace0, nom_char(','), multispace0),
            parse_row_constructor,
        )(i)?;
        let (i, _) = multispace0(i)?;
        let (i, _) = nom_char(')')(i)?;
        for row in &rows {
            if row.len() != left.len() {
                return Err(nom::Err::Failure(nom::error::Error::new(i, nom::error::ErrorKind::Verify)));
            }
        }
        let mut iter = rows.iter().map(|row| row_equality(&left, row));
        let first = iter.next().expect("separated_list1 yields at least one row");
        let cond = iter.fold(first, |acc, c| Condition::Or(Box::new(acc), Box::new(c)));
        let cond = if negated { Condition::Not(Box::new(cond)) } else { cond };
        return Ok((i, cond));
    }
    if negated {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }

    // (a, b) <op> (x, y)
    let (input, op_str) = nom::branch::alt((
        tag("<>"), tag("!="), tag("<="), tag(">="), tag("="), tag("<"), tag(">"),
    ))(input)?;
    let (input, right) = parse_row_constructor(input)?;
    if right.len() != left.len() {
        return Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }
    let cond = match op_str {
        "=" => row_equality(&left, &right),
        "<>" | "!=" => Condition::Not(Box::new(row_equality(&left, &right))),
        "<" => row_lexicographic(&left, &right, &Operator::LessThan, false),
        ">" => row_lexicographic(&left, &right, &Operator::GreaterThan, false),
        "<=" => row_lexicographic(&left, &right, &Operator::LessThan, true),
        ">=" => row_lexicographic(&left, &right, &Operator::GreaterThan, true),
        _ => unreachable!(),
    };
    Ok((input, cond))
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

    // Try OVERLAPS predicate: (start1, end1) OVERLAPS (start2, end2)
    // Must be tried before generic parenthesized sub-condition since both start with '('
    if let Ok((input, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(input) {
        let (input, _) = multispace0(input)?;
        if let Ok((after_a, a)) = parse_expression(input) {
            let after_a = after_a.trim_start();
            if let Ok((after_a, _)) = nom_char::<&str, nom::error::Error<&str>>(',')(after_a) {
                let after_a = after_a.trim_start();
                if let Ok((after_b, b)) = parse_expression(after_a) {
                    let after_b = after_b.trim_start();
                    if let Ok((after_close, _)) = nom_char::<&str, nom::error::Error<&str>>(')')(after_b) {
                        let after_close = after_close.trim_start();
                        if let Ok((after_overlaps, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("OVERLAPS")(after_close) {
                            let after_overlaps = after_overlaps.trim_start();
                            if let Ok((after_open2, _)) = nom_char::<&str, nom::error::Error<&str>>('(')(after_overlaps) {
                                let after_open2 = after_open2.trim_start();
                                if let Ok((after_c, c)) = parse_expression(after_open2) {
                                    let after_c = after_c.trim_start();
                                    if let Ok((after_c, _)) = nom_char::<&str, nom::error::Error<&str>>(',')(after_c) {
                                        let after_c = after_c.trim_start();
                                        if let Ok((after_d, d)) = parse_expression(after_c) {
                                            let after_d = after_d.trim_start();
                                            if let Ok((after_close2, _)) = nom_char::<&str, nom::error::Error<&str>>(')')(after_d) {
                                                return Ok((after_close2, Condition::Overlaps(
                                                    Box::new(a), Box::new(b), Box::new(c), Box::new(d),
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Row value constructor comparisons: (a, b) = (1, 2), (a, b) < (x, y),
    // (a, b) IN ((1, 2), (3, 4)). Requires >= 2 elements so ordinary
    // parenthesized expressions/conditions are unaffected.
    if let Ok((rest, cond)) = parse_row_comparison(input) {
        return Ok((rest, cond));
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

    // Try NOT UNIQUE (SELECT ...)
    if let Ok((input, _)) = nom::sequence::pair(
        tag::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("UNIQUE")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition::NotUnique(Box::new(subquery))));
    }

    // Try UNIQUE (SELECT ...)
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("UNIQUE")(input) {
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char('(')(input)?;
        let (input, _) = multispace0(input)?;
        let (input, subquery) = parse_select_statement(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = nom_char(')')(input)?;
        return Ok((input, Condition::Unique(Box::new(subquery))));
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

    // Try IS NOT DISTINCT FROM / IS DISTINCT FROM / IS NOT NULL / IS NULL
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("IS")(input) {
        let (input, _) = multispace1(input)?;
        if let Ok((input, _)) = nom::sequence::pair(
            tag::<&str, &str, nom::error::Error<&str>>("NOT"),
            nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("DISTINCT")),
        )(input) {
            let (input, _) = multispace1(input)?;
            let (input, _) = tag_no_case("FROM")(input)?;
            let (input, _) = multispace0(input)?;
            let (input, right) = parse_expression(input)?;
            return Ok((input, Condition::Comparison {
                left,
                operator: Operator::IsNotDistinctFrom,
                right,
                upper_bound: None,
            }));
        }
        if let Ok((input, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("DISTINCT")(input) {
            let (input, _) = multispace1(input)?;
            let (input, _) = tag_no_case("FROM")(input)?;
            let (input, _) = multispace0(input)?;
            let (input, right) = parse_expression(input)?;
            return Ok((input, Condition::Comparison {
                left,
                operator: Operator::IsDistinctFrom,
                right,
                upper_bound: None,
            }));
        }
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
        // IS [NOT] TRUE / FALSE / UNKNOWN boolean tests
        if let Ok((input, op)) = parse_boolean_test_tail(input) {
            return Ok((input, Condition::Comparison {
                left,
                operator: op,
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

    // Try NOT SIMILAR TO ... ESCAPE ...
    if let Ok((input, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("SIMILAR")),
    )(input) {
        let (input, _) = multispace1(input)?;
        let (input, _) = tag_no_case("TO")(input)?;
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_expression(input)?;
        let (input, escape) = parse_optional_escape(input);
        return Ok((input, Condition::Comparison {
            left, operator: Operator::NotSimilar, right, upper_bound: escape,
        }));
    }

    // Try SIMILAR TO ... ESCAPE ...
    if let Ok((input, _)) = nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("SIMILAR"),
        nom::sequence::preceded(multispace1::<&str, nom::error::Error<&str>>, tag_no_case("TO")),
    )(input) {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_expression(input)?;
        let (input, escape) = parse_optional_escape(input);
        return Ok((input, Condition::Comparison {
            left, operator: Operator::Similar, right, upper_bound: escape,
        }));
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

    // Try @> (JSON contains) before generic operator parse
    if let Ok((rest, _)) = tag::<&str, &str, nom::error::Error<&str>>("@>")(input) {
        let (rest, _) = multispace0(rest)?;
        let (rest, right) = parse_expression(rest)?;
        return Ok((rest, Condition::Comparison {
            left,
            operator: Operator::JsonContains,
            right,
            upper_bound: None,
        }));
    }

    let (input, operator) = parse_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, right) = parse_expression(input)?;

    Ok((input, Condition::Comparison { left, operator, right, upper_bound: None }))
}

/// Try to parse an arithmetic operator surrounded by optional whitespace
fn parse_arith_add_sub(input: &str) -> IResult<&str, ArithOp> {
    let (input, _) = multispace0(input)?;
    // Use nom::combinator::not to reject -> and ->> (JSON operators)
    let (input, op) = nom::branch::alt((
        nom::combinator::map(nom_char('+'), |_| ArithOp::Add),
        nom::combinator::value(ArithOp::Sub, nom::sequence::preceded(
            nom_char('-'),
            nom::combinator::peek(nom::combinator::not(nom_char('>'))),
        )),
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
pub fn parse_expression(input: &str) -> IResult<&str, Expression> {
    let (mut input, mut left) = parse_json_expr(input)?;
    // || has lower precedence than -> / ->> / arithmetic
    while let Ok((remaining, _)) = nom::sequence::delimited(
        multispace0::<&str, nom::error::Error<&str>>,
        tag("||"),
        multispace0::<&str, nom::error::Error<&str>>,
    )(input) {
        let (remaining, right) = parse_json_expr(remaining)?;
        left = Expression::BinaryOp(Box::new(left), ArithOp::Concat, Box::new(right));
        input = remaining;
    }

    // Postfix AT TIME ZONE 'UTC' / '+HH:MM' — shifts a timestamp by a fixed offset
    if let Ok((rest, offset)) = parse_at_time_zone(input) {
        return Ok((rest, Expression::AtTimeZone(Box::new(left), offset)));
    }

    Ok((input, left))
}

/// Parse AT TIME ZONE 'zone' and return the zone's offset in seconds.
/// Supported zones: 'UTC' and fixed offsets like '+05:30' or '-08:00'.
fn parse_at_time_zone(input: &str) -> IResult<&str, i64> {
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TIME")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("ZONE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, zone) = parse_string_value(input)?;
    let zone = match zone {
        Value::String(s) => s,
        _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    };
    let offset = parse_zone_offset(&zone)
        .ok_or_else(|| nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
    Ok((input, offset))
}

/// Convert a zone string to its offset in seconds: 'UTC' => 0, '+HH:MM' / '-HH:MM' => signed offset
fn parse_zone_offset(zone: &str) -> Option<i64> {
    let z = zone.trim().to_uppercase();
    if z == "UTC" || z == "Z" || z == "GMT" { return Some(0); }
    let (sign, rest) = match z.strip_prefix('+') {
        Some(r) => (1, r),
        None => (-1, z.strip_prefix('-')?),
    };
    let (h, m) = match rest.split_once(':') {
        Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
        None => (rest.parse::<i64>().ok()?, 0),
    };
    if h > 14 || m > 59 { return None; }
    Some(sign * (h * 3600 + m * 60))
}

/// Parse JSON field access: expr -> key  or  expr ->> key  (higher precedence than ||)
fn parse_json_expr(input: &str) -> IResult<&str, Expression> {
    let (mut input, mut left) = parse_arith_expr(input)?;
    loop {
        let trimmed = input.trim_start();
        // ->> must be checked before -> to avoid mis-parsing
        if let Ok((rest, _)) = tag::<&str, &str, nom::error::Error<&str>>("->>")( trimmed) {
            let (rest, _) = multispace0(rest)?;
            let (rest, right) = parse_arith_expr(rest)?;
            left = Expression::BinaryOp(Box::new(left), ArithOp::JsonGetText, Box::new(right));
            input = rest;
        } else if let Ok((rest, _)) = tag::<&str, &str, nom::error::Error<&str>>("->")(trimmed) {
            let (rest, _) = multispace0(rest)?;
            let (rest, right) = parse_arith_expr(rest)?;
            left = Expression::BinaryOp(Box::new(left), ArithOp::JsonGet, Box::new(right));
            input = rest;
        } else {
            break;
        }
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
            parse_expression_json_build_object,
            parse_expression_json_build_array,
        )),
        nom::branch::alt((
            parse_expression_cast,
            parse_expression_replace,
            parse_expression_translate,
            parse_expression_trim_spec,
            parse_expression_overlay,
            parse_expression_lpad,
            parse_expression_rpad,
        )),
        nom::branch::alt((
            // Date/time expressions (try before window and scalar to catch keywords)
            parse_expression_current_date,
            parse_expression_current_timestamp,
            parse_expression_current_time,
            parse_expression_current_user,
            parse_expression_extract,
            parse_expression_date_trunc,
            parse_expression_datediff,
            parse_expression_dateadd,
            parse_expression_date_part,
            parse_expression_interval,
            parse_expression_date_literal,
            parse_expression_timestamp_literal,
            // JSON expressions
            parse_expression_json_literal,
            parse_expression_json_typeof,
            parse_expression_json_array_length,
        )),
        nom::branch::alt((
            parse_expression_window,
            parse_expression_scalar_func,
            parse_expression_aggregate,
            parse_expression_qualified_column,
            parse_expression_literal,
            parse_expression_user_func,
            parse_expression_simple_column,
        )),
    ))(input)
}

// Parse a user-defined function call: name(expr, expr, ...)
fn parse_expression_user_func(input: &str) -> IResult<&str, Expression> {
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    // Use try to peek at '(' without consuming — if no '(' it's not a function call
    let (input, _) = nom::combinator::peek(nom_char('('))(input)?;
    // Now parse the full function call
    let (input, _) = nom_char('(')(input)?;
    let (input, args) = nom::multi::separated_list0(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        |i| {
            let (i, _) = multispace0(i)?;
            parse_expression(i)
        },
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::UserFunc(name.to_string(), args)))
}

// ── Helper: parse FUNC(expr) ──
fn parse_single_arg_fn<'a>(input: &'a str, name: &str,
    ctor: impl Fn(Expression) -> Expression,
) -> IResult<&'a str, Expression> {
    let (input, _) = tag_no_case(name)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, ctor(expr)))
}

// ── Helper: parse FUNC(expr, expr, ...) with separated_list1 ──
fn parse_vararg_fn1<'a>(input: &'a str, name: &str,
    ctor: fn(Vec<Expression>) -> Expression,
) -> IResult<&'a str, Expression> {
    let (input, _) = tag_no_case(name)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, exprs) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, ctor(exprs)))
}

// ── Helper: parse KEYWORD 'string' literal ──
fn parse_keyword_literal<'a>(input: &'a str, keyword: &str,
    validate: fn(&str) -> Option<Value>,
) -> IResult<&'a str, Expression> {
    let (input, _) = tag_no_case(keyword)(input)?;
    let (input, _) = multispace1(input)?;
    let (input, val) = parse_string_value(input)?;
    if let Value::String(s) = val {
        match validate(&s) {
            Some(v) => Ok((input, Expression::Literal(v))),
            None => Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
        }
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))
    }
}

fn parse_expression_coalesce(input: &str) -> IResult<&str, Expression> {
    parse_vararg_fn1(input, "COALESCE", Expression::Coalesce)
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
            tag_no_case("CHARACTER_LENGTH"),
            tag_no_case("CHAR_LENGTH"),
            tag_no_case("OCTET_LENGTH"),
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
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => ScalarFunc::CharLength,
        "OCTET_LENGTH" => ScalarFunc::OctetLength,
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
    parse_vararg_fn1(input, "CONCAT", Expression::Concat)
}

fn parse_expression_substr(input: &str) -> IResult<&str, Expression> {
    let (input, _) = nom::branch::alt((tag_no_case("SUBSTRING"), tag_no_case("SUBSTR")))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;

    // Spec form: SUBSTRING(str FROM start [FOR len])
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("FROM")(input) {
        let (i, _) = multispace1(i)?;
        let (i, start) = parse_expression(i)?;
        let (i, _) = multispace0(i)?;
        let (i, len) = nom::combinator::opt(|i| {
            let (i, _) = tag_no_case("FOR")(i)?;
            let (i, _) = multispace1(i)?;
            let (i, len) = parse_expression(i)?;
            let (i, _) = multispace0(i)?;
            Ok((i, len))
        })(i)?;
        let (i, _) = nom_char(')')(i)?;
        return Ok((i, Expression::Substr(Box::new(s), Box::new(start), len.map(Box::new))));
    }

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

/// Parse TRANSLATE(str, from_chars, to_chars)
fn parse_expression_translate(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("TRANSLATE")(input)?;
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
    Ok((input, Expression::Translate(Box::new(s), Box::new(from), Box::new(to))))
}

/// Parse spec-form TRIM([LEADING|TRAILING|BOTH] ['chars'] FROM str).
/// Plain TRIM(str) is handled by the generic scalar function parser.
fn parse_expression_trim_spec(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("TRIM")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;

    let (input, mode) = nom::combinator::opt(nom::branch::alt((
        nom::combinator::map(tag_no_case("LEADING"), |_| TrimMode::Leading),
        nom::combinator::map(tag_no_case("TRAILING"), |_| TrimMode::Trailing),
        nom::combinator::map(tag_no_case("BOTH"), |_| TrimMode::Both),
    )))(input)?;
    let (input, _) = multispace0(input)?;

    let (input, chars) = nom::combinator::opt(parse_string_value)(input)?;
    let chars = match chars {
        Some(Value::String(s)) => Some(s),
        _ => None,
    };
    let (input, _) = multispace0(input)?;

    // Without FROM this is a plain TRIM(expr) — let the generic parser handle it
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::ScalarFunc(
        ScalarFunc::TrimChars(mode.unwrap_or(TrimMode::Both), chars),
        Box::new(s),
    )))
}

/// Parse OVERLAY(str PLACING replacement FROM start [FOR len]).
/// Desugars into SUBSTR(str, 1, start-1) || replacement || SUBSTR(str, start+len).
fn parse_expression_overlay(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("OVERLAY")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, s) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("PLACING")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, replacement) = parse_expression(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, start) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;
    let (input, len) = nom::combinator::opt(|i| {
        let (i, _) = tag_no_case("FOR")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, len) = parse_expression(i)?;
        let (i, _) = multispace0(i)?;
        Ok((i, len))
    })(input)?;
    let (input, _) = nom_char(')')(input)?;

    // Replaced span length defaults to the replacement's character length
    let len = len.unwrap_or_else(|| Expression::ScalarFunc(ScalarFunc::CharLength, Box::new(replacement.clone())));
    let prefix = Expression::Substr(
        Box::new(s.clone()),
        Box::new(Expression::Literal(Value::Int(1))),
        Some(Box::new(Expression::BinaryOp(
            Box::new(start.clone()),
            ArithOp::Sub,
            Box::new(Expression::Literal(Value::Int(1))),
        ))),
    );
    let suffix = Expression::Substr(
        Box::new(s),
        Box::new(Expression::BinaryOp(Box::new(start), ArithOp::Add, Box::new(len))),
        None,
    );
    Ok((input, Expression::Concat(vec![prefix, replacement, suffix])))
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
    parse_vararg_fn1(input, "GREATEST", Expression::Greatest)
}

fn parse_expression_least(input: &str) -> IResult<&str, Expression> {
    parse_vararg_fn1(input, "LEAST", Expression::Least)
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

/// Parse CURRENT_TIMESTAMP, LOCALTIMESTAMP or NOW() — all return the current timestamp
fn parse_expression_current_timestamp(input: &str) -> IResult<&str, Expression> {
    // Try CURRENT_TIMESTAMP / LOCALTIMESTAMP first (no parens)
    if let Ok((rest, _)) = nom::branch::alt((
        tag_no_case::<&str, &str, nom::error::Error<&str>>("CURRENT_TIMESTAMP"),
        tag_no_case::<&str, &str, nom::error::Error<&str>>("LOCALTIMESTAMP"),
    ))(input) {
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

/// Parse CURRENT_TIME or LOCALTIME — the current time of day
fn parse_expression_current_time(input: &str) -> IResult<&str, Expression> {
    let (rest, _) = nom::branch::alt((
        tag_no_case("CURRENT_TIME"),
        tag_no_case("LOCALTIME"),
    ))(input)?;
    // Reject when part of a longer word (e.g. LOCALTIMEZONE-ish identifiers)
    if rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((rest, Expression::CurrentTime))
}

/// Parse CURRENT_USER, SESSION_USER or USER — the session user name
fn parse_expression_current_user(input: &str) -> IResult<&str, Expression> {
    let (rest, _) = nom::branch::alt((
        tag_no_case("CURRENT_USER"),
        tag_no_case("SESSION_USER"),
        tag_no_case("USER"),
    ))(input)?;
    if rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    Ok((rest, Expression::CurrentUser))
}

/// Parse DATE 'YYYY-MM-DD' literal
fn parse_expression_date_literal(input: &str) -> IResult<&str, Expression> {
    parse_keyword_literal(input, "DATE", |s| parse_date_str(s).map(Value::Date))
}

/// Parse TIMESTAMP 'YYYY-MM-DD HH:MM:SS' literal
fn parse_expression_timestamp_literal(input: &str) -> IResult<&str, Expression> {
    parse_keyword_literal(input, "TIMESTAMP", |s| parse_timestamp_str(s).map(Value::Timestamp))
}

/// Parse JSON 'string' literal
fn parse_expression_json_literal(input: &str) -> IResult<&str, Expression> {
    parse_keyword_literal(input, "JSON", |s| {
        serde_json::from_str::<serde_json::Value>(s).ok().map(|_| Value::Json(s.to_string()))
    })
}

/// Parse JSON_TYPEOF(expr)
fn parse_expression_json_typeof(input: &str) -> IResult<&str, Expression> {
    parse_single_arg_fn(input, "JSON_TYPEOF", |e| Expression::JsonTypeOf(Box::new(e)))
}

fn parse_expression_json_array_length(input: &str) -> IResult<&str, Expression> {
    parse_single_arg_fn(input, "JSON_ARRAY_LENGTH", |e| Expression::JsonArrayLength(Box::new(e)))
}

/// Parse JSON_BUILD_OBJECT(key, val, ...)
fn parse_expression_json_build_object(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("JSON_BUILD_OBJECT")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, args) = nom::multi::separated_list1(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    // Pair up consecutive args: k1, v1, k2, v2, ...
    if args.len() % 2 != 0 {
        return Err(nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }
    let mut pairs = Vec::new();
    for chunk in args.chunks(2) {
        pairs.push((chunk[0].clone(), chunk[1].clone()));
    }
    Ok((input, Expression::JsonBuildObject(pairs)))
}

fn parse_expression_json_build_array(input: &str) -> IResult<&str, Expression> {
    let (input, _) = tag_no_case("JSON_BUILD_ARRAY")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, args) = nom::multi::separated_list0(
        nom::sequence::delimited(multispace0, nom_char(','), multispace0),
        parse_expression,
    )(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = nom_char(')')(input)?;
    Ok((input, Expression::JsonBuildArray(args)))
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
pub(crate) fn parse_window_clause(input: &str) -> IResult<&str, Vec<(String, WindowSpec)>> {
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

    // Simple CASE: an operand before the first WHEN. Each branch desugars into
    // an equality comparison against the operand.
    let starts_with_when = match tag_no_case::<&str, &str, nom::error::Error<&str>>("WHEN")(input) {
        Ok((rest, _)) => !rest.chars().next().is_some_and(|c| c.is_alphanumeric() || c == '_'),
        Err(_) => false,
    };
    let (input, operand) = if starts_with_when {
        (input, None)
    } else {
        let (i, expr) = parse_expression(input)?;
        let (i, _) = multispace1(i)?;
        (i, Some(expr))
    };

    let mut branches: Vec<(Condition, Expression)> = Vec::new();
    let mut input = input;
    loop {
        let (input_after_when, _) = match tag_no_case::<&str, &str, nom::error::Error<&str>>("WHEN")(input) {
            Ok(r) => r,
            Err(_) => break,
        };
        let (input_after_when, _) = multispace1(input_after_when)?;
        let (input_after_when, condition) = match &operand {
            Some(op_expr) => {
                let (i, comparand) = parse_expression(input_after_when)?;
                (i, Condition::Comparison {
                    left: op_expr.clone(),
                    operator: Operator::Equals,
                    right: comparand,
                    upper_bound: None,
                })
            }
            None => parse_condition(input_after_when)?,
        };
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

/// Parse the tail of a boolean test after IS: [NOT] TRUE | FALSE | UNKNOWN
fn parse_boolean_test_tail(input: &str) -> IResult<&str, Operator> {
    let (input, negated) = match nom::sequence::pair(
        tag_no_case::<&str, &str, nom::error::Error<&str>>("NOT"),
        multispace1::<&str, nom::error::Error<&str>>,
    )(input) {
        Ok((rest, _)) => (rest, true),
        Err(_) => (input, false),
    };
    let (rest, word) = nom::branch::alt((
        tag_no_case("TRUE"),
        tag_no_case("FALSE"),
        tag_no_case("UNKNOWN"),
    ))(input)?;
    if rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    let op = match (word.to_uppercase().as_str(), negated) {
        ("TRUE", false) => Operator::IsTrue,
        ("TRUE", true) => Operator::IsNotTrue,
        ("FALSE", false) => Operator::IsFalse,
        ("FALSE", true) => Operator::IsNotFalse,
        ("UNKNOWN", false) => Operator::IsUnknown,
        (_, false) => Operator::IsUnknown,
        (_, true) => Operator::IsNotUnknown,
    };
    Ok((rest, op))
}

/// Parse optional ESCAPE clause for SIMILAR TO and LIKE patterns
fn parse_optional_escape(input: &str) -> (&str, Option<Expression>) {
    let trimmed = input.trim_start();
    if let Ok((rest, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("ESCAPE")(trimmed) {
        let rest = rest.trim_start();
        if let Ok((rest, expr)) = parse_expression(rest) {
            return (rest, Some(expr));
        }
    }
    (input, None)
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
pub(crate) fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    let (input, value) = nom::branch::alt((
        parse_date_value,
        parse_timestamp_value,
        parse_time_value,
        parse_interval_value,
        parse_bit_string_value,
        parse_string_value,
        parse_null_value,
        parse_bool_value,
        parse_float_value,
        parse_int_value,
    ))(input)?;
    Ok((input, value))
}

/// Parse TIME 'HH:MM:SS' as a plain string value (TIME columns store strings)
fn parse_time_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag_no_case("TIME")(input)?;
    let (input, _) = multispace1(input)?;
    parse_string_value(input)
}

/// Parse INTERVAL 'n' UNIT as its integer number of seconds
fn parse_interval_value(input: &str) -> IResult<&str, Value> {
    let (input, expr) = parse_expression_interval(input)?;
    match expr {
        Expression::Literal(v) => Ok((input, v)),
        _ => Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    }
}

/// Parse B'1010' bit-string literal as a plain string value
fn parse_bit_string_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = tag_no_case("B")(input)?;
    parse_string_value(input)
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
pub(crate) fn parse_identifier(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        nom::character::complete::alpha1,
        nom::bytes::complete::take_while(|c: char| c.is_alphanumeric() || c == '_'),
    )))(input)
}

/// Parse a table name, optionally schema-qualified (e.g. `information_schema.tables`).
/// Returns the full name as a single owned String.
pub(crate) fn parse_table_name(input: &str) -> IResult<&str, String> {
    let (input, first) = parse_identifier(input)?;
    let (input, qualifier) = nom::combinator::opt(nom::sequence::preceded(
        nom::bytes::complete::tag("."),
        parse_identifier,
    ))(input)?;
    let name = match qualifier {
        Some(second) => format!("{}.{}", first, second),
        None => first.to_string(),
    };
    Ok((input, name))
}

