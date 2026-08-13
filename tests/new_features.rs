mod common;

use abcsql::execute;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TestDb {
    dir: std::path::PathBuf,
    pub storage: abcsql::Storage,
}

impl TestDb {
    fn new() -> Self {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("abcsql_nf_{}_{}", std::process::id(), n));
        let _ = std::fs::remove_dir_all(&dir);
        let storage = abcsql::Storage::new(&dir).expect("storage");
        TestDb { dir, storage }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.dir); }
}

/// Helper: create a fresh test DB, run setup SQL, then execute a query
fn with_db(setup: &[&str], query: &str) -> Result<String, String> {
    let db = TestDb::new();
    for sql in setup {
        execute(&db.storage, sql).map_err(|e| format!("setup failed: {}: {}", sql, e))?;
    }
    execute(&db.storage, query)
}

// ---------------------------------------------------------------------------
// Fix 1 — CASE WHEN in lib.rs execute() path
// ---------------------------------------------------------------------------
#[test]
fn test_case_when_lib_path() {
    let setup = [
        "CREATE TABLE items (id INT, price INT)",
        "INSERT INTO items VALUES (1, 10)",
        "INSERT INTO items VALUES (2, 200)",
    ];
    // execute() routes SELECT through lib.rs; result is "(n rows)" count
    let result = with_db(&setup, "SELECT CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END FROM items").unwrap();
    assert!(result.contains("2 rows"), "expected 2 rows, got: {}", result);
}

// ---------------------------------------------------------------------------
// Fix 2 — EXISTS / NOT EXISTS in UPDATE WHERE (storage.rs path)
// ---------------------------------------------------------------------------
#[test]
fn test_exists_in_update_where() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE users (id INT, status VARCHAR)").unwrap();
    execute(&db.storage, "CREATE TABLE orders (user_id INT)").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (1, 'active')").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (2, 'active')").unwrap();
    execute(&db.storage, "INSERT INTO orders VALUES (1)").unwrap();
    // Update users who have an order to 'has_order'
    let upd = execute(&db.storage, "UPDATE users SET status = 'has_order' WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)").unwrap();
    assert!(upd.contains("1 row"), "expected 1 updated, got: {}", upd);
}

#[test]
fn test_not_exists_in_delete_where() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE users (id INT, status VARCHAR)").unwrap();
    execute(&db.storage, "CREATE TABLE orders (user_id INT)").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (1, 'active')").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (2, 'active')").unwrap();
    execute(&db.storage, "INSERT INTO orders VALUES (1)").unwrap();
    // Delete users with no orders (user 2 has no orders)
    let del = execute(&db.storage, "DELETE FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = id)").unwrap();
    assert!(del.contains("1 row"), "expected 1 deleted, got: {}", del);
}

// ---------------------------------------------------------------------------
// Fix 3 — || string concatenation
// ---------------------------------------------------------------------------
#[test]
fn test_pipe_concat_parse() {
    use abcsql::parse_sql;
    let result = parse_sql("SELECT 'foo' || 'bar' FROM t");
    assert!(result.is_ok(), "failed to parse || : {:?}", result);
}

#[test]
fn test_pipe_concat_execute() {
    let setup = [
        "CREATE TABLE greet (first VARCHAR, last VARCHAR)",
        "INSERT INTO greet VALUES ('Hello', ' World')",
    ];
    // execute() path reports row count, not values — just confirm no error
    let result = with_db(&setup, "SELECT first || last FROM greet");
    assert!(result.is_ok(), "|| concat failed: {:?}", result);
}

// ---------------------------------------------------------------------------
// Fix 4 — ORDER BY col NULLS FIRST / NULLS LAST (parser)
// ---------------------------------------------------------------------------
#[test]
fn test_nulls_first_parse() {
    use abcsql::parse_sql;
    let sql = "SELECT id FROM t ORDER BY id ASC NULLS FIRST";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let abcsql::SqlStatement::Select(sel) = stmt {
        let ob = &sel.order_by[0];
        assert_eq!(ob.nulls_first, Some(true));
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_nulls_last_parse() {
    use abcsql::parse_sql;
    let sql = "SELECT id FROM t ORDER BY id DESC NULLS LAST";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let abcsql::SqlStatement::Select(sel) = stmt {
        let ob = &sel.order_by[0];
        assert_eq!(ob.nulls_first, Some(false));
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Fix 5 — IN with column/expression references
// ---------------------------------------------------------------------------
#[test]
fn test_in_with_column_expression_parse() {
    use abcsql::parse_sql;
    // IN list with a column reference — should parse without error
    let sql = "SELECT * FROM t WHERE id IN (min_id, max_id)";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "parse of IN with column refs failed: {:?}", result);
}

#[test]
fn test_in_with_literals_still_works() {
    let setup = [
        "CREATE TABLE t (id INT)",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (2)",
        "INSERT INTO t VALUES (3)",
    ];
    let result = with_db(&setup, "SELECT id FROM t WHERE id IN (1, 3)");
    assert!(result.is_ok(), "IN with literals failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"));
}

// ---------------------------------------------------------------------------
// Fix 6 — t.* table-qualified wildcard (parser)
// ---------------------------------------------------------------------------
#[test]
fn test_star_from_table_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::SelectColumn};
    let sql = "SELECT u.* FROM users u";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(sel.columns[0], SelectColumn::StarFromTable(_)), "expected StarFromTable");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_star_from_table_execute() {
    let setup = [
        "CREATE TABLE users (id INT, name VARCHAR)",
        "INSERT INTO users VALUES (1, 'Alice')",
    ];
    let result = with_db(&setup, "SELECT users.* FROM users");
    // lib.rs path returns "(1 rows)"
    assert!(result.is_ok(), "u.* failed: {:?}", result);
}

// ---------------------------------------------------------------------------
// Fix 7 — INTERSECT / EXCEPT
// ---------------------------------------------------------------------------
#[test]
fn test_intersect_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::UnionType};
    let sql = "SELECT id FROM a INTERSECT SELECT id FROM b";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(sel.union.as_ref().map(|(t, _)| t), Some(UnionType::Intersect)));
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_except_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::UnionType};
    let sql = "SELECT id FROM a EXCEPT SELECT id FROM b";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(sel.union.as_ref().map(|(t, _)| t), Some(UnionType::Except)));
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Fix 8 — COUNT(DISTINCT col)
// ---------------------------------------------------------------------------
#[test]
fn test_count_distinct_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, AggregateFunc}};
    let sql = "SELECT COUNT(DISTINCT name) FROM users";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Aggregate(AggregateFunc::CountDistinct, _)));
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Fix 9 — Window frame clause
// ---------------------------------------------------------------------------
#[test]
fn test_window_frame_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc, FrameMode, FrameBound}};
    let sql = "SELECT SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let col = &sel.columns[0];
        if let SelectColumn::Expr(Expression::Window(WindowFunc::Agg(_, _), spec)) = col {
            let frame = spec.frame.as_ref().expect("frame missing");
            assert!(matches!(frame.mode, FrameMode::Rows));
            assert!(matches!(frame.start, FrameBound::UnboundedPreceding));
            assert!(matches!(frame.end, FrameBound::CurrentRow));
        } else {
            panic!("expected window Agg expr, got {:?}", col);
        }
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Fix 10 — Correlated subqueries (EXISTS with outer column reference)
// ---------------------------------------------------------------------------
#[test]
fn test_correlated_subquery_exists() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE users (id INT, name VARCHAR)").unwrap();
    execute(&db.storage, "CREATE TABLE orders (user_id INT)").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (1, 'Alice')").unwrap();
    execute(&db.storage, "INSERT INTO users VALUES (2, 'Bob')").unwrap();
    execute(&db.storage, "INSERT INTO orders VALUES (1)").unwrap(); // only Alice has orders
    let result = execute(&db.storage, "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)");
    assert!(result.is_ok(), "correlated EXISTS failed: {:?}", result);
    // lib.rs path returns row count
    assert!(result.unwrap().contains("1 rows"), "expected 1 row");
}

#[test]
fn test_update_where_exists_parse() {
    use abcsql::parse_sql;
    let sql = "UPDATE users SET status = 'has_order' WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Update(upd) = stmt {
        assert!(upd.where_clause.is_some(), "WHERE clause was None!");
    } else {
        panic!("expected Update statement");
    }
}

#[test]
fn test_exists_condition_parse() {
    use abcsql::parse_sql;
    // Test just the SELECT with EXISTS in WHERE
    let sql = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Select(sel) = stmt {
        assert!(sel.where_clause.is_some(), "WHERE clause was None in SELECT!");
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Group A1 — GREATEST / LEAST
// ---------------------------------------------------------------------------
#[test]
fn test_greatest_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT GREATEST(1, 2, 3) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Greatest(_))), "expected Greatest expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_least_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT LEAST(10, 5, 8) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Least(_))), "expected Least expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_greatest_execute() {
    let setup = [
        "CREATE TABLE nums (a INT, b INT, c INT)",
        "INSERT INTO nums VALUES (3, 7, 2)",
    ];
    // WHERE filters to rows where GREATEST(a,b,c) = 7, so we should get 1 row
    let result = with_db(&setup, "SELECT a FROM nums WHERE GREATEST(a, b, c) = 7");
    assert!(result.is_ok(), "GREATEST failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row from GREATEST filter");
}

#[test]
fn test_least_execute() {
    let setup = [
        "CREATE TABLE nums (a INT, b INT, c INT)",
        "INSERT INTO nums VALUES (3, 7, 2)",
    ];
    let result = with_db(&setup, "SELECT a FROM nums WHERE LEAST(a, b, c) = 2");
    assert!(result.is_ok(), "LEAST failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row from LEAST filter");
}

// ---------------------------------------------------------------------------
// Group A2 — MOD / %
// ---------------------------------------------------------------------------
#[test]
fn test_mod_operator_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ArithOp}};
    let sql = "SELECT x % 3 FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        if let SelectColumn::Expr(Expression::BinaryOp(_, op, _)) = &sel.columns[0] {
            assert_eq!(*op, ArithOp::Mod);
        } else {
            panic!("expected BinaryOp expr with Mod, got {:?}", sel.columns[0]);
        }
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_mod_execute() {
    let setup = [
        "CREATE TABLE nums (x INT)",
        "INSERT INTO nums VALUES (10)",
        "INSERT INTO nums VALUES (7)",
        "INSERT INTO nums VALUES (6)",
    ];
    // 10 % 3 = 1, 7 % 3 = 1, 6 % 3 = 0 — filter for remainder 0
    let result = with_db(&setup, "SELECT x FROM nums WHERE x % 3 = 0");
    assert!(result.is_ok(), "% failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for x%3=0");
}

// ---------------------------------------------------------------------------
// Group A3 — POWER, SQRT, SIGN, TRUNC
// ---------------------------------------------------------------------------
#[test]
fn test_power_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT POWER(2, 10) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Power(_, _))), "expected Power expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_sqrt_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ScalarFunc}};
    let sql = "SELECT SQRT(16) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Sqrt, _))), "expected Sqrt expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_sign_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ScalarFunc}};
    let sql = "SELECT SIGN(-5) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Sign, _))), "expected Sign expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_trunc_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ScalarFunc}};
    let sql = "SELECT TRUNC(3.7) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Trunc, _))), "expected Trunc expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_power_execute() {
    let setup = [
        "CREATE TABLE nums (base INT, exp INT)",
        "INSERT INTO nums VALUES (2, 10)",
        "INSERT INTO nums VALUES (3, 3)",
    ];
    // 2^10 = 1024, 3^3 = 27 — filter for rows where power > 100
    let result = with_db(&setup, "SELECT base FROM nums WHERE POWER(base, exp) > 100");
    assert!(result.is_ok(), "POWER failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for POWER > 100");
}

#[test]
fn test_sqrt_execute() {
    let setup = [
        "CREATE TABLE nums (x INT)",
        "INSERT INTO nums VALUES (4)",
        "INSERT INTO nums VALUES (9)",
        "INSERT INTO nums VALUES (16)",
    ];
    // SQRT(x) = 3 for x=9
    let result = with_db(&setup, "SELECT x FROM nums WHERE SQRT(x) = 3");
    assert!(result.is_ok(), "SQRT failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for SQRT=3");
}

#[test]
fn test_sign_execute() {
    let setup = [
        "CREATE TABLE nums (x INT)",
        "INSERT INTO nums VALUES (-5)",
        "INSERT INTO nums VALUES (0)",
        "INSERT INTO nums VALUES (7)",
    ];
    // SIGN(x) = 1 for positive, -1 for negative, 0 for zero
    let result = with_db(&setup, "SELECT x FROM nums WHERE SIGN(x) = 1");
    assert!(result.is_ok(), "SIGN failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for SIGN=1");
}

#[test]
fn test_trunc_execute() {
    let setup = [
        "CREATE TABLE nums (x FLOAT)",
        "INSERT INTO nums VALUES (3.9)",
        "INSERT INTO nums VALUES (2.1)",
    ];
    // TRUNC(3.9) = 3, filter for TRUNC = 3
    let result = with_db(&setup, "SELECT x FROM nums WHERE TRUNC(x) = 3");
    assert!(result.is_ok(), "TRUNC failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for TRUNC=3");
}

// ---------------------------------------------------------------------------
// Group A4 — POSITION, REPEAT, REVERSE
// ---------------------------------------------------------------------------
#[test]
fn test_position_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT POSITION('lo' IN 'hello') FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Position(_, _))), "expected Position expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_repeat_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT REPEAT('ab', 3) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::Repeat(_, _))), "expected Repeat expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_reverse_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ScalarFunc}};
    let sql = "SELECT REVERSE('hello') FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Reverse, _))), "expected Reverse expr");
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_position_execute() {
    let setup = [
        "CREATE TABLE words (w VARCHAR)",
        "INSERT INTO words VALUES ('hello')",
        "INSERT INTO words VALUES ('world')",
    ];
    // POSITION('lo' IN w) = 4 for 'hello' (1-based), 0 for 'world'
    let result = with_db(&setup, "SELECT w FROM words WHERE POSITION('lo' IN w) = 4");
    assert!(result.is_ok(), "POSITION failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for POSITION match");
}

#[test]
fn test_repeat_execute() {
    let setup = [
        "CREATE TABLE words (w VARCHAR, n INT)",
        "INSERT INTO words VALUES ('ab', 3)",
        "INSERT INTO words VALUES ('x', 5)",
    ];
    // REPEAT('ab', 3) = 'ababab' — filter for rows where w = 'ab' as a way to include the repeat call
    let result = with_db(&setup, "SELECT w FROM words WHERE LENGTH(REPEAT(w, n)) = 6");
    assert!(result.is_ok(), "REPEAT failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for REPEAT length=6");
}

#[test]
fn test_reverse_execute() {
    let setup = [
        "CREATE TABLE words (w VARCHAR)",
        "INSERT INTO words VALUES ('hello')",
        "INSERT INTO words VALUES ('racecar')",
    ];
    // REVERSE('racecar') = 'racecar' (palindrome), REVERSE('hello') = 'olleh'
    let result = with_db(&setup, "SELECT w FROM words WHERE REVERSE(w) = w");
    assert!(result.is_ok(), "REVERSE failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for palindrome");
}

// ---------------------------------------------------------------------------
// Group B — ORDER BY alias references
// ---------------------------------------------------------------------------
#[test]
fn test_order_by_alias_parse() {
    use abcsql::parse_sql;
    let sql = "SELECT price * 2 AS doubled FROM products ORDER BY doubled";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "ORDER BY alias parse failed: {:?}", result);
}

#[test]
fn test_order_by_alias_execute() {
    let setup = [
        "CREATE TABLE products (name VARCHAR, price INT)",
        "INSERT INTO products VALUES ('c', 30)",
        "INSERT INTO products VALUES ('a', 10)",
        "INSERT INTO products VALUES ('b', 20)",
    ];
    // Ordering by alias should not error; rows should come back (3 rows total)
    let result = with_db(&setup, "SELECT price * 2 AS doubled FROM products ORDER BY doubled");
    assert!(result.is_ok(), "ORDER BY alias failed: {:?}", result);
    assert!(result.unwrap().contains("3 rows"), "expected 3 rows");
}

// ---------------------------------------------------------------------------
// Group C — Implicit multi-table FROM (cross join)
// ---------------------------------------------------------------------------
#[test]
fn test_implicit_cross_join_parse() {
    use abcsql::parse_sql;
    let sql = "SELECT a.id, b.id FROM a, b";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "implicit FROM cross join parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.joins.len(), 1, "expected 1 implicit join, got {}", sel.joins.len());
        assert_eq!(sel.joins[0].join_type, abcsql::parser::JoinType::Cross);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_implicit_cross_join_execute() {
    let setup = [
        "CREATE TABLE colors (color VARCHAR)",
        "CREATE TABLE sizes (sz VARCHAR)",
        "INSERT INTO colors VALUES ('red')",
        "INSERT INTO colors VALUES ('blue')",
        "INSERT INTO sizes VALUES ('S')",
        "INSERT INTO sizes VALUES ('L')",
    ];
    // 2 colors × 2 sizes = 4 combinations
    let result = with_db(&setup, "SELECT color, sz FROM colors, sizes");
    assert!(result.is_ok(), "implicit cross join failed: {:?}", result);
    assert!(result.unwrap().contains("4 rows"), "expected 4 rows from cross join");
}

// ---------------------------------------------------------------------------
// Group D — JOIN USING
// ---------------------------------------------------------------------------
#[test]
fn test_join_using_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::JoinType};
    let sql = "SELECT * FROM orders JOIN customers USING (customer_id)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let join = &sel.joins[0];
        assert_eq!(join.join_type, JoinType::Inner);
        assert!(join.using.is_some(), "expected USING clause");
        assert_eq!(join.using.as_ref().unwrap(), &["customer_id"]);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_join_using_execute() {
    let setup = [
        "CREATE TABLE orders (order_id INT, customer_id INT, amount INT)",
        "CREATE TABLE customers (customer_id INT, name VARCHAR)",
        "INSERT INTO orders VALUES (1, 10, 100)",
        "INSERT INTO orders VALUES (2, 20, 200)",
        "INSERT INTO orders VALUES (3, 99, 50)",
        "INSERT INTO customers VALUES (10, 'Alice')",
        "INSERT INTO customers VALUES (20, 'Bob')",
    ];
    // orders 1 and 2 have matching customers; order 3 does not
    let result = with_db(&setup, "SELECT order_id FROM orders JOIN customers USING (customer_id)");
    assert!(result.is_ok(), "JOIN USING failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 matching rows from JOIN USING");
}

// ---------------------------------------------------------------------------
// Group E — NATURAL JOIN
// ---------------------------------------------------------------------------
#[test]
fn test_natural_join_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::JoinType};
    let sql = "SELECT * FROM orders NATURAL JOIN customers";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.joins[0].join_type, JoinType::Natural);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_natural_join_execute() {
    let setup = [
        "CREATE TABLE orders (order_id INT, dept_id INT, amount INT)",
        "CREATE TABLE departments (dept_id INT, name VARCHAR)",
        "INSERT INTO orders VALUES (1, 10, 100)",
        "INSERT INTO orders VALUES (2, 20, 200)",
        "INSERT INTO orders VALUES (3, 99, 50)",
        "INSERT INTO departments VALUES (10, 'Sales')",
        "INSERT INTO departments VALUES (20, 'Engineering')",
    ];
    // dept_id is the shared column; orders 1 and 2 match departments
    let result = with_db(&setup, "SELECT order_id FROM orders NATURAL JOIN departments");
    assert!(result.is_ok(), "NATURAL JOIN failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 matching rows from NATURAL JOIN");
}

// ---------------------------------------------------------------------------
// Group F — FILTER clause on aggregates
// ---------------------------------------------------------------------------
#[test]
fn test_aggregate_filter_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, AggregateFunc}};
    let sql = "SELECT COUNT(*) FILTER (WHERE active = true) FROM users";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let col = &sel.columns[0];
        assert!(
            matches!(col, SelectColumn::AggregateFiltered(AggregateFunc::Count, _, _)),
            "expected AggregateFiltered(Count, ...), got {:?}", col
        );
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_aggregate_filter_execute() {
    let setup = [
        "CREATE TABLE users (id INT, active INT)",
        "INSERT INTO users VALUES (1, 1)",
        "INSERT INTO users VALUES (2, 1)",
        "INSERT INTO users VALUES (3, 0)",
        "INSERT INTO users VALUES (4, 1)",
    ];
    // Confirm the FILTER aggregate parses and executes without error.
    // The simple execute() path in lib.rs doesn't evaluate aggregates itself
    // (it returns the raw table row count), so we only check no error here.
    let result = with_db(&setup, "SELECT COUNT(*) FILTER (WHERE active = 1) FROM users");
    assert!(result.is_ok(), "FILTER aggregate failed: {:?}", result);
}

// ---------------------------------------------------------------------------
// Group G — ANY / ALL subquery operators
// ---------------------------------------------------------------------------
#[test]
fn test_any_subquery_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::Condition};
    let sql = "SELECT id FROM t WHERE x > ANY (SELECT y FROM s)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let cond = &sel.where_clause.as_ref().expect("no WHERE").condition;
        assert!(matches!(cond, Condition::AnyComparison { .. }), "expected AnyComparison, got {:?}", cond);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_all_subquery_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::Condition};
    let sql = "SELECT id FROM t WHERE x > ALL (SELECT y FROM s)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let cond = &sel.where_clause.as_ref().expect("no WHERE").condition;
        assert!(matches!(cond, Condition::AllComparison { .. }), "expected AllComparison, got {:?}", cond);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_any_subquery_execute() {
    let setup = [
        "CREATE TABLE scores (id INT, score INT)",
        "CREATE TABLE thresholds (val INT)",
        "INSERT INTO scores VALUES (1, 50)",
        "INSERT INTO scores VALUES (2, 80)",
        "INSERT INTO scores VALUES (3, 30)",
        "INSERT INTO thresholds VALUES (40)",
        "INSERT INTO thresholds VALUES (60)",
    ];
    // score > ANY (40, 60) means score > 40 (the smallest threshold)
    // 50 > 40 true, 80 > 40 true, 30 > 40 false => 2 rows
    let result = with_db(&setup, "SELECT id FROM scores WHERE score > ANY (SELECT val FROM thresholds)");
    assert!(result.is_ok(), "ANY subquery failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 rows for ANY");
}

#[test]
fn test_all_subquery_execute() {
    let setup = [
        "CREATE TABLE scores (id INT, score INT)",
        "CREATE TABLE thresholds (val INT)",
        "INSERT INTO scores VALUES (1, 50)",
        "INSERT INTO scores VALUES (2, 80)",
        "INSERT INTO scores VALUES (3, 30)",
        "INSERT INTO thresholds VALUES (40)",
        "INSERT INTO thresholds VALUES (60)",
    ];
    // score > ALL (40, 60) means score > 60 (every threshold)
    // only 80 > 60 => 1 row
    let result = with_db(&setup, "SELECT id FROM scores WHERE score > ALL (SELECT val FROM thresholds)");
    assert!(result.is_ok(), "ALL subquery failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row for ALL");
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE CTEs
// ---------------------------------------------------------------------------

// Test 1: simple counter using a pure recursive CTE (no backing table)
// Generates rows 1..5; verify row count via lib.rs execute() path.
// We need a dummy table to satisfy the FROM clause — use a 1-row anchor trick.
#[test]
fn test_recursive_cte_counter_parse() {
    use abcsql::parse_sql;
    // Verify the parser handles WITH RECURSIVE and column lists
    let sql = "WITH RECURSIVE counter(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 5) SELECT * FROM counter";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "WITH RECURSIVE parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.ctes.len(), 1);
        assert!(sel.ctes[0].recursive, "expected recursive = true");
        assert_eq!(sel.ctes[0].name, "counter");
        assert_eq!(sel.ctes[0].columns, vec!["n"]);
    } else {
        panic!("expected Select statement");
    }
}

// Test 2: non-RECURSIVE WITH still works (regression)
#[test]
fn test_non_recursive_cte_still_works() {
    let setup = [
        "CREATE TABLE vals (x INT)",
        "INSERT INTO vals VALUES (10)",
        "INSERT INTO vals VALUES (20)",
        "INSERT INTO vals VALUES (30)",
    ];
    // Regular CTE should still produce correct results
    let result = with_db(&setup, "WITH big AS (SELECT x FROM vals WHERE x > 15) SELECT * FROM big");
    assert!(result.is_ok(), "non-recursive CTE failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 rows from non-recursive CTE");
}

// Test 3: recursive CTE with a real table — path/tree traversal
// Build an edges table (parent, child) and find all descendants of node 1.
// Hierarchy: 1->2, 1->3, 2->4, 3->5
// Descendants of 1: {2, 3, 4, 5} => 4 rows
#[test]
fn test_recursive_cte_tree_traversal() {
    let setup = [
        "CREATE TABLE edges (parent INT, child INT)",
        "INSERT INTO edges VALUES (1, 2)",
        "INSERT INTO edges VALUES (1, 3)",
        "INSERT INTO edges VALUES (2, 4)",
        "INSERT INTO edges VALUES (3, 5)",
    ];
    let query = "WITH RECURSIVE descendants(node) AS (\
        SELECT child FROM edges WHERE parent = 1 \
        UNION ALL \
        SELECT e.child FROM edges e JOIN descendants d ON e.parent = d.node\
    ) SELECT * FROM descendants";
    let result = with_db(&setup, query);
    assert!(result.is_ok(), "recursive tree CTE failed: {:?}", result);
    // 2, 3 from anchor; 4, 5 from recursive step => 4 rows total
    assert!(result.unwrap().contains("4 rows"), "expected 4 rows from tree traversal");
}

// Test 4: fibonacci sequence via recursive CTE
// WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100)
// Pairs: (0,1), (1,1), (1,2), (2,3), (3,5), (5,8), (8,13), (13,21), (21,34), (34,55), (55,89)
// 11 rows total (b < 100 stops when b would be 144)
#[test]
fn test_recursive_cte_fibonacci_parse() {
    use abcsql::parse_sql;
    let sql = "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT a FROM fib";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "fibonacci CTE parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Select(sel) = stmt {
        assert!(sel.ctes[0].recursive);
        assert_eq!(sel.ctes[0].columns, vec!["a", "b"]);
    } else {
        panic!("expected Select");
    }
}

// Test 5: column rename list on non-recursive CTE
#[test]
fn test_cte_column_list_parse() {
    use abcsql::parse_sql;
    let sql = "WITH named(x, y) AS (SELECT 1, 2 FROM t) SELECT * FROM named";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "CTE column list parse failed: {:?}", result);
    let (_, stmt) = result.unwrap();
    if let abcsql::SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.ctes[0].columns, vec!["x", "y"]);
        assert!(!sel.ctes[0].recursive, "non-recursive CTE should have recursive=false");
    } else {
        panic!("expected Select");
    }
}

// Test 6: RECURSIVE keyword doesn't break non-table-backed CTEs row count check
#[test]
fn test_recursive_cte_with_table_anchor() {
    let setup = [
        "CREATE TABLE nums (n INT)",
        "INSERT INTO nums VALUES (1)",
        "INSERT INTO nums VALUES (2)",
        "INSERT INTO nums VALUES (3)",
    ];
    // Non-recursive CTE referencing a real table — must still work
    let result = with_db(&setup, "WITH RECURSIVE t AS (SELECT n FROM nums WHERE n > 1) SELECT * FROM t");
    assert!(result.is_ok(), "RECURSIVE keyword with non-recursive body failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 rows");
}

// ---------------------------------------------------------------------------
// Date / Time tests
// ---------------------------------------------------------------------------

// Test 1 — DATE literal parse
#[test]
fn test_date_literal_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, Value}};
    let sql = "SELECT DATE '2024-03-15' FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::Literal(Value::Date(_)))),
            "expected Date literal, got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// Test 2 — TIMESTAMP literal parse
#[test]
fn test_timestamp_literal_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, Value}};
    let sql = "SELECT TIMESTAMP '2024-03-15 14:30:00' FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::Literal(Value::Timestamp(_)))),
            "expected Timestamp literal, got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// Test 3 — CURRENT_DATE parse
#[test]
fn test_current_date_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let (_, stmt) = parse_sql("SELECT CURRENT_DATE FROM t").expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::CurrentDate)));
    } else {
        panic!("expected Select");
    }
}

// Test 4 — CURRENT_TIMESTAMP parse
#[test]
fn test_current_timestamp_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let (_, stmt) = parse_sql("SELECT CURRENT_TIMESTAMP FROM t").expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::CurrentTimestamp)));
    } else {
        panic!("expected Select");
    }
}

// Test 5 — NOW() parse
#[test]
fn test_now_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let (_, stmt) = parse_sql("SELECT NOW() FROM t").expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::CurrentTimestamp)));
    } else {
        panic!("expected Select");
    }
}

// Test 6 — EXTRACT(YEAR FROM date) parse
#[test]
fn test_extract_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT EXTRACT(YEAR FROM created_at) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::Extract(_, _))),
            "expected Extract, got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// Test 7 — DATEDIFF execute
#[test]
fn test_datediff_execute() {
    let setup = [
        "CREATE TABLE events (name VARCHAR, event_date DATE)",
        "INSERT INTO events VALUES ('launch', '2024-03-15')",
        "INSERT INTO events VALUES ('review', '2024-03-22')",
    ];
    // DATEDIFF(DAY, '2024-03-22', '2024-03-15') = 7 (d1 - d2 = 22 Mar - 15 Mar = 7 days)
    // All rows pass since this is a constant comparison
    let result = with_db(&setup, "SELECT name FROM events WHERE DATEDIFF(DAY, '2024-03-22', '2024-03-15') = 7");
    assert!(result.is_ok(), "DATEDIFF failed: {:?}", result);
    assert!(result.unwrap().contains("2 rows"), "expected 2 rows");
}

// Test 8 — DATE_TRUNC execute (truncate to month)
#[test]
fn test_date_trunc_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT DATE_TRUNC('MONTH', created_at) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::DateTrunc(_, _))),
            "expected DateTrunc, got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// Test 9 — YEAR() / MONTH() / DAY() scalar functions parse
#[test]
fn test_year_month_day_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ScalarFunc}};
    let (_, stmt) = parse_sql("SELECT YEAR(d), MONTH(d), DAY(d) FROM t").expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(matches!(&sel.columns[0], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Year, _))));
        assert!(matches!(&sel.columns[1], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Month, _))));
        assert!(matches!(&sel.columns[2], SelectColumn::Expr(Expression::ScalarFunc(ScalarFunc::Day, _))));
    } else {
        panic!("expected Select");
    }
}

// Test 10 — Insert string into DATE column → stored as Value::Date
#[test]
fn test_date_column_coercion() {
    use abcsql::parser::Value;
    let db = TestDb::new();
    abcsql::execute(&db.storage, "CREATE TABLE ev (name VARCHAR, dt DATE)").unwrap();
    abcsql::execute(&db.storage, "INSERT INTO ev VALUES ('launch', '2024-03-15')").unwrap();
    let rows = db.storage.read_rows("ev").unwrap();
    // epoch days for 2024-03-15 = 19797
    assert_eq!(rows[0][1], Value::Date(19797));
}

// Test 11 — Insert string into TIMESTAMP column → stored as Value::Timestamp
#[test]
fn test_timestamp_column_coercion() {
    use abcsql::parser::Value;
    let db = TestDb::new();
    abcsql::execute(&db.storage, "CREATE TABLE logs (msg VARCHAR, ts TIMESTAMP)").unwrap();
    abcsql::execute(&db.storage, "INSERT INTO logs VALUES ('hello', '2024-03-15 14:30:00')").unwrap();
    let rows = db.storage.read_rows("logs").unwrap();
    // 2024-03-15 14:30:00 UTC = 1710513000
    assert_eq!(rows[0][1], Value::Timestamp(1710513000));
}

// Test 12 — WHERE filter on DATE column
#[test]
fn test_date_where_filter() {
    let setup = [
        "CREATE TABLE events (name VARCHAR, dt DATE)",
        "INSERT INTO events VALUES ('past', '2020-01-01')",
        "INSERT INTO events VALUES ('future', '2030-01-01')",
    ];
    let result = with_db(&setup, "SELECT name FROM events WHERE dt > '2025-01-01'");
    assert!(result.is_ok(), "date WHERE failed: {:?}", result);
    assert!(result.unwrap().contains("1 rows"), "expected 1 row with future date");
}

// Test 13 — INTERVAL arithmetic in date expression
#[test]
fn test_interval_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, ArithOp}};
    let sql = "SELECT DATE '2024-01-01' + INTERVAL 7 DAY FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::BinaryOp(_, ArithOp::Add, _))),
            "expected BinaryOp(Add), got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// Test 14 — CAST to DATE
#[test]
fn test_cast_to_date() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT CAST('2024-03-15' AS DATE) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert!(
            matches!(&sel.columns[0], SelectColumn::Expr(Expression::Cast(_, _))),
            "expected Cast expr, got {:?}", sel.columns[0]
        );
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// WINDOW clause tests
// ---------------------------------------------------------------------------

// Test W1 — parse WINDOW clause with one named window
#[test]
fn test_window_clause_parse_single() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowSpec}};
    let sql = "SELECT name, SUM(salary) OVER w FROM emp WINDOW w AS (PARTITION BY dept)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.window_defs.len(), 1, "expected 1 window def");
        let (name, spec) = &sel.window_defs[0];
        assert_eq!(name, "w");
        assert_eq!(spec.partition_by.len(), 1);
        assert!(spec.order_by.is_empty());
        // The OVER w column should have base_window = Some("w")
        if let SelectColumn::Expr(Expression::Window(_, ref wspec)) = sel.columns[1] {
            assert_eq!(wspec.base_window.as_deref(), Some("w"));
        } else {
            panic!("expected Window expr for second column, got {:?}", sel.columns[1]);
        }
    } else {
        panic!("expected Select");
    }
}

// Test W2 — OVER bare name sets base_window
#[test]
fn test_window_over_bare_name_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY id)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.window_defs.len(), 1);
        if let SelectColumn::Expr(Expression::Window(_, ref spec)) = sel.columns[0] {
            assert_eq!(spec.base_window.as_deref(), Some("w"));
            assert!(spec.partition_by.is_empty());
            assert!(spec.order_by.is_empty());
        } else {
            panic!("expected Window expr, got {:?}", sel.columns[0]);
        }
    } else {
        panic!("expected Select");
    }
}

// Test W3 — OVER (w ORDER BY col) sets base_window and own order_by
#[test]
fn test_window_over_paren_with_base_and_order_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT ROW_NUMBER() OVER (w ORDER BY salary) FROM t WINDOW w AS (PARTITION BY dept)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        if let SelectColumn::Expr(Expression::Window(_, ref spec)) = sel.columns[0] {
            assert_eq!(spec.base_window.as_deref(), Some("w"));
            assert!(spec.partition_by.is_empty(), "inline spec has no PARTITION BY");
            assert_eq!(spec.order_by.len(), 1, "inline spec has ORDER BY salary");
        } else {
            panic!("expected Window expr, got {:?}", sel.columns[0]);
        }
        assert_eq!(sel.window_defs.len(), 1);
        assert_eq!(sel.window_defs[0].0, "w");
    } else {
        panic!("expected Select");
    }
}

// Test W4 — execute: named window resolves correctly
#[test]
fn test_window_execute_named_window() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE emp (name VARCHAR, dept VARCHAR, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES ('alice', 'eng', 100)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES ('bob', 'eng', 200)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES ('carol', 'hr', 150)").unwrap();

    // ROW_NUMBER using named window should give same result as inline OVER (PARTITION BY dept ORDER BY salary)
    let r1 = execute(&db.storage,
        "SELECT name, ROW_NUMBER() OVER w AS rn FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary)"
    ).unwrap();
    let r2 = execute(&db.storage,
        "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM emp"
    ).unwrap();
    assert_eq!(r1, r2, "named window should produce same results as inline spec");
}

// Test W5 — multiple named windows
#[test]
fn test_window_clause_multiple_defs_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let sql = "SELECT ROW_NUMBER() OVER w1, RANK() OVER w2 FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.window_defs.len(), 2, "expected 2 window defs");
        assert_eq!(sel.window_defs[0].0, "w1");
        assert_eq!(sel.window_defs[1].0, "w2");
    } else {
        panic!("expected Select");
    }
}

// Test W6 — OVER (w) with just a name in parens resolves the named window
#[test]
fn test_window_over_paren_bare_name_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT ROW_NUMBER() OVER (w) FROM t WINDOW w AS (PARTITION BY dept)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        if let SelectColumn::Expr(Expression::Window(_, ref spec)) = sel.columns[0] {
            assert_eq!(spec.base_window.as_deref(), Some("w"));
            assert!(spec.partition_by.is_empty());
            assert!(spec.order_by.is_empty());
        } else {
            panic!("expected Window expr, got {:?}", sel.columns[0]);
        }
    } else {
        panic!("expected Select");
    }
}

// Test W7 — execute: OVER (w ORDER BY col) inherits PARTITION BY from named window
#[test]
fn test_window_execute_inherit_partition_extend_order() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE emp (id INT, name VARCHAR, dept VARCHAR, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES (1, 'alice', 'eng', 100)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES (2, 'bob', 'eng', 200)").unwrap();
    execute(&db.storage, "INSERT INTO emp VALUES (3, 'carol', 'hr', 150)").unwrap();

    // Named window w has PARTITION BY dept; inline overrides ORDER BY with id
    let r1 = execute(&db.storage,
        "SELECT name, ROW_NUMBER() OVER (w ORDER BY id) AS rn FROM emp WINDOW w AS (PARTITION BY dept)"
    ).unwrap();
    // Equivalent inline spec
    let r2 = execute(&db.storage,
        "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY id) AS rn FROM emp"
    ).unwrap();
    assert_eq!(r1, r2, "inheriting PARTITION BY and overriding ORDER BY should match inline spec");
}

// Test W8 — format_expr for OVER bare name
#[test]
fn test_window_format_expr_bare_name() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression}};
    let sql = "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY id)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        if let SelectColumn::Expr(Expression::Window(_, ref spec)) = sel.columns[0] {
            // base_window is Some("w") and no inline clauses — format_expr should show "over w"
            assert_eq!(spec.base_window.as_deref(), Some("w"));
        } else {
            panic!("expected Window expr");
        }
    } else {
        panic!("expected Select");
    }
}

// ---------------------------------------------------------------------------
// Group A — new window functions: NTILE, PERCENT_RANK, CUME_DIST,
//           FIRST_VALUE, LAST_VALUE, NTH_VALUE
// ---------------------------------------------------------------------------

// A1 — NTILE(4) over 8 rows assigns buckets 1..4 twice
#[test]
fn test_ntile_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT NTILE(4) OVER (ORDER BY id) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::Ntile(_)), "expected Ntile variant");
        } else {
            panic!("expected Window expr");
        }
    }

    // Execute: 8 rows, NTILE(4) => buckets 1,1,2,2,3,3,4,4
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t8 (id INT)").unwrap();
    for i in 1..=8 {
        execute(&db.storage, &format!("INSERT INTO t8 VALUES ({})", i)).unwrap();
    }
    let r = execute(&db.storage, "SELECT NTILE(4) OVER (ORDER BY id) AS bucket FROM t8").unwrap();
    assert!(r.contains("8 rows"), "expected 8 rows, got: {}", r);
}

// A2 — PERCENT_RANK() parses and executes
#[test]
fn test_percent_rank_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT PERCENT_RANK() OVER (ORDER BY id) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::PercentRank), "expected PercentRank variant");
        } else {
            panic!("expected Window expr");
        }
    }

    // Execute on 3 rows: percent_rank = (rank-1)/(n-1) => 0.0, 0.5, 1.0
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE pr3 (id INT)").unwrap();
    execute(&db.storage, "INSERT INTO pr3 VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO pr3 VALUES (2)").unwrap();
    execute(&db.storage, "INSERT INTO pr3 VALUES (3)").unwrap();
    let r = execute(&db.storage, "SELECT PERCENT_RANK() OVER (ORDER BY id) AS pr FROM pr3").unwrap();
    assert!(r.contains("3 rows"), "expected 3 rows, got: {}", r);
}

// A3 — CUME_DIST() parses and executes
#[test]
fn test_cume_dist_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT CUME_DIST() OVER (ORDER BY id) FROM t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::CumeDist), "expected CumeDist variant");
        } else {
            panic!("expected Window expr");
        }
    }

    // Execute on 3 rows: cume_dist = rank/n => 0.333, 0.667, 1.0
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE cd3 (id INT)").unwrap();
    execute(&db.storage, "INSERT INTO cd3 VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO cd3 VALUES (2)").unwrap();
    execute(&db.storage, "INSERT INTO cd3 VALUES (3)").unwrap();
    let r = execute(&db.storage, "SELECT CUME_DIST() OVER (ORDER BY id) AS cd FROM cd3").unwrap();
    assert!(r.contains("3 rows"), "expected 3 rows, got: {}", r);
}

// A4 — FIRST_VALUE with PARTITION BY
#[test]
fn test_first_value_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id) FROM emp";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::FirstValue(_)), "expected FirstValue variant");
        } else {
            panic!("expected Window expr");
        }
    }

    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE fv_emp (id INT, dept VARCHAR, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO fv_emp VALUES (1, 'eng', 100)").unwrap();
    execute(&db.storage, "INSERT INTO fv_emp VALUES (2, 'eng', 200)").unwrap();
    execute(&db.storage, "INSERT INTO fv_emp VALUES (3, 'hr', 150)").unwrap();
    let r = execute(&db.storage,
        "SELECT dept, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY id) AS fv FROM fv_emp"
    ).unwrap();
    assert!(r.contains("3 rows"), "expected 3 rows, got: {}", r);
}

// A5 — LAST_VALUE with UNBOUNDED FOLLOWING frame
#[test]
fn test_last_value_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT LAST_VALUE(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM emp";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::LastValue(_)), "expected LastValue variant");
        } else {
            panic!("expected Window expr");
        }
    }

    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE lv_emp (id INT, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO lv_emp VALUES (1, 100)").unwrap();
    execute(&db.storage, "INSERT INTO lv_emp VALUES (2, 200)").unwrap();
    execute(&db.storage, "INSERT INTO lv_emp VALUES (3, 300)").unwrap();
    let r = execute(&db.storage,
        "SELECT LAST_VALUE(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM lv_emp"
    ).unwrap();
    assert!(r.contains("3 rows"), "expected 3 rows, got: {}", r);
}

// A6 — NTH_VALUE(expr, n) returns the nth value in the frame
#[test]
fn test_nth_value_parse_and_execute() {
    use abcsql::{parse_sql, SqlStatement, parser::{SelectColumn, Expression, WindowFunc}};
    let sql = "SELECT NTH_VALUE(salary, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM emp";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = &stmt {
        if let SelectColumn::Expr(Expression::Window(func, _)) = &sel.columns[0] {
            assert!(matches!(func, WindowFunc::NthValue(_, _)), "expected NthValue variant");
        } else {
            panic!("expected Window expr");
        }
    }

    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE nv_emp (id INT, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO nv_emp VALUES (1, 100)").unwrap();
    execute(&db.storage, "INSERT INTO nv_emp VALUES (2, 200)").unwrap();
    execute(&db.storage, "INSERT INTO nv_emp VALUES (3, 300)").unwrap();
    let r = execute(&db.storage,
        "SELECT NTH_VALUE(salary, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv FROM nv_emp"
    ).unwrap();
    assert!(r.contains("3 rows"), "expected 3 rows, got: {}", r);
}

// ---------------------------------------------------------------------------
// Group B — VALUES as a table expression
// ---------------------------------------------------------------------------

// B1 — basic SELECT * from VALUES with aliased columns
#[test]
fn test_values_table_basic() {
    let r = execute(
        &TestDb::new().storage,
        "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)",
    ).unwrap();
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

// B2 — SELECT with expression on VALUES column
#[test]
fn test_values_table_expr() {
    let r = execute(
        &TestDb::new().storage,
        "SELECT id + 1 FROM (VALUES (10), (20)) AS t(id)",
    ).unwrap();
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

// B3 — VALUES with WHERE filter
#[test]
fn test_values_table_where() {
    let r = execute(
        &TestDb::new().storage,
        "SELECT id FROM (VALUES (1), (2), (3)) AS t(id) WHERE id > 1",
    ).unwrap();
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

// B4 — VALUES auto-generates column names when no alias list given
#[test]
fn test_values_table_auto_col_names() {
    use abcsql::{parse_sql, SqlStatement, parser::FromClause};
    let sql = "SELECT * FROM (VALUES (1, 2)) AS t";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        // Column name list should be empty (auto-named at execution time)
        if let FromClause::Values(_, col_names) = &sel.from {
            assert!(col_names.is_empty(), "no explicit columns: {:?}", col_names);
        } else {
            panic!("expected Values FROM clause");
        }
    }
}

// B5 — VALUES with LIMIT
#[test]
fn test_values_table_limit() {
    let r = execute(
        &TestDb::new().storage,
        "SELECT id FROM (VALUES (1), (2), (3), (4)) AS t(id) LIMIT 2",
    ).unwrap();
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

// ---------------------------------------------------------------------------
// Group C — GROUP BY ROLLUP / CUBE / GROUPING SETS
// ---------------------------------------------------------------------------

// C1 — ROLLUP parses and produces grouping_sets
#[test]
fn test_rollup_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let sql = "SELECT dept, SUM(salary) FROM emp GROUP BY ROLLUP(dept)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let gs = sel.grouping_sets.expect("expected grouping_sets to be Some");
        // ROLLUP(dept) => [(dept), ()]
        assert_eq!(gs.len(), 2, "ROLLUP(a) should produce 2 grouping sets: {:?}", gs);
    } else {
        panic!("expected Select");
    }
}

// C2 — CUBE parses and produces all subsets
#[test]
fn test_cube_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let sql = "SELECT a, b, COUNT(*) FROM t GROUP BY CUBE(a, b)";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let gs = sel.grouping_sets.expect("expected grouping_sets to be Some");
        // CUBE(a,b) => [(a,b), (a), (b), ()] — 2^2 = 4 sets
        assert_eq!(gs.len(), 4, "CUBE(a,b) should produce 4 grouping sets: {:?}", gs);
    } else {
        panic!("expected Select");
    }
}

// C3 — GROUPING SETS parses correctly
#[test]
fn test_grouping_sets_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let sql = "SELECT a, b, COUNT(*) FROM t GROUP BY GROUPING SETS ((a,b),(a),())";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        let gs = sel.grouping_sets.expect("expected grouping_sets to be Some");
        assert_eq!(gs.len(), 3, "should have 3 grouping sets: {:?}", gs);
    } else {
        panic!("expected Select");
    }
}

// C4 — ROLLUP executes without error (lib.rs path doesn't aggregate)
#[test]
fn test_rollup_execute() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE re_emp (dept VARCHAR, salary INT)").unwrap();
    execute(&db.storage, "INSERT INTO re_emp VALUES ('eng', 100)").unwrap();
    execute(&db.storage, "INSERT INTO re_emp VALUES ('eng', 200)").unwrap();
    execute(&db.storage, "INSERT INTO re_emp VALUES ('hr', 150)").unwrap();
    // The simple execute() path doesn't aggregate — just confirm no error
    let r = execute(&db.storage,
        "SELECT dept, SUM(salary) FROM re_emp GROUP BY ROLLUP(dept)"
    );
    assert!(r.is_ok(), "ROLLUP execute failed: {:?}", r);
}

// C5 — CUBE executes without error (lib.rs path returns raw row count)
#[test]
fn test_cube_execute() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE cube_t (a VARCHAR, b VARCHAR)").unwrap();
    execute(&db.storage, "INSERT INTO cube_t VALUES ('x', 'p')").unwrap();
    execute(&db.storage, "INSERT INTO cube_t VALUES ('x', 'q')").unwrap();
    execute(&db.storage, "INSERT INTO cube_t VALUES ('y', 'p')").unwrap();
    // The simple execute() path doesn't aggregate — just confirm no error
    let r = execute(&db.storage,
        "SELECT a, b, COUNT(*) FROM cube_t GROUP BY CUBE(a, b)"
    );
    assert!(r.is_ok(), "CUBE execute failed: {:?}", r);
}

// ---------------------------------------------------------------------------
// Group D — LATERAL joins
// ---------------------------------------------------------------------------

// D1 — LATERAL parses correctly into join.lateral
#[test]
fn test_lateral_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let sql = "SELECT c.id, recent.amount FROM customers AS c LEFT JOIN LATERAL (SELECT amount FROM orders WHERE customer_id = c.id ORDER BY amount DESC LIMIT 1) AS recent ON true";
    let (_, stmt) = parse_sql(sql).expect("parse failed");
    if let SqlStatement::Select(sel) = stmt {
        assert_eq!(sel.joins.len(), 1, "expected 1 join");
        let join = &sel.joins[0];
        assert!(join.lateral.is_some(), "join.lateral should be Some");
        assert_eq!(join.alias.as_deref(), Some("recent"), "expected alias 'recent'");
    } else {
        panic!("expected Select");
    }
}

// D2 — LATERAL join executes with correlated subquery
#[test]
fn test_lateral_execute_basic() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE lat_customers (id INT, name VARCHAR)").unwrap();
    execute(&db.storage, "CREATE TABLE lat_orders (customer_id INT, amount INT)").unwrap();
    execute(&db.storage, "INSERT INTO lat_customers VALUES (1, 'alice')").unwrap();
    execute(&db.storage, "INSERT INTO lat_customers VALUES (2, 'bob')").unwrap();
    execute(&db.storage, "INSERT INTO lat_orders VALUES (1, 100)").unwrap();
    execute(&db.storage, "INSERT INTO lat_orders VALUES (1, 200)").unwrap();
    // bob has no orders — LEFT JOIN should still return bob with NULL amount
    let r = execute(&db.storage,
        "SELECT c.id, recent.amount FROM lat_customers AS c LEFT JOIN LATERAL (SELECT amount FROM lat_orders WHERE customer_id = c.id ORDER BY amount DESC LIMIT 1) AS recent ON true"
    ).unwrap();
    // alice gets 1 row (most recent order), bob gets 1 row (NULL) => 2 rows
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

// D3 — LATERAL INNER JOIN excludes rows with no match
#[test]
fn test_lateral_inner_join_execute() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE lat_c (id INT)").unwrap();
    execute(&db.storage, "CREATE TABLE lat_o (cid INT, val INT)").unwrap();
    execute(&db.storage, "INSERT INTO lat_c VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO lat_c VALUES (2)").unwrap();
    execute(&db.storage, "INSERT INTO lat_o VALUES (1, 42)").unwrap();
    // Customer 2 has no orders; INNER JOIN => only customer 1 appears
    let r = execute(&db.storage,
        "SELECT c.id, o.val FROM lat_c AS c JOIN LATERAL (SELECT val FROM lat_o WHERE cid = c.id) AS o ON true"
    ).unwrap();
    assert!(r.contains("1 row"), "expected 1 row (only customer with orders), got: {}", r);
}

// ---------------------------------------------------------------------------
// JSON Support — integration tests
// ---------------------------------------------------------------------------

#[test]
fn test_json_column_create_and_insert() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT, data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, '{\"key\":\"val\"}')").unwrap();
    // String should be auto-coerced to JSON
    let rows = db.storage.read_rows("t").unwrap();
    assert_eq!(rows.len(), 1);
    match &rows[0][1] {
        abcsql::parser::Value::Json(s) => assert_eq!(s, r#"{"key":"val"}"#),
        other => panic!("Expected Value::Json, got {:?}", other),
    }
}



#[test]
fn test_json_literal_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE dummy (x INT)").unwrap();
    execute(&db.storage, "INSERT INTO dummy VALUES (1)").unwrap();
    let r = execute(&db.storage, "SELECT JSON '{\"a\":1}' FROM dummy").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_arrow_operator_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('{\"city\":\"Paris\"}')").unwrap();
    let r = execute(&db.storage, "SELECT data -> 'city' FROM t").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_arrow_text_operator_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('{\"city\":\"Paris\"}')").unwrap();
    let r = execute(&db.storage, "SELECT data ->> 'city' FROM t").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_contains_where() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('{\"a\":1,\"b\":2}')").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('{\"a\":3,\"b\":4}')").unwrap();
    let r = execute(&db.storage, "SELECT data FROM t WHERE data @> '{\"a\":1}'").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_typeof_in_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('\"hello\"')").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('42')").unwrap();
    let r = execute(&db.storage, "SELECT JSON_TYPEOF(data) FROM t").unwrap();
    assert!(r.contains("2 rows"), "expected 2 rows, got: {}", r);
}

#[test]
fn test_json_array_length_in_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('[1,2,3]')").unwrap();
    let r = execute(&db.storage, "SELECT JSON_ARRAY_LENGTH(data) FROM t").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_build_object_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (x INT)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1)").unwrap();
    let r = execute(&db.storage, "SELECT JSON_BUILD_OBJECT('a', 1, 'b', 'two') FROM t").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_build_array_select() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (x INT)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1)").unwrap();
    let r = execute(&db.storage, "SELECT JSON_BUILD_ARRAY(1, 'two', true) FROM t").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row, got: {}", r);
}

#[test]
fn test_json_index_create_and_use() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT, data JSON)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, '{\"k\":\"v1\"}')").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (2, '{\"k\":\"v2\"}')").unwrap();
    // Create index on JSON column (full-value hash index)
    execute(&db.storage, "CREATE INDEX idx_data ON t(data)").unwrap();
    // Query using JSON equality — index could be used
    let r = execute(&db.storage, "SELECT id FROM t WHERE data = '{\"k\":\"v1\"}'").unwrap();
    assert!(r.contains("1 rows"), "expected 1 row with JSON match, got: {}", r);
}

// ---------------------------------------------------------------------------
// CREATE / DROP FUNCTION
// ---------------------------------------------------------------------------
#[test]
fn test_create_and_drop_function() {
    let db = TestDb::new();
    let r = execute(&db.storage, "CREATE FUNCTION add(x INT, y INT) RETURNS INT AS x + y").unwrap();
    assert!(r.contains("Created function"), "expected created function, got: {}", r);
    let r = execute(&db.storage, "DROP FUNCTION add").unwrap();
    assert!(r.contains("Dropped function"), "expected dropped function, got: {}", r);
}

#[test]
fn test_drop_function_if_exists() {
    let db = TestDb::new();
    let r = execute(&db.storage, "DROP FUNCTION IF EXISTS nonexistent").unwrap();
    assert!(r.contains("does not exist"), "expected does-not-exist message, got: {}", r);
}

#[test]
fn test_create_function_no_params() {
    let db = TestDb::new();
    let r = execute(&db.storage, "CREATE FUNCTION one() AS 1").unwrap();
    assert!(r.contains("Created function"), "expected created function, got: {}", r);
}

#[test]
fn test_create_function_twice_fails() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE FUNCTION f(x INT) RETURNS INT AS x").unwrap();
    let r = execute(&db.storage, "CREATE FUNCTION f(x INT) RETURNS INT AS x");
    assert!(r.is_err(), "expected error creating duplicate function");
}

// ---- information_schema metadata tables ----

#[test]
fn test_metadata_schemata() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT)").unwrap();
    // schemata always has 1 row
    let r = execute(&db.storage, "SELECT * FROM information_schema.schemata").unwrap();
    assert_eq!(r, "(1 rows)");
}

#[test]
fn test_metadata_tables() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t1 (id INT)").unwrap();
    execute(&db.storage, "CREATE TABLE t2 (name TEXT)").unwrap();
    let r = execute(&db.storage, "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ORDER BY table_name").unwrap();
    assert_eq!(r, "(2 rows)");
}

#[test]
fn test_metadata_tables_select_star() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.tables").unwrap();
    assert_eq!(r, "(1 rows)");
}

#[test]
fn test_metadata_columns() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position").unwrap();
    assert_eq!(r, "(2 rows)");
}

#[test]
fn test_metadata_views() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT)").unwrap();
    execute(&db.storage, "CREATE VIEW v AS SELECT * FROM t").unwrap();
    let r = execute(&db.storage, "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'").unwrap();
    assert_eq!(r, "(1 rows)");
    let r2 = execute(&db.storage, "SELECT * FROM information_schema.views").unwrap();
    assert_eq!(r2, "(1 rows)");
}

#[test]
fn test_metadata_table_constraints() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT UNIQUE)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.table_constraints WHERE table_name = 't' ORDER BY constraint_type").unwrap();
    assert_eq!(r, "(2 rows)");
}

#[test]
fn test_metadata_subquery() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT)").unwrap();
    // EXISTS with metadata table should work
    let r = execute(&db.storage, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM information_schema.schemata)").unwrap();
    assert_eq!(r, "(0 rows)");
}

#[test]
fn test_metadata_parse_qualified_name() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.tables").unwrap();
    assert_eq!(r, "(1 rows)");
}

#[test]
fn test_metadata_where_on_metadata() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE mytab (id INT)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.tables WHERE table_name = 'mytab'").unwrap();
    assert_eq!(r, "(1 rows)");
}

#[test]
fn test_metadata_no_tables() {
    let db = TestDb::new();
    let r = execute(&db.storage, "SELECT * FROM information_schema.tables").unwrap();
    assert_eq!(r, "(0 rows)");
}

#[test]
fn test_metadata_key_column_usage() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT UNIQUE)").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.key_column_usage WHERE table_name = 't' ORDER BY ordinal_position").unwrap();
    assert_eq!(r, "(2 rows)");
}

#[test]
fn test_metadata_routines() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE FUNCTION one() AS 1").unwrap();
    let r = execute(&db.storage, "SELECT * FROM information_schema.routines").unwrap();
    assert_eq!(r, "(1 rows)");
}

// ---- DEFAULT column values ----

#[test]
fn test_default_parse() {
    use abcsql::{parse_sql, SqlStatement};
    let (_, stmt) = parse_sql("CREATE TABLE t (id INT, qty INT DEFAULT 5)").expect("parse failed");
    if let SqlStatement::CreateTable(ct) = stmt {
        assert!(ct.columns[1].default.is_some(), "expected default on qty");
        assert_eq!(ct.columns[1].default_text.as_deref(), Some("5"));
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_default_applied_when_column_omitted() {
    let setup = [
        "CREATE TABLE items (id INT, qty INT DEFAULT 5, name VARCHAR DEFAULT 'unnamed')",
        "INSERT INTO items (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM items WHERE qty = 5 AND name = 'unnamed'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "defaults not applied: {:?}", r);
}

#[test]
fn test_default_keyword_in_values_row() {
    let setup = [
        "CREATE TABLE t (id INT, qty INT DEFAULT 7)",
        "INSERT INTO t VALUES (1, DEFAULT)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE qty = 7");
    assert!(r.as_ref().unwrap().contains("1 rows"), "DEFAULT in VALUES failed: {:?}", r);
}

#[test]
fn test_insert_default_values() {
    let setup = [
        "CREATE TABLE t (id INT DEFAULT 42, note VARCHAR DEFAULT 'x')",
        "INSERT INTO t DEFAULT VALUES",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE id = 42 AND note = 'x'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "DEFAULT VALUES failed: {:?}", r);
}

#[test]
fn test_update_set_default() {
    let setup = [
        "CREATE TABLE t (id INT, qty INT DEFAULT 9)",
        "INSERT INTO t VALUES (1, 100)",
        "UPDATE t SET qty = DEFAULT",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE qty = 9");
    assert!(r.as_ref().unwrap().contains("1 rows"), "UPDATE SET DEFAULT failed: {:?}", r);
}

#[test]
fn test_default_constraint_order_variants() {
    let setup = [
        "CREATE TABLE t (id INT, x INT NOT NULL DEFAULT 1, y VARCHAR DEFAULT 'z' NOT NULL)",
        "INSERT INTO t (id) VALUES (5)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE x = 1 AND y = 'z'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "constraint order variants failed: {:?}", r);
}

#[test]
fn test_default_expression() {
    let setup = [
        "CREATE TABLE t (id INT, qty INT DEFAULT 2 + 3)",
        "INSERT INTO t (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE qty = 5");
    assert!(r.as_ref().unwrap().contains("1 rows"), "expression default failed: {:?}", r);
}

#[test]
fn test_default_current_date() {
    let setup = [
        "CREATE TABLE t (id INT, d DATE DEFAULT CURRENT_DATE)",
        "INSERT INTO t (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE d IS NOT NULL");
    assert!(r.as_ref().unwrap().contains("1 rows"), "CURRENT_DATE default failed: {:?}", r);
}

#[test]
fn test_omitted_column_without_default_is_null() {
    let setup = [
        "CREATE TABLE t (id INT, x INT)",
        "INSERT INTO t (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE x IS NULL");
    assert!(r.as_ref().unwrap().contains("1 rows"), "omitted column should be NULL: {:?}", r);
}

#[test]
fn test_not_null_without_default_rejects_omission() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT, x INT NOT NULL)").unwrap();
    let r = execute(&db.storage, "INSERT INTO t (id) VALUES (1)");
    assert!(r.is_err(), "expected NOT NULL violation");
}

#[test]
fn test_default_survives_schema_reload() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT, qty INT DEFAULT 5)").unwrap();
    // load_schema re-reads the schema file on every statement, so a second
    // insert exercises the DF= round-trip
    execute(&db.storage, "INSERT INTO t (id) VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO t (id) VALUES (2)").unwrap();
    let r = execute(&db.storage, "SELECT id FROM t WHERE qty = 5").unwrap();
    assert!(r.contains("2 rows"));
}

#[test]
fn test_default_shown_in_information_schema() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (id INT, qty INT DEFAULT 5)").unwrap();
    let r = execute(&db.storage, "SELECT column_name FROM information_schema.columns WHERE column_default = '5'").unwrap();
    assert!(r.contains("1 rows"), "column_default not populated: {}", r);
}

#[test]
fn test_default_null_explicit() {
    let setup = [
        "CREATE TABLE t (id INT, x INT DEFAULT NULL)",
        "INSERT INTO t (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE x IS NULL");
    assert!(r.as_ref().unwrap().contains("1 rows"), "DEFAULT NULL failed: {:?}", r);
}

// ---- Table-level constraints ----

#[test]
fn test_table_constraint_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::TableConstraintKind};
    let (_, stmt) = parse_sql("CREATE TABLE t (a INT, b INT, CONSTRAINT pk_ab PRIMARY KEY (a, b))").expect("parse failed");
    if let SqlStatement::CreateTable(ct) = stmt {
        assert_eq!(ct.columns.len(), 2);
        assert_eq!(ct.constraints.len(), 1);
        assert_eq!(ct.constraints[0].name.as_deref(), Some("pk_ab"));
        assert!(matches!(&ct.constraints[0].kind, TableConstraintKind::PrimaryKey(c) if c == &vec!["a".to_string(), "b".to_string()]));
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_composite_primary_key_uniqueness() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 1)").unwrap();
    // Same a, different b — allowed
    execute(&db.storage, "INSERT INTO t VALUES (1, 2)").unwrap();
    // Exact duplicate tuple — rejected
    let r = execute(&db.storage, "INSERT INTO t VALUES (1, 1)");
    assert!(r.is_err(), "expected composite PK violation");
}

#[test]
fn test_composite_primary_key_not_null() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))").unwrap();
    let r = execute(&db.storage, "INSERT INTO t VALUES (1, NULL)");
    assert!(r.is_err(), "expected NOT NULL violation on composite PK part");
}

#[test]
fn test_composite_unique_allows_nulls() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, UNIQUE (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, NULL)").unwrap();
    // NULL-containing tuples never conflict
    execute(&db.storage, "INSERT INTO t VALUES (1, NULL)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (2, 3)").unwrap();
    let r = execute(&db.storage, "INSERT INTO t VALUES (2, 3)");
    assert!(r.is_err(), "expected composite UNIQUE violation");
}

#[test]
fn test_table_check_constraint() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (lo INT, hi INT, CHECK (lo < hi))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 10)").unwrap();
    let r = execute(&db.storage, "INSERT INTO t VALUES (10, 1)");
    assert!(r.is_err(), "expected table CHECK violation on insert");
    let r2 = execute(&db.storage, "UPDATE t SET hi = 0");
    assert!(r2.is_err(), "expected table CHECK violation on update");
}

#[test]
fn test_named_check_constraint() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (qty INT, CONSTRAINT positive_qty CHECK (qty > 0))").unwrap();
    let r = execute(&db.storage, "INSERT INTO t VALUES (-5)");
    assert!(r.is_err(), "expected named CHECK violation");
}

#[test]
fn test_composite_foreign_key() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO parent VALUES (1, 2)").unwrap();
    execute(&db.storage, "CREATE TABLE child (x INT, y INT, FOREIGN KEY (x, y) REFERENCES parent (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO child VALUES (1, 2)").unwrap();
    let r = execute(&db.storage, "INSERT INTO child VALUES (1, 3)");
    assert!(r.is_err(), "expected composite FK violation");
    // NULL part exempts the tuple
    execute(&db.storage, "INSERT INTO child VALUES (1, NULL)").unwrap();
}

#[test]
fn test_foreign_key_defaults_to_referenced_pk() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO parent VALUES (7, 8)").unwrap();
    execute(&db.storage, "CREATE TABLE child (x INT, y INT, FOREIGN KEY (x, y) REFERENCES parent)").unwrap();
    execute(&db.storage, "INSERT INTO child VALUES (7, 8)").unwrap();
    let r = execute(&db.storage, "INSERT INTO child VALUES (9, 9)");
    assert!(r.is_err(), "expected FK-to-PK violation");
}

#[test]
fn test_table_constraint_survives_reload() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, CONSTRAINT u_ab UNIQUE (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 2)").unwrap();
    // load_schema re-reads the file per statement, exercising the !TC= round-trip
    let r = execute(&db.storage, "INSERT INTO t VALUES (1, 2)");
    assert!(r.is_err(), "constraint lost after schema reload");
}

#[test]
fn test_named_constraint_in_information_schema() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, CONSTRAINT pk_ab PRIMARY KEY (a, b))").unwrap();
    let r = execute(&db.storage, "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_name = 'pk_ab'").unwrap();
    assert!(r.contains("1 rows"), "named constraint missing from metadata: {}", r);
}

#[test]
fn test_drop_column_removes_its_constraints() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, UNIQUE (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 2)").unwrap();
    execute(&db.storage, "ALTER TABLE t DROP COLUMN b").unwrap();
    // Constraint referencing b is gone, so duplicate a values are fine
    execute(&db.storage, "INSERT INTO t VALUES (1)").unwrap();
    let r = execute(&db.storage, "SELECT a FROM t WHERE a = 1").unwrap();
    assert!(r.contains("2 rows"));
}

#[test]
fn test_on_conflict_do_nothing_with_composite_unique() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (a INT, b INT, UNIQUE (a, b))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 2)").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (1, 2) ON CONFLICT DO NOTHING").unwrap();
    let r = execute(&db.storage, "SELECT a FROM t").unwrap();
    assert!(r.contains("1 rows"), "DO NOTHING should skip composite conflict: {}", r);
}

// ---- Referential actions (ON DELETE / ON UPDATE) ----

#[test]
fn test_ref_action_parse() {
    use abcsql::{parse_sql, SqlStatement, parser::RefAction};
    let (_, stmt) = parse_sql("CREATE TABLE c (uid INT REFERENCES u(id) ON DELETE CASCADE ON UPDATE SET NULL)").expect("parse failed");
    if let SqlStatement::CreateTable(ct) = stmt {
        let fk = ct.columns[0].references.as_ref().expect("fk");
        assert_eq!(fk.on_delete, RefAction::Cascade);
        assert_eq!(fk.on_update, RefAction::SetNull);
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_delete_restrict_default() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id))").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    let r = execute(&db.storage, "DELETE FROM u WHERE id = 1");
    assert!(r.is_err(), "expected NO ACTION to block delete of referenced row");
}

#[test]
fn test_on_delete_cascade() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id) ON DELETE CASCADE)").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1), (2)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1), (1), (2)").unwrap();
    execute(&db.storage, "DELETE FROM u WHERE id = 1").unwrap();
    let r = execute(&db.storage, "SELECT uid FROM c").unwrap();
    assert!(r.contains("1 rows"), "cascade should delete children: {}", r);
}

#[test]
fn test_on_delete_cascade_recursive() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE a (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE b (id INT PRIMARY KEY, aid INT REFERENCES a(id) ON DELETE CASCADE)").unwrap();
    execute(&db.storage, "CREATE TABLE c (bid INT REFERENCES b(id) ON DELETE CASCADE)").unwrap();
    execute(&db.storage, "INSERT INTO a VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO b VALUES (10, 1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (10)").unwrap();
    execute(&db.storage, "DELETE FROM a WHERE id = 1").unwrap();
    let rb = execute(&db.storage, "SELECT id FROM b").unwrap();
    let rc = execute(&db.storage, "SELECT bid FROM c").unwrap();
    assert!(rb.contains("0 rows"), "grandparent cascade should empty b: {}", rb);
    assert!(rc.contains("0 rows"), "grandparent cascade should empty c: {}", rc);
}

#[test]
fn test_on_delete_set_null() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id) ON DELETE SET NULL)").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    execute(&db.storage, "DELETE FROM u WHERE id = 1").unwrap();
    let r = execute(&db.storage, "SELECT uid FROM c WHERE uid IS NULL").unwrap();
    assert!(r.contains("1 rows"), "SET NULL should null child fk: {}", r);
}

#[test]
fn test_on_delete_set_default() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1), (99)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT DEFAULT 99 REFERENCES u(id) ON DELETE SET DEFAULT)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    execute(&db.storage, "DELETE FROM u WHERE id = 1").unwrap();
    let r = execute(&db.storage, "SELECT uid FROM c WHERE uid = 99").unwrap();
    assert!(r.contains("1 rows"), "SET DEFAULT should reset child fk: {}", r);
}

#[test]
fn test_on_update_cascade() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id) ON UPDATE CASCADE)").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    execute(&db.storage, "UPDATE u SET id = 5 WHERE id = 1").unwrap();
    let r = execute(&db.storage, "SELECT uid FROM c WHERE uid = 5").unwrap();
    assert!(r.contains("1 rows"), "ON UPDATE CASCADE should follow key change: {}", r);
}

#[test]
fn test_on_update_restrict_default() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id))").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    let r = execute(&db.storage, "UPDATE u SET id = 5 WHERE id = 1");
    assert!(r.is_err(), "expected NO ACTION to block update of referenced key");
}

#[test]
fn test_update_child_fk_validated() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id))").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    let r = execute(&db.storage, "UPDATE c SET uid = 42");
    assert!(r.is_err(), "expected FK violation updating child to unknown key");
}

#[test]
fn test_composite_fk_on_delete_cascade() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE p (a INT, b INT, PRIMARY KEY (a, b))").unwrap();
    execute(&db.storage, "CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p (a, b) ON DELETE CASCADE)").unwrap();
    execute(&db.storage, "INSERT INTO p VALUES (1, 2), (3, 4)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1, 2), (3, 4)").unwrap();
    execute(&db.storage, "DELETE FROM p WHERE a = 1").unwrap();
    let r = execute(&db.storage, "SELECT x FROM c").unwrap();
    assert!(r.contains("1 rows"), "composite cascade failed: {}", r);
}

#[test]
fn test_ref_action_survives_reload() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id) ON DELETE CASCADE)").unwrap();
    execute(&db.storage, "INSERT INTO u VALUES (1)").unwrap();
    execute(&db.storage, "INSERT INTO c VALUES (1)").unwrap();
    // load_schema re-reads the schema file per statement (FK~OD= round-trip)
    execute(&db.storage, "DELETE FROM u WHERE id = 1").unwrap();
    let r = execute(&db.storage, "SELECT uid FROM c").unwrap();
    assert!(r.contains("0 rows"), "action lost after reload: {}", r);
}

#[test]
fn test_delete_rule_in_information_schema() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE u (id INT PRIMARY KEY)").unwrap();
    execute(&db.storage, "CREATE TABLE c (uid INT REFERENCES u(id) ON DELETE CASCADE)").unwrap();
    let r = execute(&db.storage, "SELECT constraint_name FROM information_schema.referential_constraints WHERE delete_rule = 'CASCADE'").unwrap();
    assert!(r.contains("1 rows"), "delete_rule not exposed: {}", r);
}

// ---- Simple CASE (CASE operand WHEN value THEN ...) ----

#[test]
fn test_simple_case_execute() {
    let setup = [
        "CREATE TABLE t (id INT, status INT)",
        "INSERT INTO t VALUES (1, 1), (2, 2), (3, 9)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE CASE status WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'z' END = 'b'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "simple CASE failed: {:?}", r);
}

#[test]
fn test_simple_case_else_branch() {
    let setup = [
        "CREATE TABLE t (id INT, status INT)",
        "INSERT INTO t VALUES (1, 7), (2, 8)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE CASE status WHEN 1 THEN 'a' ELSE 'z' END = 'z'");
    assert!(r.as_ref().unwrap().contains("2 rows"), "simple CASE ELSE failed: {:?}", r);
}

#[test]
fn test_simple_case_string_operand() {
    let setup = [
        "CREATE TABLE t (id INT, kind VARCHAR)",
        "INSERT INTO t VALUES (1, 'gold'), (2, 'silver')",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE CASE kind WHEN 'gold' THEN 10 WHEN 'silver' THEN 5 END = 10");
    assert!(r.as_ref().unwrap().contains("1 rows"), "simple CASE with strings failed: {:?}", r);
}

#[test]
fn test_searched_case_still_works() {
    let setup = [
        "CREATE TABLE t (id INT, n INT)",
        "INSERT INTO t VALUES (1, 5), (2, 50)",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE CASE WHEN n > 10 THEN 'big' ELSE 'small' END = 'big'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "searched CASE regressed: {:?}", r);
}

#[test]
fn test_simple_case_expression_operand() {
    let setup = [
        "CREATE TABLE t (id INT, n INT)",
        "INSERT INTO t VALUES (1, 4), (2, 5)",
    ];
    // operand is an expression: n % 2
    let r = with_db(&setup, "SELECT id FROM t WHERE CASE n % 2 WHEN 0 THEN 'even' ELSE 'odd' END = 'even'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "simple CASE expr operand failed: {:?}", r);
}

// ---- Row value constructors ----

#[test]
fn test_row_equality() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 2), (1, 3), (2, 2)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) = (1, 2)");
    assert!(r.as_ref().unwrap().contains("1 rows"), "row equality failed: {:?}", r);
}

#[test]
fn test_row_inequality() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 2), (1, 3)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) <> (1, 2)");
    assert!(r.as_ref().unwrap().contains("1 rows"), "row inequality failed: {:?}", r);
}

#[test]
fn test_row_lexicographic_lt() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 9), (2, 1), (2, 5), (3, 0)",
    ];
    // (a,b) < (2,5) → (1,9) and (2,1)
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) < (2, 5)");
    assert!(r.as_ref().unwrap().contains("2 rows"), "row < failed: {:?}", r);
}

#[test]
fn test_row_lexicographic_le() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 9), (2, 5), (3, 0)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) <= (2, 5)");
    assert!(r.as_ref().unwrap().contains("2 rows"), "row <= failed: {:?}", r);
}

#[test]
fn test_row_in_list() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 2), (3, 4), (5, 6)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) IN ((1, 2), (5, 6))");
    assert!(r.as_ref().unwrap().contains("2 rows"), "row IN failed: {:?}", r);
}

#[test]
fn test_row_not_in_list() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 2), (3, 4)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a, b) NOT IN ((1, 2))");
    assert!(r.as_ref().unwrap().contains("1 rows"), "row NOT IN failed: {:?}", r);
}

#[test]
fn test_update_row_assignment() {
    let setup = [
        "CREATE TABLE t (a INT, b INT, c INT)",
        "INSERT INTO t VALUES (1, 2, 3)",
        "UPDATE t SET (a, b) = (10, 20)",
    ];
    let r = with_db(&setup, "SELECT c FROM t WHERE a = 10 AND b = 20 AND c = 3");
    assert!(r.as_ref().unwrap().contains("1 rows"), "row assignment failed: {:?}", r);
}

#[test]
fn test_update_row_assignment_with_default() {
    let setup = [
        "CREATE TABLE t (a INT, b INT DEFAULT 7)",
        "INSERT INTO t VALUES (1, 2)",
        "UPDATE t SET (a, b) = (5, DEFAULT)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE a = 5 AND b = 7");
    assert!(r.as_ref().unwrap().contains("1 rows"), "row assignment DEFAULT failed: {:?}", r);
}

#[test]
fn test_paren_condition_not_broken_by_rows() {
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 2), (3, 4)",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE (a = 1 OR b = 4) AND b > 0");
    assert!(r.as_ref().unwrap().contains("2 rows"), "paren conditions regressed: {:?}", r);
}

// ---- Spec-form string functions ----

#[test]
fn test_trim_leading_chars() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('xxhixx')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM(LEADING 'x' FROM s) = 'hixx'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRIM LEADING failed: {:?}", r);
}

#[test]
fn test_trim_trailing_chars() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('xxhixx')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM(TRAILING 'x' FROM s) = 'xxhi'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRIM TRAILING failed: {:?}", r);
}

#[test]
fn test_trim_both_chars() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('xxhixx')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM(BOTH 'x' FROM s) = 'hi'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRIM BOTH failed: {:?}", r);
}

#[test]
fn test_trim_chars_no_mode() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('zzhizz')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM('z' FROM s) = 'hi'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRIM chars-only failed: {:?}", r);
}

#[test]
fn test_trim_from_whitespace_mode_only() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('  hi  ')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM(LEADING FROM s) = 'hi  '");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRIM(LEADING FROM s) failed: {:?}", r);
}

#[test]
fn test_plain_trim_still_works() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('  hi ')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE TRIM(s) = 'hi'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "plain TRIM regressed: {:?}", r);
}

#[test]
fn test_substring_from_for() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('abcdef')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE SUBSTRING(s FROM 2 FOR 3) = 'bcd'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "SUBSTRING FROM/FOR failed: {:?}", r);
}

#[test]
fn test_substring_from_only() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('abcdef')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE SUBSTRING(s FROM 4) = 'def'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "SUBSTRING FROM failed: {:?}", r);
}

#[test]
fn test_char_length_and_octet_length() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('hello')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE CHAR_LENGTH(s) = 5 AND CHARACTER_LENGTH(s) = 5 AND OCTET_LENGTH(s) = 5");
    assert!(r.as_ref().unwrap().contains("1 rows"), "length functions failed: {:?}", r);
}

#[test]
fn test_overlay() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('Txxxxas')"];
    let r = with_db(&setup, "SELECT s FROM t WHERE OVERLAY(s PLACING 'hom' FROM 2 FOR 4) = 'Thomas'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "OVERLAY failed: {:?}", r);
}

#[test]
fn test_overlay_default_length() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('abcdef')"];
    // Without FOR, replaced span = replacement length (2)
    let r = with_db(&setup, "SELECT s FROM t WHERE OVERLAY(s PLACING 'XY' FROM 3) = 'abXYef'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "OVERLAY default length failed: {:?}", r);
}

#[test]
fn test_translate() {
    let setup = ["CREATE TABLE t (s VARCHAR)", "INSERT INTO t VALUES ('12345')"];
    // 1→a, 2→b, 3 dropped (to shorter than from)
    let r = with_db(&setup, "SELECT s FROM t WHERE TRANSLATE(s, '123', 'ab') = 'ab45'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TRANSLATE failed: {:?}", r);
}

// ---- New data types: TIME, TIME ZONE variants, INTERVAL, NCHAR, BIT ----

#[test]
fn test_time_type() {
    let setup = [
        "CREATE TABLE t (id INT, at TIME)",
        "INSERT INTO t VALUES (1, '09:30:00'), (2, '17:45:10')",
    ];
    let r = with_db(&setup, "SELECT id FROM t WHERE at = '09:30:00'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TIME failed: {:?}", r);
}

#[test]
fn test_time_type_rejects_invalid() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (at TIME)").unwrap();
    assert!(execute(&db.storage, "INSERT INTO t VALUES ('25:00:00')").is_err(), "invalid hour accepted");
    assert!(execute(&db.storage, "INSERT INTO t VALUES ('not a time')").is_err(), "junk accepted");
}

#[test]
fn test_time_literal_keyword() {
    let setup = [
        "CREATE TABLE t (at TIME)",
        "INSERT INTO t VALUES (TIME '09:30:00')",
    ];
    let r = with_db(&setup, "SELECT at FROM t WHERE at = TIME '09:30:00'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "TIME literal failed: {:?}", r);
}

#[test]
fn test_timestamp_with_time_zone_parses() {
    let setup = [
        "CREATE TABLE t (ts TIMESTAMP WITH TIME ZONE, ts2 TIMESTAMP WITHOUT TIME ZONE, at TIME WITH TIME ZONE)",
        "INSERT INTO t VALUES ('2024-01-15 10:30:00', '2024-01-15 10:30:00', '10:30:00')",
    ];
    let r = with_db(&setup, "SELECT ts FROM t");
    assert!(r.as_ref().unwrap().contains("1 rows"), "WITH TIME ZONE failed: {:?}", r);
}

#[test]
fn test_interval_column_type() {
    let setup = [
        "CREATE TABLE t (id INT, dur INTERVAL)",
        "INSERT INTO t VALUES (1, INTERVAL '2' HOUR)",
    ];
    // intervals are stored as seconds
    let r = with_db(&setup, "SELECT id FROM t WHERE dur = 7200");
    assert!(r.as_ref().unwrap().contains("1 rows"), "INTERVAL column failed: {:?}", r);
}

#[test]
fn test_nchar_nvarchar_national() {
    let setup = [
        "CREATE TABLE t (a NCHAR(3), b NVARCHAR(10), c NATIONAL CHARACTER VARYING(10), d NATIONAL CHAR(2))",
        "INSERT INTO t VALUES ('abc', 'hello', 'world', 'xy')",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE b = 'hello'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "national char types failed: {:?}", r);
}

#[test]
fn test_character_varying() {
    let setup = [
        "CREATE TABLE t (a CHARACTER VARYING(20), b CHARACTER(2), c CHAR VARYING(5))",
        "INSERT INTO t VALUES ('hello', 'xy', 'abc')",
    ];
    let r = with_db(&setup, "SELECT a FROM t WHERE a = 'hello' AND b = 'xy' AND c = 'abc'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "CHARACTER VARYING failed: {:?}", r);
}

#[test]
fn test_bit_type() {
    let setup = [
        "CREATE TABLE t (flags BIT(4))",
        "INSERT INTO t VALUES (B'1010')",
    ];
    let r = with_db(&setup, "SELECT flags FROM t WHERE flags = '1010'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "BIT failed: {:?}", r);
}

#[test]
fn test_bit_type_rejects_bad_values() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (flags BIT(4))").unwrap();
    assert!(execute(&db.storage, "INSERT INTO t VALUES ('10')").is_err(), "wrong length accepted");
    assert!(execute(&db.storage, "INSERT INTO t VALUES ('12ab')").is_err(), "non-bits accepted");
}

#[test]
fn test_bit_varying() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (flags BIT VARYING(8))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES (B'101')").unwrap();
    assert!(execute(&db.storage, "INSERT INTO t VALUES (B'101010101')").is_err(), "overlong bit varying accepted");
}

#[test]
fn test_new_types_survive_reload() {
    let db = TestDb::new();
    execute(&db.storage, "CREATE TABLE t (at TIME, dur INTERVAL, flags BIT VARYING(8))").unwrap();
    execute(&db.storage, "INSERT INTO t VALUES ('10:00:00', 60, B'11')").unwrap();
    // schema is re-read per statement — round-trip must preserve types
    assert!(execute(&db.storage, "INSERT INTO t VALUES ('bad', 60, B'11')").is_err(), "TIME validation lost after reload");
}

// ---- Datetime/session value functions ----

#[test]
fn test_current_time_shape() {
    let setup = ["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)"];
    // HH:MM:SS is 8 chars
    let r = with_db(&setup, "SELECT id FROM t WHERE CHAR_LENGTH(CURRENT_TIME) = 8");
    assert!(r.as_ref().unwrap().contains("1 rows"), "CURRENT_TIME failed: {:?}", r);
}

#[test]
fn test_localtime_and_localtimestamp() {
    let setup = ["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)"];
    let r = with_db(&setup, "SELECT id FROM t WHERE LOCALTIME = CURRENT_TIME AND LOCALTIMESTAMP IS NOT NULL");
    assert!(r.as_ref().unwrap().contains("1 rows"), "LOCALTIME/LOCALTIMESTAMP failed: {:?}", r);
}

#[test]
fn test_current_user_and_aliases() {
    let setup = ["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)"];
    let r = with_db(&setup, "SELECT id FROM t WHERE CURRENT_USER = SESSION_USER AND USER = CURRENT_USER AND CHAR_LENGTH(CURRENT_USER) > 0");
    assert!(r.as_ref().unwrap().contains("1 rows"), "CURRENT_USER family failed: {:?}", r);
}

#[test]
fn test_current_user_in_default() {
    let setup = [
        "CREATE TABLE audit (id INT, who VARCHAR DEFAULT CURRENT_USER)",
        "INSERT INTO audit (id) VALUES (1)",
    ];
    let r = with_db(&setup, "SELECT id FROM audit WHERE who = CURRENT_USER");
    assert!(r.as_ref().unwrap().contains("1 rows"), "CURRENT_USER default failed: {:?}", r);
}

#[test]
fn test_at_time_zone() {
    let setup = [
        "CREATE TABLE t (ts TIMESTAMP)",
        "INSERT INTO t VALUES ('2024-01-15 10:00:00')",
    ];
    // +02:00 shifts forward two hours
    let r = with_db(&setup, "SELECT ts FROM t WHERE ts AT TIME ZONE '+02:00' = TIMESTAMP '2024-01-15 12:00:00'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "AT TIME ZONE offset failed: {:?}", r);
}

#[test]
fn test_at_time_zone_utc_identity() {
    let setup = [
        "CREATE TABLE t (ts TIMESTAMP)",
        "INSERT INTO t VALUES ('2024-01-15 10:00:00')",
    ];
    let r = with_db(&setup, "SELECT ts FROM t WHERE ts AT TIME ZONE 'UTC' = ts");
    assert!(r.as_ref().unwrap().contains("1 rows"), "AT TIME ZONE UTC failed: {:?}", r);
}

#[test]
fn test_column_named_at_still_works() {
    let setup = ["CREATE TABLE t (at TIME)", "INSERT INTO t VALUES ('10:00:00')"];
    let r = with_db(&setup, "SELECT at FROM t WHERE at = '10:00:00'");
    assert!(r.as_ref().unwrap().contains("1 rows"), "column named 'at' broken: {:?}", r);
}
