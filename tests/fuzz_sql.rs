// Regression/fuzz test: generates random SQL statements (valid and invalid)
// and runs them through the parser and storage engine, ensuring no panics.

mod common;
use common::TestDb;

const NUM_ITERATIONS: usize = 500;

/// Simple deterministic PRNG (xorshift64)
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }

    fn pick<'a>(&mut self, items: &'a [&str]) -> &'a str {
        items[self.next() as usize % items.len()]
    }

    fn bool(&mut self) -> bool {
        self.next() % 2 == 0
    }
}

fn random_identifier(rng: &mut Rng) -> String {
    let names = ["foo", "bar", "baz", "t1", "t2", "users", "orders", "x", "id", "name", "age", "val", "count"];
    names[rng.next() as usize % names.len()].to_string()
}

fn random_value(rng: &mut Rng) -> String {
    match rng.range(0, 4) {
        0 => format!("{}", rng.range(0, 1000) as i64 - 500),
        1 => format!("'{}'", random_identifier(rng)),
        2 => "NULL".to_string(),
        _ => format!("{}", rng.range(0, 99999)),
    }
}

fn random_type(rng: &mut Rng) -> String {
    match rng.range(0, 4) {
        0 => "INT".to_string(),
        1 => "VARCHAR".to_string(),
        2 => format!("VARCHAR({})", rng.range(1, 256)),
        _ => rng.pick(&["INT", "VARCHAR", "BIGINT", "TEXT", "BOOLEAN"]).to_string(),
    }
}

fn random_operator(rng: &mut Rng) -> &'static str {
    let ops = ["=", "!=", "<>", ">", "<", ">=", "<="];
    ops[rng.next() as usize % ops.len()]
}

fn gen_create_table(rng: &mut Rng) -> String {
    let table = random_identifier(rng);
    let ncols = rng.range(1, 5);
    let cols: Vec<String> = (0..ncols)
        .map(|_| format!("{} {}", random_identifier(rng), random_type(rng)))
        .collect();
    format!("CREATE TABLE {} ({})", table, cols.join(", "))
}

fn gen_insert(rng: &mut Rng) -> String {
    let table = random_identifier(rng);
    let nvals = rng.range(1, 5);
    let vals: Vec<String> = (0..nvals).map(|_| random_value(rng)).collect();
    format!("INSERT INTO {} VALUES ({})", table, vals.join(", "))
}

fn gen_select(rng: &mut Rng) -> String {
    let table = random_identifier(rng);
    let cols = if rng.bool() {
        "*".to_string()
    } else {
        let n = rng.range(1, 4);
        (0..n).map(|_| random_identifier(rng)).collect::<Vec<_>>().join(", ")
    };

    let mut sql = format!("SELECT {} FROM {}", cols, table);

    // maybe add WHERE
    if rng.bool() {
        sql += &format!(
            " WHERE {} {} {}",
            random_identifier(rng),
            random_operator(rng),
            random_value(rng)
        );
    }

    // maybe add ORDER BY
    if rng.bool() {
        sql += &format!(" ORDER BY {}", random_identifier(rng));
        if rng.bool() {
            sql += " DESC";
        }
    }

    // maybe add LIMIT
    if rng.bool() {
        sql += &format!(" LIMIT {}", rng.range(1, 100));
    }

    sql
}

fn gen_update(rng: &mut Rng) -> String {
    let table = random_identifier(rng);
    let nassign = rng.range(1, 3);
    let assignments: Vec<String> = (0..nassign)
        .map(|_| format!("{} = {}", random_identifier(rng), random_value(rng)))
        .collect();
    let mut sql = format!("UPDATE {} SET {}", table, assignments.join(", "));
    if rng.bool() {
        sql += &format!(
            " WHERE {} {} {}",
            random_identifier(rng),
            random_operator(rng),
            random_value(rng)
        );
    }
    sql
}

fn gen_delete(rng: &mut Rng) -> String {
    let table = random_identifier(rng);
    let mut sql = format!("DELETE FROM {}", table);
    if rng.bool() {
        sql += &format!(
            " WHERE {} {} {}",
            random_identifier(rng),
            random_operator(rng),
            random_value(rng)
        );
    }
    sql
}

fn gen_garbage(rng: &mut Rng) -> String {
    let templates = [
        "SELEC * FORM foo",
        "INSERT foo VALUES (1)",
        "CREATE foo (a INT)",
        "DROP TABLE foo",
        "",
        "   ",
        ";;;",
        "SELECT",
        "SELECT FROM",
        "CREATE TABLE",
        "INSERT INTO",
        "DELETE",
        "UPDATE SET",
        "SELECT * FROM foo WHERE",
        "SELECT * FROM foo ORDER BY",
        "SELECT * FROM foo LIMIT",
        "SELECT * FROM foo LIMIT -1",
        "CREATE TABLE t ()",
        "INSERT INTO t VALUES ()",
        "SELECT 1 + 2",
        "SELECT * FROM foo JOIN",
        "SELECT * FROM foo JOIN bar",
        "SELECT * FROM foo JOIN bar ON",
    ];
    templates[rng.next() as usize % templates.len()].to_string()
}

fn gen_random_sql(rng: &mut Rng) -> String {
    match rng.range(0, 7) {
        0 => gen_create_table(rng),
        1 => gen_insert(rng),
        2 => gen_select(rng),
        3 => gen_update(rng),
        4 => gen_delete(rng),
        5 | 6 => gen_garbage(rng),
        _ => unreachable!(),
    }
}

#[test]
fn fuzz_parser_does_not_panic() {
    let mut rng = Rng::new(12345);
    for _ in 0..NUM_ITERATIONS {
        let sql = gen_random_sql(&mut rng);
        // just make sure parse_sql doesn't panic
        let _ = abcsql::parse_sql(&sql);
    }
}

#[test]
fn fuzz_database_does_not_panic() {
    let db = TestDb::new();

    // seed with a valid table so operations have something to hit
    let _ = abcsql::execute(&db.storage, "CREATE TABLE t1 (id INT, name VARCHAR)");
    let _ = abcsql::execute(&db.storage, "INSERT INTO t1 VALUES (1, 'alice')");
    let _ = abcsql::execute(&db.storage, "INSERT INTO t1 VALUES (2, 'bob')");
    let _ = abcsql::execute(&db.storage, "CREATE TABLE t2 (id INT, val INT)");
    let _ = abcsql::execute(&db.storage, "INSERT INTO t2 VALUES (1, 100)");

    let mut rng = Rng::new(67890);
    for _ in 0..NUM_ITERATIONS {
        let sql = gen_random_sql(&mut rng);
        // execute should never panic, errors are fine
        let _ = abcsql::execute(&db.storage, &sql);
    }
}

#[test]
fn fuzz_mixed_workload() {
    // runs a longer mixed workload with a fixed seed for reproducibility
    let db = TestDb::new();
    let mut rng = Rng::new(0xDEAD_BEEF);

    for _ in 0..NUM_ITERATIONS * 2 {
        let sql = gen_random_sql(&mut rng);
        let _ = abcsql::execute(&db.storage, &sql);
    }
}
