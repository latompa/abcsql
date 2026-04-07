# TODO

## High Priority

- [x] Fix JOIN implementation - 8 ignored tests in `parser.rs` ready to validate
- [x] Build query executor layer to apply WHERE/JOIN logic to SELECT statements
- [x] Add SELECT column selection (currently only `SELECT *` works)

## SQL Features

- [x] ORDER BY clause
- [x] LIMIT clause
- [x] GROUP BY clause
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [x] DISTINCT keyword
- [ ] LIKE operator for string matching
- [ ] Arithmetic operators in expressions
- [ ] Subqueries

## Data Types

- [ ] FLOAT / DOUBLE
- [ ] BOOLEAN
- [ ] DATE / TIMESTAMP
- [ ] AUTO_INCREMENT

## Schema & Constraints

- [ ] PRIMARY KEY constraint
- [ ] FOREIGN KEY constraint
- [ ] NOT NULL constraint
- [ ] UNIQUE constraint
- [ ] ALTER TABLE statement

## Performance & Storage

- [ ] Indexing (B-tree or hash)
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)

## Tooling

- [ ] Interactive REPL / SQL shell
