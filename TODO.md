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
- [x] LIKE operator for string matching
- [x] Arithmetic operators in expressions
- [x] Subqueries (WHERE ... IN (SELECT ...))

## Data Types

- [x] FLOAT / DOUBLE
- [x] BOOLEAN
- [x] DATE / TIMESTAMP
- [x] AUTO_INCREMENT
- [ ] JSON

## Schema & Constraints

- [x] PRIMARY KEY constraint
- [x] FOREIGN KEY constraint
- [x] NOT NULL constraint
- [x] UNIQUE constraint
- [ ] ALTER TABLE statement

## Additional
- [ ] COALESCE
- [ ] CAST functions (string to int etc)

## Performance & Storage

- [ ] Indexing (B-tree or hash)
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)

## Tooling

- [ ] Interactive REPL / SQL shell

## Common table structures
- [ ] 1 - n (customer has many orders)
- [ ] 1 - n - 1 (product has many tags (via join table))
