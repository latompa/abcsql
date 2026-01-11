# ABCSQL

A lightweight, file-based SQL database written in Rust.

## Features

### 1. SQL Support

ABCSQL provides support for core SQL operations:

- **SELECT**: Query data from tables with filtering and projection
- **INSERT**: Add new records to tables
- **CREATE TABLE**: Define table schemas with column types and constraints

### 2. File-Based Backend

Tables are persisted as files on disk:

- Each table is stored as a separate file (e.g., `users.data` for a table named `users`)
- Simple, portable storage format
- No external database server required

### 3. Query Planner

Advanced query planning capabilities:

- **Multi-table joins**: Support for joining 2 or more tables
- Efficient query execution plans
- Optimized join algorithms for better performance

## Getting Started

```bash
# Build the project
cargo build

# Run the project
cargo run
```

## Example Usage

```sql
-- Create a table
CREATE TABLE users (id INT, name VARCHAR(255), email VARCHAR(255));

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

-- Query data
SELECT * FROM users;
SELECT name, email FROM users WHERE id = 1;

-- Join tables
SELECT u.name, o.product 
FROM users u 
JOIN orders o ON u.id = o.user_id;
```

## Project Status

🚧 In Development

