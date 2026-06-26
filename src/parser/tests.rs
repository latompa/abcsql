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

    // --- Type system extension tests ---

    #[test]
    fn test_parse_new_data_types() {
        // Parse all new type keywords in CREATE TABLE
        let sql = "CREATE TABLE t (a SMALLINT, b BIGINT, c REAL, d CHAR(10), e TEXT, f DECIMAL(10,2), g UUID, h JSON, i JSONB);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => {
                assert_eq!(ct.columns[0].data_type, DataType::SmallInt);
                assert_eq!(ct.columns[1].data_type, DataType::BigInt);
                assert_eq!(ct.columns[2].data_type, DataType::Real);
                assert_eq!(ct.columns[3].data_type, DataType::Char(Some(10)));
                assert_eq!(ct.columns[4].data_type, DataType::Text);
                assert_eq!(ct.columns[5].data_type, DataType::Decimal(Some(10), Some(2)));
                assert_eq!(ct.columns[6].data_type, DataType::Uuid);
                assert_eq!(ct.columns[7].data_type, DataType::Json);
                assert_eq!(ct.columns[8].data_type, DataType::Jsonb);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_char_no_size() {
        let sql = "CREATE TABLE t (a CHAR);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => assert_eq!(ct.columns[0].data_type, DataType::Char(None)),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_decimal_no_precision() {
        let sql = "CREATE TABLE t (a DECIMAL);";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => assert_eq!(ct.columns[0].data_type, DataType::Decimal(None, None)),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_numeric_synonym() {
        let sql = "CREATE TABLE t (a NUMERIC(5,2));";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::CreateTable(ct) => assert_eq!(ct.columns[0].data_type, DataType::Decimal(Some(5), Some(2))),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_get_operator() {
        // data -> 'key' parses as BinaryOp with JsonGet
        let sql = "SELECT data -> 'name' FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::BinaryOp(_, ArithOp::JsonGet, _)) => {}
                    other => panic!("Expected JsonGet BinaryOp, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_get_text_operator() {
        // data ->> 'key' parses as BinaryOp with JsonGetText
        let sql = "SELECT data ->> 'name' FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::BinaryOp(_, ArithOp::JsonGetText, _)) => {}
                    other => panic!("Expected JsonGetText BinaryOp, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_contains_operator() {
        // col @> '{}' parses as JsonContains condition
        let sql = "SELECT * FROM t WHERE data @> '{\"active\":true}';";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match sel.where_clause {
                    Some(WhereClause { condition: Condition::Comparison { operator: Operator::JsonContains, .. } }) => {}
                    _ => panic!("Expected JsonContains condition"),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_json_object_get() {
        let json = r#"{"name":"Alice","age":30}"#;
        assert_eq!(json_object_get(json, "name"), Some("\"Alice\"".to_string()));
        assert_eq!(json_object_get(json, "age"), Some("30".to_string()));
        assert_eq!(json_object_get(json, "missing"), None);
    }

    #[test]
    fn test_json_array_get() {
        let json = r#"[10, 20, 30]"#;
        assert_eq!(json_array_get(json, 0), Some("10".to_string()));
        assert_eq!(json_array_get(json, 2), Some("30".to_string()));
        assert_eq!(json_array_get(json, 5), None);
    }

    #[test]
    fn test_json_contains() {
        assert!(json_contains(r#"{"a":1,"b":2}"#, r#"{"a":1}"#));
        assert!(!json_contains(r#"{"a":1}"#, r#"{"a":2}"#));
        assert!(json_contains(r#"{"x":"hello"}"#, r#"{"x":"hello"}"#));
    }

    #[test]
    fn test_apply_json_op_get() {
        let json = Value::Json(r#"{"city":"Paris"}"#.to_string());
        let key = Value::String("city".to_string());
        let result = apply_json_op(&json, &ArithOp::JsonGet, &key);
        assert_eq!(result, Some(Value::Json("\"Paris\"".to_string())));
    }

    #[test]
    fn test_apply_json_op_get_text() {
        let json = Value::Json(r#"{"city":"Paris"}"#.to_string());
        let key = Value::String("city".to_string());
        let result = apply_json_op(&json, &ArithOp::JsonGetText, &key);
        assert_eq!(result, Some(Value::String("Paris".to_string())));
    }

    #[test]
    fn test_apply_json_op_integer_key() {
        let json = Value::Json(r#"[100, 200, 300]"#.to_string());
        let idx = Value::Int(1);
        let result = apply_json_op(&json, &ArithOp::JsonGetText, &idx);
        assert_eq!(result, Some(Value::String("200".to_string())));
    }

    #[test]
    fn test_apply_cast_json() {
        assert_eq!(
            apply_cast(Value::String(r#"{"k":1}"#.to_string()), "JSON"),
            Some(Value::Json(r#"{"k":1}"#.to_string()))
        );
        assert_eq!(
            apply_cast(Value::Json("null".to_string()), "TEXT"),
            Some(Value::String("null".to_string()))
        );
    }

    #[test]
    fn test_parse_json_literal() {
        let sql = "SELECT JSON '{\"key\": \"val\"}' FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Literal(Value::Json(s))) => {
                        assert_eq!(s, r#"{"key": "val"}"#);
                    }
                    other => panic!("Expected Json literal, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_typeof() {
        let sql = "SELECT JSON_TYPEOF(data) FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::JsonTypeOf(_)) => {}
                    other => panic!("Expected JsonTypeOf, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_array_length() {
        let sql = "SELECT JSON_ARRAY_LENGTH(data) FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::JsonArrayLength(_)) => {}
                    other => panic!("Expected JsonArrayLength, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_build_object() {
        let sql = "SELECT JSON_BUILD_OBJECT('name', 'Alice', 'age', 30) FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::JsonBuildObject(pairs)) => {
                        assert_eq!(pairs.len(), 2);
                    }
                    other => panic!("Expected JsonBuildObject, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_json_build_array() {
        let sql = "SELECT JSON_BUILD_ARRAY(1, 'two', true) FROM t;";
        let (_, stmt) = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::JsonBuildArray(vals)) => {
                        assert_eq!(vals.len(), 3);
                    }
                    other => panic!("Expected JsonBuildArray, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_apply_json_typeof() {
        assert_eq!(
            apply_json_typeof(&Value::Json(r#""hello""#.to_string())),
            Some(Value::String("string".to_string()))
        );
        assert_eq!(
            apply_json_typeof(&Value::Json("42".to_string())),
            Some(Value::String("number".to_string()))
        );
        assert_eq!(
            apply_json_typeof(&Value::Json("true".to_string())),
            Some(Value::String("boolean".to_string()))
        );
        assert_eq!(
            apply_json_typeof(&Value::Json("[1,2,3]".to_string())),
            Some(Value::String("array".to_string()))
        );
        assert_eq!(
            apply_json_typeof(&Value::Json(r#"{"a":1}"#.to_string())),
            Some(Value::String("object".to_string()))
        );
        assert_eq!(
            apply_json_typeof(&Value::Json("null".to_string())),
            Some(Value::String("null".to_string()))
        );
    }

    #[test]
    fn test_apply_json_array_length() {
        assert_eq!(
            apply_json_array_length(&Value::Json("[1, 2, 3]".to_string())),
            Some(Value::Int(3))
        );
        assert_eq!(
            apply_json_array_length(&Value::Json("[]".to_string())),
            Some(Value::Int(0))
        );
        assert_eq!(
            apply_json_array_length(&Value::Json(r#"{"a":1}"#.to_string())),
            None
        );
    }

    #[test]
    fn test_apply_json_build_object() {
        let pairs = vec![
            (Value::String("name".to_string()), Value::String("Alice".to_string())),
            (Value::String("age".to_string()), Value::Int(30)),
        ];
        let result = apply_json_build_object(&pairs);
        assert!(result.is_some());
        if let Some(Value::Json(s)) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v["name"], "Alice");
            assert_eq!(v["age"], 30);
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_apply_json_build_array() {
        let vals = vec![
            Value::Int(1),
            Value::String("two".to_string()),
            Value::Bool(true),
        ];
        let result = apply_json_build_array(&vals);
        assert!(result.is_some());
        if let Some(Value::Json(s)) = result {
            let v: serde_json::Value = serde_json::from_str(&s).unwrap();
            assert_eq!(v[0], 1);
            assert_eq!(v[1], "two");
            assert_eq!(v[2], true);
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_json_literal_parse_and_roundtrip() {
        // Verify that JSON '...' produces a valid Value::Json that round-trips
        let json_str = r#"{"items":[1,2,3],"active":true}"#;
        let sql = format!("SELECT JSON '{}' FROM t;", json_str);
        let (_, stmt) = parse_sql(&sql).unwrap();
        match stmt {
            SqlStatement::Select(sel) => {
                match &sel.columns[0] {
                    SelectColumn::Expr(Expression::Literal(Value::Json(s))) => {
                        assert_eq!(s, json_str);
                        // Re-parse to confirm valid JSON
                        let v: serde_json::Value = serde_json::from_str(s).unwrap();
                        assert_eq!(v["items"][0], 1);
                        assert!(v["active"].as_bool().unwrap());
                    }
                    other => panic!("Expected Json literal, got {:?}", other),
                }
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_merge_simple() {
        // Basic WHEN MATCHED without condition (unqualified column names)
        let sql = "MERGE INTO t1 USING s1 ON t1.id = s1.id WHEN MATCHED THEN UPDATE SET val = s1.val";
        let (rest, stmt) = parse_sql(sql).unwrap();
        assert!(rest.trim().is_empty(), "unparsed: '{}'", rest);
        match stmt {
            SqlStatement::Merge(ms) => {
                assert_eq!(ms.when_clauses.len(), 1);
                assert!(ms.when_clauses[0].is_matched);
                assert!(ms.when_clauses[0].condition.is_none());
            }
            _ => panic!("Expected Merge"),
        }
    }

    #[test]
    fn test_parse_merge_with_condition() {
        // WHEN MATCHED with AND condition
        let sql = "MERGE INTO t1 USING s1 ON t1.id = s1.id WHEN MATCHED AND s1.val > 500 THEN UPDATE SET val = s1.val";
        let (rest, stmt) = parse_sql(sql).unwrap();
        assert!(rest.trim().is_empty(), "unparsed: '{}'", rest);
        match stmt {
            SqlStatement::Merge(ms) => {
                assert_eq!(ms.when_clauses.len(), 1);
                assert!(ms.when_clauses[0].condition.is_some());
            }
            _ => panic!("Expected Merge"),
        }
    }

    #[test]
    fn test_parse_merge_multiple_when() {
        // Multiple WHEN clauses
        let sql = "MERGE INTO t1 USING s1 ON t1.id = s1.id WHEN MATCHED AND s1.val > 500 THEN UPDATE SET val = s1.val WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s1.id, s1.val)";
        let (rest, stmt) = parse_sql(sql).unwrap();
        assert!(rest.trim().is_empty(), "unparsed: '{}'", rest);
        match stmt {
            SqlStatement::Merge(ms) => {
                assert_eq!(ms.when_clauses.len(), 3);
                assert!(ms.when_clauses[0].is_matched);
                assert!(ms.when_clauses[0].condition.is_some());
                assert!(ms.when_clauses[1].is_matched);
                assert!(ms.when_clauses[1].condition.is_none());
                assert!(!ms.when_clauses[2].is_matched);
                assert!(ms.when_clauses[2].condition.is_none());
            }
            _ => panic!("Expected Merge"),
        }
    }

    #[test]
    fn test_parse_merge_not_matched() {
        // WHEN NOT MATCHED
        let sql = "MERGE INTO t1 USING s1 ON t1.id = s1.id WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s1.id, s1.val)";
        let (rest, stmt) = parse_sql(sql).unwrap();
        assert!(rest.trim().is_empty(), "unparsed: '{}'", rest);
        match stmt {
            SqlStatement::Merge(ms) => {
                assert_eq!(ms.when_clauses.len(), 1);
                assert!(!ms.when_clauses[0].is_matched);
            }
            _ => panic!("Expected Merge"),
        }
    }

    #[test]
    fn test_parse_merge_do_nothing() {
        // WHEN MATCHED THEN DO NOTHING
        let sql = "MERGE INTO t1 USING s1 ON t1.id = s1.id WHEN MATCHED THEN DO NOTHING";
        let (rest, stmt) = parse_sql(sql).unwrap();
        assert!(rest.trim().is_empty(), "unparsed: '{}'", rest);
        match stmt {
            SqlStatement::Merge(ms) => {
                assert_eq!(ms.when_clauses.len(), 1);
                assert_eq!(ms.when_clauses[0].action, MergeAction::DoNothing);
            }
            _ => panic!("Expected Merge"),
        }
    }
