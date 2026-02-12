use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_rollback_removes_inserted_data() {
    let mut client = common::setup_server_and_client("rollback_test");

    client.simple_query("CREATE TABLE test_rollback (id INT);");
    client.simple_query("COMMIT;");

    client.simple_query("BEGIN;");
    client.simple_query("INSERT INTO test_rollback VALUES (100);");
    let rows_before = client.simple_query("SELECT * FROM test_rollback;");
    assert_eq!(
        rows_before.len(),
        1,
        "Data should be visible within the transaction"
    );
    assert_eq!(rows_before[0][0], "100");

    client.simple_query("ROLLBACK;");

    let rows_after = client.simple_query("SELECT * FROM test_rollback;");
    println!("Rows after rollback: {:?}", rows_after);
    assert!(rows_after.is_empty(), "Data should be gone after rollback");
}

#[test]
#[serial]
fn test_create_index_and_select() {
    let mut client = common::setup_server_and_client("index_test");

    client.simple_query("CREATE TABLE test_index (id INT, name TEXT);");
    client.simple_query("INSERT INTO test_index VALUES (1, 'one');");
    client.simple_query("INSERT INTO test_index VALUES (2, 'two');");
    client.simple_query("INSERT INTO test_index VALUES (3, 'three');");
    client.simple_query("COMMIT;");

    client.simple_query("CREATE INDEX idx_id ON test_index(id);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT name FROM test_index WHERE id = 2;");
    assert_eq!(rows.len(), 1, "Should find one row by index");
    assert_eq!(rows[0][0], "two");
}

#[test]
#[serial]
fn test_join_tables() {
    let mut client = common::setup_server_and_client("join_test");

    client.simple_query("CREATE TABLE users (user_id INT, user_name TEXT);");
    client.simple_query("CREATE TABLE orders (order_id INT, user_id INT, item TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO users VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO users VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO users VALUES (3, 'Charlie');");
    client.simple_query("INSERT INTO orders VALUES (101, 1, 'Laptop');");
    client.simple_query("INSERT INTO orders VALUES (102, 2, 'Mouse');");
    client.simple_query("INSERT INTO orders VALUES (103, 1, 'Keyboard');");
    client.simple_query("INSERT INTO orders VALUES (104, 3, 'Monitor');");
    client.simple_query("COMMIT;");

    let mut result = client.simple_query("SELECT users.user_name, orders.item FROM users JOIN orders ON users.user_id = orders.user_id;");

    assert_eq!(result.len(), 4, "JOIN should produce 4 rows");

    result.sort();

    assert_eq!(result[0], vec!["Alice", "Keyboard"]);
    assert_eq!(result[1], vec!["Alice", "Laptop"]);
    assert_eq!(result[2], vec!["Bob", "Mouse"]);
    assert_eq!(result[3], vec!["Charlie", "Monitor"]);
}

#[test]
#[serial]
fn test_boolean_type() {
    let mut client = common::setup_server_and_client("boolean_test");

    client.simple_query("CREATE TABLE test_bool (id INT, is_active BOOLEAN);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO test_bool VALUES (1, TRUE);");
    client.simple_query("INSERT INTO test_bool VALUES (2, FALSE);");
    client.simple_query("INSERT INTO test_bool VALUES (3, TRUE);");
    client.simple_query("COMMIT;");

    let all_rows = client.simple_query("SELECT id, is_active FROM test_bool ORDER BY id;");
    println!("All rows: {:?}", all_rows);
    assert_eq!(all_rows.len(), 3);
    assert_eq!(all_rows[0], vec!["1", "t"]);
    assert_eq!(all_rows[1], vec!["2", "f"]);
    assert_eq!(all_rows[2], vec!["3", "t"]);

    let true_rows = client.simple_query("SELECT id FROM test_bool WHERE is_active = TRUE;");
    assert_eq!(true_rows.len(), 2);
    assert!(true_rows.contains(&vec!["1".to_string()]));
    assert!(true_rows.contains(&vec!["3".to_string()]));

    let false_rows = client.simple_query("SELECT id FROM test_bool WHERE is_active = FALSE;");
    assert_eq!(false_rows.len(), 1);
    assert_eq!(false_rows[0][0], "2");
}

#[test]
#[serial]
fn test_date_type() {
    let mut client = common::setup_server_and_client("date_test");

    client.simple_query("CREATE TABLE events (id INT, event_date DATE);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO events VALUES (1, DATE '2024-01-15');");
    client.simple_query("INSERT INTO events VALUES (2, DATE '2025-07-06');");
    client.simple_query("INSERT INTO events VALUES (3, DATE '2024-01-15');");
    client.simple_query("COMMIT;");

    let all_rows = client.simple_query("SELECT id, event_date FROM events ORDER BY id;");
    assert_eq!(all_rows.len(), 3);
    assert_eq!(all_rows[0], vec!["1", "2024-01-15"]);
    assert_eq!(all_rows[1], vec!["2", "2025-07-06"]);
    assert_eq!(all_rows[2], vec!["3", "2024-01-15"]);

    let filtered_rows =
        client.simple_query("SELECT id FROM events WHERE event_date = DATE '2024-01-15';");
    assert_eq!(filtered_rows.len(), 2);
    assert!(filtered_rows.contains(&vec!["1".to_string()]));
    assert!(filtered_rows.contains(&vec!["3".to_string()]));
}

#[test]
#[serial]
fn test_order_by_sorts_unsorted_input() {
    let mut client = common::setup_server_and_client("order_by_unsorted_test");

    client.simple_query("CREATE TABLE sort_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO sort_test VALUES (3, 'c');");
    client.simple_query("INSERT INTO sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO sort_test VALUES (2, 'b');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id, payload FROM sort_test ORDER BY id;");
    assert_eq!(rows, vec![vec!["1", "a"], vec!["2", "b"], vec!["3", "c"]]);
}

#[test]
#[serial]
fn test_order_by_supports_multiple_columns() {
    let mut client = common::setup_server_and_client("order_by_multi_test");

    client.simple_query("CREATE TABLE multi_sort_test (group_id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO multi_sort_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO multi_sort_test VALUES (1, 'z');");
    client.simple_query("INSERT INTO multi_sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO multi_sort_test VALUES (2, 'a');");
    client.simple_query("COMMIT;");

    let rows =
        client.simple_query("SELECT group_id, name FROM multi_sort_test ORDER BY group_id, name;");
    assert_eq!(
        rows,
        vec![
            vec!["1", "a"],
            vec!["1", "z"],
            vec!["2", "a"],
            vec!["2", "b"],
        ]
    );
}

#[test]
#[serial]
fn test_order_by_supports_qualified_column_reference() {
    let mut client = common::setup_server_and_client("order_by_qualified_test");

    client.simple_query("CREATE TABLE qualified_sort_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO qualified_sort_test VALUES (3, 'c');");
    client.simple_query("INSERT INTO qualified_sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO qualified_sort_test VALUES (2, 'b');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, payload FROM qualified_sort_test ORDER BY qualified_sort_test.id;",
    );
    assert_eq!(rows, vec![vec!["1", "a"], vec!["2", "b"], vec!["3", "c"]]);
}
