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
fn test_single_table_alias_supports_qualified_projection_and_filter() {
    let mut client = common::setup_server_and_client("single_table_alias_test");

    client.simple_query("CREATE TABLE alias_users_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO alias_users_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO alias_users_test VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO alias_users_test VALUES (3, 'Bob');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT u.id FROM alias_users_test AS u WHERE u.name = 'Bob' ORDER BY u.id;");
    assert_eq!(rows, vec![vec!["2"], vec!["3"]]);
}

#[test]
#[serial]
fn test_join_with_table_aliases() {
    let mut client = common::setup_server_and_client("join_alias_test");

    client.simple_query("CREATE TABLE alias_join_users (uid INT, uname TEXT);");
    client.simple_query("CREATE TABLE alias_join_orders (oid INT, uid INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO alias_join_users VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO alias_join_users VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO alias_join_orders VALUES (101, 1);");
    client.simple_query("INSERT INTO alias_join_orders VALUES (102, 2);");
    client.simple_query("INSERT INTO alias_join_orders VALUES (103, 1);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT u.uname, o.oid FROM alias_join_users u JOIN alias_join_orders o ON u.uid = o.uid ORDER BY o.oid;",
    );
    assert_eq!(
        rows,
        vec![
            vec!["Alice", "101"],
            vec!["Bob", "102"],
            vec!["Alice", "103"],
        ]
    );
}

#[test]
#[serial]
fn test_is_null_and_is_not_null_filters() {
    let mut client = common::setup_server_and_client("is_null_filter_test");

    client.simple_query("CREATE TABLE nullable_users_test (id INT, deleted_at TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nullable_users_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nullable_users_test VALUES (2, '2026-02-12');");
    client.simple_query("INSERT INTO nullable_users_test VALUES (3, NULL);");
    client.simple_query("COMMIT;");

    let null_rows = client
        .simple_query("SELECT id FROM nullable_users_test WHERE deleted_at IS NULL ORDER BY id;");
    assert_eq!(null_rows, vec![vec!["1"], vec!["3"]]);

    let not_null_rows = client.simple_query(
        "SELECT id FROM nullable_users_test WHERE deleted_at IS NOT NULL ORDER BY id;",
    );
    assert_eq!(not_null_rows, vec![vec!["2"]]);
}

#[test]
#[serial]
fn test_where_like_filter() {
    let mut client = common::setup_server_and_client("like_filter_test");

    client.simple_query("CREATE TABLE like_users_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO like_users_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO like_users_test VALUES (2, 'Albert');");
    client.simple_query("INSERT INTO like_users_test VALUES (3, 'Bob');");
    client.simple_query("COMMIT;");

    let prefix_rows =
        client.simple_query("SELECT id FROM like_users_test WHERE name LIKE 'Al%' ORDER BY id;");
    assert_eq!(prefix_rows, vec![vec!["1"], vec!["2"]]);

    let single_char_rows =
        client.simple_query("SELECT id FROM like_users_test WHERE name LIKE '_ob' ORDER BY id;");
    assert_eq!(single_char_rows, vec![vec!["3"]]);

    let not_like_rows = client
        .simple_query("SELECT id FROM like_users_test WHERE name NOT LIKE 'Al%' ORDER BY id;");
    assert_eq!(not_like_rows, vec![vec!["3"]]);

    let ilike_rows =
        client.simple_query("SELECT id FROM like_users_test WHERE name ILIKE 'al%' ORDER BY id;");
    assert_eq!(ilike_rows, vec![vec!["1"], vec!["2"]]);

    let not_ilike_rows = client
        .simple_query("SELECT id FROM like_users_test WHERE name NOT ILIKE 'al%' ORDER BY id;");
    assert_eq!(not_ilike_rows, vec![vec!["3"]]);
}

#[test]
#[serial]
fn test_where_in_and_not_in_filters() {
    let mut client = common::setup_server_and_client("in_filter_test");

    client.simple_query("CREATE TABLE in_users_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO in_users_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO in_users_test VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO in_users_test VALUES (3, 'Carol');");
    client.simple_query("INSERT INTO in_users_test VALUES (4, 'Dave');");
    client.simple_query("COMMIT;");

    let in_id_rows =
        client.simple_query("SELECT id FROM in_users_test WHERE id IN (1, 3) ORDER BY id;");
    assert_eq!(in_id_rows, vec![vec!["1"], vec!["3"]]);

    let not_in_id_rows =
        client.simple_query("SELECT id FROM in_users_test WHERE id NOT IN (1, 3) ORDER BY id;");
    assert_eq!(not_in_id_rows, vec![vec!["2"], vec!["4"]]);

    let in_text_rows = client
        .simple_query("SELECT id FROM in_users_test WHERE name IN ('Bob', 'Dave') ORDER BY id;");
    assert_eq!(in_text_rows, vec![vec!["2"], vec!["4"]]);
}

#[test]
#[serial]
fn test_where_between_and_not_between_filters() {
    let mut client = common::setup_server_and_client("between_filter_test");

    client.simple_query("CREATE TABLE between_values_test (id INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO between_values_test VALUES (5);");
    client.simple_query("INSERT INTO between_values_test VALUES (10);");
    client.simple_query("INSERT INTO between_values_test VALUES (15);");
    client.simple_query("INSERT INTO between_values_test VALUES (20);");
    client.simple_query("INSERT INTO between_values_test VALUES (25);");
    client.simple_query("COMMIT;");

    let between_rows = client
        .simple_query("SELECT id FROM between_values_test WHERE id BETWEEN 10 AND 20 ORDER BY id;");
    assert_eq!(between_rows, vec![vec!["10"], vec!["15"], vec!["20"]]);

    let not_between_rows = client.simple_query(
        "SELECT id FROM between_values_test WHERE id NOT BETWEEN 10 AND 20 ORDER BY id;",
    );
    assert_eq!(not_between_rows, vec![vec!["5"], vec!["25"]]);
}

#[test]
#[serial]
fn test_coalesce_function_in_projection_and_filter() {
    let mut client = common::setup_server_and_client("coalesce_test");

    client.simple_query("CREATE TABLE coalesce_values_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO coalesce_values_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO coalesce_values_test VALUES (2, 'value');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, COALESCE(payload, 'fallback') FROM coalesce_values_test ORDER BY id;",
    );
    assert_eq!(rows, vec![vec!["1", "fallback"], vec!["2", "value"]]);

    let filtered_rows = client.simple_query(
        "SELECT id FROM coalesce_values_test WHERE COALESCE(payload, 'fallback') = 'fallback';",
    );
    assert_eq!(filtered_rows, vec![vec!["1"]]);
}

#[test]
#[serial]
fn test_nullif_function_in_projection_and_filter() {
    let mut client = common::setup_server_and_client("nullif_test");

    client.simple_query("CREATE TABLE nullif_values_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nullif_values_test VALUES (1, 'same');");
    client.simple_query("INSERT INTO nullif_values_test VALUES (2, 'value');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT id, NULLIF(payload, 'same') FROM nullif_values_test ORDER BY id;");
    assert_eq!(rows, vec![vec!["1", "NULL"], vec!["2", "value"]]);

    let filtered_rows = client
        .simple_query("SELECT id FROM nullif_values_test WHERE NULLIF(payload, 'same') = 'value';");
    assert_eq!(filtered_rows, vec![vec!["2"]]);
}

#[test]
#[serial]
fn test_lower_and_upper_functions_in_projection_and_filter() {
    let mut client = common::setup_server_and_client("lower_upper_test");

    client.simple_query("CREATE TABLE case_values_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO case_values_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO case_values_test VALUES (2, 'BoB');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT id, LOWER(name), UPPER(name) FROM case_values_test ORDER BY id;");
    assert_eq!(
        rows,
        vec![vec!["1", "alice", "ALICE"], vec!["2", "bob", "BOB"]]
    );

    let filtered_rows =
        client.simple_query("SELECT id FROM case_values_test WHERE LOWER(name) = 'bob';");
    assert_eq!(filtered_rows, vec![vec!["2"]]);
}

#[test]
#[serial]
fn test_length_and_trim_functions_in_projection_and_filter() {
    let mut client = common::setup_server_and_client("length_trim_test");

    client.simple_query("CREATE TABLE trim_values_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO trim_values_test VALUES (1, '  Alpha  ');");
    client.simple_query("INSERT INTO trim_values_test VALUES (2, 'xxbetaxx');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, LENGTH(TRIM(payload)), LTRIM(payload, ' x'), RTRIM(payload, ' x') \
         FROM trim_values_test ORDER BY id;",
    );
    assert_eq!(
        rows,
        vec![
            vec!["1", "5", "Alpha  ", "  Alpha"],
            vec!["2", "8", "betaxx", "xxbeta"]
        ]
    );

    let filtered_rows =
        client.simple_query("SELECT id FROM trim_values_test WHERE LENGTH(TRIM(payload)) = 5;");
    assert_eq!(filtered_rows, vec![vec!["1"]]);
}

#[test]
#[serial]
fn test_substring_replace_concat_and_strpos_functions() {
    let mut client = common::setup_server_and_client("string_functions_test");

    client.simple_query("CREATE TABLE text_ops_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO text_ops_test VALUES (1, 'alpha beta');");
    client.simple_query("INSERT INTO text_ops_test VALUES (2, 'gamma');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, SUBSTRING(payload, 1, 5), REPLACE(payload, 'a', 'x'), \
         CONCAT('row-', id), STRPOS(payload, 'beta') \
         FROM text_ops_test ORDER BY id;",
    );
    assert_eq!(
        rows,
        vec![
            vec!["1", "alpha", "xlphx betx", "row-1", "7"],
            vec!["2", "gamma", "gxmmx", "row-2", "0"]
        ]
    );

    let filtered_rows =
        client.simple_query("SELECT id FROM text_ops_test WHERE STRPOS(payload, 'beta') > 0;");
    assert_eq!(filtered_rows, vec![vec!["1"]]);
}

#[test]
#[serial]
fn test_arithmetic_operators_in_projection_and_filter() {
    let mut client = common::setup_server_and_client("arithmetic_ops_test");

    client.simple_query("CREATE TABLE arithmetic_values_test (id INT, a INT, b INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO arithmetic_values_test VALUES (1, 6, 4);");
    client.simple_query("INSERT INTO arithmetic_values_test VALUES (2, 9, 3);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, a * b, a / b, a % b, a + b - 1 FROM arithmetic_values_test ORDER BY id;",
    );
    assert_eq!(
        rows,
        vec![
            vec!["1", "24", "1", "2", "9"],
            vec!["2", "27", "3", "0", "11"]
        ]
    );

    let filtered_rows =
        client.simple_query("SELECT id FROM arithmetic_values_test WHERE a * b = 27;");
    assert_eq!(filtered_rows, vec![vec!["2"]]);
}

#[test]
#[serial]
fn test_abs_greatest_and_least_functions() {
    let mut client = common::setup_server_and_client("extrema_functions_test");

    client.simple_query(
        "CREATE TABLE extrema_values_test (id INT, delta INT, left_v INT, right_v INT);",
    );
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO extrema_values_test VALUES (1, -7, 2, 9);");
    client.simple_query("INSERT INTO extrema_values_test VALUES (2, 4, 10, 3);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT id, ABS(delta), GREATEST(left_v, right_v), LEAST(left_v, right_v) \
         FROM extrema_values_test ORDER BY id;",
    );
    assert_eq!(
        rows,
        vec![vec!["1", "7", "9", "2"], vec!["2", "4", "10", "3"]]
    );

    let filtered_rows =
        client.simple_query("SELECT id FROM extrema_values_test WHERE ABS(delta) = 7;");
    assert_eq!(filtered_rows, vec![vec!["1"]]);
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

#[test]
#[serial]
fn test_order_by_supports_desc_direction() {
    let mut client = common::setup_server_and_client("order_by_desc_test");

    client.simple_query("CREATE TABLE desc_sort_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO desc_sort_test VALUES (3, 'c');");
    client.simple_query("INSERT INTO desc_sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO desc_sort_test VALUES (2, 'b');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id, payload FROM desc_sort_test ORDER BY id DESC;");
    assert_eq!(rows, vec![vec!["3", "c"], vec!["2", "b"], vec!["1", "a"]]);
}

#[test]
#[serial]
fn test_order_by_supports_select_alias_reference() {
    let mut client = common::setup_server_and_client("order_by_select_alias_test");

    client.simple_query("CREATE TABLE alias_sort_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO alias_sort_test VALUES (3, 'c');");
    client.simple_query("INSERT INTO alias_sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO alias_sort_test VALUES (2, 'b');");
    client.simple_query("COMMIT;");

    let rows =
        client.simple_query("SELECT id AS item_id FROM alias_sort_test ORDER BY item_id DESC;");
    assert_eq!(rows, vec![vec!["3"], vec!["2"], vec!["1"]]);
}

#[test]
#[serial]
fn test_order_by_supports_nulls_last_for_ascending() {
    let mut client = common::setup_server_and_client("order_by_nulls_last_asc_test");

    client.simple_query("CREATE TABLE nulls_sort_asc_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nulls_sort_asc_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nulls_sort_asc_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO nulls_sort_asc_test VALUES (3, 'a');");
    client.simple_query("COMMIT;");

    let rows =
        client.simple_query("SELECT id FROM nulls_sort_asc_test ORDER BY payload ASC NULLS LAST;");
    assert_eq!(rows, vec![vec!["3"], vec!["2"], vec!["1"]]);
}

#[test]
#[serial]
fn test_order_by_supports_nulls_first_for_ascending() {
    let mut client = common::setup_server_and_client("order_by_nulls_first_asc_test");

    client.simple_query("CREATE TABLE nulls_sort_first_asc_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nulls_sort_first_asc_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nulls_sort_first_asc_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO nulls_sort_first_asc_test VALUES (3, 'a');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT id FROM nulls_sort_first_asc_test ORDER BY payload ASC NULLS FIRST;");
    assert_eq!(rows, vec![vec!["1"], vec!["3"], vec!["2"]]);
}

#[test]
#[serial]
fn test_order_by_supports_nulls_last_for_descending() {
    let mut client = common::setup_server_and_client("order_by_nulls_last_desc_test");

    client.simple_query("CREATE TABLE nulls_sort_desc_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nulls_sort_desc_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nulls_sort_desc_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO nulls_sort_desc_test VALUES (3, 'a');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT id FROM nulls_sort_desc_test ORDER BY payload DESC NULLS LAST;");
    assert_eq!(rows, vec![vec!["2"], vec!["3"], vec!["1"]]);
}

#[test]
#[serial]
fn test_order_by_defaults_to_nulls_last_for_ascending() {
    let mut client = common::setup_server_and_client("order_by_nulls_default_asc_test");

    client.simple_query("CREATE TABLE nulls_sort_default_asc_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nulls_sort_default_asc_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nulls_sort_default_asc_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO nulls_sort_default_asc_test VALUES (3, 'a');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id FROM nulls_sort_default_asc_test ORDER BY payload;");
    assert_eq!(rows, vec![vec!["3"], vec!["2"], vec!["1"]]);
}

#[test]
#[serial]
fn test_order_by_defaults_to_nulls_first_for_descending() {
    let mut client = common::setup_server_and_client("order_by_nulls_default_desc_test");

    client.simple_query("CREATE TABLE nulls_sort_default_desc_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO nulls_sort_default_desc_test VALUES (1, NULL);");
    client.simple_query("INSERT INTO nulls_sort_default_desc_test VALUES (2, 'b');");
    client.simple_query("INSERT INTO nulls_sort_default_desc_test VALUES (3, 'a');");
    client.simple_query("COMMIT;");

    let rows =
        client.simple_query("SELECT id FROM nulls_sort_default_desc_test ORDER BY payload DESC;");
    assert_eq!(rows, vec![vec!["1"], vec!["2"], vec!["3"]]);
}

#[test]
#[serial]
fn test_order_by_supports_position_reference() {
    let mut client = common::setup_server_and_client("order_by_position_test");

    client.simple_query("CREATE TABLE position_sort_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO position_sort_test VALUES (3, 'c');");
    client.simple_query("INSERT INTO position_sort_test VALUES (1, 'a');");
    client.simple_query("INSERT INTO position_sort_test VALUES (2, 'b');");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id, payload FROM position_sort_test ORDER BY 1 DESC;");
    assert_eq!(rows, vec![vec!["3", "c"], vec!["2", "b"], vec!["1", "a"]]);
}

#[test]
#[serial]
fn test_select_order_by_limit_offset() {
    let mut client = common::setup_server_and_client("limit_offset_test");

    client.simple_query("CREATE TABLE limit_test (id INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO limit_test VALUES (3);");
    client.simple_query("INSERT INTO limit_test VALUES (1);");
    client.simple_query("INSERT INTO limit_test VALUES (5);");
    client.simple_query("INSERT INTO limit_test VALUES (2);");
    client.simple_query("INSERT INTO limit_test VALUES (4);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id FROM limit_test ORDER BY id LIMIT 2 OFFSET 1;");
    assert_eq!(rows, vec![vec!["2"], vec!["3"]]);
}

#[test]
#[serial]
fn test_insert_and_select_null_literal() {
    let mut client = common::setup_server_and_client("null_literal_test");

    client.simple_query("CREATE TABLE null_test (id INT, payload TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO null_test VALUES (1, NULL);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query("SELECT id, payload FROM null_test ORDER BY id;");
    assert_eq!(rows, vec![vec!["1", ""]]);
}

#[test]
#[serial]
fn test_group_by_count_star_and_having() {
    let mut client = common::setup_server_and_client("group_by_having_test");

    client.simple_query("CREATE TABLE agg_test (group_id INT, val INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO agg_test VALUES (1, 10);");
    client.simple_query("INSERT INTO agg_test VALUES (1, 20);");
    client.simple_query("INSERT INTO agg_test VALUES (2, 30);");
    client.simple_query("INSERT INTO agg_test VALUES (2, 40);");
    client.simple_query("INSERT INTO agg_test VALUES (2, 50);");
    client.simple_query("COMMIT;");

    let grouped_rows = client.simple_query(
        "SELECT group_id, COUNT(*) AS cnt FROM agg_test GROUP BY group_id ORDER BY group_id;",
    );
    assert_eq!(grouped_rows, vec![vec!["1", "2"], vec!["2", "3"]]);

    let having_rows =
        client.simple_query("SELECT group_id, COUNT(*) AS cnt FROM agg_test GROUP BY group_id HAVING cnt > 2 ORDER BY group_id;");
    assert_eq!(having_rows, vec![vec!["2", "3"]]);
}

#[test]
#[serial]
fn test_group_by_lowercase_sum_avg_with_having_alias() {
    let mut client = common::setup_server_and_client("group_by_sum_avg_lowercase_test");

    client.simple_query("CREATE TABLE agg_lower_test (group_id INT, val INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO agg_lower_test VALUES (1, 10);");
    client.simple_query("INSERT INTO agg_lower_test VALUES (1, 20);");
    client.simple_query("INSERT INTO agg_lower_test VALUES (2, 30);");
    client.simple_query("INSERT INTO agg_lower_test VALUES (2, 40);");
    client.simple_query("INSERT INTO agg_lower_test VALUES (2, 50);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT group_id, sum(val) AS total, avg(val) AS avg_val FROM agg_lower_test GROUP BY group_id HAVING total > 40 ORDER BY group_id;",
    );
    assert_eq!(rows, vec![vec!["2", "120", "40"]]);
}

#[test]
#[serial]
fn test_count_null_expression_ignores_nulls() {
    let mut client = common::setup_server_and_client("count_null_expr_test");

    client.simple_query("CREATE TABLE count_null_test (id INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO count_null_test VALUES (1);");
    client.simple_query("INSERT INTO count_null_test VALUES (2);");
    client.simple_query("INSERT INTO count_null_test VALUES (3);");
    client.simple_query("COMMIT;");

    let rows = client.simple_query(
        "SELECT COUNT(NULL) AS null_cnt, COUNT(*) AS total_cnt FROM count_null_test;",
    );
    assert_eq!(rows, vec![vec!["0", "3"]]);
}

#[test]
#[serial]
fn test_where_text_equality_filter() {
    let mut client = common::setup_server_and_client("where_text_filter_test");

    client.simple_query("CREATE TABLE text_filter_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO text_filter_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO text_filter_test VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO text_filter_test VALUES (3, 'Charlie');");
    client.simple_query("COMMIT;");

    let rows =
        client.simple_query("SELECT id FROM text_filter_test WHERE name = 'Bob' ORDER BY id;");
    assert_eq!(rows, vec![vec!["2"]]);
}

#[test]
#[serial]
fn test_where_text_not_equal_filter_with_bang_operator() {
    let mut client = common::setup_server_and_client("where_text_not_equal_filter_test");

    client.simple_query("CREATE TABLE text_filter_bang_test (id INT, name TEXT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO text_filter_bang_test VALUES (1, 'Alice');");
    client.simple_query("INSERT INTO text_filter_bang_test VALUES (2, 'Bob');");
    client.simple_query("INSERT INTO text_filter_bang_test VALUES (3, 'Charlie');");
    client.simple_query("COMMIT;");

    let rows = client
        .simple_query("SELECT id FROM text_filter_bang_test WHERE name != 'Bob' ORDER BY id;");
    assert_eq!(rows, vec![vec!["1"], vec!["3"]]);
}
