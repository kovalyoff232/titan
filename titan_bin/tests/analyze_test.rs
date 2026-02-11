use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_analyze_calculates_distinct_values() {
    let mut client = common::setup_server_and_client("analyze_test");

    client.simple_query("CREATE TABLE test_analyze (id INT, category TEXT);");
    client.simple_query("INSERT INTO test_analyze VALUES (1, 'A');");
    client.simple_query("INSERT INTO test_analyze VALUES (2, 'B');");
    client.simple_query("INSERT INTO test_analyze VALUES (3, 'A');");
    client.simple_query("INSERT INTO test_analyze VALUES (4, 'C');");
    client.simple_query("INSERT INTO test_analyze VALUES (5, 'B');");
    client.simple_query("INSERT INTO test_analyze VALUES (6, 'A');");
    client.simple_query("COMMIT;");

    client.simple_query("ANALYZE test_analyze;");
    client.simple_query("COMMIT;");

    let stats =
        client.simple_query("SELECT staattnum, stadistinct FROM pg_statistic WHERE stakind = 1;");

    assert_eq!(stats.len(), 2, "Should have statistics for 2 columns");

    let mut id_distinct = "-1".to_string();
    let mut category_distinct = "-1".to_string();

    for row in stats {
        let col_num = &row[0];
        let distinct_count = &row[1];
        if col_num == "0" {
            id_distinct = distinct_count.clone();
        } else if col_num == "1" {
            category_distinct = distinct_count.clone();
        }
    }

    assert_eq!(id_distinct, "6", "Incorrect distinct count for 'id'");
    assert_eq!(
        category_distinct, "3",
        "Incorrect distinct count for 'category'"
    );
}

#[test]
#[serial]
fn test_analyze_persists_total_row_count_statistic() {
    let mut client = common::setup_server_and_client("analyze_total_rows_test");

    client.simple_query("CREATE TABLE test_rows (id INT, name TEXT);");
    client.simple_query("INSERT INTO test_rows VALUES (1, 'a');");
    client.simple_query("INSERT INTO test_rows VALUES (2, 'b');");
    client.simple_query("INSERT INTO test_rows VALUES (3, 'c');");
    client.simple_query("COMMIT;");

    client.simple_query("ANALYZE test_rows;");
    client.simple_query("COMMIT;");

    let stats =
        client.simple_query("SELECT stakind, stadistinct FROM pg_statistic WHERE stakind = 0;");
    assert_eq!(
        stats.len(),
        1,
        "Should persist one table-level row-count stat"
    );
    assert_eq!(stats[0][0], "0");
    assert_eq!(stats[0][1], "3");
}
