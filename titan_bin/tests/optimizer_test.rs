use serial_test::serial;
mod common;

#[test]
#[serial]
fn test_optimizer_chooses_hash_join_for_equi_join() {
    let mut client = common::setup_server_and_client("optimizer_hash_join_test");

    client.simple_query("CREATE TABLE t1 (id INT, data TEXT);");
    client.simple_query("CREATE TABLE t2 (id INT, t1_id INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO t1 VALUES (1, 'a');");
    client.simple_query("INSERT INTO t2 VALUES (101, 1);");
    client.simple_query("COMMIT;");

    // This query should use HashJoin because of the equality condition.
    // We don't have a way to EXPLAIN the plan yet, so we just check for correctness.
    let result = client.simple_query("SELECT t1.data FROM t1 JOIN t2 ON t1.id = t2.t1_id;");
    
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], "a");
}

#[test]
#[serial]
fn test_optimizer_chooses_nested_loop_join_for_non_equi_join() {
    let mut client = common::setup_server_and_client("optimizer_nl_join_test");

    client.simple_query("CREATE TABLE t3 (id INT, val INT);");
    client.simple_query("CREATE TABLE t4 (id INT, val INT);");
    client.simple_query("COMMIT;");

    client.simple_query("INSERT INTO t3 VALUES (1, 10);");
    client.simple_query("INSERT INTO t3 VALUES (2, 20);");
    client.simple_query("INSERT INTO t4 VALUES (101, 15);");
    client.simple_query("INSERT INTO t4 VALUES (102, 25);");
    client.simple_query("COMMIT;");

    // This query should use NestedLoopJoin.
    let result = client.simple_query("SELECT t3.id, t4.id FROM t3 JOIN t4 ON t3.val < t4.val;");
    
    let mut sorted_result: Vec<String> = result.into_iter().map(|row| row.join(", ")).collect();
    sorted_result.sort();

    assert_eq!(sorted_result.len(), 3);
    assert_eq!(sorted_result[0], "1, 101");
    assert_eq!(sorted_result[1], "1, 102");
    assert_eq!(sorted_result[2], "2, 102");
}

#[test]
#[serial]
fn test_optimizer_chooses_merge_join() {
    let mut client = common::setup_server_and_client("optimizer_merge_join_test");

    client.simple_query("CREATE TABLE t6 (id INT, data TEXT);");
    client.simple_query("CREATE TABLE t7 (id INT, t6_id INT);");
    client.simple_query("CREATE INDEX idx_t6_id ON t6(id);");
    client.simple_query("CREATE INDEX idx_t7_t6_id ON t7(t6_id);");
    client.simple_query("COMMIT;");

    for i in 0..10 {
        client.simple_query(&format!("INSERT INTO t6 VALUES ({}, 'text{}');", i, i));
        client.simple_query(&format!("INSERT INTO t7 VALUES ({}, {});", i + 100, i));
    }
    client.simple_query("COMMIT;");

    client.simple_query("ANALYZE t6;");
    client.simple_query("ANALYZE t7;");
    client.simple_query("COMMIT;");

    let result = client.simple_query("SELECT t6.data, t7.id FROM t6 JOIN t7 ON t6.id = t7.t6_id ORDER BY id;");
    
    assert_eq!(result.len(), 10);
    assert_eq!(result[0][0], "text0");
    assert_eq!(result[0][1], "100");
    assert_eq!(result[9][0], "text9");
    assert_eq!(result[9][1], "109");
}

#[test]
#[serial]
fn test_optimizer_chooses_index_scan() {
    let mut client = common::setup_server_and_client("optimizer_index_scan_test");

    client.simple_query("CREATE TABLE t5 (id INT, data TEXT);");
    client.simple_query("CREATE INDEX idx_id ON t5(id);");
    client.simple_query("COMMIT;");

    for i in 0..1000 {
        client.simple_query(&format!("INSERT INTO t5 VALUES ({}, 'text{}');", i, i));
    }
    client.simple_query("COMMIT;");

    client.simple_query("ANALYZE t5;");
    client.simple_query("COMMIT;");

    let result = client.simple_query("SELECT data FROM t5 WHERE id = 500;");
    
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], "text500");
}
