use serial_test::serial;

mod common;

#[test]
#[serial]
fn test_vacuum_removes_dead_tuples() {
    let mut client = common::setup_server_and_client("vacuum_test");

    // 1. Create table and insert data
    client.simple_query("CREATE TABLE test_vacuum (id INT);");
    client.simple_query("INSERT INTO test_vacuum VALUES (1);");
    client.simple_query("INSERT INTO test_vacuum VALUES (2);");
    client.simple_query("INSERT INTO test_vacuum VALUES (3);");
    client.simple_query("COMMIT;");

    // 2. Check that all data is there
    let rows1 = client.simple_query("SELECT * FROM test_vacuum;");
    assert_eq!(rows1.len(), 3, "Should have 3 rows before delete");

    // 3. Delete some data
    client.simple_query("DELETE FROM test_vacuum WHERE id = 2;");
    client.simple_query("COMMIT;");

    // 4. Check that the data is gone
    let rows2 = client.simple_query("SELECT * FROM test_vacuum;");
    assert_eq!(rows2.len(), 2, "Should have 2 rows after delete");

    // 5. Run VACUUM
    client.simple_query("VACUUM test_vacuum;");
    client.simple_query("COMMIT;");

    // 6. Check that the remaining data is still accessible
    let rows3 = client.simple_query("SELECT * FROM test_vacuum;");
    assert_eq!(rows3.len(), 2, "Should still have 2 rows after vacuum");

    let mut ids: Vec<String> = rows3.into_iter().map(|row| row[0].clone()).collect();
    ids.sort();

    assert_eq!(ids, vec!["1", "3"]);
}
