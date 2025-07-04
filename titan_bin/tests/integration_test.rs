mod common;
use common::TestClient;

#[test]
fn test_rollback_removes_inserted_data() {
    // 1. Connect to the server
    let mut client = TestClient::connect("127.0.0.1:5433");

    // 2. Setup the table in its own transaction
    client.query("CREATE TABLE test_rollback (id INT);");
    client.query("COMMIT;");

    // 3. Run the test transaction
    // Send BEGIN, INSERT, and SELECT in the same query string to ensure they run in the same transaction
    let rows_before = client.query("BEGIN; INSERT INTO test_rollback VALUES (100); SELECT * FROM test_rollback;");
    assert_eq!(rows_before.len(), 1, "Data should be visible within the transaction");
    assert_eq!(rows_before[0][0], "100");

    // 4. Rollback
    client.query("ROLLBACK;");

    // 5. Verify the data is NOT there after rollback
    let rows_after = client.query("SELECT * FROM test_rollback;");
    assert_eq!(rows_after.len(), 0, "Data should be gone after rollback");
}

#[test]
fn test_create_index_and_select() {
    // 1. Connect to the server
    let mut client = TestClient::connect("127.0.0.1:5433");

    // 2. Setup the table and data
    client.query("CREATE TABLE test_index (id INT, name TEXT);");
    client.query("INSERT INTO test_index VALUES (1, 'one');");
    client.query("INSERT INTO test_index VALUES (2, 'two');");
    client.query("INSERT INTO test_index VALUES (3, 'three');");
    client.query("COMMIT;");

    // 3. Create the index
    client.query("CREATE INDEX idx_id ON test_index(id);");
    client.query("COMMIT;");

    // 4. Verify SELECT with index works
    let rows = client.query("SELECT name FROM test_index WHERE id = 2;");
    assert_eq!(rows.len(), 1, "Should find one row by index");
    assert_eq!(rows[0][0], "two");
}
