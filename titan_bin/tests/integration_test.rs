use serial_test::serial;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

mod common;
use common::TestClient;

struct TestSetup {
    _server_process: Child,
    // Keep the temp files in scope to prevent them from being deleted
    _db_file: NamedTempFile,
    _wal_file: NamedTempFile,
}

impl TestSetup {
    fn new() -> Self {
        let db_file = NamedTempFile::new().expect("Failed to create temp db file");
        let wal_file = NamedTempFile::new().expect("Failed to create temp wal file");

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let server_path = format!("{}/../target/debug/titan_bin", manifest_dir);
        let server_process = Command::new(server_path)
            .env("TITAN_DB_PATH", db_file.path())
            .env("TITAN_WAL_PATH", wal_file.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server");

        // Wait for the server to start
        thread::sleep(Duration::from_secs(1));

        TestSetup {
            _server_process: server_process,
            _db_file: db_file,
            _wal_file: wal_file,
        }
    }
}

impl Drop for TestSetup {
    fn drop(&mut self) {
        self._server_process.kill().expect("Failed to kill server process");
    }
}

#[test]
#[serial]
fn test_rollback_removes_inserted_data() {
    let _setup = TestSetup::new();
    // 1. Connect to the server
    let mut client = TestClient::connect("127.0.0.1:5433");

    // 2. Setup the table in its own transaction
    client.query("CREATE TABLE test_rollback (id INT);");

    // 3. Run the test transaction
    let rows_before = client.query("BEGIN; INSERT INTO test_rollback VALUES (100); SELECT * FROM test_rollback;");
    assert_eq!(
        rows_before.len(),
        1,
        "Data should be visible within the transaction"
    );
    assert_eq!(rows_before[0][0], "100");

    // 4. Rollback
    client.query("ROLLBACK;");

    // 5. Verify the data is NOT there after rollback
    let rows_after = client.query("SELECT * FROM test_rollback;");
    println!("Rows after rollback: {:?}", rows_after);
    assert_eq!(rows_after.len(), 0, "Data should be gone after rollback");
}

#[test]
#[serial]
fn test_create_index_and_select() {
    let _setup = TestSetup::new();
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
