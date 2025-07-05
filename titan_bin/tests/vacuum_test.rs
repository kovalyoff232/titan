
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
fn test_vacuum_removes_dead_tuples() {
    let _setup = TestSetup::new();
    let mut client = TestClient::connect("127.0.0.1:5433");

    // 1. Create table and insert data
    client.query("CREATE TABLE test_vacuum (id INT);");
    client.query("INSERT INTO test_vacuum VALUES (1);");
    client.query("INSERT INTO test_vacuum VALUES (2);");
    client.query("INSERT INTO test_vacuum VALUES (3);");
    client.query("COMMIT;");

    // 2. Check that all data is there
    let rows1 = client.query("SELECT * FROM test_vacuum;");
    assert_eq!(rows1.len(), 3, "Should have 3 rows before delete");

    // 3. Delete some data
    client.query("DELETE FROM test_vacuum WHERE id = 2;");
    client.query("COMMIT;");

    // 4. Check that the data is gone
    let rows2 = client.query("SELECT * FROM test_vacuum;");
    assert_eq!(rows2.len(), 2, "Should have 2 rows after delete");

    // 5. Run VACUUM
    client.query("VACUUM test_vacuum;");
    client.query("COMMIT;");

    // 6. Check that the remaining data is still accessible
    let rows3 = client.query("SELECT * FROM test_vacuum;");
    assert_eq!(rows3.len(), 2, "Should still have 2 rows after vacuum");
    
    let mut ids: Vec<String> = rows3.into_iter().map(|r| r[0].clone()).collect();
    ids.sort();
    
    assert_eq!(ids, vec!["1", "3"]);
}
