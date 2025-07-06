use postgres::{Client, NoTls, SimpleQueryMessage};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_PORT: AtomicU16 = AtomicU16::new(6000);

/// A wrapper around the postgres::Client that ensures the server process
/// is killed when the client goes out of scope.
pub struct TestClient {
    pub client: Client,
    pub addr: String,
    _server_process: Child, // The underscore prevents warnings about it being unused
    _dir: TempDir,          // Ensures the temp directory is cleaned up
}

impl Drop for TestClient {
    fn drop(&mut self) {
        // Best effort to kill the server process.
        let _ = self._server_process.kill();
    }
}

impl TestClient {
    /// Executes a simple query and returns the results as a vector of rows,
    /// where each row is a vector of column strings.
    pub fn simple_query(&mut self, query: &str) -> Vec<Vec<String>> {
        let rows = self.client.simple_query(query).unwrap();
        rows.into_iter()
            .filter_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    Some(
                        (0..row.len())
                            .map(|i| {
                                let val = row.get(i).unwrap_or_default();
                                if val == "true" {
                                    "t".to_string()
                                } else if val == "false" {
                                    "f".to_string()
                                } else {
                                    val.to_string()
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    None
                }
            })
            .collect()
    }
}

pub fn setup_server_and_client(test_name: &str) -> TestClient {
    let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
    let addr = format!("127.0.0.1:{}", port);
    let client_addr = format!("host=localhost port={} user=postgres", port);

    let dir = tempdir().unwrap();
    let db_path = dir.path().join(format!("{}.db", test_name));
    let wal_path = dir.path().join(format!("{}.wal", test_name));

    // Ensure the parent directory for the WAL file exists.
    std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();

    let db_path_str = db_path.to_str().unwrap().to_string();
    let wal_path_str = wal_path.to_str().unwrap().to_string();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let server_path = format!("{}/../target/debug/titan_bin", manifest_dir);
    let server_process = Command::new(server_path)
        .env("TITAN_DB_PATH", &db_path_str)
        .env("TITAN_WAL_PATH", &wal_path_str)
        .env("TITAN_ADDR", &addr)
        .spawn()
        .expect("Failed to start server");

    // Give the server a moment to start up
    thread::sleep(Duration::from_millis(500));

    let client = Client::connect(&client_addr, NoTls).unwrap();
    TestClient {
        client,
        addr: client_addr,
        _server_process: server_process,
        _dir: dir,
    }
}

