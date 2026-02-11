use postgres::{Client, NoTls, SimpleQueryMessage};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::{TempDir, tempdir};

pub struct TestClient {
    pub client: Client,
    pub _addr: String,
    _server_process: Child,
    _dir: TempDir,
}

impl Drop for TestClient {
    fn drop(&mut self) {
        let _ = self._server_process.kill();
        let _ = self._server_process.wait();
    }
}

impl TestClient {
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
    let dir = tempdir().unwrap();
    let db_path = dir.path().join(format!("{}.db", test_name));
    let wal_path = dir.path().join(format!("{}.wal", test_name));

    std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();

    let db_path_str = db_path.to_str().unwrap().to_string();
    let wal_path_str = wal_path.to_str().unwrap().to_string();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let server_path = format!("{}/../target/debug/titan_bin", manifest_dir);

    for _ in 0..10 {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map(|addr| addr.port())
            .expect("Failed to allocate free local port for test server");
        let addr = format!("127.0.0.1:{}", port);
        let client_addr = format!("host=localhost port={} user=postgres", port);

        let mut server_process = Command::new(&server_path)
            .env("TITAN_DB_PATH", &db_path_str)
            .env("TITAN_WAL_PATH", &wal_path_str)
            .env("TITAN_ADDR", &addr)
            .spawn()
            .expect("Failed to start server");

        for _ in 0..20 {
            if let Ok(client) = Client::connect(&client_addr, NoTls) {
                return TestClient {
                    client,
                    _addr: client_addr,
                    _server_process: server_process,
                    _dir: dir,
                };
            }
            thread::sleep(Duration::from_millis(100));
        }

        let _ = server_process.kill();
        let _ = server_process.wait();
    }

    panic!("Failed to connect to test server after multiple port attempts");
}
