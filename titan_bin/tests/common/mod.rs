use postgres::{Client, NoTls};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_PORT: AtomicU16 = AtomicU16::new(6000);

pub fn setup_server_and_client(test_name: &str) -> (TempDir, Child, Client, u16) {
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
    (dir, server_process, client, port)
}

