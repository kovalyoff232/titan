use postgres::{Client, NoTls, SimpleQueryMessage};
use serial_test::serial;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn server_binary_path() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/../target/debug/titan_bin")
}

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn start_server(db_path: &Path, wal_path: &Path, addr: &str) -> Child {
    Command::new(server_binary_path())
        .env("TITAN_DB_PATH", db_path)
        .env("TITAN_WAL_PATH", wal_path)
        .env("TITAN_ADDR", addr)
        .spawn()
        .expect("failed to start server")
}

fn connect_with_retry(conn_str: &str, timeout: Duration) -> Client {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect(conn_str, NoTls) {
            Ok(client) => return client,
            Err(e) => {
                if Instant::now() >= deadline {
                    panic!("failed to connect in time: {e}");
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn stop_server(server: &mut Child) {
    let _ = server.kill();
    let _ = server.wait();
}

fn query_rows(client: &mut Client, sql: &str) -> Vec<Vec<String>> {
    let rows = client.simple_query(sql).expect("query should succeed");
    rows.into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or_default().to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect()
}

#[test]
#[serial]
fn committed_rows_survive_multiple_restarts() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("restart_recovery.db");
    let wal_path = temp_dir.path().join("restart_recovery.wal");
    let port = pick_free_port();
    let addr = format!("127.0.0.1:{port}");
    let conn_str = format!("host=localhost port={port} user=postgres");

    let mut server = start_server(&db_path, &wal_path, &addr);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));

    query_rows(&mut client, "CREATE TABLE restart_t (id INT, name TEXT);");
    query_rows(&mut client, "COMMIT;");
    query_rows(&mut client, "BEGIN;");
    query_rows(&mut client, "INSERT INTO restart_t VALUES (1, 'alpha');");
    query_rows(&mut client, "COMMIT;");

    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let rows_after_restart = query_rows(&mut client, "SELECT id, name FROM restart_t ORDER BY id;");
    assert_eq!(
        rows_after_restart,
        vec![vec!["1".to_string(), "alpha".to_string()]]
    );

    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let rows_after_second_restart =
        query_rows(&mut client, "SELECT id, name FROM restart_t ORDER BY id;");
    assert_eq!(
        rows_after_second_restart,
        vec![vec!["1".to_string(), "alpha".to_string()]]
    );

    drop(client);
    stop_server(&mut server);
}
