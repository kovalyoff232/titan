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

fn start_server(db_path: &Path, wal_path: &Path, addr: &str, failpoints: Option<&str>) -> Child {
    let mut cmd = Command::new(server_binary_path());
    cmd.env("TITAN_DB_PATH", db_path)
        .env("TITAN_WAL_PATH", wal_path)
        .env("TITAN_ADDR", addr);
    if let Some(points) = failpoints {
        cmd.env("TITAN_FAILPOINTS", points);
    }
    cmd.spawn().expect("failed to start server")
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

fn wait_for_background_sync() {
    thread::sleep(Duration::from_millis(400));
}

fn try_query_rows(client: &mut Client, sql: &str) -> Result<Vec<Vec<String>>, postgres::Error> {
    let rows = client.simple_query(sql)?;
    Ok(rows
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or_default().to_string())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .collect())
}

fn query_rows(client: &mut Client, sql: &str) -> Vec<Vec<String>> {
    try_query_rows(client, sql).expect("query should succeed")
}

#[test]
#[serial]
fn crash_matrix_commit_before_wal_does_not_persist_insert() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("crash_before_wal.db");
    let wal_path = temp_dir.path().join("crash_before_wal.wal");
    let port = pick_free_port();
    let addr = format!("127.0.0.1:{port}");
    let conn_str = format!("host=localhost port={port} user=postgres");

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    query_rows(&mut client, "CREATE TABLE crash_t (id INT, name TEXT);");
    query_rows(&mut client, "COMMIT;");
    wait_for_background_sync();
    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, Some("tm.commit.before_wal"));
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    client.simple_query("BEGIN;").expect("begin");
    client
        .simple_query("INSERT INTO crash_t VALUES (1, 'before_wal');")
        .expect("insert");
    let commit_result = client.simple_query("COMMIT;");
    assert!(
        commit_result.is_err(),
        "commit should fail when tm.commit.before_wal failpoint is active"
    );
    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let select_result = try_query_rows(&mut client, "SELECT id, name FROM crash_t ORDER BY id;");
    drop(client);
    stop_server(&mut server);

    match select_result {
        Ok(rows) => {
            assert!(
                rows.is_empty(),
                "row must not persist if commit fails before WAL commit record"
            );
        }
        Err(e) => {
            let Some(db_err) = e.as_db_error() else {
                panic!("unexpected non-db error during post-restart verification: {e:?}");
            };
            assert_eq!(
                db_err.code().code(),
                "XX000",
                "unexpected sqlstate during post-restart verification: {db_err:?}"
            );
        }
    }
}

#[test]
#[serial]
fn crash_matrix_flush_failure_after_commit_still_recovers_row() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("crash_flush.db");
    let wal_path = temp_dir.path().join("crash_flush.wal");
    let port = pick_free_port();
    let addr = format!("127.0.0.1:{port}");
    let conn_str = format!("host=localhost port={port} user=postgres");

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    query_rows(&mut client, "CREATE TABLE crash_t2 (id INT, name TEXT);");
    query_rows(&mut client, "COMMIT;");
    wait_for_background_sync();
    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, Some("bpm.flush.before_page"));
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    client.simple_query("BEGIN;").expect("begin");
    client
        .simple_query("INSERT INTO crash_t2 VALUES (1, 'durable_after_flush_fail');")
        .expect("insert");
    let commit_result = client.simple_query("COMMIT;");
    assert!(
        commit_result.is_err(),
        "commit should fail when bpm.flush.before_page failpoint is active"
    );
    wait_for_background_sync();
    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let recovery_rows = try_query_rows(
        &mut client,
        "SELECT oid, relname FROM pg_class ORDER BY oid;",
    );
    drop(client);
    stop_server(&mut server);

    let _rows = recovery_rows.expect("recovery catalog query should succeed");
}

#[test]
#[serial]
fn crash_matrix_commit_after_wal_keeps_server_usable_and_recovers() {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("crash_after_wal.db");
    let wal_path = temp_dir.path().join("crash_after_wal.wal");
    let port = pick_free_port();
    let addr = format!("127.0.0.1:{port}");
    let conn_str = format!("host=localhost port={port} user=postgres");

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let _ = query_rows(&mut client, "SELECT oid FROM pg_class ORDER BY oid;");
    wait_for_background_sync();
    drop(client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, Some("tm.commit.after_wal"));
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));

    client.simple_query("BEGIN;").expect("begin");
    let _ = query_rows(
        &mut client,
        "SELECT oid, relname FROM pg_class ORDER BY oid;",
    );

    let commit_result = client.simple_query("COMMIT;");
    assert!(
        commit_result.is_err(),
        "commit should fail when tm.commit.after_wal failpoint is active"
    );

    drop(client);
    let mut probe_client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let follow_up = try_query_rows(&mut probe_client, "SELECT oid FROM pg_class ORDER BY oid;");
    let follow_up_err = follow_up.expect_err(
        "follow-up query should fail while tm.commit.after_wal failpoint remains active",
    );
    let Some(db_err) = follow_up_err.as_db_error() else {
        panic!("expected db error for failpoint follow-up query: {follow_up_err:?}");
    };
    assert_eq!(
        db_err.code().code(),
        "XX000",
        "unexpected sqlstate for failpoint follow-up query: {db_err:?}"
    );
    drop(probe_client);
    stop_server(&mut server);

    let mut server = start_server(&db_path, &wal_path, &addr, None);
    let mut client = connect_with_retry(&conn_str, Duration::from_secs(8));
    let recovery_rows = try_query_rows(&mut client, "SELECT oid FROM pg_class ORDER BY oid;");
    drop(client);
    stop_server(&mut server);

    assert!(
        recovery_rows.is_ok(),
        "restart recovery should remain queryable after tm.commit.after_wal failure"
    );
}
