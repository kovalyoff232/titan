use postgres::{Client, NoTls};
use serial_test::serial;
use std::sync::mpsc;
use std::thread;

mod common;

#[test]
#[serial]
fn own_write_is_visible_before_commit_and_hidden_after_rollback() {
    let mut test_client = common::setup_server_and_client("mvcc_own_write_test");

    test_client.simple_query("CREATE TABLE mvcc_t (id INT, v INT);");
    test_client.simple_query("COMMIT;");

    test_client.simple_query("BEGIN;");
    test_client.simple_query("INSERT INTO mvcc_t VALUES (1, 10);");
    let own_view = test_client.simple_query("SELECT id FROM mvcc_t WHERE id = 1;");
    assert_eq!(own_view.len(), 1);
    assert_eq!(own_view[0][0], "1");

    test_client.simple_query("ROLLBACK;");
    let post_rollback_view = test_client.simple_query("SELECT id FROM mvcc_t WHERE id = 1;");
    assert!(post_rollback_view.is_empty());
}

#[test]
#[serial]
#[ignore]
fn concurrent_uncommitted_insert_is_invisible_until_commit() {
    let mut test_client = common::setup_server_and_client("mvcc_uncommitted_insert_test");
    test_client.simple_query("CREATE TABLE mvcc_t2 (id INT, v INT);");
    test_client.simple_query("COMMIT;");

    let addr_for_writer = test_client._addr.to_string();
    let (inserted_tx, inserted_rx) = mpsc::channel();
    let (commit_tx, commit_rx) = mpsc::channel();
    let (committed_tx, committed_rx) = mpsc::channel();

    let writer = thread::spawn(move || {
        let mut client = Client::connect(&addr_for_writer, NoTls).unwrap();
        client.simple_query("BEGIN;").unwrap();
        client
            .simple_query("INSERT INTO mvcc_t2 VALUES (1, 100);")
            .unwrap();
        inserted_tx.send(()).unwrap();
        commit_rx.recv().unwrap();
        client.simple_query("COMMIT;").unwrap();
        committed_tx.send(()).unwrap();
    });

    inserted_rx.recv().unwrap();

    let addr_for_reader = test_client._addr.to_string();
    let mut reader = Client::connect(&addr_for_reader, NoTls).unwrap();
    reader.simple_query("BEGIN;").unwrap();
    let rows_while_uncommitted = reader
        .simple_query("SELECT id FROM mvcc_t2 WHERE id = 1;")
        .unwrap();
    assert!(rows_while_uncommitted.is_empty());
    reader.simple_query("COMMIT;").unwrap();

    commit_tx.send(()).unwrap();
    committed_rx.recv().unwrap();
    writer.join().unwrap();

    let rows_after_commit = test_client.simple_query("SELECT id FROM mvcc_t2 WHERE id = 1;");
    assert_eq!(rows_after_commit.len(), 1);
    assert_eq!(rows_after_commit[0][0], "1");
}

#[test]
#[serial]
#[ignore]
fn uncommitted_delete_is_visible_to_others_and_committed_delete_is_hidden() {
    let mut test_client = common::setup_server_and_client("mvcc_delete_visibility_test");
    test_client.simple_query("CREATE TABLE mvcc_t3 (id INT, v INT);");
    test_client.simple_query("INSERT INTO mvcc_t3 VALUES (1, 7);");
    test_client.simple_query("COMMIT;");

    let addr_for_deleter = test_client._addr.to_string();
    let (deleted_tx, deleted_rx) = mpsc::channel();
    let (commit_tx, commit_rx) = mpsc::channel();
    let (committed_tx, committed_rx) = mpsc::channel();

    let deleter = thread::spawn(move || {
        let mut client = Client::connect(&addr_for_deleter, NoTls).unwrap();
        client.simple_query("BEGIN;").unwrap();
        client
            .simple_query("DELETE FROM mvcc_t3 WHERE id = 1;")
            .unwrap();
        deleted_tx.send(()).unwrap();
        commit_rx.recv().unwrap();
        client.simple_query("COMMIT;").unwrap();
        committed_tx.send(()).unwrap();
    });

    deleted_rx.recv().unwrap();

    let addr_for_reader = test_client._addr.to_string();
    let mut reader = Client::connect(&addr_for_reader, NoTls).unwrap();
    reader.simple_query("BEGIN;").unwrap();
    let rows_while_delete_uncommitted = reader
        .simple_query("SELECT id FROM mvcc_t3 WHERE id = 1;")
        .unwrap();
    assert_eq!(rows_while_delete_uncommitted.len(), 1);
    reader.simple_query("COMMIT;").unwrap();

    commit_tx.send(()).unwrap();
    committed_rx.recv().unwrap();
    deleter.join().unwrap();

    let rows_after_delete_commit = test_client.simple_query("SELECT id FROM mvcc_t3 WHERE id = 1;");
    assert!(rows_after_delete_commit.is_empty());
}
