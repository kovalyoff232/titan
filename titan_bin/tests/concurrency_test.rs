use postgres::{Client, NoTls};
use serial_test::serial;
use std::thread;
use std::time::Duration;

mod common;

const TX_STAGGER_MS: u64 = 40;
const HOLD_BEFORE_UPDATE_MS: u64 = 80;
const HOLD_BEFORE_COMMIT_MS: u64 = 120;
const DEADLOCK_WAIT_MS: u64 = 80;

#[test]
#[serial]
fn test_concurrent_updates_conflict() {
    let mut test_client = common::setup_server_and_client("concurrency_test");

    test_client.simple_query("CREATE TABLE accounts (id INT, balance INT);");
    test_client.simple_query("INSERT INTO accounts VALUES (1, 100);");
    test_client.simple_query("COMMIT;");

    let addr_clone1 = test_client._addr.to_string();
    let handle1 = thread::spawn(move || {
        let mut client1 = Client::connect(&addr_clone1, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        client1
            .simple_query("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
            .unwrap();

        thread::sleep(Duration::from_millis(HOLD_BEFORE_UPDATE_MS));
        client1
            .simple_query("UPDATE accounts SET balance = 110 WHERE id = 1")
            .unwrap();
        thread::sleep(Duration::from_millis(HOLD_BEFORE_COMMIT_MS));
        client1.simple_query("COMMIT;").unwrap();
    });

    let addr_clone2 = test_client._addr.to_string();
    let handle2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(TX_STAGGER_MS));

        let mut client2 = Client::connect(&addr_clone2, NoTls).unwrap();
        client2.simple_query("BEGIN;").unwrap();
        client2
            .simple_query("SELECT balance FROM accounts WHERE id = 1")
            .unwrap();

        let result = client2.simple_query("UPDATE accounts SET balance = 120 WHERE id = 1");
        assert!(
            result.is_err(),
            "expected serialization conflict on second concurrent update"
        );
    });

    handle1.join().unwrap();
    handle2.join().unwrap();

    let final_rows = test_client.simple_query("SELECT balance FROM accounts WHERE id = 1");
    assert_eq!(final_rows.len(), 1);
    assert_eq!(final_rows[0].len(), 1);
    let final_balance: i32 = final_rows[0][0].parse().unwrap();
    assert_eq!(final_balance, 110);
}

#[test]
#[serial]
fn test_deadlock_detection() {
    let mut test_client = common::setup_server_and_client("deadlock_test");

    test_client.simple_query("CREATE TABLE deadlock_test (id INT);");
    test_client.simple_query("INSERT INTO deadlock_test VALUES (1);");
    test_client.simple_query("INSERT INTO deadlock_test VALUES (2);");
    test_client.simple_query("COMMIT;");

    let addr_clone1 = test_client._addr.to_string();
    let handle1 = thread::spawn(move || {
        let mut client1 = Client::connect(&addr_clone1, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        client1
            .simple_query("UPDATE deadlock_test SET id = 10 WHERE id = 1;")
            .unwrap();

        thread::sleep(Duration::from_millis(DEADLOCK_WAIT_MS));
        let result = client1.simple_query("UPDATE deadlock_test SET id = 20 WHERE id = 2;");

        if result.is_err() {
            client1.simple_query("ROLLBACK;").unwrap();
        } else {
            client1.simple_query("COMMIT;").unwrap();
        }
        result
    });

    let addr_clone2 = test_client._addr.to_string();
    let handle2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(TX_STAGGER_MS));

        let mut client2 = Client::connect(&addr_clone2, NoTls).unwrap();
        client2.simple_query("BEGIN;").unwrap();
        client2
            .simple_query("UPDATE deadlock_test SET id = 20 WHERE id = 2;")
            .unwrap();

        thread::sleep(Duration::from_millis(DEADLOCK_WAIT_MS));
        let result = client2.simple_query("UPDATE deadlock_test SET id = 10 WHERE id = 1;");

        if result.is_err() {
            client2.simple_query("ROLLBACK;").unwrap();
        } else {
            client2.simple_query("COMMIT;").unwrap();
        }
        result
    });

    let res1 = handle1.join().unwrap();
    let res2 = handle2.join().unwrap();

    assert!(
        res1.is_err() || res2.is_err(),
        "one transaction must fail due to deadlock"
    );
    assert!(
        !(res1.is_ok() && res2.is_ok()),
        "both transactions cannot succeed in deadlock cycle"
    );

    let final_rows = test_client.simple_query("SELECT * FROM deadlock_test;");
    let mut final_state: Vec<String> = final_rows.into_iter().flatten().collect();
    final_state.sort();

    let valid_final_state = final_state == vec!["1", "2"] || final_state == vec!["10", "20"];
    assert!(
        valid_final_state,
        "unexpected final state: {:?}",
        final_state
    );
}
