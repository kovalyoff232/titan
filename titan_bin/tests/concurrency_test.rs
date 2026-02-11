use postgres::{Client, NoTls};
use serial_test::serial;
use std::thread;
use std::time::Duration;

mod common;

#[test]
#[serial]
fn test_concurrent_updates_conflict() {
    let mut test_client = common::setup_server_and_client("concurrency_test");

    test_client.simple_query("CREATE TABLE accounts (id INT, balance INT);");
    test_client.simple_query("INSERT INTO accounts VALUES (1, 100);");
    test_client.simple_query("COMMIT;");
    println!("[MAIN] Таблица 'accounts' создана и заполнена.");

    let addr_clone1 = test_client._addr.to_string();
    let handle1 = thread::spawn(move || {
        println!("[TX1] Поток запущен.");
        let mut client1 = Client::connect(&addr_clone1, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        println!("[TX1] Транзакция начата.");

        let _result = client1
            .simple_query("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
            .unwrap();
        println!("[TX1] Прочитан баланс: 100");

        println!("[TX1] Засыпаем на 200 мс...");
        thread::sleep(Duration::from_millis(200));

        client1
            .simple_query("UPDATE accounts SET balance = 110 WHERE id = 1")
            .unwrap();
        println!("[TX1] Баланс обновлен на 110.");

        thread::sleep(Duration::from_millis(400));

        println!("[TX1] Коммит...");
        client1.simple_query("COMMIT;").unwrap();
        println!("[TX1] Транзакция завершена.");
    });

    let addr_clone2 = test_client._addr.to_string();
    let handle2 = thread::spawn(move || {
        println!("[TX2] Поток запущен.");
        thread::sleep(Duration::from_millis(100));

        let mut client2 = Client::connect(&addr_clone2, NoTls).unwrap();
        client2.simple_query("BEGIN;").unwrap();
        println!("[TX2] Транзакция начата.");

        client2
            .simple_query("SELECT balance FROM accounts WHERE id = 1")
            .unwrap();
        println!("[TX2] Прочитан баланс: 100");

        println!("[TX2] Пытаемся обновить баланс...");
        let result = client2.simple_query("UPDATE accounts SET balance = 120 WHERE id = 1");

        assert!(result.is_err(), "Ожидалась ошибка конфликта сериализации");
        println!("[TX2] Ожидаемо получили ошибку при обновлении. Откат.");
    });

    handle1.join().unwrap();
    handle2.join().unwrap();

    println!("[MAIN] Оба потока завершены. Проверяем итоговый результат.");
    let final_rows = test_client.simple_query("SELECT balance FROM accounts WHERE id = 1");
    assert_eq!(final_rows.len(), 1, "Должна остаться одна запись");
    assert_eq!(final_rows[0].len(), 1, "В записи должен быть один столбец");
    let final_balance: i32 = final_rows[0][0].parse().unwrap();
    println!("[MAIN] Итоговый баланс: {}.", final_balance);
    assert_eq!(
        final_balance, 110,
        "Итоговый баланс должен быть от первой транзакции"
    );
}

#[test]
#[serial]
fn test_deadlock_detection() {
    let mut test_client = common::setup_server_and_client("deadlock_test");

    test_client.simple_query("CREATE TABLE deadlock_test (id INT);");
    test_client.simple_query("INSERT INTO deadlock_test VALUES (1);");
    test_client.simple_query("INSERT INTO deadlock_test VALUES (2);");
    test_client.simple_query("COMMIT;");
    println!("[MAIN] Table 'deadlock_test' created and populated.");

    let addr_clone1 = test_client._addr.to_string();
    let handle1 = thread::spawn(move || {
        println!("[TX1] Thread started.");
        let mut client1 = Client::connect(&addr_clone1, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        println!("[TX1] Transaction started.");

        client1
            .simple_query("UPDATE deadlock_test SET id = 10 WHERE id = 1;")
            .unwrap();
        println!("[TX1] Locked resource 1.");

        thread::sleep(Duration::from_millis(200));

        println!("[TX1] Attempting to lock resource 2...");
        let result = client1.simple_query("UPDATE deadlock_test SET id = 20 WHERE id = 2;");

        if result.is_err() {
            println!("[TX1] Failed as expected due to deadlock.");
            client1.simple_query("ROLLBACK;").unwrap();
        } else {
            println!("[TX1] Succeeded, committing.");
            client1.simple_query("COMMIT;").unwrap();
        }
        result
    });

    let addr_clone2 = test_client._addr.to_string();
    let handle2 = thread::spawn(move || {
        println!("[TX2] Thread started.");

        thread::sleep(Duration::from_millis(100));

        let mut client2 = Client::connect(&addr_clone2, NoTls).unwrap();
        client2.simple_query("BEGIN;").unwrap();
        println!("[TX2] Transaction started.");

        client2
            .simple_query("UPDATE deadlock_test SET id = 20 WHERE id = 2;")
            .unwrap();
        println!("[TX2] Locked resource 2.");

        thread::sleep(Duration::from_millis(200));

        println!("[TX2] Attempting to lock resource 1...");
        let result = client2.simple_query("UPDATE deadlock_test SET id = 10 WHERE id = 1;");

        if result.is_err() {
            println!("[TX2] Failed as expected due to deadlock.");
            client2.simple_query("ROLLBACK;").unwrap();
        } else {
            println!("[TX2] Succeeded, committing.");
            client2.simple_query("COMMIT;").unwrap();
        }
        result
    });

    let res1 = handle1.join().unwrap();
    let res2 = handle2.join().unwrap();

    println!("[MAIN] Both threads finished.");

    assert!(
        res1.is_err() || res2.is_err(),
        "One of the transactions should have failed due to deadlock"
    );
    assert!(
        !(res1.is_ok() && res2.is_ok()),
        "Both transactions cannot succeed"
    );

    println!("[MAIN] Verifying final state.");

    let final_rows = test_client.simple_query("SELECT * FROM deadlock_test;");
    let mut final_state: Vec<String> = final_rows.into_iter().flatten().collect();
    final_state.sort();

    let tx1_committed = final_state == vec!["10", "20"];
    let tx2_committed = final_state == vec!["10", "20"];
    let nothing_committed = final_state == vec!["1", "2"];

    println!("[MAIN] Final state: {:?}", final_state);
    assert!(
        tx1_committed || tx2_committed || nothing_committed,
        "Final state is not one of the expected outcomes."
    );
}
