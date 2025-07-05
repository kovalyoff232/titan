use postgres::{Client, NoTls};
use serial_test::serial;
use std::thread;
use std::time::Duration;

mod common;

#[test]
#[serial]
fn test_concurrent_updates_conflict() {
    // --- Настройка ---
    let mut test_client = common::setup_server_and_client("concurrency_test");
    let addr = test_client.addr.clone();

    test_client.simple_query("CREATE TABLE accounts (id INT, balance INT);");
    test_client.simple_query("INSERT INTO accounts VALUES (1, 100);");
    test_client.simple_query("COMMIT;");
    println!("[MAIN] Таблица 'accounts' создана и заполнена.");

    // --- Транзакции ---
    let addr_clone1 = addr.to_string();
    let handle1 = thread::spawn(move || {
        println!("[TX1] Поток запущен.");
        let mut client1 = Client::connect(&addr_clone1, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        println!("[TX1] Транзакция начата.");

        let result = client1
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

    let addr_clone2 = addr.to_string();
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

        assert!(
            result.is_err(),
            "Ожидалась ошибка конфликта сериализации"
        );
        println!("[TX2] Ожидаемо получили ошибку при обновлении. Откат.");
    });

    // --- Проверка ---
    handle1.join().unwrap();
    handle2.join().unwrap();

    println!("[MAIN] Оба потока завершены. Проверяем итоговый результат.");
    let final_result = test_client.simple_query("SELECT balance FROM accounts WHERE id = 1");
    let final_balance: i32 = final_result[0].parse().unwrap();

    println!("[MAIN] Итоговый баланс: {}.", final_balance);
    assert_eq!(
        final_balance, 110,
        "Итоговый баланс должен быть от первой транзакции"
    );
}