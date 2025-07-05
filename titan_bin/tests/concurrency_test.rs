use postgres::{Client, NoTls, SimpleQueryMessage};
use serial_test::serial;
use std::thread;
use std::time::Duration;

mod common;

#[test]
#[serial]
fn test_concurrent_updates_conflict() {
    // --- Настройка ---
    let (_dir, mut _server_process, mut client1, port) =
        common::setup_server_and_client("concurrency_test");
    let client2_addr = format!("host=localhost port={} user=postgres", port);

    // Используем simple_query для избежания расширенного протокола
    client1
        .simple_query("CREATE TABLE accounts (id INT, balance INT);")
        .unwrap();
    client1
        .simple_query("INSERT INTO accounts VALUES (1, 100);")
        .unwrap();
    client1.simple_query("COMMIT;").unwrap();
    println!("[MAIN] Таблица 'accounts' создана и заполнена.");

    // --- Транзакции ---
    let client1_addr = client2_addr.clone();
    let handle1 = thread::spawn(move || {
        println!("[TX1] Поток запущен.");
        let mut client1 = Client::connect(&client1_addr, NoTls).unwrap();
        client1.simple_query("BEGIN;").unwrap();
        println!("[TX1] Транзакция начата.");

        // Читаем начальное значение с блокировкой
        let result = client1
            .simple_query("SELECT balance FROM accounts WHERE id = 1 FOR UPDATE")
            .unwrap();
        if let Some(SimpleQueryMessage::Row(row)) = result.get(1) {
            let balance_str = row.get(0).unwrap();
            let balance: i32 = balance_str.parse().unwrap();
            println!("[TX1] Прочитан баланс: {}.", balance);
            assert_eq!(balance, 100);
        } else {
            panic!("TX1 не смогла прочитать строку");
        }

        // "Засыпаем", чтобы дать второй транзакции шанс начаться
        println!("[TX1] Засыпаем на 200 мс...");
        thread::sleep(Duration::from_millis(200));

        // Обновляем значение
        client1
            .simple_query("UPDATE accounts SET balance = 110 WHERE id = 1")
            .unwrap();
        println!("[TX1] Баланс обновлен на 110.");

        // "Засыпаем" еще раз перед коммитом
        thread::sleep(Duration::from_millis(400));

        println!("[TX1] Коммит...");
        client1.simple_query("COMMIT;").unwrap();
        println!("[TX1] Транзакция завершена.");
    });

    let handle2 = thread::spawn(move || {
        println!("[TX2] Поток запущен.");
        // Даем первой транзакции время на чтение
        thread::sleep(Duration::from_millis(100));

        let mut client2 = Client::connect(&client2_addr, NoTls).unwrap();
        client2.simple_query("BEGIN;").unwrap();
        println!("[TX2] Транзакция начата.");

        // Читаем значение. Должны увидеть значение *до* коммита TX1.
        println!("[TX2] Читаем баланс...");
        let result = client2
            .simple_query("SELECT balance FROM accounts WHERE id = 1")
            .unwrap();
        if let Some(SimpleQueryMessage::Row(row)) = result.get(1) {
            let balance_str = row.get(0).unwrap();
            let balance: i32 = balance_str.parse().unwrap();
            println!("[TX2] Прочитан баланс: {}.", balance);
            assert_eq!(
                balance, 100,
                "TX2 должна видеть состояние до коммита TX1"
            );
        } else {
            panic!("TX2 не смогла прочитать строку");
        }

        // Пытаемся обновить то же значение. Это должно вызвать конфликт или ожидание.
        println!("[TX2] Пытаемся обновить баланс...");
        let result = client2.simple_query("UPDATE accounts SET balance = 120 WHERE id = 1");

        // В текущей реализации MVCC с обнаружением конфликтов сериализации,
        // эта транзакция должна завершиться ошибкой.
        assert!(
            result.is_err(),
            "Ожидалась ошибка конфликта сериализации"
        );
        println!("[TX2] Ожидаемо получили ошибку при обновлении. Откат.");
        // Транзакция автоматически откатывается при ошибке
    });

    // --- Проверка ---
    handle1.join().unwrap();
    handle2.join().unwrap();

    println!("[MAIN] Оба потока завершены. Проверяем итоговый результат.");
    let final_result = client1
        .simple_query("SELECT balance FROM accounts WHERE id = 1")
        .unwrap();

    let mut final_balance = 0;
    if let Some(SimpleQueryMessage::Row(row)) = final_result.get(1) {
        final_balance = row.get(0).unwrap().parse().unwrap();
    }

    println!("[MAIN] Итоговый баланс: {}.", final_balance);
    assert_eq!(
        final_balance, 110,
        "Итоговый баланс должен быть от первой транзакции"
    );

    // Не забываем остановить сервер
    _server_process.kill().unwrap();
}