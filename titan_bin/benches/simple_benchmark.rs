use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::LockManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use titan_bin::catalog::SystemCatalog;
use titan_bin::executor;
use titan_bin::parser;

fn setup_test_db() -> (
    Arc<BufferPoolManager>,
    Arc<TransactionManager>,
    Arc<LockManager>,
    Arc<Mutex<WalManager>>,
    Arc<Mutex<SystemCatalog>>,
) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let wal_path = temp_dir.path().join("test.wal");

    let pager = Pager::open(db_path).unwrap();
    let bpm = Arc::new(BufferPoolManager::new(pager));
    let tm = Arc::new(TransactionManager::new(1));
    let lm = Arc::new(LockManager::new());
    let wal = Arc::new(Mutex::new(WalManager::open(wal_path).unwrap()));
    let system_catalog = Arc::new(Mutex::new(SystemCatalog::new()));

    let tx_id = tm.begin();
    let snapshot = tm.create_snapshot(tx_id);

    let create_stmt =
        parser::sql_parser("CREATE TABLE test_table (id INT, name TEXT, value INT);").unwrap();
    executor::execute(
        &create_stmt[0],
        &bpm,
        &tm,
        &lm,
        &wal,
        &system_catalog,
        tx_id,
        &snapshot,
    )
    .unwrap();

    for i in 0..100 {
        let insert_stmt = parser::sql_parser(&format!(
            "INSERT INTO test_table VALUES ({}, 'name{}', {});",
            i,
            i,
            i * 10
        ))
        .unwrap();
        executor::execute(
            &insert_stmt[0],
            &bpm,
            &tm,
            &lm,
            &wal,
            &system_catalog,
            tx_id,
            &snapshot,
        )
        .unwrap();
    }

    tm.commit(tx_id).unwrap();

    (bpm, tm, lm, wal, system_catalog)
}

fn benchmark_simple_select(c: &mut Criterion) {
    let (bpm, tm, lm, wal, system_catalog) = setup_test_db();

    c.bench_function("simple_select", |b| {
        b.iter(|| {
            let tx_id = tm.begin();
            let snapshot = tm.create_snapshot(tx_id);
            let select_stmt =
                parser::sql_parser("SELECT * FROM test_table WHERE id < 50;").unwrap();
            let result = executor::execute(
                black_box(&select_stmt[0]),
                &bpm,
                &tm,
                &lm,
                &wal,
                &system_catalog,
                tx_id,
                &snapshot,
            );
            tm.commit(tx_id).unwrap();
            result
        })
    });
}

fn benchmark_aggregate(c: &mut Criterion) {
    let (bpm, tm, lm, wal, system_catalog) = setup_test_db();

    c.bench_function("aggregate_sum", |b| {
        b.iter(|| {
            let tx_id = tm.begin();
            let snapshot = tm.create_snapshot(tx_id);
            let select_stmt = parser::sql_parser("SELECT SUM(value) FROM test_table;").unwrap();
            let result = executor::execute(
                black_box(&select_stmt[0]),
                &bpm,
                &tm,
                &lm,
                &wal,
                &system_catalog,
                tx_id,
                &snapshot,
            );
            tm.commit(tx_id).unwrap();
            result
        })
    });
}

criterion_group!(benches, benchmark_simple_select, benchmark_aggregate);
criterion_main!(benches);
