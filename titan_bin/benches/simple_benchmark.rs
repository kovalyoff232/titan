use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::LockManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use titan_bin::catalog::SystemCatalog;
use titan_bin::executor::{self, ExecuteCtx};
use titan_bin::parser;
use titan_bin::types::ExecuteResult;

struct BenchState {
    _temp_dir: TempDir,
    bpm: Arc<BufferPoolManager>,
    tm: Arc<TransactionManager>,
    lm: Arc<LockManager>,
    wal: Arc<Mutex<WalManager>>,
    system_catalog: Arc<Mutex<SystemCatalog>>,
}

fn execute_stmt(
    state: &BenchState,
    stmt: &parser::Statement,
    tx_id: u32,
) -> Result<ExecuteResult, titan_bin::errors::ExecutionError> {
    let snapshot = state.tm.create_snapshot(tx_id);
    let exec_ctx = ExecuteCtx {
        bpm: &state.bpm,
        tm: &state.tm,
        lm: &state.lm,
        wm: &state.wal,
        system_catalog: &state.system_catalog,
        tx_id,
        snapshot: &snapshot,
    };
    executor::execute(stmt, &exec_ctx)
}

fn setup_test_db() -> BenchState {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let wal_path = temp_dir.path().join("test.wal");

    let pager = Pager::open(db_path).unwrap();
    let bpm = Arc::new(BufferPoolManager::new(pager));
    let tm = Arc::new(TransactionManager::new(1));
    let lm = Arc::new(LockManager::new());
    let wal = Arc::new(Mutex::new(WalManager::open(wal_path).unwrap()));
    let system_catalog = Arc::new(Mutex::new(SystemCatalog::new()));
    let state = BenchState {
        _temp_dir: temp_dir,
        bpm,
        tm,
        lm,
        wal,
        system_catalog,
    };

    let tx_id = state.tm.begin();

    let create_stmt =
        parser::sql_parser("CREATE TABLE test_table (id INT, name TEXT, value INT);").unwrap();
    execute_stmt(&state, &create_stmt[0], tx_id).unwrap();

    for i in 0..100 {
        let insert_stmt = parser::sql_parser(&format!(
            "INSERT INTO test_table VALUES ({}, 'name{}', {});",
            i,
            i,
            i * 10
        ))
        .unwrap();
        execute_stmt(&state, &insert_stmt[0], tx_id).unwrap();
    }

    state.tm.commit(tx_id).unwrap();

    state
}

fn benchmark_simple_select(c: &mut Criterion) {
    let state = setup_test_db();

    c.bench_function("simple_select", |b| {
        b.iter(|| {
            let tx_id = state.tm.begin();
            let select_stmt =
                parser::sql_parser("SELECT * FROM test_table WHERE id < 50;").unwrap();
            let result = execute_stmt(&state, black_box(&select_stmt[0]), tx_id);
            state.tm.commit(tx_id).unwrap();
            result
        })
    });
}

fn benchmark_aggregate(c: &mut Criterion) {
    let state = setup_test_db();

    c.bench_function("aggregate_sum", |b| {
        b.iter(|| {
            let tx_id = state.tm.begin();
            let select_stmt = parser::sql_parser("SELECT SUM(value) FROM test_table;").unwrap();
            let result = execute_stmt(&state, black_box(&select_stmt[0]), tx_id);
            state.tm.commit(tx_id).unwrap();
            result
        })
    });
}

criterion_group!(benches, benchmark_simple_select, benchmark_aggregate);
criterion_main!(benches);
