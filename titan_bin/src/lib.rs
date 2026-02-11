use crate::types::{Column, ExecuteResult, ResultSet};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::LockManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use bytes::{BufMut, BytesMut};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

pub mod aggregate_executor;
pub mod catalog;
pub mod errors;
pub mod executor;
pub mod limit_executor;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod sql_extensions;
pub mod types;
pub mod window_executor;

const SSL_REQUEST_CODE: u32 = 80_877_103;
const PG_PROTOCOL_V3: u32 = 196_608;
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

pub fn debug_logs_enabled() -> bool {
    std::env::var_os("TITAN_DEBUG_LOG").is_some()
}

#[macro_export]
macro_rules! titan_debug_log {
    ($($arg:tt)*) => {
        if $crate::debug_logs_enabled() {
            println!($($arg)*);
        }
    };
}

fn parse_startup_code(startup_buf: &[u8], read_len: usize) -> io::Result<u32> {
    if read_len < 8 || startup_buf.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "startup packet too short",
        ));
    }

    let mut code = [0u8; 4];
    code.copy_from_slice(&startup_buf[4..8]);
    Ok(u32::from_be_bytes(code))
}

fn parse_message_payload_len(len_bytes: [u8; 4]) -> io::Result<usize> {
    let total_len = i32::from_be_bytes(len_bytes);
    if total_len < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid message length prefix",
        ));
    }
    let payload_len = (total_len - 4) as usize;
    if payload_len > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message payload exceeds limit",
        ));
    }
    Ok(payload_len)
}

fn parse_simple_query_payload(payload: &[u8]) -> io::Result<String> {
    if payload.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty query payload",
        ));
    }
    let text = if payload.last() == Some(&0) {
        &payload[..payload.len() - 1]
    } else {
        payload
    };
    Ok(String::from_utf8_lossy(text).to_string())
}

fn lock_wal<'a>(wal: &'a Arc<Mutex<WalManager>>) -> io::Result<MutexGuard<'a, WalManager>> {
    wal.lock()
        .map_err(|_| io::Error::other("wal lock poisoned"))
}

fn abort_transaction(
    tm: &TransactionManager,
    wal: &Arc<Mutex<WalManager>>,
    bpm: &Arc<BufferPoolManager>,
    lm: &LockManager,
    tx_id: u32,
) -> io::Result<()> {
    let mut wal_guard = lock_wal(wal)?;
    tm.abort(tx_id, &mut wal_guard, bpm)?;
    drop(wal_guard);
    lm.unlock_all(tx_id);
    Ok(())
}

fn commit_transaction(
    tm: &TransactionManager,
    wal: &Arc<Mutex<WalManager>>,
    bpm: &Arc<BufferPoolManager>,
    lm: &LockManager,
    tx_id: u32,
    flush_pages: bool,
) -> io::Result<()> {
    let mut wal_guard = lock_wal(wal)?;
    tm.commit_with_wal(tx_id, &mut wal_guard)?;
    drop(wal_guard);
    lm.unlock_all(tx_id);
    if flush_pages {
        bpm.flush_all_pages()?;
    }
    Ok(())
}

fn write_message(stream: &mut TcpStream, msg_type: u8, data: &[u8]) -> io::Result<()> {
    let len = (data.len() + 4) as i32;
    stream.write_all(&[msg_type])?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(data)?;
    Ok(())
}

fn send_error_response(stream: &mut TcpStream, message: &str, code: &str) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_u8(b'S');
    data.put_slice(b"ERROR\0");
    data.put_u8(b'C');
    data.put_slice(code.as_bytes());
    data.put_u8(b'\0');
    data.put_u8(b'M');
    data.put_slice(message.as_bytes());
    data.put_u8(b'\0');
    data.put_u8(b'\0');
    write_message(stream, b'E', &data)
}

fn send_ready_for_query(stream: &mut TcpStream) -> io::Result<()> {
    write_message(stream, b'Z', b"I")
}

fn send_command_complete(stream: &mut TcpStream, tag: &str) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_slice(tag.as_bytes());
    data.put_u8(b'\0');
    write_message(stream, b'C', &data)
}

fn send_row_description(stream: &mut TcpStream, columns: &[Column]) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_i16(columns.len() as i16);
    for (i, col) in columns.iter().enumerate() {
        data.put_slice(col.name.as_bytes());
        data.put_u8(b'\0');
        data.put_i32(0);
        data.put_i16(i as i16 + 1);
        data.put_i32(col.type_id as i32);
        let type_size = match col.type_id {
            16 => 1,
            23 => 4,
            1082 => 4,
            _ => -1,
        };
        data.put_i16(type_size);
        data.put_i32(-1);
        data.put_i16(0);
    }
    write_message(stream, b'T', &data)
}

fn send_data_row(stream: &mut TcpStream, columns: &[Column], row: &[String]) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_i16(row.len() as i16);
    for (i, val) in row.iter().enumerate() {
        let final_val = if columns[i].type_id == 16 {
            if val.to_lowercase() == "t" { "t" } else { "f" }
        } else {
            val.as_str()
        };
        data.put_i32(final_val.len() as i32);
        data.put_slice(final_val.as_bytes());
    }
    write_message(stream, b'D', &data)
}

fn handle_client(
    mut stream: TcpStream,
    bpm: Arc<BufferPoolManager>,
    tm: Arc<TransactionManager>,
    lm: Arc<LockManager>,
    wal: Arc<Mutex<WalManager>>,
    system_catalog: Arc<Mutex<catalog::SystemCatalog>>,
) -> io::Result<()> {
    crate::titan_debug_log!(
        "[handle_client] New connection from: {}",
        stream.peer_addr()?
    );
    let mut buffer = BytesMut::with_capacity(1024);

    let mut startup_buf = [0; 1024];
    let mut startup_len = stream.read(&mut startup_buf)?;
    if startup_len == 0 {
        return Ok(());
    }

    let request_code = parse_startup_code(&startup_buf, startup_len)?;
    if request_code == SSL_REQUEST_CODE {
        crate::titan_debug_log!("[handle_client] SSLRequest received, denying.");
        stream.write_all(b"N")?;
        startup_len = stream.read(&mut startup_buf)?;
        if startup_len == 0 {
            return Ok(());
        }
        let request_code = parse_startup_code(&startup_buf, startup_len)?;
        if request_code == SSL_REQUEST_CODE {
            send_error_response(&mut stream, "Repeated SSLRequest", "08P01")?;
            return Ok(());
        }
    }

    let protocol_version = parse_startup_code(&startup_buf, startup_len)?;
    crate::titan_debug_log!("[handle_client] Protocol version: {}", protocol_version);
    if protocol_version != PG_PROTOCOL_V3 {
        send_error_response(&mut stream, "Unsupported protocol version", "08P01")?;
        return Ok(());
    }

    crate::titan_debug_log!("[handle_client] Sending AuthenticationOk and ReadyForQuery.");
    write_message(&mut stream, b'R', &0i32.to_be_bytes())?;
    send_ready_for_query(&mut stream)?;

    let mut in_transaction = false;
    let mut in_explicit_transaction = false;
    let mut tx_id = 0;

    loop {
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).is_err() {
            crate::titan_debug_log!("[handle_client] Connection closed by peer.");
            if in_transaction {
                if let Err(e) = abort_transaction(&tm, &wal, &bpm, &lm, tx_id) {
                    crate::titan_debug_log!(
                        "[handle_client] Failed to abort tx_id {}: {}",
                        tx_id,
                        e
                    );
                }
            }
            return Ok(());
        }

        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes)?;
        let len = parse_message_payload_len(len_bytes)?;

        buffer.resize(len, 0);
        stream.read_exact(&mut buffer)?;

        match msg_type[0] {
            b'Q' => {
                let query_str = match parse_simple_query_payload(&buffer[..len]) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::titan_debug_log!("[handle_client] Invalid query payload: {}", e);
                        send_error_response(&mut stream, "Invalid simple query payload", "08P01")?;
                        send_ready_for_query(&mut stream)?;
                        continue;
                    }
                };
                crate::titan_debug_log!("[handle_client] Received query: '{}'", query_str);

                let stmts = match parser::sql_parser(&query_str) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::titan_debug_log!("[handle_client] Parsing failed: {:?}", e);
                        send_error_response(
                            &mut stream,
                            &format!("Parsing failed: {:?}", e),
                            "42601",
                        )?;
                        send_ready_for_query(&mut stream)?;
                        continue;
                    }
                };

                let mut last_result: Option<ExecuteResult> = None;

                for stmt in stmts {
                    if !in_transaction {
                        tx_id = tm.begin();
                        in_transaction = true;
                        crate::titan_debug_log!(
                            "[handle_client] Started transaction with tx_id: {}",
                            tx_id
                        );
                    }

                    crate::titan_debug_log!(
                        "[handle_client] Executing statement: {:?} in tx_id: {}",
                        stmt,
                        tx_id
                    );
                    let snapshot = tm.create_snapshot(tx_id);
                    let exec_ctx = executor::ExecuteCtx {
                        bpm: &bpm,
                        tm: &tm,
                        lm: &lm,
                        wm: &wal,
                        system_catalog: &system_catalog,
                        tx_id,
                        snapshot: &snapshot,
                    };
                    let result = executor::execute(&stmt, &exec_ctx);

                    match &stmt {
                        parser::Statement::Begin => {
                            in_explicit_transaction = true;
                            send_command_complete(&mut stream, "BEGIN")?;
                            last_result = None;
                            continue;
                        }
                        parser::Statement::Commit => {
                            commit_transaction(&tm, &wal, &bpm, &lm, tx_id, true)?;
                            in_transaction = false;
                            in_explicit_transaction = false;
                            send_command_complete(&mut stream, "COMMIT")?;
                            last_result = None;
                            continue;
                        }
                        parser::Statement::Rollback => {
                            abort_transaction(&tm, &wal, &bpm, &lm, tx_id)?;
                            in_transaction = false;
                            in_explicit_transaction = false;
                            send_command_complete(&mut stream, "ROLLBACK")?;
                            last_result = None;
                            continue;
                        }
                        _ => {}
                    }

                    match result {
                        Ok(res) => {
                            last_result = Some(res);
                        }
                        Err(errors::ExecutionError::SerializationFailure) => {
                            crate::titan_debug_log!(
                                "[handle_client] Serialization failure for tx_id: {}.",
                                tx_id
                            );
                            send_error_response(&mut stream, "Serialization failure", "40001")?;
                            if let Err(e) = abort_transaction(&tm, &wal, &bpm, &lm, tx_id) {
                                crate::titan_debug_log!(
                                    "[handle_client] Failed to abort tx_id {}: {}",
                                    tx_id,
                                    e
                                );
                            }
                            in_transaction = false;
                            in_explicit_transaction = false;
                            last_result = None;
                            break;
                        }
                        Err(e) => {
                            crate::titan_debug_log!("[handle_client] Execution failed: {:?}", e);
                            send_error_response(
                                &mut stream,
                                &format!("Execution failed: {:?}", e),
                                "XX000",
                            )?;
                            if in_transaction {
                                if let Err(abort_err) =
                                    abort_transaction(&tm, &wal, &bpm, &lm, tx_id)
                                {
                                    crate::titan_debug_log!(
                                        "[handle_client] Failed to abort tx_id {}: {}",
                                        tx_id,
                                        abort_err
                                    );
                                }
                                in_transaction = false;
                                in_explicit_transaction = false;
                            }
                            last_result = None;
                            break;
                        }
                    }
                }

                if let Some(execute_result) = last_result {
                    match execute_result {
                        ExecuteResult::Ddl => send_command_complete(&mut stream, "DDL")?,
                        ExecuteResult::Insert(count) => {
                            send_command_complete(&mut stream, &format!("INSERT 0 {}", count))?
                        }
                        ExecuteResult::Delete(count) => {
                            send_command_complete(&mut stream, &format!("DELETE {}", count))?
                        }
                        ExecuteResult::Update(count) => {
                            send_command_complete(&mut stream, &format!("UPDATE {}", count))?
                        }
                        ExecuteResult::ResultSet(ResultSet { columns, rows }) => {
                            send_row_description(&mut stream, &columns)?;
                            let row_count = rows.len();
                            for row in &rows {
                                send_data_row(&mut stream, &columns, row)?;
                            }
                            send_command_complete(&mut stream, &format!("SELECT {}", row_count))?;
                        }
                    }
                }

                if in_transaction && !in_explicit_transaction {
                    crate::titan_debug_log!(
                        "[handle_client] Committing implicit transaction with tx_id: {}",
                        tx_id
                    );
                    commit_transaction(&tm, &wal, &bpm, &lm, tx_id, true)?;
                    in_transaction = false;
                }

                send_ready_for_query(&mut stream)?;
            }
            b'X' => {
                crate::titan_debug_log!(
                    "[handle_client] Terminate message received. Closing connection."
                );
                if in_transaction {
                    if let Err(e) = abort_transaction(&tm, &wal, &bpm, &lm, tx_id) {
                        crate::titan_debug_log!(
                            "[handle_client] Failed to abort tx_id {}: {}",
                            tx_id,
                            e
                        );
                    }
                }
                return Ok(());
            }
            _ => {
                crate::titan_debug_log!(
                    "[handle_client] Received unknown message type: {}",
                    msg_type[0]
                );
            }
        }
    }
}

pub fn run_server(db_path: &str, wal_path: &str, addr: &str) -> std::io::Result<()> {
    crate::titan_debug_log!(
        "[run_server] Starting server with db_path: {} and wal_path: {}",
        db_path,
        wal_path
    );

    let mut pager_for_recovery = Pager::open(db_path)?;
    let db_is_new = pager_for_recovery.num_pages == 0;
    let highest_tx_id = WalManager::recover(wal_path, &mut pager_for_recovery)?;
    crate::titan_debug_log!(
        "[RECOVERY] Completed. Highest TX ID found: {}",
        highest_tx_id
    );

    let pager = Pager::open(db_path)?;
    let bpm = Arc::new(BufferPoolManager::new(pager));
    let tm = Arc::new(TransactionManager::new(highest_tx_id + 1));
    let lm = Arc::new(LockManager::new());
    let wal = Arc::new(Mutex::new(WalManager::open(wal_path)?));
    let system_catalog = Arc::new(Mutex::new(catalog::SystemCatalog::new()));

    if db_is_new {
        crate::titan_debug_log!("[INIT] New database detected, initializing system tables.");
        initialize_db(&bpm, &tm, &lm, &wal, &system_catalog)
            .map_err(|e| io::Error::other(format!("failed to initialize database: {:?}", e)))?;

        crate::titan_debug_log!("[INIT] Flushing all pages to disk after initialization.");
        bpm.flush_all_pages()?;
        crate::titan_debug_log!("[INIT] Flushing completed.");
    }

    let listener = TcpListener::bind(addr)?;
    println!("TitanDB is running on port {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let bpm_clone = Arc::clone(&bpm);
                let tm_clone = Arc::clone(&tm);
                let lm_clone = Arc::clone(&lm);
                let wal_clone = Arc::clone(&wal);
                let system_catalog_clone = Arc::clone(&system_catalog);
                thread::spawn(move || {
                    if let Err(e) = handle_client(
                        stream,
                        bpm_clone,
                        tm_clone,
                        lm_clone,
                        wal_clone,
                        system_catalog_clone,
                    ) {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}

fn initialize_db(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wal: &Arc<Mutex<WalManager>>,
    system_catalog: &Arc<Mutex<catalog::SystemCatalog>>,
) -> Result<(), errors::ExecutionError> {
    crate::titan_debug_log!("[initialize_db] Starting database initialization.");
    let tx_id = tm.begin();
    let snapshot = tm.create_snapshot(tx_id);

    crate::titan_debug_log!("[initialize_db] Creating pg_class table.");
    let create_pg_class = parser::CreateTableStatement {
        table_name: "pg_class".to_string(),
        columns: vec![
            parser::ColumnDef {
                name: "oid".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "relname".to_string(),
                data_type: parser::DataType::Text,
            },
        ],
    };
    let exec_ctx = executor::ExecuteCtx {
        bpm,
        tm,
        lm,
        wm: wal,
        system_catalog,
        tx_id,
        snapshot: &snapshot,
    };
    executor::execute(&parser::Statement::CreateTable(create_pg_class), &exec_ctx)?;
    crate::titan_debug_log!("[initialize_db] pg_class table created.");

    crate::titan_debug_log!("[initialize_db] Creating pg_attribute table.");
    let create_pg_attribute = parser::CreateTableStatement {
        table_name: "pg_attribute".to_string(),
        columns: vec![
            parser::ColumnDef {
                name: "attrelid".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "attname".to_string(),
                data_type: parser::DataType::Text,
            },
            parser::ColumnDef {
                name: "atttypid".to_string(),
                data_type: parser::DataType::Int,
            },
        ],
    };
    executor::execute(
        &parser::Statement::CreateTable(create_pg_attribute),
        &exec_ctx,
    )?;
    crate::titan_debug_log!("[initialize_db] pg_attribute table created.");

    crate::titan_debug_log!("[initialize_db] Creating pg_statistic table.");
    let create_pg_statistic = parser::CreateTableStatement {
        table_name: "pg_statistic".to_string(),
        columns: vec![
            parser::ColumnDef {
                name: "starelid".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "staattnum".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "stadistinct".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "stakind".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "staop".to_string(),
                data_type: parser::DataType::Int,
            },
            parser::ColumnDef {
                name: "stanumbers".to_string(),
                data_type: parser::DataType::Text,
            },
            parser::ColumnDef {
                name: "stavalues".to_string(),
                data_type: parser::DataType::Text,
            },
        ],
    };
    executor::execute(
        &parser::Statement::CreateTable(create_pg_statistic),
        &exec_ctx,
    )?;
    crate::titan_debug_log!("[initialize_db] pg_statistic table created.");

    let mut wal_guard = lock_wal(wal)
        .map_err(|e| errors::ExecutionError::GenericError(format!("wal lock failed: {}", e)))?;
    tm.commit_with_wal(tx_id, &mut wal_guard)?;
    drop(wal_guard);
    lm.unlock_all(tx_id);
    crate::titan_debug_log!("[initialize_db] Initialization transaction committed.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        PG_PROTOCOL_V3, SSL_REQUEST_CODE, parse_message_payload_len, parse_simple_query_payload,
        parse_startup_code,
    };

    #[test]
    fn parses_startup_code() {
        let mut buf = [0u8; 8];
        buf[4..8].copy_from_slice(&PG_PROTOCOL_V3.to_be_bytes());
        let code = parse_startup_code(&buf, 8).unwrap();
        assert_eq!(code, PG_PROTOCOL_V3);
    }

    #[test]
    fn rejects_short_startup_packet() {
        let buf = [0u8; 4];
        assert!(parse_startup_code(&buf, 4).is_err());
    }

    #[test]
    fn parses_message_payload_length() {
        let payload_len = parse_message_payload_len((9_i32).to_be_bytes()).unwrap();
        assert_eq!(payload_len, 5);
    }

    #[test]
    fn rejects_invalid_message_payload_length() {
        assert!(parse_message_payload_len((3_i32).to_be_bytes()).is_err());
    }

    #[test]
    fn parses_query_payload_with_trailing_nul() {
        let q = parse_simple_query_payload(b"SELECT 1;\0").unwrap();
        assert_eq!(q, "SELECT 1;");
    }

    #[test]
    fn parses_query_payload_without_trailing_nul() {
        let q = parse_simple_query_payload(b"SELECT 2").unwrap();
        assert_eq!(q, "SELECT 2");
    }

    #[test]
    fn rejects_empty_query_payload() {
        assert!(parse_simple_query_payload(b"").is_err());
    }

    #[test]
    fn ssl_request_code_constant_is_stable() {
        assert_eq!(SSL_REQUEST_CODE, 80_877_103);
    }
}
