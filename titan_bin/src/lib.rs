use crate::types::{Column, ExecuteResult, ResultSet};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::LockManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use bytes::{BufMut, BytesMut};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
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
    println!(
        "[handle_client] New connection from: {}",
        stream.peer_addr()?
    );
    let mut buffer = BytesMut::with_capacity(1024);

    let mut startup_buf = [0; 1024];
    let _n = stream.read(&mut startup_buf)?;

    let request_code = u32::from_be_bytes(startup_buf[4..8].try_into().unwrap());
    if request_code == 80877103 {
        println!("[handle_client] SSLRequest received, denying.");
        stream.write_all(b"N")?;
        let n = stream.read(&mut startup_buf)?;
        if n == 0 {
            return Ok(());
        }
    }

    let protocol_version = u32::from_be_bytes(startup_buf[4..8].try_into().unwrap());
    println!("[handle_client] Protocol version: {}", protocol_version);
    if protocol_version != 196608 {
        send_error_response(&mut stream, "Unsupported protocol version", "08P01")?;
        return Ok(());
    }

    println!("[handle_client] Sending AuthenticationOk and ReadyForQuery.");
    write_message(&mut stream, b'R', &0i32.to_be_bytes())?;
    send_ready_for_query(&mut stream)?;

    let mut in_transaction = false;
    let mut in_explicit_transaction = false;
    let mut tx_id = 0;

    loop {
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).is_err() {
            println!("[handle_client] Connection closed by peer.");
            if in_transaction {
                tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                lm.unlock_all(tx_id);
            }
            return Ok(());
        }

        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes)?;
        let len = i32::from_be_bytes(len_bytes) as usize - 4;

        buffer.resize(len, 0);
        stream.read_exact(&mut buffer)?;

        match msg_type[0] {
            b'Q' => {
                let query_str = String::from_utf8_lossy(&buffer[..len - 1]).to_string();
                println!("[handle_client] Received query: '{}'", query_str);

                let stmts = match parser::sql_parser(&query_str) {
                    Ok(s) => s,
                    Err(e) => {
                        println!("[handle_client] Parsing failed: {:?}", e);
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
                        println!("[handle_client] Started transaction with tx_id: {}", tx_id);
                    }

                    println!(
                        "[handle_client] Executing statement: {:?} in tx_id: {}",
                        stmt, tx_id
                    );
                    let snapshot = tm.create_snapshot(tx_id);
                    let result = executor::execute(
                        &stmt,
                        &bpm,
                        &tm,
                        &lm,
                        &wal,
                        &system_catalog,
                        tx_id,
                        &snapshot,
                    );

                    match &stmt {
                        parser::Statement::Begin => {
                            in_explicit_transaction = true;
                            send_command_complete(&mut stream, "BEGIN")?;
                            last_result = None;
                            continue;
                        }
                        parser::Statement::Commit => {
                            tm.commit_with_wal(tx_id, &mut wal.lock().unwrap())?;
                            lm.unlock_all(tx_id);
                            bpm.flush_all_pages()?;
                            in_transaction = false;
                            in_explicit_transaction = false;
                            send_command_complete(&mut stream, "COMMIT")?;
                            last_result = None;
                            continue;
                        }
                        parser::Statement::Rollback => {
                            tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                            lm.unlock_all(tx_id);
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
                            println!(
                                "[handle_client] Serialization failure for tx_id: {}.",
                                tx_id
                            );
                            send_error_response(&mut stream, "Serialization failure", "40001")?;
                            tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                            lm.unlock_all(tx_id);
                            in_transaction = false;
                            in_explicit_transaction = false;
                            last_result = None;
                            break;
                        }
                        Err(e) => {
                            println!("[handle_client] Execution failed: {:?}", e);
                            send_error_response(
                                &mut stream,
                                &format!("Execution failed: {:?}", e),
                                "XX000",
                            )?;
                            if in_transaction {
                                tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                                lm.unlock_all(tx_id);
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
                    println!(
                        "[handle_client] Committing implicit transaction with tx_id: {}",
                        tx_id
                    );
                    tm.commit_with_wal(tx_id, &mut wal.lock().unwrap())?;
                    lm.unlock_all(tx_id);
                    bpm.flush_all_pages()?;
                    in_transaction = false;
                }

                send_ready_for_query(&mut stream)?;
            }
            b'X' => {
                println!("[handle_client] Terminate message received. Closing connection.");
                if in_transaction {
                    tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                    lm.unlock_all(tx_id);
                }
                return Ok(());
            }
            _ => {
                println!(
                    "[handle_client] Received unknown message type: {}",
                    msg_type[0]
                );
            }
        }
    }
}

pub fn run_server(db_path: &str, wal_path: &str, addr: &str) -> std::io::Result<()> {
    println!(
        "[run_server] Starting server with db_path: {} and wal_path: {}",
        db_path, wal_path
    );

    let mut pager_for_recovery = Pager::open(db_path)?;
    let db_is_new = pager_for_recovery.num_pages == 0;
    let highest_tx_id = WalManager::recover(wal_path, &mut pager_for_recovery)?;
    println!(
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
        println!("[INIT] New database detected, initializing system tables.");
        initialize_db(&bpm, &tm, &lm, &wal, &system_catalog)
            .expect("Failed to initialize database");

        println!("[INIT] Flushing all pages to disk after initialization.");
        bpm.flush_all_pages()
            .expect("Failed to flush pages after init");
        println!("[INIT] Flushing completed.");
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
                        println!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                println!("Connection failed: {}", e);
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
    println!("[initialize_db] Starting database initialization.");
    let tx_id = tm.begin();
    let snapshot = tm.create_snapshot(tx_id);

    println!("[initialize_db] Creating pg_class table.");
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
    executor::execute(
        &parser::Statement::CreateTable(create_pg_class),
        bpm,
        tm,
        lm,
        wal,
        system_catalog,
        tx_id,
        &snapshot,
    )?;
    println!("[initialize_db] pg_class table created.");

    println!("[initialize_db] Creating pg_attribute table.");
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
        bpm,
        tm,
        lm,
        wal,
        system_catalog,
        tx_id,
        &snapshot,
    )?;
    println!("[initialize_db] pg_attribute table created.");

    println!("[initialize_db] Creating pg_statistic table.");
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
        bpm,
        tm,
        lm,
        wal,
        system_catalog,
        tx_id,
        &snapshot,
    )?;
    println!("[initialize_db] pg_statistic table created.");

    tm.commit_with_wal(tx_id, &mut wal.lock().unwrap())?;
    lm.unlock_all(tx_id);
    println!("[initialize_db] Initialization transaction committed.");
    Ok(())
}
