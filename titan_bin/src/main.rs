use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::pager::Pager;
use bedrock::transaction::TransactionManager;
use bedrock::wal::WalManager;
use crate::executor::{ExecuteResult, ResultSet, Column};
use bytes::{BufMut, BytesMut};

mod parser;
mod executor;

fn write_message(stream: &mut TcpStream, msg_type: u8, data: &[u8]) -> io::Result<()> {
    let len = (data.len() + 4) as i32;
    stream.write_all(&[msg_type])?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(data)?;
    Ok(())
}

fn send_error_response(stream: &mut TcpStream, message: &str) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_u8(b'S'); // Severity
    data.put_slice(b"ERROR\0");
    data.put_u8(b'C'); // Code
    data.put_slice(b"08P01\0"); // protocol_violation
    data.put_u8(b'M'); // Message
    data.put_slice(message.as_bytes());
    data.put_u8(b'\0');
    data.put_u8(b'\0'); // End of message
    write_message(stream, b'E', &data)
}

fn send_ready_for_query(stream: &mut TcpStream) -> io::Result<()> {
    write_message(stream, b'Z', &[b'I']) // 'I' for Idle
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
        data.put_i32(0); // Table OID (unknown)
        data.put_i16(i as i16 + 1); // Column index
        data.put_i32(col.type_id as i32); // Type OID
        data.put_i16(if col.type_id == 23 { 4 } else { -1 }); // Type size (-1 for variable)
        data.put_i32(-1); // Type modifier
        data.put_i16(0); // Format code (text)
    }
    write_message(stream, b'T', &data)
}

fn send_data_row(stream: &mut TcpStream, row: &[String]) -> io::Result<()> {
    let mut data = BytesMut::new();
    data.put_i16(row.len() as i16);
    for val in row {
        data.put_i32(val.len() as i32);
        data.put_slice(val.as_bytes());
    }
    write_message(stream, b'D', &data)
}

fn handle_client(mut stream: TcpStream, bpm: Arc<BufferPoolManager>, tm: Arc<TransactionManager>, wal: Arc<Mutex<WalManager>>) -> io::Result<()> {
    println!("[handle_client] New connection from: {}", stream.peer_addr()?);
    let mut buffer = BytesMut::with_capacity(1024);

    // --- Startup Phase ---
    let mut startup_buf = [0; 1024];
    let _n = stream.read(&mut startup_buf)?;

    let request_code = u32::from_be_bytes(startup_buf[4..8].try_into().unwrap());
    if request_code == 80877103 { // SSLRequest
        println!("[handle_client] SSLRequest received, denying.");
        stream.write_all(&[b'N'])?;
        let n = stream.read(&mut startup_buf)?;
        if n == 0 { return Ok(()); }
    }
    
    let protocol_version = u32::from_be_bytes(startup_buf[4..8].try_into().unwrap());
    println!("[handle_client] Protocol version: {}", protocol_version);
    if protocol_version != 196608 { // 3.0
        send_error_response(&mut stream, "Unsupported protocol version")?;
        return Ok(());
    }
    
    println!("[handle_client] Sending AuthenticationOk and ReadyForQuery.");
    write_message(&mut stream, b'R', &0i32.to_be_bytes())?;
    send_ready_for_query(&mut stream)?;

    // --- Query Loop ---
    let mut in_transaction = false;
    let mut tx_id = 0;

    loop {
        let mut msg_type = [0u8; 1];
        if stream.read_exact(&mut msg_type).is_err() {
            println!("[handle_client] Connection closed by peer.");
            if in_transaction {
                tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
            }
            return Ok(());
        }

        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes)?;
        let len = i32::from_be_bytes(len_bytes) as usize - 4;

        buffer.resize(len, 0);
        stream.read_exact(&mut buffer)?;

        match msg_type[0] {
            b'Q' => { // Simple Query
                let query_str = String::from_utf8_lossy(&buffer[..len-1]).to_string();
                println!("[handle_client] Received query: '{}'", query_str);

                let stmts = match parser::sql_parser(&query_str) {
                    Ok(s) => s,
                    Err(e) => {
                        println!("[handle_client] Parsing failed: {:?}", e);
                        send_error_response(&mut stream, &format!("Parsing failed: {:?}", e))?;
                        send_ready_for_query(&mut stream)?;
                        continue;
                    }
                };

                let mut last_result: Option<ExecuteResult> = None;
                let mut tx_already_ended = false;

                for stmt in stmts {
                    // If a transaction hasn't been started manually with BEGIN, start one now.
                    // This creates an implicit transaction for single statements.
                    if !in_transaction {
                        tx_id = tm.begin();
                        in_transaction = true;
                        println!("[handle_client] Started implicit transaction with tx_id: {}", tx_id);
                    }

                    println!("[handle_client] Executing statement: {:?} in tx_id: {}", stmt, tx_id);
                    let snapshot = tm.create_snapshot(tx_id);
                    let result = executor::execute(&stmt, &bpm, &tm, &wal, tx_id, &snapshot);

                    // Handle transaction control statements explicitly
                    match &stmt {
                        parser::Statement::Begin => {
                            // The transaction was already started implicitly above.
                            // We just need to send the response.
                            send_command_complete(&mut stream, "BEGIN")?;
                            last_result = None; // Don't send another response below
                            continue;
                        }
                        parser::Statement::Commit => {
                            tm.commit(tx_id);
                            bpm.flush_all_pages()?;
                            in_transaction = false;
                            tx_already_ended = true;
                            send_command_complete(&mut stream, "COMMIT")?;
                            last_result = None; // Don't send another response below
                            continue;
                        }
                        parser::Statement::Rollback => {
                            tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm)?;
                            in_transaction = false;
                            tx_already_ended = true;
                            send_command_complete(&mut stream, "ROLLBACK")?;
                            last_result = None; // Don't send another response below
                            continue;
                        }
                        _ => {}
                    }

                    // Handle the result of other statements
                    match result {
                        Ok(res) => {
                            last_result = Some(res);
                        }
                        Err(e) => {
                            println!("[handle_client] Execution failed: {:?}", e);
                            send_error_response(&mut stream, &format!("Execution failed: {:?}", e))?;
                            // Abort the transaction on any error
                            if in_transaction {
                                tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                                in_transaction = false;
                                tx_already_ended = true;
                            }
                            last_result = None;
                            break; // Stop processing further statements in the batch
                        }
                    }
                }

                // Send the response for the *last* statement in the batch, if any.
                if let Some(execute_result) = last_result {
                    match execute_result {
                        ExecuteResult::Ddl => send_command_complete(&mut stream, "DDL")?,
                        ExecuteResult::Insert(count) => send_command_complete(&mut stream, &format!("INSERT 0 {}", count))?,
                        ExecuteResult::Delete(count) => send_command_complete(&mut stream, &format!("DELETE {}", count))?,
                        ExecuteResult::Update(count) => send_command_complete(&mut stream, &format!("UPDATE {}", count))?,
                        ExecuteResult::ResultSet(ResultSet { columns, rows }) => {
                            send_row_description(&mut stream, &columns)?;
                            let row_count = rows.len();
                            for row in &rows {
                                send_data_row(&mut stream, &row)?;
                            }
                            send_command_complete(&mut stream, &format!("SELECT {}", row_count))?;
                        }
                    }
                }

                // If the transaction is still active and wasn't explicitly committed/rolled back,
                // it means it was an implicit, single-statement transaction that should be committed.
                if in_transaction && !tx_already_ended {
                    println!("[handle_client] Committing implicit transaction with tx_id: {}", tx_id);
                    tm.commit(tx_id);
                    in_transaction = false;
                }

                send_ready_for_query(&mut stream)?;
            }
            b'X' => { // Terminate
                println!("[handle_client] Terminate message received. Closing connection.");
                if in_transaction {
                    tm.abort(tx_id, &mut wal.lock().unwrap(), &bpm).unwrap();
                }
                return Ok(());
            }
            _ => {
                println!("[handle_client] Received unknown message type: {}", msg_type[0]);
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let db_path = "titan.db";
    let wal_path = "titan.wal";

    // --- Recovery Phase ---
    let mut pager_for_recovery = Pager::open(db_path)?;
    let db_is_new = pager_for_recovery.num_pages == 0;
    let highest_tx_id = WalManager::recover(wal_path, &mut pager_for_recovery)?;
    println!("[RECOVERY] Completed. Highest TX ID found: {}", highest_tx_id);

    // --- Initialization after Recovery ---
    let pager = Pager::open(db_path)?;
    let bpm = Arc::new(BufferPoolManager::new(pager));
    let tm = Arc::new(TransactionManager::new(highest_tx_id + 1));
    let wal = Arc::new(Mutex::new(WalManager::open(wal_path)?));

    if db_is_new {
        println!("[INIT] New database detected, initializing system tables.");
        initialize_db(&bpm, &tm, &wal).expect("Failed to initialize database");
        // *** THE CRITICAL FIX ***
        println!("[INIT] Flushing all pages to disk after initialization.");
        bpm.flush_all_pages().expect("Failed to flush pages after init");
        println!("[INIT] Flushing completed.");
    }

    let listener = TcpListener::bind("127.0.0.1:5433")?;
    println!("TitanDB is running on port 5433");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let bpm_clone = Arc::clone(&bpm);
                let tm_clone = Arc::clone(&tm);
                let wal_clone = Arc::clone(&wal);
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, bpm_clone, tm_clone, wal_clone) {
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

/// Initializes the system catalogs for a new database.
fn initialize_db(bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wal: &Arc<Mutex<WalManager>>) -> Result<(), executor::ExecutionError> {
    println!("[initialize_db] Starting database initialization.");
    let tx_id = tm.begin();
    let snapshot = tm.create_snapshot(tx_id);

    // Create pg_class
    println!("[initialize_db] Creating pg_class table.");
    let create_pg_class = parser::CreateTableStatement {
        table_name: "pg_class".to_string(),
        columns: vec![
            parser::ColumnDef { name: "oid".to_string(), data_type: parser::DataType::Int },
            parser::ColumnDef { name: "relname".to_string(), data_type: parser::DataType::Text },
        ],
    };
    executor::execute(&parser::Statement::CreateTable(create_pg_class), bpm, tm, wal, tx_id, &snapshot)?;
    println!("[initialize_db] pg_class table created.");

    // Create pg_attribute
    println!("[initialize_db] Creating pg_attribute table.");
    let create_pg_attribute = parser::CreateTableStatement {
        table_name: "pg_attribute".to_string(),
        columns: vec![
            parser::ColumnDef { name: "attrelid".to_string(), data_type: parser::DataType::Int },
            parser::ColumnDef { name: "attname".to_string(), data_type: parser::DataType::Text },
            parser::ColumnDef { name: "atttypid".to_string(), data_type: parser::DataType::Int },
        ],
    };
    executor::execute(&parser::Statement::CreateTable(create_pg_attribute), bpm, tm, wal, tx_id, &snapshot)?;
    println!("[initialize_db] pg_attribute table created.");
    
    tm.commit(tx_id);
    println!("[initialize_db] Initialization transaction committed.");
    Ok(())
}
