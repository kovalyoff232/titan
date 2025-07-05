use std::net::TcpStream;
use std::io::{Read, Write};
use tempfile::{tempdir, TempDir};
use postgres::{Client, NoTls};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicU16, Ordering};
use std::process::{Child, Command, Stdio};

static NEXT_PORT: AtomicU16 = AtomicU16::new(6000);

// A simple client that understands a subset of the Postgres protocol.
pub struct TestClient {
    stream: TcpStream,
}

pub fn setup_server_and_client(test_name: &str) -> (TempDir, Child, Client, u16) {
    let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
    let addr = format!("127.0.0.1:{}", port);
    let client_addr = format!("host=localhost port={} user=postgres", port);

    let dir = tempdir().unwrap();
    let db_path = dir.path().join(format!("{}.db", test_name));
    let wal_path = dir.path().join(format!("{}.wal", test_name));
    
    // Ensure the parent directory for the WAL file exists.
    std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();

    let db_path_str = db_path.to_str().unwrap().to_string();
    let wal_path_str = wal_path.to_str().unwrap().to_string();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let server_path = format!("{}/../target/debug/titan_bin", manifest_dir);
    let server_process = Command::new(server_path)
        .env("TITAN_DB_PATH", &db_path_str)
        .env("TITAN_WAL_PATH", &wal_path_str)
        .env("TITAN_ADDR", &addr)
        .spawn()
        .expect("Failed to start server");

    // Give the server a moment to start up
    thread::sleep(Duration::from_millis(500));

    let client = Client::connect(&client_addr, NoTls).unwrap();
    (dir, server_process, client, port)
}


impl TestClient {
    pub fn connect(addr: &str) -> Self {
        let mut stream = TcpStream::connect(addr).expect("Failed to connect");
        // Perform startup
        let mut startup_message = Vec::new();
        let protocol_version = 196608i32; // 3.0
        startup_message.extend_from_slice(&protocol_version.to_be_bytes());
        let user = "user\0test\0";
        startup_message.extend_from_slice(user.as_bytes());
        let mut final_message = Vec::new();
        final_message.extend_from_slice(&((startup_message.len() + 4) as i32).to_be_bytes());
        final_message.extend_from_slice(&startup_message);
        stream.write_all(&final_message).unwrap();
        stream.flush().unwrap();
        // Read until ReadyForQuery
        Self::read_to_ready(&mut stream);
        Self { stream }
    }

    pub fn query(&mut self, sql: &str) -> Vec<Vec<String>> {
        // Send Query message
        let query_bytes = sql.as_bytes();
        let len = (query_bytes.len() + 1 + 4) as i32;
        self.stream.write_all(&[b'Q']).unwrap();
        self.stream.write_all(&len.to_be_bytes()).unwrap();
        self.stream.write_all(query_bytes).unwrap();
        self.stream.write_all(&[0]).unwrap();
        self.stream.flush().unwrap();

        Self::read_to_ready(&mut self.stream)
    }

    fn read_to_ready(stream: &mut TcpStream) -> Vec<Vec<String>> {
        let mut rows = Vec::new();
        let mut in_query_response = true;
        while in_query_response {
            let mut msg_type = [0u8; 1];
            if stream.read_exact(&mut msg_type).is_err() {
                break; // Connection closed
            }
            let mut len_bytes = [0u8; 4];
            stream.read_exact(&mut len_bytes).unwrap();
            let len = i32::from_be_bytes(len_bytes) as usize - 4;
            let mut msg_body = vec![0; len];
            if len > 0 {
                stream.read_exact(&mut msg_body).unwrap();
            }

            match msg_type[0] {
                b'D' => { // DataRow
                    let mut row_data = Vec::new();
                    let mut cursor = 2; // Skip column count
                    while cursor < msg_body.len() {
                        let col_len = i32::from_be_bytes(msg_body[cursor..cursor+4].try_into().unwrap()) as usize;
                        cursor += 4;
                        let col_val = String::from_utf8_lossy(&msg_body[cursor..cursor+col_len]).to_string();
                        row_data.push(col_val);
                        cursor += col_len;
                    }
                    rows.push(row_data);
                }
                b'Z' => { // ReadyForQuery
                    if let Some(status) = msg_body.get(0) {
                        if *status == b'I' { // Idle
                            in_query_response = false;
                        }
                    }
                }
                b'C' => { // CommandComplete
                    // Could potentially clear rows here if we wanted to only return the last result set
                }
                _ => {} // Ignore other messages like RowDescription etc.
            }
        }
        rows
    }
}

