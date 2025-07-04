use std::net::TcpStream;
use std::io::{Read, Write};

// A simple client that understands a subset of the Postgres protocol.
pub struct TestClient {
    stream: TcpStream,
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
        loop {
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
                    break;
                }
                _ => {} // Ignore other messages like CommandComplete, RowDescription etc.
            }
        }
        rows
    }
}
