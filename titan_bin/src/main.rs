//! The main entry point for the TitanDB server.
//!
//! This file parses command-line arguments and starts the server.

use titan_bin::run_server;

fn main() -> std::io::Result<()> {
    let db_path = std::env::var("TITAN_DB_PATH").unwrap_or("titan.db".to_string());
    let wal_path = std::env::var("TITAN_WAL_PATH").unwrap_or("titan.wal".to_string());
    let addr = std::env::var("TITAN_ADDR").unwrap_or("127.0.0.1:5433".to_string());
    run_server(&db_path, &wal_path, &addr)
}
