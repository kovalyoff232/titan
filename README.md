# TitanDB: A Modern Relational Database from Scratch

TitanDB is a relational database management system (RDBMS) built entirely in Rust. It is designed as a modern, high-performance, and ACID-compliant database, drawing architectural inspiration from PostgreSQL. This project serves as a deep dive into the core components of a database, from disk-level page management to sophisticated query optimization.

## Key Features

- **ACID-Compliant Transaction Manager**: Ensures data integrity through:
  - **Multi-Version Concurrency Control (MVCC)**: Writers don't block readers, and readers don't block writers.
  - **Write-Ahead Logging (WAL)**: For durability and atomicity.
  - **ARIES-style Recovery**: Guarantees database consistency even after a crash.
  - **Deadlock Detection**: A waits-for graph detects and resolves deadlocks between transactions.

- **Pluggable Storage Engine ("Bedrock")**: The foundation of TitanDB, responsible for reliable data storage.
  - **Buffer Pool Manager**: An efficient in-memory cache for disk pages using a Clock-Sweep (Second-Chance) eviction policy.
  - **B-Tree Indexing**: Fully-featured B-Tree implementation for fast data retrieval, supporting insert, search, and delete operations.

- **Advanced Query Processor**: A sophisticated pipeline that turns SQL text into results.
  - **SQL Parser**: Hand-crafted using the `chumsky` library.
  - **Cost-Based Query Optimizer**: Analyzes table statistics (`ANALYZE`) to select the most efficient query execution plan. It intelligently chooses between `Sequential Scans` and `Index Scans`.
  - **Multiple Join Algorithms**: Supports `Nested Loop`, `Hash Join`, and `Merge Join` to handle complex queries efficiently.
  - **Volcano-style Executor**: Executes the physical plan using the iterator model.

- **PostgreSQL Wire Protocol Compatibility**: Use your favorite tools, like `psql` or any standard PostgreSQL driver, to connect to TitanDB.

- **Core Utilities**: Includes essential maintenance commands like `VACUUM` to reclaim space and `ANALYZE` to gather statistics for the query optimizer.

## Current Status

**Alpha / Educational.** TitanDB is a work-in-progress and should be considered experimental. It successfully implements the core functionality of a relational database and is a great resource for learning.

The current implementation uses a **multi-threaded** server model, where each client connection is handled in a separate thread.

## Getting Started

### Prerequisites

- Rust (latest stable version)
- A PostgreSQL client, such as `psql`.

### Building

Clone the repository and build the project using Cargo:

```sh
git clone <repository-url>
cd titan
cargo build --release
```

### Running the Server

You can run the server directly with Cargo:

```sh
cargo run --release
```

The server will start and listen for connections. By default, it uses the following configuration:
- **Database file**: `titan.db`
- **WAL file**: `titan.wal`
- **Address**: `127.0.0.1:5433`

You can override these settings using environment variables:

```sh
export TITAN_DB_PATH=/path/to/my.db
export TITAN_WAL_PATH=/path/to/my.wal
export TITAN_ADDR=0.0.0.0:6000
cargo run --release
```

### Connecting with `psql`

Once the server is running, you can connect to it using `psql`:

```sh
psql -h 127.0.0.1 -p 5433
```

### Example Session

Here is a small example of what you can do in TitanDB:

```sql
-- Create a new table
CREATE TABLE users (
    id INT,
    name TEXT,
    created_at DATE
);

-- Insert some data
INSERT INTO users VALUES (1, 'alice', DATE '2025-01-15');
INSERT INTO users VALUES (2, 'bob', DATE '2025-02-20');

-- Select the data
SELECT * FROM users WHERE id = 2;
-- Expected output:
--  id | name | created_at
-- ----+------+------------
--  2  | bob  | 2025-02-20
-- (1 row)

-- Analyze the table to gather statistics for the optimizer
ANALYZE users;

-- Create an index for faster lookups
CREATE INDEX idx_users_id ON users(id);
```
