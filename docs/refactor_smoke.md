# Refactor Smoke Scenarios

This document defines quick smoke scenarios for refactor validation.
Use short timeouts for every step. If a step hangs, fail fast and investigate.

## 1. Parser smoke
- Query: `SELECT 1;`
- Query: `SELECT id FROM users WHERE id = 1;`
- Query: `WITH recent AS (SELECT id FROM users) SELECT * FROM recent;`
- Expectation: no parse errors for valid input; syntax errors for invalid SQL are deterministic.

## 2. Planner smoke
- Single-table filter: `SELECT id FROM users WHERE id = 1;`
- Join: `SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id;`
- Sort + limit: `SELECT id FROM users ORDER BY id LIMIT 10;`
- Expectation: logical/physical plans are produced without panic.

## 3. Executor smoke
- DDL: create/drop temp table lifecycle.
- DML: insert/update/delete one row and verify row counts.
- SELECT: projection/filter/join path returns rows with expected schema order.
- Expectation: executor returns stable row counts and no transaction leaks.

## 4. WAL/transaction smoke
- Start transaction, perform write, commit, verify visibility.
- Start transaction, perform write, rollback, verify data unchanged.
- Restart process and verify committed rows survive recovery.
- Expectation: commit persists, rollback discards, recovery replays committed state.

## 5. Mandatory timeout envelope
- `format-check`: 120s
- `build-check`: 900s
- `test-run`: 900s
- Any timeout is a failure and must be treated as blocking for merge.