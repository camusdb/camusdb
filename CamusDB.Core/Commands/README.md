# Commands

Entry point for all database operations. Split into two sub-modules:

## Executor

`CommandExecutor` is the top-level facade. It wires together catalogs, storage, and transactions to carry out every SQL statement the engine supports. Internally it delegates to focused controllers:

- **DDL controllers** (`SQLExecutorCreateTableCreator`, `TableColumnAdder`, `TableIndexAdder`, …) — handle `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, and their index equivalents.
- **DML controllers** (`RowInserter`, `RowUpdater`, `RowDeleter`) — handle `INSERT`, `UPDATE`, `DELETE`.
- **Query controllers** (`QueryExecutor` and the `Queries/` sub-package) — handle `SELECT`, including index-scan planning, join execution, subquery flattening, aggregation, `DISTINCT`, and scalar function evaluation.
- **Database controllers** (`DatabaseOpener`, `DatabaseCreator`, `DatabaseDropper`, …) — manage database-level lifecycle.

Each operation is described by a *ticket* (e.g. `InsertTicket`, `QueryTicket`) that carries validated, typed parameters rather than raw strings.

## Validator

`CommandValidator` checks each ticket for structural validity (required fields, name lengths, type constraints) before the executor touches storage. Each command type has its own validator class under `Validators/`.
