# Tickets

Input tickets — one record type per command. Callers construct a ticket, pass it through the validator, then hand it to `CommandExecutor`. Tickets carry only validated, typed data; no raw SQL strings reach the executor layer (except `ExecuteSQLTicket` which carries the raw SQL for the parser).

| Ticket | Command |
|--------|---------|
| `CreateDatabaseTicket` | `CREATE DATABASE` |
| `DropDatabaseTicket` | `DROP DATABASE` |
| `CloseDatabaseTicket` | Close / release a database handle |
| `CreateTableTicket` | `CREATE TABLE` |
| `DropTableTicket` | `DROP TABLE` |
| `AlterTableTicket` | `ALTER TABLE … ADD/DROP COLUMN` |
| `AlterColumnTicket` | Per-column details for an `ALTER TABLE` operation |
| `AlterIndexTicket` | `CREATE INDEX` / `DROP INDEX` / `ALTER INDEX` |
| `InsertTicket` | `INSERT INTO` (single row) |
| `InsertBatchTicket` | Batch insert (multiple rows in one transaction) |
| `UpdateTicket` | `UPDATE … SET … WHERE` |
| `DeleteTicket` | `DELETE FROM … WHERE` |
| `QueryTicket` | `SELECT` (all clauses: projection, where, joins, order by, limit, …) |
| `QueryByIdTicket` | Point lookup by primary key |
| `OpenTableTicket` | Internal ticket to open and cache a table descriptor |
| `ExecuteSQLTicket` | Raw SQL string — parsed and dispatched by `SqlExecutor` |
