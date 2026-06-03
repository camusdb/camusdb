# Results

Return types from `CommandExecutor` methods. Each result carries the minimal information the caller needs to confirm the operation and, where appropriate, provide feedback (e.g. rows affected, generated IDs).

| Result | Returned by |
|--------|-------------|
| `InsertResult` | `InsertRow` — includes the generated `ObjectIdValue` for the new row |
| `UpdateResult` | `UpdateRows` — number of rows matched and updated |
| `DeleteResult` | `DeleteRows` — number of rows deleted |
| `DeleteByIdResult` | `DeleteById` — whether the target row was found and deleted |
| `CreateTableResult` | `CreateTable` — the new table's schema |
| `ExecuteDDLSQLResult` | DDL executed via raw SQL — confirmation that the statement succeeded |
| `ExecuteNonSQLResult` | Non-query SQL statements (e.g. `BEGIN`, `COMMIT`) |
