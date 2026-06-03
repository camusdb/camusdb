# DML Controllers

SQL-level DML command controllers. Each `SQLExecutor*Creator` translates a raw SQL AST into a typed ticket and delegates to the corresponding lower-level controller.

| File | Responsibility |
|------|---------------|
| `SQLExecutorInsertCreator` | Parses `INSERT INTO` AST → `InsertTicket` → `RowInserter` |
| `SQLExecutorUpdateCreator` | Parses `UPDATE` AST → `UpdateTicket` → `RowUpdater` |
| `SQLExecutorDeleteCreator` | Parses `DELETE FROM` AST → `DeleteTicket` → `RowDeleter` |
| `SQLExecutorQueryCreator` | Parses `SELECT` AST → `QueryTicket` → `QueryExecutor` |
| `SQLExecutorBaseCreator` | Shared helpers (literal evaluation, expression parsing) used by all SQL creators |
| `SelectQueryCreator` | Builds a `SelectQuery` (logical query model) from a `SELECT` AST |
| `QueryTicketAdapter` | Converts a `SelectQuery` into a `QueryTicket` with analyzed predicates and ORDER BY |
