# Executor

Top-level command execution facade.

`CommandExecutor` is the single public entry point for all database operations. It owns instances of every controller and routes each typed ticket to the correct one. All operations follow the same lifecycle: validate ticket → open database/table → begin transaction → delegate to controller → commit or rollback.

`SqlExecutor` handles the `ExecuteSQL` path: it calls `SQLParserProcessor` to produce an AST, then dispatches to the appropriate SQL creator (see `Controllers/DML/SQLExecutor*`).

`ISchemaDdlForwarder` is the contract used to forward DDL operations to follower nodes when the current node is a Kommander leader.

Sub-packages:

| Package | Contents |
|---------|---------|
| `Controllers/` | All controller implementations (DDL, DML, queries, functions, database lifecycle) |
| `Models/` | Data transfer types: tickets, results, query models, plan nodes, state machine steps |
