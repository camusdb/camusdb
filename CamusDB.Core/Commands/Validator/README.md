# Validator

Input validation for all command tickets. Validation runs before any storage or catalog access, so bad input is rejected cheaply at the boundary.

`CommandValidator` is the single entry point. It holds one validator instance per command type and dispatches to it based on the ticket type.

`ValidatorBase` provides shared helpers (name length checks, character set validation, null checks) used by multiple validators.

Per-command validators:

| Validator | Checks |
|-----------|--------|
| `CreateDatabaseValidator` | Database name non-empty, valid characters, max length |
| `DropDatabaseValidator` | Database name present |
| `CreateTableValidator` | Table name, at least one column, column names unique, valid types, at most one primary key |
| `DropTableValidator` | Table name present |
| `AlterTableValidator` | Table name present, operation type valid, column spec non-empty |
| `AlterIndexValidator` | Index name present, operation type valid |
| `InsertValidator` | Table name present, at least one column-value pair, value types consistent |
| `UpdateValidator` | Table name present, at least one SET clause |
| `DeleteValidator` | Table name present |
| `QueryValidator` | Table name or SQL text present |
| `QueryByIdValidator` | Table name and row ID present |
| `ExecuteSQLValidator` | SQL text non-empty |
| `CloseDatabaseValidator` | Database name present |
