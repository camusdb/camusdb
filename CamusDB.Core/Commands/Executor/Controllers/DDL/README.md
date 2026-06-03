# DDL Controllers

SQL-level DDL command controllers. Each `SQLExecutor*Creator` translates a raw SQL AST node into a typed ticket and calls the corresponding lower-level controller.

| File | Responsibility |
|------|---------------|
| `SQLExecutorCreateTableCreator` | Parses `CREATE TABLE` AST → `CreateTableTicket` → `TableCreator` |
| `SQLExecutorDropTableCreator` | Parses `DROP TABLE` AST → `DropTableTicket` → `TableDropper` |
| `SQLExecutorAlterTableCreator` | Parses `ALTER TABLE … ADD/DROP COLUMN` → `AlterTableTicket` → `TableColumnAlterer` |
| `SQLExecutorAlterIndexCreator` | Parses `ALTER INDEX` / `CREATE INDEX` / `DROP INDEX` → `AlterIndexTicket` → `TableIndexAlterer` |
| `TableColumnAdder` | Adds a column to an existing table: updates the schema and back-fills null values in existing rows |
| `TableColumnDropper` | Drops a column from an existing table: updates the schema and removes the column's data from existing rows |
| `TableIndexAdder` | Builds a new index on an existing table by scanning all rows and writing index entries |
| `TableIndexDropper` | Removes an index's entries from storage and drops it from the schema |

All multi-step operations are driven by a `FluxMachine` so each phase (open table, acquire lock, update catalog, write storage, commit) is an explicit, named step.
