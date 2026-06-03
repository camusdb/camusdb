# Controllers

Controller implementations for every database operation. Each controller is a focused, stateless (or near-stateless) class that executes one category of work.

## Database lifecycle

| Controller | Responsibility |
|------------|---------------|
| `DatabaseOpener` | Opens an existing database, loads schema and table descriptors |
| `DatabaseCreator` | Creates a new database directory and initial schema |
| `DatabaseCloser` | Flushes pending work and releases resources for a database |
| `DatabaseDropper` | Deletes a database and all its data |
| `DatabaseDescriptors` | In-memory registry of open `DatabaseDescriptor` objects |

## Table lifecycle

| Controller | Responsibility |
|------------|---------------|
| `TableOpener` | Loads a table's storage (`KvTableStore`) and index descriptors |
| `TableCreator` | Runs the `CREATE TABLE` Flux state machine |
| `TableDropper` | Drops a table and its indexes from storage and the catalog |
| `TableColumnAlterer` | `ALTER TABLE … ADD/DROP COLUMN` — updates catalog and back-fills/removes data |
| `TableIndexAlterer` | `ALTER INDEX` — delegates to `TableIndexAdder` / `TableIndexDropper` |

## Row operations (DML)

| Controller | Responsibility |
|------------|---------------|
| `RowInserter` | Inserts a single row; acquires locks, writes data, updates every index |
| `RowUpdater` | Updates rows matching a predicate; re-indexes changed columns |
| `RowDeleter` | Deletes rows matching a predicate; removes index entries |
| `RowSerializer` | Encodes a `Dictionary<string, ColumnValue>` to bytes for storage |
| `RowDeserializer` | Decodes raw bytes from storage back to a `Dictionary<string, ColumnValue>` |

## Query execution

`QueryExecutor` is the top-level SELECT coordinator. It calls into the `Queries/` sub-package for planning and execution, and into `Functions/` for scalar function evaluation.

`SchemaQuerier` handles `information_schema` queries (table/column/index metadata).

`TransactionStarter` provides helpers for acquiring a `KvTransaction` within a command.

Sub-packages:
- [`DDL/`](DDL/README.md) — SQL-level DDL controllers
- [`DML/`](DML/README.md) — SQL-level DML controllers and `SELECT` ticket builders
- [`Queries/`](Queries/README.md) — query planning and pipeline execution
- [`Functions/`](Functions/README.md) — scalar function registry and evaluation
