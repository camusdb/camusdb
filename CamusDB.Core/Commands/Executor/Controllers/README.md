# Controllers

Controller implementations for every database operation. Each controller is a focused, stateless (or near-stateless) class that executes one category of work.

## Statement services

Extracted from `CommandExecutor` so the facade stays a composition root. Each owns one category of
work and documents the invariant that makes it correct.

| Service | Responsibility |
|---------|---------------|
| `ServerLevelStatementDispatcher` | Statements that open no database (CREATE/DROP/RENAME/RELINK DATABASE, COMMENT ON DATABASE, users, grants, cluster settings). Shared by the DDL and no-rows entry points so the two lists cannot drift |
| `DatabaseLifecycleService` | Create, branch, open, close, drop, relink, rename — and the fences that keep those correct against each other |
| `DDL/DdlStatementDispatcher` | Routes a parsed DDL statement to the service that executes it |
| `DDL/SchemaDdlService` | Table/column/index/constraint/comment DDL, and the three execution shapes (single transaction, two-phase index DDL, staged coordinator) |
| `DDL/DdlForwardingCoordinator` | Forwards DDL to the schema leader and waits until the change is visible locally |
| `DDL/TableSettingsService` | Version-neutral `ALTER TABLE … SET/RESET` storage parameters |
| `DDL/CreateTableAsSelectExecutor` | `CREATE TABLE … AS SELECT` and the relation-staging primitives a matview refresh uses |
| `DML/NonQueryStatementDispatcher` | Statements returning no rows; also accepts schema DDL by forwarding |
| `DML/RowCommandService` | Ticket-based `Insert` / `Update` / `Delete` / `Query` / `QueryById` |
| `DML/SetTransactionStatement` | The `SET TRANSACTION` family, shared by both entry points |
| `Queries/SelectStatementExecutor` | The read path: `SELECT` in all its forms, the `SHOW` family, distributed query fragments |
| `Queries/QueryResultStream` | Presents materialized rows as the async cursor every read path returns |
| `Auth/StatementAuthorizer` | The statement-level authorization gate and the ambient scope the per-table check reads |
| `Auth/UserAdminService` | Server-level user and grant administration |
| `Maintenance/BackgroundSchedulerHost` | The leader-owned background loops, their start ordering, and teardown |
| `Maintenance/MetadataDiscoveryService` | Finds databases/tables with background work by reading authoritative KV metadata |
| `Maintenance/StartupRecoveryService` | Reclaims what a crashed or dead run left behind |

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
