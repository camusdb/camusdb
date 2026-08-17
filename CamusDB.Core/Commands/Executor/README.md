# Executor

Top-level command execution facade.

`CommandExecutor` is the single public entry point for all database operations. It is a **thin
facade**: it composes the services below, fans configuration swaps out to them via `ApplyOptions`,
disposes them, and otherwise delegates. Behavior lives in the services, not here — a method on
`CommandExecutor` that is more than a one-line delegation is a sign something was put in the wrong
place.

`ExecutorContext` carries the engine-level collaborators every extracted service needs (logger,
shared node, database registry, the openers, statistics, validator, cluster-mode flag). It
deliberately does **not** carry configuration: a component that reads a tunable keeps its own
`CamusDBOptions` field and receives published swaps through its own `ApplyOptions`, wired into
`CommandExecutor.ApplyOptions`. Handing a snapshot out of the context would look like injection while
silently latching one value for the process lifetime.

## Composition order matters

Services are constructed in dependency order inside the constructor. A collaborator **captured by
another constructor** must already be assigned when that constructor runs — unlike a facade field
read per call, which tolerates any order. Getting this wrong yields a `null` captured at composition
time that only fails much later, deep inside a statement. The two statement dispatchers and
`ServerLevelStatementDispatcher` guard their captured services with `ArgumentNullException.ThrowIfNull`
so a future reordering fails loudly at startup instead.

## Statement entry points

| Entry point | Handled by |
|---|---|
| `ExecuteSQLQuery` (rows) | `Controllers/Queries/SelectStatementExecutor` |
| `ExecuteNonSQLQuery` (no rows) | `Controllers/DML/NonQueryStatementDispatcher` |
| `ExecuteDDLSQL` | `Controllers/DDL/DdlStatementDispatcher` |

Statements that open **no database** — CREATE/DROP/RENAME/RELINK DATABASE, COMMENT ON DATABASE, user
and grant administration, cluster settings — are dispatched by the shared
`Controllers/ServerLevelStatementDispatcher`, consulted by both the DDL and no-rows entry points.
It is shared on purpose: a client routes any non-SELECT statement to whichever endpoint it uses for
those, and when the two lists were maintained separately they drifted.
`CamusDB.Tests/CommandsExecutor/TestServerLevelStatementParity.cs` pins that parity.

`ISchemaDdlForwarder` is the contract used to forward DDL to the schema leader when this node is a
follower; `Controllers/DDL/DdlForwardingCoordinator` owns that routing and the wait for the change to
become visible locally.

Sub-packages:

| Package | Contents |
|---------|---------|
| `Controllers/` | All controller and service implementations |
| `Models/` | Data transfer types: tickets, results, query models, plan nodes, state machine steps |
