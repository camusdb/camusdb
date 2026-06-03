# Models

Data transfer and domain model types used throughout the executor. Organized by concern:

| Sub-package | Contents |
|-------------|---------|
| [`Tickets/`](Tickets/README.md) | Input tickets — one per command type, carry all validated parameters for an operation |
| [`Results/`](Results/README.md) | Output result types returned to callers after an operation completes |
| [`Plans/`](Plans/README.md) | Physical plan node tree produced by the query planner |
| [`Predicates/`](Predicates/README.md) | Output of predicate analysis — indexable comparisons, column comparisons, residuals |
| [`Queries/`](Queries/README.md) | Logical query model — `SelectQuery`, `BoundSelectQuery`, join sources, projections |
| [`StateMachines/`](StateMachines/README.md) | Flux state and step enums for each multi-step DML/DDL operation |

Top-level model types:

| Type | Purpose |
|------|---------|
| `DatabaseDescriptor` | Runtime descriptor for an open database (schema, open tables, Kahuna reference) |
| `TableDescriptor` | Runtime descriptor for an open table (schema reference, `KvTableStore`) |
| `ColumnValue` | Discriminated union of a typed scalar value (int, float, string, bool, date, objectId, null) |
| `CompositeColumnValue` | Ordered list of `ColumnValue`s forming a composite index key |
| `QueryResultRow` | A single output row from a SELECT: a dictionary of column name → `ColumnValue` |
| `QueryFilter` | A compiled filter predicate (AST node + metadata) |
| `QueryOrderBy` | An ORDER BY item (expression + direction) |
| `QueryPlan` | The full execution plan for a SELECT (scan node, filter, row limit, physical tree) |
| `QueryPlanStep` | Describes one index scan step (type, index, bounds) |
