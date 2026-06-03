# Queries

Logical query model — the typed representation of a SELECT statement after parsing and before physical planning.

## Logical query model

| Type | Description |
|------|-------------|
| `SelectQuery` | Parsed SELECT — projection list, FROM sources, WHERE AST, joins, GROUP BY, ORDER BY, LIMIT |
| `BoundSelectQuery` | `SelectQuery` after name resolution: table/column references resolved to open descriptors |
| `QuerySource` | Discriminated union for a query source: table, derived table (subquery), or join |
| `TableSource` | A base table referenced in FROM |
| `JoinSource` | A JOIN source with kind (INNER/LEFT/RIGHT/CROSS) and ON predicate |
| `DerivedTableSource` | A subquery used as a table source (`FROM (SELECT …) AS alias`) |
| `BoundTableSource` | A `TableSource` with its open `TableDescriptor` |
| `BoundJoinRightSource` | The right side of a join after binding |
| `BoundDerivedTableSource` | A derived table after binding, including its output schema |

## Projection and ordering

| Type | Description |
|------|-------------|
| `ProjectionItem` | One item in the SELECT list — expression AST + optional alias |
| `OrderByItem` | One ORDER BY item — expression AST + direction |
| `AggregateCall` | An aggregate function call (`COUNT`, `SUM`, etc.) extracted from the projection |
| `AggregateKind` | Enum of supported aggregate functions |
| `DerivedColumnSchema` | Output column name and type inferred for a derived table |

## Runtime helpers

| Type | Description |
|------|-------------|
| `BoundRow` | A row together with its source table binding, used during join execution |
| `BoundPredicate` | A filter predicate bound to specific table sources |
| `ColumnRef` | A qualified column reference (`table.column`) resolved during binding |
| `QueryRowNameResolver` | Resolves ambiguous column names across multiple bound sources |
