# Query Planner

This is a developer reference for the SQL query pipeline. It describes every stage from SQL text to result rows, with enough detail to locate and change any part without breaking correctness.

## Pipeline overview

Every `SELECT` travels through five sequential stages before rows reach the caller:

```
SQL text
  │
  ▼
[1] SQLParserProcessor.Parse()
        produces NodeAst (raw AST)
  │
  ▼
[2] SelectQueryCreator.CreateSelectQuery()
        produces SelectQuery (typed logical model)
  │
  ▼
[2b] SubqueryRewriter (optional)
        rewrites IN / NOT IN subqueries into value-list AST nodes
  │
  ▼
[3] QueryBinder.BindAsync()
        opens tables, resolves aliases → BoundSelectQuery
  │
  ▼
[3b] ExistsSubqueryPreparer (optional)
        registers correlated EXISTS subquery executors
  │
  ▼
[4] QueryPlanner.GetPlan()  (single-table)
     or JoinQueryPlanner.GetPlan()  (joins / derived tables)
        produces QueryPlan with a PhysicalPlanNode tree
  │
  ▼
[5] QueryExecutor / QueryJoinExecutor
        walks the plan tree, drives IAsyncEnumerable<QueryResultRow>
```

The entry point is `CommandExecutor.ExecuteSQLQuery` (around line 641 of `CommandExecutor.cs`). It orchestrates every stage and returns `(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)` to the HTTP layer.

---

## Stage 1 — Parser

**File:** `CamusDB.Core/SQLParser/SQLParser.Language.grammar.y` (source), regenerated into `SQLParser.Parser.Generated.cs`

`SQLParserProcessor.Parse(sql)` runs the LALR(1) parser and returns a `NodeAst`. Every AST node is a single mutable `NodeAst` class with:

- `NodeType nodeType` — what kind of node this is (e.g. `Select`, `ExprEquals`, `Identifier`)
- `NodeAst? leftAst`, `rightAst` — primary children
- `NodeAst? extendedOne` … `extendedFive` — extra slots for clauses that don't fit left/right (GROUP BY uses `extendedOne`, ORDER BY uses `extendedTwo`, HAVING uses `extendedThree`, etc.)
- `string? yytext` — leaf value (column name, string literal, number)

**Parser regeneration:** the grammar and lexer files are MSBuild items (`YaccFile` / `LexFile`). Running `dotnet build CamusDB.Core/CamusDB.Core.csproj` automatically regenerates the two `Generated.cs` files. Commit the regenerated files together with any grammar change. Never hand-edit them.

---

## Stage 2 — Logical Query Model

**Files:**
- `SelectQueryCreator.cs` — converts AST → `SelectQuery`
- `CamusDB.Core/Commands/Executor/Models/Queries/` — all logical model types

`SelectQueryCreator.CreateSelectQuery(ast)` visits the `NodeAst.Select` node and produces an immutable `SelectQuery` record:

```csharp
public sealed record SelectQuery(
    QuerySource Source,       // FROM clause tree
    IReadOnlyList<ProjectionItem> Projections,
    BoundPredicate? Where,
    IReadOnlyList<NodeAst>? GroupBy,
    BoundPredicate? Having,
    IReadOnlyList<OrderByItem>? OrderBy,
    NodeAst? Limit,
    NodeAst? Offset,
    bool IsDistinct);
```

The `QuerySource` hierarchy represents the FROM clause as a tree:

| Type | Meaning |
|------|---------|
| `TableSource` | `FROM users` or `FROM users u` |
| `JoinSource` | `… JOIN posts p ON p.user_id = u.id` — wraps a left `QuerySource` and a right `QuerySource` |
| `DerivedTableSource` | `FROM (SELECT …) alias` |

`JoinSource` nodes nest left-deep: `A JOIN B JOIN C` becomes `JoinSource(JoinSource(A, B), C)`.

`QueryTicketAdapter.ToQueryTicket` bridges the new `SelectQuery` → legacy `QueryTicket` for the single-table executor path. The legacy `QueryTicket` carries the same data but in a flat shape that the original operators understand.

**Subquery rewriting (stage 2b):** before binding, `SubqueryRewriter` walks the WHERE predicate and, for uncorrelated `IN`/`NOT IN` subqueries, executes the inner query once, materializes the result set into a `SubqueryValueListAst` node, and replaces the subquery AST. This lets the downstream filterer treat `IN` as a value-list membership check without special subquery execution on every outer row. `EXISTS` subqueries are left in place for stage 3b.

---

## Stage 3 — Binding

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryBinder.cs`

`QueryBinder.BindAsync(database, selectQuery)` produces a `BoundSelectQuery`:

```csharp
public sealed class BoundSelectQuery
{
    public SelectQuery Query { get; }
    public IReadOnlyList<BoundTableSource> Sources { get; }      // opened TableDescriptors
    public IReadOnlyList<BoundDerivedTableSource> DerivedSources { get; }
    public QueryRowNameResolver RowNames { get; }
    public bool IsMultiSource { get; }   // true when joins or derived tables exist
}
```

Binding does the following in order:

1. **Open all table sources** — `TableOpener.OpenAsync` is called for every `TableSource` in the `QuerySource` tree, left-to-right. The opened `TableDescriptor` is stored in a `BoundTableSource` which also records the alias.
2. **Detect duplicate aliases** — throws `CamusDBException(InvalidInput)` on alias reuse.
3. **Build `QueryRowNameResolver`** — records which alias owns which column names and detects ambiguous unqualified references. A column name that appears in only one source can be referenced unqualified; one that appears in multiple sources requires `alias.column` notation.
4. **Validate projections** — ensures column references in `SELECT` items resolve in the row name map.
5. **Validate GROUP BY and ORDER BY** — checks that group expressions and sort keys resolve.
6. **Validate projection / grouping consistency** — non-aggregate projections in a grouped query must appear in `GROUP BY`.
7. **Validate JOIN predicates** — ensures `ON` predicate column references resolve to the correct sources.

For subqueries, `QueryBinder.BindSubqueryAsync` performs the same steps but skips HAVING validation (the caller validates separately).

**Stage 3b — EXISTS preparation:** `ExistsSubqueryPreparer` walks the `WHERE` and `HAVING` predicates, finds `EXISTS(subquery)` nodes, and registers an `ExistsSubqueryExecutor` for each. The executor is stored in the `ExistsSubqueryRegistry` threaded into `QueryTicket`. During row filtering, `QueryFilterer` calls the executor per outer row for correlated EXISTS.

---

## Stage 4 — Physical Planning

The planner converts a bound query into a tree of `PhysicalPlanNode` objects. There are two planners: one for single-table queries and one for joins.

### 4a. Single-table planner — `QueryPlanner`

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryPlanner.cs`

`QueryPlanner.GetPlan(database, table, ticket)` works in three phases:

**Phase A — Scan selection**

1. Calls `PredicateAnalyzer.AnalyzeTicket(ticket)` to classify every WHERE predicate (described below).
2. Calls `IndexScanSelector.TrySelectScan(table, analysis, orderBy)` to pick the best scan node.
3. Calls `PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep, table)` to assemble the runtime filter — predicates not absorbed by the chosen scan are combined into a single `NodeAst` AND tree that `QueryFilterer` evaluates per row.
4. Checks whether the chosen scan already satisfies the ORDER BY, so `SortNode` can be elided.
5. Calls `TryComputeScanRowLimit` to push LIMIT down into the scan when it is safe (no filter, no aggregation, no DISTINCT, no unsatisfied ORDER BY).

**Phase B — Plan tree construction**

The planner builds a linear operator chain from leaf (scan) to root (outermost operator). The shape depends on the query:

| Query shape | Operator chain (leaf → root) |
|-------------|------------------------------|
| Plain SELECT | `Scan → [Filter] → [Sort] → [Limit] → [Aggregate] → [HavingFilter] → [Project]` |
| GROUP BY | `Scan → [Filter] → Aggregate → [HavingFilter] → [Sort] → [Project] → [Limit]` |
| SELECT DISTINCT | `Scan → [Filter] → [Aggregate] → [HavingFilter] → [Project] → Distinct → [Sort] → [Limit]` |

`FilterNode` is a plan-tree node but does not become a `QueryPlanStep` — instead its predicate becomes `QueryPlan.ExecutionFilter`, which scan operators apply inline during row decoding (this avoids a separate streaming pass).

**Phase C — Post-plan passes**

After the tree is built:
- `QueryPlanStepAdapter.PopulateLinearSteps(plan)` flattens the tree into `plan.Steps` (a `List<QueryPlanStep>`) that the legacy linear executor in `QueryExecutor` walks.
- `ProjectionPushdownPlanner.Apply(plan)` annotates scan nodes with `RequiredColumns` (QP6.1) so `RowEncoder.DecodeAsync` only deserializes the columns actually needed downstream.

### Physical plan nodes

All nodes extend `PhysicalPlanNode` (`Models/Plans/PhysicalPlanNode.cs`), which has `PhysicalPlanNode? Input` (single child) and `IReadOnlySet<string>? RequiredColumns` (set by pushdown).

| Node | Step type | Meaning |
|------|-----------|---------|
| `TableScanNode(PrimaryRows)` | `FullScanFromTableIndex` | Full table scan via primary KV rows |
| `TableScanNode(ForcedIndex)` | `FullScanFromIndex` | Forced index scan (via `USE INDEX` hint) |
| `IndexLookupNode` | `QueryFromIndex` | Point lookup on a unique or multi-column index |
| `IndexRangeScanNode` | `RangeScanFromIndex` | Bounded range scan on an index |
| `FilterNode` | _(inline as ExecutionFilter)_ | Row predicate applied during scan |
| `AggregateNode` | `Aggregate` | Grouped or global aggregation |
| `HavingFilterNode` | `HavingFilter` | Post-aggregate row filter |
| `SortNode` | `SortBy` | In-memory sort (N-key comparator) |
| `ProjectNode` | `ReduceToProjections` | Column projection / aliasing |
| `DistinctNode` | `Distinct` | Duplicate elimination over projection tuples |
| `LimitNode` | `Limit` | LIMIT / OFFSET |
| `NestedLoopJoinNode` | _(join path only)_ | Nested-loop inner join |
| `IndexNestedLoopJoinNode` | _(join path only)_ | Index-probed inner join |
| `DerivedTableScanNode` | _(join path only)_ | Scan of a materialized derived table |

### 4b. PredicateAnalyzer

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/PredicateAnalyzer.cs`

`PredicateAnalyzer` classifies the WHERE predicate into three buckets:

1. **`IndexableComparisons`** (`List<AnalyzedComparison>`) — column-vs-constant comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`, `BETWEEN`). These can drive index selection and, once absorbed by a scan, are removed from the execution filter.
2. **`ColumnComparisons`** (`List<AnalyzedColumnComparison>`) — column-vs-column comparisons (used in join ON predicates and cross-table WHERE filters). Always become residual filters for now; a future pass may push them into join predicates.
3. **`ResidualConjuncts`** (`List<NodeAst>`) — everything else: OR, LIKE, ILIKE, non-deterministic expressions, subquery predicates. Always re-evaluated at runtime.

`CollectAndConjuncts` recursively splits any `AND` tree into individual terms before classification. This means `year >= 2001 AND year < 2005` produces two `AnalyzedComparison` entries that `IndexScanSelector` can combine into a single range scan.

`BuildExecutionFilter` reconstructs an AND tree from whichever comparisons were **not** absorbed by the chosen scan. Absorbed comparisons are suppressed using `IndexScanBoundAnalysis.IsComparisonAbsorbedByScan`, which checks that the scan key bounds make the predicate logically redundant.

The analyzer also accepts structured `QueryFilter` objects (the programmatic filter API) and merges them with the AST-derived analysis via `AnalyzeFilters` / `Merge`.

### 4c. IndexScanSelector

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/IndexScanSelector.cs`

`TrySelectScan` iterates all indexes on the table and scores each one:

| Match type | Score |
|-----------|-------|
| Full equality on all index columns, unique index | `10000 + column_count` |
| Full equality on all index columns, non-unique index | `5000 + column_count × 10` |
| Equality prefix + range on next column | `5000 + prefix_length × 10 + 1` |
| Equality prefix only | `5000 + prefix_length` |
| ORDER BY prefix match (no predicate) | `1000` |

The highest-scoring index wins. Ties are broken by choosing the index with fewer columns (more selective).

**Composite index matching** (implemented in `TryMatchPredicateIndex`):

1. Walk index columns left to right.
2. For each column in order, check whether there is an equality predicate (`=`). Accumulate equality prefix length.
3. If the full index is covered by equality: produce `QueryFromIndex` (unique) or a prefix-bounded `RangeScanFromIndex` (non-unique). The upper bound is computed by `BuildPrefixScanUpperBound`, which increments the last equality value by one ULP/integer to create an exclusive upper bound. String and Id column types cannot form an exclusive upper bound by increment, so they fall back to a full scan.
4. If the prefix is partial: check for a range predicate (`>`, `>=`, `<`, `<=`) on the next column and produce `RangeScanFromIndex` with both an equality prefix cap and the range bounds. The tighter of the two upper bounds wins (`TightenUpperBound`).

**ORDER BY scan elision:** if no predicate matches any index, `TrySelectOrderByScan` looks for an index whose leading columns match the ORDER BY columns exactly (ascending only). If found, a full `RangeScanFromIndex` (unbounded) is returned. `ScanSatisfiesOrderBy` confirms the match so `QueryPlanner` knows to omit `SortNode`.

### 4d. Join planner — `JoinQueryPlanner`

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/JoinQueryPlanner.cs`

When `BoundSelectQuery.IsMultiSource` is true, `JoinQueryPlanner.GetPlan` builds a join tree instead:

1. **Predicate pushdown** — `JoinPredicatePushdown.Analyze(bound, where)` splits the WHERE predicate into per-source scan filters (single-source predicates that can run during the table scan) and a post-join filter (cross-source predicates applied after row merging). The result is a `JoinPredicatePushdown.Result` with `ScanFiltersByAlias` and `PostJoinFilter`.
2. **Tree construction** — `BuildJoinTree` recurses the `QuerySource` tree:
   - `TableSource` → `TableScanNode` with its pushed-down filter
   - `DerivedTableSource` → `DerivedTableScanNode` (inner query materialized lazily)
   - `JoinSource` → left child is built recursively; right source is examined for equi-join opportunities by `JoinEquiJoinAnalyzer.TryMatch`. If the right join column has an index, the node is `IndexNestedLoopJoinNode`; otherwise it is `NestedLoopJoinNode`.
3. `QueryPlanStepAdapter.PopulateLinearSteps` and `ProjectionPushdownPlanner.Apply` run exactly as in the single-table planner.

---

## Stage 5 — Execution

### 5a. Single-table execution — `QueryExecutor`

**File:** `CamusDB.Core/Commands/Executor/Controllers/QueryExecutor.cs`

`ExecuteQueryPlanInternal` walks `plan.Steps` in order and pipes `IAsyncEnumerable<QueryResultRow>` through each operator:

- **`FullScanFromTableIndex`** → `QueryScanner.ScanUsingTableIndex` — iterates all rows via `KvTableStore.ScanRows`, decodes each row with `RowEncoder.DecodeAsync(schema, txId, rowId, data, requiredColumns)`, applies the execution filter inline.
- **`FullScanFromIndex`** → `QueryScanner.ScanUsingIndex` — iterates via the forced index.
- **`QueryFromIndex`** → `QueryUsingIndex` — unique index: `LookupUnique` → single row fetch. Non-unique index: builds a prefix upper bound and falls through to `QueryUsingRangeIndex`.
- **`RangeScanFromIndex`** → `QueryUsingRangeIndexInternal` — `KvTableStore.ScanIndex(txId, indexName, keyTypes, fromBound, toBound, unique, fromInclusive, toInclusive, maxRows: plan.ScanRowLimit)` → fetch + decode each matching row.
- **`SortBy`** → `QuerySorter.SortResultset` — materializes the full cursor into memory, sorts by the `OrderBy` columns using an N-key comparator, streams the sorted result.
- **`Aggregate`** → `QueryAggregator.AggregateResultset` — groups rows by the GROUP BY key (or global aggregate if no GROUP BY), accumulates COUNT / SUM / AVG / MIN / MAX, emits one result row per group.
- **`HavingFilter`** → `QueryFilterer.FilterHavingResultset` — evaluates the HAVING predicate against aggregated rows using the `QueryHavingWorkspace` scope (allows aggregate aliases like `x` in `HAVING x > 0`).
- **`ReduceToProjections`** → `QueryProjector.ProjectResultset` — evaluates each projection expression and renames columns to their output aliases.
- **`Distinct`** → `QueryDistincter.DistinctResultset` — hashes output row tuples, suppresses duplicates. NULL values compare equal for deduplication purposes.
- **`Limit`** → `QueryLimiter.LimitResultset` — skips OFFSET rows, stops after LIMIT rows.

All operators return `IAsyncEnumerable<QueryResultRow>`, so the chain is fully lazy — rows are streamed one at a time from storage to the caller without materializing the full result set, except where an operator requires full materialization (Sort, grouped Aggregate).

### 5b. Join execution — `QueryJoinExecutor`

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryJoinExecutor.cs`

`ExecuteJoinQuery` drives `ExecuteJoinTree(plan.Root, plan)`, then applies the post-join WHERE filter (if any), then hands the cursor to `QueryPostScanPipeline.Apply` for aggregation, sort, project, distinct, and limit.

`ExecuteJoinTree` pattern-matches on the plan node type:

- **`TableScanNode` with `BoundSource`** → `ScanBoundTable` — full table scan with inline per-alias execution filter.
- **`DerivedTableScanNode`** → `ScanDerivedTable` — lazily materializes the inner `BoundSelectQuery` the first time it is encountered (result is cached in `plan.DerivedMaterializations` so repeated scans within the same query see the same rows).
- **`NestedLoopJoinNode`** → `ExecuteNestedLoopJoin` — for each left row, scans the full right source, merges rows, evaluates the ON predicate.
- **`IndexNestedLoopJoinNode`** → `ExecuteIndexNestedLoopJoin` — for each left row, extracts the join key and probes the right index:
  - unique index → `LookupUnique` → single row
  - non-unique index → `ScanIndex` with equality prefix → iterate matching rows, stop when key changes

Row merging is handled by `QueryRowMerger.MergeRows` and `QualifyRow`:
- All right-side column names are stored with the qualified key `alias.column` and also as unqualified if no collision with the left side.
- Joined rows carry `RowId = default(ObjectIdValue)` — there is no single source row ID for a merged row.

### 5c. Post-scan pipeline — `QueryPostScanPipeline`

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryPostScanPipeline.cs`

`Apply` mirrors the `QueryPlanner` operator order for joins and derived-table queries. It applies the same aggregation / HAVING / sort / project / distinct / limit chain that `QueryExecutor` applies for single-table queries, so the two paths produce identical results.

Operator ordering rules enforced here:

| Has GROUP BY | Has DISTINCT | Order |
|-------------|-------------|-------|
| yes | — | scan → where → aggregate → having → sort → project → limit |
| no | yes | scan → where → [aggregate → having] → project → distinct → sort → limit |
| no | no | scan → where → sort → limit → [aggregate → having] → project |

---

## Optimization passes

### Projection pushdown (QP6.1)

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/ProjectionPushdownPlanner.cs`

`ProjectionPushdownPlanner.Apply` runs after the plan tree is built. `RequiredColumnAnalyzer.ComputeSingleTable(ticket)` walks the projection list, WHERE filter, ORDER BY, GROUP BY, and HAVING to collect every column name that any operator will need. For join plans, `ComputeJoinPlan` does the same per alias. The resulting `IReadOnlySet<string>?` is stored on every scan node as `RequiredColumns` and passed to `RowEncoder.DecodeAsync` so it skips decoding unreferenced columns. `null` means "all columns" (e.g. `SELECT *`).

### Limit pushdown (QP6.3)

**File:** `QueryPlanner.cs`, method `TryComputeScanRowLimit`

When all of these hold, the LIMIT value (plus OFFSET) is pushed to `QueryPlan.ScanRowLimit`:
- No runtime filter (`ExecutionFilter is null`)
- No aggregation in the projection
- No GROUP BY / HAVING
- No DISTINCT
- ORDER BY is satisfied by the scan (so the scan already emits rows in the correct order)

Scan operators pass `maxRows: plan.ScanRowLimit` to `KvTableStore.ScanIndex`, which stops iterating after that many entries. This avoids fetching rows from storage that LIMIT would discard.

### Filter absorption by scan bounds

**File:** `CamusDB.Core/Commands/Executor/Controllers/Queries/IndexScanBoundAnalysis.cs`

`IndexScanBoundAnalysis.IsComparisonAbsorbedByScan` checks whether an `AnalyzedComparison` is logically implied by the scan's key bounds. For example, if a scan has `fromBound=5, fromInclusive=true` for an integer column, a filter `col >= 3` is trivially true for all rows the scan returns and can be dropped from the execution filter. This avoids redundant per-row evaluation for comparisons already encoded in the index seek.

---

## File map for maintainers

| Concern | File(s) |
|---------|---------|
| SQL grammar and lexer | `CamusDB.Core/SQLParser/SQLParser.Language.grammar.y`, `SQLParser.Language.analyzer.lex` |
| AST node types | `CamusDB.Core/SQLParser/NodeAst.cs`, `NodeType.cs` |
| Logical query model | `CamusDB.Core/Commands/Executor/Models/Queries/` |
| Logical model builder | `CamusDB.Core/Commands/Executor/Controllers/DML/SelectQueryCreator.cs` |
| Legacy query ticket bridge | `CamusDB.Core/Commands/Executor/Controllers/DML/QueryTicketAdapter.cs` |
| Binder | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryBinder.cs` |
| Subquery rewriting | `CamusDB.Core/Commands/Executor/Controllers/Queries/SubqueryRewriter.cs` |
| Subquery execution | `CamusDB.Core/Commands/Executor/Controllers/Queries/SubqueryQueryExecutor.cs` |
| EXISTS preparation | `ExistsSubqueryPreparer.cs`, `ExistsSubqueryExecutor.cs` |
| Physical plan nodes | `CamusDB.Core/Commands/Executor/Models/Plans/` |
| Single-table planner | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryPlanner.cs` |
| Join planner | `CamusDB.Core/Commands/Executor/Controllers/Queries/JoinQueryPlanner.cs` |
| Predicate classification | `CamusDB.Core/Commands/Executor/Controllers/Queries/PredicateAnalyzer.cs` |
| Index scan selection | `CamusDB.Core/Commands/Executor/Controllers/Queries/IndexScanSelector.cs` |
| Scan bound absorption | `CamusDB.Core/Commands/Executor/Controllers/Queries/IndexScanBoundAnalysis.cs` |
| Join predicate pushdown | `CamusDB.Core/Commands/Executor/Controllers/Queries/JoinPredicatePushdown.cs` |
| Join equi-join analysis | `CamusDB.Core/Commands/Executor/Controllers/Queries/JoinEquiJoinAnalyzer.cs` |
| Plan tree → linear steps | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryPlanStepAdapter.cs` |
| Projection pushdown | `CamusDB.Core/Commands/Executor/Controllers/Queries/ProjectionPushdownPlanner.cs` |
| Post-scan pipeline | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryPostScanPipeline.cs` |
| Single-table executor | `CamusDB.Core/Commands/Executor/Controllers/QueryExecutor.cs` |
| Join executor | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryJoinExecutor.cs` |
| Derived table executor | `CamusDB.Core/Commands/Executor/Controllers/Queries/DerivedTableExecutor.cs` |
| Row scanning | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryScanner.cs` |
| Row filtering | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryFilterer.cs` |
| Sorting | `CamusDB.Core/Commands/Executor/Controllers/Queries/QuerySorter.cs` |
| Aggregation | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryAggregator.cs` |
| Projection | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryProjector.cs` |
| DISTINCT | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryDistincter.cs` |
| HAVING evaluation | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryHavingEvaluator.cs` |
| LIMIT / OFFSET | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryLimiter.cs` |
| Row merge for joins | `CamusDB.Core/Commands/Executor/Controllers/Queries/QueryRowMerger.cs` |
| Expression evaluator | `CamusDB.Core/SQLParser/SqlExecutor.cs` |
| KV table access | `CamusDB.Core/Storage/Kv/KvTableStore.cs` |
| Row encoding / decoding | `CamusDB.Core/CommandsExecutor/Models/RowEncoder.cs` |
| Query plan model | `CamusDB.Core/Commands/Executor/Models/QueryPlan.cs`, `QueryPlanStep.cs`, `QueryPlanStepType.cs` |
| Planner tests | `CamusDB.Tests/CommandsExecutor/TestQueryPlanner.cs`, `TestPredicateAnalyzer.cs`, `TestJoinQueryPlanner.cs` |
| Integration tests | `CamusDB.Tests/CommandsExecutor/TestExecuteSqlSelect.cs` |

---

## Adding a new SQL feature — checklist

1. **Lexer / grammar** — add tokens in `analyzer.lex`, extend rules in `grammar.y`, rebuild to regenerate `Generated.cs` files.
2. **AST → logical model** — update `SelectQueryCreator` (and `NodeAst` slot assignments if a new clause needs a slot) to populate the new field on `SelectQuery` or a model type under `Models/Queries/`.
3. **`QueryTicketAdapter`** — if the legacy `QueryTicket` path still runs (single-table queries), carry the new field through the adapter.
4. **Binder** — add validation in `QueryBinder` (scope rules, ambiguity, type checks).
5. **Planner** — add a plan node if needed, update `QueryPlanner.GetPlan` and/or `JoinQueryPlanner.GetPlan` to insert the new node in the correct position in the operator chain.
6. **`QueryPlanStepAdapter`** — add the new node's `Flatten` case to emit the correct `QueryPlanStep`.
7. **Executor** — add the new step type to the `switch` in `QueryExecutor.ExecuteQueryPlanInternal` and/or the corresponding operator in `QueryPostScanPipeline.Apply`.
8. **Operator implementation** — implement the operator as an `IAsyncEnumerable<QueryResultRow>` transformer (or a new operator class following the pattern of `QuerySorter`, `QueryAggregator`, etc.).
9. **Tests** — add parser tests, planner unit tests asserting the correct plan shape, and execution tests with exact expected rows.
