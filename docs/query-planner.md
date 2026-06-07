# Query Planner — Concepts & Developer Guide

This document explains how CamusDB turns a SQL `SELECT` string into result rows. It is written to be
read top to bottom by someone new to the project: **Part I** builds the mental model and vocabulary,
**Part II** walks the pipeline stage by stage, **Parts III–V** cover plan inspection, distributed-ready
metadata, and optimizations, and **Part VI** is an honest map of what is *not* built yet so you know
where to contribute. Parts VII–IX are reference material (file map, extension checklist, glossary).

If you only want to *use* `EXPLAIN`, see [`docs/explain.md`](./explain.md). This document is about how
the planner works internally and how to change it.

---

# Part I — Concepts

## What a query planner is, and why it exists

SQL is *declarative*: a query says **what** rows you want, not **how** to fetch them.
`SELECT * FROM robots WHERE id = 5` does not say "use the primary-key index" — it just describes the
result. The query planner is the component that decides the **how**: which index to use, whether to
sort, in what order to join tables, where to evaluate each predicate. Its job is to produce a correct
*execution plan* and, among the many correct plans, pick a fast one.

CamusDB sits on top of **Kahuna**, an ordered, transactional key–value store. So the planner's deeper
job is **translating relational operations into KV operations**: a table scan becomes an ordered range
read over a key prefix; an indexed lookup becomes a point read; a join becomes nested reads. This is the
same "SQL layer over a KV layer" split that CockroachDB and YugabyteDB use, and it is the single most
important idea in this codebase. The planner knows about tables, indexes, rows, and expressions; Kahuna
knows only about ordered keys and values.

## The five-stage pipeline (the core mental model)

Every `SELECT` flows through five stages. Keeping them separate is a deliberate design choice — it is
what lets us add joins or subqueries without destabilizing single-table queries.

```
 SQL text
    │  "SELECT email FROM users WHERE id = 5"
    ▼
[1] PARSE            SQLParserProcessor.Parse()              → NodeAst        (raw syntax tree)
    ▼
[2] BUILD MODEL      SelectQueryCreator.CreateSelectQuery()  → SelectQuery    (typed logical query)
    ▼
[3] BIND            QueryBinder.BindAsync()                  → BoundSelectQuery (names resolved, tables opened)
    ▼
[4] PLAN            QueryPlanner / JoinQueryPlanner          → QueryPlan      (physical operator tree)
    ▼
[5] EXECUTE         QueryExecutor / QueryJoinExecutor        → IAsyncEnumerable<QueryResultRow>
```

Each stage has a single responsibility and a distinct data type as its output. A useful way to remember
it: **parse** turns text into a tree, **bind** attaches meaning (which table is `u`? does column `email`
exist? is it ambiguous?), **plan** decides the strategy, **execute** runs it.

The entry point that orchestrates all five stages is `CommandExecutor.ExecuteSQLQuery`
(`CommandExecutor.cs:1146`). It returns `(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)` to the
HTTP layer.

## Logical vs physical plans

This distinction trips up newcomers, so it is worth stating plainly:

- A **logical plan** says *what* relational result is wanted: "the rows of `users` where `id = 5`,
  projected to `email`." It says nothing about indexes. In CamusDB the logical model is `SelectQuery`
  (and its bound form `BoundSelectQuery`).
- A **physical plan** says *how* to compute it: "point-lookup `users` by primary key `id = 5`, then
  project `email`." In CamusDB the physical plan is a tree of `PhysicalPlanNode` objects rooted at
  `QueryPlan.Root`.

The planner (stage 4) is exactly the function `logical plan → physical plan`. Two physical plans can be
*logically equivalent* (return the same rows) but wildly different in cost — choosing between them is the
planner's reason to exist.

## Heuristics vs cost-based optimization (where CamusDB is today)

There are two ways to choose a plan:

- **Rule/heuristic-based:** deterministic rules — "if an equality predicate matches a unique index, use
  a point lookup"; "put the most selective table first in a join." Fast, predictable, easy to test.
- **Cost-based (CBO):** estimate the cost of several candidate plans using table statistics (row counts,
  selectivity) and pick the cheapest. More powerful, but needs statistics and a cost model.

**CamusDB is primarily heuristic-based, with a small cost model bolted on.** This is intentional: the
project follows the "heuristics before CBO" path. A first cost model now exists — `CostEstimator`
annotates every plan node with an estimated row count and a `PlanCost`, fed by row-count statistics —
but it currently drives exactly **one** decision: whether a predicate-driven index range scan should be
replaced by a full table scan. Everything else (which index, join algorithm, operator order) is still
decided by deterministic rules. So the accurate mental model is: *rules choose the plan; the cost model
vetoes one specific index-vs-scan choice.* See [Part V](#part-v--optimization-passes) for the cost model
and [Part VI](#part-vi--current-capabilities--roadmap) for what remains.

## The core data structures and how they flow

Five types carry the query through the pipeline. Understanding their relationship is most of
understanding the planner:

| Stage output | Type | What it is | Key shape |
|---|---|---|---|
| 1 | `NodeAst` | Raw syntax tree | One mutable class; `nodeType` + `leftAst`/`rightAst` + `extendedOne..Five` + `yytext` |
| 2 | `SelectQuery` | Typed logical query | Immutable record: `Source`, `Projections`, `Where`, `GroupBy`, `Having`, `OrderBy`, `Limit`, `Offset`, `IsDistinct` |
| 3 | `BoundSelectQuery` | Logical query + resolved names | Adds opened `BoundTableSource`s, `QueryRowNameResolver`, `IsMultiSource` |
| 4 | `QueryPlan` | Physical plan | `Root` (a `PhysicalPlanNode` tree) + `Steps` (a flattened linear list) |
| 5 | `QueryResultRow` | A result row | `readonly struct`: `ObjectIdValue RowId` + `Dictionary<string,ColumnValue> Row` (keyed by column name) |

Two subtleties worth internalizing early:

1. **`NodeAst` never disappears.** Even after stage 2, expressions (the `WHERE` predicate, projection
   expressions, `ON` conditions) are still raw `NodeAst` subtrees — they are evaluated at runtime by
   `SqlExecutor.EvalExpr`. The models wrap *structure* (which table, which projection) in typed objects
   but keep *expressions* as AST. So you will see `NodeAst` flowing all the way into the executor.
2. **A `QueryPlan` holds the plan twice.** `Root` is the real tree (used by the renderer, EXPLAIN, the
   join executor, and all the distributed-ready metadata). `Steps` is a flattened, leaf-first linear list produced by
   `QueryPlanStepAdapter`, consumed by the *single-table* executor, which predates the tree. They share
   the same node instances (the linear list points at the same `PhysicalPlanNode` objects), so per-node
   data like runtime stats is visible through both. When you add a node type, you touch both the tree
   builder and the flattener.

## Two execution paths, and why there are two

CamusDB has **two executors**, and knowing which one runs is essential:

- **Single-table path** (`QueryExecutor`): the original, linear, step-by-step executor. Runs when the
  query has exactly one source. Walks `plan.Steps`.
- **Join / multi-source path** (`QueryJoinExecutor`): runs when `BoundSelectQuery.IsMultiSource` is true
  (any join, comma join, or derived table). Walks the `plan.Root` tree recursively, then hands the
  merged cursor to `QueryPostScanPipeline` for the shared aggregate/sort/project/distinct/limit stages.

This duality is historical: the single-table linear path came first; the tree-based join path was added
on top. Both deliberately produce identical post-scan behavior because the join path reuses
`QueryPostScanPipeline`, which mirrors the single-table operator ordering. When you change post-scan
behavior (e.g. how `DISTINCT` works), change it in a way that both paths see.

## The streaming model

Every operator is an `IAsyncEnumerable<QueryResultRow>` transformer, so the pipeline is **lazy by
default**: rows are pulled one at a time from storage through the operator chain to the caller, without
materializing the whole result set. The exceptions are operators that *must* see all input before
emitting anything: `SortNode` (needs all rows to sort) and a grouped `AggregateNode` (needs all rows to
finish each group). Keep this in mind when adding operators — prefer streaming; materialize only when the
relational semantics force it.

---

# Part II — The pipeline, stage by stage

## Stage 1 — Parser

**Files:** `CamusDB.Core/SQLParser/SQLParser.Language.grammar.y` (grammar),
`SQLParser.Language.analyzer.lex` (lexer), regenerated into `SQLParser.Parser.Generated.cs` /
`SQLParser.Scanner.Generated.cs`.

`SQLParserProcessor.Parse(sql)` runs the generated LALR(1) parser and returns a `NodeAst`. Every node is
the same mutable `NodeAst` class:

- `NodeType nodeType` — the kind of node (`Select`, `ExprEquals`, `Identifier`, …; see `NodeType.cs`)
- `NodeAst? leftAst`, `rightAst` — primary children
- `NodeAst? extendedOne … extendedFive` — extra slots for clauses that don't fit left/right. The
  `select_stmt` grammar rule assigns: projection, table, where, order, limit, offset, group, distinct
  flag, having across these slots — **read the `select_stmt` rule before adding a clause** so you claim a
  free slot correctly.
- `string? yytext` — leaf value (a column name, string literal, or number)

**Parser regeneration is automatic at build.** The grammar/lexer are MSBuild `YaccFile`/`LexFile` items.
Running `dotnet build CamusDB.Core/CamusDB.Core.csproj` regenerates the two `Generated.cs` files. They
**are committed** — stage the regenerated files with your grammar change, and never hand-edit them.

**What the parser already understands:** `SELECT [DISTINCT]`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`,
`LIMIT`/`OFFSET`, `[INNER] JOIN … ON …`, comma joins, dotted identifiers (`u.email`), scalar / `IN` /
`NOT IN` / `EXISTS` subqueries, and `EXPLAIN [(LOGICAL|PHYSICAL|ANALYZE)]`.

## Stage 2 — Logical Query Model

**Files:** `Controllers/DML/SelectQueryCreator.cs` (builder),
`Models/Queries/` (all logical model types).

`SelectQueryCreator.CreateSelectQuery(ast)` visits the `NodeAst.Select` node and produces an immutable
`SelectQuery` record:

```csharp
public sealed record SelectQuery(
    QuerySource Source,                       // FROM clause as a tree
    IReadOnlyList<ProjectionItem> Projections,
    BoundPredicate? Where,
    IReadOnlyList<NodeAst>? GroupBy,
    BoundPredicate? Having,
    IReadOnlyList<OrderByItem>? OrderBy,
    NodeAst? Limit,
    NodeAst? Offset,
    bool IsDistinct);
```

The `QuerySource` hierarchy models the FROM clause as a tree:

| Type | Meaning |
|------|---------|
| `TableSource` | `FROM users` or `FROM users u` |
| `JoinSource` | `… JOIN posts p ON p.user_id = u.id` — wraps left + right `QuerySource` and an `ON` predicate |
| `DerivedTableSource` | `FROM (SELECT …) alias` |

`JoinSource` nests left-deep: `A JOIN B JOIN C` becomes `JoinSource(JoinSource(A, B), C)`.

`QueryTicketAdapter.ToQueryTicket` bridges `SelectQuery` → the legacy `QueryTicket` that the single-table
executor consumes. `QueryTicket` carries the same data in a flatter shape understood by the original
operators.

**Stage 2b — Subquery rewriting (`SubqueryRewriter`).** Before binding, the rewriter walks the `WHERE`
predicate and, for **uncorrelated** scalar / `IN` / `NOT IN` subqueries, *executes the inner query once*,
materializes its result, and replaces the subquery AST with a constant or a `SubqueryValueListAst`
membership node. This lets the downstream filterer treat `IN` as a plain value-list check.
`NOT IN` carries three-valued (NULL-aware) semantics. `EXISTS` is left in place for stage 3b.
*Note:* because this stage executes inner subqueries, a query with a subquery does touch storage during
planning — relevant to `EXPLAIN` (see Part III).

## Stage 3 — Binding

**File:** `Controllers/Queries/QueryBinder.cs`

`QueryBinder.BindAsync(database, selectQuery)` resolves names against the catalog and produces a
`BoundSelectQuery`:

```csharp
public sealed class BoundSelectQuery
{
    public SelectQuery Query { get; }
    public IReadOnlyList<BoundTableSource> Sources { get; }          // opened TableDescriptors + alias
    public IReadOnlyList<BoundDerivedTableSource> DerivedSources { get; }
    public QueryRowNameResolver RowNames { get; }
    public bool IsMultiSource { get; }   // true → joins/derived tables → join executor path
}
```

Binding, in order:

1. **Open all table sources** — `TableOpener` opens every `TableSource` in the FROM tree, left to right;
   each opened `TableDescriptor` + alias becomes a `BoundTableSource`.
2. **Detect duplicate aliases** — `CamusDBException(InvalidInput)` on reuse.
3. **Build `QueryRowNameResolver`** — records which alias owns which columns. A column unique across all
   sources may be referenced unqualified; a name in multiple sources requires `alias.column`, else
   "ambiguous column."
4. **Validate projections, GROUP BY, ORDER BY** — every referenced column must resolve.
5. **Projection/grouping consistency** — non-aggregate projected columns in a grouped query must appear
   in `GROUP BY`.
6. **Validate JOIN `ON` predicates** — column references must resolve to the joined sources.

**Stage 3b — EXISTS preparation (`ExistsSubqueryPreparer`).** Walks `WHERE`/`HAVING` for `EXISTS(...)`
nodes and registers an executor per subquery in an `ExistsSubqueryRegistry` threaded into `QueryTicket`.
Uncorrelated `EXISTS` is evaluated once; correlated `EXISTS` is evaluated per outer row by
`QueryFilterer`, with the outer row's qualified columns injected into the subquery scope.

## Stage 4 — Physical Planning

The planner converts a bound query into a `PhysicalPlanNode` tree. There are two planners: single-table
and join.

### 4a. Single-table planner — `QueryPlanner`

**File:** `Controllers/Queries/QueryPlanner.cs`. `QueryPlanner.GetPlan(database, table, ticket)`:

**Phase A — Scan selection**

1. `PredicateAnalyzer.Analyze` classifies the WHERE predicate (§4b).
2. `IndexScanSelector` picks the best scan node (§4c).
3. `PredicateAnalyzer.BuildExecutionFilter` assembles the residual runtime filter (predicates the scan
   did not absorb) into one `NodeAst` AND-tree stored as `QueryPlan.ExecutionFilter`.
4. If the chosen index scan already satisfies `ORDER BY`, the scan node's `OutputOrdering` is set
   and the `SortNode` is **elided** — this is the single source of truth for sort elision.
5. `TryComputeScanRowLimit` pushes `LIMIT` (+ `OFFSET`) into the scan when safe (no filter, no
   aggregation, no GROUP BY/HAVING, no DISTINCT, and ORDER BY satisfied by the scan).

**Phase B — Plan tree construction.** The planner builds a leaf→root operator chain. Order depends on
the query shape:

| Query shape | Operator chain (leaf → root) |
|-------------|------------------------------|
| Plain SELECT | `Scan → [Filter] → [Sort] → [Limit] → [Aggregate] → [HavingFilter] → [Project]` |
| GROUP BY | `Scan → [Filter] → Aggregate → [HavingFilter] → [Sort] → [Project] → [Limit]` |
| SELECT DISTINCT | `Scan → [Filter] → [Aggregate] → [HavingFilter] → [Project] → Distinct → [Sort] → [Limit]` |

(The "Plain SELECT" row applies to *global* aggregates with no `GROUP BY`: limit is applied before the
single-group aggregate.) `FilterNode` is a tree node but never becomes a step — its predicate is the
inline `ExecutionFilter` applied during the scan, avoiding a separate streaming pass.

**Phase C — Post-plan passes**

- `QueryPlanStepAdapter.PopulateLinearSteps(plan)` flattens `Root` into `plan.Steps` (leaf-first) for the
  linear executor. It also fills `plan.StepNodes` with the *same* node instances (used by EXPLAIN ANALYZE
  to attach stats). Filter/join/derived nodes produce no step.
- `ProjectionPushdownPlanner.Apply(plan)` annotates scan nodes with `RequiredColumns` (§Part V) so the
  row decoder only deserializes needed columns.

### Physical plan nodes

All extend `PhysicalPlanNode` (`Models/Plans/PhysicalPlanNode.cs`): a single `Input` child, plus
`RequiredColumns` (projection pushdown) and the distributed-ready properties (Part IV).

| Node | Step type | Meaning |
|------|-----------|---------|
| `TableScanNode(PrimaryRows)` | `FullScanFromTableIndex` | Full scan over primary KV rows |
| `TableScanNode(ForcedIndex)` | `FullScanFromIndex` | Forced index scan (`@{FORCE_INDEX=…}`) |
| `IndexLookupNode` | `QueryFromIndex` | Point lookup on a unique index |
| `IndexRangeScanNode` | `RangeScanFromIndex` | Bounded range scan on an index |
| `FilterNode` | _(inline ExecutionFilter)_ | Residual row predicate applied during scan |
| `AggregateNode` | `Aggregate` | Grouped or global aggregation |
| `HavingFilterNode` | `HavingFilter` | Post-aggregate row filter |
| `SortNode` | `SortBy` | In-memory N-key sort |
| `ProjectNode` | `ReduceToProjections` | Column projection / aliasing |
| `DistinctNode` | `Distinct` | Duplicate elimination over projection tuples |
| `LimitNode` | `Limit` | LIMIT / OFFSET |
| `NestedLoopJoinNode` | _(join path only)_ | Nested-loop inner join |
| `IndexNestedLoopJoinNode` | _(join path only)_ | Index-probed inner join |
| `DerivedTableScanNode` | _(join path only)_ | Scan of a materialized derived table |

### 4b. PredicateAnalyzer

**File:** `Controllers/Queries/PredicateAnalyzer.cs`. Classifies the WHERE predicate into:

1. **Indexable comparisons** — column-vs-constant (`=`, `<`, `>`, `<=`, `>=`, `BETWEEN`). Can drive index
   selection; once absorbed by a scan they are dropped from the execution filter.
2. **Column comparisons** — column-vs-column (join `ON`, cross-table WHERE). Currently always residual.
3. **Residual conjuncts** — everything else (`OR`, `LIKE`, `ILIKE`, non-deterministic, subquery
   predicates). Always re-evaluated per row.

`CollectAndConjuncts` splits an `AND` tree into individual terms first, so `year >= 2001 AND year < 2005`
yields two comparisons that `IndexScanSelector` fuses into one range scan. `BuildExecutionFilter`
reconstructs an AND-tree from the *unabsorbed* comparisons;
`IndexScanBoundAnalysis.IsComparisonAbsorbedByScan` decides which are made redundant by the scan bounds.

### 4c. IndexScanSelector

**File:** `Controllers/Queries/IndexScanSelector.cs`. `TrySelectScan` scores every index and picks the
best (ties → fewer columns = more selective):

| Match type | Score |
|-----------|-------|
| Full equality on all columns, unique index | `10000 + column_count` |
| Full equality on all columns, non-unique index | `5000 + column_count × 10` |
| Equality prefix + range on next column | `5000 + prefix_length × 10 + 1` |
| Equality prefix only | `5000 + prefix_length` |
| ORDER BY prefix match (no predicate) | `1000` |

**Composite matching:** walk index columns left to right, accumulate the equality prefix; if fully
covered, emit `QueryFromIndex` (unique) or a prefix-bounded `RangeScanFromIndex` (non-unique, upper bound
via `BuildPrefixScanUpperBound`). Note **equality on a non-unique index is a single-value range scan**,
not a point lookup — `WHERE year = 2000` on a multi index renders as
`index-range-scan(... from>=2000, to<2001)`, while `WHERE id = …` on the unique PK renders as
`index-lookup`. If a partial prefix is followed by a range predicate on the next column, both bounds are
combined.

**ORDER BY scan elision:** if no predicate matches, `TrySelectOrderByScan` looks for an index whose
leading columns equal the ORDER BY columns (ascending only) and returns an unbounded range scan;
`ScanSatisfiesOrderBy` confirms so the planner omits `SortNode`. Descending order is not satisfiable by
the ascending index encoding and forces a real `SortNode`.

### 4d. Join planner — `JoinQueryPlanner`

**File:** `Controllers/Queries/JoinQueryPlanner.cs`. Runs when `IsMultiSource`:

1. **Join-order heuristics** — `JoinOrderOptimizer.Reorder` may reorder sources first (§Part V).
2. **Predicate pushdown** — `JoinPredicatePushdown.Analyze` splits WHERE into per-source scan filters
   (`ScanFiltersByAlias`) and a cross-source `PostJoinFilter`.
3. **Tree construction** — `BuildJoinTree` recurses the (possibly reordered) `QuerySource`:
   - `TableSource` → `TableScanNode` with its pushed-down filter
   - `DerivedTableSource` → `DerivedTableScanNode` (inner query materialized lazily at execution)
   - `JoinSource` → left built recursively; the right source is checked by `JoinEquiJoinAnalyzer`. If the
     right join key is indexed → `IndexNestedLoopJoinNode`; else `NestedLoopJoinNode`.
4. Same Phase-C passes as the single-table planner.

## Stage 5 — Execution

### 5a. Single-table — `QueryExecutor`

**File:** `Controllers/QueryExecutor.cs`. `ExecuteQueryPlanInternal` walks `plan.Steps`, chaining
`IAsyncEnumerable<QueryResultRow>` through each operator:

- **Scans** (`FullScanFromTableIndex` / `FullScanFromIndex` / `QueryFromIndex` / `RangeScanFromIndex`) →
  `QueryScanner` / `QueryUsingIndex` / `QueryUsingRangeIndex`: read from `KvTableStore`, decode each row
  with `RowEncoder.DecodeAsync(schema, txId, rowId, data, requiredColumns)`, apply the inline filter,
  honor `ScanRowLimit`.
- **`SortBy`** → `QuerySorter` — materializes, sorts by an N-key comparator over the actual ORDER BY
  columns, streams.
- **`Aggregate`** → `QueryAggregator` — groups by the GROUP BY key (or one global group), accumulates
  `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, emits one row per group.
- **`HavingFilter`** → `QueryFilterer.FilterHavingResultset` — evaluates HAVING against aggregated rows
  in the `QueryHavingWorkspace` scope (so `HAVING x > 0` resolves an aggregate alias `x`).
- **`ReduceToProjections`** → `QueryProjector`.
- **`Distinct`** → `QueryDistincter` — dedups output tuples; NULLs compare equal.
- **`Limit`** → `QueryLimiter` — skips OFFSET, stops after LIMIT.

### 5b. Join — `QueryJoinExecutor`

**File:** `Controllers/Queries/QueryJoinExecutor.cs`. `ExecuteJoinQuery` drives `ExecuteJoinTree(Root)`,
applies the `PostJoinFilter`, then hands the cursor to `QueryPostScanPipeline.Apply`.
`ExecuteJoinTree` matches node type:

- `TableScanNode` → full scan with per-alias inline filter.
- `DerivedTableScanNode` → materializes the inner query once, caches in `plan.DerivedMaterializations`.
- `NestedLoopJoinNode` → for each left row, scan the full right source, merge, evaluate `ON`.
- `IndexNestedLoopJoinNode` → for each left row, probe the right index (unique → point lookup;
  non-unique → equality-prefix scan).

Row merging (`QueryRowMerger`): right columns are stored as `alias.column` and also unqualified when
there is no collision; **merged rows carry `RowId = default`** — there is no single source row id.

### 5c. Post-scan pipeline — `QueryPostScanPipeline`

**File:** `Controllers/Queries/QueryPostScanPipeline.cs`. `Apply` reproduces the planner's operator order
for the join path so both executors agree:

| GROUP BY | DISTINCT | Order |
|----------|----------|-------|
| yes | — | scan → where → aggregate → having → sort → project → limit |
| no | yes | scan → where → [aggregate → having] → project → distinct → sort → limit |
| no | no | scan → where → sort → limit → [aggregate → having] → project |

---

# Part III — Inspecting plans: PlanRenderer & EXPLAIN

Being able to *see* a plan is essential for debugging the planner. CamusDB has an internal renderer and a
user-facing `EXPLAIN`. (User-facing reference: [`docs/explain.md`](./explain.md).)

## PlanRenderer

**File:** `Controllers/Queries/PlanRenderer.cs`. Walks a `PhysicalPlanNode` tree and produces a
deterministic, indented, multi-line string with one **canonical node name** per node — these names are
the stable vocabulary reused everywhere:

```
table-scan, index-lookup, index-range-scan, filter, having-filter, aggregate, sort,
limit, project, distinct, nested-loop-join, index-nested-loop-join, derived-table-scan
```

`PlanRenderer.Render(plan, includeRequiredColumns, includeDistributedProperties)` produces the string
form; `PlanRenderer.WalkNodes(root, plan)` yields `(name, detail)` pairs in depth-first, parent-before-
child order. Both share one `GetRenderLine` so the string form and the EXPLAIN rows never diverge. The
verbose `includeDistributedProperties` flag appends distributed-ready metadata (`order=[…]`, `decomposable=…`).

## EXPLAIN

**Files:** grammar `explain_stmt`, `Controllers/Queries/ExplainExecutor.cs`, wired in
`CommandExecutor.ExecuteSQLQuery`.

`EXPLAIN [(LOGICAL|PHYSICAL)] SELECT …` runs **parse → bind → plan but not execution**, then returns one
result row per plan node with columns: `stage`, `node`, `detail`, `estimated_rows`
(`PhysicalPlanNode.EstimatedCardinality` from the cost model), and `estimated_cost`
(`PhysicalPlanNode.Cost.Total`). These are now populated for single-table plans; for join plans they are
rough placeholders (see the cost-model caveats in Part V). `(LOGICAL)` and `(PHYSICAL)` currently render the same physical tree
(there is no separate logical-plan representation yet); they differ only in the `stage` label.
An unknown option word (e.g. `EXPLAIN (VERBOSE)`) is rejected with `InvalidInput` rather than silently
treated as a plain explain.

*Caveat:* because stage 2b/3b execute uncorrelated subqueries during planning, `EXPLAIN` of a query that
contains a subquery does read storage for the inner query. The outer query is never executed.

## EXPLAIN ANALYZE

`EXPLAIN (ANALYZE) SELECT …` actually executes the query, drains the cursor, and adds **actual** runtime
counters per node. Counters live in `PlanNodeStats` (`Models/Plans/PlanNodeStats.cs`):
`RowsRead`, `RowsEmitted`, `KvPointLookups`, `KvScanEntries`, and `ElapsedMs` (root-only).
Extra result columns: `actual_rows`, `rows_read`, `actual_time_ms` (NULL on non-root nodes),
`kv_lookups`, `kv_scan_entries`.

Design points worth knowing:

- **Gated and zero-cost when off.** Counters are only allocated/updated when `QueryPlan.CollectRuntimeStats`
  is set (only EXPLAIN ANALYZE sets it). Normal `SELECT` pays nothing.
- **Filter rows are folded into the scan.** A `filter` row reports `actual_rows` from the child scan's
  post-filter emit count; its KV counters are 0 (storage cost is charged to the scan).
- **Joins are not yet supported under ANALYZE** — it throws `InvalidInput`, because the linear executor it
  uses emits no step for join nodes. Use plain `EXPLAIN` for joins. (Join instrumentation is future work.)

---

# Part IV — Distributed-ready plan properties

Even though execution is single-process today, plan nodes carry optional metadata so a future
distributed executor (CockroachDB-style "do local work near data, merge at a coordinator") is not
blocked. On `PhysicalPlanNode`:

| Property | Meaning | Status today |
|----------|---------|--------------|
| `OutputOrdering` | The ordering this node guarantees on its output | Set on index scans that satisfy ORDER BY and on `SortNode`; drives sort elision |
| `EstimatedCardinality` | Estimated output rows | Populated by the `CostEstimator` (single-table accurate; join estimates are placeholders) |
| `Cost` (`PlanCost?`) | Weighted cost estimate for the node | Populated by the `CostEstimator`; `null` if the plan was not costed |
| `PartitionLocality` | Partition affinity hint | `null` (single-partition placeholder) |
| `CanDecomposeToLocalPlusMerge` | Whether the operator splits into per-partition local work + a merge | `true` for scan/filter/project; `AggregateNode` only for `COUNT`/`SUM`/`MIN`/`MAX` (not `AVG`); `false` for sort/limit/distinct/join |

These are populated only by the single-table `QueryPlanner` today; join plans leave them at defaults.
They are descriptive metadata — no execution behavior depends on them yet except sort elision (which uses
`OutputOrdering`).

---

# Part V — Optimization passes

| Pass | Where | What it does |
|------|-------|--------------|
| **Sort elision** | `IndexScanSelector` + `QueryPlanner` | If an index scan emits rows already in ORDER BY order, set `OutputOrdering` and skip `SortNode`. |
| **Projection pushdown** | `ProjectionPushdownPlanner` + `RequiredColumnAnalyzer` | Compute the set of columns any operator needs and store it on scan nodes as `RequiredColumns`; `RowEncoder.DecodeAsync` then skips unreferenced columns. `null` = all columns (`SELECT *`). |
| **Limit pushdown** | `QueryPlanner.TryComputeScanRowLimit` | Push `LIMIT`+`OFFSET` into the scan (`ScanRowLimit`) when no filter/aggregation/GROUP BY/HAVING/DISTINCT and ORDER BY is scan-satisfied. Scans stop reading early. |
| **Filter absorption** | `IndexScanBoundAnalysis` | Drop residual comparisons already implied by the scan's key bounds (e.g. `col >= 3` when the scan seeks from `5`). |
| **Join predicate pushdown** | `JoinPredicatePushdown` | Single-source WHERE predicates run during that table's scan; cross-source predicates become the post-join filter. |
| **Join-order heuristics** | `JoinOrderOptimizer` | Reorder inner-join sources before tree construction. |
| **Cost-based scan choice** | `CostEstimator` | Annotate every node with `EstimatedCardinality` + `PlanCost`; replace a predicate-driven index range scan with a full table scan when it would touch too much of the table. Range selectivity uses real per-column min/max when available, else fixed defaults. |
| **Semi-/anti-join rewrite** | `SemiJoinAnalyzer` + `SemiJoinExecutor` | Rewrite eligible uncorrelated `IN`/`NOT IN` into an index-probing semi/anti/null-aware-anti join (instead of materializing), but only when the inner column is indexed; otherwise fall back to materialization. |
| **DISTINCT streaming** | `QueryDistincter` + `IndexScanSelector` | When the `DISTINCT` columns form an index set-prefix and are all NOT NULL, scan in index order and dedup by comparing adjacent rows (O(1) memory) instead of a hash set. |

## Cost model in detail

**Files:** `Models/Plans/PlanCost.cs`, `Controllers/Queries/CostEstimator.cs`. This is CamusDB's first
(deliberately small) cost model. It does two things:

1. **Annotates the plan.** `CostEstimator.AnnotatePlan(root, db, table, stats)` walks the tree bottom-up
   and assigns each node an `EstimatedCardinality` and a `PlanCost`. Row counts come from the table statistics
   (`StatisticsManager.GetRowCountEstimate`); when stats are absent it degrades to a fixed
   `DefaultTableRowCount` (10 000) and fixed selectivity constants (range with both bounds → 10 %, one
   bound → 40 %, filter → 10 %, group-by → 20 %, distinct → 70 %). `PlanCost.Total` is a weighted sum of
   KV point lookups, range-scan entries, post-index row fetches (all weight 1.0) and in-memory rows
   (0.1); `NetworkFactor` is reserved for sharding and always 0. These annotations feed `EXPLAIN`'s
   `estimated_rows` / `estimated_cost`.
2. **Vetoes one plan choice.** In `QueryPlanner`, after `IndexScanSelector` picks a predicate-driven
   `RangeScanFromIndex`, `CostEstimator.ShouldPreferFullScan` replaces it with a full table scan when the
   estimated index entries reach the breakeven fraction (40 %) of the table. It only fires when row-count
   stats exist, never touches unique point lookups, and skips ORDER-BY-driven unbounded scans (so sort
   elision is preserved). Results are always identical — only the access path (and speed) changes.

**Important limitations to know before relying on it:**

- **One-bound range selectivity now uses real min/max when available.** `EstimateRangeScanRows`
  reads the column's persisted min/max (`StatisticsManager.GetColumnMinMax`) to compute actual range
  selectivity, so a genuinely selective bound (`id > <near-max>`) keeps its index while a non-selective one
  (`year > <near-min>`) flips to full scan. The fixed-40 % assumption is now only the **fallback** when
  min/max is unavailable (no stats yet, or non-numeric columns — strings/ids fall back to fixed).
- **Join costs are placeholders.** `JoinQueryPlanner` calls `AnnotatePlan` with `table: null`, so every
  source in a join is costed with the default 10 000 row count — `EstimatedCardinality`/`estimated_cost`
  for join plans are not meaningful yet. Join-order and algorithm choice do not consume the cost
  model; they remain heuristic.
- **The cost model is advisory and single-decision.** It computes costs for all nodes but only the
  range-scan-vs-full-scan choice consumes them. Unique-lookup-vs-scan and nested-loop-vs-index-nested-loop
  remain rule-based.

## Join-order heuristics in detail

**File:** `Controllers/Queries/JoinOrderOptimizer.cs`. A deterministic, rule-based reorder applied to
**inner joins only** (it bails to the declared order if any `JoinSource.Kind != Inner`, since outer joins
are not commutative). It flattens the join tree into leaves plus a pool of `ON` predicates, scores each
leaf, and rebuilds a left-deep tree:

| Score | Source has… | Effect |
|-------|-------------|--------|
| 0 | equality predicate on a **unique-indexed** column (point lookup, ≤1 row) | placed outermost (drives the loop fewest times) |
| 1 | any predicate on an indexed column | next |
| 2 | no pushable predicate | last |

A stable secondary sort on declared position keeps results deterministic. Two safety properties: a
**feasibility guard** ensures every rebuilt join edge still has a connecting predicate (a chain join with
no connecting predicate after reordering falls back to the declared order), and reordering is semantics-
preserving because all `ON` predicates are kept and re-applied where their referenced aliases are in
scope. Scoring is by *scan selectivity*, not by which side holds the indexed join key — INL-join
placement is left to `JoinEquiJoinAnalyzer` and the declared order; a future cost model can unify
the two.

---

# Part VI — Current capabilities & roadmap

## What works today

Parse, bind, plan, and execute for: single-table SELECT with index selection (unique/composite/range),
`WHERE` with predicate analysis and filter absorption, `GROUP BY` + `HAVING`, global and grouped
aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), `SELECT DISTINCT` (hash or index-ordered streaming),
`ORDER BY` (with index-based sort elision), `LIMIT`/`OFFSET` (with pushdown), `[INNER]`/comma joins
(nested-loop and index-nested-loop), derived tables, scalar/`IN`/`NOT IN`/`EXISTS` subqueries with
semi/anti-join rewrite for indexed `IN`/`NOT IN`, projection pushdown, join-order heuristics, advisory
statistics (row counts, per-index counts, per-column min/max — persisted, configurable flush cadence), a
small min/max-driven cost model that vetoes low-selectivity index range scans, an error/semantics matrix,
and full plan inspection via `EXPLAIN` / `EXPLAIN ANALYZE`.

## Gaps and where to contribute

These are the meaningful missing pieces and where new work fits.

| Area | State | Notes |
|------|-------|-------|
| **Table statistics — row counts** | **Done** | `StatisticsManager` tracks/persists `RowCount` per table (Kahuna meta KV `{db}:stats:{tableId}`), with a configurable flush cadence (`stats_flush_interval_ms`) and a close-hook flush. |
| **Index counts & column min/max** | **Done** | `StatisticsManager` tracks/persists per-index entry counts and per-column min/max (`ColumnMinMax`/`ScalarBound`), respecting index element-state. Consumed by the cost model for real range selectivity. |
| **Cost model** | **Done (small)** | `PlanCost` + `CostEstimator` populate `EstimatedCardinality`/`Cost` and veto low-selectivity index range scans. Range selectivity is min/max-driven; join costs are still rough; only the range-vs-fullscan decision is cost-driven. |
| **Plan-cache hooks** | **Partial** | Plans record `TableSchemaVersion`; no stable query-shape identifier yet. No plan reuse. |
| **Semi-/anti-join rewrite** | **Done** | Eligible uncorrelated `IN`/`NOT IN` over an **indexed** inner column rewrite to semi / anti / null-aware-anti join (`SemiJoinAnalyzer`/`SemiJoinExecutor`); non-indexed falls back to materialization. Three-valued `NOT IN` semantics preserved. |
| **DISTINCT streaming** | **Done** | `SELECT DISTINCT` over a NOT-NULL index set-prefix streams (adjacent-row dedup, O(1) memory); otherwise hash dedup. |
| **EXPLAIN ANALYZE for joins** | **Missing** | Throws today; needs the join executor instrumented with `PlanNodeStats`. |
| **Logical EXPLAIN** | **Cosmetic** | `(LOGICAL)` renders the physical tree relabeled; there is no distinct logical-plan rendering. |
| **Per-node timing** | **Root-only** | `actual_time_ms` is only measured for the whole plan, not per operator. |
| **Error/semantics matrix** | **Done** | `TestErrorMatrix.cs` — 14 negative cases (ambiguous column, bad HAVING refs, multi-column/multi-row subqueries, `COUNT(DISTINCT)`, etc.) asserting precise codes + messages. |
| **Query microbenchmarks** | **Missing** | No benchmarks for grouped aggregation, join algorithms, or subquery materialization. The one remaining roadmap item. |

Explicitly deferred (by design): OUTER joins, window functions, CTEs, quantified predicates beyond
`IN`/`NOT IN` (`ANY`/`ALL`/`SOME`), `COUNT(DISTINCT …)`, full cost-based join reordering, and distributed
execution.

---

# Part VII — File map for maintainers

| Concern | File(s) |
|---------|---------|
| SQL grammar and lexer | `SQLParser/SQLParser.Language.grammar.y`, `SQLParser.Language.analyzer.lex` |
| AST node types | `SQLParser/NodeAst.cs`, `NodeType.cs` |
| Logical query model | `Commands/Executor/Models/Queries/` |
| Logical model builder | `Commands/Executor/Controllers/DML/SelectQueryCreator.cs` |
| Legacy query ticket bridge | `Commands/Executor/Controllers/DML/QueryTicketAdapter.cs` |
| Binder | `Commands/Executor/Controllers/Queries/QueryBinder.cs` |
| Subquery rewriting / execution | `SubqueryRewriter.cs`, `SubqueryQueryExecutor.cs`, `ScalarSubqueryExecutor.cs`, `InSubqueryExecutor.cs` |
| EXISTS preparation / execution | `ExistsSubqueryPreparer.cs`, `ExistsSubqueryExecutor.cs` |
| Physical plan nodes | `Commands/Executor/Models/Plans/` |
| Plan node runtime stats | `Commands/Executor/Models/Plans/PlanNodeStats.cs` |
| Cost model | `Commands/Executor/Models/Plans/PlanCost.cs`, `Controllers/Queries/CostEstimator.cs` |
| Single-table planner | `Commands/Executor/Controllers/Queries/QueryPlanner.cs` |
| Join planner | `Commands/Executor/Controllers/Queries/JoinQueryPlanner.cs` |
| Join-order heuristics | `Commands/Executor/Controllers/Queries/JoinOrderOptimizer.cs` |
| Predicate classification | `Commands/Executor/Controllers/Queries/PredicateAnalyzer.cs` |
| Index scan selection / bound absorption | `IndexScanSelector.cs`, `IndexScanBoundAnalysis.cs` |
| Join predicate pushdown / equi-join analysis | `JoinPredicatePushdown.cs`, `JoinEquiJoinAnalyzer.cs` |
| Plan tree → linear steps | `Commands/Executor/Controllers/Queries/QueryPlanStepAdapter.cs` |
| Projection pushdown | `ProjectionPushdownPlanner.cs`, `RequiredColumnAnalyzer.cs` |
| Plan rendering / EXPLAIN | `PlanRenderer.cs`, `ExplainExecutor.cs` |
| Post-scan pipeline | `Commands/Executor/Controllers/Queries/QueryPostScanPipeline.cs` |
| Single-table executor | `Commands/Executor/Controllers/QueryExecutor.cs` |
| Join / derived executor | `QueryJoinExecutor.cs`, `DerivedTableExecutor.cs` |
| Scan / filter / sort / aggregate / project / distinct / having / limit | `QueryScanner.cs`, `QueryFilterer.cs`, `QuerySorter.cs`, `QueryAggregator.cs`, `QueryProjector.cs`, `QueryDistincter.cs`, `QueryHavingEvaluator.cs`, `QueryLimiter.cs` |
| Row merge for joins | `Commands/Executor/Controllers/Queries/QueryRowMerger.cs` |
| Expression evaluator | `Commands/Executor/Controllers/SqlExecutor.cs` |
| Table statistics (row counts) | `Statistics/StatisticsManager.cs`, `Statistics/Models/TableStatistics.cs`; flush cadence `CamusDBConfig.StatsFlushIntervalMs` / `stats_flush_interval_ms` |
| KV table access | `Storage/Kv/KvTableStore.cs` |
| Row encoding / decoding | `CommandsExecutor/Models/RowEncoder.cs` |
| Query plan model | `Commands/Executor/Models/QueryPlan.cs`, `QueryPlanStep.cs`, `QueryPlanStepType.cs` |
| Planner / EXPLAIN tests | `TestQueryPlanner.cs`, `TestPredicateAnalyzer.cs`, `TestJoinQueryPlanner.cs`, `TestPlanRenderer.cs`, `TestPlanDistributedProperties.cs`, `TestExplainExecutor.cs`, `TestExplainAnalyzeExecutor.cs`, `TestCostEstimator.cs`, `TestStatisticsManager.cs` |
| Integration tests | `CamusDB.Tests/CommandsExecutor/TestExecuteSqlSelect.cs` |

---

# Part VIII — Adding a new SQL feature — checklist

1. **Lexer / grammar** — add tokens in `analyzer.lex`, extend rules in `grammar.y`, rebuild to regenerate
   the `Generated.cs` files, and stage them.
2. **AST → logical model** — update `SelectQueryCreator` (and claim a `NodeAst` slot if a new clause needs
   one) to populate `SelectQuery` or a new `Models/Queries/` type.
3. **`QueryTicketAdapter`** — carry the new field through if the single-table path needs it.
4. **Binder** — add scope/ambiguity/type validation in `QueryBinder`.
5. **Planner** — add a `PhysicalPlanNode` if needed; insert it at the correct point in `QueryPlanner` and
   `JoinQueryPlanner`. Set the distributed-ready properties (`CanDecomposeToLocalPlusMerge`, `OutputOrdering`) where they
   apply.
6. **`QueryPlanStepAdapter`** — add the node's `Flatten` case (emit a step, or skip like Filter/joins).
7. **Executor** — handle the new step in `QueryExecutor` **and** the corresponding stage in
   `QueryPostScanPipeline` so single-table and join paths agree.
8. **Operator** — implement it as an `IAsyncEnumerable<QueryResultRow>` transformer (follow
   `QuerySorter`/`QueryAggregator`). Stream unless semantics force materialization.
9. **Renderer** — add a canonical node name + detail in `PlanRenderer` so `EXPLAIN` shows it; if it has
   runtime cost, wire `PlanNodeStats` for EXPLAIN ANALYZE.
10. **Tests** — parser tests, planner tests asserting plan shape (via `PlanRenderer`), and execution
    tests with exact expected rows. Run the parser, planner, and `TestExecuteSqlSelect` suites.

---

# Part IX — Glossary

- **AST (`NodeAst`)** — the raw syntax tree from the parser; expressions stay in this form through
  execution.
- **Logical plan (`SelectQuery` / `BoundSelectQuery`)** — *what* result is wanted, names resolved.
- **Physical plan (`QueryPlan.Root`)** — *how* to compute it: a tree of `PhysicalPlanNode`.
- **Binding** — resolving column/table names against the catalog and opening table descriptors.
- **Predicate** — a boolean expression (`WHERE`/`ON`/`HAVING`). *Residual* = must be evaluated per row.
- **Pushdown** — moving work (filters, projections, limits) closer to the scan to do less.
- **Sort elision** — skipping a sort because an index already yields rows in the requested order.
- **Nested-loop join / index-nested-loop join** — for each left row, scan the right source / probe the
  right index.
- **Heuristic vs cost-based** — rule-driven plan choice vs statistics-driven cost comparison. CamusDB is
  mostly heuristic, with a small cost model that vetoes one index-vs-scan choice.
- **`PlanCost` / cost model** — per-node estimated cardinality + weighted I/O cost (`CostEstimator`), fed
  by row-count stats; surfaced in `EXPLAIN` and used to prefer a full scan over a low-selectivity index
  range scan.
- **Streaming (`IAsyncEnumerable`)** — pulling rows one at a time without materializing the whole result.
- **SQL-over-KV** — the architectural boundary where relational operations become Kahuna key/value reads.

For `EXPLAIN` output as a user feature, see [`docs/explain.md`](./explain.md).
