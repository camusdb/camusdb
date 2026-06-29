# Query Planner — Concepts & Developer Guide

This document explains how CamusDB turns a SQL `SELECT` string into result rows. It is written to be
read top to bottom by someone new to the project: **Part I** builds the mental model and vocabulary,
**Part II** walks the pipeline stage by stage, **Parts III–IV** cover plan inspection and
distributed-ready metadata, **Part V** is the cost-based optimizer (statistics → cardinality estimation →
cost model → cost-based access-path and join-order search), and **Part VI** is an honest map of what is
*not* built yet so you know where to contribute. Parts VII–IX are reference material (file map, extension
checklist, glossary).

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

**CamusDB has a heuristic planner with a real cost-based optimizer layered on top, gated behind
config flags.** The project followed the "heuristics before CBO" path, and the heuristic planner is
still the default. But a full cost-based optimizer now exists and, when enabled, drives the two biggest
plan decisions — **which access path** to use per table and **what order** to join tables — by costing
alternatives against statistics rather than applying rules.

The mental model has three layers:

1. **Always on:** the cost model annotates every node with an estimated cardinality and a `PlanCost`,
   and a handful of decisions are cost-driven regardless of flags — the range-scan-vs-full-scan veto,
   join-algorithm choice (INLJ vs hash vs merge), and the unique-`IN` vs range-scan comparison.
2. **`cost_based_access_path_enabled` (default off):** the planner enumerates *all* viable index access
   paths per table and picks the cheapest by cost, instead of the rule-scored "first viable index" pick.
3. **`cost_based_join_order_enabled` (default off):** a System-R-style dynamic program enumerates join
   orders and picks the cheapest left-deep tree, instead of the scan-selectivity heuristic.

Both flags degrade to the exact heuristic path when off (or when statistics are absent), so plans are
byte-identical to the rule-based planner until you opt in. The cost model is fed by a full statistics
stack — equi-depth histograms, distinct-value counts (NDV), per-column min/max — built by `ANALYZE`,
plus a network-cost dimension for sharded deployments. See
[Part V — The cost-based optimizer](#part-v--the-cost-based-optimizer) for the whole stack and
[Part VI](#part-vi--current-capabilities--roadmap) for what remains.

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
| `DistinctNode` | `Distinct` | Duplicate elimination over projection tuples (hash, or `IsStreaming` adjacent-row dedup) |
| `SemiJoinNode` | `SemiJoinProbe` | Index-probing semi / anti / null-aware-anti join from an `IN`/`NOT IN` rewrite |
| `IndexInListScanNode` | `InListScanFromIndex` | One index seek per value for `x IN (v1, v2, …)`, unioned and row-id-deduped |
| `LimitNode` | `Limit` | LIMIT / OFFSET |
| `NestedLoopJoinNode` | _(join path only)_ | Nested-loop inner join |
| `IndexNestedLoopJoinNode` | _(join path only)_ | Index-probed inner join |
| `HashJoinNode` | _(join path only)_ | In-memory hash-table inner equi-join (builds the smaller side) |
| `MergeJoinNode` | _(join path only)_ | Streaming two-pointer inner equi-join over index-ordered sides |
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
covered, emit `QueryFromIndex` (unique) or a prefix-bounded `RangeScanFromIndex` (non-unique). Note
**equality on a non-unique index is a single-value range scan**, not a point lookup — `WHERE year = 2000`
on a multi index renders as `index-range-scan(... from>=2000, to<2001)`, while `WHERE id = …` on the
unique PK renders as `index-lookup`. If a partial prefix is followed by a range predicate on the next
column, both bounds are combined.

**String/Id equality on a non-unique index** has no computable "next value" for an exclusive upper bound,
so it uses an **inclusive `[v, v]`** range instead — `ScanIndex` appends a high (U+FFFF) sentinel so all
`encode(v) + rowId` entries are captured, then the decoded-key bounds filter trims to `key == v` (the same
mechanism the value-list `IN` path uses). This covers all three shapes — full equality, an equality prefix
only, and an equality prefix before a trailing range. For a string-prefix + a *half-open* trailing range
(`name = 'a' AND year > 2000`), the open side is capped at the inclusive prefix so the scan stays within
the prefix rather than running to the end of the index. (Before this, non-unique `String`/`Id` equality
fell back to a full table scan.)

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
   - `JoinSource` → left built recursively; the right source is checked by `JoinEquiJoinAnalyzer`
     and `JoinQueryPlanner` for algorithm selection:
     - Right join key has a single-column index → `IndexNestedLoopJoinNode` (INLJ); unless…
     - Both sides have free index ordering (secondary index prefix covers the join key) **and** the
       outer side is large (> 100 rows estimated) → `MergeJoinNode` (streaming merge join); or…
     - Equi-join with stats available and hash preferred over INLJ → `HashJoinNode` (build =
       smaller estimated side); else…
     - No suitable right index → `HashJoinNode` for equi-joins, `NestedLoopJoinNode` otherwise.
4. Same Phase-C passes as the single-table planner.

## Stage 5 — Execution

### 5a. Single-table — `QueryExecutor`

**File:** `Controllers/QueryExecutor.cs`. `ExecuteQueryPlanInternal` walks `plan.Steps`, chaining
`IAsyncEnumerable<QueryResultRow>` through each operator:

- **Scans** (`FullScanFromTableIndex` / `FullScanFromIndex` / `QueryFromIndex` / `RangeScanFromIndex`) →
  `QueryScanner` / `QueryUsingIndex` / `QueryUsingRangeIndex`: read from `KvTableStore`, decode each row
  with `RowEncoder.DecodeAsync(schema, txId, rowId, data, requiredColumns)`, apply the inline filter,
  honor `ScanRowLimit`.
- **`InListScanFromIndex`** → `QueryUsingInListIndex` — one index seek per `IN`-list value (point lookup or
  equality range), unioned with row-id dedup, residual predicate re-applied per emitted row.
- **`SortBy`** → `QuerySorter` — materializes, sorts by an N-key comparator over the actual ORDER BY
  columns, streams. Resolves an alias-qualified order column (`u.position`) against the bare row key when
  the row isn't keyed by the qualified name (single-table scans).
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
- `HashJoinNode` → materialise the build side into an in-memory hash table keyed on the equi-join
  columns; stream the probe side and look up each row. Falls back to nested-loop if the build
  side exceeds `HashJoinMaxBuildRows`. Build side is the smaller estimated input.
- `MergeJoinNode` → when both sides have free index ordering (ForcedIndex scan or upstream
  SortNode), advance two enumerators in lockstep; buffer only the current equal-key run on the
  right side (O(run size) memory). When only one or neither side is pre-ordered, the unordered
  side(s) are materialised and sorted first.

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
table-scan, index-lookup, index-range-scan, index-in-list, filter, having-filter, aggregate, sort,
limit, project, distinct, semi-join, anti-join, null-aware-anti-join, nested-loop-join,
index-nested-loop-join, hash-join, merge-join, derived-table-scan
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
(`PhysicalPlanNode.Cost.Total`). These are populated for single-table plans, and for join plans too when
the cost-based join-order flag is on (otherwise join-node estimates remain heuristic; see Part V).
`(LOGICAL)` and `(PHYSICAL)` currently render the same physical tree
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
| `EstimatedCardinality` | Estimated output rows | Populated by the `CostEstimator` (single-table accurate; join estimates real when the cost flags are on) |
| `Cost` (`PlanCost?`) | Weighted cost estimate for the node | Populated by the `CostEstimator`; `null` if the plan was not costed. Carries a real `NetworkFactor` (see below) |
| `Distribution` (`DataDistribution?`) | How the node's output rows are spread across the cluster | Set on scan leaves: `Gathered` (single-node/unsharded), `Partitioned(keyColumns)` (range-sharded by these columns), or `Replicated`. `null` on non-leaf nodes |
| `CanDecomposeToLocalPlusMerge` | Whether the operator splits into per-partition local work + a merge | `true` for scan/filter/project; `AggregateNode` only for `COUNT`/`SUM`/`MIN`/`MAX` (not `AVG`); `false` for sort/limit/distinct/all join operators |

`OutputOrdering` is set by the single-table `QueryPlanner` on index scans and sort nodes, and by
`JoinQueryPlanner` on `MergeJoinNode` (ascending on the left key columns — a downstream `ORDER BY`
on those columns can be elided). `HashJoinNode` leaves `OutputOrdering` null (hash does not preserve order).
All join nodes set `CanDecomposeToLocalPlusMerge = false`.

**`Distribution` is a real physical property, not a stub.** It replaces the old `PartitionLocality`
placeholder. `PlacementReader` derives it from the table/index sharding scheme: when
`KeyRangeShardingEnabled` is on, a primary-row scan reports `Partitioned(pkColumns)` and a
secondary-index scan reports `Partitioned(indexColumns)`; a point lookup is always `Gathered` (it
returns ≤1 row, pulled to the coordinator). With sharding off, everything is `Gathered`. It is a
schema-derived snapshot — it encodes *which columns* partition the data, not the live Kahuna range map
(partition count, which node is remote). The network **cost** that consumes it lives in
`PlanCost.NetworkFactor` (see [Part V — Network & distribution cost](#network--distribution-cost)).
Distribution and `NetworkFactor` are the only distributed-execution scaffolding wired today; actual
remote operator execution is future work.

---

# Part V — The cost-based optimizer

CamusDB's optimizer is built as a stack: **statistics** feed a **cardinality estimator**, which feeds a
**cost model**, which drives **plan choices**. Each layer is usable on its own and degrades gracefully
when the layer below it has no data. This part documents the whole stack, then the individual
optimization passes.

```
                          ┌─────────────────────────────────────────────────────────┐
   ANALYZE <table>  ───►  │  StatisticsManager   (persisted to Kahuna {db}:stats:id) │
                          │   • RowCount / per-index entry counts                    │
                          │   • per-column min/max         (ColumnMinMax)            │
                          │   • equi-depth histograms      (ColumnHistogram)         │
                          │   • distinct-value counts      (ColumnNdv / KeyNdv)      │
                          └───────────────────────────────┬─────────────────────────┘
                                                          │  hints (advisory, never throw)
                                                          ▼
                          ┌─────────────────────────────────────────────────────────┐
                          │  CardinalityEstimator                                    │
                          │   • filter selectivity  (histogram / NDV / IN-list)      │
                          │   • join cardinality    |A⋈B| ≈ |A||B| / max(NDV)        │
                          │   • FK-aware ≤1-match, composite-key correlation         │
                          └───────────────────────────────┬─────────────────────────┘
                                                          │  estimated rows
                                                          ▼
                          ┌─────────────────────────────────────────────────────────┐
                          │  CostEstimator → PlanCost.Total                          │
                          │   = kvLookups + rangeEntries + rowFetches                │
                          │     + 0.1·inMemoryRows + NetworkFactor                   │
                          │   NetworkFactor = remoteRows · rowWidth · NetWeight      │
                          └───────────────┬─────────────────────────┬───────────────┘
                                          │                         │
                       always-on veto     │   flag: access-path     │   flag: join-order
                                          ▼                         ▼
                   range-scan-vs-full-scan,     pick cheapest        System-R DP over
                   join-algorithm (INLJ/         index/scan per       table subsets
                   hash/merge)                   table                (JoinEnumerator)
```

Everything in the bottom row consumes the cost. The two flagged choices (`cost_based_access_path_enabled`,
`cost_based_join_order_enabled`) are off by default and fall back to the heuristic when off or when stats
are missing, so the optimizer is strictly additive.

## Optimization passes

| Pass | Where | What it does |
|------|-------|--------------|
| **Sort elision** | `IndexScanSelector` + `QueryPlanner` | If an index scan emits rows already in ORDER BY order, set `OutputOrdering` and skip `SortNode`. |
| **Projection pushdown** | `ProjectionPushdownPlanner` + `RequiredColumnAnalyzer` | Compute the set of columns any operator needs and store it on scan nodes as `RequiredColumns`; `RowEncoder.DecodeAsync` then skips unreferenced columns. `null` = all columns (`SELECT *`). |
| **Limit pushdown** | `QueryPlanner.TryComputeScanRowLimit` | Push `LIMIT`+`OFFSET` into the scan (`ScanRowLimit`) when no filter/aggregation/GROUP BY/HAVING/DISTINCT and ORDER BY is scan-satisfied. Scans stop reading early. |
| **Filter absorption** | `IndexScanBoundAnalysis` | Drop residual comparisons already implied by the scan's key bounds (e.g. `col >= 3` when the scan seeks from `5`). |
| **Join predicate pushdown** | `JoinPredicatePushdown` | Single-source WHERE predicates run during that table's scan; cross-source predicates become the post-join filter. |
| **Join-order heuristics** | `JoinOrderOptimizer` | Reorder inner-join sources before tree construction (scan-selectivity scoring, left-deep). The default when the join-order flag is off. |
| **Cost-based scan veto** | `CostEstimator.ShouldPreferFullScan` | Always-on: replace a predicate-driven index range scan with a full table scan when it would touch too much of the table. Selectivity is histogram → min/max → fixed-constant. |
| **Cost-based access-path selection** | `IndexScanSelector.EnumerateViableSteps` + `QueryPlanner.PickCheapestIndexOrFullScan` | Flag `cost_based_access_path_enabled`: enumerate *every* viable index for the predicate plus the full-scan baseline, cost each, pick the cheapest — subsuming the rule-scored "first viable index" pick and the veto above. |
| **Cost-based join-order enumeration** | `JoinEnumerator` (System-R DP) | Flag `cost_based_join_order_enabled`: bottom-up dynamic program over table subsets that memoizes the cheapest left-deep sub-plan, replacing the scan-selectivity heuristic. Falls back to the heuristic for outer joins or >12 tables. |
| **Semi-/anti-join rewrite** | `SemiJoinAnalyzer` + `SemiJoinExecutor` | Rewrite eligible uncorrelated `IN`/`NOT IN` into an index-probing semi/anti/null-aware-anti join (instead of materializing), but only when the inner column is indexed; otherwise fall back to materialization. |
| **DISTINCT streaming** | `QueryDistincter` + `IndexScanSelector` | When the `DISTINCT` columns form an index set-prefix and are all NOT NULL, scan in index order and dedup by comparing adjacent rows (O(1) memory) instead of a hash set. |
| **Value-list `IN` → index seeks** | `PredicateAnalyzer` + `QueryPlanner` + `IndexInListScanNode` | Turn `x IN (v1, v2, …)` on an indexed column into one index seek per value (point lookup for a unique index, equality range for a non-unique one), unioned and row-id-deduped, instead of a full scan + residual membership filter. Cost-gated, and cost-compared against a competing range scan so a selective unique `IN` wins. Falls back to a residual filter when the column is unindexed or the list is too large. |
| **UPDATE/DELETE locate partial decode** | `RowUpdater` / `RowDeleter` + `RequiredColumnAnalyzer.ComputeForLocate` | The row-locating scan for `UPDATE`/`DELETE` decodes only the columns the `WHERE` (and SET expressions) reference, not the whole row. The write phase still does one full decode per matched row to re-encode it and maintain indexes. |

## The cost model in detail

**Files:** `Models/Plans/PlanCost.cs`, `Controllers/Queries/CostEstimator.cs`.

`CostEstimator.AnnotatePlan(root, db, table, stats)` walks the tree bottom-up and assigns each node an
`EstimatedCardinality` and a `PlanCost`. The single source of truth for one node's cost is
`EstimateNodeCost`; `EstimateScanLeafCost` is a thin public wrapper used during candidate enumeration so
a leaf can be costed without building a whole plan tree (the cost-based passes rely on this).

`PlanCost` is a `readonly struct` of logical-I/O counters plus a network term, with `Total` the weighted
sum the optimizer compares:

```
Total = 1.0·KvPointLookups          // random read to the primary store
      + 1.0·KvRangeScanEntries      // sequential index-leaf read
      + 1.0·RowFetchesAfterIndex    // primary fetch after a non-covering index hit
      + 0.1·InMemoryRows            // sort/group/distinct buffer row (cheap)
      + NetworkFactor               // bytes shipped from remote partitions (see below)
```

Row counts come from `StatisticsManager.GetRowCountEstimate`. When stats are absent the model degrades to
`DefaultTableRowCount` (10 000) and fixed selectivity constants (range both-bounds → 10 %, one-bound → 40 %,
filter → 10 %, group-by → 20 %, distinct → 70 %). These annotations feed `EXPLAIN`'s `estimated_rows` /
`estimated_cost`.

**Always-on cost decisions** (no flag): the range-scan-vs-full-scan veto (`ShouldPreferFullScan` — replace
a predicate range scan with a full scan when estimated entries reach the 40 % breakeven), join-algorithm
selection (INLJ vs hash vs merge), and the unique-`IN` vs range-scan comparison. These existed before the
flagged passes and remain on by default.

> **KV cost note.** There are no index-only (covering) scans in the KV storage model — every secondary
> index hit pays a `RowFetchesAfterIndex` to read the primary row. So a non-covering index scan costs
> `≈ 2·matchedRows` and the cost model never assumes a "free" covering read.

## Statistics foundation

**Files:** `Statistics/StatisticsManager.cs`, `Statistics/Models/*`, `Controllers/TableAnalyzer.cs`.
Persisted to Kahuna meta KV at `{dbId}:stats:{tableId}` via `MetaJsonContext`; advisory — a missing or
stale value never throws, it just falls back. Statistics come in two flavours:

- **Incrementally maintained on DML** (cheap, always current-ish): `RowCount`, per-index entry counts,
  per-column min/max (`ColumnMinMax` of typed `ScalarBound`). Flushed on a debounced cadence
  (`stats_flush_interval_ms`) and on database close.
- **Built by `ANALYZE` only** (a one-pass scan; drift between runs is acceptable): equi-depth
  **histograms** and **distinct-value counts**.

`ANALYZE [TABLE] <name>` (`TableAnalyzer`) scans the table once (or samples the first
`stats_analyze_sample_rows` rows for large tables) and rebuilds every statistic in a single pass, then
persists. The histogram is **equi-depth** — each bucket holds roughly the same number of rows, so bucket
*widths* vary and dense value ranges get more buckets:

```
  value:   1   2   3 .. 8   9 .. 40        90 .. 99
  rows:   ▉▉▉▉▉▉▉▉▉▉▉▉   ▏ ▏ ▏ ▏ ▏ ▏        ▉▉▉▉▉▉▉   (skewed: dense at the ends)
  buckets:[ b0 ][ b1 ]  [ b2 ][ b3 ]  ...  [ bN ]    (≈ equal row counts, unequal widths)
           each ColumnHistogramBucket = { UpperBound, CumulativeRows, DistinctInBucket }
```

`ColumnHistogram` exposes `CumulativeFraction(v)` (fraction of rows ≤ v, interpolating within the boundary
bucket) and `RangeFraction(lo, hi)`. Distinct-value counts are stored as `ColumnNdv` (per column) and
`KeyNdv` (per composite-index prefix, keyed by a comma-joined column signature — `ANALYZE` emits an entry
for *every* prefix length 2…N of each composite index, so a `WHERE a=? AND b=?` over a 3-column index can
look up `KeyNdv["a,b"]`).

## Cardinality estimation

**File:** `Controllers/Queries/CardinalityEstimator.cs`. Turns a predicate (or a join) + statistics into
an estimated row fraction/count. Falls back to the fixed constants above when no stat is available.

**Filter selectivity** (`EstimateFilterSelectivity` → per-predicate):

| Predicate | Estimate |
|-----------|----------|
| `col = v` | `1/NDV` (preferred) → else histogram bucket `rows/total/distinct` → else fixed |
| `col <> v` | `1 − equality(v)` |
| `col > v` / `>= v` | histogram `RangeFraction(v, ∞)` |
| `col < v` / `<= v` | histogram `RangeFraction(−∞, v)` |
| `col IN (a,b,…)` | sum of per-value equality selectivities, capped at 1.0 |
| `p AND q` | product (independence) — **unless** the equality columns form a composite-index prefix, then `1/KeyNdv` for that tuple (corrects correlated columns like `city`+`zip`) |

**Join cardinality** (`EstimateJoinCardinality`): the textbook estimate

```
|A ⋈ B| ≈ |A| · |B| / max(NDV_A.key, NDV_B.key)
```

with one refinement — **FK-aware uniqueness**: if the join key *contains every column* of a unique index
on one side (`IsJoinKeyUnique`, set-containment, **not** prefix-match — a join on `tenant_id` alone of a
`UNIQUE(tenant_id, email)` is *not* unique), that side contributes ≤1 match per row and the result is the
other side's row count. Multi-column join keys use `KeyNdv`; single-column keys use `ColumnNdv`.

## Network & distribution cost

**Files:** `Controllers/Queries/PlacementReader.cs`, `RowWidthEstimator.cs`, `Models/Plans/DataDistribution.cs`.
Tier-1 network cost is fully computable on today's pull-to-coordinator engine: a scan against a remote
partition ships its result to the executing node, so the cost is *bytes shipped*.

- **`DataDistribution`** (set on scan leaves) records *which columns* partition the data — `Gathered`,
  `Partitioned(keyColumns)`, or `Replicated` (see [Part IV](#part-iv--distributed-ready-plan-properties)).
- **`NetworkFactor`** in `PlanCost` is `remoteRows · rowWidthBytes · NetWeight`, charged **only to
  `Partitioned` leaves** (a `Gathered` point lookup pays 0 — it is local by definition). `remoteRows =
  rows · (N−1)/N` where `N = ClusterPartitionCount` (a uniform-placement approximation; the live Kahuna
  range map is not read yet). `rowWidthBytes` comes from `RowWidthEstimator` (per-column type sizes).
  `NetWeight` (default `0.01`) is calibrated so one remote 100-byte row ≈ one local KV lookup.

With sharding off or a single partition, `NetworkFactor` is 0 and the cost reduces to the single-node
model — so enabling the network dimension never changes single-node plans. Once on, it rewards selective
remote access and filter pushdown automatically, because both ship fewer bytes.

## Cost-based access-path selection

**Flag:** `cost_based_access_path_enabled` (default off). **Files:** `IndexScanSelector.EnumerateViableSteps`,
`QueryPlanner.PickCheapestIndexOrFullScan`.

The heuristic `IndexScanSelector.TrySelectScan` picks the *first viable* index by a rule score (longest
equality prefix). The cost-based path instead **enumerates all candidates** — one step per index that
matches the predicate, plus the implicit full-scan baseline — costs each with `EstimateScanLeafCost`, and
keeps the cheapest. So a higher-scored index loses to a lower-scored one that is genuinely cheaper, and the
range-vs-full-scan veto becomes a true cost comparison rather than a fixed-threshold flip.

It engages only when the flag is on, stats and a row count exist, and the query is not an `UPDATE`/`DELETE`
(those keep the heuristic to avoid widening exclusive row-range locks). ORDER BY needs no special handling:
sort-elision is applied downstream from the *chosen* scan node's own ordering, so a cost-picked
order-satisfying index still elides the sort. IN-list competition runs after the cost pick, exactly as in
the heuristic path.

## Cost-based join-order enumeration

**Flag:** `cost_based_join_order_enabled` (default off). **File:** `Controllers/Queries/JoinEnumerator.cs`.

When off, join order is the heuristic `JoinOrderOptimizer.Reorder` — a deterministic, inner-joins-only
reorder that flattens the tree into leaves + a pool of `ON` predicates, scores each leaf (0 = equality on a
unique index → outermost; 1 = any indexed predicate; 2 = no pushable predicate → last), and rebuilds a
left-deep tree, with a feasibility guard that every rebuilt edge still has a connecting predicate.

When on, `JoinEnumerator` runs a **System-R-style bottom-up dynamic program** over table subsets (the
classic textbook algorithm). `dp[mask]` is the cheapest left-deep sub-plan joining exactly the tables in
the bitmask `mask`:

```
  dp[{A}] dp[{B}] dp[{C}]                         singletons = per-table scan cost
        \   |   /
  dp[{A,B}] dp[{A,C}] dp[{B,C}]                   size 2: best of (sub ⋈ leaf) over connected splits
            \    |    /
          dp[{A,B,C}]                             size N: the answer (cheapest full join)

  extend dp[leftMask] with leaf `bit`:
     cost = INLJ (leftCard·2, if `bit` has an index on the join key)  vs  hash (rightScanCost) → min
     only across a CONNECTING predicate (no cross products)
```

Subsets of each size are walked with **Gosper's hack** (next integer with the same popcount). Only splits
joined by a connecting predicate are considered, so cross products are never enumerated; the N−1 tree-edge
`ON` predicates are each attached exactly once, and cycle/extra predicates ride in the post-join filter
(`pushdown.PostJoinFilter`) — so reordering is result-preserving. The search is **capped at 12 tables**
(`MaxTablesForEnumeration`); wider joins, and any outer join, fall back to the heuristic.

The classic win this unlocks: a star join `events ⋈ sessions ⋈ users` where the heuristic keeps the large
declared-first `events` outermost (paying a full `events` scan), while the DP drives the tiny filtered
`sessions` into `events`'s `session_id` index via INLJ — orders of magnitude cheaper.

> **Not yet modeled (documented gaps):** "interesting orders/distributions" (keeping a costlier sub-plan
> that provides an ordering a parent needs) — `SubplanEntry` tracks cost + cardinality but not output
> ordering; bushy plans (left-deep only); and comma-join / `WHERE`-clause join predicates (only `JOIN … ON`
> predicates are pooled — others fall back to the heuristic).

## Config flags summary

| Flag (`config.yml`) | `CamusDBConfig` field | Default | Effect when on |
|---|---|---|---|
| `cost_based_access_path_enabled` | `CostBasedAccessPathEnabled` | `false` | Cost-based per-table access-path selection |
| `cost_based_join_order_enabled` | `CostBasedJoinOrderEnabled` | `false` | System-R join-order DP |
| `plan_cache_enabled` | `PlanCacheEnabled` | `false` | Per-process LRU plan cache (see below) |
| `plan_cache_max_entries` | `PlanCacheMaxEntries` | `512` | LRU capacity; 0 = effectively disabled |
| `key_range_sharding` | `KeyRangeShardingEnabled` | `false` | Enables `Partitioned` distributions + non-zero `NetworkFactor` |
| `initial_partitions` | `ClusterPartitionCount` | `1` | `N` in the `(N−1)/N` remote-fraction estimate |

All cost flags are **off by default and additive**: off (or with no stats) → the planner is byte-identical
to the heuristic. This is a hard invariant — a bad histogram or stale row count cannot silently regress a
production plan until an operator opts in.

### Plan cache (`plan_cache_enabled`)

When enabled, the cache stores the *optimization decision* — which index or join ordering was chosen —
keyed by `QueryShapeId`, a SHA-256 fingerprint of the query structure with literal values stripped out.
On a hit, the planner re-binds the current query's predicates into the cached structural choice and skips
cost enumeration.

**Plan-stability tradeoff.** Because the cache key ignores literal values, the access-path decision is
shared across all queries of the same shape regardless of the filter value:

```sql
-- Q1 seeds the cache with, say, an index scan on `status_idx`
SELECT * FROM orders WHERE status = 'pending'

-- Q2 is a cache hit → inherits the index-scan decision even though 'all' might be non-selective
SELECT * FROM orders WHERE status = 'all'
```

Both decisions are *correct* — the planner would choose the same index for either literal — but if
ANALYZE updates statistics that would change the choice (e.g. `status = 'all'` is now cheaper as a
full scan), the cache will not re-score until the next schema change (DDL) invalidates the entry.
ANALYZE alone does not invalidate the cache.

**Recovery.** To force a cache flush without a schema change: restart the process, or set
`plan_cache_enabled: false` and reload config. In the field, the cache is fully transparent:
disabling it degrades to the uncached code path with no correctness risk.

---

# Part VI — Current capabilities & roadmap

## What works today

Parse, bind, plan, and execute for: single-table SELECT with index selection (unique/composite/range),
`WHERE` with predicate analysis and filter absorption, `GROUP BY` + `HAVING`, global and grouped
aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), `SELECT DISTINCT` (hash or index-ordered streaming),
`ORDER BY` (with index-based sort elision), `LIMIT`/`OFFSET` (with pushdown), `[INNER]`/comma joins
(nested-loop, index-nested-loop, hash, and merge join — chosen cost-based), derived tables,
scalar/`IN`/`NOT IN`/`EXISTS` subqueries with
semi/anti-join rewrite for indexed `IN`/`NOT IN`, index-driven value-list `IN` (`x IN (v1, v2, …)`),
projection pushdown (including partial-decode locate for `UPDATE`/`DELETE`), an error/semantics matrix,
and full plan inspection via `EXPLAIN` / `EXPLAIN ANALYZE`.

**The cost-based optimizer stack** (see [Part V](#part-v--the-cost-based-optimizer)): a full statistics
foundation (`ANALYZE`-built equi-depth histograms and distinct-value counts, plus DML-maintained row
counts / index counts / min/max, all persisted), a cardinality estimator (histogram/NDV selectivity,
FK-aware join cardinality, composite-key correlation), a cost model with a real network/distribution
dimension (`DataDistribution` + `NetworkFactor`), two opt-in cost-based search passes —
per-table **access-path selection** (`cost_based_access_path_enabled`) and **join-order enumeration**
via a System-R dynamic program (`cost_based_join_order_enabled`) — and an opt-in **plan cache**
(`plan_cache_enabled`) that memoizes the access-path and join-order decision by query shape,
skipping re-enumeration on repeated queries. All three flags default off and degrade to the
heuristic planner with byte-identical plans.

## Gaps and where to contribute

These are the meaningful missing pieces and where new work fits.

| Area | State | Notes |
|------|-------|-------|
| **Table statistics — row counts** | **Done** | `StatisticsManager` tracks/persists `RowCount` per table (Kahuna meta KV `{db}:stats:{tableId}`), with a configurable flush cadence (`stats_flush_interval_ms`) and a close-hook flush. |
| **Index counts & column min/max** | **Done** | `StatisticsManager` tracks/persists per-index entry counts and per-column min/max (`ColumnMinMax`/`ScalarBound`), respecting index element-state. |
| **Histograms, NDV & `ANALYZE`** | **Done** | `ANALYZE [TABLE] <name>` (`TableAnalyzer`) one-pass-builds equi-depth `ColumnHistogram`s and distinct-value counts (`ColumnNdv`/`KeyNdv`, including every composite prefix), persisted. Sampling above `stats_analyze_sample_rows`. |
| **Cardinality estimator** | **Done** | `CardinalityEstimator`: histogram/NDV filter selectivity, IN-list, composite-key correlation, and FK-aware join cardinality (`|A||B|/max(NDV)`). |
| **Cost model + network** | **Done** | `PlanCost`/`CostEstimator` populate `EstimatedCardinality`/`Cost` incl. a real `NetworkFactor` (`DataDistribution` + `PlacementReader` + `RowWidthEstimator`); always-on vetoes (range-vs-scan, join algorithm) plus the two flagged passes below. |
| **Cost-based access-path selection** | **Done (opt-in)** | `cost_based_access_path_enabled`: enumerate + cost all index candidates per table, pick cheapest. Off by default → heuristic. |
| **Cost-based join-order enumeration** | **Done (opt-in)** | `cost_based_join_order_enabled`: System-R DP over table subsets (`JoinEnumerator`), capped at 12 tables, inner-joins-only, falls back to heuristic. Off by default → heuristic. |
| **Plan-cache hooks** | **Partial** | Plans record `TableSchemaVersion`; no stable query-shape identifier yet. No plan reuse. The next phase — caching optimized plans keyed by query shape — is unbuilt. |
| **Interesting orders in the join DP** | **Missing** | The DP costs by scan/join cost only; it does not keep a costlier sub-plan that supplies an ordering/distribution a parent needs (would avoid a later sort/exchange). |
| **Semi-/anti-join rewrite** | **Done** | Eligible uncorrelated `IN`/`NOT IN` over an **indexed** inner column rewrite to semi / anti / null-aware-anti join (`SemiJoinAnalyzer`/`SemiJoinExecutor`); non-indexed falls back to materialization. Three-valued `NOT IN` semantics preserved. |
| **DISTINCT streaming** | **Done** | `SELECT DISTINCT` over a NOT-NULL index set-prefix streams (adjacent-row dedup, O(1) memory); otherwise hash dedup. |
| **EXPLAIN ANALYZE for joins** | **Missing** | Throws today; needs the join executor instrumented with `PlanNodeStats`. |
| **Logical EXPLAIN** | **Cosmetic** | `(LOGICAL)` renders the physical tree relabeled; there is no distinct logical-plan rendering. |
| **Per-node timing** | **Root-only** | `actual_time_ms` is only measured for the whole plan, not per operator. |
| **Error/semantics matrix** | **Done** | `TestErrorMatrix.cs` — 14 negative cases (ambiguous column, bad HAVING refs, multi-column/multi-row subqueries, `COUNT(DISTINCT)`, etc.) asserting precise codes + messages. |
| **Query microbenchmarks** | **Missing** | No benchmarks for grouped aggregation, join algorithms, or subquery materialization. The last item from the original optimizer backlog (R14). |

Explicitly deferred (by design): OUTER joins, window functions, CTEs, quantified predicates beyond
`IN`/`NOT IN` (`ANY`/`ALL`/`SOME`), `COUNT(DISTINCT …)`, the optimized-plan cache, bushy join plans,
"interesting orders" in the join DP, and distributed *execution* (the distribution property and network
cost are modeled; remote operator execution is not).

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
| Cost-based join-order DP | `Commands/Executor/Controllers/Queries/JoinEnumerator.cs` |
| Cost model & cardinality estimator | `Commands/Executor/Controllers/Queries/CostEstimator.cs`, `CardinalityEstimator.cs`, `Models/Plans/PlanCost.cs` |
| Network / distribution cost | `Commands/Executor/Controllers/Queries/PlacementReader.cs`, `RowWidthEstimator.cs`, `Models/Plans/DataDistribution.cs` |
| Predicate classification | `Commands/Executor/Controllers/Queries/PredicateAnalyzer.cs` |
| Index scan selection / bound absorption | `IndexScanSelector.cs`, `IndexScanBoundAnalysis.cs` |
| Join predicate pushdown / equi-join analysis | `JoinPredicatePushdown.cs`, `JoinEquiJoinAnalyzer.cs` |
| Semi-/anti-join rewrite (`IN`/`NOT IN`) | `SemiJoinAnalyzer.cs`, `SemiJoinExecutor.cs`, `SemiJoinSpec.cs`, `SemiJoinMode.cs`, `Models/Plans/SemiJoinNode.cs` |
| Value-list `IN` → index seeks | `Models/Predicates/AnalyzedInList.cs`, `Models/Plans/IndexInListScanNode.cs`, `PredicateAnalyzer.cs`, `QueryPlanner.cs` (`TryBuildInListScanNode` / `TryBuildCompetingInListScanNode`), `QueryExecutor.QueryUsingInListIndex` |
| UPDATE/DELETE locate partial decode | `Controllers/RowUpdater.cs`, `Controllers/RowDeleter.cs`, `RequiredColumnAnalyzer.ComputeForLocate`, `QueryTicket.LocateColumns` |
| Query-shape id / plan-cache hooks | `QueryShapeComputer.cs`, `QueryPlan.QueryShapeId`/`SchemaDeps` |
| Plan tree → linear steps | `Commands/Executor/Controllers/Queries/QueryPlanStepAdapter.cs` |
| Projection pushdown | `ProjectionPushdownPlanner.cs`, `RequiredColumnAnalyzer.cs` |
| Plan rendering / EXPLAIN | `PlanRenderer.cs`, `ExplainExecutor.cs` |
| Post-scan pipeline | `Commands/Executor/Controllers/Queries/QueryPostScanPipeline.cs` |
| Single-table executor | `Commands/Executor/Controllers/QueryExecutor.cs` |
| Join / derived executor | `QueryJoinExecutor.cs`, `DerivedTableExecutor.cs` |
| Scan / filter / sort / aggregate / project / distinct / having / limit | `QueryScanner.cs`, `QueryFilterer.cs`, `QuerySorter.cs`, `QueryAggregator.cs`, `QueryProjector.cs`, `QueryDistincter.cs`, `QueryHavingEvaluator.cs`, `QueryLimiter.cs` |
| Row merge for joins | `Commands/Executor/Controllers/Queries/QueryRowMerger.cs` |
| Expression evaluator | `Commands/Executor/Controllers/SqlExecutor.cs` |
| Table statistics (row counts, index counts, min/max, histograms, NDV) | `Statistics/StatisticsManager.cs`, `Statistics/Models/TableStatistics.cs`, `ColumnMinMax.cs`, `ScalarBound.cs`, `ColumnHistogram.cs`, `ColumnHistogramBucket.cs`; flush cadence `CamusDBConfig.StatsFlushIntervalMs` / `stats_flush_interval_ms` |
| `ANALYZE` (stats builder) | `Commands/Executor/Controllers/TableAnalyzer.cs` |
| KV table access | `Storage/Kv/KvTableStore.cs` |
| Row encoding / decoding | `CommandsExecutor/Models/RowEncoder.cs` |
| Query plan model | `Commands/Executor/Models/QueryPlan.cs`, `QueryPlanStep.cs`, `QueryPlanStepType.cs` |
| Planner / EXPLAIN tests | `TestQueryPlanner.cs`, `TestPredicateAnalyzer.cs`, `TestJoinQueryPlanner.cs`, `TestPlanRenderer.cs`, `TestPlanDistributedProperties.cs`, `TestExplainExecutor.cs`, `TestExplainAnalyzeExecutor.cs`, `TestCostEstimator.cs`, `TestStatisticsManager.cs` |
| Cost-optimizer tests | `TestStatisticsHistogram.cs`, `TestStatisticsNdv.cs`, `TestAnalyzeTable.cs`, `TestCardinalityEstimator.cs`, `TestPlanCostBasedAccessPath.cs`, `TestPlanCostBasedJoinOrder.cs` (+ `…Persistence.cs` variants) |
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
- **Heuristic vs cost-based** — rule-driven plan choice vs statistics-driven cost comparison. CamusDB
  defaults to the heuristic planner with a real cost-based optimizer layered on top behind config flags
  (cost-based access-path selection and join-order enumeration).
- **`PlanCost` / cost model** — per-node estimated cardinality + weighted I/O cost plus a `NetworkFactor`
  (`CostEstimator`), fed by histograms / NDV / min/max / row counts; surfaced in `EXPLAIN` and consumed
  by the access-path, join-algorithm, and join-order choices.
- **Cardinality estimator** — `CardinalityEstimator`: turns a predicate or join + statistics into an
  estimated row count (histogram/NDV selectivity, FK-aware join cardinality).
- **Histogram / NDV** — equi-depth per-column row distribution (`ColumnHistogram`) and distinct-value
  counts (`ColumnNdv`/`KeyNdv`), built by `ANALYZE`; the inputs that make selectivity estimates accurate.
- **`DataDistribution` / `NetworkFactor`** — how a scan leaf's rows are spread across partitions
  (`Gathered`/`Partitioned`/`Replicated`) and the modeled cost of shipping remote rows to the coordinator.
- **Streaming (`IAsyncEnumerable`)** — pulling rows one at a time without materializing the whole result.
- **SQL-over-KV** — the architectural boundary where relational operations become Kahuna key/value reads.

For `EXPLAIN` output as a user feature, see [`docs/explain.md`](./explain.md).
