# Query Planner — Concepts & Developer Guide

This document explains how CamusDB turns a SQL `SELECT` string into result rows. It is written to be
read top to bottom by someone new to the project. **Part I** builds the mental model and vocabulary,
**Part II** walks the pipeline stage by stage, **Parts III–IV** cover plan inspection and
distributed execution, **Part V** is the cost-based optimizer (statistics → cardinality estimation →
cost model → cost-based access-path and join-order search), and **Part VI** maps what is
*not* built yet, which shows where you can contribute. Parts VII–IX are reference material (file map, extension
checklist, glossary).

If you only want to *use* `EXPLAIN`, see [`docs/explain.md`](./explain.md). This document is about how
the planner works internally and how to change it.

---

# Part I — Concepts

## What a query planner is, and why it exists

SQL is *declarative*: a query says **what** rows you want and leaves **how** to fetch them to the
engine. `SELECT * FROM robots WHERE id = 5` names no index; it describes the
result. The query planner decides the **how**: which index to use, whether to
sort, in what order to join tables, where to evaluate each predicate. Its job is to produce a correct
*execution plan* and, among the many correct plans, pick a fast one.

CamusDB sits on top of **Kahuna**, an ordered, transactional key–value store. So the planner's deeper
job is **translating relational operations into KV operations**: a table scan becomes an ordered range
read over a key prefix; an indexed lookup becomes a point read; a join becomes nested reads. This is the
same "SQL layer over a KV layer" split that CockroachDB and YugabyteDB use, and it is the single most
important idea in this codebase. The planner works with tables, indexes, rows and expressions. Kahuna
knows only about ordered keys and values.

## The five-stage pipeline (the core mental model)

A `SELECT` flows through five stages. They are kept separate on purpose. This means that joins or
subqueries can be added while single-table queries remain stable.

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

Each stage has a single responsibility and a distinct data type as its output. In short:
**parse** turns text into a tree, **bind** attaches meaning (which table `u` is, whether column `email`
exists, whether it is ambiguous), **plan** decides the strategy, and **execute** produces the rows.

The entry point that orchestrates all five stages is `CommandExecutor.ExecuteSQLQuery`
(`CommandExecutor.cs:1146`). It returns `(DatabaseDescriptor, IAsyncEnumerable<QueryResultRow>)` to the
HTTP layer.

## Logical vs physical plans

Newcomers often confuse the two kinds of plan.

- A **logical plan** says *what* relational result is wanted: "the rows of `users` where `id = 5`,
  projected to `email`." It does not mention indexes. In CamusDB the logical model is `SelectQuery`
  (and its bound form `BoundSelectQuery`).
- A **physical plan** says *how* to compute it: "point-lookup `users` by primary key `id = 5`, then
  project `email`." In CamusDB the physical plan is a tree of `PhysicalPlanNode` objects rooted at
  `QueryPlan.Root`.

The planner (stage 4) is the function `logical plan → physical plan`. Two physical plans can be
*logically equivalent* (return the same rows) and differ widely in cost. The planner exists to choose
between them.

## Heuristics vs cost-based optimization (where CamusDB is today)

A planner can choose a plan by rules or by cost.

- **Rule/heuristic-based:** deterministic rules — "if an equality predicate matches a unique index, use
  a point lookup"; "put the most selective table first in a join." Fast, predictable, easy to test.
- **Cost-based (CBO):** estimate the cost of several candidate plans using table statistics (row counts,
  selectivity) and pick the cheapest. More powerful, but needs statistics and a cost model.

**CamusDB has a heuristic planner with a real cost-based optimizer layered on top, gated behind
config flags.** The project followed the "heuristics before CBO" path. Both cost-based flags are on
by default. This means that the cost-based optimizer drives the two biggest plan decisions — **which access path**
to use per table and **what order** to join tables — by costing alternatives against statistics. When
a flag is off, or when usable statistics are missing, the heuristic planner makes that decision.

The mental model has three layers, from always on to flag-gated.

1. **Always on:** the cost model annotates each node with an estimated cardinality and a `PlanCost`,
   and a handful of decisions are cost-driven regardless of flags — the range-scan-vs-full-scan veto,
   join-algorithm choice (INLJ vs hash vs merge), and the unique-`IN` vs range-scan comparison.
2. **`cost_based_access_path_enabled` (default on):** the planner enumerates *all* viable index access
   paths per table and picks the cheapest by cost. The heuristic path takes the rule-scored "first viable index".
3. **`cost_based_join_order_enabled` (default on):** a System-R-style dynamic program enumerates join
   orders and picks the cheapest left-deep tree. The heuristic path orders tables by scan selectivity.

Both flags degrade to the heuristic path when off (or when statistics are absent). With the join-order
flag off, the heuristic plans are used unchanged, except that a cross product that the predicates do
not need is repaired. The cost model is fed by a full statistics
stack — equi-depth histograms, distinct-value counts (NDV), per-column min/max — built by `ANALYZE`,
plus a network-cost dimension for sharded deployments. See
[Part V — The cost-based optimizer](#part-v--the-cost-based-optimizer) for the full stack and
[Part VI](#part-vi--current-capabilities--roadmap) for what remains.

## The core data structures and how they flow

Five types carry the query through the pipeline. Understanding their relationship is most of
understanding the planner.

| Stage output | Type | What it is | Key shape |
|---|---|---|---|
| 1 | `NodeAst` | Raw syntax tree | One mutable class; `nodeType` + `leftAst`/`rightAst` + `extendedOne..Five` + `yytext` |
| 2 | `SelectQuery` | Typed logical query | Immutable record: `Source`, `Projections`, `Where`, `GroupBy`, `Having`, `OrderBy`, `Limit`, `Offset`, `IsDistinct` |
| 3 | `BoundSelectQuery` | Logical query + resolved names | Adds opened `BoundTableSource`s, `QueryRowNameResolver`, `IsMultiSource` |
| 4 | `QueryPlan` | Physical plan | `Root` (a `PhysicalPlanNode` tree) + `Steps` (a flattened linear list) |
| 5 | `QueryResultRow` | A result row | `readonly struct`: `ObjectIdValue RowId` + `Dictionary<string,ColumnValue> Row` (keyed by column name) |

Two points about these types are worth learning early.

1. **`NodeAst` is used through to execution.** After stage 2, expressions such as the `WHERE`
   predicate and `ON` conditions are raw `NodeAst` subtrees — they are evaluated at runtime by
   `SqlExecutor.EvalExpr`. The models wrap *structure* (which table, which projection) in typed objects
   but keep *expressions* as AST. So you will see `NodeAst` flowing all the way into the executor.
2. **A `QueryPlan` contains the plan twice.** `Root` is the real tree. The renderer, EXPLAIN and
   the join executor use it, and the distributed-ready metadata is attached to it. `Steps` is a flattened, leaf-first linear list produced by
   `QueryPlanStepAdapter`, consumed by the *single-table* executor, which predates the tree. They share
   the same node instances (the linear list points at the same `PhysicalPlanNode` objects). This
   means that per-node data like runtime stats is visible through both. When you add a node type, you touch both the tree
   builder and the flattener.

## Two execution paths, and why there are two

CamusDB has **two executors**, and you need to know which one executes a given query.

- **Single-table path** (`QueryExecutor`): the original, linear, step-by-step executor. It is used when
  the query has exactly one source. Walks `plan.Steps`.
- **Join / multi-source path** (`QueryJoinExecutor`): used when `BoundSelectQuery.IsMultiSource` is
  true (any join, comma join, or derived table). Walks the `plan.Root` tree recursively, then hands the
  merged cursor to `QueryPostScanPipeline` for the shared aggregate/sort/project/distinct/limit stages.

This duality is historical: the single-table linear path came first; the tree-based join path was added
on top. Both deliberately produce identical post-scan behavior because the join path reuses
`QueryPostScanPipeline`, which mirrors the single-table operator ordering. When you change post-scan
behavior (e.g. how `DISTINCT` works), change it in a way that both paths see.

## The streaming model

Each operator is an `IAsyncEnumerable<QueryResultRow>` transformer. This means that the pipeline is
**lazy by default**: rows are pulled one at a time from storage through the operator chain to the
caller, and the result set is not materialized. The exceptions are operators that *must* see all input before
emitting anything: `SortNode` (needs all rows to sort) and a grouped `AggregateNode` (needs all rows to
finish each group). Keep this in mind when adding operators — prefer streaming, and materialize rows
if the relational semantics require it.

---

# Part II — The pipeline, stage by stage

## Stage 1 — Parser

**Files:** `CamusDB.Core/SQLParser/SQLParser.Language.grammar.y` (grammar),
`SQLParser.Language.analyzer.lex` (lexer), regenerated into `SQLParser.Parser.Generated.cs` /
`SQLParser.Scanner.Generated.cs`.

`SQLParserProcessor.Parse(sql)` calls the generated LALR(1) parser and returns a `NodeAst`. All nodes
are instances of the same mutable `NodeAst` class, which has the following fields.

- `NodeType nodeType` — the kind of node (`Select`, `ExprEquals`, `Identifier`, …; see `NodeType.cs`)
- `NodeAst? leftAst`, `rightAst` — primary children
- `NodeAst? extendedOne … extendedFive` — extra slots for clauses that don't fit left/right. The
  `select_stmt` grammar rule assigns: projection, table, where, order, limit, offset, group, distinct
  flag, having across these slots — **read the `select_stmt` rule before adding a clause** so you claim a
  free slot correctly.
- `string? yytext` — leaf value, such as a column name or a number

**Parser regeneration is automatic at build.** The grammar/lexer are MSBuild `YaccFile`/`LexFile` items.
Running `dotnet build CamusDB.Core/CamusDB.Core.csproj` regenerates the two `Generated.cs` files. They
**are committed** — stage the regenerated files with your grammar change. Do not edit them by hand.

**What the parser already understands:** `SELECT [DISTINCT]`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`,
`LIMIT`/`OFFSET`, `[INNER] JOIN … ON …`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`, `CROSS JOIN`, comma joins,
dotted identifiers (`u.email`), scalar / `IN` /
`NOT IN` / `EXISTS` subqueries, and `EXPLAIN [(LOGICAL|PHYSICAL|ANALYZE)]`.

## Stage 2 — Logical Query Model

**Files:** `Controllers/DML/SelectQueryCreator.cs` (builder),
`Models/Queries/` (all logical model types).

`SelectQueryCreator.CreateSelectQuery(ast)` visits the `NodeAst.Select` node and produces an immutable
`SelectQuery` record.

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

The `QuerySource` hierarchy models the FROM clause as a tree of three node types.

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
predicate. For an **uncorrelated** scalar / `IN` / `NOT IN` subquery, it *executes the inner query once*
and materializes the result. It then replaces the subquery AST with a constant or a `SubqueryValueListAst`
membership node. This lets the downstream filterer treat `IN` as a value-list check.
`NOT IN` carries three-valued (NULL-aware) semantics. In the `WHERE`, `EXISTS` is left in place for
stage 3b. The select list, the `HAVING` and each `JOIN ON` predicate are rewritten the same way,
`EXISTS` included. Each **correlated** subquery of the `WHERE` and of the select list is left in place
for stage 3b; one in the `HAVING`, a `JOIN ON` or the select list of a query that groups or aggregates
returns `FeatureNotSupported`. Those positions have no per-row stage. The walk visits all child slots
of a node. For example, a `CASE` stores its `ELSE` in one slot, and a `BETWEEN` stores its bounds. This
means that a subquery at any depth is folded. The same rewriter serves `UPDATE`, `DELETE`, `INSERT VALUES` and `ON CONFLICT DO UPDATE`,
which have no bind pipeline. In the `WHERE` of an `UPDATE` or `DELETE` and in the `SET` values of an
`UPDATE` it folds each uncorrelated subquery and leaves each correlated one, which
`SelectBindPipeline.PrepareWriteClausesAsync` prepares as stage 3b does, with the target table as the
outer query. The locate scan then evaluates it for each row, at the statement timestamp; a `SET` value
reads the value that the locate scan computed for its row.
*Note:* because this stage executes inner subqueries, a query with a subquery does touch storage during
planning — relevant to `EXPLAIN` (see Part III).

## Stage 3 — Binding

**File:** `Controllers/Queries/QueryBinder.cs`

`QueryBinder.BindAsync(database, selectQuery)` resolves names against the catalog and produces a
`BoundSelectQuery`.

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

Binding has six steps, in this order.

1. **Open all table sources** — `TableOpener` opens each `TableSource` in the FROM tree, left to right;
   each opened `TableDescriptor` + alias becomes a `BoundTableSource`.
2. **Detect duplicate aliases** — `CamusDBException(InvalidInput)` on reuse.
3. **Build `QueryRowNameResolver`** — records which alias owns which columns. A column unique across all
   sources may be referenced unqualified; a name in multiple sources requires `alias.column`, else
   "ambiguous column." For a join, the binder then writes that owner into the predicates
   (`JoinPredicateQualifier`): each bare column name of an `ON` or of the `WHERE` becomes
   `alias.column`. The equi-join hoist of a `FROM` list is applied to the qualified `WHERE`
   (`CommaJoinNormalizer.HoistResolvedNames`). Each planner stage that reads the source of a column
   from its qualifier therefore gets the same input for a bare and for a qualified name. See
   [`docs/joins.md`](./joins.md#column-names-in-join-predicates).
4. **Validate projections, GROUP BY, ORDER BY** — each referenced column must resolve.
5. **Projection/grouping consistency** — non-aggregate projected columns in a grouped query must appear
   in `GROUP BY`.
6. **Validate JOIN `ON` predicates** — column references must resolve to the joined sources.

**Stage 3b — Correlated subquery preparation (`ExistsSubqueryPreparer`).** Walks the `WHERE` and the
select list for `EXISTS(...)` nodes and correlated scalar, `IN`, `NOT IN` and quantified subqueries,
at any depth. It registers each correlated subquery in an `ExistsSubqueryRegistry` threaded into `QueryTicket`.
Each one becomes a new node with a hidden name. This means that the parsed tree is not changed.
`SubqueryScopeAnalyzer` decides whether a body is correlated. A name resolves to the subquery first and
then to the outer query. This means that a bare outer name also makes the body correlated. Uncorrelated `EXISTS` is evaluated once; a correlated
subquery is evaluated per outer row by `QueryFilterer` in the `WHERE` and by `QueryProjector` in the select
list. A correlated scalar subquery records the type of its column, and the statement checks are repeated
after this stage. This means that a type mistake returns an error before any outer row is read. A correlated `EXISTS` whose body
reads one table and needs only its `FROM` and `WHERE` takes the fast form
(`CorrelatedExistsScan`): the executor reads the inner table, with the outer row's columns injected into
the subquery scope under inner keys first, and with an index seek when one fits. Any other body is a
`CorrelatedSubqueryBody`: each outer name is replaced by a hidden placeholder that has the type of the
outer column. The body goes through the full pipeline for each outer row, with the outer values as
parameters. Each outer name resolves one time to the keys of the outer row that contain it
(`CorrelatedOuterValues`). This means that a row reads only the cells that the body names. Such a body binds once when no nested subquery reads an outer value, and once per row
otherwise. It is always checked at prepare time. This means that an error does not depend on the outer
rows. Each prepared subquery can have a `CorrelatedResultCache`, which stores the result for each distinct set of
outer values for the rest of the execution: the boolean of an `EXISTS`, the value of a scalar subquery, or
the membership node of an `IN`, `NOT IN` or quantified subquery. The key is the outer values that the body
reads, compared field by field. There is no cache for a body with a volatile function, for a body that
reads a hidden value of an enclosing level, or for a fast-form `EXISTS` with an index seek. Under `AND`, `OR` and `NOT` the filterer evaluates a correlated `EXISTS` in place; under any other node, such as a `CASE`
branch or an `IS NULL` test, `CorrelatedSubqueryValues` evaluates each correlated subquery
of the subtree for the row first and puts its value in a per-row copy of the parameters, under the node's
hidden name, where the evaluator reads it. The tree is not copied. The binding records the result
type of a `COALESCE` or a `CASE` by node identity. An error of a subquery is stored with its value and
thrown when the evaluator reads that value. This means that a `CASE` branch that is not evaluated does
not fail. In a join, a `WHERE` conjunct that contains a correlated subquery is not pushed to one table's
scan. The outer columns it reads are not visible in the predicate. For this reason, it is evaluated after
the join, where all columns are present.

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
5. `TryComputeScanRowLimit` pushes `LIMIT` (+ `OFFSET`) into the scan when this is safe. It is safe
   when all of the following are true.
   - There is no filter.
   - There is no aggregation, GROUP BY or HAVING.
   - There is no DISTINCT.
   - The scan satisfies ORDER BY.

**Phase B — Plan tree construction.** The planner builds a leaf→root operator chain. The order of the
operators depends on the query shape.

| Query shape | Operator chain (leaf → root) |
|-------------|------------------------------|
| Ungrouped SELECT | `Scan → [Filter] → [Sort] → [Limit] → [Aggregate] → [HavingFilter] → [Project]` |
| GROUP BY | `Scan → [Filter] → Aggregate → [HavingFilter] → [Sort] → [Project] → [Limit]` |
| SELECT DISTINCT | `Scan → [Filter] → [Aggregate] → [HavingFilter] → [Project] → Distinct → [Sort] → [Limit]` |

(The "Ungrouped SELECT" row also applies to *global* aggregates, which have no `GROUP BY`: limit is
applied before the single-group aggregate.) `FilterNode` is a tree node that does not become a step. Its
predicate is the inline `ExecutionFilter` applied during the scan, which avoids a separate streaming pass.

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
| `GinIndexScanNode` | `GinScanFromIndex` | Entry scans of a GIN index for a jsonb predicate; candidate rows checked by the filter (§4c′) |
| `LimitNode` | `Limit` | LIMIT / OFFSET |
| `NestedLoopJoinNode` | _(join path only)_ | Nested-loop inner join |
| `IndexNestedLoopJoinNode` | _(join path only)_ | Index-probed inner join |
| `HashJoinNode` | _(join path only)_ | In-memory hash-table inner equi-join (builds the smaller side) |
| `MergeJoinNode` | _(join path only)_ | Streaming two-pointer inner equi-join over index-ordered sides |
| `DerivedTableScanNode` | _(join path only)_ | Scan of a materialized derived table |

### 4b. PredicateAnalyzer

**File:** `Controllers/Queries/PredicateAnalyzer.cs`. Classifies the WHERE predicate into three kinds of term.

1. **Indexable comparisons** — column-vs-constant (`=`, `<`, `>`, `<=`, `>=`, `BETWEEN`). Can drive index
   selection; once absorbed by a scan they are dropped from the execution filter. A constant
   (`PredicateAnalyzer.TryGetConstant`) is evaluated once, before any row is read. This means that it
   must give one value for the statement. A literal, a parameter and a statement-clock call such as `now()` qualify. An
   operand that calls a function that can give a new value on each call (`random()`, `gen_id()`,
   `nextval()`, `clock_timestamp()`; `ScalarFunctionEvaluator.ContainsPerCallVolatileFunction`) does
   not qualify, and its term remains in the residual filter.
2. **Column comparisons** — column-vs-column (join `ON`, cross-table WHERE). Currently always residual.
3. **Residual conjuncts** — everything else (`OR`, `LIKE`, `ILIKE`, non-deterministic, subquery
   predicates). Always re-evaluated per row.

`CollectAndConjuncts` splits an `AND` tree into individual terms. For example, `year >= 2001 AND year < 2005`
yields two comparisons that `IndexScanSelector` fuses into one range scan. `BuildExecutionFilter`
reconstructs an AND-tree from the *unabsorbed* comparisons;
`IndexScanBoundAnalysis.IsComparisonAbsorbedByScan` decides which are made redundant by the scan bounds.

**Constants are rewritten into the column's type before any selector sees them**
(`CoerceConstantsForColumns`). The row evaluator compares a mixed Integer64/Float64 pair by widening both
to double (`MixedNumericComparison`, the one shared rule), but an index key is typed: `KeyEncoder` encodes
the two types differently, and `ColumnValue.CompareTo` throws for the pair. An unconverted `1.0` on an INT
column would therefore build a lookup key that addresses no entry, and `a < 1.5` would hand the scan a
bound it cannot compare. `NumericBoundNormalizer` moves each literal into the column's domain exactly, as the following table shows.

| Column | Literal | Rewrite |
|--------|---------|---------|
| Integer64 | integral Float64 (`1.0`) | same operator, `1` |
| Integer64 | fractional, `=` / `!=` (`a = 1.5`) | no integer equals it: the comparison does not drive index selection and is evaluated in the residual filter. The literal is not rounded |
| Integer64 | fractional, `<` / `<=` (`a < 1.5`) | `a <= 1` |
| Integer64 | fractional, `>` / `>=` (`a > 1.5`) | `a >= 2` |
| Integer64 | NaN, ±Infinity, outside the `long` range | residual only |
| Float64 | Integer64 (`f = 1`) | `f = 1.0` (the evaluator widens the same way, even beyond 2^53) |
| Float32 | any numeric | converted only if the value survives a `float` round trip; otherwise residual |
| Numeric | Integer64 (`p = 2`) | `p = NUMERIC '2'`, exact |
| Numeric | `NUMERIC '…'` | same type, unchanged |
| Numeric | Float64 / Float32 (`p < 9.99`) | residual only. The evaluator compares NUMERIC with a float as doubles, and near 10¹⁹ thousands of NUMERIC values widen to one double. This means that no exact key range matches. `p < NUMERIC '9.99'` uses the index |
| Integer64 | `NUMERIC '…'`, integral (`n = NUMERIC '2'`) | `n = 2` |
| Integer64 | `NUMERIC '…'`, fractional | as a fractional float above (`<` → floor, `>` → ceiling, `=` → residual), computed on the 128-bit value and not through a double. For example, `n = NUMERIC '9007199254740992'` does not match `9007199254740993` |
| any | `IN (…)` items | each item converted. An item that can't be converted exactly (`1.5` on INT) is dropped, since only equality applies to a list item. A float item on a NUMERIC column takes the full list off the index |

The same pass parses a bare string literal on a Uuid/Id column into that type. Each planner entry that
feeds a table's predicates to a selector applies it: the single-table planner, the join-leaf builder,
the join-leaf costing, and the cardinality estimator. `IndexScanSelector` drops any comparison whose constant does not match its column
type after the pass. This means that a caller that skips the pass loses the index shortcut, but returns
the right rows.

### 4b′. Expression index predicates

**File:** `Controllers/Queries/ExpressionIndexPredicates.cs`. Called by `QueryPlanner.GetPlan` after
`ResolveQualifiedColumnNames` and before `CoerceConstantsForColumns`. For a table with an expression
index ([expression-indexes.md](expression-indexes.md)), it looks at each residual conjunct of the shape
`expr op constant`, `constant op expr`, `expr BETWEEN a AND b`, `expr IN (...)` or
`expr = a OR expr = b`. When the canonical text of `expr` (names in lower case, no table qualifier) equals
the canonical text of an index expression, the conjunct becomes an indexable comparison or an IN list on
that expression's hidden key name, the name the index has at that key position. The selector, the bound
absorption and the constant coercion then treat the name like a column, through three pieces.

- `TableDescriptor.ExpressionKeys` maps each hidden key name to its expression;
  `TableDescriptor.ExpressionKeyType` gives its type to the helpers that type index keys. This means
  that an expression key is not decoded with the String fallback of a missing column.
- `BoundIndexExpression.KeyColumn` is a column shape for the hidden name, which `FindColumn` returns.
  This means that a constant moves into the expression's type by the rules of the table above.
- A comparison carries its original conjunct. A comparison that no scan absorbs is evaluated on the row
  as written.

A join leaf, and a query whose rows are keyed by qualified names, is not matched.

### 4c. IndexScanSelector

**File:** `Controllers/Queries/IndexScanSelector.cs`. `TrySelectScan` scores each index and picks the
best. On a tie, the index with fewer columns wins, as the more selective one.

| Match type | Score |
|-----------|-------|
| Full equality on all columns, unique index | `10000 + column_count` |
| Full equality on all columns, non-unique index | `5000 + column_count × 10` |
| Equality prefix + range on next column | `5000 + prefix_length × 10 + 1` |
| Equality prefix only | `5000 + prefix_length` |
| ORDER BY prefix match (no predicate) | `1000` |

**Composite matching:** walk index columns left to right, accumulate the equality prefix; if fully
covered, emit `QueryFromIndex` (unique) or a prefix-bounded `RangeScanFromIndex` (non-unique).
**Equality on a non-unique index is a single-value range scan**: `WHERE year = 2000`
on a multi index renders as `index-range-scan(... from>=2000, to<2001)`, while `WHERE id = …` on the
unique PK renders as `index-lookup`. If a partial prefix is followed by a range predicate on the next
column, both bounds are combined.

**Equality on a value whose "next value" can't be computed** uses an **inclusive `[v, v]`** range in
place of an exclusive upper bound. That is the case for a type with no successor (`String`, `Id`, `date`,
`datetime`, `float32`, `bytes`), for a value at its type's maximum (`true`, the largest `int64`, `uuid`
or `NUMERIC`), and for a descending column, whose successor sorts the wrong way. `ScanIndex` appends a
high (U+FFFF) sentinel so all `encode(v) + rowId` entries are captured, then the decoded-key bounds
filter trims to `key == v` (the same mechanism the value-list `IN` path uses). This covers each equality shape, including an equality prefix before a trailing range.
For such a prefix + a *half-open* trailing range (`name = 'a' AND year > 2000`), the open side is capped
at the inclusive prefix. This means that the scan ends where the prefix ends. A missing successor does not prevent the use of the index. One helper,
`IndexScanSelector.EqualitySeekUpperBound`, picks the bound for the planner and for each executor seek.

**The step records its equality prefix.** The bounds alone do not say how many leading columns the
predicate fixed: the half-open `[v, next(v))` of an equality looks like a range on that column, and a
prefix plus a range carries the range value in only one of the two bounds. Each step from the matcher
therefore carries `EqualityPrefixLength`, copied onto the `IndexRangeScanNode`. Two readers depend on it.
The cost model prices the first `n` columns by distinct count and the column after them, when a bound
reaches it, by its histogram (below). The residual filter drops an equality on one of these columns for
all column types. The seek returns only rows that contain the bound value. Before the count existed, an
equality on a `uuid`, `date`, `datetime`, `bytes` or `NUMERIC` column remained in the filter and was evaluated
again on each row.

**ORDER BY scan elision:** if no predicate matches, `TrySelectOrderByScan` looks for an index whose
leading key parts equal the ORDER BY keys, each in the direction of that key, and returns an unbounded range scan;
`ScanSatisfiesOrderBy` confirms so the planner omits `SortNode`. A key and an index part match when they
name the same column, or when an ORDER BY expression has the canonical text of the index expression at
that position and reads only columns of the table (`OrderKeyMatches`). A direction that differs from the
index direction forces a real `SortNode`: there is no backward scan.

**Unique indexes are not complete row sets.** A unique index carries **no entry** for a row with a NULL in
*any* of its key columns. NULLs are distinct. This means that such rows are exempt from the constraint,
and no index entry is written for them. A scan over a *prefix* of `UNIQUE (a, b)` therefore returns only the rows whose `b` is non-NULL,
yet the row `(a = 1, b = NULL)` satisfies `WHERE a = 1`. The row does not enter the pipeline, and a residual filter therefore can't repair that. The rule, implemented once in `IndexScanSelector.UniqueIndexHoldsEveryQualifyingRow`,
is that **a unique index may serve a query only if each key column the query does not constrain with an
`=`, `<`, `<=`, `>` or `>=` comparison is declared NOT NULL**. A constrained column needs no proof — a NULL
there fails the predicate anyway — so a full-key lookup is always allowed. `a = 2 AND b > 0` can use its
prefix-plus-range scan even when `b` is nullable. `!=` does not count as a constraint. When the rule fails
the index is not a candidate; the planner falls through to another index or the table scan. Five
sites apply it: the shared predicate matcher (`TryMatchPredicateIndex`, which serves heuristic selection,
cost enumeration, plan-cache replay and join leaves), the ORDER BY scan, the streaming DISTINCT/GROUP BY
index (`TryFindStreamingDistinctIndex`), the correlated `EXISTS` prefix seek, and the merge join's
free-ordering index scan. A non-unique index stores all rows (NULL keys carry the row-id suffix), and this rule does not apply
to it. A `PRIMARY KEY` is stored as an ordinary unique index. For this reason, each primary-key
column is made NOT NULL at `CREATE TABLE` whether or not it was written, and `DROP NOT NULL` on a
primary-key column is refused; otherwise a composite key would lose its prefix scans and seeks under
this rule. Tables created before that behavior keep their declared nullability, and a composite key
whose columns are nullable is treated like any other incomplete unique index.

**NULL keys and range bounds.** A comparison with a NULL operand is UNKNOWN, which is not true.
This means that `WHERE b < 10` must not return a row whose `b` is NULL. A non-unique index *does* store
that row, with NULL ordered first. A scan with an open lower side starts at those entries, and the
planner has already dropped the absorbed conjunct from the residual filter. The index accessor therefore skips any
key with a NULL in a bound-covered column (`KvIndexAccessor.HasNullInBoundColumns`): a bound on a column
comes from a comparison on it, and no comparison admits NULL. The evaluator side of the same rule is in
`SQLExecutorBaseCreator.EvalComparison`, `EvalAnd` and `EvalOr`, which all predicate walkers share
(for example, WHERE and HAVING). For this reason, UNKNOWN propagates through `NOT`, `AND`, `OR` and
`CASE` identically on the table-scan and index paths.

### 4c′. GIN index scans

<a id="gin-index-scans"></a>

**Files:** `Controllers/Queries/GinScanSelector.cs` (match), `Models/Jsonb/JsonbGinEntries.cs`
(entries), `QueryExecutor.QueryUsingGinIndex` (execution). A GIN index (`IndexType.Inverted`) over a
`jsonb` column is not in `TableDescriptor.Indexes`. That map contains the ordered indexes, which the
ordered-scan rules read. GIN indexes are in `TableDescriptor.InvertedIndexes` (and both in
`AllIndexes`, which the write and drop paths read). So no ordered-index rule can pick a GIN index by
mistake.

**When.** The single-table planner tries a GIN scan when no ordered index narrows the scan: after the
rule-based selection and the IN-list scan find no index, and in the cost-based path when the cheapest
plan is the full scan. A `FORCE_INDEX` hint that names a GIN index takes the GIN scan when the
predicate can use that index, and the primary-row scan when it cannot. The join planner does not use
GIN indexes.

**Match.** `GinScanSelector` reads the top-level conjuncts in `PredicateAnalysis.ResidualConjuncts`.
The jsonb operators arrive there as calls of their functions (`@>` is `jsonb_contains`, and so on).
The shapes in the table match on the index column when the other operand is a constant
(`PredicateAnalyzer.TryGetConstant`), for example a literal or a parameter. An expression that reads no
column is also a constant.

| Conjunct | Requirements (an AND of OR groups) |
|---|---|
| `col @> c`, `c <@ col` | one group for each leaf of `c`, each with its exact entry; an empty container below the root asks for its path as a prefix |
| `col ? k` | one group: the prefix `k<key>;`, or the exact entries `[=s<key>` and `=s<key>` |
| `col ?\| ks` | one group with the requirements of all keys |
| `col ?& ks` | one group per key |
| `col ->> 'k' = t` | one group: the string entry of `t`, and the boolean or number entry whose text is `t` |
| `col -> 'k' = d` | the requirements of `col @> {"k": d}` |

All matching conjuncts on one index are ANDed. When several GIN indexes match, the one with the most
groups wins. `@>` with `{}`, `[]` or a top-level scalar, and `->>` with text that starts with `{` or
`[`, give no requirement. No entry names them.

**Superset and check.** Entries lose array positions and structure. This means that a row can contain all required
entries and fail the operator (`[{"a":1},{"b":2}]` contains the entries of `[{"a":1,"b":2}]`). The scan
absorbs no conjunct: the full predicate remains in `BuildExecutionFilter`, and the executor checks each
candidate with `MeetPlanFilterAsync`. A requirement must not exclude a matching row. It can include
extra ones.

**Execution.** Each requirement is one index scan: an exact entry reads `[e]` inclusive, and a prefix reads
`[p, p + U+007F)`. For an exact entry, the store appends the index sentinel. This means that all row ids
under the entry are in range. The row ids of a group are united, and the groups are intersected in order.
A later group therefore removes ids that an earlier group did not return. An empty set ends the scans. The candidates
are fetched in row-id order through `GetRowsBatch`, `IndexScanFetchBatchSize` at a time.

**Locks.** Each scanned range takes `AcquireBoundedIndexRangeLockAsync` first: shared for a read,
exclusive for the locate phase of UPDATE and DELETE. A row that starts to match writes an entry into
each group's ranges. This means that the first group's lock covers it, also when the scan stops early.

**Cost.** Statistics do not describe documents. For this reason, the node is priced at the fallback filter
selectivity (`FilterSelectivity`, 0.10 of the table) with `rows × requirements` scanned entries. The
estimate does not change the choice: the GIN scan is used whenever it matches and no ordered index
does.

**Plan cache.** A GIN decision is not cached. Its entries come from the literal, which the query shape
abstracts away. The next execution computes the entries again (the same rule as the IN-list scan).

**EXPLAIN** shows `gin-scan(index=<name>, groups=<n>, entries=<m>)` under the `filter(...)` node that
contains the check.

### 4d. Join planner — `JoinQueryPlanner`

**File:** `Controllers/Queries/JoinQueryPlanner.cs`. The join planner is used when `IsMultiSource` is
true, and it works in the steps below.

0. **Derived-table estimates** — `DerivedTableRowEstimator` plans the query of each derived table as a
   separate query, and records the estimate of the plan root. The plan cache and the trace are not used
   for this plan. The join-order
   search, the build-side choice and `CostEstimator` read it in place of the fixed 10,000 rows. The plan of
   an inner query applies the same step to the derived tables of that query. This means that a CTE chain
   gets its estimates from the bottom up. The step is applied to each plan, a plan-cache hit included. The
   annotation of a cached plan reads the estimates too. The derived-table executor plans each query again
   when it executes it. As a result, a chain of `n` levels costs about `n²/2` extra plans. A set operation, a work table and a recursive CTE reader
   keep the default.
1. **Join order** — `JoinEnumerator` (cost-based, the default) or `JoinOrderOptimizer.Choose`
   (heuristic, with the cross-product repair) may reorder sources first and move a conjunct to the join
   where its last table enters (§Part V). A tree that contains an outer join is planned in its declared order.
   The returned `JoinOrderChoice` carries the tree and the filter that is applied after its last join.
2. **Predicate pushdown** — `JoinPredicatePushdown.Analyze` splits WHERE into per-source scan filters
   (`ScanFiltersByAlias`) and a cross-source `PostJoinFilter`. A single-alias conjunct is pushed down
   if its alias is not *null-extended*, that is, in the right subtree of a `LeftOuter` join on its path
   to the root. A conjunct on the null-extended side is evaluated after the join. If it were pushed below
   the outer join, it would filter before padding and turn the join back into an inner join.
3. **Tree construction** — `BuildJoinTree` recurses the (possibly reordered) `QuerySource` and maps
   each source type to a plan node.
   - `TableSource` → `TableScanNode` with its pushed-down filter
   - `DerivedTableSource` → `DerivedTableScanNode` (inner query materialized lazily at execution)
   - `JoinSource` → left built recursively; the right source is checked by `JoinEquiJoinAnalyzer`
     and `JoinQueryPlanner` for algorithm selection. Each physical join node carries the
     `JoinKind` (`Inner` or `LeftOuter`). The same operators serve both kinds, since in each of
     them the left input is the preserved side. The algorithm is chosen in this order.
     - Right join key has a single-column index → `IndexNestedLoopJoinNode` (INLJ). When several
       `ON` conjuncts could drive the lookup, one on a unique index wins over one on a non-unique
       index, whatever their order in the `ON`: a unique probe returns one row, a non-unique probe
       returns all rows with the value, and a popular value can mean thousands of them per left
       row. Both run a page of left rows at a time (`IndexScanFetchBatchSize` rows): the distinct
       keys of the page are read with one batched index read (a point probe per key on a unique
       index, a bounded seek per key on a non-unique one). The rows found are read with one batched
       row read. A page therefore costs two store calls, where a per-row probe costs one or two for
       each left row. The rows come out in left-row order. A non-unique key with more entries than
       one probe page is read separately, page by page. A non-unique probe walks the store's equality
       scan. The raw window `[v, v + sentinel]` in the direction of the index contains exactly the
       entries of the key. This means that the scan returns their row ids and decodes no key. The
       probe asks for the store's default page. The store stops a bounded scan at the key. This means
       that a page costs the entries of the key only, and the largest page is the fewest round trips
       for a key with many entries. The join caches the right rows of each probed key in a memo for
       the rest of the join (up to 65,536 rows). An outer stream that repeats a key, for example the
       cast rows of one movie, probes the store once per distinct key. A key that repeats in one page
       of a unique probe shares the page's probe. The `ON` predicate is evaluated for each pair. The
       next rule applies unless…
     - Both sides have free index ordering (secondary index prefix covers the join key) **and** the
       outer side is large (> 100 rows estimated) → `MergeJoinNode` (streaming merge join); or…
     - Equi-join with stats available and hash preferred over INLJ → `HashJoinNode` (build =
       smaller estimated side for an inner join; **always the right side for a left outer join**.
       The preserved side is then the probe, and an unmatched probe row is padded in stream order); else…
     - No suitable right index → `HashJoinNode` for equi-joins, `NestedLoopJoinNode` otherwise.
   - An equi-join key needs one declared type on both sides. The hash table, the merge order and an
     index key all treat two types as unequal, but the evaluator compares `int64` with `numeric`
     exactly and with `float64` by value. `JoinEquiJoinAnalyzer` therefore does not take a mixed pair
     (`a.n = b.p` with `int64` and `numeric`) as a key. The conjunct remains in `ON`, and the nested
     loop evaluates it. The semi-join rewrite of `x IN (SELECT …)` applies the same rule to its index
     probe.
   - The rule trusts declared types. This means that each cell of a key column must have the declared
     type of its column. Storage enforces this for a base-table column. A derived-table column might not
     have it: `COALESCE(p, 0)` is declared `numeric` but gives an `int64` zero for a NULL `p`. For this
     reason, `DerivedTableExecutor` widens each numeric cell to the declared type of its column
     (`NumericWidening`). A `CASE` is declared as the widest numeric type of all its branches. As a
     result, the widening does not have to narrow a value.
   - A `RIGHT JOIN` does not reach the planner: `SelectQueryCreator` rewrites it into a `LeftOuter`
     join with the operands swapped. A `CROSS JOIN` becomes the comma form (or an inner join on the
     literal `TRUE` when mixed with `ON` joins). See [`docs/joins.md`](./joins.md).
4. Same Phase-C passes as the single-table planner.

## Stage 5 — Execution

### 5a. Single-table — `QueryExecutor`

**File:** `Controllers/QueryExecutor.cs`. `ExecuteQueryPlanInternal` walks `plan.Steps` and chains
`IAsyncEnumerable<QueryResultRow>` through the operator of each step.

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
  in the `QueryHavingWorkspace` scope (so `HAVING x > 0` resolves an aggregate alias `x`, and
  `HAVING a.k > 0` the group key `a.k` under the qualified text the predicate uses).
- **`ReduceToProjections`** → `QueryProjector` — for an ungrouped query (no `GROUP BY`), emits each row as a `QueryRow` (a
  value array with one `RowLayout` that all rows share). With a `*` in the select list, the output layout
  depends on the input columns. The projector builds it one time for each input layout, and a dictionary
  input row then gives a dictionary output row. A `GROUP BY` query emits dictionary rows.
- **`Distinct`** → `QueryDistincter` — dedups output tuples; NULLs compare equal.
- **`Limit`** → `QueryLimiter` — skips OFFSET, stops after LIMIT.

### 5b. Join — `QueryJoinExecutor`

**File:** `Controllers/Queries/QueryJoinExecutor.cs`. `ExecuteJoinQuery` drives `ExecuteJoinTree(Root)`,
applies the `PostJoinFilter`, then hands the cursor to `QueryPostScanPipeline.Apply`.
`ExecuteJoinTree` dispatches on the node type.

- `TableScanNode` → full scan with per-alias inline filter.
- `DerivedTableScanNode` → materializes the inner query once, caches in `plan.DerivedMaterializations`.
- `NestedLoopJoinNode` with `RangeSeek` → for each left row, evaluate the bounds against the left row,
  rewrite them into the key type (`NumericBoundNormalizer`), combine them with
  `IndexScanSelector.TryBuildLeadingColumnRange`, and read that index range through
  `JoinLeafScanner.ScanBoundTableByIndexRange` (range lock, counters and right filter as any join
  leaf); then evaluate `ON` on each pair. The planner (`JoinRangeSeekAnalyzer`) gives a join a seek
  if all of these are true: the join has no equality key, `ON` bounds the first column of an
  ascending right index on both sides with left-only expressions, and the right table has at least `MinRangeSeekRightRows` (64)
  rows or no row count yet. See [`joins.md`](./joins.md#range-seek).
- `NestedLoopJoinNode` → for each left row, walk the right source, merge, evaluate `ON`.
  A base-table right side is read from storage once, on the first left row, and kept for the
  other left rows (`NestedLoopInnerSpool`: in memory, or in a spill file when spilling is on).
  It is read again for each left row when its filter calls a volatile function, or when spilling
  is off and it has more than `HashJoinMaxBuildRows` rows or `HashJoinMaxBuildBytes` estimated
  bytes.
  Left outer: a left row whose inner scan accepted no pair is emitted once, padded.
- `IndexNestedLoopJoinNode` → for each left row, probe the right index (unique → point lookup;
  non-unique → equality-prefix scan). A NULL lookup value is not probed. Left outer: a NULL
  lookup value yields one padded row. So does a probe that finds no row or whose rows all fail `ON`.
- `HashJoinNode` → materialise the build side into an in-memory hash table keyed on the equi-join
  columns; stream the probe side and look up each row. A key can be an expression of one side
  (`JoinEquiJoinAnalyzer` matches it, `JoinKeyExpressionList` computes it for each row from the key
  column list of that side); such a node is not used as a merge join or a broadcast probe. See
  [`joins.md`](./joins.md#expression-keys). A build side that passes the spill caps is executed
  as a Grace hash join. With spill off, a nested-loop join is used if the build side exceeds
  `HashJoinMaxBuildRows` rows or `HashJoinMaxBuildBytes` estimated bytes. Build side is the smaller estimated input for an inner
  join and always the right input for a left outer join. Left outer: a probe row is padded at once if it has
  a NULL key or if no row of its bucket passes `ON`. The Grace path applies the same
  rule per partition (a probe row is unmatched overall exactly when it is unmatched in its
  partition) and sets NULL-keyed probe rows aside in one extra spill file to pad them once each.
- `MergeJoinNode` → when both sides have free index ordering (ForcedIndex scan or upstream
  SortNode), advance two enumerators in lockstep; buffer only the current equal-key run on the
  right side (O(run size) memory). When only one or neither side is pre-ordered, the unordered
  side(s) are materialised and sorted first. Left outer: a left row is padded when it has a NULL key, when
  the right stream has passed it, or when its run produced no `ON` pass. The left rows that
  remain after the right stream ends are padded too. The streaming path does not stop at the end of
  the right side alone.

The padded row of a left outer join is built by `OuterJoinPadding`: its right-column key list is
computed up front from the alias's required-column set and schema version (the same inputs the leaf
scanner decodes from). This means that the layout exists before any right row is seen and is shared by matched
and padded rows. A real right row with a key outside that layout throws "shape diverged".

Row merging (`QueryRowMerger`): right columns are stored as `alias.column` and also unqualified when
there is no collision; **merged rows carry `RowId = default`** — there is no single source row id.

### 5c. Post-scan pipeline — `QueryPostScanPipeline`

**File:** `Controllers/Queries/QueryPostScanPipeline.cs`. `Apply` reproduces the planner's operator order
for the join path. This means that both executors apply the operators in the same order.

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
deterministic, indented, multi-line string with one **canonical node name** per node. These names are
a stable vocabulary that is reused across the codebase.

```
table-scan, index-lookup, index-range-scan, index-in-list, filter, having-filter, aggregate, sort,
limit, project, distinct, semi-join, anti-join, null-aware-anti-join, nested-loop-join,
index-nested-loop-join, hash-join, merge-join, derived-table-scan, function-scan
```

`PlanRenderer.Render(plan, includeRequiredColumns, includeDistributedProperties)` produces the string
form; `PlanRenderer.WalkNodes(root, plan)` yields `(name, detail)` pairs in depth-first, parent-before-
child order. Both share one `GetRenderLine`. This means that the string form and the EXPLAIN rows always match. The
verbose `includeDistributedProperties` flag appends distributed-ready metadata (`order=[…]`, `decomposable=…`).

## EXPLAIN

**Files:** grammar `explain_stmt`, `Controllers/Queries/ExplainExecutor.cs`, wired in
`CommandExecutor.ExecuteSQLQuery`.

`EXPLAIN [(LOGICAL|PHYSICAL)] SELECT …` performs **parse → bind → plan but not execution**. It returns
one result row per plan node, with the columns `stage`, `node`, `detail`, `estimated_rows`
(`PhysicalPlanNode.EstimatedCardinality` from the cost model), and `estimated_cost`
(`PhysicalPlanNode.Cost.Total`). These are populated for single-table plans, and for join plans too when
the cost-based join-order flag is on (otherwise join-node estimates remain heuristic; see Part V).
`(LOGICAL)` and `(PHYSICAL)` currently render the same physical tree
(there is no separate logical-plan representation yet); they differ only in the `stage` label.
An unknown option word (e.g. `EXPLAIN (VERBOSE)`) returns an `InvalidInput` error. It is not treated
as a default explain.

*Caveat:* because stage 2b/3b execute uncorrelated subqueries during planning, `EXPLAIN` of a query that
contains a subquery does read storage for the inner query. The outer query is not executed.

## EXPLAIN ANALYZE

`EXPLAIN (ANALYZE) SELECT …` executes the query to the end of the cursor and adds **actual** runtime
counters per node. Counters live in `PlanNodeStats` (`Models/Plans/PlanNodeStats.cs`):
`RowsRead`, `RowsEmitted`, `KvPointLookups`, `KvScanEntries`, `CursorTicks` (the inclusive time of the
node's row cursor), `ElapsedMs` (the root, and the run of a derived table's query), the `Lateral*`
counters of a nested-loop join over a `LATERAL` body, `RanAs` (the
strategy a hash join actually ran, when it differs from the plan) and `BroadcastSpans` (per-span actuals
of a broadcast probe).
Extra result columns: `actual_rows`, `rows_read`, `actual_time_ms` (on each node, see
[`explain.md`](./explain.md#operator-times)),
`kv_lookups`, `kv_scan_entries`.

The instrumentation follows four design rules.

- **Gated and zero-cost when off.** Counters and timers are only allocated/updated when `QueryPlan.CollectRuntimeStats`
  is set (only EXPLAIN ANALYZE sets it). A normal `SELECT` pays no extra cost.
- **Filter rows are folded into the scan.** The `actual_rows` of a `filter` row is the child scan's
  post-filter emit count; its KV counters are 0 (storage cost is charged to the scan).
- **Join plans are instrumented too.** A join plan is executed by `QueryJoinExecutor.ExecuteJoinPlan`.
  The join executor allocates no `PlanNodeStats`. For this reason, `ExplainExecutor` attaches one to
  each node before the cursor opens. A join node has one plan input, its left side. This means that its
  storage counters
  (`rows_read`, `kv_lookups`, `kv_scan_entries`) count the reads of its right side, and `actual_rows`
  counts the rows the join produced. The row's detail adds `ran=` when a hash join changed strategy at
  run time. See [`docs/explain.md`](./explain.md) for the per-operator rules.
- **Partial aggregation is off under ANALYZE.** The gather branch in `QueryExecutor` takes the
  partial-aggregate path if runtime stats are not collected. This means that `EXPLAIN (ANALYZE)` of a
  distributed `COUNT`/`SUM`/`MIN`/`MAX` measures the row-gather path. A normal `SELECT` uses the
  partial-aggregate plan.

---

# Part IV — Distributed-ready plan properties

CamusDB gathers eligible key-range scans concurrently by span. A peer leader can execute a
fragment when the plan has a shippable filter, a partial aggregate, or an eligible inner hash-join
probe. These paths are opt-in through `distributed_query_execution` and require a multi-span
cluster placement; other plan shapes use their ordinary execution path. Plan nodes carry five
distributed-ready properties.

| Property | Meaning | Status today |
|----------|---------|--------------|
| `OutputOrdering` | The ordering this node guarantees on its output | Set on index scans that satisfy ORDER BY, on a streaming `DistinctNode` or `AggregateNode` (the index order of their grouping columns), and on `SortNode`; drives sort elision |
| `EstimatedCardinality` | Estimated output rows | Populated by the `CostEstimator` (single-table accurate; join estimates real when the cost flags are on) |
| `Cost` (`PlanCost?`) | Weighted cost estimate for the node | Populated by the `CostEstimator`; `null` if the plan was not costed. Carries a real `NetworkFactor` (see below) |
| `Distribution` (`DataDistribution?`) | How the node's output rows are spread across the cluster | Set on scan leaves: `Gathered` (single-node/unsharded), `Partitioned(keyColumns)` (range-sharded by these columns), or `Replicated`. `null` on non-leaf nodes |
| `CanDecomposeToLocalPlusMerge` | Whether the operator splits into per-partition local work + a merge | `true` for scan/filter/project; `AggregateNode` only for `COUNT`/`SUM`/`MIN`/`MAX` (not `AVG`); `false` for sort/limit/distinct/all join operators |

`CanDecomposeToLocalPlusMerge` is plan metadata. The eligibility rule for partial aggregates is
wider: a supported grouped `AVG` can be decomposed internally into per-span `SUM` and `COUNT`
and finalized after the merge.

`OutputOrdering` is set by the single-table `QueryPlanner` on index scans and sort nodes, and by
`JoinQueryPlanner` on `MergeJoinNode` (ascending on the left key columns — a downstream `ORDER BY`
on those columns can be elided). `HashJoinNode` leaves `OutputOrdering` null (hash does not preserve order).
All join nodes set `CanDecomposeToLocalPlusMerge = false`.

**`Distribution` is a real physical property.** It replaces the old `PartitionLocality`
placeholder. `PlacementReader` derives it from the table/index sharding scheme: when
`KeyRangeShardingEnabled` is on, the distribution of a primary-row scan is `Partitioned(pkColumns)`, and
that of a secondary-index scan is `Partitioned(indexColumns)`; a point lookup is always `Gathered` (it
returns ≤1 row, pulled to the coordinator). With sharding off, everything is `Gathered`. It is a
schema-derived snapshot: it encodes *which columns* partition the data. It does not read the live Kahuna
range map (partition count, which node is remote). The network **cost** that consumes it lives in
`PlanCost.NetworkFactor` (see [Part V — Network & distribution cost](#network--distribution-cost)).
When a primary-row scan qualifies, the planner wraps it in a `GatherNode`. The gather sends a span
to its leader through `IQueryFragmentTransport` when a shippable filter or another supported
fragment shape can be executed there. Unfiltered spans can also be read concurrently through Kahuna's
locator routing. The peer evaluates the filter and streams survivors; a qualifying aggregate computes
per-span partials that the coordinator merges. Consider `GROUP BY ... ORDER BY MAX(x) DESC LIMIT n`, or `MIN(x) ASC` on a NOT NULL column, when the
select list contains no other aggregate. For these queries, each span sends its local top
`OFFSET + LIMIT` groups, ties at the last value included (the `top_k` detail of the aggregate row). The
union of those groups contains each group of the result with its exact value. This means that the merge
and the bounded sort above it give the same answer. A peer that does not know the request field sends
all groups, which is also correct. `COUNT` and `SUM` add up over the spans. A span therefore cannot rank
its groups by them alone, and these aggregates send all groups. An eligible inner hash join broadcasts a bounded
build side so peers can probe their local spans. See
[key-range sharding](key-range-sharding.md#interaction-with-distributed-query-execution) for the
wire path and `EXPLAIN` for the gather and eligibility reason. General remote execution of
arbitrary plan subtrees remains outside this fragment model.

---

# Part V — The cost-based optimizer

CamusDB's optimizer is built as a stack: **statistics** feed a **cardinality estimator**, which feeds a
**cost model**, which drives **plan choices**. Each layer is usable by itself and degrades gracefully
when the layer below it is empty. This part documents the full stack, then the individual
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
`cost_based_join_order_enabled`) are on by default. When a flag is off or stats are missing, the heuristic
is used for that choice. This means that the optimizer is strictly additive.

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
| **Cost-based access-path selection** | `IndexScanSelector.EnumerateViableSteps` + `QueryPlanner.PickCheapestIndexOrFullScan` | Flag `cost_based_access_path_enabled`: enumerate *all* viable indexes for the predicate plus the full-scan baseline, cost each, pick the cheapest — subsuming the rule-scored "first viable index" pick and the veto above. |
| **Cost-based join-order enumeration** | `JoinEnumerator` (System-R DP) over `JoinGraph` | Flag `cost_based_join_order_enabled`: bottom-up dynamic program over table subsets that memoizes the cheapest left-deep sub-plan, replacing the scan-selectivity heuristic. Tables connect through `ON` and `WHERE` conjuncts, a step pays for its intermediate input, and each table set has one row estimate. The search visits only the table sets an order can reach, stops at a step budget and then takes a greedy order. Above 30 tables the heuristic (with its cross-product repair) is used, and a tree with an outer join is planned in its declared order under both. |
| **Semi-/anti-join rewrite** | `SemiJoinAnalyzer` + `SemiJoinExecutor` | Rewrite eligible uncorrelated `IN`/`NOT IN` into an index-probing semi/anti/null-aware-anti join if the inner column is indexed. Otherwise the subquery is materialized. The inner `WHERE` is evaluated per probe in the name scope of the subquery. This means that it can qualify a column with the inner alias. If that scope cannot resolve a name, the predicate goes to the materialization path, which returns the error. |
| **DISTINCT streaming** | `QueryDistincter` + `IndexScanSelector` | When the `DISTINCT` columns form an index set-prefix and are all NOT NULL, and the index contains all rows (any non-unique index, or a unique index whose key columns are *all* NOT NULL), scan in index order and dedup by comparing adjacent rows (O(1) memory). A hash set is not needed. The same condition gates streaming `GROUP BY`. |
| **Value-list `IN` → index seeks** | `PredicateAnalyzer` + `QueryPlanner` + `IndexInListScanNode` | Turn `x IN (v1, v2, …)` on an indexed column into one index seek per value (point lookup for a unique index, equality range for a non-unique one), unioned and row-id-deduped. This replaces a full scan + residual membership filter. Cost-gated, and cost-compared against a competing range scan. A selective unique `IN` therefore wins. When the column is unindexed or the list is too large, a residual filter is used. |
| **UPDATE/DELETE locate partial decode** | `RowUpdater` / `RowDeleter` + `RequiredColumnAnalyzer.ComputeForLocate` | The row-locating scan for `UPDATE`/`DELETE` decodes only the columns the `WHERE` (and SET expressions) reference. The write phase does one full decode per matched row to re-encode it and maintain indexes. |
| **UPDATE/DELETE by primary key without a locate read** | `RememberedRowLocate` + `UniqueRowIdCache` | When the locate plan is a point lookup on the primary key and the store remembers the key's row id, the statement skips the locate query: the write phase locks that row, reads it at the latest value and re-checks the full predicate there. Applies to optimistic transactions and to Serializable ones, if the predicate contains no subquery and the statement uses no `LIMIT`. A pessimistic Read Committed statement uses its locate read, which reads at the statement timestamp and takes no lock. If the remembered row is gone, the statement drops the id and executes the locate plan, whose index read is authoritative. |

## The cost model in detail

**Files:** `Models/Plans/PlanCost.cs`, `Controllers/Queries/CostEstimator.cs`.

`CostEstimator.AnnotatePlan(root, db, table, stats)` walks the tree bottom-up and assigns each node an
`EstimatedCardinality` and a `PlanCost`. The single source of truth for one node's cost is
`EstimateNodeCost`; `EstimateScanLeafCost` is a thin public wrapper used during candidate enumeration. It lets the
cost-based passes cost a leaf before a plan tree exists.

`PlanCost` is a `readonly struct` of logical-I/O counters plus a network term, with `Total` the weighted
sum the optimizer compares. Each counter has a weight of 1.0, except in-memory rows at 0.1.

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

> **KV cost note.** There are no index-only (covering) scans in the KV storage model. Each secondary
> index hit pays a `RowFetchesAfterIndex` to read the primary row. This means that a non-covering index
> scan costs `≈ 2·matchedRows`, and the cost model does not assume a "free" covering read.

## Statistics foundation

**Files:** `Statistics/StatisticsManager.cs`, `Statistics/Models/*`, `Controllers/TableAnalyzer.cs`.
Persisted to Kahuna meta KV at `{dbId}:stats:{tableId}` via `MetaJsonContext`; advisory. A missing or
stale value does not throw, and a default is used in its place. Everything described in this section is readable from a
SQL console with [`SHOW STATISTICS FOR <table>`](show-statistics.md). Some statistics are maintained by
DML, and the others are built by `ANALYZE`.

- **Incrementally maintained on DML** (cheap, always current-ish): `RowCount`, per-index entry counts,
  per-column min/max (`ColumnMinMax` of typed `ScalarBound`). Flushed on a debounced cadence
  (`stats_flush_interval_ms`) and on database close.
- **Built by `ANALYZE` only** (a one-pass scan; drift between two `ANALYZE` calls is acceptable): equi-depth
  **histograms** and **distinct-value counts**.

`ANALYZE [TABLE] <name>` (`TableAnalyzer`) scans the table once (or samples the first
`stats_analyze_sample_rows` rows for large tables) and rebuilds all statistics in a single pass, then
persists them. The histogram is **equi-depth**: each bucket contains roughly the same number of rows.
This means that bucket *widths* vary, and dense value ranges get more buckets.

```
  value:   1   2   3 .. 8   9 .. 40        90 .. 99
  rows:   ▉▉▉▉▉▉▉▉▉▉▉▉   ▏ ▏ ▏ ▏ ▏ ▏        ▉▉▉▉▉▉▉   (skewed: dense at the ends)
  buckets:[ b0 ][ b1 ]  [ b2 ][ b3 ]  ...  [ bN ]    (≈ equal row counts, unequal widths)
           each ColumnHistogramBucket = { UpperBound, CumulativeRows, DistinctInBucket }
```

`ColumnHistogram` exposes `CumulativeFraction(v)` (fraction of rows ≤ v, interpolating within the boundary
bucket) and `RangeFraction(lo, hi)`. Distinct-value counts are stored as `ColumnNdv` (per column) and
`KeyNdv` (per composite-index prefix, keyed by a comma-joined column signature). `ANALYZE` emits an entry
for *each* prefix length 2…N of a composite index. For example, `WHERE a=? AND b=?` over a 3-column
index can look up `KeyNdv["a,b"]`.

## Cardinality estimation

**File:** `Controllers/Queries/CardinalityEstimator.cs`. Turns a predicate (or a join) + statistics into
an estimated row fraction/count. When no stat is available, the fixed constants above are used.

**Filter selectivity** (`EstimateFilterSelectivity` → per-predicate) is estimated from the predicate
shape, as the following table shows.

| Predicate | Estimate |
|-----------|----------|
| `col = v` | the share of `v` from the statistics when `v` is a **heavy value** (one of the most common values `ANALYZE` kept, or a value that fills complete buckets of an older histogram) → else the residual `(1 − heavy shares) / (NDV − heavy count)`, at least one row, which is `1/NDV` when the column's statistics list no heavy value → else histogram bucket `rows/total/distinct` → else fixed |
| `col <> v` | `1 − equality(v)` |
| `col > v` / `>= v` | histogram `RangeFraction(v, ∞)` |
| `col < v` / `<= v` | histogram `RangeFraction(−∞, v)` |
| `col IN (a,b,…)` | sum of per-value equality selectivities, capped at 1.0 |
| `col LIKE 'abc%'` | the histogram range of the prefix, `[abc, abd)` |
| `col LIKE 'abc%x%'`, `col LIKE 'abc_'`, `col LIKE 'abc\_%'` | the prefix range, scaled by the share of the bucket bounds in it that match the full pattern. The prefix ends at the first `%`, `_` or backslash |
| `col LIKE '%abc%'`, `col ILIKE …`, `col LIKE … OR col LIKE …` | the share of the histogram's bucket bounds that match, each bound weighted by its bucket's rows (the bounds sample the sorted column at each hundredth rank. A pattern unrelated to the order therefore hits them in proportion, and a heavy value counts with all its buckets); half a bucket when none matches; the fallback if no histogram exists |
| `col NOT LIKE …` | `1 − pattern(…)` |
| `p AND q` | product (independence) — **unless** the equality columns form a composite-index prefix, then `1/KeyNdv` for that tuple (corrects correlated columns like `city`+`zip`) |

An equality reads the histogram before the distinct count. The reason is that `1/NDV` gives the same
price to all values of a column. On a skewed column that is wrong in both directions: a cast note that a tenth of the
rows carry is priced at one row in ten thousand, and the join search then starts from that filter
and probes the rest of the query from a result a thousand times larger than it planned for; a rare
value is priced at the average. The heavy values of a column are the most common values, which `ANALYZE`
stores next to its equi-depth histogram, each with its counted rows (`stats_top_values`, default 100).
If the column has at most that many distinct values, all values are stored. A status or a kind column
is therefore described exactly. Otherwise the stored values are those with at least two rows and more
than 1.25 times the average rows per value. The join estimate uses the same values, see *Join
cardinality* below. A histogram built before the list existed has the values that fill complete
buckets, two percent of the rows or more. An inclusive range bound on a heavy value carries its full
mass, and the cumulative fraction at a heavy
value counts the rows at or below the end of its run.

**Join cardinality** (`EstimateJoinCardinality`): the textbook estimate

```
|A ⋈ B| ≈ |A| · |B| / max(NDV_A.key, NDV_B.key)
```

The estimator refines this formula in four ways.

- **NULL keys do not match**: `ANALYZE` counts the share of each column's rows that are NULL
  (`TableStatistics.ColumnNullFraction`). A join estimate multiplies each side by the share of its rows
  that contain a key (`CardinalityEstimator.NonNullFraction`, the product over the key's columns).
  The histogram and the distinct count of a column describe its non-null rows. This means that the
  factor applies to each form below. A cast table whose character column is NULL in half the rows joins the
  characters at half its size, where the row count alone said all of it. Statistics written before
  the count existed assume no NULLs, as before.
- **FK-aware uniqueness**: if the join key *contains all columns* of a unique index
  on one side (`IsJoinKeyUnique`, set-containment), that side is unique. A prefix match does not count:
  a join on `tenant_id` alone of a `UNIQUE(tenant_id, email)` is *not* unique. For a unique side, that side contributes ≤1 match per row, and the result is the
  other side's keyed row count. Multi-column join keys use `KeyNdv`; single-column keys use `ColumnNdv`.
- **Skew-aware keys** (`JoinKeyDistribution`): a single-column key that is unique on neither side and has
  a histogram on both is estimated from the two value distributions, `|A| · |B| · Σ_v f_A(v) · f_B(v)`.
  The textbook formula assumes that all values are equally common. When both sides concentrate on
  the same values (customers and stores both in the popular cities), the true join is many times larger.
  The **heavy values** of a column are the most common values, stored next to its histogram
  (`stats_top_values`), each with its counted rows. A histogram built before the list existed has the
  values that fill complete buckets (`DistinctInBucket = 1`), with the rows of those buckets as their
  frequency. The rest of the rows are the **residual**, spread evenly over the rest of the distinct
  values. The sum is taken over the heavy values of either side (a value heavy on one side only meets the
  other side's residual at its average frequency), plus the two residuals joined with the textbook
  formula. When neither side has a heavy value, this is exactly the textbook estimate. The list matters
  because the skew that sizes a join is mostly below the bucket line. On the Join Order Benchmark,
  the most popular movie has one percent of the cast rows and the next ones half a percent. The join
  of two such columns is three times the textbook estimate, and the complete buckets alone see 1.7 times.
  ANALYZE builds histograms for the leading column of each index and, up to
  `stats_analyze_max_columns`, for the other columns of the table. This means that the refinement
  applies to all analyzed join keys.
- **A dimension row named by a filter: the plan-time lookup** (`PlanTimeLeafLookup`). A filter such
  as `it.info = 'release dates'` selects one row of a small table, and no statistic can say which key
  that row carries into the fact table. The average key of `movie_info.info_type_id` has 394 rows,
  and the named one has 3,208. Two priors that guessed the named key is a popular one were measured and
  rejected: true on the Join Order Benchmark, false on the retail data set, and the guess cost the
  retail tier a fifth of its work. The executor therefore reads the row before the search starts, when
  the leaf's table has a single-column unique key, at most `stats_plan_time_lookup_max_rows` rows
  (default 2,000; `0` turns the lookup off), and a filter with an equality or an `IN` list on
  constants: one scan of the small table under the query's own snapshot, the leaf's filter evaluated
  as the leaf scan evaluates it, and at most 64 kept rows. With the keys known, the leaf's row estimate
  is the kept count, the pair selectivity between the leaf and a fact column is the mean of the keys'
  shares of that column (`CardinalityEstimator.KeyShareSelectivity`, each key read from the column's
  histogram as an equality is), and the step cost of a probe through those keys returns that share
  in place of the average (`CostEstimator.EstimateIndexLookupProfile`). A key that is not a heavy
  value is priced at the average. This means that a random key does not change the estimate. If a scan
  meets more than twice the cap (a stale count) or selects more than 64 rows, the lookup is discarded and
  the statistics are used. EXPLAIN reads the same keys. This means that it shows the plan that the
  statement will execute.

**Rules and traces.** Each factor of an estimate has a rule (`EstimateRule`), the branch of the
estimator that produced it, for example `equality-ndv` or `join-skew-pair`. Under `EXPLAIN (ANALYZE)` the planner
records the rule, the value and the inputs of each factor on the node (`PhysicalPlanNode.EstimateTrace`,
printed as the `estimate_trace` column). A statement and the other `EXPLAIN` forms do not record a
trace. The plan-quality harness reads the trace of each node and lists the estimate errors by rule
(`docs/plan-quality-harness.md`). An experiment that fitted a multiplier and an exponent per rule
to the rows plans produced was measured and not adopted: per-rule scaling explained little of the
error, and the fit did not transfer across query templates. A lower estimate error also lost plans
at a cost break-even. The reason is that an operator choice flips at the break-even, whatever the
estimate is.

## Network & distribution cost

**Files:** `Controllers/Queries/PlacementReader.cs`, `RowWidthEstimator.cs`, `Models/Plans/DataDistribution.cs`.
The network factor estimates scan-row transfer to the coordinator from placement and estimated row
width. Actual traffic can be lower when a fragment filters or aggregates rows at the peer. For distributed
plans, the factor is therefore an approximation of shipped bytes.

- **`DataDistribution`** (set on scan leaves) records *which columns* partition the data: `Gathered`,
  `Partitioned(keyColumns)`, or `Replicated` (see [Part IV](#part-iv--distributed-ready-plan-properties)).
- **`NetworkFactor`** in `PlanCost` is `remoteRows · rowWidthBytes · NetWeight`, charged **only to
  `Partitioned` leaves** (a `Gathered` point lookup pays 0 — it is local by definition). `remoteRows =
  rows · (N−1)/N` where `N = ClusterPartitionCount` (a uniform-placement approximation; the live Kahuna
  range map is not read yet). `rowWidthBytes` comes from `RowWidthEstimator` (per-column type sizes).
  `NetWeight` (default `0.01`) is calibrated so one remote 100-byte row ≈ one local KV lookup.

When a `NetworkFactor` looks wrong, [`SHOW RANGES`](show-ranges.md) tells the two
explanations apart. It lists each span of a relation's key space and whether this node believes it
leads it. This shows whether the data is remote or whether this node's range map is
behind a peer's.

With sharding off or a single partition, `NetworkFactor` is 0 and the cost reduces to the single-node
model. This means that enabling the network dimension does not change single-node plans. Once on, it
rewards selective remote access and filter pushdown automatically, since both ship fewer bytes.

## Cost-based access-path selection

**Flag:** `cost_based_access_path_enabled` (default on). **Files:** `IndexScanSelector.EnumerateViableSteps`,
`QueryPlanner.PickCheapestIndexOrFullScan`.

The heuristic `IndexScanSelector.TrySelectScan` picks the *first viable* index by a rule score (longest
equality prefix). The cost-based path **enumerates all candidates**: one step per index that
matches the predicate, plus the implicit full-scan baseline. It costs each with `EstimateScanLeafCost` and
picks the cheapest. This means that a higher-scored index loses to a lower-scored one that costs less.
The range-vs-full-scan veto also becomes a true cost comparison, where the heuristic uses a fixed threshold.

It is used if the flag is on, stats and a row count exist, and the query is not an `UPDATE`/`DELETE`.
`UPDATE` and `DELETE` use the heuristic to avoid widening exclusive row-range locks. IN-list competition
takes place after the cost pick, exactly as in the heuristic path.

**The lock of a table scan** (`QueryPlanner.ScanHoldsPredicateLocks`, `KeepsBoundedScan`). The cost model
prices reads only. Some scans take predicate locks that last until the transaction ends. For these
statements, the lock matters more. A bounded index scan locks the range it reads. A table scan locks the
full row range of the table, and the statement then conflicts with all writes to that table. These
statements are the scans of a Serializable read-write transaction and the locate scan of an `UPDATE` or
`DELETE`. For them, the planner picks the bounded index scan over a table scan in two cases, whatever the
estimates say.

- the scan pins at least one leading key column by equality (`WHERE usersId = @p`), or the predicate is an
  IN list on the first column of an index;
- the table contains no more than 50 rows (or no more than `lock_escalation_threshold`, when that setting is
  below 50).

If this rule did not exist, the plan would follow the statistics of the moment. On a table of one or two rows the table
scan is as cheap as the index. After `ANALYZE`, a filter that selects most rows of a table (one tenant that
owns all rows) makes the table scan cheaper at any size. A pure range on a larger table (`year > 2000`)
is decided by the cost comparison. It can read a large share of the table, and its row locks then escalate
to the full row range in any plan. Join leaves follow the same rule (`JoinLeafAccess`), and the join-order search
costs the leaves the builder makes. With the plan cache on, a locking statement and a lock-free statement of
one shape have separate entries, and a cached decision is planned again when the row count of the table
moved by more than 4 times.

**Which index an IN list can seek** (`IndexScanSelector.TryPickInListIndex`). The list column must be the
first column of a readable index. A unique index of one column answers each value with a point lookup. A
non-unique index, and a unique index of several columns (a composite primary key), answer each value with a
seek over the entries that start with it. A unique index of several columns qualifies if its later
columns are `NOT NULL`. A unique index contains no entry for a row with a NULL key column, and the seek
would then lose rows. An OR chain of equalities on one column (`k = 'a' OR k = 'b'`) is analyzed as the list
`k IN ('a', 'b')` and takes the same path. The list gate needs column statistics to price a long list.
If they are missing, each value counts as a tenth of the table. A list of five or more values on a table
that has not been analyzed therefore reads the full table. The exception is a scan that takes predicate
locks (see above), where the list always seeks.

**How an index range scan is priced** (`CostEstimator.EstimateRangeScanRows`). The equality prefix of the
step (`EqualityPrefixLength`) is priced by distinct count: `KeyNdv` of the prefix tuple, else the column's
NDV, else the product of the column NDVs. If no distinct count exists for a prefix (for example, a `bool`
column or a column `ANALYZE` did not reach), it is priced at the equality fallback 0.10. The filter estimate uses the same
constant. This means that the prefix weighs the same in each step that binds it. The column after the prefix is
priced by its histogram over the bounds the step puts on it, else by linear interpolation between its
min and max, else, when the step pins one value of it (the last column of a prefix-only step), by its
distinct count, else by the fixed range constants for the bounds that column itself has (one side 0.50,
two sides 0.10). The last rule makes `studio_id = ? AND created_at >= ?` use the index
`(studio_id, created_at)` when four indexes start with `studio_id`. A `uuid` column has no histogram.
Before this rule, the prefix-only steps on the other three indexes were priced at the fixed 10 %. The step
that also bound `created_at` was priced by the histogram at its true third of the table, and lost.

**Ties.** Two candidates with the same cost are separated by three rules, in order.

1. A covering candidate wins.
2. The candidate whose bounds address more key columns wins (`IndexScanSelector.BoundColumnCount`). It
   reads a subset of what the shorter step reads.
3. The ordinal index name decides. This means that all nodes of a cluster pick the same plan from a
   Dictionary whose iteration order differs across descriptor rebuilds.

Two steps tie in cost when the statistics cannot tell them apart, for example a range column for which no
histogram exists. The tie must not go to the index whose name sorts first.

**`FORCE_INDEX` names the index the scan must use.** `FROM t@{FORCE_INDEX=idx} WHERE …` first asks the
matcher for the step `idx` can serve for the predicate. When there is one, that bounded scan is the plan:
it does not compete with the other indexes on cost or score, and the range-vs-full-scan veto does not
apply. When the predicate cannot bound `idx` (no comparison on its leading column, or a unique index that
would omit rows with a NULL key), the automatic choice is made as if there were no hint. The hint then
becomes an end-to-end scan of `idx` if no automatic scan is chosen. A name that is not a readable index of
the table is an error. The hint is part of the plan-cache shape. This means that the hinted and the
unhinted statement do not share a cached decision.

**ORDER BY and LIMIT are part of the comparison.** The cheapest scan alone is not always the cheapest
result, and two rules account for this.

- With an `ORDER BY`, a candidate that does not deliver that order also pays for the sort of its estimated
  output. The cost is 0.1 for each row the sort buffers, as the plan annotation prices a `SortNode`. A
  bounded sort buffers `limit + offset` rows. A candidate that delivers the order pays no extra cost.
- With a `LIMIT` on a row-preserving query (no DISTINCT, grouping, HAVING, aggregate or semi-join), the
  index that delivers the `ORDER BY` order joins the competition even when no predicate drives it. It
  stops once enough rows survive the filter. Its cost is `min(tableRows, (limit + offset) / selectivity)`
  entries at 2 units each (1 when the index covers the projection). `selectivity` is the estimate of the
  full `WHERE`. A conjunct the statistics cannot estimate counts at the fallback 0.1, which only makes
  this candidate look more expensive. If there is no `LIMIT`, it would read all entries and a row for
  each, and it loses to the full scan.

**A column fixed by `=` does not affect the order.** Sort elision skips the columns that a top-level `=` conjunct
fixes, on the index side and on the `ORDER BY` side (`IndexScanSelector.ConstantColumns`). So
`WHERE category = ? ORDER BY created` over an index on `(category, created)` needs no sort, and the `LIMIT`
then goes into the scan.

## Cost-based join-order enumeration

**Flag:** `cost_based_join_order_enabled` (default `true`). **Files:** `Controllers/Queries/JoinGraph.cs`,
`JoinEnumerator.cs`, `JoinOrderOptimizer.cs`, `JoinOrderChoice.cs`.

### The query graph

The planner splits the predicates of an inner-join tree into **conjuncts** and works with each one
separately. `JoinGraph` flattens the tree into its leaves (declared order) and a **conjunct pool**,
which is built by the rules below.

- each `ON` clause is split at its top-level `AND`s, and a literal `TRUE` is dropped. This means that
  `CROSS JOIN`, and a comma join with no equi-join conjunct, add no conjunct;
- each cross-table `WHERE` conjunct that `JoinPredicatePushdown` leaves after the last join is added
  when it is safe to evaluate at a join. It must name two or more tables and contain no subquery. Each of its identifiers must
  resolve to one table, and it must pass `FragmentFilterShippability.IsShippable`. The
  shippability check means that a join that the broadcast planner can ship from does not become one it
  cannot ship.

Each conjunct gets a bitmask of the tables it names. A conjunct with two or more tables is an **edge**;
a single-table conjunct and a constant do not connect tables. A table connects to a set of tables when an edge
names it, names at least one table of the set, and names no other table.

The graph cannot represent some trees, and the planner then uses the whole-predicate heuristic path.
This is the case when a join is not inner, when an `ON` conjunct contains a subquery, or when an `ON`
identifier does not resolve to exactly one table. A function name and a cast target type are skipped; they are not column references.

**The attach rule.** For a leaf order, `JoinGraph.BuildTree` puts each conjunct at the join where the last
of its tables enters the tree. A constant, and a conjunct on the first table only, go to the first join.
The `ON` of a join is its conjuncts in pool order (the `ON` conjuncts in tree order, then the taken `WHERE`
conjuncts), or `TRUE` when it has none. Each conjunct is attached exactly once. The build counts them,
and the original tree is used if the count is wrong. For inner joins, the results do not change. An `ON`
is a filter over the joined rows. This means that any join at or above the point where all of its tables
are present gives the same rows.

```
  FROM r0, r1, r2 WHERE r0.a = r2.a AND r1.b = r2.b

  declared tree (comma normalizer)        conjunct graph            rebuilt tree, order r2, r0, r1
  ((r0 ⋈ r1 ON TRUE)                      r0 ── r2 ── r1            ((r2 ⋈ r0 ON r0.a = r2.a)
     ⋈ r2 ON r0.a = r2.a AND r1.b = r2.b)                               ⋈ r1 ON r1.b = r2.b)
```

**Pinned conjuncts.** An `ON` conjunct that fails `FragmentFilterShippability.IsShippable` is not moved
independently and does not connect tables. Two kinds of conjunct are pinned.

- A **volatile** conjunct (`random()`, `now()`, …) is evaluated once for each row that reaches its join.
  If it moved, the number of evaluations would change. For example, a constant `random() < 0.5` moved from
  the last join to the first would decide once for many rows. The conjunct is bound to the **scope** of
  the join it was written on (that join and all tables before it), and it attaches where the last table of
  the scope enters. The scope is also an **order boundary**: the DP, the repair and the plan-cache replay
  accept only orders in which the scope's tables join before any other table. This means that each
  combination of their rows reaches the conjunct exactly once. The heuristic `Reorder` uses the declared
  order for a tree with a volatile `ON` predicate.
- Any other unshippable conjunct (a parameter) blocks a broadcast join on the join that contains it. It
  moves with its `ON` clause: it attaches where the last table of that clause enters, which is where the
  whole-predicate path puts the clause. For any order, it therefore blocks the same join as before and no
  other.

> **A conjunct can run at an earlier join than the query text shows.** A conjunct that moves from a late
> `ON`, or from `WHERE`, into an earlier join is evaluated on rows that a later join would remove. A conjunct that
> can raise a runtime error (a division, a cast) can then raise it on such a row. Scan pushdown already
> does the same for single-table conjuncts.

### The heuristic order and the cross-product repair

With the flag off, the order is the heuristic `JoinOrderOptimizer.Reorder`. This deterministic reorder
scores each leaf (0 = equality on a unique index → outermost; 1 = any indexed predicate; 2 = no pushable
predicate). It then rebuilds a left-deep tree with complete `ON` predicates. A feasibility guard checks
that each rebuilt join has a connecting predicate.

Complete predicates can leave a cross product that the predicates do not need. In the comma join above,
no edge connects `r1` to `r0`. For this reason, the declared tree joins them `ON TRUE`.
`JoinOrderOptimizer.Choose` therefore checks the heuristic tree. Suppose that no `ON` conjunct connects the
inputs of some join of the tree, **and** the conjunct graph is connected. Then `Choose` builds a connected order
through `JoinGraph`: from the scored order, each step takes the first remaining table that connects to the
tables already in the tree. A tree that contains no avoidable cross product is returned unchanged, and its
plan is the same as before. If the predicates of a query need a cross product, the heuristic tree is used.

The repair needs no statistics. It is applied whenever the heuristic plans the join: with the flag off, while
a table's row count is not loaded yet, above the 30-table enumeration cap, and when the enumerator finds
no plan.

### The dynamic program

When the flag is on, `JoinEnumerator` uses a **System-R-style bottom-up dynamic program** over table
subsets of the graph. `dp[mask]` is the cheapest left-deep plan that joins exactly the tables in `mask`.
The diagram shows how the subsets build on each other.

```
  dp[{A}] dp[{B}] dp[{C}]                         singletons = per-table scan cost
        \   |   /
  dp[{A,B}] dp[{A,C}] dp[{B,C}]                   size 2: best of (sub ⋈ leaf) over connected splits
            \    |    /
          dp[{A,B,C}]                             size N: the answer (cheapest full join)

  extend dp[leftMask] with leaf `bit` (only when `bit` connects to leftMask):
     step = CostEstimator.JoinStepCost(rows[leftMask], leafRows[bit], scanCost[bit], equiKey, indexLookup)
```

The DP visits **only the subsets an order can reach**. A subset of size `k` is a candidate when some
reachable subset of size `k − 1` extends it through an edge. The work is therefore the number of
connected (left set, right table) pairs, where a full search would cost `n · 2^n`. A sparse join graph of
17 tables costs about half a million steps. The candidates of a size are costed in ascending mask order,
and the tables of each from the last declared to the first. This means that the declared order wins a
tie. Only connected splits are candidates. This means that the DP does not cost a cross product, and a
query that needs one goes to the heuristic. Up to 12 tables, the row estimate of each subset is computed
up front. Above that, each subset is estimated when it is reached. The winning order is rebuilt through `JoinGraph.BuildTree`.

The search has two bounds. It plans at most **30 tables** (`MaxTablesForEnumeration`, the width of the
subset masks); a wider join takes the heuristic. It also costs at most **2,000,000 steps**
(`MaxCostedStepsForEnumeration`). Past that limit, a **greedy order** plans the join. The greedy
order starts with the table that has the fewest rows after its filter. At each step, it adds the
connected table that gives the smallest estimated result (the cheaper step on a tie, then the first
declared). The greedy tree is built and costed like a searched one, and `EXPLAIN (ANALYZE)` shows it as
`join-order=greedy`. The step count depends only on the join graph. This means that the same query takes
the same path each time.

**The step cost** is one function, `CostEstimator.JoinStepCost`, with the operator that the builder
selects for the conjuncts that the step attaches. The checks are those of the builder
(`JoinEquiJoinAnalyzer.TryExtractEquiKeys` and `TryMatch`), one conjunct at a time. For example, a
composite key `a.x = b.x AND a.y = b.y` also offers an index lookup on `b.x`.

| Operator | Step cost | When |
|---|---|---|
| hash | `rightScanCost + leftRows + 0.1 · min(leftRows, rightRows)` | an equi-key conjunct |
| index lookup | `memoHit · leftRows + probes · (probeWeight + matchesPerProbe)` | an index on a key column of the right table, and hash is not strictly cheaper (`PrefersHashOverIndexLookup`, the same rule `JoinQueryPlanner` uses) |
| nested loop | `rightScanCost + 0.1 · max(1, leftRows) · max(1, rightRows)` | no equi-key conjunct |

**The index-lookup step** is priced from the shape of its probes (`IndexLookupProfile`, built by
`CostEstimator.EstimateIndexLookupProfile` from the statistics). The formula has four inputs.

- Each left row costs `memoHit` = 0.65 of the unit a hash join pays for a left row. The operator
  stores the rows of each probed key in a memo. A left row whose key was probed before pairs with
  rows already in memory as a hash probe does. The lookup goes through a dictionary on the one key
  value, where a hash join uses a hash table over extracted key arrays. Measured on the JOB data set at scale 0.003 (2026-10-19):
  a memo hit with the pair it emits costs 0.32 µs against 0.51 µs for a hash probe with its pair.
  A hit priced far below that (0.1) once made a large join followed by memoized lookups look almost
  free, and the search moved selective tables to the end; the constant carries its derivation in
  `CostEstimator`.
- `probes` is the number of distinct keys of the left input: the distinct values expected among the
  left rows that hold a key, `CostEstimator.ExpectedDistinctKeys`. The keyed rows are
  `leftRows × nonNull`, where `nonNull` is the share of the left column's rows that hold a value (a
  NULL key is not probed; a cast table whose character column is NULL in half the rows probes the
  characters for half of them). Among `r` keyed rows of a table of `N` rows whose column has `d`
  distinct values, the expected keys are `d · (1 − (1 − r / N)^(N / d))`, the estimate PostgreSQL
  applies to a grouped column under a restriction: the rows of one key are spread over the table.
  This means that a part of the rows reaches a part of the keys, and keys grow slower than rows. The
  value is at most `min(r, d)`, which was the count before. It is also the count when the table's row
  count is not known. If there is no distinct count, each keyed row is a probe. On the Join Order
  Benchmark, the cast rows of the movies one keyword names (3,800 by estimate) were priced as 3,800 probes
  of `name`. However, 2,100 distinct people are among the 4,900 rows the chain produces. The estimate
  gives 3,300, and the probe then wins over a hash join that scans the full `name` table. The formula
  assumes an even spread. Popular keys that concentrate in the filtered rows repeat more than it says.
  The join-order search bounds `r` further by the rows the key's table can have in the left input:
  the smallest estimate of any subset of the left tables that contains it. A join after that table
  multiplies its rows but adds none of them. For example, the cast rows of a movie, repeated once for
  each of its companies, probe the people once each. The bound needs the estimate of each
  subset, which the search stores for joins of up to 12 tables. When the probed rows would not fit
  the memo (65,536 rows), each keyed left row is a probe too.
- `probeWeight` is the cost of one probe in rows read from storage: 3 for a unique index (one batched
  point read) and 16 for a non-unique index (one batched bounded scan), solved from the retail data set
  at scale 4 on memory storage, where a scanned row costs about 0.55 µs, a unique probe about 2.2 to 4.8 µs
  with its row fetch and a non-unique probe about 10 µs with its 2.4 row fetches. The constants carry
  their derivation in `CostEstimator`. A faster store changes the weight, and the formula is unchanged.
- `matchesPerProbe` is the rows one probe returns: 1 for a unique index, else the right table's rows
  over the NDV of the indexed column (1 when unknown), or the named keys' share of the column when the
  plan-time lookup knows the keys. Each is one row fetch. The leaf model charges a range scan two
  reads for each row (the index entry and the row fetch), and `EXPLAIN (ANALYZE)` counts a probe's
  rows the same way, but the step cost does not: priced at two, the rows of one bounded scan of a
  popular key (3,208 `movie_info` rows for one info type on the Join Order Benchmark) made the
  search prefer many probes of small keys and small hash builds in eight JOB plans, which read up
  to a third fewer rows and ran two to three times slower. One bounded scan that returns a thousand
  rows costs far less time than a thousand units of the model, and each extra probe pays for a
  round trip; the rows-read count of the harness is not the time.

A hash join therefore wins sooner than before: a unique probe is worth it while the right table has
about four rows for each distinct left key, and a non-unique probe while it has about seventeen rows plus
the rows the key returns. The fan-out term puts a filtered fact table before a small dimension:
with `customers` first, each of its 8,000 rows probes `orders` through the non-unique `customer_id` index
and returns three rows; with `orders` first, its 2,400 filtered rows join the customers with a hash join or
with probes by primary key, and either costs a fraction of the 8,000 probes.

`rightScanCost` is the cost of reading the right table: the same leaf cost the table would have as the
first input of the order. When its pushed filter drives an index (a range or an IN list, past the breakeven
veto), a hash or nested-loop join reads the right table through that index
(`JoinQueryPlanner.AttachRightAccessPath`, shown as `right-index=` in `EXPLAIN`), and the cost is the index
cost; otherwise it is the table's row count. `rightRows` is the filtered row count.

**The builder builds the costed operator.** For a tree from the DP, `JoinEnumerator.EstimateSteps` hands
`JoinQueryPlanner` the left rows, right rows and right read cost of each join, and the builder decides
index lookup against hash, the merge-join size threshold and the hash build side from those numbers, through
`CostEstimator.SelectJoinStepOperator` — the rule the step cost uses. Without them the builder sees a join
input only as a fixed 10,000 rows and a filtered right input only by its output rows, and could build a hash
join where the DP costed an index lookup. The heuristic order keeps the builder's own estimates.

The `leftRows` term makes the order matter: the left input of a step is the intermediate result
of the steps before it, so the total of a plan is the scan costs plus the size of each intermediate
result. Without it, every hash-only order of a query costs the sum of its scans and ties.

**Ties.** For each subset, candidates are tried from the last declared table to the first, and a candidate
replaces the current best only when it is strictly cheaper. The declared order is the first candidate for
every prefix set, so two orders of equal cost keep the order the user wrote.

### Row estimate of a table set

A dynamic program over subsets is correct only when "the best plan for set S" has one meaning, which needs
one row estimate for S, the same for every plan of S. `JoinEnumerator.EstimateSubsetRows` computes it from
the subset alone, as a product of three factors.

```
rows[S] = Π leafRows[t ∈ S] × Π selectivity(g ⊆ S) × Π classFactor(C, S)
```

- **Column classes.** The columns that equi-conjuncts join are grouped into classes of equal columns
  (`a.x = b.x` and `b.x = c.x` put all three in one class). A class with columns of three or more tables
  counts once for a subset, however many of its equalities the query writes: over the `k` tables of the
  subset with a column in it, it gives `k − 1` factors, `1 / d` for each table except the one with the
  smallest distinct count `d`. JOB names one movie through up to six columns and writes all fifteen
  equalities; multiplying fifteen selectivities made every join of such a query look almost empty.
- **Pair groups.** The equi-conjuncts `x.c = y.d` whose class has only those two columns form one group for
  each pair of tables, with one selectivity, `CardinalityEstimator.EstimateEquiJoinSelectivity`:
  `1 / max(ndvX, ndvY)`; `FallbackSelectivity` (0.1) when neither side has a count. A composite key uses its
  composite NDV. A single-column key, unique on neither side, with a histogram on both, uses the
  skew-aware selectivity of `JoinKeyDistribution` (see *Join cardinality* above).
- **Skewed classes.** When every column of a class has a value distribution
  (`CardinalityEstimator.EstimateJoinKeyDistribution`: uniform over the row count for a unique key, the
  histogram's most common values plus a uniform residual for a column with a histogram, uniform over the
  NDV for a column with a count and no histogram) and a column of the subset has heavy values, the `k − 1` factors
  are the skew-aware pair selectivities between an **anchor**, the skewed column of the subset with the
  fewest distinct values, and each other column. The distinct-count factors are the special case with no
  heavy value. This makes the cycle `customers ⋈ orders ⋈ stores ⋈ cities`, where customers and
  stores both concentrate in the popular cities, estimate at the size the data has instead of 1/20 of it.
  The factor is deliberately **one pair at a time**, never the sum over the product of every column's
  frequency: the mean of a product of five or six heavy-tailed frequencies is dominated by the few most
  common values, and a leaf filter that excludes them (one keyword, one company) makes that estimate wrong
  by orders of magnitude; on the Join Order Benchmark cliques it turned cheap index lookups into
  whole-table hash joins.
- The distinct count of a column is its **table row count** when it covers a unique index, else its column
  or composite-key NDV. The conjunct's own alias gives the table of each column, so a unique key is
  recognized wherever its table sits in the tree.
- The non-equi edges of one set of tables that bound a column of one of those tables from both ends
  (`n.n >= s.lo AND n.n <= s.hi`, or `n.n BETWEEN s.lo AND s.hi`) make one range group with
  `RangeJoinSelectivity` (0.005, PostgreSQL's default for a range), whatever the number of bounds
  (`CardinalityEstimator.CountJoinRanges`). A fallback for each bound would multiply shares that are not
  independent: four bounds at 0.1 each give 0.0001. The plan annotation of a nested-loop join with no
  equality uses the same rule, with one `FallbackSelectivity` for the conjuncts that are in no range.
- Every other edge (non-equi, an expression, three or more tables) is its own group with
  `FallbackSelectivity`. Single-table conjuncts and constants are not counted.
- The values are doubles with no floor or clamp; a caller that needs a row count floors and clamps it.

A unique side gives the standard estimate `leftRows × filteredRightRows / rightTableRows`, so a scan
filter on the unique side reduces it.

A typical case where the DP wins is the star join `events ⋈ sessions ⋈ users`. The heuristic keeps the
large declared-first `events` outermost (paying a full `events` scan), while the DP drives the tiny filtered
`sessions` into `events`'s `session_id` index via INLJ, which is orders of magnitude cheaper.

### Plan cache and the build mode

The plan cache stores the join as an alias order plus a **build mode** (`JoinBuildMode`). A
`WholePredicate` entry (the heuristic tree) replays through `JoinOrderOptimizer.ReorderByAliases`. A
`Conjunct` entry (a repaired tree) or `CostBased` entry (a DP tree) rebuilds the tree through `JoinGraph` from
the current query — also when the order equals the declared order, because the DP can keep the declared
order and still move a conjunct to an earlier join. A `CostBased` hit also recomputes the step estimates, so
it builds the operators of the miss; the one value it does not recompute is the bound on the distinct keys of
each lookup step, which the miss read from the estimate of every subset of tables and stores with the order,
because a replay estimates only the prefixes of that order. If the graph refuses the current query or the order is not connected, the
planner plans from the start, as on a miss, and counts a replay failure. `ON` literals always come from the
current query.

> **Not yet modeled (documented gaps):**
> - "interesting orders/distributions" (keeping a costlier sub-plan that provides an ordering a parent
>   needs), and merge join in the DP — the builder can select a merge join that the DP did not cost;
> - bushy plans (left-deep only), and a component-by-component plan for a query that needs a cross product;
> - more than 30 tables (the heuristic plans them, with the repair), and a join past the step budget gets
>   the greedy order, which is not guaranteed cheapest;
> - implied join edges: the column classes fix the estimate of `a.x = b.x AND b.x = c.x AND a.x = c.x`, but
>   when the query writes only `a.x = b.x AND b.x = c.x`, no edge joins `a` and `c` directly;
> - the probe weights are the average cost of a batched probe; a popular key of a skewed column returns
>   many more rows than `matchesPerProbe`, and a left input whose distinct keys a filter or an earlier join
>   reduced is priced by the column's distinct count in its table, an upper bound;
> - a tree with one outer join gets no DP at all and keeps its declared order;
> - a repaired or heuristic tree is built with the builder's own estimates (a join input counts as a fixed
>   10,000 rows);
> - `EXPLAIN` estimates come from `CostEstimator.AnnotatePlan`, not from the DP. For a join over a join they
>   resolve the table of the left key from its alias qualifier and use that key's distinct count — never
>   the unique-key shortcut, because an earlier join can repeat a key that is unique in its table — but the
>   two estimates can still differ. Both use the skew-aware pair estimate, so they agree on a join of two
>   skewed columns; the anchored pairs of a class of three or more columns are modeled by the DP only.
> - a cross-table correlation that no column statistic shows: in the retail data set eight orders in ten
>   go to a store in the customer's own city, so `o.store_id = st.id AND st.city_id = c.city_id` keeps
>   far more rows than the product of the two selectivities. The skew-aware estimate closes most of the
>   gap on that cycle (an estimate of 1/176 of the rows became 1/10), not all of it.

## Config flags summary

| Flag (`config.yml`) | `CamusDBOptions` field | Default | Effect when on |
|---|---|---|---|
| `cost_based_access_path_enabled` | `CostBasedAccessPathEnabled` | `true` | Cost-based per-table access-path selection |
| `cost_based_join_order_enabled` | `CostBasedJoinOrderEnabled` | `true` | System-R join-order DP |
| `plan_cache_enabled` | `PlanCacheEnabled` | `false` | Per-process LRU plan cache (see below) |
| `plan_cache_max_entries` | `PlanCacheMaxEntries` | `512` | LRU capacity; 0 = effectively disabled |
| `key_range_sharding` | `KeyRangeShardingEnabled` | `false` | Enables `Partitioned` distributions + non-zero `NetworkFactor` |
| `distributed_query_execution` | `DistributedQueryExecutionEnabled` | `false` | Fragments eligible key-range scans across span leaders; can push filters, partial aggregates, and bounded inner-join probes |
| `broadcast_join_max_build_rows` | `BroadcastJoinMaxBuildRows` | `10000` | Maximum broadcast hash-join build-side rows; 0 disables this path |
| `initial_partitions` | `ClusterPartitionCount` | `1` standalone, `3` cluster | `N` in the `(N−1)/N` remote-fraction estimate |

The two cost-based switches are **on by default**; the plan cache is off. The flags are additive.
Access-path costing engages on a DML-maintained row count alone: a table that has never been
`ANALYZE`d is costed with default selectivities rather than histograms/NDV, and can get a worse plan
than the heuristic would pick. Automatic `ANALYZE` ([automatic-analyze.md](automatic-analyze.md))
fills in the statistics once the table passes `auto_analyze_min_stale_rows`. With both switches off,
the planner gives the heuristic plans — that is the fallback if a bad histogram or a stale row count
regresses a production plan. The one difference from the plain heuristic is the cross-product repair: a
heuristic tree with a cross product that the predicates do not need is rebuilt in a connected order.

### Plan cache (`plan_cache_enabled`)

When enabled, the cache stores the *optimization decision* — which index or join ordering was chosen —
keyed by `QueryShapeId`, a SHA-256 fingerprint of the query structure with literal values stripped out.
On a hit, the planner re-binds the current query's predicates into the cached structural choice and skips
cost enumeration.

**What a single-table entry holds.** The index name plus how it was used (`ScanDecisionKind`): a
*predicate scan* (lookup or bounded range — the bounds are re-derived from the current query's literals
on every hit, so no literal is ever stored), an *ORDER BY index scan* (unbounded), a *full index scan*
(the streaming `DISTINCT`/`GROUP BY` override or a user-forced index), or a table scan. The kind matters
because a predicate-free decision has nothing to re-bind: replaying it through the predicate matcher
would find no comparisons, report a failed replay, and re-select on every hit. Those kinds are rebuilt
directly after re-checking that the index is still readable and, for a unique index, still holds every
row. A hit whose decision cannot be rebuilt falls back to a full replan and replaces the entry; the cache
counts these in `ReplayFailures` beside `Hits` and `Misses`, because a hit is counted at lookup time and
`Hits` alone cannot distinguish a cache that serves plans from one that only pays lookup plus replan.

**Plan stability across literals.** The cache key ignores literal values. This means that one entry
serves all queries of a shape, as in this example.

```sql
-- Q1 seeds the cache with, say, an index range scan on `price_idx`
SELECT * FROM orders WHERE price > 990

-- Q2 has the same shape, but almost every row matches: a full scan is cheaper
SELECT * FROM orders WHERE price > 1
```

A single-table entry therefore also keeps the estimate it was made for: the estimated selectivity of the
whole `WHERE` and the `LIMIT` demand (`limit + offset`, which matters with an `ORDER BY`). On a hit the
planner estimates both again for the current literals. When either moved by more than 4 times, in either
direction, it plans from the start and the fresh decision replaces the entry; `PlanCache.EstimateReplans`
counts these. The estimates come from the statistics only (no storage read), and a unique point lookup is
not guarded, because its cost does not depend on the literal.

Limits: the guard sees what the estimator sees. On a table with no `ANALYZE` every literal gets the same
default estimate, so nothing moves. An equality uses `1 / NDV`, the same for every value, so a skewed
column (`status = 'rare'` against `status = 'all'`) does not move the estimate either; the planner's own
fresh choice cannot tell those literals apart, so the replayed decision is the one it would make.
Schema changes (DDL), index changes, and `ANALYZE` all invalidate cached decisions.

**Recovery.** To force a cache flush without a schema change: restart the process, or set
`plan_cache_enabled: false` and reload config. In the field, the cache is fully transparent:
disabling it degrades to the uncached code path with no correctness risk.

---

# Part VI — Current capabilities & roadmap

## What works today

Parse, bind, plan, and execute for: single-table SELECT with index selection (unique/composite/range),
`WHERE` with predicate analysis and filter absorption, `GROUP BY` + `HAVING`, global and grouped
aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), `SELECT DISTINCT` (hash or index-ordered streaming),
`ORDER BY` (with index-based sort elision), `LIMIT`/`OFFSET` (with pushdown), inner, left/right outer,
cross and comma joins (nested-loop, index-nested-loop, hash, and merge join — chosen cost-based;
see [`docs/joins.md`](./joins.md)), derived tables,
scalar/`IN`/`NOT IN`/`EXISTS` subqueries with
semi/anti-join rewrite for indexed `IN`/`NOT IN`, index-driven value-list `IN` (`x IN (v1, v2, …)`),
projection pushdown (including partial-decode locate for `UPDATE`/`DELETE`), an error/semantics matrix,
and full plan inspection via `EXPLAIN` / `EXPLAIN ANALYZE`.

**The cost-based optimizer stack** (see [Part V](#part-v--the-cost-based-optimizer)): a full statistics
foundation (`ANALYZE`-built equi-depth histograms and distinct-value counts, plus DML-maintained row
counts / index counts / min/max, all persisted), a cardinality estimator (histogram/NDV selectivity,
FK-aware join cardinality, composite-key correlation), a cost model with a real network/distribution
dimension (`DataDistribution` + `NetworkFactor`), two cost-based search passes, on by default —
per-table **access-path selection** (`cost_based_access_path_enabled`) and **join-order enumeration**
via a System-R dynamic program (`cost_based_join_order_enabled`) — and an opt-in **plan cache**
(`plan_cache_enabled`) that memoizes the access-path and join-order decision by query shape,
skipping re-enumeration on repeated queries. The two cost-based passes default on and the plan cache
defaults off. With all three off, the planner falls back to the heuristic plans; the heuristic only
repairs a cross product that the predicates do not need.

## Gaps and where to contribute

These are the meaningful missing pieces and where new work fits.

| Area | State | Notes |
|------|-------|-------|
| **Table statistics — row counts** | **Done** | `StatisticsManager` tracks/persists `RowCount` per table (Kahuna meta KV `{db}:stats:{tableId}`), with a configurable flush cadence (`stats_flush_interval_ms`) and a close-hook flush. |
| **Index counts & column min/max** | **Done** | `StatisticsManager` tracks/persists per-index entry counts and per-column min/max (`ColumnMinMax`/`ScalarBound`), respecting index element-state. |
| **Histograms, NDV & `ANALYZE`** | **Done** | `ANALYZE [TABLE] <name>` (`TableAnalyzer`) one-pass-builds equi-depth `ColumnHistogram`s and distinct-value counts (`ColumnNdv`/`KeyNdv`, including every composite prefix), persisted. Sampling above `stats_analyze_sample_rows`. |
| **Cardinality estimator** | **Done** | `CardinalityEstimator`: histogram/NDV filter selectivity, IN-list, composite-key correlation, and FK-aware join cardinality (`|A||B|/max(NDV)`). |
| **Cost model + network** | **Done** | `PlanCost`/`CostEstimator` populate `EstimatedCardinality`/`Cost` incl. a real `NetworkFactor` (`DataDistribution` + `PlacementReader` + `RowWidthEstimator`); always-on vetoes (range-vs-scan, join algorithm) plus the two flagged passes below. |
| **Cost-based access-path selection** | **Done** | `cost_based_access_path_enabled`: enumerate + cost all index candidates per table, pick cheapest. On by default; off → heuristic. |
| **Cost-based join-order enumeration** | **Done** | `cost_based_join_order_enabled`: System-R DP over the reachable table subsets (`JoinEnumerator`) of the conjunct graph (`JoinGraph`), up to 30 tables and a budget of 2,000,000 costed steps with a greedy order past it, inner-joins-only, falls back to the heuristic with its cross-product repair. On by default; off → heuristic. |
| **Plan cache** | **Done (opt-in)** | `plan_cache_enabled`: per-process LRU cache of the access-path and join-order decision, keyed by `QueryShapeId`. Off by default. |
| **Interesting orders in the join DP** | **Missing** | The DP costs by scan/join cost only; it does not keep a costlier sub-plan that supplies an ordering/distribution a parent needs (would avoid a later sort/exchange). |
| **Semi-/anti-join rewrite** | **Done** | Eligible uncorrelated `IN`/`NOT IN` over an **indexed** inner column rewrite to semi / anti / null-aware-anti join (`SemiJoinAnalyzer`/`SemiJoinExecutor`); non-indexed falls back to materialization. Three-valued `NOT IN` semantics preserved. |
| **DISTINCT streaming** | **Done** | `SELECT DISTINCT` over a NOT-NULL index set-prefix streams (adjacent-row dedup, O(1) memory); otherwise hash dedup. |
| **EXPLAIN ANALYZE for joins** | **Done** | Every join operator records `PlanNodeStats`; a join row counts the reads of its right side, shows `ran=` when a hash join changed strategy at run time, and shows the join-order search estimate next to the plan annotation's ([`explain.md`](./explain.md#join-plans)). |
| **Logical EXPLAIN** | **Cosmetic** | `(LOGICAL)` renders the physical tree relabeled; there is no distinct logical-plan rendering. |
| **Per-node timing** | **Done** | `actual_time_ms` is the inclusive time of each operator's row cursor, also inside the plan of a derived table; a `LATERAL` join shows the runs and the time of its body ([`explain.md`](./explain.md#operator-times)). |
| **Error/semantics matrix** | **Done** | `TestErrorMatrix.cs` — 14 negative cases (ambiguous column, bad HAVING refs, multi-column/multi-row subqueries, `COUNT(DISTINCT)`, etc.) asserting precise codes + messages. |
| **Query microbenchmarks** | **Missing** | No benchmarks for grouped aggregation, join algorithms, or subquery materialization. |

Explicitly deferred (by design): `FULL OUTER JOIN`, `NATURAL JOIN`, `USING`, join-order enumeration
across an outer join, outer-join simplification (a null-rejecting `WHERE` turning a left join into an
inner join), CTEs, `COUNT(DISTINCT …)`, bushy join plans,
"interesting orders" in the join DP, and general remote execution of arbitrary plan subtrees.
Distributed fragments currently cover eligible span scans, partial aggregates, and bounded
broadcast probes for inner hash joins; see Part IV for their eligibility.

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
| Join-order heuristics and cross-product repair | `Commands/Executor/Controllers/Queries/JoinOrderOptimizer.cs` |
| Join query graph (conjunct pool, attach rule) | `Commands/Executor/Controllers/Queries/JoinGraph.cs`, `JoinOrderChoice.cs` |
| Cost-based join-order DP | `Commands/Executor/Controllers/Queries/JoinEnumerator.cs` |
| Cost model & cardinality estimator | `Commands/Executor/Controllers/Queries/CostEstimator.cs`, `CardinalityEstimator.cs`, `Models/Plans/PlanCost.cs` |
| Network / distribution cost | `Commands/Executor/Controllers/Queries/PlacementReader.cs`, `RowWidthEstimator.cs`, `Models/Plans/DataDistribution.cs` |
| Predicate classification | `Commands/Executor/Controllers/Queries/PredicateAnalyzer.cs` |
| Index scan selection / bound absorption | `IndexScanSelector.cs`, `IndexScanBoundAnalysis.cs` |
| Expression index predicates (WHERE and ORDER BY) | `ExpressionIndexPredicates.cs`, `IndexScanSelector.OrderKeyMatches`, `Controllers/DDL/IndexExpressionBinder.cs` |
| Join predicate pushdown / equi-join analysis | `JoinPredicatePushdown.cs`, `JoinEquiJoinAnalyzer.cs` |
| Semi-/anti-join rewrite (`IN`/`NOT IN`) | `SemiJoinAnalyzer.cs`, `SemiJoinExecutor.cs`, `SemiJoinSpec.cs`, `SemiJoinMode.cs`, `Models/Plans/SemiJoinNode.cs` |
| Value-list `IN` → index seeks | `Models/Predicates/AnalyzedInList.cs`, `Models/Plans/IndexInListScanNode.cs`, `PredicateAnalyzer.cs`, `QueryPlanner.cs` (`TryBuildInListScanNode` / `TryBuildCompetingInListScanNode`), `QueryExecutor.QueryUsingInListIndex` |
| UPDATE/DELETE locate partial decode | `Controllers/RowUpdater.cs`, `Controllers/RowDeleter.cs`, `RequiredColumnAnalyzer.ComputeForLocate`, `QueryTicket.LocateColumns` |
| UPDATE/DELETE by primary key without a locate read | `Controllers/RememberedRowLocate.cs`, `RowUpdater.LocateTuplesToUpdate`, `RowDeleter.LocateTupleToDelete`, `Storage/Kv/UniqueRowIdCache.cs` |
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
| Table statistics (row counts, index counts, min/max, histograms, NDV) | `Statistics/StatisticsManager.cs`, `Statistics/Models/TableStatistics.cs`, `ColumnMinMax.cs`, `ScalarBound.cs`, `ColumnHistogram.cs`, `ColumnHistogramBucket.cs`; flush cadence `CamusDBOptions.StatsFlushIntervalMs` / `stats_flush_interval_ms` |
| `ANALYZE` (stats builder) | `Commands/Executor/Controllers/TableAnalyzer.cs` |
| KV table access | `Storage/Kv/KvTableStore.cs` |
| Row encoding / decoding | `CommandsExecutor/Models/RowEncoder.cs` |
| Query plan model | `Commands/Executor/Models/QueryPlan.cs`, `QueryPlanStep.cs`, `QueryPlanStepType.cs` |
| Planner / EXPLAIN tests | `TestQueryPlanner.cs`, `TestPredicateAnalyzer.cs`, `TestJoinQueryPlanner.cs`, `TestPlanRenderer.cs`, `TestPlanDistributedProperties.cs`, `TestExplainExecutor.cs`, `TestExplainAnalyzeExecutor.cs`, `TestExplainAnalyzeJoins.cs`, `TestCostEstimator.cs`, `TestStatisticsManager.cs` |
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
- **Nested-loop join / index-nested-loop join** — for each left row, compare with the right source
  (read once and kept) / probe the right index.
- **Heuristic vs cost-based** — rule-driven plan choice vs statistics-driven cost comparison. CamusDB
  has a heuristic planner with a cost-based optimizer layered on top behind config flags
  (cost-based access-path selection and join-order enumeration), both on by default.
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

For `EXPLAIN` output as a user feature, see [`docs/explain.md`](./explain.md). For how histograms/NDV
are built by `ANALYZE` and kept fresh automatically in the background, see
[`docs/automatic-analyze.md`](./automatic-analyze.md).
