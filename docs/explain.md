# EXPLAIN Output Reference

> **Stability notice:** `EXPLAIN` output is diagnostic only and may change while the query
> planner is in alpha. Node names, column names, and detail formats documented here are
> considered **stable** within a minor version; any breaking change will be noted in the
> changelog. Do not build production logic that parses this output.

---

## Syntax

```sql
EXPLAIN SELECT ...
EXPLAIN (PHYSICAL) SELECT ...   -- identical to plain EXPLAIN
EXPLAIN (LOGICAL)  SELECT ...   -- same plan, stage column = "logical"
EXPLAIN (ANALYZE)  SELECT ...   -- executes the query and adds actual counters
EXPLAIN [( ... )]  SELECT ... FOR UPDATE [NOWAIT | SKIP LOCKED]   -- a locking read, see below
```

`EXPLAIN` without `ANALYZE` builds the physical plan but does **not** open any table cursor
or read any row data. `EXPLAIN (ANALYZE)` executes the full query, drains the result, and
reports actual runtime counters alongside the estimated columns.

An unrecognized option word (e.g. `EXPLAIN (VERBOSE) ...`) is rejected with an error rather
than silently treated as a plain `EXPLAIN`.

Every form reads as the query reads: in a Read Committed transaction it runs at one statement
timestamp, as the `SELECT` does.

> **Locking reads.** Every form accepts a [`FOR UPDATE` clause](./select-for-update.md#explain), as
> in PostgreSQL. The plan then has a `lock_rows` row below the `limit` and above the `sort`.
> - Plain `EXPLAIN` takes no lock. It needs `SELECT` and `UPDATE` on the table, as the statement
>   does, but it also works in a read-only or optimistic transaction.
> - `EXPLAIN (ANALYZE)` runs the statement and takes real row locks, which stay until the
>   transaction ends. Every check of the statement applies, and `NOWAIT` fails it as it fails the
>   statement. In autocommit it runs in a writable transaction.

> **Subquery note:** planning a statement that contains an uncorrelated subquery executes that
> inner subquery once (it is materialized during planning), so `EXPLAIN` of such a statement
> does read storage for the inner query. The outer query is never executed by plain `EXPLAIN`.

> **FROM-less limitation:** a [FROM-less `SELECT`](./fromless-select.md) has no plan tree, so
> plain `EXPLAIN` renders a fixed `constant-source` → `project` (→ `limit`) shape rather than a
> costed plan, and `EXPLAIN (ANALYZE)` is rejected (there is no table access to measure — use
> plain `EXPLAIN`). Unlike a real query, `EXPLAIN` of a FROM-less `SELECT` does **not**
> pre-materialize its projection subqueries; they appear only as their output column names.

---

## Result-row schema

### Plain EXPLAIN

| Column           | Type      | Description |
|------------------|-----------|-------------|
| `stage`          | `STRING`  | `"physical"` for `EXPLAIN` / `EXPLAIN (PHYSICAL)`, `"logical"` for `EXPLAIN (LOGICAL)` |
| `node`           | `STRING`  | Canonical node name (see table below) |
| `detail`         | `STRING`  | Node-specific key facts (see table below) |
| `estimated_rows` | `INT64`   | Cost-model estimate of the node's output cardinality. `NULL` when the plan was not costed. |
| `estimated_cost` | `FLOAT64` | Cost-model weighted cost for the node (unitless; lower is cheaper). `NULL` when not costed. |

`estimated_rows` / `estimated_cost` come from the cost model. Estimates use table statistics —
row counts, per-column min/max, and (after `ANALYZE`) equi-depth histograms and distinct-value
counts — when available, and fall back to fixed defaults otherwise, so the exact numbers depend
on what statistics have been collected and will differ between deployments. When an estimate looks
wrong, [`SHOW STATISTICS FOR <table>`](show-statistics.md) shows the inputs that produced it and
whether they were collected at all. `estimated_cost` also
includes a network term (`NetworkFactor`) for range-sharded deployments; it is 0 on a single node.
Hash, merge and nested-loop join estimates use the distinct-value counts and unique keys of the
join-key tables; an index-nested-loop join estimates one row for each outer row. For a join over a
join, the alias qualifier of the left key names its table, and the key's distinct count in that table
is used; a key unique in its table is not taken as unique there, because an earlier join can repeat
it. These estimates come from the plan annotation, not from the join-order search, so the two can
differ. `(LOGICAL)` and `(PHYSICAL)` currently
render the same physical tree, differing only in the `stage` label. See
[`docs/query-planner.md` → Part V](./query-planner.md#part-v--the-cost-based-optimizer) for the
cost model itself.

### EXPLAIN ANALYZE

All columns from plain `EXPLAIN`, plus:

| Column            | Type       | Description |
|-------------------|------------|-------------|
| `actual_rows`     | `INT64`    | Rows emitted (yielded) by this operator to its parent. |
| `rows_read`       | `INT64`    | Rows fetched and decoded from storage before filtering (scan operators). `0` for pipeline operators (they read from a cursor, not storage). |
| `actual_time_ms`  | `FLOAT64`  | Total wall-clock milliseconds for the **entire plan** (root node only). `NULL` on all other nodes (per-node timing is planned for a future release). |
| `kv_lookups`      | `INT64`    | KV point-lookups issued (unique-index lookups). |
| `kv_scan_entries` | `INT64`    | KV scan entries visited (table or range-index scans). |
| `estimate_trace`  | `STRING`   | How the node's row estimate was made, as one JSON object: the formula, the rule that dominates it, the input rows and one entry for each factor with the rule, the value and the statistics it read (`ndv`, `rows`, `alternative`, `textbook`, `keyed`). A join the join-order search estimated lists the leaves, the groups and the column classes of its table subset. `NULL` when the node has no estimate. The plan-quality harness records it for every node and reports the estimate errors by rule; see `docs/plan-quality-harness.md`. |

`actual_rows` ≤ `rows_read` for scan nodes: `rows_read` counts rows decoded from storage
before predicate evaluation; `actual_rows` counts rows that passed all filters and were
yielded upstream.

#### Join plans

A join plan is a chain: each join node has one plan input, its left side, and names its right
side (a table or a derived table) without a row of its own. `EXPLAIN (ANALYZE)` therefore
reports a join row this way:

- `actual_rows` is the number of rows the join produced.
- `rows_read`, `kv_lookups` and `kv_scan_entries` count the reads of the **right side**:
  - `nested-loop-join` reads a base-table right side from storage once, keeps its rows (in
    memory, or in a spill file when spilling is on), and compares every left row with them. So
    `rows_read` is the right rows one time. The join reads nothing on the right when the left
    side is empty. It reads the right side again for every left row (`rows_read` is left rows ×
    right rows, and the detail shows `ran=nested-loop-rescan`) in two cases: the right-side
    filter calls a volatile function, or spilling is off and the right side has more rows than
    `HashJoinMaxBuildRows`.
  - `index-nested-loop-join` probes a page of outer rows at a time (`IndexScanFetchBatchSize`
    rows): the distinct keys of the page cost one index read and one row read for the whole page,
    and the detail shows the number of pages as `probe-batches=`. Over a unique index it adds one
    `kv_lookups` for each distinct non-NULL key it probes. Over a non-unique index it adds one
    `kv_scan_entries` for each index entry it visits; the probe is bounded at the key, so no entry
    past it is visited, and a key with more entries than one probe page is read on its own, page by
    page. `rows_read` counts the right rows it fetched, before the right-side filter. A key that an
    earlier outer row probed, or that another outer row of the same page probes, is answered from
    the join's memo and adds nothing to these counters (`probe-memo-hits=`).
  - `hash-join` reads its right side one time, as the build or as the probe.
  - A `hash-join` or `nested-loop-join` whose right table has a pushed filter on an indexed column
    reads that table through the index; the detail shows `right-index=<name>` (or
    `right-in-list=<name>`), and `rows_read` counts only the rows the index returned.
  - `merge-join` adds the reads of its ordered right input (an index walk or a sorted scan).
    That input is a node, but the plan walk does not render it.
- A leaf row (`table-scan`, `index-range-scan`, `index-in-list`) counts its own reads, as on a
  single-table plan. A `derived-table-scan` row counts the rows of its materialized result. Its
  inner query has no rows here, so its `rows_read` is `0`. A derived table on the right side
  adds nothing to the join's `rows_read`. A `function-scan` row reads nothing from storage: its
  `rows_read`, `kv_lookups` and `kv_scan_entries` are `0`, and `actual_rows` counts the rows it
  generated.
- `actual_time_ms` is on the root row only, as on a single-table plan.

The work after the last join is not a plan node for a join query: the filter that runs after the
joins, grouping, ordering, `LIMIT` and projection. So the root row is the last join, not the final
result. A `LIMIT` shows as a join that produced fewer rows, because the cursor is lazy.

A join row's detail can also show two values that only execution knows:

| Suffix | Meaning |
|--------|---------|
| `ran=grace-hash` | The hash build was larger than the spill threshold, so the join ran as a Grace hash join. |
| `ran=nested-loop-fallback` | The hash build was larger than `HashJoinMaxBuildRows` with spilling off, so the join ran as a nested-loop join. |
| `ran=nested-loop-rescan` | The nested-loop join read its right side again for every left row, instead of one time (see above). |
| `ran=broadcast` | The build was small enough to ship, so the join probed the other side span by span. One `broadcast-span` row follows for each span. |
| `probe-memo-hits=<n>` | The index nested-loop join answered `n` probes from its memo of earlier probes: outer rows whose key an earlier outer row had already probed, or whose key another outer row of the same probe page probes. A hit reads nothing, so `kv_lookups`, `kv_scan_entries` and `rows_read` count the distinct keys only. The marker appears only when `n` is above zero. |
| `probe-batches=<n>` | The index nested-loop join probed its index in `n` pages of outer rows. Each page costs one batched index read and one batched row read for its distinct keys, instead of one or two reads per outer row. A page whose keys the memo all answered issues no call and is not counted; a non-unique key wider than one probe page is read on its own and is not part of the page's batch. The marker appears only when `n` is above zero. |
| `join-order-est-rows=<n>` | The join-order search's estimate of the rows of this join. It appears only when the search costed the order (statistics loaded, cost-based join order on). |

`estimated_rows` comes from the plan annotation, and the join-order search chose the order with
its own estimate (`join-order-est-rows`). The two models can disagree. With both next to
`actual_rows`, you can see which one was closer.

A node can run more than one time in one statement. A hash join fallback reads its build input
again, for example. The counters add up over each run, which is the work the statement did.

The `plan-info` row of `EXPLAIN (ANALYZE)` also shows how long planning took and, on a join plan,
how the join order was chosen. Plain `EXPLAIN` does not show these values.

| Suffix | Meaning |
|--------|---------|
| `planning-ms=<ms>` | The time of the bind and plan stages. It includes the subqueries that the bind stage runs to fold into literals. |
| `join-order=cost-based` | The cost-based search chose the join order. |
| `join-order=greedy` | The cost-based search passed its step budget, and the greedy order of the enumerator was built and costed instead: the smallest table first, then at each step the connected table with the smallest estimated result. |
| `join-order=heuristic` | The cost-based search did not choose the order. The cause is one of these: the search is off, the statistics are not loaded, the join has more than 30 tables, or no order avoids a cross product. |
| `join-order=cached` | A plan-cache hit replayed an earlier join order. |
| `join-steps-costed=<n>` | The number of join steps that the cost-based search costed. It is 0 when the search did not run. For the same query and the same statistics, the count is the same on each run, so you can compare it between versions. You cannot do that with a time. |

A `broadcast-span` row has the same columns as a `gather-span` row (see the distributed section
below). The node that owns the probe input counts each probe row that this node decoded, from a
local span or shipped from a peer: the left leaf for a right build, or the join row for a left
build. The rows that a peer scanned and did not ship are only in its `broadcast-span` row.

---

### The lock step under EXPLAIN ANALYZE

The `lock_rows` row of a locking read adds three counters to its detail. They are printed also
when they are zero. PostgreSQL does not have them: it shows only the rows in and out of the step.

| Counter | Meaning |
|---------|---------|
| `locked=<n>` | Row locks the step got. This includes the rows that `recheck-dropped` counts, because their locks stay until the transaction ends. |
| `skipped-held=<n>` | Rows that `SKIP LOCKED` left out because another transaction held them. Always 0 for the other wait policies, which fail the statement on a held row. |
| `recheck-dropped=<n>` | Rows the step locked and then did not return: a concurrent commit deleted the row, or changed it so that it no longer matches the `WHERE` clause, after the scan chose it. |

`actual_rows` of the step is `locked − recheck-dropped`. Example, with rows 2 and 3 held by another
transaction:

```text
EXPLAIN (ANALYZE) SELECT id FROM jobs ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED

node       detail                                                                 actual_rows
limit      3                                                                      3
lock_rows  for update, skip locked, locked=3, skipped-held=2, recheck-dropped=0   3
...
```

The step sent rows 1, 2 and 3 (it locked row 1 and skipped rows 2 and 3), and then rows 4 and 5.

## Canonical node names

One row is emitted per physical plan node, in depth-first order (parent before children).

| Node name                | When it appears | Key detail fields |
|--------------------------|-----------------|-------------------|
| `table-scan`             | Full table scan or forced-index scan | `table=<name>` (forced-index adds `, forced-index=<name>`) |
| `index-lookup`           | Equality on a **unique** index | `index=<name>, key=<value>` |
| `index-range-scan`       | Range predicate (`<`, `>`, `BETWEEN`), or equality on a **non-unique** index | `index=<name>, from>=<val>, to<<val>` |
| `index-in-list`          | `x IN (v1, v2, …)` on an indexed column — one index seek per distinct value, results unioned | `index=<name>, values=<n>` (count of seek values) |
| `filter`                 | Residual predicate not satisfied by the chosen index | `<expr>` |
| `aggregate`              | `GROUP BY` or aggregate functions | `group=[<exprs>], aggs=[<calls>]` |
| `having-filter`          | `HAVING` clause applied after aggregation | `<expr>` |
| `sort`                   | `ORDER BY` not satisfied by scan ordering | `<col> ASC/DESC, ...` |
| `limit`                  | `LIMIT` / `OFFSET` | `<n>` or `<n> offset <m>` |
| `project`                | Column projection after other pipeline stages | _(no detail)_ |
| `distinct`               | `SELECT DISTINCT` | `streaming: true` (index-ordered, O(1) memory) or `hash` |
| `semi-join`              | `IN (subquery)` rewritten to a semi-join over an indexed inner column | `outer=<col>, inner=<table>.<col>, index=<name>` |
| `anti-join`              | `NOT IN (subquery)` over a **NOT NULL** indexed inner column | `outer=<col>, inner=<table>.<col>, index=<name>` |
| `null-aware-anti-join`   | `NOT IN (subquery)` over a **nullable** indexed inner column (SQL three-valued semantics) | `outer=<col>, inner=<table>.<col>, index=<name>` |
| `nested-loop-join`       | Join without a usable index on the right side | `on=<expr>, right=<alias>` |
| `index-nested-loop-join` | Join where the right side's join key is indexed | `on=<expr>, index=<name>, left=<col>, right=<col>` |
| `hash-join`              | Equi-join using an in-memory hash table; chosen over INLJ when the outer side is large relative to the inner | `on=<left>=<right>, build=<alias>` (build-filter appended when a pushed-down filter is present) |
| `merge-join`             | Equi-join using a streaming two-pointer merge; chosen when both sides have free index ordering on the join key | `on=<left>=<right>, right-order=forced-index=<name>\|sort` (right-filter appended when present) |
| `derived-table-scan`     | Subquery in the `FROM` clause | `alias=<alias>` |
| `window`                 | The window functions of a `SELECT`, one entry per distinct `(PARTITION BY, ORDER BY)`. Under `ANALYZE`, the detail adds `partitions=<n> rows=<n> spilled=<n>` ([window-functions.md](./window-functions.md#explain)) | `partition=[…] order=[…] functions=[…]` |
| `function-scan`          | A set-returning function in the `FROM` clause, such as `generate_series` (no table access). With constant arguments, `estimated_rows` is the exact row count | `<function>(<args>), alias=<alias>, column=<column>` |
| `constant-source`        | The single synthetic row of a **FROM-less** `SELECT` (no table access) | `1 row` |
| `union`                  | `UNION [ALL]` | `all`, or `distinct: hash` (`, spill` when spill is enabled) |
| `intersect`, `except`    | `INTERSECT [ALL]`, `EXCEPT [ALL]` | `hash` or `all, hash` (`, spill` when spill is enabled) |
| `operand`                | One operand of the set operation above it; the rows that follow are that operand's plan. Under `ANALYZE`, `actual_rows` is the rows the operand returned | `left` or `right` |
| `lock_rows`              | `SELECT … FOR UPDATE`: locks each candidate row, reads it again and checks the `WHERE` clause again. Under `ANALYZE`, the detail adds `locked=<n>, skipped-held=<n>, recheck-dropped=<n>` (see below) | `for update`, `for update, nowait` or `for update, skip locked` |

Notes:
- A set operation renders each operand's plan under an `operand` row; see
  [`docs/set-operations.md`](./set-operations.md#explain). A FROM-less operand reads a one-row
  `function-scan(*single_row*(), …)`.
- Uncorrelated `IN`/`NOT IN` over an **indexed** inner column become a `semi-join` /
  `anti-join` / `null-aware-anti-join`. When the inner column is **not** indexed, the subquery
  is materialized instead and no join node appears.
- A `distinct` row reports `streaming: true` when the input arrives in index order covering
  all (NOT NULL) DISTINCT columns; otherwise `hash`.
- `hash-join` `build=<alias>` names the side materialised into the in-memory hash table; the
  planner picks the smaller estimated side as the build side to minimise memory. For a left outer
  join the build side is always the right table, so the preserved side is the probe.
- Every join node appends `kind=left-outer` for a `LEFT [OUTER] JOIN` (a `RIGHT JOIN` is executed as
  a left join with the operands swapped). An inner join renders no kind. Example:

  ```text
  hash-join(on=e.game_id=id, build=g, kind=left-outer)
    table-scan(table=environments)
  ```

  See [`docs/joins.md`](./joins.md) for the contract and the pushdown rule the plan reflects.
- `merge-join` streams both inputs when both arrive pre-ordered (ForcedIndex scan or an upstream
  sort); only the current equal-key run is buffered — O(run size) memory, not O(n+m).
- **Where a join predicate appears.** For an inner-join tree the planner places each predicate
  conjunct at the join where the last of its tables enters, not where the query text writes it
  ([`query-planner.md` → Cost-based join-order enumeration](./query-planner.md#cost-based-join-order-enumeration)).
  A conjunct can therefore appear on an earlier join than the `ON` it was written in, and a
  cross-table `WHERE` conjunct such as `c.z = a.z` can appear on a join instead of after the last
  join. `nested-loop-join` and `index-nested-loop-join` render the whole `on=` predicate of the join.
  `hash-join` and `merge-join` render only their key pairs: an equi-conjunct that moved into the join
  shows up as one more key (`on=b.y=y, a.z=z`), and a conjunct that is not a key is evaluated in the
  join but is not shown. The filter that runs after the last join is not a plan node, so it is not
  shown either. A `nested-loop-join(on=true, …)` is a cross product: the planner keeps one only when
  the predicates need it (no conjunct connects the two sides).

> **The plan reflects the active planner config.** Which access path a table uses, and the order
> of join nodes, depend on whether the cost-based optimizer is enabled. By default
> `cost_based_access_path_enabled` and `cost_based_join_order_enabled` are on; with them off, `EXPLAIN`
> shows the heuristic plan. With statistics collected (run `ANALYZE <table>`) the cost-based passes
> can show a different (cheaper) index or join order than without — the output is correct for
> whatever configuration produced it. See
> [`docs/query-planner.md` → Part V](./query-planner.md#part-v--the-cost-based-optimizer).

The examples below focus on the `stage` / `node` / `detail` columns (the stable part of the
output). Every row also carries `estimated_rows` / `estimated_cost` as described above; the
`EXPLAIN ANALYZE` examples show the full column set.

## Trailing informational rows

After the plan-node rows, `EXPLAIN` may append a few informational rows. They are not plan
operators — `estimated_rows` / `estimated_cost` are `NULL` — but they surface plan-level facts.

| Node name   | When it appears | Detail |
|-------------|-----------------|--------|
| `plan-info` | When the plan carries a shape id (plan-cache metadata) | `shape=<id>, schema-deps=[table@version, ...]` |
| `cache`     | When the query carries a `{cache=…}` (or `@{cache=…}`) result-cache hint | `family=<name>, eligible=<true\|false>[, reason=<why>], ttl=<n>ms\|default, strict=<true\|false>` |

The `cache` row answers **"will this result be cached?"** as a static plan property — it does
**not** probe the cache or reveal whether a live entry currently exists (that is inherently racy;
the authoritative run-time outcome is in the query response's `cacheStatus`). `eligible=true` means
the query is a cacheable shape and the feature is on. `eligible=false` names why the hint will be
ignored:

- `reason=join` — the query is a join; only single-table results are cached, so the hint is inert.
- `reason=cache-disabled` — the result cache is off (`query_result_cache_enabled: false`).

`ttl` and `strict` echo the hint's options (`ttl=default` when no `ttl=` was given). Eligibility
here is the plan-level view; at run time the cache additionally applies only to autocommit reads —
an explicit transaction always reads live storage. A query with no `{cache=…}` hint emits no
`cache` row. See [`docs/query-result-cache.md`](./query-result-cache.md) for the full feature.

---

## Worked examples

### Full table scan

```sql
EXPLAIN SELECT * FROM robots;
```

```
stage     node        detail
physical  table-scan  table=robots
```

The planner chose a full table scan — no usable index for the (empty) `WHERE` clause.

---

### Equality on a non-unique index → index-range-scan

```sql
EXPLAIN SELECT * FROM robots WHERE year = 2023;
```

(Assumes a non-unique `year_idx` on column `year`.)

```
stage     node               detail
physical  index-range-scan   index=year_idx, from>=2023, to<2024
```

For a **non-unique** index, an equality predicate is rewritten as a range scan. For numeric/ordinal
types it is half-open (`>= value`, `< successor(value)`), as shown above. When there is no computable
successor — a **`String`/`Id`/`date`/`datetime`/`float32`/`bytes`** column, a value at its type's
maximum (`true`, the largest `int64`), or a descending column — it uses an **inclusive
`[value, value]`** range instead
(`from>=value, to<=value`); the scan engine appends a high sentinel internally to capture every
`encode(value)+rowId` index entry and then trims to exact-key matches. Either way the result is the same
set of rows — only the bound rendering differs. The `index-lookup` node only appears for **unique** index
equality (primary key or `UNIQUE` constraint), where at most one row can match.

---

### Primary-key equality → index-lookup

```sql
EXPLAIN SELECT * FROM robots WHERE id = '507f1f77bcf86cd799439011';
```

```
stage     node          detail
physical  index-lookup  index=~pk, key='507f1f77bcf86cd799439011'
```

Because `~pk` is unique, the planner issues a single point-lookup rather than a range scan.

---

### Range scan with residual filter

```sql
EXPLAIN SELECT * FROM robots WHERE year >= 2020 AND name = 'Bishop';
```

```
stage     node               detail
physical  filter             name = 'Bishop'
physical  index-range-scan   index=year_idx, from>=2020
```

The `year >= 2020` predicate drives an index range scan; `name = 'Bishop'` cannot be pushed into
the index and appears as a residual `filter` above the scan.

> **Tree depth in result rows:** the `node` column contains the bare node name with no leading
> spaces. Parent-before-child row order conveys tree depth; indentation is not present in the
> actual result set.

---

### Aggregate with GROUP BY

```sql
EXPLAIN SELECT year, COUNT(*) FROM robots GROUP BY year;
```

```
stage     node        detail
physical  aggregate   group=[year], aggs=[count(*)]
physical  table-scan  table=robots
```

---

### SELECT DISTINCT — streaming vs hash

```sql
EXPLAIN SELECT DISTINCT code FROM teams;   -- code is NOT NULL with an index
```

```
stage     node              detail
physical  distinct          streaming: true
physical  index-range-scan  index=code_idx
```

When the DISTINCT columns form an index set-prefix and are all NOT NULL, the scan emits rows
in index order and `distinct` deduplicates adjacent rows with O(1) memory. Otherwise the
`distinct` row shows `hash` and a hash set is used.

---

### Hash join — equi-join with no index on the join key

```sql
EXPLAIN SELECT o.name, li.product
        FROM orders o
        JOIN line_items li ON li.order_id = o.id;
-- orders.id is the PK; line_items.order_id has no secondary index
```

```
stage     node        detail
physical  hash-join   on=o.id=order_id, build=li
physical  table-scan  table=orders
```

The plan walk follows the left input of each join, so the right input (`line_items`) has no row
of its own; the join line names it (`build=li`).

`build=li` means `line_items` is materialized into the in-memory hash table; `orders` is
streamed as the probe side. The planner chose `li` as the build side because it estimated
fewer rows than `orders`. When the build side exceeds `HashJoinMaxBuildRows` (default 1 000 000)
the executor falls back to nested-loop join for that query.

---

### Merge join — both sides have a secondary index on the join key

```sql
EXPLAIN SELECT o.name, li.product
        FROM orders o
        JOIN line_items li ON li.order_id = o.ext_key;
-- orders has index orders_ext_key_idx on (ext_key)
-- line_items has index li_order_id_idx on (order_id)
-- both sides estimated > 100 rows → cost model picks merge
```

```
stage     node              detail
physical  merge-join        on=o.ext_key=order_id, right-order=forced-index=li_order_id_idx
physical  table-scan        table=orders, forced-index=orders_ext_key_idx
```

Both sides use a `forced-index` scan so their rows arrive in join-key order: the left one has its
own row, and the right one is named by `right-order` on the join line (`right-order=sort` when the
right side is sorted instead). The executor streams
both sides simultaneously and buffers only the current equal-key run — no full materialization
of either side. `LeftIsOrdered = RightIsOrdered = true` on the `MergeJoinNode`.

---

### IN subquery rewritten to a semi-join

```sql
EXPLAIN SELECT * FROM robots WHERE owner_id IN (SELECT id FROM owners);
```

```
stage     node        detail
physical  semi-join   outer=owner_id, inner=owners.id, index=~pk
physical  table-scan  table=robots
```

Because the inner column `owners.id` is indexed, the `IN` is executed as an index-probing
semi-join instead of materializing the subquery. `NOT IN` produces `anti-join`
(NOT NULL inner column) or `null-aware-anti-join` (nullable inner column). When the inner
column is not indexed, no join node appears — the subquery is materialized.

---

### ORDER BY satisfied by an index (sort elision)

```sql
EXPLAIN SELECT * FROM robots ORDER BY year;
```

```
stage     node              detail
physical  index-range-scan  index=year_idx
```

No `sort` node appears: the index scan already guarantees the requested ordering, so the sort
is elided.

The same holds for streaming grouping and streaming `DISTINCT`. An `aggregate(... streaming: true)`
emits its groups in the order of the index it reads, so `ORDER BY` on the grouping columns in that
order and direction (or on their output aliases) needs no sort:

```sql
EXPLAIN SELECT status, COUNT(*) AS cnt FROM sales GROUP BY status ORDER BY status LIMIT 20;
```

There is no `sort` row, and the `LIMIT` stops the scan after 20 groups. `ORDER BY` in the other
direction, or on an aggregate (`ORDER BY cnt`), keeps the `sort`.

---

### LIMIT pushdown

```sql
EXPLAIN SELECT * FROM robots LIMIT 10;
```

```
stage     node        detail
physical  limit       10
physical  table-scan  table=robots
```

The scan stops reading after the first 10 rows rather than scanning the entire table.

---

### EXPLAIN ANALYZE — full table scan

```sql
EXPLAIN (ANALYZE) SELECT * FROM robots;
```

```
stage    node        detail        estimated_rows  estimated_cost  actual_rows  rows_read  actual_time_ms  kv_lookups  kv_scan_entries
analyze  table-scan  table=robots  42              42.0            42           42         3.1             0           42
```

The table contains 42 rows, no filter, so `actual_rows` = `rows_read` = `kv_scan_entries` =
42. `actual_time_ms` is only populated on the **root** node (the outermost operator).

---

### EXPLAIN ANALYZE — non-unique index range scan with a limit

```sql
EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year = 2022 LIMIT 5;
```

(Assumes a non-unique `year_idx` on `year`; 3 robots have `year = 2022`.)

```
stage    node               detail                                estimated_rows  estimated_cost  actual_rows  rows_read  actual_time_ms  kv_scan_entries  kv_lookups
analyze  limit              5                                     5               6.0             3            0          14.2             0                0
analyze  index-range-scan   index=year_idx, from>=2022, to<2023   ...             ...             3            3          NULL             3                0
```

- `limit` node: emits 3 rows (fewer than the cap of 5); `actual_time_ms` = 14.2 ms (total
  plan time, root node only).
- `index-range-scan` node: 3 `kv_scan_entries` (index entries in `[2022, 2023)`), 3
  `rows_read` (rows fetched), 3 `actual_rows` (all passed; no residual predicate).

(`estimated_*` are cost-model estimates and vary with collected statistics.)

---

### EXPLAIN ANALYZE — a join chain

JOB query 1a on a small synthetic copy of the IMDB data, after `ANALYZE` of every table
(abridged detail, estimated cost and time columns left out):

```sql
EXPLAIN (ANALYZE)
SELECT MIN(mc.note), MIN(t.title), MIN(t.production_year)
FROM company_type AS ct, info_type AS it, movie_companies AS mc, movie_info_idx AS mi_idx, title AS t
WHERE ct.kind = 'production companies' AND it.info = 'top 250 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')
  AND ct.id = mc.company_type_id AND t.id = mc.movie_id AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id AND it.id = mi_idx.info_type_id;
```

```
node                    detail (abridged)                                            estimated_rows  actual_rows  rows_read  kv_lookups  kv_scan_entries
index-nested-loop-join  index=~pk, right=id, join-order-est-rows=6                   1               6            6          6           0
index-nested-loop-join  index=~pk, right-filter=it.info = 'top 250 rank',
                        join-order-est-rows=6                                        1               6            88         88          0
index-nested-loop-join  index=movie_id_movie_info_idx, join-order-est-rows=63        1               88           88         0           142
index-nested-loop-join  index=company_type_id_movie_companies,
                        right-filter=NOT mc.note LIKE '...' AND (... OR ...),
                        join-order-est-rows=65                                       1               54           1162       0           1163
table-scan              table=company_type                                           1               1            4          0           4
```

Read it from the bottom. One company type passes its filter. The first join probes the
non-unique `company_type_id_movie_companies` index for it, visits 1163 entries, fetches 1162
rows and keeps the 54 that pass the right-side filter. The plan annotation estimated 1 row at
every step, because it estimates one row for each outer row of an index-nested-loop join. The
join-order search estimated 65, 63, 6 and 6, which is much closer to the actual 54, 88, 6 and 6.
The `it` join fetches 88 rows by primary key and keeps 6: its filter runs after the fetch.

---

## Verbose / distributed-properties mode

`PlanRenderer.Render(plan, includeDistributedProperties: true)` appends distribution
metadata to each node line:

```
table-scan(table=robots) order=[year ASC] decomposable=true dist=partitioned(id)
```

| Suffix            | Meaning |
|-------------------|---------|
| `order=[...]`     | The ordering this node guarantees on its output (e.g. an index scan that satisfies `ORDER BY`). Absent when ordering is undefined. |
| `decomposable=true/false` | Whether the node's work can be split into per-partition local computation plus a coordinator-side merge. Always `false` for sort and limit nodes. `aggregate` is `true` only for `COUNT`/`SUM`/`MIN`/`MAX`; `AVG` is `false`. |
| `dist=...`        | How this node's output rows are distributed across the cluster: `gathered` (single-node / point lookup / sharding off), `partitioned(col1,col2)` (range-sharded by these key columns), or `replicated`. Set only on scan leaves; absent on pipeline nodes. With key-range sharding off, every scan is `gathered`. |

This mode is used by internal tooling and tests; it is not exposed through the SQL `EXPLAIN`
statement.

When `distributed_query_execution` is enabled, ordinary `EXPLAIN` instead shows a `gather`
node for an eligible multi-span primary-row scan and a `distribution` row with
`distributed=yes`. If the scan does not qualify, the row says `distributed=no` and gives the
reason. A gather can run eligible fragments on peer leaders; its presence alone does not mean
every span was dispatched remotely. `EXPLAIN (ANALYZE)` adds a `gather-span` row per span to
show which spans ran remotely, how many rows the peer scanned and shipped, and whether the span
fell back to local execution. A hash join that ran as a broadcast probe adds a `broadcast-span`
row per probed span, with the same detail; for a remote span, `actual_rows` counts only the
probe rows that matched, because the peer ships nothing else. Partial aggregation is disabled under `ANALYZE`, so the reported
plan is the row-gather path that actually ran. See [query planning](query-planner.md#part-iv--distributed-ready-plan-properties)
for eligibility and operator limits.

---

## Notes on filter stats in EXPLAIN ANALYZE

A `filter` is **folded into the scan** during execution: the predicate is evaluated inside the
scan loop, not as a separate pipeline stage. As a result:

- `filter` rows in `EXPLAIN ANALYZE` show `actual_rows` equal to the scan's post-filter emit
  count (the rows that passed the predicate).
- `kv_lookups` and `kv_scan_entries` are `0` on the filter row — all storage costs are
  attributed to the scan node directly below.
- `actual_time_ms` is `NULL` (filter evaluation time is not separately measured; it is
  included in the scan's wall clock, reported only on the root node). When the filter is the
  root node, it carries the time of the whole plan, as every root node does.
