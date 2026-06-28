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
```

`EXPLAIN` without `ANALYZE` builds the physical plan but does **not** open any table cursor
or read any row data. `EXPLAIN (ANALYZE)` executes the full query, drains the result, and
reports actual runtime counters alongside the estimated columns.

An unrecognized option word (e.g. `EXPLAIN (VERBOSE) ...`) is rejected with an error rather
than silently treated as a plain `EXPLAIN`.

> **Join limitation:** `EXPLAIN (ANALYZE)` is not yet supported for queries with `JOIN` — it
> raises an error. Use plain `EXPLAIN` to inspect join plans; full join instrumentation is
> planned for a future release.

> **Subquery note:** planning a statement that contains an uncorrelated subquery executes that
> inner subquery once (it is materialized during planning), so `EXPLAIN` of such a statement
> does read storage for the inner query. The outer query is never executed by plain `EXPLAIN`.

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
on what statistics have been collected and will differ between deployments. `estimated_cost` also
includes a network term (`NetworkFactor`) for range-sharded deployments; it is 0 on a single node.
Single-table estimates are accurate; join-node estimates are accurate when the cost-based
join-order flag is on and otherwise remain heuristic. `(LOGICAL)` and `(PHYSICAL)` currently
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

`actual_rows` ≤ `rows_read` for scan nodes: `rows_read` counts rows decoded from storage
before predicate evaluation; `actual_rows` counts rows that passed all filters and were
yielded upstream.

---

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
| `nested-loop-join`       | Inner join without a usable index on the right side | `on=<expr>, right=<alias>` |
| `index-nested-loop-join` | Inner join where the right side's join key is indexed | `on=<expr>, index=<name>, left=<col>, right=<col>` |
| `hash-join`              | Inner equi-join using an in-memory hash table; chosen over INLJ when the outer side is large relative to the inner | `on=<left>=<right>, build=<alias>` (build-filter appended when a pushed-down filter is present) |
| `merge-join`             | Inner equi-join using a streaming two-pointer merge; chosen when both sides have free index ordering on the join key | `on=<left>=<right>` (right-filter appended when present) |
| `derived-table-scan`     | Subquery in the `FROM` clause | `alias=<alias>` |

Notes:
- Uncorrelated `IN`/`NOT IN` over an **indexed** inner column become a `semi-join` /
  `anti-join` / `null-aware-anti-join`. When the inner column is **not** indexed, the subquery
  is materialized instead and no join node appears.
- A `distinct` row reports `streaming: true` when the input arrives in index order covering
  all (NOT NULL) DISTINCT columns; otherwise `hash`.
- `hash-join` `build=<alias>` names the side materialised into the in-memory hash table; the
  planner picks the smaller estimated side as the build side to minimise memory.
- `merge-join` streams both inputs when both arrive pre-ordered (ForcedIndex scan or an upstream
  sort); only the current equal-key run is buffered — O(run size) memory, not O(n+m).

> **The plan reflects the active planner config.** Which access path a table uses, and the order
> of join nodes, depend on whether the cost-based optimizer is enabled. By default the planner is
> rule-based, so `EXPLAIN` shows the heuristic plan. With `cost_based_access_path_enabled` and/or
> `cost_based_join_order_enabled` turned on **and** statistics collected (run `ANALYZE <table>`),
> the same query may show a different (cheaper) index or join order — the output is correct for
> whatever configuration produced it. See
> [`docs/query-planner.md` → Part V](./query-planner.md#part-v--the-cost-based-optimizer).

The examples below focus on the `stage` / `node` / `detail` columns (the stable part of the
output). Every row also carries `estimated_rows` / `estimated_cost` as described above; the
`EXPLAIN ANALYZE` examples show the full column set.

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

For a **non-unique** index, an equality predicate is rewritten as a half-open range scan
(`>= value`, `< successor(value)`). The `index-lookup` node only appears for **unique**
index equality (primary key or `UNIQUE` constraint), where at most one row can match.

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
physical  table-scan  table=line_items
```

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
physical  merge-join        on=o.ext_key=order_id
physical  table-scan        table=orders, forced-index=orders_ext_key_idx
physical  table-scan        table=line_items, forced-index=li_order_id_idx
```

Both scans use a `forced-index` so their rows arrive in join-key order. The executor streams
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

## Verbose / distributed-properties mode

`PlanRenderer.Render(plan, includeDistributedProperties: true)` appends distributed-ready
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

---

## Notes on filter stats in EXPLAIN ANALYZE

A `filter` is **folded into the scan** during execution: the predicate is evaluated inside the
scan loop, not as a separate pipeline stage. As a result:

- `filter` rows in `EXPLAIN ANALYZE` show `actual_rows` equal to the scan's post-filter emit
  count (the rows that passed the predicate).
- `kv_lookups` and `kv_scan_entries` are `0` on the filter row — all storage costs are
  attributed to the scan node directly below.
- `actual_time_ms` is `NULL` (filter evaluation time is not separately measured; it is
  included in the scan's wall clock, reported only on the root node).
