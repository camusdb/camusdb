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

> **Join limitation (R7):** `EXPLAIN (ANALYZE)` is not yet supported for queries with
> `JOIN`. Use plain `EXPLAIN` to inspect join plans; `EXPLAIN (ANALYZE)` on a join raises
> an error. Full join instrumentation is planned for R7.

---

## Result-row schema

### Plain EXPLAIN (R3)

| Column           | Type      | Description |
|------------------|-----------|-------------|
| `stage`          | `STRING`  | `"physical"` for `EXPLAIN` / `EXPLAIN (PHYSICAL)`, `"logical"` for `EXPLAIN (LOGICAL)` |
| `node`           | `STRING`  | Canonical node name (see table below) |
| `detail`         | `STRING`  | Node-specific key facts (see table below) |
| `estimated_rows` | `NULL`    | Reserved for the R9 cost model; always `NULL` today |
| `estimated_cost` | `NULL`    | Reserved for the R9 cost model; always `NULL` today |

### EXPLAIN ANALYZE (R5)

All columns from plain `EXPLAIN`, plus:

| Column            | Type       | Description |
|-------------------|------------|-------------|
| `actual_rows`     | `INT64`    | Rows emitted (yielded) by this operator to its parent. `NULL` for operators without instrumentation. |
| `rows_read`       | `INT64`    | Rows fetched and decoded from storage before filtering (scan operators only). `0` for pipeline operators (they read from a cursor, not storage). |
| `actual_time_ms`  | `FLOAT64`  | Total wall-clock milliseconds for the **entire plan** (root node only). `NULL` on all other nodes (per-node timing is planned for a future release). |
| `kv_lookups`      | `INT64`    | KV point-lookups issued (unique-index lookups). |
| `kv_scan_entries` | `INT64`    | KV scan entries visited (table or range-index scans). |

`actual_rows` ≤ `rows_read` for scan nodes: `rows_read` counts rows decoded from storage
before predicate evaluation; `actual_rows` counts rows that passed all filters and were
yielded upstream.

---

## Canonical node names

One row is emitted per physical plan node, in depth-first order (parent before children).

| Node name              | When it appears | Key detail fields |
|------------------------|-----------------|-------------------|
| `table-scan`           | Full table scan or forced-index scan | `table=<name>` (forced-index adds `, forced-index=<name>`) |
| `index-lookup`         | Equality predicate on an indexed column(s) | `index=<name>, key=<value>` |
| `index-range-scan`     | Range predicate (`<`, `>`, `BETWEEN`, prefix) on an index | `index=<name>, from>=<val>, to<<val>` |
| `filter`               | Residual predicate not satisfied by the chosen index | `filter(<expr>)` |
| `aggregate`            | `GROUP BY` or aggregate functions | `group=[<exprs>], aggs=[<calls>]` |
| `having-filter`        | `HAVING` clause applied after aggregation | `having-filter(<expr>)` |
| `sort`                 | `ORDER BY` not satisfied by scan ordering | `sort(<col> ASC/DESC, ...)` |
| `limit`                | `LIMIT` / `OFFSET` | `limit(<n>)` or `limit(<n> offset <m>)` |
| `project`              | Column projection (non-`SELECT *` after other pipeline stages) | _(no detail)_ |
| `distinct`             | `SELECT DISTINCT` | _(no detail)_ |
| `nested-loop-join`     | Inner join without a usable index on the right side | `on=<expr>, right=<alias>` |
| `index-nested-loop-join` | Inner join where the right side's join key is indexed | `on=<expr>, index=<name>, left=<col>, right=<col>` |
| `derived-table-scan`   | Subquery in the `FROM` clause | `alias=<alias>` |

---

## Worked examples

### Full table scan

```sql
EXPLAIN SELECT * FROM robots;
```

```
stage     node        detail              estimated_rows  estimated_cost
physical  table-scan  table=robots        NULL            NULL
```

The planner chose a full table scan — no usable index for the (empty) `WHERE` clause.

---

### Equality on a non-unique index → index-range-scan

```sql
EXPLAIN SELECT * FROM robots WHERE year = 2023;
```

(Assumes a non-unique `year_idx` on column `year`.)

```
stage     node               detail                                         estimated_rows  estimated_cost
physical  index-range-scan   index=year_idx, from>=2023, to<2024            NULL            NULL
```

For a **non-unique** index, an equality predicate is rewritten as a half-open range scan
(`>= value`, `< successor(value)`). The `index-lookup` node only appears for **unique**
index equality (primary key or `UNIQUE` constraint), where the planner knows at most one
row can match.

No `filter` row appears because the index range fully covers the predicate.

---

### Primary-key equality → index-lookup

```sql
EXPLAIN SELECT * FROM robots WHERE id = '507f1f77bcf86cd799439011';
```

(The primary-key index `~pk` is unique.)

```
stage     node          detail                                                   estimated_rows  estimated_cost
physical  index-lookup  index=~pk, key='507f1f77bcf86cd799439011'               NULL            NULL
```

Because `~pk` is a unique index, the planner issues a single point-lookup rather than a
range scan. One KV read suffices to locate the row.

---

### Range scan with residual filter

```sql
EXPLAIN SELECT * FROM robots WHERE year >= 2020 AND name = 'R2';
```

```
stage     node               detail                      estimated_rows  estimated_cost
physical  filter             name = 'R2'                 NULL            NULL
physical  index-range-scan   index=year_idx, from>=2020  NULL            NULL
```

The `year >= 2020` predicate drives an index range scan; `name = 'R2'` cannot be pushed
into the index and appears as a residual `filter` above the scan.

> **Tree depth in result rows:** the `node` column contains the bare node name with no
> leading spaces. Parent-before-child row order conveys tree depth; indentation is not
> present in the actual result set.

---

### Aggregate with GROUP BY

```sql
EXPLAIN SELECT year, COUNT(*) FROM robots GROUP BY year;
```

```
stage     node        detail                            estimated_rows  estimated_cost
physical  aggregate   group=[year], aggs=[count(*)]    NULL            NULL
physical  table-scan  table=robots                     NULL            NULL
```

---

### ORDER BY satisfied by an index (sort elision)

```sql
EXPLAIN SELECT * FROM robots ORDER BY year;
```

```
stage     node              detail              estimated_rows  estimated_cost
physical  index-range-scan  index=year_idx      NULL            NULL
```

No `sort` node appears: the index scan already guarantees the requested ordering
(`OutputOrdering` is set on the scan node; the planner elides the `SortNode`).

---

### LIMIT pushdown

```sql
EXPLAIN SELECT * FROM robots LIMIT 10;
```

```
stage     node        detail         estimated_rows  estimated_cost
physical  limit       limit(10)      NULL            NULL
physical  table-scan  table=robots   NULL            NULL
```

The scan respects a `ScanRowLimit` pushed down from the planner — it stops reading after
the first 10 rows rather than scanning the entire table.

---

### EXPLAIN ANALYZE — full table scan

```sql
EXPLAIN (ANALYZE) SELECT * FROM robots;
```

```
stage    node        detail          estimated_rows  estimated_cost  actual_rows  rows_read  actual_time_ms  kv_lookups  kv_scan_entries
analyze  table-scan  table=robots    NULL            NULL            42           42         NULL            0           42
```

`actual_rows` = `rows_read` = `kv_scan_entries` = 42 (the table contains 42 rows, no
filter). `actual_time_ms` is `NULL` on the scan row — it is only populated on the **root**
node (the outermost operator in the plan).

---

### EXPLAIN ANALYZE — non-unique index range scan with a limit

```sql
EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year = 2022 LIMIT 5;
```

(Assumes a non-unique `year_idx` on `year`; 3 robots have `year = 2022`.)

```
stage    node               detail                                    estimated_rows  estimated_cost  actual_rows  rows_read  actual_time_ms  kv_scan_entries  kv_lookups
analyze  limit              limit(5)                                  NULL            NULL            3            0          14.2             0                0
analyze  index-range-scan   index=year_idx, from>=2022, to<2023      NULL            NULL            3            3          NULL             3                0
```

- `limit` node: emits 3 rows (fewer than the cap of 5); `actual_time_ms` = 14.2 ms (total
  plan time, root node only).
- `index-range-scan` node: 3 `kv_scan_entries` (index entries visited in the `[2022, 2023)`
  range), 3 `rows_read` (rows fetched from storage), 3 `actual_rows` (all passed the
  filter, since no residual predicate exists).

---

## Verbose / distributed-properties mode

`PlanRenderer.Render(plan, includeDistributedProperties: true)` appends R4 metadata to each
node line:

```
table-scan(table=robots) order=[year ASC] decomposable=true
```

| Suffix            | Meaning |
|-------------------|---------|
| `order=[...]`     | `OutputOrdering` — the ordering this node guarantees on its output (e.g. an index scan that satisfies `ORDER BY`). Absent when ordering is undefined. |
| `decomposable=true/false` | Whether the node's work can be split into per-partition local computation plus a coordinator-side merge. Always `false` for sort and limit nodes. `AggregateNode` is `true` only for `COUNT`/`SUM`/`MIN`/`MAX`; `AVG` is `false`. |

This mode is used by internal tooling and tests; it is not exposed through the SQL
`EXPLAIN` statement.

---

## Notes on FilterNode stats in EXPLAIN ANALYZE

`FilterNode` is **folded into the scan** during execution: the predicate is evaluated
inside the scan loop via `QueryPlan.ExecutionFilter`, not as a separate pipeline stage.
As a result:

- `filter` rows in `EXPLAIN ANALYZE` output show `actual_rows` equal to the scan's
  post-filter emit count (the rows that passed the predicate).
- `kv_lookups` and `kv_scan_entries` are `0` on the filter row — all storage costs are
  attributed to the scan node directly below.
- `actual_time_ms` is `NULL` (filter evaluation time is not separately measured; it is
  included in the scan's wall clock, which is itself only reported on the root node).
