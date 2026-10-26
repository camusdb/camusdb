# Statement statistics — `SHOW STATEMENT STATS`

CamusDB can keep running totals for every statement shape a node runs, and hand them back as a SQL
result set:

```sql
SHOW STATEMENT STATS;
SHOW STATEMENT STATS LIKE '%FROM orders%';
```

It is the CamusDB form of PostgreSQL's `pg_stat_statements` and CockroachDB's statement statistics.
The feature is **off by default** and **bounded in memory**. See [Limits](#limits) before you build
anything on it.

## What it is for

The [slow query log](slow-query-log.md) shows single statements that crossed a threshold. It cannot
show a statement that is fast each time but runs a million times an hour, and it cannot show that a
statement became slower than it was yesterday. Statement statistics answer those questions:

- Which statements use the most time in total (`total_ms`)?
- Which statements read far more rows than they return (`rows_read` against `rows_returned`)?
- Which statements scan whole tables (`full_scans`) or spill to disk (`spills`)?
- Which statements fail, and which fail with a code the client must retry (`errors`, `retries`)?
- Which statements changed their plan (`plan_changes`)?
- Which statements have two personalities — fast on most runs, slow on some (`cv`)?

## What is recorded

`SELECT`, `INSERT`, `UPDATE` and `DELETE`, including the `RETURNING` forms and `INSERT … SELECT`.
Schema changes, transaction control and the `SHOW` family are not recorded. That is also why reading
the statistics never changes them.

A statement is recorded when it ends: when its cursor is drained, when it fails, or when the client
stops reading. This is the same point at which the slow query log records a statement, and the two
features share one set of execution counters.

### Shapes

Executions are grouped by **shape**, a fingerprint of the statement with its literals removed:

- Every literal (number, string, boolean, `NULL`, bytes, object id) and every placeholder becomes `?`.
  `WHERE id = 1`, `WHERE id = 2` and `WHERE id = @id` are one shape.
- A list made only of constants becomes one `?*`. An `IN (…)` list of 3 values and one of 300 values
  are one shape. A multi-row `VALUES` list of constants is one shape at any length, so batch inserts
  of different sizes share a row.
- Identifiers are kept, and are compared without case, like the engine compares them.

The shape id is a 16-character hex digest. It is the same on every node and after a restart, for the
same build. The key of a row is the database plus the shape id.

The shape comes from the parse tree, not from a plan. Every recorded statement has a parse tree on
every configuration, including when the plan cache is off. The fingerprint is computed once per
distinct statement text and kept with the cached parse tree, so an application that sends
parameterized statements computes it once.

## Turning it on

```yml
statement_stats_enabled: true
statement_stats_max_shapes: 2000
```

| Setting | Default | What it does |
|---|---|---|
| `statement_stats_enabled` | `false` | Master switch. Runtime-mutable: `SET CLUSTER SETTING statement_stats_enabled = true` affects the next statement. Turning it off stops recording and keeps the totals. |
| `statement_stats_max_shapes` | `2000` | Shapes held at most. Restart-class: the table is sized when the engine starts. |

The sample text of each shape is cut to `slow_query_log_max_sql_length`, so one setting bounds the
statement text that both features hold. Both settings are node-scoped.

## Result columns

Every additive column is a **running total** since the shape was first seen on this node. To measure
a window, read the statement twice and subtract.

| Column | Type | Meaning |
|---|---|---|
| `node` | `STRING` | The node that answered. |
| `database` | `STRING` | The database of the statement. |
| `shape_id` | `STRING` | The shape fingerprint, 16 hex characters. |
| `kind` | `STRING` | `select`, `insert`, `update` or `delete`. |
| `sample_sql` | `STRING` | The text of the first statement seen for the shape, with any password literal replaced by `'***'`, then cut to `slow_query_log_max_sql_length`. |
| `calls` | `INT64` | Executions that ended. |
| `errors` | `INT64` | Executions that failed. |
| `total_ms` | `FLOAT64` | Sum of the durations. |
| `sum_sq_ms` | `FLOAT64` | Sum of the squared durations. With `total_ms` and `calls`, it gives the variance of any window: `(Δsum_sq / Δcalls) − (Δtotal / Δcalls)²`. |
| `min_ms`, `max_ms` | `FLOAT64` | Shortest and longest duration since the shape was first seen. |
| `mean_ms`, `stddev_ms`, `cv` | `FLOAT64` | Mean, standard deviation and coefficient of variation (stddev ÷ mean), derived from the totals when the statement runs. They describe the whole life of the shape. |
| `rows_read` | `INT64` | Rows read from storage, with the slow query log's definition. |
| `rows_returned` | `INT64` | Rows returned, or rows affected for a write. |
| `full_scans` | `INT64` | Executions in which some part read a whole relation. |
| `spills` | `INT64` | Executions in which a blocking operator wrote to a spill file. |
| `retries` | `INT64` | Failed executions whose error code means "replay from `BEGIN`": `CADB0502`, `CADB0504`, `CADB0505`. See [the retry contract](serializable-retry-contract.md). |
| `plan_changes` | `INT64` | Executions whose plan was different from the plan of the execution before. |
| `last_plan` | `STRING` | A hash of the last plan, or `NULL` for a shape that never built a plan (an `INSERT … VALUES`). |
| `first_seen_at`, `last_seen_at` | `STRING` | ISO-8601 UTC. |

Rows come back sorted by `cv`, highest first. A statement whose duration has two regimes — a plan that
changes, a cache that is sometimes cold — has a high `cv`, and it is the statement to look at first.

### What `plan_changes` compares

The plan hash covers the decisions that change how data is read: the plan's node kinds, the table that
each scan reads, whether a scan reads the rows or an index, and which index. It does not include
bounds, literals or estimates. Two executions that differ only in their literals have the same hash.
A new index or a fresh `ANALYZE` that changes an access path gives a different hash.

The hash is taken when the plan is built, not from what the execution touched. An execution whose
outer side was empty never probes its inner index, and that is a property of the data, not of the plan.

## Worked example

```sql
SHOW STATEMENT STATS LIKE '%orders%';
```

| kind | calls | total_ms | mean_ms | cv | rows_read | rows_returned | full_scans | plan_changes | sample_sql |
|---|---|---|---|---|---|---|---|---|---|
| select | 48210 | 96120.4 | 1.99 | 2.71 | 115704000 | 48210 | 48210 | 0 | SELECT * FROM orders WHERE customer_id = 17 |
| update | 1203 | 3110.9 | 2.58 | 0.41 | 1203 | 1203 | 0 | 0 | UPDATE orders SET status = 2 WHERE id = 9 |

The first shape is fast on each run, so the slow query log never sees it. It still uses 96 seconds of
the node's time, reads 2,400 rows for each row it returns, and scans the whole table on every run:
`customer_id` has no index. Compare [`SHOW INDEX USAGE`](index-usage.md) for the other side of the
same question.

## Privileges

`SHOW STATEMENT STATS` requires a **superuser**, the same bar as `SHOW SLOW QUERIES`. The sample text
holds the literals of whoever ran the shape first.

## Reading the window correctly

- **Totals are cumulative since the shape was first seen.** A shape appears with zero totals the first
  time it runs. Subtract two readings for a rate.
- **The table never evicts.** An eviction would reset a shape's totals, and a reader that subtracts two
  readings would see a negative window. When the table is full, a new shape is not admitted, and the
  counter `statement_stats.dropped` (source `camusdb`) in [`SHOW ENGINE STATS`](engine-stats.md)
  counts it. `statement_stats.shapes` is the number of shapes held. A `dropped` counter that moves is
  the signal to raise `statement_stats_max_shapes`.

## Limits

1. **It does not survive a restart.** The totals live in memory. Keep readings outside the node if you
   need history.
2. **It is one node's view.** The statement reports the node that answered it. Inspect a cluster one
   node at a time, and sum the rows if you need a cluster total.
3. **It is bounded.** At most `statement_stats_max_shapes` shapes; see `statement_stats.dropped`.
4. **The sample is the first text seen**, not the slowest or the latest one.

## Related

- [Slow query log](slow-query-log.md) — single statements over a threshold, with their full text.
- [Index usage](index-usage.md) — `SHOW INDEX USAGE`, which indexes are read and which are not.
- [`EXPLAIN` and `EXPLAIN ANALYZE`](explain.md) — the plan of one statement.
- [`SHOW ENGINE STATS`](engine-stats.md) — node-level metrics, including `statement_stats.dropped`.
