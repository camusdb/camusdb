# `SHOW STATISTICS FOR <table>`

CamusDB's cost-based optimizer prices plans from per-table statistics: a row count, per-column
minimum and maximum values, equi-depth histograms, and approximate distinct-value counts. Those
values decide whether a query uses an index or scans, and which side of a join is built. This
statement shows them.

```sql
SHOW STATISTICS FOR robots;
SHOW STATISTICS FOR TABLE robots;   -- TABLE is an optional noise word
```

It answers three questions that `EXPLAIN` alone cannot: has `ANALYZE` actually run on this table,
what does the optimizer believe about the data, and how stale has that belief become.

This is table statistics, not engine introspection — for Kahuna/Kommander runtime metrics see
[`SHOW ENGINE STATS`](engine-stats.md).

## Result columns

One row per statistics *target*, discriminated by `kind`.

| Column | Type | Meaning |
|--------|------|---------|
| `table` | string | The table the statistics belong to. |
| `kind` | string | `table`, `column`, `key`, or `index` — what this row describes. |
| `target` | string | NULL on the `table` row; the column name, the key-tuple signature, or the index name. |
| `estimated_rows` | int64 | Estimated rows in the table; on an `index` row, that index's entry count. |
| `distinct_count` | int64 | Approximate distinct values (NDV) for a `column` or `key` row. |
| `min_value` | string | Smallest value observed in the column. |
| `max_value` | string | Largest value observed in the column. |
| `histogram_buckets` | int64 | Number of equi-depth buckets in the column's histogram. |
| `last_analyzed` | string | When `ANALYZE` last read this table; NULL if it never has. |
| `stale_mutations` | int64 | Row mutations since that `ANALYZE`. |

`last_analyzed` and `stale_mutations` describe the whole table and repeat on every row, so any single
row tells you how much to trust the rest.

A NULL means one of two things, and the `kind` column tells you which: the value does not apply to
that row (`distinct_count` on an `index` row), or it has never been collected (`histogram_buckets`
before any `ANALYZE`).

### Row kinds

- **`table`** — the table-wide counters. Always present, even when nothing has ever been collected,
  so "this table has no statistics" is a visible answer rather than an empty result.
- **`column`** — one per column carrying any estimate. Min/max is maintained on writes for indexed
  columns; `distinct_count` and `histogram_buckets` appear only after `ANALYZE`.
- **`key`** — one per composite-index key prefix, with the columns comma-joined (`city,zip`). These
  correct the optimizer's independence assumption for correlated equality predicates.
- **`index`** — one per index, with its approximate entry count.

Rows arrive grouped in that order, columns in schema order and keys and indexes ordered by name.

## A worked example

A freshly created table has nothing to report but says so:

```
> SHOW STATISTICS FOR robots;

table   kind   target  estimated_rows  distinct_count  min_value  max_value  histogram_buckets  last_analyzed  stale_mutations
robots  table  NULL    NULL            NULL            NULL       NULL       NULL               NULL           0
```

After inserting 20 rows — still without `ANALYZE` — the write-maintained statistics appear. The row
count comes from this node's live counters, so it is visible immediately, before any flush to
storage:

```
robots  table   NULL      20    NULL  NULL  NULL  NULL  NULL  20
robots  column  year      NULL  NULL  2000  2019  NULL  NULL  20
robots  index   ~pk       20    NULL  NULL  NULL  NULL  NULL  20
robots  index   year_idx  20    NULL  NULL  NULL  NULL  NULL  20
```

Note what is missing: no `distinct_count`, no histogram. The optimizer is costing this table with
fallback selectivities. `ANALYZE` fills them in and resets the staleness counter:

```
> ANALYZE robots;
> SHOW STATISTICS FOR robots;

robots  table   NULL      20    NULL  NULL  NULL  NULL  2026-08-15T…  0
robots  column  year      NULL  20    2000  2019  8     2026-08-15T…  0
robots  index   ~pk       20    NULL  NULL  NULL  NULL  2026-08-15T…  0
robots  index   year_idx  20    NULL  NULL  NULL  NULL  2026-08-15T…  0
```

## Reading it alongside `EXPLAIN`

When [`EXPLAIN`](explain.md) shows an `estimated_rows` you do not believe, this statement shows the
inputs that produced it:

- `col = v` is priced at `1 / distinct_count`. A `distinct_count` of NULL means the estimate fell
  back to a fixed constant, so an index may look far less attractive than it is.
- Range predicates are priced from the histogram. `histogram_buckets` of NULL means the same
  fallback.
- `WHERE a = ? AND b = ?` over a composite index uses the `key` row for `a,b` instead of multiplying
  two independent selectivities — which is what keeps correlated columns like city and postcode from
  being estimated as far more selective than they are.

If the estimates are wrong and the statistics are absent or old, run `ANALYZE`. If they are wrong
while `last_analyzed` is recent and `stale_mutations` is low, the cost model — not the input — is
what to look at. See [the query planner guide](query-planner.md) for how these values are consumed.

## Staleness

`stale_mutations` counts inserts, updates and deletes since the last `ANALYZE`. Its ratio to
`estimated_rows` is exactly what [automatic `ANALYZE`](automatic-analyze.md) thresholds on, so this
statement shows how close a table is to being refreshed on its own — and, on a table that never
crosses the threshold, why it never is.

## Value rendering

Bounds print as the literal that produced them: dates as ISO-8601, UUIDs in canonical form, numbers
in invariant culture.

`last_analyzed` prints as UTC ISO-8601 with millisecond precision (`2026-08-15T09:33:13.289Z`).

String bounds are **ordinal**, matching the byte order the indexes themselves use. A value like
`"árbol"` therefore sorts *above* `"zebra"` rather than before it, exactly as it does in an index
scan. Bounds are only tracked for ordered types, so a boolean column reports none.

## Scope and freshness

The values are the answering node's view:

- A node that is tracking the table answers from its live counters, which **include mutations it has
  not flushed to storage yet** — fresher than what is persisted.
- A node that is not tracking it point-reads the persisted statistics without starting to track it,
  so inspecting a table never makes it resident.

Statistics are cached per node and an `ANALYZE` run on one node does not invalidate another node's
cached copy, so in a cluster two nodes may report different values for the same table for a while.
This is the same per-node approximation described under
[automatic `ANALYZE`](automatic-analyze.md#known-limitations). When you need to confirm that an
`ANALYZE` published, run the statement on the node that ran it.

Everything here is advisory. A missing or unreadable statistics blob renders as NULLs; it is never a
statement error.

## Permissions

`SELECT` on the table. The bounds are real values drawn from the table's columns, so reading them
requires the privilege to read the table — and nothing more: unlike the configuration and engine
introspection statements, this one is not superuser-gated.

## Relations that are not tables

A materialized view stores its own rows, so it has its own statistics and is a valid target. It
reports them straight after a refresh, without waiting for an `ANALYZE`: the population counts rows
and index entries as it writes them, and the refresh hands those counts to the view along with the
storage they describe.

What a refresh does *not* produce is histograms or distinct-value counts — only `ANALYZE` builds
those — and it discards the ones the previous contents had, since they describe rows that no longer
exist. So a freshly refreshed view reports exact counts, no distributions, and `last_analyzed` of
NULL. Its `stale_mutations` reflects the rows just written, which is what lets
[automatic `ANALYZE`](automatic-analyze.md) notice it and fill in the rest.

A plain view stores nothing and therefore has no statistics of its own; the statement says so and
points you at the tables its definition reads.

## See also

- [Automatic ANALYZE](automatic-analyze.md) — how statistics are refreshed in the background.
- [Query planner](query-planner.md) — how the optimizer consumes them.
- [EXPLAIN](explain.md) — where `estimated_rows` comes from.
- [Engine statistics](engine-stats.md) — `SHOW ENGINE STATS`, the runtime-metrics counterpart.
