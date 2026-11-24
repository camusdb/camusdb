# `SHOW STATISTICS FOR <table>`

CamusDB's cost-based optimizer prices plans from per-table statistics. These include a row count and
per-column minimum and maximum values. They also include equi-depth histograms and approximate
distinct-value counts. Those values decide whether a query uses an index or scans, and which side of
a join is built. This statement shows them.

```sql
SHOW STATISTICS FOR robots;
SHOW STATISTICS FOR TABLE robots;   -- TABLE is an optional noise word
```

It shows three things that `EXPLAIN` alone does not: whether `ANALYZE` has run on this table,
what the optimizer believes about the data, and how stale that belief is.

This statement shows table statistics. For Kahuna/Kommander runtime metrics see
[`SHOW ENGINE STATS`](engine-stats.md).

## Result columns

One row per statistics *target*, discriminated by `kind`.

| Column | Type | Meaning |
|--------|------|---------|
| `table` | string | The table the statistics belong to. |
| `kind` | string | What this row describes. See [Row kinds](#row-kinds) for the four values. |
| `target` | string | NULL on the `table` row. On the other rows, the name of the column, key tuple, or index. |
| `estimated_rows` | int64 | Estimated rows in the table; on an `index` row, that index's entry count. |
| `distinct_count` | int64 | Approximate distinct values (NDV) for a `column` or `key` row. |
| `min_value` | string | Smallest value observed in the column. |
| `max_value` | string | Largest value observed in the column. |
| `histogram_buckets` | int64 | Number of equi-depth buckets in the column's histogram. |
| `last_analyzed` | string | When `ANALYZE` last read this table. NULL if `ANALYZE` has not run on it. |
| `stale_mutations` | int64 | Row mutations since that `ANALYZE`. |

`last_analyzed` and `stale_mutations` describe the table and repeat on all rows. Any single row
tells you how much to trust the rest.

A NULL means one of two things, and the `kind` column tells you which. Either the value does not
apply to that row (`distinct_count` on an `index` row), or it has not been collected yet
(`histogram_buckets` before any `ANALYZE`).

### Row kinds

- **`table`** — the table-wide counters. This row is always present. A table for which no statistics
  have been collected returns this one row.
- **`column`** — one per column carrying any estimate. Min/max is maintained on writes for indexed
  columns; `distinct_count` and `histogram_buckets` appear only after `ANALYZE`.
- **`key`** — one per composite-index key prefix, with the columns comma-joined (`city,zip`). These
  correct the optimizer's independence assumption for correlated equality predicates.
- **`index`** — one per index, with its approximate entry count.

Rows arrive grouped in that order, columns in schema order and keys and indexes ordered by name.

## A worked example

A freshly created table returns one `table` row, and its estimates are NULL.

```
> SHOW STATISTICS FOR robots;

table   kind   target  estimated_rows  distinct_count  min_value  max_value  histogram_buckets  last_analyzed  stale_mutations
robots  table  NULL    NULL            NULL            NULL       NULL       NULL               NULL           0
```

After you insert 20 rows, the write-maintained statistics appear, before any `ANALYZE`. The row
count comes from this node's live counters. It is visible immediately, before any flush to
storage.

```
robots  table   NULL      20    NULL  NULL  NULL  NULL  NULL  20
robots  column  year      NULL  NULL  2000  2019  NULL  NULL  20
robots  index   ~pk       20    NULL  NULL  NULL  NULL  NULL  20
robots  index   year_idx  20    NULL  NULL  NULL  NULL  NULL  20
```

The `distinct_count` and histogram columns are NULL. The optimizer is costing this table with
fallback selectivities. `ANALYZE` fills them in and resets the staleness counter.

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
inputs that produced it. Each kind of predicate is priced from a different value.

- `col = v` is priced at `1 / distinct_count`. A `distinct_count` of NULL means the estimate used a
  fixed constant. An index may then look far less attractive than it is.
- Range predicates are priced from the histogram. `histogram_buckets` of NULL means the same
  fallback.
- `WHERE a = ? AND b = ?` over a composite index uses the `key` row for `a,b`. Multiplying two
  independent selectivities would estimate correlated columns like city and postcode as far more
  selective than they are.

If the estimates are wrong and the statistics are absent or old, run `ANALYZE`. If they are wrong
while `last_analyzed` is recent and `stale_mutations` is low, the input is sound. In that case, look
at the cost model. See [the query planner guide](query-planner.md) for how these values are consumed.

## Staleness

`stale_mutations` counts inserts, updates and deletes since the last `ANALYZE`. Its ratio to
`estimated_rows` is what [automatic `ANALYZE`](automatic-analyze.md) thresholds on. This means that
the statement shows how close a table is to an automatic refresh. For a table that does not cross
the threshold, it also shows why the table is not refreshed.

## Value rendering

Bounds print as the literal that produced them. For example, dates print as ISO-8601 and numbers
print in invariant culture. UUIDs print in canonical form.

`last_analyzed` prints as UTC ISO-8601 with millisecond precision (`2026-08-15T09:33:13.289Z`).

String bounds are **ordinal**, matching the byte order the indexes themselves use. A value like
`"árbol"` therefore sorts *above* `"zebra"`, as it does in an index
scan. Bounds are tracked for ordered types only. A boolean column shows no bounds.

## Scope and freshness

The values are the answering node's view. Whether that node is tracking the table decides where the
values come from.

- A node that is tracking the table answers from its live counters, which **include mutations it has
  not flushed to storage yet** — fresher than what is persisted.
- A node that is not tracking it point-reads the persisted statistics. That node does not start to
  track the table. This means that inspecting a table does not make it resident.

Statistics are cached per node. An `ANALYZE` run on one node does not invalidate another node's
cached copy. In a cluster, two nodes may therefore show different values for the same table for a
while.
This is the same per-node approximation described under
[automatic `ANALYZE`](automatic-analyze.md#known-limitations). When you need to confirm that an
`ANALYZE` published, run the statement on the node that ran it.

The statistics are advisory. A missing or unreadable statistics blob renders as NULLs. It does not
cause a statement error.

## Permissions

`SELECT` on the table. The bounds are real values drawn from the table's columns. Reading them
therefore requires the privilege to read the table. No other privilege is needed: unlike the configuration
and engine introspection statements, this one is not superuser-gated.

## Relations that are not tables

A materialized view stores rows and has statistics for them. It is therefore a valid target.
Its statistics are available straight after a refresh, before any `ANALYZE`. The population counts
rows and index entries as it writes them. The refresh hands those counts to the view along with the
storage they describe.

A refresh does *not* produce histograms or distinct-value counts — only `ANALYZE` builds
those — and it discards the ones the previous contents had, since they describe rows that no longer
exist. A freshly refreshed view therefore shows exact counts and a NULL `last_analyzed`, with no
distributions. Its `stale_mutations` reflects the rows just written, which is what lets
[automatic `ANALYZE`](automatic-analyze.md) notice it and fill in the rest.

A non-materialized view stores no rows and therefore has no statistics. The statement says so and points you at the tables its definition reads.

## See also

- [Automatic ANALYZE](automatic-analyze.md) — how statistics are refreshed in the background.
- [Query planner](query-planner.md) — how the optimizer consumes them.
- [EXPLAIN](explain.md) — where `estimated_rows` comes from.
- [Engine statistics](engine-stats.md) — `SHOW ENGINE STATS`, the runtime-metrics counterpart.
