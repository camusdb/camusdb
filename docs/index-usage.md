# Index usage — `SHOW INDEX USAGE`

CamusDB counts, for each index, how often statements read through it and how often writes change it:

```sql
SHOW INDEX USAGE;                -- every index of every table in the context database
SHOW INDEX USAGE FROM orders;    -- the indexes of one table
```

It is the CamusDB form of PostgreSQL's `pg_stat_user_indexes` and CockroachDB's
`crdb_internal.index_usage_statistics`. The counts are **on by default**.

## What it is for

An index that no statement reads still costs one write for each row an `INSERT` adds, and more for an
`UPDATE` that changes its key. `SHOW INDEXES` lists the indexes, and `SHOW STATISTICS FOR` gives their
sizes, but neither says whether an index is read. This statement does.

An index is a candidate to drop when, over a long window:

- `reads` is 0,
- `constraint_reads` is 0, and
- `writes` is not 0.

A unique index or a primary key enforces a constraint, so it is not removable because its reads are 0.
An index that backs a foreign key shows its use in `constraint_reads`.

## What is counted

| Counted as | When |
|---|---|
| `reads` | A statement opens a read through the index: a unique lookup, a range scan, an `IN`-list seek set (one read for the whole list), a semi-join or `EXISTS` probe, an index nested-loop probe batch, or a forced-index scan. |
| `constraint_reads` | A write checks a key through the index: a unique key an `INSERT` or `UPDATE` writes, the parent key a foreign-key check looks up, or the child key a parent delete or update probes. |
| `entries_read` | The index entries that those reads returned. |
| `writes` | The index entries that an `INSERT`, `UPDATE` or `DELETE` wrote or removed. An `UPDATE` that changes a key counts 2 (the old entry and the new one). An `UPDATE` that does not change the key of an index counts 0 for it. |

A full table scan reads the rows, not an index, so it counts on no index.

`EXPLAIN` without `ANALYZE` plans and does not execute, so it counts nothing. `EXPLAIN (ANALYZE)`
executes the statement, so it counts.

Writes are counted when the write batch is accepted. A transaction that rolls back later still shows
the writes it staged.

## Result columns

| Column | Type | Meaning |
|---|---|---|
| `node` | `STRING` | The node that answered. |
| `database` | `STRING` | The context database. |
| `table` | `STRING` | The table. |
| `index` | `STRING` | The index name; `~pk` for the primary key. |
| `unique` | `BOOL` | True for a unique index or the primary key. |
| `reads` | `INT64` | Statement reads opened through the index. |
| `constraint_reads` | `INT64` | Reads that enforce a unique key or a foreign key. |
| `entries_read` | `INT64` | Index entries returned by all reads. |
| `last_read_at` | `STRING` | ISO-8601 UTC time of the newest read or constraint read; `NULL` when there was none since `tracked_since`. |
| `writes` | `INT64` | Index entries written or removed by DML. |
| `tracked_since` | `STRING` | ISO-8601 UTC time from which this node has counted the index. |

Every readable index has a row, also when every count is 0 — that row is the answer the statement
exists to give. An index in the middle of a backfill is not listed, the same as in `SHOW INDEXES`: it
takes writes and cannot be read yet.

### Reading `tracked_since`

The counts are complete from `tracked_since` onward:

- An index that existed when this node first opened its table counts from the **node start**. A table
  that this node never opened also reports the node start, with zero reads: a read opens the table, so
  that is true.
- An index that appeared later, on a table this node already tracked — the case of `CREATE INDEX`
  after the start — counts from the **time the node first saw it**.

Always compare a window with `tracked_since` before you call an index unused. An index with zero reads
since an hour ago is not evidence of anything.

## Worked example

```sql
SHOW INDEX USAGE FROM orders;
```

| index | unique | reads | constraint_reads | writes | last_read_at | tracked_since |
|---|---|---|---|---|---|---|
| ~pk | true | 912044 | 48120 | 48120 | 2026-10-26T15:59:58.201Z | 2026-10-12T08:00:01.004Z |
| orders_status_idx | false | 18821 | 0 | 61402 | 2026-10-26T15:59:41.880Z | 2026-10-12T08:00:01.004Z |
| orders_legacy_ref_idx | false | 0 | 0 | 48120 | NULL | 2026-10-12T08:00:01.004Z |

`orders_legacy_ref_idx` took 48,120 writes in two weeks and served no read and no constraint check. It
is a candidate to drop. Check every node first: the counts are per node, and a report that runs on one
node only reads through that node.

## Settings

```yml
index_usage_enabled: true
```

| Setting | Default | What it does |
|---|---|---|
| `index_usage_enabled` | `true` | Runtime-mutable and node-scoped. Off: no new counts, and the counts already taken are kept. |

The cost is a few interlocked additions per index access path and per write batch, never one per row.

## Privileges

`SHOW INDEX USAGE` requires a **superuser**, the same bar as `SHOW ENGINE STATS`. The counts describe
the whole node's workload, which no per-table grant scopes down.

## Limits

1. **It does not survive a restart.** The counts live in memory and start at zero when the node
   starts. Keep readings outside the node if you need a window longer than the node's uptime.
2. **It is one node's view.** Each node counts the statements it ran. An index is unused only if it
   is unused on every node.
3. **Inspection does not load tables.** The index list comes from the database's catalog, so the
   statement does not make every table of a database resident.

## Related

- [Statement statistics](statement-stats.md) — `SHOW STATEMENT STATS`, the statements behind the reads.
- [`SHOW STATISTICS FOR`](show-statistics.md) — index sizes and the optimizer's view of the data.
- [`SHOW ENGINE STATS`](engine-stats.md) — node-level metrics.
- [Slow query log](slow-query-log.md) — single slow statements, with the full-scan flag.
