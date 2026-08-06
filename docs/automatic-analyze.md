# Automatic ANALYZE (background statistics refresh)

CamusDB keeps the optimizer's statistics fresh on its own. As tables change, a background job
detects when a table's histograms and distinct-value counts have gone stale and re-`ANALYZE`s it —
without a user running `ANALYZE TABLE`, without spiking memory/CPU/disk, and without interfering with
foreground queries or writes.

This document describes how it works and how to tune it. For the statistics themselves and how the
planner consumes them, see [`docs/query-planner.md`](./query-planner.md). Manual `ANALYZE TABLE` is
unchanged and remains the authoritative way to force a refresh.

> **Status:** the feature is **on by default** (`auto_analyze_enabled = true`). Set it to `false` in
> `config.yml` to go back to manual-only `ANALYZE`.

## Why it exists

The cost-based optimizer relies on per-table statistics — row counts, per-column min/max, histograms,
and number-of-distinct-values (NDV) — to estimate selectivity and pick plans. Those are built by
`ANALYZE`. Between runs, DML keeps the row/index counts live, but **histograms and NDV drift**: after
enough inserts/updates/deletes they no longer describe the data, and the planner makes poor choices
(wrong index, wrong join order). Automatic ANALYZE closes that gap by refreshing stale tables in the
background, modeled on CockroachDB's automatic statistics collection.

## How staleness is detected

Every committed row mutation (insert + update + delete) increments a per-table
`MutationsSinceAnalyze` counter, tracked in `StatisticsManager` and persisted alongside the rest of a
table's statistics. A table is considered **stale** when:

```
mutations_since_analyze >= fraction_stale_rows * row_count + min_stale_rows
```

- `fraction_stale_rows` (default `0.20`) is the proportional trigger — a fifth of the table changing.
- `min_stale_rows` (default `500`) is an absolute floor, so small tables and light churn don't cause
  constant re-analysis.
- A table that has tracked data but has **never** been analyzed is stale immediately.
- An empty, never-mutated table is never stale.

Deletes count as churn too, so a table that is fully rewritten (same row count, all rows replaced)
still trips the threshold.

## How a background ANALYZE runs (cheap and non-interfering)

The background collector produces the **same statistics** as manual `ANALYZE`, but with three hard
guarantees that keep it from disrupting a busy node.

### 1. Bounded, constant peak memory

Manual `ANALYZE` counts exactly, holding every distinct value in memory — fine for a user command,
but its memory would scale with table size. The background path instead samples with fixed-size
sketches, so peak memory is a function of the sample size, **not** the table size:

- **Histograms** are built from a bounded [reservoir sample](../CamusDB.Core/Statistics/ReservoirSampler.cs)
  (Vitter's Algorithm R) of at most `auto_analyze_histogram_sample_rows` (default `10,000`) values per
  column.
- **NDV** is estimated with a small [HyperLogLog](../CamusDB.Core/Statistics/HyperLogLog.cs) sketch
  (~2 KB per column at the default precision), rather than an exact distinct-set.
- **Row and index-entry counts** stay exact — they're running integers, O(1) memory.

Both sketches use deterministic, seeded hashing, so a given data set yields reproducible statistics.

### 2. Throttled scan

The scan is paced to `auto_analyze_max_rows_per_second` (default `50,000`), delaying between batches
so it never saturates a core or the KV read path. It also re-checks ownership and load at each batch
boundary (`auto_analyze_ownership_check_rows`, default `1,000` rows).

### 3. Lock-free reads — no interference, no priority inversion

The background scan opens its **own** serializable read-only snapshot transaction (a single minted
HLC timestamp, no range locks, no read-set folding). Because it takes no locks, it can neither block
nor be blocked by a foreground query or writer, and it can never cause a writer to abort. This is why
it is a separate code path from manual `ANALYZE` — the manual path runs on the *caller's* transaction,
which, inside an explicit read-write transaction, would fold the scanned rows into that transaction's
read set and could make it abort.

### Atomic, delta-safe publication

When the scan finishes, the new statistics are published in a **single KV transaction** — never a
partial mix of old and new fields. Publication also preserves any DML that committed **during** the
scan: row and index counts are updated by the scan's *correction* (scanned − baseline) applied to the
current live value, so concurrent inserts/deletes are not clobbered back to snapshot-time values.
Min/max is replaced with the freshly scanned truth (so it corrects delete-drift); histograms and NDV
are rebuilt wholesale.

If the scan is cancelled (shutdown, a load surge, or leadership loss) or the publish fails, **nothing
is persisted** and the staleness counter is not reset — the table stays marked stale and is retried
later. The whole operation is idempotent and safe to re-run.

## Turning it off for one table

A specific table can be exempted from **automatic** collection while the rest of the database stays
on (e.g. a high-churn append-only log where periodic re-analysis is wasted work):

```sql
ALTER TABLE application_logs SET (sql_stats_automatic_collection_enabled = false);  -- opt out
ALTER TABLE application_logs SET (sql_stats_automatic_collection_enabled = true);   -- opt back in
```

The setting gates **only the background scheduler** — a manual `ANALYZE TABLE application_logs` still
runs. It defaults to enabled (an unset table is analyzed normally), rides the table's schema without
affecting row encoding, and survives restarts. In a clustered deployment over HTTP the statement must
be issued on the schema leader.

## Scheduling and cluster behavior

A background loop, owned by the engine and modeled on the orphan-reclaimer, sweeps on an interval
(`auto_analyze_check_interval_ms`, default `60,000`). Each sweep:

1. **Runs on one node per cluster.** Only the node that leads the database-registry partition performs
   the sweep, so N nodes don't each analyze the same table, and a failover hands the work to the new
   leader.
2. **Discovers candidates from authoritative metadata.** The owner enumerates every database in the
   registry and every table's per-object meta key — *not* its local open-object list — and reads each
   table's persisted staleness. So a table created and mutated on one node is still found and analyzed
   by the owner, even if the owner never opened it.
3. **Backs off under load.** A foreground-load probe (in-flight explicit transactions plus in-flight
   HTTP and gRPC data requests) gates the sweep: above `auto_analyze_load_pause_threshold`, it skips
   starting new analyses and cancels a running one at the next batch boundary. Because the reads are
   lock-free, this addresses only raw CPU/IO contention — there is no lock-based priority inversion to
   solve.
4. **Bounds concurrency and spreads work.** At most `auto_analyze_max_concurrent` (default `1`) tables
   are analyzed at once; the candidate order is shuffled so a burst of simultaneously-stale tables is
   spread across sweeps rather than starving a tail.

### Exactly-once publication (the fence)

Discovery, the sweep, and the scan all re-check leadership, but the definitive guarantee is a
**per-table publish fence**: immediately before writing the new generation, the analyzer acquires a
cluster-visible exclusive lock on `{dbId}/meta/analyze:{tableId}` and re-confirms ownership *under the
lock*. Two nodes can't both hold the lock, and a former leader that lost its lease mid-scan fails the
under-lock ownership check and aborts — so exactly one node ever publishes, even across a failover.
The scan itself remains lock-free; only the brief publish is fenced.

A per-node in-flight claim additionally prevents the timer loop and any forced run from overlapping on
the same table on one node.

## Configuration

All knobs live in `CamusDBConfig` (see [`docs/configuration.md`](./configuration.md) for how config is
loaded). The feature is on by default; the tuning defaults around it are deliberately conservative.

| Setting | Default | Meaning |
|---|---|---|
| `auto_analyze_enabled` | `true` | Master switch. Off ⇒ the loop never runs; statistics refresh only on manual `ANALYZE`. |
| `auto_analyze_check_interval_ms` | `60000` | Sweep interval. `<= 0` also disables the loop. |
| `auto_analyze_fraction_stale_rows` | `0.20` | Proportional staleness trigger. |
| `auto_analyze_min_stale_rows` | `500` | Absolute mutation floor before a table is ever stale. |
| `auto_analyze_max_concurrent` | `1` | Max background analyses running at once on a node. |
| `auto_analyze_max_rows_per_second` | `50000` | Scan-rate throttle (CPU/IO cap). `<= 0` disables throttling. |
| `auto_analyze_histogram_sample_rows` | `10000` | Reservoir size per column — the memory bound. |
| `auto_analyze_hll_precision` | `11` | HyperLogLog index bits (register count `2^p`); ~2 KB/column, ~2.3% error. |
| `auto_analyze_load_pause_threshold` | `16` | Foreground in-flight work above which the sweep backs off. `<= 0` disables. |
| `auto_analyze_ownership_check_rows` | `1000` | Rows between mid-scan ownership/load re-checks. |

## Resource-safety summary

- **Memory** is bounded by the reservoir sample plus the HLL sketches — independent of table size.
- **CPU/disk** are bounded by the rows/second throttle and single-analysis concurrency, on one node.
- **Foreground work** is never blocked or aborted (lock-free snapshot reads), and the load probe backs
  the job off under surge.
- **Failure** — a crash, failover, cancel, or failed publish leaves statistics internally consistent
  and the table still marked stale for retry; no partial or double-applied generation.

## Relationship to manual `ANALYZE TABLE`

Manual `ANALYZE TABLE` is unchanged: it counts exactly (no sampling), runs on the caller's
transaction, and — like the background path — publishes atomically and resets the staleness counter.
Running it by hand is always available and takes precedence in the moment; automatic ANALYZE simply
keeps things fresh between manual runs.

## Known limitations

- **Cluster mutation counting is approximate.** Each node flushes its own view of a table's mutation
  count to the shared statistics blob (last-writer-wins), so the cluster-wide count can undercount.
  This only *delays* a refresh; it never corrupts data. A durable per-table counter is a possible
  future improvement.
- **Cached query plans are not invalidated by fresh statistics.** A plan already in the plan cache
  keeps running until it is re-planned for other reasons; auto-analyze does not force re-planning.
