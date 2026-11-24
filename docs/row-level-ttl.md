# Row-level TTL

Expire rows automatically. A table names a column that stores an expiry instant. A background sweep
deletes rows whose instant has passed, and you don't need to issue a `DELETE`.

The parameter names follow **CockroachDB's row-level TTL**, and so do their values and defaults. If you
know that feature, you already know this one. Where CamusDB supports less, the parameter exists with a
*narrower set of values*. For more information, see [Differences from
CockroachDB](#differences-from-cockroachdb).

---

## Turning it on

The node-level switch (`ttl_enabled` in `config.yml`) is **on by default**. This means that one step is
left: on the table you want swept, name the column that stores the expiry instant. Until a table does
that, it is not a TTL table and the sweep ignores it entirely.

Setting `ttl_enabled: false` stops the sweep loop node-wide, and the feature becomes completely inert.
An expired row is then never collected, whatever the tables say.

**On the table**, TTL is enabled by naming the column with the expiry instant.

```sql
ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at');
```

The column type can be `DATETIME` or `DATE`. It can also be `INT64`, which holds Unix epoch
milliseconds. Presence of this parameter
is what enables TTL for the table.

A row expires when its column value is in the past. **A `NULL` never expires.** This gives you an
explicit "keep this forever" value, and you don't need a second flag column.

**A table that a foreign key references cannot be a TTL table.** The sweep deletes expired rows on a
separate path that does not check for child rows. This means that it could leave an orphaned child row.
Both orders return `CADB0533` (`FeatureNotSupported`): setting `ttl_expiration_expression` on a
referenced table, and adding a foreign key that references a TTL table. A TTL table can be a child. See [foreign-keys.md](foreign-keys.md).

### Turning it off

```sql
ALTER TABLE sessions SET (ttl_pause = 'true');   -- stop sweeping, keep the configuration
ALTER TABLE sessions RESET (ttl);                -- remove the configuration entirely
```

`RESET (ttl)` clears *all* TTL parameters at once. This means that no tuning is left behind pointing at a
cleared column. `RESET` also takes a single parameter (`RESET (ttl_job_cron)`) to restore just that one
to its node default.

---

## Parameters

Set with `ALTER TABLE t SET (key = value, ...)`, cleared with `RESET`.

| Parameter | Type | Default | What it does |
|---|---|---|---|
| `ttl_expiration_expression` | column name | — | The column holding the expiry instant. Enables TTL. |
| `ttl_pause` | boolean | `false` | Stops the sweep. The configuration is not discarded. |
| `ttl_job_cron` | cron macro | `'@daily'` | How often the table is swept. |
| `ttl_select_batch_size` | integer | `500` | Rows read per scan batch. |
| `ttl_delete_batch_size` | integer | `100` | Rows deleted per transaction. |
| `ttl_select_rate_limit` | integer | `0` | Scan cap, rows/second. `0` is **unlimited**. |
| `ttl_delete_rate_limit` | integer | `100` | Delete cap, rows/second. `0` is **unlimited**. |
| `ttl_grace_ms` | integer | `0` | Extra delay past expiry before a row is eligible. |

The valid values of `ttl_job_cron` are `'@hourly'`, `'@daily'`, `'@weekly'` and `'@monthly'`.
`'@midnight'` is another spelling of `'@daily'`. It sets a **cadence**: `@daily` means "about once a day". The sweep does not
run at a fixed wall-clock time such as midnight. Sweeps are ordered by the cluster's hybrid logical clock. Wall time is not used.

Settings can also be given inline on `CREATE TABLE ... WITH (...)`. `SHOW CREATE TABLE` renders this
form. This means that its output re-creates the same table.

```sql
SHOW CREATE TABLE sessions;
-- CREATE TABLE `sessions` (...) WITH (ttl_expiration_expression = 'expires_at', ttl_grace_ms = 5000);
```

### Node defaults

Each per-table tuning parameter has a node default that applies when the table does not set it. You
can set a fleet-wide policy and override it only where a table needs it.

| Config key | Default | Supplies the default for |
|---|---|---|
| `ttl_enabled` | `true` | the master switch |
| `ttl_default_job_cron` | `'@daily'` | `ttl_job_cron` |
| `ttl_default_select_batch_size` | `500` | `ttl_select_batch_size` |
| `ttl_default_delete_batch_size` | `100` | `ttl_delete_batch_size` |
| `ttl_default_select_rate_limit` | `0` | `ttl_select_rate_limit` |
| `ttl_default_delete_rate_limit` | `100` | `ttl_delete_rate_limit` |
| `ttl_spans_per_table` | `64` | how many spans a run divides the table into |
| `ttl_max_concurrent_spans_per_node` | `1` | spans one node works at once |
| `ttl_load_pause_threshold` | `16` | in-flight foreground transactions that pause the sweep |
| `ttl_span_lease_ms` | `30000` | span claim lease |
| `ttl_span_lease_renew_interval_ms` | `10000` | how often an owner renews its claim |

---

## Expired rows stay visible

**An expired row remains readable until the sweep deletes it.** TTL is a background collector and
applies no filter at read time. This means that `SELECT` returns rows whose expiry has passed but which
have not yet been swept.

This is deliberate. Filtering at read time would put a predicate on each read of a TTL table. It would
also change index-only scans. An index entry does not carry the TTL column unless you added it as an
`INCLUDE` column. The engine would then have to fetch rows that index-only scans do not read today.
That cost would apply to all queries, permanently, to hide a row for at most one sweep interval.

If your application needs exact expiry semantics, add an explicit filter to your queries.

```sql
SELECT * FROM sessions WHERE expires_at > NOW();
```

Treat TTL as reclamation of space. It does not enforce visibility. The lag is bounded by
`ttl_job_cron`. A table that needs tight bounds should sweep more often.

---

## Tuning

**Start with the delete rate.** `ttl_delete_rate_limit` is the throttle that
governs the sweep's impact; the batch sizes govern transaction shape. The default of 100
rows/second is conservative on purpose.

**Keep delete batches small.** Each row costs one mutation *per index entry* plus one for the row
itself. For example, a table with four indexes spends five mutations per row. `ttl_delete_batch_size`
must be well under the 20,000-mutation transaction limit. A short delete transaction also prevents
foreground writers from queueing behind the sweep's locks.

**Priority orders admission only.** Sweep transactions use `Background` priority, which
decides who waits at the door when a concurrency ceiling is configured. Once admitted, a background
delete takes ordinary exclusive locks and blocks a foreground writer like any other transaction. Small
batches and short transactions protect latency; the priority tag does not.

**Spans divide the work; `ttl_max_concurrent_spans_per_node` decides how much runs at once.** The limit
is node-local. A cluster of N nodes can have N times that many spans in flight. Rate limits are also
per node and shared across a table's concurrent spans. This means that raising concurrency does not
multiply the configured rate. A worker reads only the rows of its span, because the span bounds are
seeks into the store. Adding spans does not add scan work.

**Span division is currently uniform over the row-id space, and that is not balanced.** Row ids embed a
timestamp in seconds. This means that each of 64 spans is roughly two years wide, and an active
table's rows land almost entirely in one of them. Correctness does not depend on the distribution,
because each row is swept exactly once either way. However, throughput does depend on it, and raising `ttl_spans_per_table` will not currently spread an
append-heavy table across workers.

**Use `ttl_grace_ms` for clock skew only.** It delays eligibility past the expiry
instant, which absorbs a writer with a slow clock. To make rows live 30 days, set the column
30 days ahead. Do not set a 30-day grace period.

---

## Watching it

```sql
SHOW ENGINE STATS LIKE 'ttl.%';
```

| Metric | Meaning |
|---|---|
| `ttl.rows_expired` | rows deleted by the sweep on this node |
| `ttl.rows_skipped_recheck` | rows that looked expired at scan time but were spared at delete time |
| `ttl.rows_failed` | rows whose delete failed or could not be resolved |
| `ttl.spans_completed` | spans this node finished |
| `ttl.spans_reclaimed` | spans this node took over from a worker whose lease lapsed |
| `ttl.runs_planned` | TTL runs that this node minted as planner |
| `ttl.runs_completed` | TTL runs that this node retired as finished |
| `ttl.sweep_duration_ms` | cumulative time this node spent sweeping |

Per-table rows are tagged `db=…,table=…`.

| Metric | Meaning |
|---|---|
| `ttl.table.state` | 0 idle, 1 paused, 2 progressing, 3 waiting, 4 failing, 5 stalled |
| `ttl.table.spans_done` / `ttl.table.span_count` | how far the open run has got |
| `ttl.table.rows_deleted` / `ttl.table.rows_failed` | this run's totals |
| `ttl.table.horizon_ms` | the instant this run judges rows against |
| `ttl.table.last_observed_ms` | when a tick last looked at this table |

**Read `ttl.table.state` before the totals.** Cumulative counters show whether TTL works at all.
They cannot say *which* table stopped. This is because a healthy table's numbers hide those of a silent one. The states
that matter most are the ones that look alike from the outside.

- `idle` means that there is no work to do.
- `waiting` means that another node has claimed the spans.
- `stalled` means that an open run has passed its cadence and has not advanced.
- `failing` means that deletes are being attempted and not landing. Check `ttl.rows_failed`.

Counters are per node and cumulative since start; in a cluster, sum across nodes. They require metric
collection to be enabled.

**`ttl.rows_failed` should be zero.** A non-zero value means a delete transaction failed or its outcome
could not be resolved. Those rows are deliberately left *before* the sweep's checkpoint. This means that the next
attempt retries them, and no data is lost. However, a persistently non-zero
count means something is failing and the sweep is making less progress per run than it appears to.
For that reason, it is a separate count from the skip count. A skip means the system is working as
intended, and a failure means it is not. One number covering both would look healthy while data piled
up.

**`ttl.rows_skipped_recheck` counts rows that were renewed before the delete.** A row is checked again,
under lock, at the moment of deletion — so if something extended its expiry between the scan and the
delete, it survives. On a session table a steady trickle here is normal and healthy: renewals are
winning races, as they should. A number approaching `ttl.rows_expired` means the sweep is mostly fighting live traffic, and its
cadence or batch sizes deserve another look.

If `ttl.rows_expired` is flat on a table you expect to be shrinking, check in order: `ttl_enabled` on the
node, `ttl_pause` on the table, whether `ttl_job_cron` has elapsed since the last run, and whether the
expiry column is `NULL` for the rows you expected to go.

---

## Differences from CockroachDB

CamusDB implements a subset. The parameters that exist behave as CockroachDB's do.

**These parameters exist with narrower values.**

- `ttl_expiration_expression` takes a **bare column name**. CockroachDB allows an arbitrary SQL
  expression returning a timestamp. Writing one here returns an error saying expressions are
  not supported *yet* — the parameter is right, the value grammar is narrower, and widening it later
  will not require renaming anything.
- `ttl_job_cron` takes the **`@macro` forms only**. A five-field CRON expression returns an error.

**These parameters are not implemented.**

- `ttl_expire_after` — requires a hidden expiration column, which CamusDB does not add. Use `ttl_expiration_expression` with your own column, which is the
  parameter CockroachDB itself recommends.
- `ttl_disable_changefeed_replication` — CamusDB does not implement changefeeds.
- `ttl_row_stats_poll_interval`, `ttl_label_metrics` — Prometheus-specific. Use `SHOW ENGINE STATS`.

**One parameter is a CamusDB extension.**

- `ttl_grace_ms` is specific to CamusDB. In CockroachDB, you would fold a grace period into the
  expiration expression, which a bare column name cannot express.

---

## How it works

This section is for cluster operators.

A **planner** is active on one elected node per database. When a table's cadence has elapsed, it
mints a *run*. A run is a durable manifest that records the table, a span count, and a single expiry
**horizon** captured from the hybrid logical clock. All workers in the run test rows against that one
horizon. This means that two nodes can never disagree about a row on their shared boundary.

**Workers are active on all nodes.** This is the first background job in CamusDB whose work is spread
across the cluster. Before it, background work was done entirely by the leader. A worker *claims* a span by taking a
lease on the span's key. When a worker dies, its lease lapses and the span becomes free. This means
that the run does not stall. No node assigns spans. This is possible because claiming needs no
knowledge of which nodes are alive.

Each span is processed as: scan a bounded batch → keep the expired rows → delete them in small
transactions → **checkpoint after the deletes commit**. The checkpoint is stored separately from the
claim and does not expire with it. A worker that takes over a dead worker's span resumes where that
worker stopped. It does not rescan from the start.

Deletes go through the normal delete path. A row and all of its index entries are deleted in one
transaction. For this reason, TTL is not implemented with the per-key expiry of the KV layer. Expiring
the row key alone would strand the secondary index entries permanently, and index-only scans would then
return rows that no longer exist.

A run whose table id no longer matches the live schema is discarded before it does any work. This
means that a run left behind by a `DROP` can never delete rows out of a new table that happens to reuse
the name.
