# Row-level TTL

Expire rows automatically. A table names a column holding an expiry instant; a background sweep deletes
rows whose instant has passed, without anyone issuing a `DELETE`.

The parameter names, their values, and their defaults follow **CockroachDB's row-level TTL**, so if you
know that feature you already know this one. Where CamusDB supports less, it supports a *narrower value*
for the same parameter rather than a different parameter — see [Differences from
CockroachDB](#differences-from-cockroachdb).

---

## Turning it on

The node-level switch (`ttl_enabled` in `config.yml`) is **on by default**, so there is one thing left
to do: name the column that holds the expiry instant on the table you want swept. Until a table does
that it is not a TTL table and the sweep ignores it entirely.

Setting `ttl_enabled: false` stops the sweep loop node-wide and the feature becomes completely inert —
an expired row is then never collected, whatever the tables say.

**On the table**, by naming the column that holds the expiry instant:

```sql
ALTER TABLE sessions SET (ttl_expiration_expression = 'expires_at');
```

That column may be `DATETIME`, `DATE`, or `INT64` (Unix epoch milliseconds). Presence of this parameter
is what enables TTL for the table.

A row expires when its column value is in the past. **A `NULL` never expires** — that gives you an
explicit "keep this forever" value without a second flag column.

### Turning it off

```sql
ALTER TABLE sessions SET (ttl_pause = 'true');   -- stop sweeping, keep the configuration
ALTER TABLE sessions RESET (ttl);                -- remove the configuration entirely
```

`RESET (ttl)` clears *every* TTL parameter at once, so you never leave tuning behind pointing at a
cleared column. `RESET` also takes a single parameter (`RESET (ttl_job_cron)`) to restore just that one
to its node default.

---

## Parameters

Set with `ALTER TABLE t SET (key = value, ...)`, cleared with `RESET`.

| Parameter | Type | Default | What it does |
|---|---|---|---|
| `ttl_expiration_expression` | column name | — | The column holding the expiry instant. Enables TTL. |
| `ttl_pause` | boolean | `false` | Stops the sweep without discarding the configuration. |
| `ttl_job_cron` | cron macro | `'@daily'` | How often the table is swept. |
| `ttl_select_batch_size` | integer | `500` | Rows read per scan batch. |
| `ttl_delete_batch_size` | integer | `100` | Rows deleted per transaction. |
| `ttl_select_rate_limit` | integer | `0` | Scan cap, rows/second. `0` is **unlimited**. |
| `ttl_delete_rate_limit` | integer | `100` | Delete cap, rows/second. `0` is **unlimited**. |
| `ttl_grace_ms` | integer | `0` | Extra delay past expiry before a row is eligible. |

`ttl_job_cron` accepts `'@hourly'`, `'@daily'`, `'@weekly'`, `'@monthly'` (and `'@midnight'` as a
spelling of `'@daily'`). It sets a **cadence, not a wall-clock schedule** — `@daily` means "about once a
day", not "at midnight". Sweeps are ordered by the cluster's hybrid logical clock, never by wall time.

Settings can also be given inline on `CREATE TABLE ... WITH (...)`, which is the form `SHOW CREATE TABLE`
renders — so its output re-creates the same table rather than merely describing it:

```sql
SHOW CREATE TABLE sessions;
-- CREATE TABLE `sessions` (...) WITH (ttl_expiration_expression = 'expires_at', ttl_grace_ms = 5000);
```

### Node defaults

Every per-table tuning parameter falls back to a node default, so you can set a fleet-wide policy and
override it only where a table needs it.

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

## The one thing to know: expired rows stay visible

**An expired row remains readable until the sweep deletes it.** TTL is a background collector, not a
read-time filter, so `SELECT` returns rows whose expiry has passed but which have not yet been swept.

This is deliberate. Filtering at read time would put a predicate on every read of a TTL table, and it
would change index-only scans — an index entry does not carry the TTL column unless you added it as an
`INCLUDE` column, so the engine would have to fetch rows it currently never touches. The cost lands on
every query, forever, to hide a row for at most one sweep interval.

If your application needs exact expiry semantics, filter explicitly:

```sql
SELECT * FROM sessions WHERE expires_at > NOW();
```

and treat TTL as what it is — reclamation of space, not enforcement of visibility. The lag is bounded by
`ttl_job_cron`, so a table needing tight bounds should sweep more often.

---

## Tuning

**Start with the delete rate, not the batch sizes.** `ttl_delete_rate_limit` is the throttle that
actually governs the sweep's impact; the batch sizes govern transaction shape. The default of 100
rows/second is conservative on purpose.

**Delete batches are small for a reason.** Each row costs one mutation *per index entry* plus the row
itself, so a table with four indexes spends five mutations per row. `ttl_delete_batch_size` must stay
well under the 20,000-mutation transaction limit, and a short delete transaction is also what keeps
foreground writers from queueing behind the sweep's locks.

**Priority orders admission, not execution.** Sweep transactions run at `Background` priority, which
decides who waits at the door when a concurrency ceiling is configured. Once admitted, a background
delete takes ordinary exclusive locks and blocks a foreground writer exactly like any other. Small
batches and short transactions are what protect latency — not the priority tag.

**Spans divide the work; `ttl_max_concurrent_spans_per_node` decides how much runs at once.** The limit
is node-local, so a cluster of N nodes can have N times that many spans in flight. Rate limits are also
per node and shared across a table's concurrent spans, so raising concurrency does not multiply the
configured rate.

**Span division is currently uniform over the row-id space, and that is not balanced.** Row ids embed a
timestamp in seconds, so 64 spans are roughly two years wide each and an active table's rows land almost
entirely in one of them. Correctness does not depend on the distribution — every row is swept exactly
once either way — but throughput does, and raising `ttl_spans_per_table` will not currently spread an
append-heavy table across workers.

**Use `ttl_grace_ms` for clock skew**, not as a business rule. It delays eligibility past the expiry
instant, which absorbs a writer whose clock runs slow. If you want rows to live 30 days, set the column
30 days ahead — don't set a 30-day grace period.

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
| `ttl.runs_planned` | runs this node minted as planner |
| `ttl.runs_completed` | runs this node retired as finished |
| `ttl.sweep_duration_ms` | cumulative time this node spent sweeping |

Per-table rows, tagged `db=…,table=…`:

| Metric | Meaning |
|---|---|
| `ttl.table.state` | 0 idle, 1 paused, 2 progressing, 3 waiting, 4 failing, 5 stalled |
| `ttl.table.spans_done` / `ttl.table.span_count` | how far the open run has got |
| `ttl.table.rows_deleted` / `ttl.table.rows_failed` | this run's totals |
| `ttl.table.horizon_ms` | the instant this run judges rows against |
| `ttl.table.last_observed_ms` | when a tick last looked at this table |

**Read `ttl.table.state` before the totals.** Cumulative counters answer "is TTL working at all"; they
cannot say *which* table stopped, because a healthy table's numbers hide a silent one's. The states
that matter most are the ones that look alike from the outside: `idle` means nothing to do, `waiting`
means another node holds the spans, and `stalled` means an open run has passed its own cadence without
advancing. `failing` means deletes are being attempted and not landing — check `ttl.rows_failed`.

Counters are per node and cumulative since start; in a cluster, sum across nodes. They require metric
collection to be enabled.

**`ttl.rows_failed` should be zero.** A non-zero value means a delete transaction failed or its outcome
could not be resolved. Those rows are deliberately left *before* the sweep's checkpoint, so the next
attempt retries them rather than stepping over them — no data is lost, but a persistently non-zero
count means something is failing and the sweep is making less progress per run than it appears to.
It is kept separate from the skip count for exactly that reason: a skip is the system working, a
failure is not, and one number covering both would look healthy while data piled up.

**`ttl.rows_skipped_recheck` is the interesting one.** A row is checked again, under lock, at the moment
of deletion — so if something extended its expiry between the scan and the delete, it survives. On a
session table a steady trickle here is normal and healthy: it is renewals winning races, exactly as they
should. A number approaching `ttl.rows_expired` means the sweep is mostly fighting live traffic, and its
cadence or batch sizes deserve another look.

If `ttl.rows_expired` is flat on a table you expect to be shrinking, check in order: `ttl_enabled` on the
node, `ttl_pause` on the table, whether `ttl_job_cron` has elapsed since the last run, and whether the
expiry column is `NULL` for the rows you expected to go.

---

## Differences from CockroachDB

CamusDB implements a subset. The parameters that exist behave as CockroachDB's do.

**Narrower values, same parameter:**

- `ttl_expiration_expression` takes a **bare column name**. CockroachDB accepts an arbitrary SQL
  expression returning a timestamp. Writing one here is rejected with a message saying expressions are
  not supported *yet* — the parameter is right, the value grammar is narrower, and widening it later
  will not require renaming anything.
- `ttl_job_cron` takes the **`@macro` forms only**. A five-field CRON expression is rejected.

**Not implemented:**

- `ttl_expire_after` — requires an `INTERVAL` type and literal, which CamusDB does not have, plus a
  hidden expiration column. Use `ttl_expiration_expression` with your own column, which is the
  parameter CockroachDB itself recommends.
- `ttl_disable_changefeed_replication` — CamusDB has no changefeeds.
- `ttl_row_stats_poll_interval`, `ttl_label_metrics` — Prometheus-specific. Use `SHOW ENGINE STATS`.

**CamusDB extension:**

- `ttl_grace_ms` has no CockroachDB equivalent — there you would fold a grace period into the
  expiration expression, which a bare column name cannot express.

---

## How it works

Worth knowing if you operate a cluster.

A **planner** runs on one elected node per database. When a table's cadence has elapsed it mints a
*run*: a durable manifest recording the table, a single expiry **horizon** captured from the hybrid
logical clock, and a span count. Every worker in the run tests rows against that one horizon, so two
nodes can never disagree about a row on their shared boundary.

**Workers run on every node** — this is the first background job in CamusDB whose work is spread across
the cluster rather than done entirely by the leader. A worker *claims* a span rather than being assigned
one: it takes a lease on the span's key, and the lease means a worker that dies stops blocking the span
rather than stalling the run. Claiming needs no knowledge of which nodes are alive, which is why it is
claims and not assignment.

Each span is processed as: scan a bounded batch → keep the expired rows → delete them in small
transactions → **checkpoint after the deletes commit**. The checkpoint is stored separately from the
claim and does not expire with it, so a worker that takes over a dead worker's span resumes where it
stopped instead of rescanning from the start.

Deletes go through the normal delete path, so a row and all of its index entries go in one transaction.
This is why TTL is not implemented with the KV layer's own per-key expiry: expiring the row key alone
would strand every secondary index entry permanently, and index-only scans would then return rows that
no longer exist.

A run whose table id no longer matches the live schema is discarded without being driven — so a run left
behind by a `DROP` can never delete rows out of a new table that happens to reuse the name.
