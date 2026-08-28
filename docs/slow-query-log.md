# Slow query log — `SHOW SLOW QUERIES`

CamusDB can record every statement that takes longer than a threshold you set, and hand the record
back as a SQL result set:

```sql
SHOW SLOW QUERIES;
SHOW SLOW QUERIES LIKE '%FROM orders%';
```

The log is **off by default** and **bounded in memory**. It is a diagnostic you turn on when a node
is slow, not a permanent audit trail — see [Limits](#limits) before you plan around it.

## What it is for

A duration on its own tells you *that* a statement was slow. It does not tell you *why*, and by the
time you re-run the statement with `EXPLAIN` the conditions that made it slow are usually gone.

So every entry carries the execution facts that explain its duration:

- `full_scan` — the plan read a whole relation instead of seeking through an index.
- `spilled` — a sort, grouping, distinct or hash join outgrew its memory budget and wrote to disk.
- `rows_read` against `rows_returned` — reading far more rows than you returned is the signature of
  a predicate no index serves.

Those three answer most "why was this slow" questions without a second run.

## Turning it on

Four settings in `config.yml`:

```yml
slow_query_log_enabled: true
slow_query_log_threshold_ms: 1000
slow_query_log_max_entries: 200
slow_query_log_max_sql_length: 4096
```

| Setting | Default | What it does |
|---|---|---|
| `slow_query_log_enabled` | `false` | Master switch. |
| `slow_query_log_threshold_ms` | `1000` | Record a statement at or above this duration. `0` records everything. |
| `slow_query_log_max_entries` | `200` | Entries kept before the oldest is overwritten. |
| `slow_query_log_max_sql_length` | `4096` | Characters of statement text stored per entry. |

`SHOW VARIABLES LIKE 'slow_query%'` reports what a node actually resolved, which is what it obeys.

### Which of these need a restart

`slow_query_log_max_entries` is **restart-class**. The ring allocates its array when the node builds
it, so a live change cannot resize it. The other three are **runtime-mutable**: the engine re-reads
them per statement, so you can raise the threshold or turn recording off on a busy node without a
restart.

There is one asymmetry worth knowing. A node that started with `slow_query_log_enabled: false` has
no ring at all, so turning the setting on at runtime records nothing. Turning it **off** at runtime
does work, and it keeps the entries already recorded so you can still read them.

All four are **node-scoped**. Nodes may legitimately disagree, which is the point: turn the log on
for the one node you are investigating.

### Sizing it

Memory is bounded by `slow_query_log_max_entries` × `slow_query_log_max_sql_length`. At the defaults
that is roughly 800 KB of text plus per-entry overhead.

Set the threshold before you set the capacity. A threshold of `0` on a busy node turns the whole ring
over in the time it takes to run 200 statements, and the genuinely slow statement you were hunting is
evicted by the fast ones that followed it. Use `0` only on an idle node while reproducing something.

## Result columns

| Column | Type | Meaning |
|---|---|---|
| `seq` | `INT64` | Recording order on this node, starting at 1. Keeps counting after the ring wraps. |
| `started_at` | `STRING` | When the statement started, ISO-8601 UTC. |
| `duration_ms` | `FLOAT64` | Wall-clock duration. |
| `database` | `STRING` | Database named by the statement. Empty for a server-level statement. |
| `user` | `STRING` | Authenticated caller, or `NULL` when the node is not authenticating. |
| `kind` | `STRING` | Statement kind: `select`, `insert`, `update`, `create_table`, … |
| `rows_returned` | `INT64` | Rows the caller consumed, or rows affected for a mutation. |
| `rows_read` | `INT64` | Rows fetched from storage before filtering. |
| `full_scan` | `BOOL` | Some part of the statement read a whole relation. |
| `spilled` | `BOOL` | Some blocking operator wrote to a spill file. |
| `outcome` | `STRING` | `completed`, `abandoned` or `failed`. |
| `error_code` | `STRING` | Engine error code when `outcome` is `failed`, `NULL` otherwise. |
| `truncated` | `BOOL` | The stored `sql` is shorter than the statement that ran. |
| `sql` | `STRING` | The statement text as the client sent it, truncated to the configured length. |

Rows come back newest first, so a bare `SHOW SLOW QUERIES` answers "what just happened" without an
`ORDER BY`.

### Reading `seq`

`seq` keeps counting past the ring's capacity. Compare it between two readings: if the newest `seq`
advanced by more than the number of entries the ring holds, entries were overwritten in between and
you are not seeing everything that qualified. That is the signal to raise the capacity or the
threshold.

### Reading `outcome`

- `completed` — the caller consumed every row.
- `abandoned` — the caller stopped reading early. A client that disconnected, or one that took the
  first page and left. `rows_returned` is then a floor, not a total. The work was still done, which
  is why the entry is kept.
- `failed` — the statement raised, and `error_code` says how. A slow failure is usually the most
  interesting entry in the log: a lock wait that timed out, or a schema catch-up that never caught up.

## Worked example

```sql
SHOW SLOW QUERIES LIKE '%orders%';
```

| seq | duration_ms | kind | rows_returned | rows_read | full_scan | spilled | outcome | sql |
|---|---|---|---|---|---|---|---|---|
| 412 | 4180.2 | select | 12 | 2400000 | true | false | completed | SELECT * FROM orders WHERE region = 'emea' |
| 407 | 2210.7 | select | 500 | 500 | false | true | completed | SELECT * FROM orders ORDER BY total DESC |

The first row read two million rows to return twelve: `region` has no index. The second read exactly
what it returned but spilled, so it is a memory problem, not an index problem — either raise
`spill_threshold_rows` or bound the sort with a `LIMIT`. See
[Spill to disk](spill-to-disk.md) and [the query planner guide](query-planner.md).

## Privileges

`SHOW SLOW QUERIES` requires a **superuser**, a higher bar than `SHOW DATABASES` and the same bar as
[`SHOW ENGINE STATS`](engine-stats.md) and [`SHOW VARIABLES`](show-variables.md).

The reason is sharper here than for either of those: the rows carry the literal SQL text of
statements other users ran, so a predicate value from a table the caller holds no grant on can appear
verbatim in the output. No per-database grant scopes that down.

## Limits

Know these before you build anything on the log.

1. **It does not survive a restart.** The entries live in memory only. A node that restarts starts
   with an empty log. Durable slow-query history is a different feature.
2. **It is one node's log.** The statement reports the node that answered it and never gathers from
   peers. Inspect a cluster one node at a time. This is deliberate: a node is usually slow on its
   own, and answering from the leader would hide exactly that.
3. **It is a bounded sample, not every slow statement.** Once the ring is full the oldest entry goes.
   Use `seq` to tell whether you are looking at a complete picture.
4. **It is not an audit log.** Statements below the threshold are never recorded, and the text is
   truncated.

## Related

- [`SHOW ENGINE STATS`](engine-stats.md) — node-level Raft and storage metrics.
- [`SHOW VARIABLES`](show-variables.md) — what this node's configuration actually resolved to.
- [`EXPLAIN` and `EXPLAIN ANALYZE`](explain.md) — per-operator detail for one statement.
- [Spill to disk](spill-to-disk.md) — what `spilled` means and how to tune it.
- [Configuration](configuration.md) — the full settings reference.
