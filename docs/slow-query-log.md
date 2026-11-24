# Slow query log — `SHOW SLOW QUERIES`

CamusDB can record the statements that take longer than a threshold you set. You read the record
back as a SQL result set with `SHOW SLOW QUERIES`.

```sql
SHOW SLOW QUERIES;
SHOW SLOW QUERIES LIKE '%FROM orders%';
```

The log is **off by default** and **bounded in memory**. It is a diagnostic you turn on when a node
is slow. It is not a permanent audit trail. Read [Limits](#limits) before you plan around it.

## What it is for

A duration tells you *that* a statement was slow. It does not tell you *why*, and by the time you
re-run the statement with `EXPLAIN` the conditions that made it slow are usually gone.

For this reason, each entry also records the execution facts that explain its duration.

- `full_scan` — the plan read a complete relation and did not seek through an index.
- `spilled` — a sort, grouping, distinct or hash join outgrew its memory budget and wrote to disk.
- `rows_read` against `rows_returned` — reading far more rows than you returned is the signature of
  a predicate no index serves.

Those three answer most "why was this slow" questions, and you don't need a second run.

## Turning it on

The log is controlled by four settings in `config.yml`.

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

`SHOW VARIABLES LIKE 'slow_query%'` shows the values a node resolved. Those are the values the node
uses.

### Which of these need a restart

`slow_query_log_max_entries` is **restart-class**. The ring allocates its array once, and a live
change cannot resize it. The other three are **runtime-mutable**. The engine re-reads them per
statement. This means that you can turn recording on or off on a busy node, or raise the threshold,
and no restart is needed.

A node that started with `slow_query_log_enabled: false` has no ring allocated. The first
`SET CLUSTER SETTING slow_query_log_enabled = true` creates it, at the capacity the node started
with, and the next statement is recorded. When you turn the log **off** at runtime, the ring and the
entries already recorded are kept. You can read them after the log is off.

All four are **node-scoped**. Nodes may disagree. For example, you can turn the log on for the one
node you are investigating.

### Sizing it

Memory is bounded by `slow_query_log_max_entries` × `slow_query_log_max_sql_length`. At the defaults
that is roughly 800 KB of text plus per-entry overhead.

Set the threshold before you set the capacity. A threshold of `0` on a busy node turns the ring
over in the time it takes to run 200 statements, and the genuinely slow statement you were hunting is
evicted by the fast ones that followed it. Use `0` only on an idle node while reproducing something.

## Result columns

| Column | Type | Meaning |
|---|---|---|
| `seq` | `INT64` | Recording order on this node, starting at 1. The count continues after the ring wraps. |
| `started_at` | `STRING` | When the statement started, ISO-8601 UTC. |
| `duration_ms` | `FLOAT64` | Wall-clock duration. |
| `database` | `STRING` | Database named by the statement. Empty for a server-level statement. |
| `user` | `STRING` | Authenticated caller, or `NULL` when the node is not authenticating. |
| `kind` | `STRING` | Statement kind: `select`, `insert`, `update`, `create_table`, … |
| `rows_returned` | `INT64` | Rows the caller consumed, or rows affected for a mutation. |
| `rows_read` | `INT64` | Rows fetched from storage before filtering, on all sides of all joins. A nested-loop join that reads its right side again for each left row counts each read. A row a peer node ships for a distributed scan or a broadcast join is counted when it arrives; a row the peer read and did not ship is not. |
| `full_scan` | `BOOL` | Some part of the statement read a complete relation, including the right side of a hash or nested-loop join. |
| `spilled` | `BOOL` | Some blocking operator wrote to a spill file. |
| `outcome` | `STRING` | `completed`, `abandoned` or `failed`. |
| `error_code` | `STRING` | Engine error code when `outcome` is `failed`, `NULL` otherwise. |
| `truncated` | `BOOL` | The stored `sql` is shorter than the statement that ran. |
| `sql` | `STRING` | The statement text as the client sent it, with any password literal in `CREATE USER` / `ALTER USER` replaced by `'***'`, then truncated to the configured length. |

Rows come back newest first. A bare `SHOW SLOW QUERIES` therefore answers "what just happened", and
you don't need an `ORDER BY`.

### Reading `seq`

`seq` continues to count past the ring's capacity. Compare it between two readings. If the newest
`seq` advanced by more than the ring's capacity, entries were overwritten in between. You are then
missing some of the statements that qualified. That is the signal to raise the capacity or the
threshold.

### Reading `outcome`

- `completed` — the caller consumed all rows.
- `abandoned` — the caller stopped reading early. A client that disconnected, or one that took the
  first page and left. `rows_returned` is then a lower bound on the total. The entry is kept because
  the work was done.
- `failed` — the statement raised, and `error_code` says how. A slow failure is usually the most
  interesting entry in the log. Examples are a lock wait that timed out, or a schema catch-up that
  did not finish.

## Worked example

```sql
SHOW SLOW QUERIES LIKE '%orders%';
```

| seq | duration_ms | kind | rows_returned | rows_read | full_scan | spilled | outcome | sql |
|---|---|---|---|---|---|---|---|---|
| 412 | 4180.2 | select | 12 | 2400000 | true | false | completed | SELECT * FROM orders WHERE region = 'emea' |
| 407 | 2210.7 | select | 500 | 500 | false | true | completed | SELECT * FROM orders ORDER BY total DESC |

The first row read two million rows to return twelve, because `region` is not indexed. The second
read exactly what it returned but spilled. Its problem is memory, and an index would not help. You
should either raise `spill_threshold_rows` or bound the sort with a `LIMIT`. See
[Spill to disk](spill-to-disk.md) and [the query planner guide](query-planner.md).

## On the dashboard

The [operator dashboard](operator-dashboard.md) carries a **Slow queries** panel showing the newest
entries with the same columns. The panel is refreshed at 15-second intervals. It sends
`SHOW SLOW QUERIES` through the engine. This means that the superuser bar described below applies to
the panel too. A non-superuser session sees an error in the panel.

The panel clips statement text to 300 characters — the log's own limit is yours to set and can be
kilobytes, which is a fine size for a SQL client and a poor one for a table cell. The full text is
the cell's tooltip, and the complete statement is in `SHOW SLOW QUERIES`.

It draws at most 50 entries and says how many more are held. It also says when the ring has wrapped,
by comparing the newest `seq` against the capacity.

### Reading the log does not change it

`SHOW SLOW QUERIES` is never recorded, however slow it is and whatever the threshold is set to.

If the statement recorded itself, each read would evict an entry. Anything that polls the log,
including the dashboard panel, would erase the history it exists to show. On a small ring, the
history would be erased completely within a minute.

Other introspection statements are not exempt. With the threshold at `0`, all statements are
recorded. An open dashboard then fills the log with the `SHOW ENGINE STATS` polls it sends. That is
one more reason `0` belongs on an idle node while reproducing something, and not on a busy one.

## Privileges

`SHOW SLOW QUERIES` requires a **superuser**, a higher bar than `SHOW DATABASES` and the same bar as
[`SHOW ENGINE STATS`](engine-stats.md) and [`SHOW VARIABLES`](show-variables.md).

The reason is stronger here than for either of those. The rows contain the literal SQL text of
statements other users ran. A predicate value from a table that the caller has no grant on can
therefore appear verbatim in the output. No per-database grant scopes that down.

Passwords are the exception to "literal". The password in `CREATE USER … IDENTIFIED BY '…'` and both
passwords in `ALTER USER … IDENTIFIED BY '…' REPLACE '…'` are replaced with `'***'` before an entry is
stored. This means that a slow or failed account statement does not leave a credential in the log. A
password passed as a bound parameter is not part of the statement text at all.

## Limits

Know these before you build anything on the log.

1. **It does not survive a restart.** The entries live in memory only. A node that restarts starts
   with an empty log. Durable slow-query history is a different feature.
2. **It is one node's log.** The statement returns the entries of the node that answered it. It
   does not gather entries from peers. Inspect a cluster one node at a time. This is deliberate. A
   node is usually slow by itself, and answering from the leader would hide exactly that.
3. **It is a bounded sample of slow statements.** Once the ring is full, the oldest entry is
   overwritten. Use `seq` to tell whether you are looking at a complete picture.
4. **It is not an audit log.** Statements below the threshold are not recorded, and the text is
   truncated. `SHOW SLOW QUERIES` itself does not appear.

## Related

- [`SHOW ENGINE STATS`](engine-stats.md) — node-level Raft and storage metrics.
- [`SHOW VARIABLES`](show-variables.md) — what this node's configuration actually resolved to.
- [`EXPLAIN` and `EXPLAIN ANALYZE`](explain.md) — per-operator detail for one statement.
- [Statement statistics](statement-stats.md) — `SHOW STATEMENT STATS`, running totals per statement shape, including the fast statements this log does not record.
- [Index usage](index-usage.md) — `SHOW INDEX USAGE`, which indexes statements read and which they do not.
- [Spill to disk](spill-to-disk.md) — what `spilled` means and how to tune it.
- [Operator dashboard](operator-dashboard.md) — the browser panel over the same log.
- [Configuration](configuration.md) — the full settings reference.
