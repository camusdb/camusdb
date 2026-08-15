# `SHOW ENGINE STATS`

CamusDB embeds Kahuna (the transactional KV store) and Kommander (the Raft implementation) **inside its
own process**. Both libraries publish operational metrics through `System.Diagnostics.Metrics`, and
because they are in-process CamusDB observes them directly and exposes them as a SQL statement.

```sql
SHOW ENGINE STATS;
SHOW ENGINE STATS LIKE 'raft.executor%';
```

This is engine introspection, not table statistics — for the optimizer's view of your data see
[`SHOW STATISTICS FOR <table>`](show-statistics.md), [`ANALYZE`](automatic-analyze.md) and
[the query planner guide](query-planner.md).

## What it is for

Kommander logs latency warnings like this one:

```text
warn: Kommander.IRaft[0] [RaftPartitionExecutor/1] Slow dispatch: CheckLeader took 468ms
```

That line tells you a dispatch was slow *once*. `SHOW ENGINE STATS` tells you whether it is a spike or a
pattern, without leaving the SQL console:

```sql
SHOW ENGINE STATS LIKE 'raft.executor.operation_duration_ms';
```

| node | source | metric | tags | kind | count | total | min | max | last |
|---|---|---|---|---|---|---|---|---|---|
| localhost:8004 | kommander | raft.executor.operation_duration_ms | operation_class=Control,partition_id=1 | histogram | 12043 | 9821.5 | 0.01 | 468.2 | 0.03 |

The `max` is the worst dispatch since the process started; `last` is the most recent one. A `max` far
above the `total/count` average, with a `last` back at normal, is the signature of a periodic stall
rather than sustained pressure — the next section is about finding what it lines up with.

## Result columns

| column | meaning |
|---|---|
| `node` | the local Raft endpoint this row came from |
| `source` | which meter published it: `kommander` or `kahuna` |
| `metric` | instrument name as the library publishes it |
| `tags` | canonical `k=v` pairs, comma-separated and key-sorted; empty when the metric is untagged |
| `kind` | `counter`, `histogram`, or `gauge` |
| `count` | counter total, histogram observation count, or `1` for a sampled gauge |
| `total` | counter total, or histogram sum; NULL for a gauge |
| `min` / `max` / `last` | histogram distribution; `last` only for a gauge; all NULL for a counter |

Rows are ordered by `source`, `metric`, `tags`, so two invocations diff cleanly.

## Reading the window correctly

Three properties will mislead you if you assume otherwise:

- **Counters and histograms are cumulative since process start.** There is no reset. To measure a
  window, run the statement twice and subtract — a single reading of `raft.wal.batches_total` tells you
  nothing about current rate.
- **Gauges have no history.** They are sampled at the instant the statement runs and report only
  `last`. An observable *counter* is reported as a gauge whose `last` is its running total.
- **The statement is node-local and never forwards to the leader.** In a cluster each node answers for
  its own process, which is exactly what you want when one node is slow — but it means you must query
  each node to see the cluster. The `node` column is there so output pasted into an issue says which
  one it came from.

## Correlating a slow dispatch with a background cycle

The usual cause of a once-a-minute latency spike is a periodic background cycle contending with the
Raft executor. Take two readings a minute apart:

```sql
SHOW ENGINE STATS LIKE 'raft.executor%';   -- reading 1
-- wait
SHOW ENGINE STATS LIKE 'raft.executor%';   -- reading 2
```

If `raft.executor.operation_duration_ms.max` jumped between the two readings while
`raft.executor.operations_total` grew only slightly, the stall was not caused by load. Then widen the
filter and look for what *did* advance in the same interval — `kahuna.kv.write.*` for write-path
batching, `raft.wal.*` for WAL flush behavior, `kahuna.backup.*` for a backup or PITR cycle.

Kahuna's **collection-tick** statistics are the most direct answer to that question, and they arrive
with the Kahuna release after `0.9.8` — the version currently referenced, which does not publish them.
Until that package is picked up, a collection tick shows up only indirectly, as a gap that none of the
visible metrics explains. Once it is:

| metric | tells you |
|---|---|
| `kahuna.collect.cycle.duration` | how long a collect cycle held the actor's mailbox thread — spikes here line up with the latency spikes they cause |
| `kahuna.collect.evicted` | entries reclaimed, tagged `reason=tombstone\|expiry\|lru\|idle` |
| `kahuna.collect.inspected` | entries walked, tagged `scan=expiry\|lru`; far exceeding evicted means cycles are burning budget on pinned entries |
| `kahuna.collect.backlogged` | cycles that carried work past their budget — approaching the cycle count means collection is not keeping up |

Useful starting points:

| metric | tells you |
|---|---|
| `raft.executor.operation_duration_ms` | per-operation dispatch latency, by partition and operation class |
| `raft.executor.rejections_total` | proposals refused because a partition queue was full — sustained non-zero means overload |
| `raft.wal.batch_size` | WAL batching efficiency; a mean near 1 means no coalescing is happening |
| `raft.heartbeat_delay_ms` | leader scheduling pressure; well above the heartbeat interval means CPU starvation |
| `raft.elections_started_total` | leadership churn |
| `kahuna.kv.write.batches` / `kahuna.kv.write.entries` | write aggregator effectiveness — entries ÷ batches is the average coalescing factor |

## Permissions

`SHOW ENGINE STATS` requires a **superuser** when authentication is enabled. It is deliberately held to
a higher bar than `SHOW DATABASES`: that statement is filtered down to what the caller can already
reach, whereas engine metrics describe Raft topology and whole-node workload volume, which no
per-database grant scopes down. With authentication disabled, it is available like any other statement.

## Configuration

```yml
engine_metrics_enabled: true   # default
```

Observing the meters costs one delegate call and a dictionary lookup per measurement; with the flag off
no listener is attached and the meters revert to their zero-cost unobserved state. Turn it off only
when benchmarking that overhead itself.

When collection is disabled the statement **succeeds and returns zero rows** rather than raising, so a
script polling a fleet does not have to special-case which nodes have it enabled.

This setting is independent of the `diagnostics:` section, which configures OpenTelemetry/Prometheus
export. Either can be on without the other, and both can read the same meters.
