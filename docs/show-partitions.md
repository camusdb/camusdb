# `SHOW PARTITIONS`

CamusDB stores its data in Raft partitions. Partition 0 is the system partition: it holds the
cluster membership and the partition map. The data partitions hold the key spaces of the tables and
their indexes. This statement lists the partitions over SQL, as the node that runs it sees them.

```sql
SHOW PARTITIONS;
```

It answers these questions:

- How many partitions the cluster has, and which ones are active.
- Which node leads each partition, and whether this node leads it.
- Which nodes host each partition, and in which role (voter, learner, removing).
- Which partitions still wait for a replica set.
- Which partition is busy, slow to commit, or has a follower that falls behind.

For the partition that serves one table or index span, see [`SHOW RANGES`](show-ranges.md). For the
Kahuna and Kommander meters of the node, see [`SHOW ENGINE STATS`](engine-stats.md).
`GET /v1/cluster/placement` reports part of the same data over HTTP.

`PARTITIONS` is not a reserved word. It stays usable as a table name, a column name and an alias.
The statement takes no argument and no `LIKE` filter.

## Result columns

One row for each partition. Partition 0 is the first row; the data partitions follow in ascending
`partition_id` order. A removed partition is not shown.

| Column | Type | Meaning |
|--------|------|---------|
| `node` | string | Endpoint of the node that answers. Every value in the row is this node's view. |
| `partition_id` | int64 | Raft partition id. 0 is the system partition. |
| `kind` | string | `system` for partition 0, `data` for every other partition. |
| `state` | string | `active`, `splitting` or `draining`. |
| `routing` | string | `hash_range` or `unrouted`. An unrouted partition receives no hash-routed key; key-range sharding places spans on it. NULL for partition 0. |
| `hash_start` | int64 | Start of the hash range. NULL for partition 0 and for an unrouted partition. |
| `hash_end` | int64 | End of the hash range. NULL for partition 0 and for an unrouted partition. |
| `generation` | int64 | Map generation, the split, merge and move fence. NULL for partition 0. |
| `leader` | string | Leader endpoint. **NULL means unknown, not "no leader".** |
| `leader_source` | string | `local`, `gossip`, or NULL when `leader` is NULL. See [Leader columns](#leader-columns). |
| `leader_is_local` | bool | This node hosts the partition and believes that it leads it. |
| `hosted_locally` | bool | This node hosts the partition. |
| `term` | int64 | Raft term on this node. NULL when this node does not host the partition. |
| `commit_index` | int64 | Commit frontier on this node. NULL when this node does not host the partition, or when the partition has committed nothing. |
| `replication_factor` | int64 | Effective replication factor. 0 means full replication. |
| `voters` | string | Comma-joined endpoints with the voter role. **Empty means legacy full replication, not "no replicas".** |
| `learners` | string | Comma-joined endpoints with the learner role. Empty when none. |
| `removing` | string | Comma-joined endpoints with the removing role. Empty when none. |
| `awaiting_placement` | bool | The partition is active, has no replica set yet, and its replication factor asks for one. |
| `log_ops_per_sec` | float64 | Write rate of the leader (moving average). |
| `wal_queue_depth` | int64 | WAL writes queued at the leader. |
| `commit_wait_ms` | float64 | Time from enqueue to durable write at the leader (moving average). |
| `max_follower_lag` | int64 | The largest number of committed entries that a follower's durable frontier is behind. Leader only. |
| `stalled_followers` | int64 | Number of followers whose disk the leader holds in a WAL stall. Leader only. |

## Node-local answer

The statement reads only the in-memory Raft state of the node that runs it. It calls no peer and
reads no WAL or KV data.

Every node holds the same committed partition map, so every node lists the same partitions. The
leader, the term, the commit index and the follower health are this node's view. During an election,
two nodes can show different values for the same partition. That difference is information, not an
error. To compare nodes, run the statement on each node.

Each run reads live values. The statement uses no cache.

## Leader columns

When this node believes that it leads a partition, `leader` is its own endpoint and
`leader_is_local` is true.

For every other partition, `leader` is a best-effort hint:

- `local`: this node hosts the partition, and the value is its own belief. When this node has no
  belief yet, for example during an election, Kommander gives the newest gossiped claim. The column
  still shows `local` in that case.
- `gossip`: this node does not host the partition. The value is the newest load report in which a
  peer claims the partition.
- NULL: this node knows no leader.

A node learns the leaders of the partitions that it does not host only from load-report gossip.
Gossip is on when `replication_factor` is above 0, when `enable_leader_balancer` or
`enable_placement_rebalancer` is on, or when `enable_load_reports` is on. With gossip off, a node
reports NULL as the leader of each partition that it does not host.

The leader is never a correctness gate. Execution always resolves the leader again, so a stale hint
costs a network hop and not a wrong answer.

## Replica columns

An empty `voters` string means legacy full replication: every roster voter hosts the partition. It
does not mean that the partition has no replicas. The system partition always uses full replication,
so its `voters` string is always empty and its `replication_factor` is 0.

Under replica placement, `voters`, `learners` and `removing` together list every node that hosts the
partition. A learner receives the log but does not vote. A removing replica still hosts the partition
but no longer counts toward the quorum.

`awaiting_placement` is true for a partition that is active, has an empty replica list, and has a
replication factor above 0. Every roster voter hosts such a partition until the placement pass gives
it a replica set.

## Load and follower health

`log_ops_per_sec`, `wal_queue_depth` and `commit_wait_ms` describe the leader. On the leader they come
from the local WAL scheduler. On any other node they come from the last load report of the leader.
**A value of 0 can mean "no report yet"** as well as "idle".

`max_follower_lag` and `stalled_followers` exist only on the node that leads the partition, because
only the leader records what each follower acknowledged. On every other node both columns are NULL.

- The lag of one follower is the leader's commit index minus the follower's durable frontier. A
  follower that never reported a durable frontier counts as behind by the whole log.
- A follower that has not acknowledged the current leader yet is left out. Its lag is unknown, which
  is different from "caught up". When no follower has reported, `max_follower_lag` is NULL.
- Under full replication, the followers are the peers that this node sees.

## NULL values

| Column | NULL when |
|--------|-----------|
| `routing`, `hash_start`, `hash_end`, `generation` | Partition 0, which is not an entry of the hash-routing map. `hash_start` and `hash_end` are also NULL for an unrouted partition. |
| `leader`, `leader_source` | This node knows no leader. |
| `term` | This node does not host the partition. Partition 0 also reports NULL, because Kommander does not give the term of the system partition. |
| `commit_index` | This node does not host the partition, or the partition has committed nothing. |
| `max_follower_lag` | This node does not lead the partition, or no follower has reported. |
| `stalled_followers` | This node does not lead the partition. |

Any other column is NULL only when its read failed. A partition can move or be removed between the
map read and a later read for that partition. The statement then reports NULL in the cells that the
failed read feeds, and still returns every other row.

## Standalone node

A standalone node hosts and leads every partition. Every row shows `leader_is_local` and
`hosted_locally` as true, and `leader` as the local endpoint. The replica columns are empty.

A standalone node has no follower. `max_follower_lag` is NULL and `stalled_followers` is 0. The Raft
roster of a standalone node holds two phantom witness peers. They hold no data, so the statement does
not report them as followers.

## Joining node

A node that is still joining the cluster has no committed partition map yet. It returns the
partition 0 row only. The statement does not wait for the map.

## Privileges

`SHOW PARTITIONS` requires a **superuser** when authentication is enabled. It has the same bar as
`SHOW ENGINE STATS` and `GET /v1/cluster/placement`: leaders, replica sets, commit frontiers and write
load describe the cluster topology and the workload of the whole node, and no per-database grant
limits them. With authentication disabled, any client can run it.

The statement needs no database. It opens no database and runs in no transaction.

## What it does not report

- **No tables or key spaces.** A partition holds the key spaces of many tables. To find the partition
  of a table or index span, use [`SHOW RANGES`](show-ranges.md).
- **No cluster-wide form.** Every column describes the node that answers.
- **No changes.** Leadership transfer, replication-factor change and split stay on their own
  surfaces.
