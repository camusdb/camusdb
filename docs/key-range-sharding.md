# Key-range sharding

CamusDB places data by **hash routing** by default. A table's rows and each of its secondary indexes
are stored under a key prefix that hashes onto exactly one Raft partition, chosen once at startup.
This is simple and cheap, but it sets a ceiling. All write coordination for a table goes through a
single partition leader. This means that a hot table can't use more of the cluster, however many
partitions exist.

**Key-range sharding** routes those key spaces by key order. A space starts as one range that covers
all keys, and a range can later be divided into child ranges owned by *different* partitions. Writes
to different ranges are then coordinated by different leaders.

Hash routing remains the default. Key-range routing is enabled per deployment with
`key_range_sharding`.

---

## What changes when you turn it on

| | Hash routing (default) | Key-range routing |
|---|---|---|
| Placement of a table's rows | One partition, fixed at startup | One or more ranges, each on a partition; the first range starts on the partition hash routing would use |
| Placement of a secondary index | The same partition as the table's rows | Separate ranges, independent of the table's; the first range is seeded on the rows' partition |
| Range locks | One lock covers the space | Clipped per range — disjoint scans do not conflict |
| Scans | One partition answers | The ranges that intersect the scan answer; results are merged in key order |
| Splitting | Not applicable | A range can be divided, on demand or on a size threshold |
| A client transaction's session | On the leader of the first touched table's partition (hashed anchor key) | Pinned to the partition of the first touched table's one row range; unpinned when the row space is already divided |

Two consequences follow.

**Locking gets finer.** A bounded scan takes a lock over the bounds it actually read. Once a space
has been divided, that lock becomes one clipped sub-lock per range it touches. Two transactions that
scan disjoint parts of the same table or index then no longer block each other. Under hash routing,
the equivalent lock covers the single partition that owns the space.

A serializable read-only transaction takes no range lock in this mode either. It reads the ranges at
its pinned timestamp. This means that a writer's lock cannot cause it to be refused, and it cannot
make a writer wait. An autocommit `SELECT` behaves the same way. It gets the same snapshot as under
hash routing and takes no range lock, and it needs no round trip to Kahuna to begin or end it. A scan
in an explicit Read Committed transaction takes no range lock either, as under hash routing. Only a
Serializable read-write transaction locks its scans.

**Scans stay ordered and complete.** A scan resolves the range descriptors that its bounds intersect
and merges the results in key order. A `SELECT` returns the same rows in the same order whether the
space is one range or five. Paging re-resolves against the live map. This means that a range that
divides in the middle of a long scan does not truncate it.

---

## Enabling it

The following `config.yml` enables key-range sharding with four initial data partitions.

```yml
key_range_sharding: true
initial_partitions: 4
```

`CAMUS_KEY_RANGE_SHARDING` overrides the YAML value when set.

The setting is **restart-scoped and cluster-wide**. A node fixes the value when it starts, and nodes
that disagree will route the same key to different partitions.

### The mode is fixed when the cluster is created

A cluster records its routing mode once, the first time a node of the new cluster starts, in the
system key `_system/routing/mode`. Later starts read that record, and the result depends on how
`key_range_sharding` is set on the starting node.

- **`key_range_sharding` unset** (not in `config.yml` and no `CAMUS_KEY_RANGE_SHARDING`): the node
  follows the record. A change of the built-in default therefore applies only to clusters created
  after it. In `SHOW VARIABLES`, the value has the source `cluster`.
- **`key_range_sharding` set to the recorded mode:** the node starts.
- **`key_range_sharding` set to the other mode:** the node does not start. It exits with code 1 and a
  message that names both modes.

A cluster created before the record existed has its mode inferred once, on the first start of the new
version, and then recorded. If a table key space has a range descriptor, the mode is key-range.
Otherwise, if a database is registered, the mode is hash. A cluster that contains no database takes
the configured value.

**Why the mode cannot change on existing data.** A table's first range starts on the partition that
hash routing gives the table. This means that turning key-range sharding on does not move a table that
has not been registered yet. A table that an earlier Kahuna release (before 1.14.7) seeded can sit on
another partition. That release chose the seed with a different hash. Turning the mode off does not
undo it. Kahuna rebuilds its routing registry from the replicated range map. A table that has a range
descriptor is therefore routed by key range, on the partition its descriptor names, whatever the flag
says. To move data to the other mode, dump it and reimport it into a new cluster (see
[logical-dump-and-reimport.md](logical-dump-and-reimport.md)).

**Start an existing node first after the upgrade.** For a cluster created before the record existed,
the inference reads the node's local copy of the range map. A node restores this copy from its WAL
when it starts. A node that joins such a cluster with an empty data directory, before any existing
node has started the new version, does not have a copy yet and would record hash. If that happens, a
node with the flag off returns an error when a statement opens a table that is stored under key-range
routing. This means that the mismatch shows at the first statement and no wrong answer is served.

### Partition count

Registration succeeds on a node started with a single data partition, and the space genuinely is
key-range routed. The range map lives on the reserved meta partition, which means that one data
partition is enough to hold ranged data. However, one partition does not give the *benefit*. A range
has nowhere to move to. The space is one range and write coordination goes through one leader,
exactly as under hash routing.

Set `initial_partitions: 2` or more for the mode to distribute anything. Below that, the server logs a
warning at startup. Enabling the flag on a single-partition node is harmless. You get ordered range
scans and per-range locking, but no distribution.

### Which indexes are eligible

Secondary indexes are registered for key-range routing whatever the types of their key columns,
`String` included. Each type has an order-preserving ASCII key encoding. `KeyEncoder` writes a
`String` value with an ordered ASCII alphabet. This means that the byte order of the stored keys
matches the ordinal (UTF-16) order that the in-memory index path uses. A composite index that mixes
`String` and numeric columns is eligible too.

An index is hash-routed only if its key columns cannot be resolved. This happens when the index entry
lists no column ids, or lists an id that does not match one of the table's columns. Such an entry is
damaged or comes from a legacy schema, whatever the column type. The table's rows are not affected.
This happens silently: the table works, and only that one index space is hashed.

---

## Splitting a range

A split takes one range `[S, E)` on partition `P` and produces `[S, K)` on `P` and `[K, E)` on a
freshly created partition `P'`. It copies the moving half to `P'`, briefly quiesces writes to it,
takes a final catch-up copy, and then flips routing in a single replicated step.

**Rows on both sides of `K` are required.** A split at a key that has rows on one side only is
refused. One range would end up empty, and the division would not help.

**Automatic splitting is off unless you ask for it.** Kahuna has two independent auto-split branches.
CamusDB sets both to `0` and does not inherit Kahuna's defaults. This means that switching on a
routing flag cannot also switch on a rebalancing policy as a side effect.

Splitting a chosen range on demand consults neither threshold. Leaving both at `0` therefore does not
make the range admin surface unusable.

### The count branch: a range holds many keys

```yml
kahuna:
  range_split_threshold: 1000      # sampled keys in a range before it is considered for splitting; 0 disables
  range_split_min_range_size: 10   # smallest a child range may be
```

`range_split_threshold` must be `0` or at least `2 × range_split_min_range_size`. A lower threshold
can't be satisfied, and the splitter would sample and back off forever. The server doesn't start with
a configuration that can't be met.

### The load branch: a partition is saturated

The count branch cannot see the case that hurts most: a small range that receives all the writes. The
load branch decides by heat.

```yml
kahuna:
  range_split_load_threshold: 500          # sustained log ops/sec before a partition counts as hot; 0 disables
  range_split_load_min_queue_depth: 8      # WAL backlog required alongside the rate
  range_split_load_min_commit_wait_ms: 0   # optional third gate; 0 disables it
  range_split_load_window_ms: 15000        # the predicate must hold for this long
  range_split_load_poll_interval_ms: 5000  # how often the predicate is sampled
  range_split_load_imbalance_max: 0.8      # refuse a split this lopsided
  range_split_settle_window_ms: 10000      # leave a fresh child alone for this long
  range_split_indivisible_cooldown_ms: 300000
  range_merge_min_size: 10                 # key count below which two neighbours merge again
  enable_load_reports: true                # gossip load signals without the leader balancer
```

The load branch decides a split in six steps.

1. **Three gates, AND-combined.** The partition's log rate must reach `range_split_load_threshold`,
   its WAL queue depth must reach `range_split_load_min_queue_depth`, and — if you set it — its
   commit wait must reach `range_split_load_min_commit_wait_ms`. The commit-wait gate alone cannot
   trigger a split.
2. **A debounce window.** All three must hold continuously for `range_split_load_window_ms`, sampled
   once per `range_split_load_poll_interval_ms`. A poll interval at or above the window cannot observe
   a sustained window. The server doesn't start with that pair.
3. **A relief guard.** The split is skipped when no live peer is visible. A child range with nowhere
   to go adds a Raft group and relieves no load.
4. **A split key at the write centroid.** The boundary is the key that bisects *writes*, taken from
   the write-frequency histogram. When the histogram is cold, the boundary is the key-count median,
   or the 75th percentile for an append-only pattern.
5. **An indivisibility guard.** A split whose best achievable imbalance reaches
   `range_split_load_imbalance_max` is refused. One hot key produces exactly that shape, and no
   boundary can relieve it.
6. **A settle window.** A fresh child starts warm, with a filtered copy of the parent's histogram. It
   is left alone for `range_split_settle_window_ms` before it is re-evaluated. That window must be at
   least `min_leader_stability_ms`, or the child could be re-split before the balancer may move its
   leader. The server doesn't start with a shorter settle window.

### What the load branch needs before it can work

Load splitting is inert unless all of these hold. The node starts anyway and logs one warning for each
missing precondition.

| Precondition | Why |
|---|---|
| `key_range_sharding: true` | Only a key-range routed space has a range descriptor to split. |
| `initial_partitions >= 2` | A child needs a partition to move to. |
| cluster mode | A standalone node splits, but both children are in the same process. |
| a load-report source | `enable_leader_balancer`, `enable_placement_rebalancer`, a non-zero `replication_factor`, or `enable_load_reports`. If no source is enabled, a partition led on another node appears at 0 ops/sec and cannot be detected as hot. |
| `enable_leader_balancer: true` | Gossip makes the decision correct. To make it effective, the leader balancer must move the child leader off the hot node, and no other component does this. |

The last two are easy to miss. When either is missing, the feature appears to do nothing, and no error
is returned.

### Reading what the splitter did

`SHOW ENGINE STATS` includes five cumulative splitter counters, with one row per key space.

| Counter | Meaning |
|---|---|
| `kahuna.range.splits` | splits committed, by either branch |
| `kahuna.range.split.indivisible_refusals` | the load is on one key; no split can help |
| `kahuna.range.split.no_relief_skips` | no peer was available to host the child |
| `kahuna.range.split.settle_skips` | the range just split; it is being left to settle |
| `kahuna.range.merge.warm_skips` | a merge candidate is too warm to merge yet |

A missing row means the counter has not fired yet. Together the counters separate "not hot yet" from
"hot, and refused". `SHOW RANGES` and `/v1/cluster/placement` can't make that distinction. They show
the shape of the key space, and the splitter's decisions are not visible there.

### Limits of an automatic split

- **An index whose key columns cannot be resolved does not split.** It is hash-routed (see "Which
  indexes are eligible"). A hot index of that kind looks inert for a reason unrelated to these
  settings. Indexes of any column type, `String` included, split like other key spaces.
- **Heat is measured for each Raft partition.** One partition can host several ranges and also carry
  hash-routed traffic. A cold range on a hot partition is therefore a split candidate too.
- **These settings are read once, at startup.** `SET CLUSTER SETTING` cannot change them.

### What a split moves, and what it does not

- **It moves routing, and it copies the data** for the moving half onto the new partition.
- **It does not delete the old copy.** The source's rows for `[K, E)` are left on disk, unreachable
  through routing. Compaction reclaims them later.
- **It carries live range locks with it.** A lock held over a range that divides is clamped to each
  child and re-confirmed there. A transaction that held a lock before the split is protected in both
  children afterwards. It does not have to re-acquire anything or notice that the split happened.
- **It does not stop for a lock holder.** A range divides underneath a foreign range lock and does not
  wait for it.

---

## Behaviour under concurrent writes

A write that is in flight when a boundary moves is refused with a retryable outcome. It is not routed
to the wrong owner. Two distinct refusals can occur, and both are safe to replay.

- **`CADB0504` (must retry)** — the routing generation changed under the write, or the range was
  quiesced. No data was written. The store absorbs most of these internally.
- **`CADB0502` (conflict)** — the transaction's writes fall in a range another transaction has
  locked. This includes the exclusive lock a split takes over the half it is moving. The transaction
  is aborted and none of its writes were applied. Replay it from `BEGIN`.

Single-statement (autocommit) work replays both automatically, up to a bound — see
[serializable-retry-contract.md](serializable-retry-contract.md). An explicit multi-statement
transaction returns them to the client, which must restart the transaction. Retrying only the failed
statement is incorrect: the transaction's earlier reads may no longer hold.

The guarantee is that **a write the client was told had committed survives the split.** A write
refused during the window was never committed.

---

## Interaction with distributed query execution

`distributed_query_execution` divides an eligible scan into one fragment per span, and the fragments
are executed through a `Gather` exchange. Those spans come from range placement. Under hash routing, a
table has a single span and cannot be fragmented. Distributed execution needs key-range sharding to be
useful, while key-range sharding is useful by itself.

If you enable one, you should decide about the other at the same time. Don't let their defaults drift
apart. For more information about which scans can be fragmented today, see
[query-planner.md](query-planner.md).

### How a fragment crosses the wire

A coordinator sends each remote fragment as a `POST` to the peer's `/internal/query-fragment`
endpoint, authenticated by the node secret. The peer streams the surviving rows back one frame at a
time. The two nodes negotiate the encoding for each request.

- **NDJSON** (`application/x-ndjson`) — one JSON object per line, row bytes as base64. Any node can
  read and write it, and it is what a node on an older build sends and expects.
- **Binary** (`application/x-camus-query-fragment-v1`) — length-prefixed frames with the row bytes
  verbatim. A coordinator asks for it in `Accept`; a peer that understands it answers with that
  `Content-Type`, and the coordinator picks its decoder from the `Content-Type` it gets back.

The negotiation makes a rolling upgrade safe in both directions. An older peer ignores the unknown
media type and streams NDJSON. An older coordinator does not ask for binary, and a newer peer streams
NDJSON to it. Frames are flushed one at a time in both encodings. The first row leaves the peer as
soon as it survives the filter, whatever the format. Base64 costs a third more bytes per row than the
binary path and a decode pass on the coordinator. The binary path removes that cost on row-heavy
scans; a fragment carries the same content in both encodings.

---

## Seeing where the data actually went

[`SHOW RANGES`](show-ranges.md) returns one row per span of a relation's key space. Each row shows the
span's bounds in column terms and the partition serving it. It also shows the routing generation and
whether this node believes it leads the span. Use it to confirm that a split happened. A silently
refused split leaves the space as one range, and queries read it correctly. You can also use it to
find which range contains a hot key.

```sql
SHOW RANGES FROM TABLE readings;
SHOW RANGES FROM INDEX readings@amount_idx;
SHOW RANGE  FROM INDEX readings@amount_idx FOR ROW (5000);
```

The output is this node's applied view of the map. That is deliberate, and comparing two nodes is a
reasonable way to spot one that is behind.

## Limits to know about

- **The setting cannot change at runtime.** Components capture it when they are built.
- **An index whose key columns cannot be resolved is hash-routed** (see "Which indexes are
  eligible"). The column type does not matter.
- **Range locks are held in memory by the range's current leader.** A leadership change immediately
  after a lock is confirmed can strand one. This is a Kahuna-level property and is not specific to
  CamusDB.
- **Non-transactional direct writes are not covered by a split's quiesce on other nodes.** Kahuna
  enforces the split quiesce for direct, non-transactional writes only on the node that performs the
  split. CamusDB does not issue such writes. All of its row and index writes are part of a
  transaction, and transactional commits are fenced against the split's range lock wherever they were
  issued. This means that the gap does not affect SQL traffic. It matters only if something else
  writes into CamusDB's key spaces through Kahuna directly.
- **A session is pinned only while the first table's row space is one range.** The pin needs Kahuna
  1.13.4 or later on all nodes. An older node returns an error for operation registrations on a
  pinned session. After the row space is divided, a new client transaction gets an unpinned session
  on an arbitrary partition. Its registrations and commit are then usually forwarded.
- **Orphaned data after a split is reclaimed later by compaction.** Disk usage does not drop the
  moment a range moves.
- **A table's first partition is a hash of the table, as under hash routing.** With a handful of
  tables, two of them can start on one partition while another partition gets none. Only a split or
  a manual range move changes a range's partition later. The leader balancer moves leaders and does
  not move ranges. A new index or large-value space of a table starts on the same partition as the
  table's other spaces, and its hash is not used. This means that a table is on one partition until
  its first split.
