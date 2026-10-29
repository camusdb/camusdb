# Key-range sharding

CamusDB places data by **hash routing** by default: a table's rows and each of its secondary indexes
live under a key prefix that hashes onto exactly one Raft partition, chosen once at startup. That is
simple and cheap, but it fixes a ceiling — all write coordination for a table goes through a single
partition leader, so a hot table cannot use more of the cluster no matter how many partitions exist.

**Key-range sharding** routes those key spaces by key order instead. A space starts as one range
covering everything, and a range can later be divided into child ranges owned by *different*
partitions. Writes to different ranges are then coordinated by different leaders.

Hash routing remains the default. Key-range routing is enabled per deployment with
`key_range_sharding`.

---

## What changes when you turn it on

| | Hash routing (default) | Key-range routing |
|---|---|---|
| Placement of a table's rows | One partition, fixed at startup | One or more ranges, each on a partition; the first range starts on the partition hash routing would use |
| Placement of a secondary index | The same partition as the table's rows | Its own ranges, independent of the table's; the first range is seeded on the rows' partition |
| Range locks | Cover the whole space | Clipped per range — disjoint scans do not conflict |
| Scans | One partition answers | Every intersecting range answers; results merged in key order |
| Splitting | Not applicable | A range can be divided, on demand or on a size threshold |
| A client transaction's session | On the leader of the first touched table's partition (hashed anchor key) | Pinned to the partition of the first touched table's one row range; unpinned when the row space is already divided |

Two consequences are worth stating plainly:

**Locking gets finer, not just different.** A bounded scan takes a lock over the bounds it actually
read. Once a space has been divided, that becomes one clipped sub-lock per range it touches, so two
transactions scanning disjoint parts of the same table or index no longer block each other. Under
hash routing the equivalent lock covers the single partition that owns the whole space. A serializable
read-only transaction takes no range lock in this mode either: it reads every range at its pinned
timestamp, so it is never refused by a writer's lock and never makes a writer wait. An autocommit
`SELECT` is the same: it gets the snapshot it gets under hash routing, with no round trip to Kahuna to
begin or end it and no range lock. A scan in an explicit Read Committed transaction takes no range lock
either, as under hash routing. Only a Serializable read-write transaction locks its scans.

**Scans stay ordered and complete.** A scan resolves every range descriptor its bounds intersect and
merges the results in key order, so a `SELECT` returns the same rows in the same order whether the
space is one range or five. Paging re-resolves against the live map, so a range that divides in the
middle of a long scan does not truncate it.

---

## Enabling it

In `config.yml`:

```yml
key_range_sharding: true
initial_partitions: 4
```

`CAMUS_KEY_RANGE_SHARDING` overrides the YAML value when set.

The setting is **restart-scoped and cluster-wide**: a node fixes it when it starts, and nodes that
disagree will route the same key to different partitions.

### The mode is fixed when the cluster is created

A cluster records its routing mode once, the first time a node of the new cluster starts, in the
system key `_system/routing/mode`. Every later start reads that record:

- **`key_range_sharding` unset** (not in `config.yml` and no `CAMUS_KEY_RANGE_SHARDING`): the node
  follows the record. A change of the built-in default therefore applies only to clusters created
  after it. `SHOW VARIABLES` reports the value with the source `cluster`.
- **`key_range_sharding` set to the recorded mode:** the node starts.
- **`key_range_sharding` set to the other mode:** the node refuses to start and exits with code 1. The
  message names both modes.

A cluster created before the record existed has its mode inferred once, on the first start of the new
version, and then recorded: a table key space that has a range descriptor means key-range, a
registered database without one means hash, and a cluster with no database takes the configured value.

**Why the mode cannot change on existing data.** A table's first range starts on the partition that
hash routing gives the table, so turning key-range sharding on does not move a table that has not been
registered yet. A table that an earlier Kahuna release (before 1.14.7) seeded can sit on another
partition: that release chose the seed with a different hash. Turning the mode off does not undo it:
Kahuna rebuilds its routing registry from the replicated range map, so a table that has a range
descriptor stays key-range routed whatever the flag says, on the partition its descriptor names. To
move data to the other mode, dump it and reimport it into a new cluster (see
[logical-dump-and-reimport.md](logical-dump-and-reimport.md)).

**Start an existing node first after the upgrade.** The inference for a cluster without a record reads
the node's own copy of the range map, which a node restores from its WAL when it starts. A node that
joins such a cluster with an empty data directory, before any existing node has started the new
version, has no copy yet and would record hash. If that ever happens, a node with the flag off refuses
to open a table that is stored under key-range routing, so the mismatch shows at the first statement
and no wrong answer is served.

### Partition count

Registration succeeds on a node started with a single data partition, and the space genuinely is
key-range routed — the range map itself lives on the reserved meta partition, so one data partition
is enough to hold ranged data. What one partition cannot give you is the *benefit*: a range has
nowhere to move to, so the space stays one range and write coordination stays on one leader, exactly
as under hash routing.

Set `initial_partitions: 2` or more for the mode to distribute anything. Below that the server logs a
warning at startup. Enabling the flag on a single-partition node is harmless — you get ordered range
scans and per-range locking — but it is not distribution.

### Which indexes are eligible

Every secondary index is registered for key-range routing, whatever the types of its key columns,
`String` included. Each type has an order-preserving ASCII key encoding: `KeyEncoder` writes a
`String` value with an ordered ASCII alphabet, so the byte order of the stored keys matches the
ordinal (UTF-16) order that the in-memory index path uses. A composite index that mixes `String`
and numeric columns is eligible too.

The only index that stays hash-routed is one whose key columns cannot be resolved: an entry with no
column ids, or with an id that the table's columns do not hold. That is a damaged or legacy schema
entry, not a choice of type, and the table's rows are unaffected either way. This is silent: a table
with such an entry works, with that one index space hashed.

---

## Splitting a range

A split takes one range `[S, E)` on partition `P` and produces `[S, K)` on `P` and `[K, E)` on a
freshly created partition `P'`. It copies the moving half to `P'`, briefly quiesces writes to it,
takes a final catch-up copy, and then flips routing in a single replicated step.

**Rows on both sides of `K` are required.** A split key with nothing above or below it is refused —
a range would otherwise end up empty and the division would buy nothing.

**Automatic splitting is off unless you ask for it.** Kahuna has two independent auto-split
branches, and CamusDB pins both to `0` rather than inheriting Kahuna's defaults, so switching on a
routing flag cannot also switch on a rebalancing policy as a side effect.

Splitting a chosen range on demand consults neither threshold, so leaving both at `0` does not make
the range admin surface unusable.

### The count branch: a range holds many keys

```yml
kahuna:
  range_split_threshold: 1000      # sampled keys in a range before it is considered for splitting; 0 disables
  range_split_min_range_size: 10   # smallest a child range may be
```

`range_split_threshold` must be `0` or at least `2 × range_split_min_range_size`; a threshold below
that can never be satisfied and would sample and back off forever. The server refuses to start on a
configuration that cannot be met.

### The load branch: a partition is saturated

The count branch cannot see the case that hurts most — a small range that carries the whole write
rate. The load branch splits on heat instead of size.

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

How a split is decided:

1. **Three gates, AND-combined.** The partition's log rate must reach `range_split_load_threshold`,
   its WAL queue depth must reach `range_split_load_min_queue_depth`, and — if you set it — its
   commit wait must reach `range_split_load_min_commit_wait_ms`. The commit-wait gate can never fire
   on its own.
2. **A debounce window.** All three must hold continuously for `range_split_load_window_ms`, sampled
   every `range_split_load_poll_interval_ms`. A poll interval at or above the window can never
   observe a sustained window, so the server refuses to start on that pair.
3. **A relief guard.** The split is skipped when no live peer is visible, because a child range with
   nowhere to go adds a Raft group and relieves nothing.
4. **A split key at the write centroid.** The boundary is the key that bisects *writes*, taken from
   the write-frequency histogram — not the key that bisects the key count. When the histogram is
   cold it falls back to the count median, or to the 75th percentile for an append-only pattern.
5. **An indivisibility guard.** A split whose best achievable imbalance reaches
   `range_split_load_imbalance_max` is refused. One hot key produces exactly that shape, and no
   boundary can relieve it.
6. **A settle window.** A fresh child inherits a filtered histogram, so it starts warm. It is left
   alone for `range_split_settle_window_ms` before it is re-evaluated. That window must be at least
   `min_leader_stability_ms`, or the child could be re-split before the balancer may move its
   leader; the server refuses to start on that pair too.

### What the load branch needs before it can work

Load splitting is inert unless all of these hold. The node still starts in every case, and logs one
warning per cause.

| Precondition | Why |
|---|---|
| `key_range_sharding: true` | A hash-routed space has no range descriptor to split. |
| `initial_partitions >= 2` | A child needs a partition to move to. |
| cluster mode | A standalone node splits, but both children stay in the same process. |
| a load-report source | `enable_leader_balancer`, `enable_placement_rebalancer`, a non-zero `replication_factor`, or `enable_load_reports`. Without gossip, a partition led on another node reports 0 ops/sec and is never seen as hot. |
| `enable_leader_balancer: true` | Gossip alone makes the decision correct, not effective. Nothing else moves the child leader off the hot node. |

The last two are the traps. Both present as "the feature does nothing" and neither is an error.

### Reading what the splitter did

`SHOW ENGINE STATS` reports five cumulative counters, one row per key space:

| Counter | Meaning |
|---|---|
| `kahuna.range.splits` | splits committed, by either branch |
| `kahuna.range.split.indivisible_refusals` | one key holds the load; no split can help |
| `kahuna.range.split.no_relief_skips` | no peer was available to host the child |
| `kahuna.range.split.settle_skips` | the range just split; it is being left to settle |
| `kahuna.range.merge.warm_skips` | a merge candidate is still too warm to merge |

An absent row means the counter has never fired. Together they separate "not hot yet" from "hot, and
refused" — which `SHOW RANGES` and `/v1/cluster/placement` cannot, because those report the shape of
the key space rather than the splitter's decisions.

### Limits of an automatic split

- **An index whose key columns cannot be resolved never splits.** It stays hash-routed (see "Which
  indexes are eligible"), so a hot index of that kind looks inert for a reason unrelated to these
  settings. Indexes of every column type, `String` included, split like any other key space.
- **Heat is measured per Raft partition, not per range.** One partition can host several ranges and
  also carry hash-routed traffic, so a cold range on a hot partition is a split candidate too.
- **These settings are read once, at startup.** `SET CLUSTER SETTING` cannot change them.

### What a split moves, and what it does not

- **It moves routing, and it copies the data** for the moving half onto the new partition.
- **It does not delete the old copy.** The source's rows for `[K, E)` stay on disk, unreachable
  through routing. They are reclaimed later by compaction, not by the split.
- **It carries live range locks with it.** A lock held over a range that divides is clamped to each
  child and re-confirmed there, so a transaction holding a lock before the split is still protected
  in both children afterwards — it does not have to re-acquire anything, and it does not have to
  notice the split happened.
- **It does not stop for a lock holder.** A range divides underneath a foreign range lock rather than
  waiting for it.

---

## Behaviour under concurrent writes

A write that is in flight when a boundary moves is refused with a retryable outcome rather than being
routed to the wrong owner. There are two distinct refusals and both are safe to replay:

- **`CADB0504` (must retry)** — the routing generation changed under the write, or the range was
  quiesced. Nothing was written. The store absorbs most of these internally.
- **`CADB0502` (conflict)** — the transaction's writes fall inside a range another transaction has
  locked, which includes the exclusive lock a split takes over the half it is moving. The whole
  transaction is aborted and nothing was written; replay it from `BEGIN`.

Single-statement (autocommit) work replays both automatically, bounded — see
[serializable-retry-contract.md](serializable-retry-contract.md). An explicit multi-statement
transaction surfaces them to the client, which must restart the transaction; retrying just the failed
statement is incorrect, because the transaction's earlier reads may no longer hold.

The guarantee is: **a write the client was told had committed is still there after the split.** A
write refused during the window was never committed.

---

## Interaction with distributed query execution

`distributed_query_execution` fragments an eligible scan into one fragment per span and runs them
through a `Gather` exchange. Those spans come from range placement — under hash routing a table
reports a single span and there is nothing to fragment. So the two settings are related in one
direction: distributed execution does nothing useful without key-range sharding, while key-range
sharding is useful on its own.

If you enable one, decide about the other at the same time rather than letting the defaults drift
apart. See [query-planner.md](query-planner.md) for what fragmentation is eligible today.

### How a fragment crosses the wire

A coordinator sends each remote fragment as a `POST` to the peer's `/internal/query-fragment`
endpoint, authenticated by the node secret. The peer streams the surviving rows back one frame at
a time, in one of two encodings that the two nodes negotiate per request:

- **NDJSON** (`application/x-ndjson`) — one JSON object per line, row bytes as base64. Every node
  can read and write it, and it is what a node on an older build sends and expects.
- **Binary** (`application/x-camus-query-fragment-v1`) — length-prefixed frames with the row bytes
  verbatim. A coordinator asks for it in `Accept`; a peer that understands it answers with that
  `Content-Type`, and the coordinator picks its decoder from the `Content-Type` it gets back.

The negotiation makes a rolling upgrade safe in both directions. An older peer ignores the unknown
media type and streams NDJSON; an older coordinator never asks, so a newer peer streams NDJSON to
it. Frames are flushed one at a time in both encodings, so the first row leaves the peer as soon as
it survives the filter regardless of the format chosen. Base64 costs a third more bytes per row than
the binary path and a decode pass on the coordinator; the binary path exists to remove that cost on
row-heavy scans, not to change what a fragment carries.

---

## Seeing where the data actually went

[`SHOW RANGES`](show-ranges.md) reports one row per span of a relation's key space — its bounds in
column terms, the partition serving it, its routing generation, and whether this node believes it
leads it. Use it to confirm a split actually happened (a silently refused split leaves the space as
one range, which every query still reads correctly), and to find which range holds a hot key:

```sql
SHOW RANGES FROM TABLE readings;
SHOW RANGES FROM INDEX readings@amount_idx;
SHOW RANGE  FROM INDEX readings@amount_idx FOR ROW (5000);
```

Everything it reports is this node's applied view of the map. That is deliberate, and comparing two
nodes is a reasonable way to spot one that is behind.

## Limits to know about

- **The setting is not runtime-changeable.** Components capture it when they are built.
- **An index whose key columns cannot be resolved stays hash-routed** (see "Which indexes are
  eligible"). The column type does not matter.
- **Range locks are held in memory by the range's current leader.** A leadership change immediately
  after a lock is confirmed can strand one; this is a Kahuna-level property, not specific to
  CamusDB.
- **Non-transactional direct writes are not covered by a split's quiesce on other nodes.** Kahuna's
  split quiesce for direct, non-transactional writes is enforced only on the node running the split.
  CamusDB never issues such a write — every row and index write it makes is part of a transaction,
  and transactional commits are fenced against the split's range lock wherever they were issued — so
  this does not reach SQL traffic. It matters only if something else writes into CamusDB's key spaces
  through Kahuna directly.
- **A session is pinned only while the first table's row space is one range.** The pin needs Kahuna
  1.13.4 or later on every node, because an older node refuses operation registrations for a pinned
  session. After the row space is divided, a new client transaction gets an unpinned session on an
  arbitrary partition, so its registrations and commit are usually forwarded.
- **Orphaned data after a split is reclaimed by compaction**, not immediately, so disk usage does not
  drop the moment a range moves.
- **A table's first partition is a hash of the table, as under hash routing.** With a handful of
  tables, two of them can start on one partition while another partition holds none. Only a split or
  a manual range move changes a range's partition later; the leader balancer moves leaders, not
  ranges. A new index or large-value space of a table starts with the table's other spaces, not at
  its own hash, so a table stays on one partition until its first split.
