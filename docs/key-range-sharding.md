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
| Placement of a table's rows | One partition, fixed at startup | One or more ranges, each on a partition |
| Placement of a secondary index | One partition | Its own ranges, independent of the table's |
| Range locks | Cover the whole space | Clipped per range — disjoint scans do not conflict |
| Scans | One partition answers | Every intersecting range answers; results merged in key order |
| Splitting | Not applicable | A range can be divided, on demand or on a size threshold |

Two consequences are worth stating plainly:

**Locking gets finer, not just different.** A bounded scan takes a lock over the bounds it actually
read. Once a space has been divided, that becomes one clipped sub-lock per range it touches, so two
transactions scanning disjoint parts of the same table or index no longer block each other. Under
hash routing the equivalent lock covers the single partition that owns the whole space.

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
disagree will route the same key to different partitions. Change it everywhere, then restart.

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

A secondary index is registered for key-range routing only when **every** one of its key columns uses
the non-`String` ordered encoding — `Integer64`, `Float64`, `Bool`, `Id`, `Null`. An index with any
`String` key column stays hash-routed until the persistence comparator is aligned with the encoding;
the table's rows are unaffected either way.

This is silent by design: a mixed table works, with some spaces ranged and some hashed. If you expect
an index to be splittable and it never divides, check its key column types first.

---

## Splitting a range

A split takes one range `[S, E)` on partition `P` and produces `[S, K)` on `P` and `[K, E)` on a
freshly created partition `P'`. It copies the moving half to `P'`, briefly quiesces writes to it,
takes a final catch-up copy, and then flips routing in a single replicated step.

**Rows on both sides of `K` are required.** A split key with nothing above or below it is refused —
a range would otherwise end up empty and the division would buy nothing.

**Automatic splitting is off unless you ask for it.** Kahuna ships a count-based threshold, and
CamusDB deliberately pins it to `0` rather than inheriting it, so switching on a routing flag cannot
also switch on a rebalancing policy as a side effect. To enable it:

```yml
kahuna:
  range_split_threshold: 1000      # sampled keys in a range before it is considered for splitting; 0 disables
  range_split_min_range_size: 10   # smallest a child range may be
```

`range_split_threshold` must be `0` or at least `2 × range_split_min_range_size`; a threshold below
that can never be satisfied and would sample and back off forever. The server refuses to start on a
configuration that cannot be met.

Splitting a chosen range on demand does not consult the threshold, so leaving it at `0` does not make
the range admin surface unusable.

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

---

## Limits to know about

- **The setting is not runtime-changeable.** Components capture it when they are built.
- **A `String`-keyed index stays hash-routed** (above).
- **Range locks are held in memory by the range's current leader.** A leadership change immediately
  after a lock is confirmed can strand one; this is a Kahuna-level property, not specific to
  CamusDB.
- **Non-transactional direct writes are not covered by a split's quiesce on other nodes.** Kahuna's
  split quiesce for direct, non-transactional writes is enforced only on the node running the split.
  CamusDB never issues such a write — every row and index write it makes is part of a transaction,
  and transactional commits are fenced against the split's range lock wherever they were issued — so
  this does not reach SQL traffic. It matters only if something else writes into CamusDB's key spaces
  through Kahuna directly.
- **Orphaned data after a split is reclaimed by compaction**, not immediately, so disk usage does not
  drop the moment a range moves.
