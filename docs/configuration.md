# Configuration

CamusDB reads `CamusDB/Config/config.yml` at startup and merges CLI flags and environment
variables into a single resolved configuration object.

To see what a running node actually resolved — including which layer supplied each value — run
[`SHOW VARIABLES`](show-variables.md) against it rather than reconstructing the merge by hand.

## Precedence

Highest wins:

1. **CLI flags** — only flags you explicitly pass override YAML (nullable options; no sentinel defaults).
2. **Environment variables** — currently `CAMUS_KEY_RANGE_SHARDING` overrides `key_range_sharding`
   (what that setting does: [key-range-sharding.md](key-range-sharding.md)).
3. **`config.yml`**
4. **Built-in defaults** in `ConfigDefinition` / `CamusDBConfig`.

Example: YAML `mode: cluster` with `--mode standalone` starts in standalone mode. YAML
`mode: cluster` with no `--mode` flag stays in cluster mode.

## CLI ↔ YAML mapping

| YAML field | CLI flag | Default |
|------------|----------|---------|
| `data_dir` | `--data-dir` | `Data` (process cwd) |
| `mode` | `--mode` | `standalone` |
| `memory_profile` | `--memory-profile` | `prod` |
| `node_name` | `--raft-nodename` | `""` (cluster: machine name) |
| `raft_node_id` | `--raft-nodeid` | `1` |
| `raft_host` | `--raft-host` | `localhost` |
| `raft_port` | `--raft-port` | `7070` |
| `initial_partitions` | `--initial-cluster-partitions` | `1` |
| `peers` | `--initial-cluster` | `[]` |
| `http_peers` | `--http-peers` | `[]` |
| `schema_ack_wait_timeout_ms` | `--schema-ack-wait-timeout-ms` | `30000` |
| `schema_ack_live_node_lease_ms` | `--schema-ack-live-node-lease-ms` | `30000` |
| `http_port` | `--http-port` | `5095` |
| `https_port` | `--https-port` | `7141` |
| `https_certificate` | `--https-certificate` | `""` |
| `raft_certificate` | `--raft-certificate` | `""` |
| `require_tls_when_auth_enabled` | `--require-tls-when-auth-enabled` | `true` |
| `default_isolation_level` | — | `serializable` |
| `default_transaction_locking` | — | `pessimistic` |
| `default_transaction_priority` | — | `normal` |
| `transaction_admission_wait_ms` | — | `0` (node default) |
| `range_lock_expires_ms` | — | `30000` |
| `range_lock_heartbeat_interval_ms` | — | `10000` |
| `max_serializable_transaction_lifetime_ms` | — | `3600000` |
| `lock_escalation_threshold` | — | `50` |
| `lock_wait_deadline_ms` | — | `500` |
| `key_range_sharding` | — (`CAMUS_KEY_RANGE_SHARDING` env) | `false` |
| `stats_flush_interval_ms` | — | `5000` |
| `sql_parser_cache_ttl_seconds` | — | `300` |
| `sql_parser_cache_max_entries` | — | `2048` |
| `sql_parser_cache_sweep_seconds` | — | `60` |
| `spill_enabled` | — | `false` |
| `spill_threshold_rows` | — | `500000` |
| `spill_merge_fan_in` | — | `16` |
| `query_result_cache_enabled` | — | `true` |
| `query_result_cache_default_ttl_ms` | — | `5000` |
| `query_result_cache_max_entries` | — | `1024` |
| `query_result_cache_max_bytes` | — | `67108864` |
| `query_result_cache_max_entry_bytes` | — | `1048576` |
| `query_result_cache_max_entry_rows` | — | `10000` |
| `query_result_cache_max_deps` | — | `4096` |
| `query_result_cache_max_point_deps` | — | `2048` |
| `query_result_cache_max_ranges` | — | `256` |
| `query_result_cache_singleflight_wait_ms` | — | `250` |
| `query_result_cache_strict_validation_max_keys` | — | `10000` |
| `query_result_cache_sweep_interval_ms` | — | `10000` |
| `slow_query_log_enabled` | — | `false` |
| `slow_query_log_threshold_ms` | — | `1000` |
| `slow_query_log_max_entries` | — | `200` |
| `slow_query_log_max_sql_length` | — | `4096` |
| `kahuna.*` | — | mode-specific baseline |

Parser-cache, lock/isolation, spill, and query-result-cache knobs are YAML-only (operational tuning,
not per-node startup flags). The result cache is **on by default** (opt-in per query via a
`{cache=…}` hint); set `query_result_cache_enabled: false` to turn it off entirely. See
[query-result-cache.md](query-result-cache.md) for what each knob does and operator guidance.

The slow-query-log knobs are YAML-only too. The log is **off by default**; see
[slow-query-log.md](slow-query-log.md) for sizing guidance and for which of the four take effect
without a restart.

## Kahuna engine section

The nested `kahuna:` map is an allow-listed passthrough to `EmbeddedKahunaOptions`, used for
both the cluster node (`Program.cs`) and standalone per-database nodes (`DatabaseOpener`).
Unset keys keep the CamusDB baseline for that mode. Unknown keys fail validation at startup.

The authoritative allow-list is `KahunaOptionsConfig.AllowedYamlKeys`; the commented `kahuna:` block
in `CamusDB/Config/config.yml` documents each key with its meaning. Broadly it covers storage and
WAL backends, transaction timeouts and admission control, worker/IO-thread counts, Raft election and
heartbeat timings, the cache and eviction knobs described below, RocksDB shared memory, and backup /
PITR settings. A rejected key's error message lists every accepted one.

`storage_revision` names the directory Kahuna opens under `{data_dir}/kv`. The CamusDB baseline
pins it, together with the WAL revision, to the revision of the current key layout (`v2`). Do not
set it to an older revision to reach older data: the key layout of that data is not readable by the
current version, and every table would appear empty. A server that finds an older revision's
directory logs a warning at start and leaves it untouched; move the data with a logical dump and
reimport, see [logical-dump-and-reimport.md](logical-dump-and-reimport.md).

Entry eviction is governed by two mechanisms: **size-based** caps (`max_entries_per_actor`,
`max_bytes_per_actor`) that bound how much an actor holds in memory, and a **time-based**
collection sweep (`collection_interval_ms`) that evicts up to `cache_entries_to_remove` entries
older than `cache_entry_ttl_ms` each pass. Raft-log compaction is governed together by
`compact_every_operations` (how often), `compact_number_entries` (trailing entries kept), and
`max_entries_per_compaction` (per-pass removal cap).

`compact_every_operations` counts persisted WAL **batches**, not log entries: Kommander's
`RaftWriteAhead.NotifyCommitted` decrements once per batch, and under load a batch carries on the order of
a hundred entries. With a RocksDB Raft log the log is reclaimed by dropping whole SST files below a persisted
compaction floor. On Kommander 1.6.0 that floor advanced at most `max_entries_per_compaction` rows per pass,
which at Kahuna's defaults (1,000 batches, 5,000 rows) reclaimed a few hundred entries per second against
~9,000 written on the 2026-09-10 write probe and let the live log grow ~100 MB per minute per node; CamusDB
shipped a temporary 100 / 100,000 default for that version. Kommander 1.6.1 (Kahuna 1.7.6) advances the floor
to the true floor on every pass, so the Kahuna defaults are in force again and the cadence only decides how
often a pass runs.

Storage backends: `memory`, `sqlite`, `rocksdb`.

### One-phase apply-time validation

`kahuna.one_phase_apply_time_validation` (default **on**) lets a read-modify-write or read-carrying
transaction commit in **one** durable round instead of two: the bundled commit carries its
on-partition read dependencies, and every replica judges them at apply time, in log order, against the
partition's replicated committed-head ledger. With it off, those transactions pay one extra Raft
proposal plus the replica-fence exchange per commit. Eligibility is decided by the transaction, not by
the flag: only one participant partition, the anchor on that partition, and a read set the gate can
decide there. A cross-partition transaction stays on two-phase commit either way. CamusDB's grouped key
layout is what makes it pay: a table row and its index entries hash to the same partition, so a
single-row update is a one-partition transaction. Measured on a three-partition `accounts` workload,
turning it on is worth about **+24% throughput** at a 99.8% one-phase rate.

`kahuna.staged_base_fence_retention_ms` (default 600,000) is the horizon the fence — and the ledger the
gate judges against — remembers a key's last transactionally committed head for. It must comfortably
exceed the longest transaction lifetime the deployment allows: a transaction that began before the
horizon is refused, because pruned memory is indistinguishable from "no commit happened".

Three operating rules, all of them enforced by Kahuna rather than by CamusDB:

- **Same value on every node of the group** — for both keys. The gate is a property of the replicated
  command, and nodes with different horizons would judge the same bundled commit differently.
- **Never enable it across mixed Kahuna versions.** A node too old to know the check skips it and
  commits where a current node refuses, which forks the state machine. Set
  `one_phase_apply_time_validation: false` for the duration of a rolling upgrade and remove the override
  once the last old node is gone.
- **Start once with it off.** A node that starts with it on over a prepared-intent snapshot written
  before the ledger existed fails at startup, and refuses to install a partition snapshot exported
  without a ledger.

Because the default is on, the last rule is an **upgrade step**, not a one-time setup note. Bringing a
data directory written by a build that predates the committed-head ledger into a build that defaults the
gate on will fail to start. Upgrading such a deployment:

1. Set `one_phase_apply_time_validation: false` in the `kahuna:` block before starting the new build.
2. Start the cluster and let a checkpoint run, which rewrites every partition's snapshot with its ledger.
3. Remove the override and restart.

A cluster created on a build that defaults it on needs none of this — its first snapshot already carries
a ledger.

### Persistent MVCC revision retention

Every version of every row is a physical row in the KV store. Without pruning, a hot table's history —
and the disk behind it — grows without bound for the life of the store.

By default (both keys unset), CamusDB bounds the persisted history **by age, aligned with the effective
PITR window** (`kahuna.pitr_window_seconds`, default 3600): Kahuna's background writer prunes revision
rows older than the window. An as-of restore inside the window always finds the revisions it needs — a
key untouched since before the window restores from its current row, which pruning never removes.
Snapshot reads older than the retention age are **not guaranteed**; readers that must reach further
back (branch forks) pin their own snapshot floors, which pruning honors.

- `kahuna.persistent_revision_retention_age_seconds` — explicit age bound. Must be ≥ the effective
  PITR window (pruning below the window deletes history an in-window restore may need — the backup
  machinery would then fail closed). `0` disables age pruning and, together with an unset count,
  restores the unbounded-history behavior.
- `kahuna.persistent_revision_retention_count` — hard per-key revision cap, `0` (default) disabled.
  A count cap can cut below the PITR window on a hot key; prefer the age bound unless a per-key cap
  is the goal.

### Memory profile

`memory_profile` (`--memory-profile`) selects *how* the four cache-sizing knobs below are defaulted.
It changes nothing else — not worker counts, not durability, not any behavior an application can
observe other than how often a read is served from cache instead of disk.

| Profile | Block cache | Memtable sub-budget | Actor caches | Total |
|---------|-------------|---------------------|--------------|-------|
| `prod` (default) | 10% of RAM, `[320 MiB, 2 GiB]` | ¼ of it, `[128 MiB, 1 GiB]` | 6.25% of RAM, ≥ 64 MiB | ~16% of RAM (~1.5 GiB on an 8 GiB box) |
| `dev` | 64 MiB | 16 MiB | 32 MiB | ~96 MiB, on any machine |

`dev` is for a node sharing a developer machine with the application being built against it: the
budgets are fixed, so the same node is the same size on a 64 GiB workstation and in a 2 GiB
container. The cost is throughput once the working set outgrows the cache — the TPC-C run that
motivated proportional sizing was ~5x slower against a 320 MiB block cache — so it is not a server
setting. Note that the caches are ceilings filled lazily: a burst of large writes can still push
process RSS well above the cache total, because transient managed allocations and the .NET GC (server
GC by default, one heap per core) dominate the peak. `DOTNET_gcServer=0` in the environment is the
lever for that half of the footprint; it is a runtime setting, not a CamusDB one.

An explicit `kahuna.*` budget always beats the profile, so `dev` plus one raised budget is a valid
combination rather than a conflict.

### Memory-proportional cache defaults

Under `memory_profile: prod`, most unset keys keep Kahuna's own default, but the four cache-sizing
knobs are an exception: when left unset they are computed at startup from the machine's available
memory (container limits respected) rather than from a fixed constant. A fixed 320 MB block cache was measured forcing a
1.2 GB TPC-C working set through disk reads on nearly every statement; sizing it to the machine
took the same workload from 24.5 to 119.6 tx/s at 8 clients.

| Key | Computed when unset | Clamp |
|-----|---------------------|-------|
| `rocksdb_shared_memory_budget_mb` | 10% of RAM | 320 MiB – 2 GiB |
| `rocksdb_shared_memtable_budget_mb` | a quarter of the block cache | 128 MiB – 1 GiB |
| `max_bytes_per_actor` | 6.25% of RAM (≥ 64 MiB for the layer) ÷ `key_value_workers` | 8 MiB – 2 GiB per actor |
| `max_entries_per_actor` | `max_bytes_per_actor` ÷ ~512 B | 10k – 4M |

That is roughly 16% of RAM across both cache layers, and never more than 4 GiB in total however
large the machine is. The fractions and the ceilings are deliberately modest: an unconfigured node
is far more often a developer workstation or a CI container sharing the box with a compiler and an
IDE than a dedicated database server. An explicit value always wins over the computed one. On an
8 GiB, 8-core machine with none of them set: 819 MiB block cache, 204 MiB memtable sub-budget, and
64 MiB × 8 = 512 MiB of actor caches — about 1.5 GiB.

A dedicated server should raise all four explicitly; the sizing above is a floor to build from, not
a recommendation for a machine whose only job is CamusDB.

Note that the 6.25% share and its 64 MiB floor bound the actor-cache layer *as a whole*, and are
then divided by `key_value_workers`; only the 8 MiB per-actor minimum is per actor. Adding cores
therefore splits the same budget more ways rather than growing it — a machine with many cores
relative to its RAM does not end up with a multiple of the intended share.

The RocksDB pair is shared: `rocksdb_shared_memory` (default on, and a no-op unless `storage` and
`wal_storage` are both `rocksdb`) makes one block cache and one write-buffer manager serve both the
KV store and the Raft WAL. The memtable sub-budget is charged **inside** the total block-cache
budget, not added to it, and must be ≤ it. That comparison is made against the *effective*
post-merge pair, so overriding only one of the two can produce an inconsistent pair — a 100 MiB
total against a computed 512 MiB memtable — and fails startup with `InvalidConfig`. Set both
together. Likewise `max_bytes_per_actor` is **per actor**: multiply by `key_value_workers` (default:
one per CPU) to get the total.

`kahuna.key_value_write_max_in_flight_batches_per_partition` (default **1**, Kahuna 1.7.1) is how many
write-aggregator batches a partition may have waiting on Raft at once. At 1 the next batch is dispatched
only when the previous completes, so a partition's batch rate is bounded by 1 / (Raft round latency) and
throughput scales only with items per batch. Higher values pipeline batches in FIFO order; leave it at 1
until an interleaved A/B on the soak harness shows the gain for your shape. Measured 2026-09-09 on the
`bank` candidate arm with the device held constant: 2 and 4 in flight lowered throughput 15-20% — the
Raft round stretched with the in-flight count while batches got smaller.

`kahuna.key_value_write_linger_ms` (default **1**) and `kahuna.key_value_write_max_batch_items` (default
**512**) shape the write aggregator's batches: the linger is how long the oldest queued item may wait before
a batch is dispatched while nothing is in flight, the cap bounds a batch's items. The measured round costs
~1.4 ms fixed plus ~18 µs per item, so denser batches are the lever at one partition; a linger above the
~2.6 ms an item already waits trades write latency for density. This is the aggregator's linger, not
`wal_group_commit_linger_ms` (Kommander's cross-partition WAL group commit, inert at one partition).
Measured 2026-09-09: the linger itself is inert at full occupancy (1, 3 and 5 ms all gave the same
batches), because a completing batch re-dispatches whatever is queued.

`kahuna.key_value_write_post_completion_hold_ms` (default **0**, Kahuna 1.7.2) is the knob that does add
density at full occupancy: after a batch completes the aggregator holds this long before dispatching
the next, so more items accumulate per batch; the cost is that much added write latency. Qualify with an
A/B before changing it.

`kahuna.rocksdb_direct_reads` (default **off** in CamusDB; Kahuna's own default is on) selects how the
RocksDB key/value backend reads SST files. With direct I/O the block cache is the only in-RAM read
cache and every miss is a physical device read. On a node whose disk is already carrying the Raft
WAL's fsyncs those misses queue behind the flushes, and because the key/value actor performs its
as-of-timestamp history read synchronously, one device-bound miss stalls every key on that actor. In
the 45-minute `bank` soaks this took leader reads from 1 ms to 40-50 ms with the CPU idle. Buffered
reads serve misses from the page cache instead; the page cache is charged to the container's memory
cgroup and shows in `docker stats`, but it is reclaimable and does not threaten an OOM kill. Set it
`true` to restore the block-cache-only footprint.

### Raft WAL shard column families

Eight `kahuna.wal_shard_*` keys size the RocksDB column families that hold the Raft log. **CamusDB sets
none of them.** Each key is unset by default, and unset leaves Kommander's own default for that field in
force, field for field — these are overrides of a default that is tuned and measured in Kommander, not a
CamusDB posture. They take effect only when `kahuna.wal_storage` is `rocksdb`.

A Raft log row dies when a compaction pass covers it with a range tombstone. A row that meets its
tombstone inside one flush unit is dropped at flush and never reaches L0; a row flushed earlier has to be
rewritten down the levels before it can die, and that rewrite is the write amplification these knobs cut.

| Key | Kommander default | What it does |
|-----|-------------------|--------------|
| `wal_shard_write_buffer_size_mb` | 64 | Size of one shard memtable. Widens the flush unit — and the restart replay unit. |
| `wal_shard_min_write_buffer_number_to_merge` | 2 | Immutable memtables merged into one flush. The cheaper way to widen the flush unit. |
| `wal_shard_max_write_buffer_number` | 4 | Memtables per shard, mutable plus immutable. Must exceed the merge count. |
| `wal_shard_level0_file_num_compaction_trigger` | 8 | L0 files that trigger compaction into the base level. |
| `wal_shard_level0_slowdown_writes_trigger` | 28 | L0 files at which writers are slowed. |
| `wal_shard_level0_stop_writes_trigger` | 44 | L0 files at which writers are stopped. |
| `wal_shard_max_bytes_for_level_base_mb` | RocksDB's 256 | Base-level size. Sized above the retained log, the live log stays in the base level and one compaction annihilates a tombstone's rows. |
| `wal_shard_universal_compaction` | `false` (leveled) | Universal compaction instead of leveled: few large sorted runs, no L0-to-Lmax cascade. |

Three cautions apply to the whole group.

- **Memory.** The worst case per actively-written shard is `wal_shard_write_buffer_size_mb` ×
  `wal_shard_max_write_buffer_number` — 256 MB at the defaults. That competes with
  `kahuna.rocksdb_shared_memory_budget_mb` on a small node. Under the shared WriteBufferManager an
  over-budget node flushes early, which quietly returns the flush unit to its old size rather than
  growing memory, so raising the memtable knobs without raising the budget can buy nothing.
- **Restart.** A wider flush unit is a wider WAL-replay unit. A restart re-reads more before the node
  reports ready; measure that, not only the bytes written.
- **Universal makes the level size inert.** Setting `wal_shard_universal_compaction: true` together with
  `wal_shard_max_bytes_for_level_base_mb` is accepted and is not an error, but RocksDB does not consult
  level sizing under universal compaction, so the second key has no effect.

Validation rejects a size outside 1..65536 MiB, a count outside 1..64, a
`wal_shard_max_write_buffer_number` that does not exceed `wal_shard_min_write_buffer_number_to_merge`, and
L0 triggers that are not strictly increasing. The last two are checked against the **effective** values,
substituting Kommander's defaults for the keys left unset, so a one-sided override cannot form an invalid
combination silently.

## Validation errors

| Condition | Error |
|-----------|-------|
| Unknown `mode` | `InvalidConfig` |
| Unknown `memory_profile` (not `prod`/`dev`) | `InvalidConfig` |
| Port outside 1..65535 | `InvalidConfig` |
| `http_peers` count ≠ `peers` count | `InvalidConfig` |
| Invalid `default_isolation_level` | `InvalidConfig` |
| `range_lock_heartbeat_interval_ms` ≥ `range_lock_expires_ms` (when expiry > 0) | `InvalidConfig` |
| `spill_threshold_rows` ≤ 0 | `InvalidConfig` |
| `spill_merge_fan_in` ≤ 0 | `InvalidConfig` |
| Unknown `kahuna` key | `InvalidConfig` |
| Unknown `kahuna.storage` / `kahuna.wal_storage` | `InvalidConfig` |
| `kahuna.start_election_timeout_ms` ≥ `kahuna.end_election_timeout_ms` | `InvalidConfig` |
| `kahuna.staged_base_fence_retention_ms` ≤ 0 | `InvalidConfig` |
| `kahuna.persistent_revision_retention_count` < 0, or `..._age_seconds` < 0 | `InvalidConfig` |
| `kahuna.persistent_revision_retention_age_seconds` in (0, effective `kahuna.pitr_window_seconds`) | `InvalidConfig` |
| effective `kahuna.recent_heartbeat_ms` ≥ effective `kahuna.heartbeat_interval_ms` (the window is a quarter of the cadence while unset) | `InvalidConfig` |
| `kahuna.wal_shard_write_buffer_size_mb` or `kahuna.wal_shard_max_bytes_for_level_base_mb` outside 1..65536 | `InvalidConfig` |
| any `kahuna.wal_shard_*` count outside 1..64 | `InvalidConfig` |
| effective `kahuna.wal_shard_max_write_buffer_number` ≤ effective `kahuna.wal_shard_min_write_buffer_number_to_merge` | `InvalidConfig` |
| effective `kahuna.wal_shard_level0_*` triggers not strictly increasing (compaction < slowdown < stop) | `InvalidConfig` |

See `CamusDB/Config/config.yml` for inline documentation of every field.
