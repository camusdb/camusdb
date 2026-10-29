# Configuration

CamusDB reads one YAML configuration file at startup and merges CLI flags and environment variables
into a single resolved configuration object. It uses the first file it finds:

1. `--config <path>`
2. the `CAMUS_CONFIG_PATH` environment variable
3. `./camusdb.yml` or `./Config/config.yml` in the working directory
4. `~/.camusdb/config.yml` (`%APPDATA%\camusdb\config.yml` on Windows; `$CAMUS_HOME/config.yml` when
   `CAMUS_HOME` is set)
5. built-in defaults, when no file exists

A path named by 1 or 2 must exist; startup fails rather than falling through to another file.
`camusdb init` writes a short starter file to location 4 and creates the default data directory.
`CamusDB/Config/config.yml` in the repository is the annotated reference for every key; it is tracked
in git, so do not keep a node's settings there.

To see what a running node actually resolved — including which layer supplied each value — run
[`SHOW VARIABLES`](show-variables.md) against it rather than reconstructing the merge by hand.

## Precedence

Highest wins:

1. **CLI flags** — only flags you explicitly pass override YAML (nullable options; no sentinel defaults).
2. **Environment variables** — `CAMUS_KEY_RANGE_SHARDING` overrides `key_range_sharding`
   (what that setting does: [key-range-sharding.md](key-range-sharding.md)). Authentication is
   switched on with `CAMUSDB_AUTH_ENABLED`, and its secrets are read only from the environment.
3. **`config.yml`**
4. **Built-in defaults** in `ConfigDefinition` / `CamusDBOptions`.

Example: YAML `mode: cluster` with `--mode standalone` starts in standalone mode. YAML
`mode: cluster` with no `--mode` flag stays in cluster mode.

## CLI ↔ YAML mapping

| YAML field | CLI flag | Default |
|------------|----------|---------|
| `data_dir` | `--data-dir` | `~/.camusdb/data` (`$CAMUS_HOME/data` when set) |
| `mode` | `--mode` | `standalone` |
| `memory_profile` | `--memory-profile` | `prod` |
| `node_name` | `--raft-nodename` | `""` (cluster: machine name) |
| `raft_node_id` | `--raft-nodeid` | `1` |
| `raft_host` | `--raft-host` | `localhost` |
| `raft_port` | `--raft-port` | `7070` |
| `initial_partitions` | `--initial-cluster-partitions` | `1` standalone, `3` cluster |
| `peers` | `--initial-cluster` | `[]` (the Raft addresses of the other nodes; never this node's own `raft_host:raft_port`) |
| `http_peers` | `--http-peers` | `[]` |
| `extension_assemblies` | `--extension-assemblies` | `[]` (absolute paths of SAFE C# extensions; see [csharp-extensions.md](csharp-extensions.md)) |
| `unsafe_extension_assemblies` | `--unsafe-extension-assemblies` | `[]` (absolute paths of extensions loaded without the SAFE check) |
| `join_existing` | `--join-existing` | `false` (`true` joins a running cluster, with `peers` as the seed list; see [growing-a-one-node-cluster.md](growing-a-one-node-cluster.md)) |
| `pitr_bootstrap_from` | `--pitr-bootstrap-from` | unset (see [backups-and-point-in-time-recovery.md](backups-and-point-in-time-recovery.md)) |
| `pitr_bootstrap_target_time_ms` | `--pitr-bootstrap-target-time-ms` | `0` (the latest point the chain covers) |
| `schema_ack_wait_timeout_ms` | `--schema-ack-wait-timeout-ms` | `30000` |
| `schema_ack_live_node_lease_ms` | `--schema-ack-live-node-lease-ms` | `30000` |
| `unhosted_control_log_probe_interval_ms` | — | `250` |
| `http_port` | `--http-port` | `5095` |
| `https_port` | `--https-port` | `7141` |
| `https_certificate` | `--https-certificate` | `""` |
| `http3_enabled` | — | `false` (`true` also serves HTTP/3 over QUIC on `https_port`; see below) |
| `raft_certificate` | `--raft-certificate` | `""` |
| `raft_mutual_tls_enabled` | — | `false` (authenticates peers by client certificate on the Raft port; see [Node-to-node authentication](#node-to-node-authentication)) |
| `raft_peer_ca` | — | `""` (PEM CA file that peer certificates must chain to) |
| `grpc_enabled` | — | `true` |
| `grpc_port` | `--grpc-port` | `5096` |
| `grpc_certificate` | — | `""` (falls back to `raft_certificate`) |
| `grpc_tls_enabled` | — | `true` (`false` serves the client gRPC port as plaintext h2c even when a certificate is configured; the Raft port is unaffected) |
| `require_tls_when_auth_enabled` | `--require-tls-when-auth-enabled` | `true` |
| `default_isolation_level` | — | `serializable` |
| `default_transaction_locking` | — | `pessimistic` |
| `default_transaction_priority` | — | `normal` |
| `transaction_admission_wait_ms` | — | `0` (node default) |
| `range_lock_expires_ms` | — | `150000` |
| `max_serializable_transaction_lifetime_ms` | — | `3600000` |
| `lock_escalation_threshold` | — | `50` |
| `lock_wait_deadline_ms` | — | `500` |
| `replication_wait_deadline_ms` | — | `5000`; cap on a lock or write that waits on a value still replicating or a committed predecessor still settling (Kahuna's `WaitingForReplication`), kept apart from the lock-conflict cap so settlement lag on a loaded node does not fail a statement that conflicts with nothing |
| `key_range_sharding` | — (`CAMUS_KEY_RANGE_SHARDING` env) | `false`; fixed per cluster when the cluster is created. Unset, a node follows the cluster's recorded mode; set to the other mode, the node refuses to start (see [key-range sharding](key-range-sharding.md#the-mode-is-fixed-when-the-cluster-is-created)) |
| `distributed_query_execution` | — | `false`; fragments eligible key-range scans across cluster nodes (see [query planning](query-planner.md#part-iv--distributed-ready-plan-properties)) |
| `broadcast_join_max_build_rows` | — | `10000`; maximum build-side rows for a remote inner-join probe; `0` disables broadcast probes |
| `stats_flush_interval_ms` | — | `5000` |
| `sql_parser_cache_ttl_seconds` | — | `300` |
| `sql_parser_cache_max_entries` | — | `2048` |
| `sql_parser_cache_sweep_seconds` | — | `60` |
| `sql_parser_cache_max_bytes` | — | `67108864` |
| `spill_enabled` | — | `false` |
| `spill_threshold_rows` | — | `500000` |
| `spill_threshold_bytes` | — | `268435456` (256 MiB; `<= 0` = no byte cap) |
| `spill_merge_fan_in` | — | `16` |
| `spill_max_total_bytes` | — | `8589934592` (8 GiB; `<= 0` = no limit) |
| `point_read_row_id_cache_enabled` | — | `true` (a primary-key point lookup remembers its row id and skips the index read next time; root databases only. A row id is never reused and a primary key never changes, so a remembered mapping can only point at a deleted row, which the row read detects before falling back to the index. Set `false` to turn it off) |
| `point_read_row_id_cache_max_entries` | — | `1000000` |
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
| `statement_stats_enabled` | — | `false` (per-shape statement totals for `SHOW STATEMENT STATS`; see [statement-stats.md](statement-stats.md)) |
| `statement_stats_max_shapes` | — | `2000` (restart to change) |
| `index_usage_enabled` | — | `true` (per-index read and write counts for `SHOW INDEX USAGE`; see [index-usage.md](index-usage.md)) |
| `query_activity_enabled` | — | `true` |
| `query_activity_max_sql_length` | — | `1024` |
| `cluster_activity_peer_timeout_ms` | — | `2000` |
| `kahuna.*` | — | mode-specific baseline |

The table covers every key with a CLI flag plus a selection of YAML-only keys. `config.yml` documents
every accepted key with its default, and `SHOW VARIABLES` reports the resolved value of each.

Parser-cache, lock/isolation, spill, and query-result-cache knobs are YAML-only (operational tuning,
not per-node startup flags). The result cache is **on by default** (opt-in per query via a
`{cache=…}` hint); set `query_result_cache_enabled: false` to turn it off entirely. See
[query-result-cache.md](query-result-cache.md) for what each knob does and operator guidance.

The slow-query-log knobs are YAML-only too. The log is **off by default**; see
[slow-query-log.md](slow-query-log.md) for sizing guidance and for which of the four take effect
without a restart.

The query-activity knobs are YAML-only as well, and all three take effect without a restart; see
[query-activity.md](query-activity.md).

### HTTP/3 on the HTTPS API port

`http3_enabled: true` makes the HTTPS API port serve HTTP/3 over QUIC, next to HTTP/1.1 and HTTP/2.
It is off by default and takes effect at restart.

```yaml
https_port: 7141
https_certificate: /etc/camusdb/api.pfx
http3_enabled: true
```

- HTTP/3 needs TLS, so it applies to the HTTPS port only. Without `https_certificate` there is no
  HTTPS port and the setting has no effect. The plain HTTP port, the Raft port and the gRPC port
  do not change.
- QUIC runs over UDP. Open `https_port` for UDP as well as TCP, including any Docker port mapping
  (`-p 7141:7141/tcp -p 7141:7141/udp`) and firewall rule.
- The host needs the platform QUIC library: `libmsquic` on Linux and macOS, or Windows 11 /
  Windows Server 2022 and later. Without it the port keeps serving HTTP/1.1 and HTTP/2 only.
  On macOS, .NET does not search the Homebrew library folder: start the node with
  `DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib`. macOS removes `DYLD_*` variables for system
  binaries such as `nohup`, so set the variable in the environment of the node process itself.
- A client that connects over TCP gets an `Alt-Svc` header on each response. Browsers and other
  clients that support it then move to HTTP/3 on the same port.

When the setting is on but HTTP/3 cannot start (no certificate, or no QUIC library), the node still
starts and logs a warning that names the cause. `SHOW CONNECTIONS` lists HTTP/3 connections with the
protocol `HTTP/3`.

### Node-to-node authentication

`raft_mutual_tls_enabled: true` makes each node prove its identity to its peers with a client
certificate (mutual TLS). It is off by default and takes effect at restart.

```yaml
mode: cluster
raft_certificate: /etc/camusdb/tls/node.pfx
raft_mutual_tls_enabled: true
raft_peer_ca: /etc/camusdb/tls/ca.crt
```

- The Raft port requires a client certificate. It accepts one only when the certificate chains to a
  CA in `raft_peer_ca`, is inside its validity window, and allows client authentication. The system
  trust store is not used, and revocation is not checked.
- Each node presents `raft_certificate` as its client certificate. That certificate must allow both
  server and client authentication (extended key usages `serverAuth` and `clientAuth`, or no
  extended key usage at all). The PKCS#12 file has no password.
- Use a CA that issues certificates for this cluster's nodes only. Every certificate it signed is
  accepted as a peer.
- The Kommander and Kahuna gRPC services (`Rafter`, `KeyValuer`, `Locker`, `Sequencer`, `Cluster`
  and `Backups`) then answer only a verified peer on the Raft port. This holds whether user
  authentication is on or off. A user token never opens them.
- A new certificate takes a restart: the node loads its client certificate once.

**It is required for a multi-node cluster with authentication on.** The Raft and Kahuna services
carry no user token, so with authentication on a verified peer certificate is the only credential
they can show. A node with `CAMUSDB_AUTH_ENABLED=true`, at least one entry in `peers`, and this
setting off refuses to start. A single-node cluster is not affected.

## Kahuna engine section

The nested `kahuna:` map is an allow-listed passthrough to `EmbeddedKahunaOptions`, used for the one
embedded Kahuna node a process runs, in both standalone and cluster mode (`Program.cs`).
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

`compact_every_operations` counts persisted WAL **batches**, not log entries: under load a batch carries
on the order of a hundred entries. With a RocksDB Raft log, the log is reclaimed by dropping whole SST
files below a persisted compaction floor, and every pass advances that floor to the true floor. The
cadence therefore decides only how often a pass runs, not how much it may reclaim.

Storage backends: `memory`, `sqlite`, `rocksdb`, `solarium`.

### Solarium storage engine

`solarium` selects [Solarium](https://solariumdb.github.io/docs/integrations/kahuna/), a managed
key-value store with the RocksDB API, for the key-value store (`storage`), the Raft WAL
(`wal_storage`), or both. A node on Solarium needs no native RocksDB library. CamusDB ships the
`SolariumDb.Kahuna` package and registers its two engines with the embedded node, so no extension
assembly is necessary.

```yaml
kahuna:
  storage: solarium
  wal_storage: solarium
  solarium_environment: default         # default | io_uring (Linux only)
  storage_compaction_rate_mbps: 50      # optional pacing, see below
```

Rules:

- Solarium does not read RocksDB files, and RocksDB does not read Solarium files. A change of engine
  on a node with data needs a logical dump and reimport, see
  [logical-dump-and-reimport.md](logical-dump-and-reimport.md). Every node of a cluster must use the
  same `storage` and `wal_storage`.
- The `rocksdb_*` tuning keys and the `wal_shard_*` keys do not apply to Solarium. The exceptions are
  `rocksdb_shared_memory_budget_mb` and `rocksdb_shared_memtable_budget_mb`. They size the one cache
  and the one memtable budget that a Solarium store and a Solarium WAL share. Solarium shares them
  whenever a size is above 0, and `rocksdb_shared_memory` has no effect on it.
- **Solarium's cache and memtables are managed objects.** RocksDB holds its block cache and memtables
  in native memory, outside the .NET heap; Solarium holds them inside it. On a Solarium node the two
  budgets therefore count against the GC heap limit (`DOTNET_GCHeapHardLimit`,
  `DOTNET_GCHeapHardLimitPercent`, or the runtime's 75% default inside a memory-limited container),
  beside the actor caches and the application heap, and a heap limit chosen for RocksDB is too small.
  When the two keys are unset, the [memory-proportional sizing](#memory-proportional-cache-defaults)
  derives them from the managed heap budget instead of the machine memory, so they fit. When you set
  them explicitly, size the heap limit to hold them plus the working heap. Measured on a 4,096 MiB
  container with explicit 768 / 256 MiB budgets under a 2-hour fault soak:

  | Heap limit | GC committed (p50 / max) | Outcome |
  |-----|-----|-----|
  | 60% (2,458 MiB) | 2,140 / 2,420 MiB | leader died of a managed `OutOfMemoryException` at 57 min |
  | 3,072 MiB (75%) | 2,470 / 3,000 MiB | passed; the RocksDB control at 60% ran 1,550 / 2,360 MiB |

  The node logs the budgets it runs with once at startup (`Solarium shared memory: ...`, see the
  sizing section), and `gc_memory_limit_bytes` in the [node stats](node-stats.md) reports the limit
  in force. When the shared cache budget plus the actor caches take more than a third of the heap
  budget, the node also logs a warning at startup, since the configuration that died above sat at
  37% and the one that passed at 30%:

  ```text
  Solarium charges its cache and memtables to the managed heap: 921 MiB of budgets (shared cache 768 MiB, actor caches 153 MiB) take 37% of the 2457 MiB heap budget, above the 33% guard. A node at 37% ran out of heap under load. Raise the heap limit (DOTNET_GCHeapHardLimit / DOTNET_GCHeapHardLimitPercent) or lower kahuna.rocksdb_shared_memory_budget_mb and kahuna.max_bytes_per_actor.
  ```

  The computed defaults stay near 16% of the heap budget and never reach the guard; only explicit
  budgets do. The guard is not a sizing rule: the application heap under load needed about 2 GiB on
  that node whatever the budgets were.
- `solarium_environment` selects the file I/O of both layers. `io_uring` needs Linux with io_uring,
  and the node does not start where the kernel cannot create a ring. Solarium ignores the key unless
  one layer is `solarium`.
- A backup bootstrap (`pitr_bootstrap_from`) still needs RocksDB for the store and the WAL, see
  [backups-and-point-in-time-recovery.md](backups-and-point-in-time-recovery.md).

Two keys pace the flush and compaction writes of the key-value store. Both engines read them:

| Key | Effect |
|-----|--------|
| `storage_compaction_rate_mbps` | Rate limit in MB/s (10^6 bytes) for flush and compaction writes. 0 or unset = no limit. |
| `storage_max_background_compactions` | The number of compactions that run at once. 0 or unset = the engine default. |

Without either key, Solarium runs one compaction at a time and paces nothing. RocksDB runs
max(2, CPUs / 2). With a rate, Solarium also runs max(2, CPUs / 2), unless
`storage_max_background_compactions` sets the number. Several unpaced compactions take the disk from
the synced writes of a large store. Choose a rate of about a third to half of what the disk commits in
synced writes. A rate that is too low lets level 0 grow until writes slow down.

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

### Materialize on resolve

`kahuna.durable_materialize_on_resolve` (default **on** in CamusDB; Kahuna's own default is off) sets the record shape of
a committed durable transaction: the commit's settle installs each committed value from the prepared
intent on every replica, so Kahuna writes no materialization record per key. On the anchor partition a
two-row transaction then costs about four Raft entries instead of six (the one-phase bundle's three plus
one settle) and about two scheduler submissions instead of four; followers apply fewer entries and a
device writes fewer bytes per commit. CamusDB turns it on in both baselines: the shape passed two
consecutive clean two-hour fault soaks on two released Kahuna versions (conservation exact, no divergence,
every fault recovered), and it is a per-group property, so the baseline decides it for every node. An
explicit value, including `false`, always wins.

Two operating rules, both enforced by the record shape rather than by CamusDB:

- **Every node first, then the flag.** Every node must run a Kahuna build that installs on a
  materializing settle (Kahuna.Core 1.10.1 or later) before any node has this on. An older node resolves
  the intent without installing the value and loses that write locally. Turning it off is safe at any
  time, so a rolling upgrade from an older build pins `durable_materialize_on_resolve: false` on every
  node until the last one is upgraded, then removes the key.
- **Same value on every node of the group**, for the same reason as the one-phase gate above.

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

`memory_profile` (`--memory-profile`) selects *how* the four cache-sizing knobs below are defaulted,
and under `dev` it also selects workstation GC (see below). It changes nothing else — not worker
counts, not durability, not any behavior an application can observe other than how often a read is
served from cache instead of disk.

| Profile | Block cache | Memtable sub-budget | Actor caches | Total |
|---------|-------------|---------------------|--------------|-------|
| `prod` (default) | 10% of machine memory, ≤ 2 GiB | ¼ of it, raised to the Raft-log flush unit (≤ half the cache), ≤ 1 GiB — see below | 6.25% of the managed heap budget, ≥ 64 MiB | ~16% of memory (~1.5 GiB on an 8 GiB box) |
| `dev` | 64 MiB | 16 MiB | 32 MiB | ~96 MiB, on any machine |

`dev` is for a node sharing a developer machine with the application being built against it: the
budgets are fixed, so the same node is the same size on a 64 GiB workstation and in a 2 GiB
container. The cost is throughput once the working set outgrows the cache — the TPC-C run that
motivated proportional sizing was ~5x slower against a 320 MiB block cache — so it is not a server
setting. Note that the caches are ceilings filled lazily: a burst of large writes can still push
process RSS well above the cache total, because transient managed allocations and the .NET GC
dominate the peak. Server GC (the build default, one heap per core) also keeps the space a burst grew
after the burst ends: an idle dev node on a 10-core machine was measured at 2.1 GB committed for a
~160 MB live heap.

So `dev` also runs the node under workstation GC. The GC mode is fixed by the runtime before any
configuration is read, so the server starts itself again as a child process with `DOTNET_gcServer=0`
and the original arguments, and the first process only waits for it and passes its exit code
through. SIGINT, SIGTERM and SIGQUIT sent to the first process are forwarded to the child, so
`docker stop`, systemd and `kill` still shut the node down gracefully; a SIGKILL cannot be forwarded
and leaves the child running. Setting `DOTNET_gcServer` (either value) yourself turns the relaunch
off and keeps the GC mode you chose. The startup banner states which GC the node runs under.

An explicit `kahuna.*` budget always beats the profile, so `dev` plus one raised budget is a valid
combination rather than a conflict.

### Memory-proportional cache defaults

Under `memory_profile: prod`, most unset keys keep Kahuna's own default, but the four cache-sizing
knobs are an exception: when left unset they are computed at startup from the node's memory rather
than from a fixed constant. A fixed 320 MB block cache was measured forcing a 1.2 GB TPC-C working set
through disk reads on nearly every statement; sizing it to the machine took the same workload from
24.5 to 119.6 tx/s at 8 clients.

Two different memory sizes feed the computation, one for each kind of cache:

- **Machine memory** sizes the RocksDB pair. It is the container's cgroup memory limit when one is
  set, else the machine's physical RAM. RocksDB's block cache and memtables are native allocations,
  so they are never sized from the .NET heap limit.
- **Managed heap budget** sizes the actor caches, which live in the managed heap. It is what the GC
  reports: the heap hard limit when one is in force (`DOTNET_GCHeapHardLimit`,
  `DOTNET_GCHeapHardLimitPercent`, or the runtime's own 75% default inside a memory-limited
  container), else the machine memory. On a node where `storage` or `wal_storage` is `solarium` it
  also sizes the shared cache and memtable budget, because
  [Solarium keeps them in the managed heap](#solarium-storage-engine).

With no heap limit the two sizes are the same. With a heap limit they differ, and the difference
matters: the RocksDB budgets sit **outside** the heap limit and count against the container beside it,
while Solarium's sit **inside** it. On a 4,096 MiB container with `DOTNET_GCHeapHardLimitPercent=3C`
(60%, the value is hexadecimal), the heap budget is 2,457 MiB; RocksDB is sized from 4,096 MiB
(409 MiB cache, 192 MiB memtables) and Solarium from 2,457 MiB (245 MiB cache, 122 MiB memtables).
Leave room for both when you pick the heap percentage.

| Key | Computed when unset | Clamp |
|-----|---------------------|-------|
| `rocksdb_shared_memory_budget_mb` | 10% of machine memory (RocksDB), or of the managed heap budget (Solarium) | 64 MiB – 2 GiB (320 MiB floor only when 10% reaches it) |
| `rocksdb_shared_memtable_budget_mb` | a quarter of the block cache, raised to the Raft-log flush unit (never past half the cache) | 16 MiB – 1 GiB (128 MiB floor only when a quarter reaches it) |
| `max_bytes_per_actor` | 6.25% of the managed heap budget (≥ 64 MiB for the layer) ÷ the key/value actor count | 1 MiB – 2 GiB per actor (8 MiB floor only when the share reaches it) |
| `max_entries_per_actor` | `max_bytes_per_actor` ÷ ~512 B | 2k – 4M (10k floor only when the share reaches it) |

The floors in parentheses yield on a small node: below them the percentage governs, down to the lower
bound of the clamp. A fixed floor on a small container used to claim several times its share of memory.

**The memtable flush-unit floor.** When the KV store and the Raft WAL share one
WriteBufferManager (`rocksdb_shared_memory` on, `storage` and `wal_storage` both `rocksdb`), the Raft
log flushes at its flush unit plus headroom: `wal_shard_write_buffer_size_mb` ×
(`wal_shard_min_write_buffer_number_to_merge` + 1), which is 192 MiB at Kommander's defaults. Kommander
checks the same figure when it opens the WAL. Below it, RocksDB flushes the Raft log whenever the
shared budget fills, not at its own 128 MiB unit, and it logs this warning:

```text
the shared memtable budget (61 MB) is below the shard flush unit plus headroom (192 MB)
```

In a 10-minute write probe at a 61 MiB budget, all 349 Raft-log flushes were forced by the budget, at
about 6 MiB each. CamusDB therefore raises the computed memtable budget to that unit, taking at most
**half** of the block-cache budget (the memtables are charged inside that budget and reads need the
rest): a 4,096 MiB node gets a 409 MiB cache and a 192 MiB memtable budget. Below roughly 3.75 GiB of
machine memory the unit does not fit in half the cache, the budget stops at that half, and the warning
above is expected; a node that wants the unit covered there sets `rocksdb_shared_memtable_budget_mb`
explicitly. An explicit `rocksdb_shared_memtable_budget_mb` always wins, and the `dev` profile never
applies the floor.

The floor needs Kommander 1.6.6 or later and is the `RaftWalFlushUnitFloorEnabled` constant in
`EmbeddedKahunaOptionsBuilder`. It shipped off on Kommander 1.6.5: an unstarved budget let the
Raft-log store retain every write-ahead `.log` file for the life of the process, because a rarely
written column family (the meta partition's shard) never flushed without budget starvation and
nothing else released the files (443 MiB to 2,709 MiB in ten minutes per node, against ~230 MB of
live tables), and a follower restart took 4 s longer. Kommander 1.6.6 bounds the write-ahead files.

At startup the node logs the values it uses, once, before RocksDB opens:

```text
RocksDB shared memory: block cache 409 MiB, memtable budget 192 MiB; Raft-log flush unit plus headroom 192 MiB (covered). Native budgets sit outside the managed heap: machine memory 4096 MiB (cgroup), managed heap budget 2457 MiB.
```

A Solarium node logs the same figures with the budgets' place named, since there they are the
managed heap's:

```text
Solarium shared memory: cache 245 MiB, memtable budget 122 MiB; Raft-log flush unit plus headroom 256 MiB (NOT covered: Raft-log flushes will be budget-forced). Managed budgets sit inside the heap limit: managed heap budget 2457 MiB, machine memory 4096 MiB (cgroup).
```

The machine-memory source is `cgroup`, `meminfo` (Linux RAM), `sysctl` (macOS RAM), or `gc` (no
readable machine size, so the GC figure is used).

That is roughly 16% of memory across both cache layers, and never more than 4 GiB in total however
large the machine is. The fractions and the ceilings are deliberately modest: an unconfigured node
is far more often a developer workstation or a CI container sharing the box with a compiler and an
IDE than a dedicated database server. An explicit value always wins over the computed one. On an
8 GiB, 8-core machine with no heap limit and none of them set: 819 MiB block cache, 204 MiB memtable
sub-budget (above the 192 MiB flush unit without any floor), and 512 MiB of actor caches split across
32 actors (16 MiB each) — about 1.5 GiB.

A dedicated server should raise all four explicitly; the sizing above is a floor to build from, not
a recommendation for a machine whose only job is CamusDB.

Note that the 6.25% share and its 64 MiB floor bound the actor-cache layer *as a whole*, and are
then divided by the key/value actor count (`kahuna.key_value_workers`, or max(32, 4 × CPU cores)
when unset); only the 8 MiB per-actor minimum is per actor. Adding cores
therefore splits the same budget more ways rather than growing it — a machine with many cores
relative to its RAM does not end up with a multiple of the intended share.

The RocksDB pair is shared: `rocksdb_shared_memory` (default on, and a no-op unless `storage` and
`wal_storage` are both `rocksdb`) makes one block cache and one write-buffer manager serve both the
KV store and the Raft WAL. The memtable sub-budget is charged **inside** the total block-cache
budget, not added to it, and must be ≤ it. That comparison is made against the *effective*
post-merge pair, so overriding only one of the two can produce an inconsistent pair — a 100 MiB
total against an explicit 512 MiB memtable — and fails startup with `InvalidConfig`. The computed
memtable default never exceeds the total, with or without the flush-unit floor. Set both together.

A Solarium WAL (`wal_storage: solarium`) always charges its memtables to the shared memtable budget.
The memory-proportional sizing therefore raises an unset budget to 256 MiB, the Solarium WAL's
128 MiB flush unit plus one memtable of headroom, but never past half the cache. Below that size, the
Solarium WAL writes a warning at open and flushes the Raft log in small pieces. Because the Solarium
budgets are sized from the managed heap budget, the 256 MiB fits in half the cache only from a heap
budget of 5 GiB (a container of about 6.7 GiB at the runtime's 75% default); a smaller Solarium node
that wants the unit covered sets both budgets explicitly and a heap limit that holds them. Likewise `max_bytes_per_actor` is **per actor**: multiply by the actor count (`key_value_workers`,
default max(32, 4 × CPU cores)) to get the total.

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

`kahuna.key_value_write_post_completion_hold_ms` (default **1** in CamusDB; Kahuna's own default is 0)
is the knob that does add density at full occupancy: after a batch completes the aggregator holds this
long before dispatching the next, so more items accumulate per batch; the cost is that much added write
latency. The aggregator keeps one Raft round in flight per partition, so at full load a round carries
only what arrived during the previous round. The timer that ends the hold fires about 1 ms late (0.95 to
1.1 ms on the aggregator's cycle stamps), so the effective hold is the configured value plus ~1 ms. The
default was 2 ms while the round was ~2.4 ms: it roughly doubled the items per round (110 vs 51 at 128
workers on the bank shape) for +6% throughput and a *lower* write p50. With the round at ~1.6 ms, a
one-variable sweep on the same shape measured 2 / 1 / 0 ms at 9,193 / 9,421 / 8,755 ops/s: 1 ms keeps
~128 items per batch and takes 0.5 ms off the commit; 0 ms makes the commit fastest but runs 2.8× the
Raft rounds, which slows point reads (1.97 → 2.40 ms) and updates on the shared partition executor, so
the closed loop loses. An idle aggregator still dispatches on arrival, so low-load latency is unchanged.
Set `0` to restore Kahuna's dispatch-at-once behaviour; an explicit value always wins over the shipped
default.

`kahuna.key_value_write_precise_wake` (default **false**, Kahuna 1.11.6) fires the aggregator's hold,
linger and queue-age wakes from one dedicated timer thread per node instead of the timer queue. The
~1 ms lateness described above is the timer queue's granularity plus a thread-pool hop; the dedicated
thread sleeps whole milliseconds in a kernel wait and spins only the sub-millisecond remainder, so the
hold ends within 0.1-0.25 ms of its deadline at no extra CPU per transaction.
`kahuna.key_value_write_fill_target_trigger` (default **false**, Kahuna 1.11.6) adds fill-or-time
dispatch on top of the hold: a held or lingering partition buffer is dispatched as soon as it reaches
the learned fill target, the upper median of its last 16 timer-dispatched batch sizes, instead of
waiting out the hold. Every eighth fill-triggered dispatch waits for the timer as a probe, so the target
keeps following the load; the learned target is exported as the `kahuna.kv.write.fill_target` histogram.
Both are off until a soak shows the gain for your shape.

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
  growing memory, so raising the memtable knobs without raising the budget can buy nothing. The
  computed memtable budget follows these two keys through the flush-unit floor above, but only up to
  half the block cache; past that, raise `kahuna.rocksdb_shared_memtable_budget_mb` alongside them.
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
| A `peers` entry equal to this node's `raft_host:raft_port` (checked after command-line flags apply) | `InvalidConfig` |
| Invalid `default_isolation_level` | `InvalidConfig` |
| `range_lock_expires_ms` > 0 and < 2 × effective `kahuna.collection_interval_ms` (60 s by default) | `InvalidConfig` |
| `kahuna.max_transaction_timeout_ms` < `max_serializable_transaction_lifetime_ms` | `InvalidConfig` |
| `kahuna.locks_workers` or `kahuna.key_value_workers` set to ≤ 0 | `InvalidConfig` |
| `spill_threshold_rows` ≤ 0 | `InvalidConfig` |
| `spill_merge_fan_in` ≤ 0 | `InvalidConfig` |
| `large_value_rewrite_batch_rows` ≤ 0, or above half of `max_mutations_per_transaction` (when the limit > 0) | `InvalidConfig` |
| Unknown `kahuna` key | `InvalidConfig` |
| Unknown `kahuna.storage` / `kahuna.wal_storage` | `InvalidConfig` |
| `kahuna.solarium_environment` other than `default` / `io_uring` | `InvalidConfig` |
| Negative `kahuna.storage_compaction_rate_mbps` / `kahuna.storage_max_background_compactions` | `InvalidConfig` |
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
