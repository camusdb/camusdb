# Configuration

CamusDB reads one YAML configuration file at startup and merges CLI flags and environment variables
into a single resolved configuration object. It uses the first file it finds, in the order below.

1. `--config <path>`
2. the `CAMUS_CONFIG_PATH` environment variable
3. `./camusdb.yml` or `./Config/config.yml` in the working directory
4. `~/.camusdb/config.yml` (`%APPDATA%\camusdb\config.yml` on Windows; `$CAMUS_HOME/config.yml` when
   `CAMUS_HOME` is set)
5. built-in defaults, when no file exists

A path named by 1 or 2 must exist. If it does not, startup fails and no other file is tried.
`camusdb init` writes a short starter file to location 4 and creates the default data directory.
`CamusDB/Config/config.yml` in the repository is the annotated reference for all keys. It is tracked
in git. This means that you should not keep a node's settings there.

To see what a running node resolved, including which layer supplied each value, run
[`SHOW VARIABLES`](show-variables.md) against it.

## Precedence

The highest layer in this list wins.

1. **CLI flags** — only flags you explicitly pass override YAML (nullable options; no sentinel defaults).
2. **Environment variables** — `CAMUS_KEY_RANGE_SHARDING` overrides `key_range_sharding`
   (what that setting does: [key-range-sharding.md](key-range-sharding.md)). Authentication is
   switched on with `CAMUSDB_AUTH_ENABLED`, and its secrets are read only from the environment.
3. **`config.yml`**
4. **Built-in defaults** in `ConfigDefinition` / `CamusDBOptions`.

Example: YAML `mode: cluster` with `--mode standalone` starts in standalone mode. If the
`--mode` flag is omitted, YAML `mode: cluster` starts in cluster mode.

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
| `peers` | `--initial-cluster` | `[]` (the Raft addresses of the other nodes; do not include this node's `raft_host:raft_port`) |
| `http_peers` | `--http-peers` | `[]` |
| `extension_assemblies` | `--extension-assemblies` | `[]` (absolute paths of SAFE C# extensions; see [csharp-extensions.md](csharp-extensions.md)) |
| `unsafe_extension_assemblies` | `--unsafe-extension-assemblies` | `[]` (absolute paths of extensions that are loaded with the SAFE check skipped) |
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
| `grpc_certificate` | — | `""` (`raft_certificate` is used when unset) |
| `grpc_tls_enabled` | — | `true` (`false` serves the client gRPC port as plaintext h2c even when a certificate is configured; the Raft port is unaffected) |
| `require_tls_when_auth_enabled` | `--require-tls-when-auth-enabled` | `true` |
| `default_isolation_level` | — | `serializable` |
| `default_transaction_locking` | — | `pessimistic` |
| `default_transaction_priority` | — | `normal` |
| `transaction_admission_wait_ms` | — | `0` (node default) |
| `range_lock_expires_ms` | — | `150000` |
| `max_serializable_transaction_lifetime_ms` | — | `3600000` |
| `transaction_idle_timeout_ms` | — | `300000`; how long an explicit transaction may wait for its client before the node rolls it back, counted from the end of its last statement. A transaction with a running statement is not idle, also when its client reads the rows slowly. `0` or less disables it. Runtime and cluster-wide (see [idle transactions](request-resource-limits.md#idle-transactions)) |
| `transaction_reaper_interval_ms` | — | `30000`; how often the node looks for idle transactions. Runtime, per node |
| `transactional_ddl_enabled` | — | `false`; makes schema DDL in an explicit transaction commit and roll back with it, with the PostgreSQL rule. Runtime and cluster-wide. See [schema statements inside an explicit transaction](transactions-locking-and-isolation.md#97-schema-statements-inside-an-explicit-transaction) |
| `transactional_ddl_resolve_after_ms` | — | `30000`; how long the schema changes of a committing transaction may stay pending on the schema leader before the resolver ends them from their decision key (after a crash of the committing node). Runtime, per node. `0` resolves at the next run |
| `lock_escalation_threshold` | — | `1000`; the number of shared point locks that one Serializable read-write transaction can take on one table (or one index). Past this number, it replaces them with one lock on the table (or index) |
| `lock_wait_deadline_ms` | — | `500` |
| `optimistic_intent_wait_ms` | — | `100`; how long a row read of an optimistic read-write transaction waits for a write that another transaction has staged on the row and not committed. The wait lets the read return the committed result. Otherwise it could return a value that a later `UPDATE` of the same transaction would conflict on. `0` turns the wait off (see [optimistic conflicts](transactions-locking-and-isolation.md)) |
| `serializable_optimistic_point_locks` | — | `false`; whether a Serializable read-write transaction with optimistic locking takes a shared lock on each single key it reads and upgrades it when it writes the key. With `false`, the default, it takes no lock for a read of one key. The read is proved at commit, and a concurrent writer is not blocked. If the key changed, the reader's commit returns an error. Such a transaction also reads from one point in time. A read of a row it has not read yet returns an error when a row it read before has changed. This means that its statements return only rows that were committed together. A write or a delete of such a transaction gives way at once to an older running transaction that read the row, which is the order the lock would give. Scans take range locks with either value. `true` restores the locks. A reader then blocks a writer of the row, and each commit of such a transaction takes the two-phase path |
| `transaction_read_reuse_enabled` | — | `true`; whether the write phase of an `UPDATE` or `DELETE` takes a row from the transaction's own earlier read by key and does not read it again. It applies to a Serializable read-write transaction with optimistic locking that proves its single-key reads at commit, and to a row the transaction read by key and has not written since. A row that another transaction changed after the read is then refused by the write or by the commit, where the second read would have refused it. `false` reads the row again (see [optimistic conflicts](transactions-locking-and-isolation.md)) |
| `replication_wait_deadline_ms` | — | `5000`; cap on a lock or write that waits for a value to replicate or for a committed predecessor to settle (Kahuna's `WaitingForReplication`). It is separate from the lock-conflict cap. This means that settlement lag on a loaded node does not fail a statement that has no conflict |
| `key_range_sharding` | — (`CAMUS_KEY_RANGE_SHARDING` env) | `false`; fixed per cluster when the cluster is created. Unset, a node follows the cluster's recorded mode. If it is set to the other mode, the node does not start (see [key-range sharding](key-range-sharding.md#the-mode-is-fixed-when-the-cluster-is-created)) |
| `distributed_query_execution` | — | `false`; fragments eligible key-range scans across cluster nodes (see [query planning](query-planner.md#part-iv--distributed-ready-plan-properties)) |
| `broadcast_join_max_build_rows` | — | `10000`; maximum build-side rows for a remote inner-join probe; `0` disables broadcast probes |
| `stats_flush_interval_ms` | — | `5000` |
| `sql_parser_cache_ttl_seconds` | — | `300` |
| `sql_parser_cache_max_entries` | — | `2048` |
| `sql_parser_cache_sweep_seconds` | — | `60` |
| `sql_parser_cache_max_bytes` | — | `67108864` |
| `spill_enabled` | — | `true` |
| `spill_threshold_rows` | — | `500000` |
| `spill_threshold_bytes` | — | `268435456` (256 MiB; `<= 0` = no byte cap) |
| `spill_merge_fan_in` | — | `16` |
| `spill_max_total_bytes` | — | `8589934592` (8 GiB; `<= 0` = no limit) |
| `hash_join_max_build_rows` | — | `1000000`; with spill off, a hash-join build past this many rows is executed as a nested-loop join (see [spill-to-disk.md](spill-to-disk.md#configuration)) |
| `hash_join_max_build_bytes` | — | `1073741824` (1 GiB; `<= 0` = no byte limit); with spill off, a hash-join build past this many estimated bytes is executed as a nested-loop join. A nested-loop join caches its right side in memory only under this limit |
| `point_read_row_id_cache_enabled` | — | `true` (a primary-key point lookup remembers its row id and skips the index read next time; root databases only. A row id is never reused and a primary key never changes. This means that a remembered mapping can only point at a deleted row. The row read detects that case and then reads the index. An `UPDATE` or `DELETE` by primary key in an optimistic or Serializable transaction takes the remembered id too and skips its locate read; see [query-planner.md](query-planner.md). Set `false` to turn it off) |
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
| `cluster_activity_max_concurrent_fan_outs` | — | `8` |
| `max_databases` | — | `10000` (see [request-resource-limits.md](request-resource-limits.md)) |
| `max_statement_bytes` | — | `8388608` |
| `max_buffered_result_rows` | — | `1000000` |
| `max_buffered_result_bytes` | — | `268435456` |
| `max_recursive_cte_iterations` | — | `10000` (see [common-table-expressions.md](common-table-expressions.md#recursive-queries)) |
| `max_recursive_cte_rows` | — | `10000000` |
| `max_table_function_rows` | — | `5000000` (see [request-resource-limits.md](request-resource-limits.md#functions-in-from)) |
| `max_tables_per_statement` | — | `64` (see [request-resource-limits.md](request-resource-limits.md#tables-per-statement)) |
| `statement_timeout_ms` | — | `0`, no limit (see [request-resource-limits.md](request-resource-limits.md#statement-timeout)) |
| `max_open_explicit_transactions` | — | `10000` |
| `max_client_connections` | — | `10000` |
| `inline_functions_enabled` | — | `false` (accept `CREATE FUNCTION ... LANGUAGE csharp`; see [Inline functions](#inline-functions)) |
| `inline_function_call_timeout_ms` | — | `1000` |
| `inline_function_allocation_limit_bytes` | — | `16777216` |
| `inline_function_array_limit` | — | `1048576` |
| `inline_function_source_limit_bytes` | — | `65536` |
| `inline_function_compile_timeout_ms` | — | `10000` |
| `inline_function_compile_concurrency` | — | `2` |
| `inline_triggers_enabled` | — | `false` (accept `CREATE TRIGGER ... LANGUAGE csharp`; see [Inline functions](#inline-functions)) |
| `max_inline_functions_per_database` | — | `256` |
| `kahuna.*` | — | mode-specific baseline |

The table covers the keys that have a CLI flag, plus a selection of YAML-only keys. `config.yml`
documents all valid keys with their defaults, and `SHOW VARIABLES` shows the resolved value of each.

The knobs for the parser cache and the query result cache are YAML-only, and so are the lock,
isolation and spill knobs. They are operational tuning and do not have a CLI flag. The result cache is **on by default** (opt-in per query via a
`{cache=…}` hint); set `query_result_cache_enabled: false` to turn it off. See
[query-result-cache.md](query-result-cache.md) for what each knob does and operator guidance.

The slow-query-log knobs are YAML-only too. The log is **off by default**; see
[slow-query-log.md](slow-query-log.md) for sizing guidance and for which of the four take effect
while the node is running.

The query-activity knobs are YAML-only as well, and all four take effect while the node is running.
They also govern `SHOW TRANSACTIONS` and `KILL TRANSACTION`. For more information, see
[query-activity.md](query-activity.md).

### HTTP/3 on the HTTPS API port

`http3_enabled: true` makes the HTTPS API port serve HTTP/3 over QUIC, next to HTTP/1.1 and HTTP/2.
It is off by default and takes effect at restart.

```yaml
https_port: 7141
https_certificate: /etc/camusdb/api.pfx
http3_enabled: true
```

- HTTP/3 needs TLS. This means that it applies to the HTTPS port only. If `https_certificate` is not
  set, there is no HTTPS port, and the setting is ignored. The HTTP port, the Raft port and the gRPC
  port do not change.
- QUIC uses UDP. Open `https_port` for UDP as well as TCP, including any Docker port mapping
  (`-p 7141:7141/tcp -p 7141:7141/udp`) and firewall rule.
- The host needs the platform QUIC library: `libmsquic` on Linux and macOS, or Windows 11 /
  Windows Server 2022 and later. If the library is missing, the port serves HTTP/1.1 and HTTP/2 only.
  On macOS, .NET does not search the Homebrew library folder: start the node with
  `DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib`. macOS removes `DYLD_*` variables for system
  binaries such as `nohup`. Set the variable in the environment of the node process itself.
- A client that connects over TCP gets an `Alt-Svc` header on each response. Browsers and other
  clients that support it then move to HTTP/3 on the same port.

When the setting is on but HTTP/3 cannot start (no certificate, or no QUIC library), the node starts
anyway and logs a warning that names the cause. `SHOW CONNECTIONS` lists HTTP/3 connections with the
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

- The Raft port requires a client certificate. The certificate must chain to a CA in `raft_peer_ca`,
  be within its validity window, and allow client authentication. The system
  trust store is not used, and revocation is not checked.
- Each node presents `raft_certificate` as its client certificate. That certificate must allow both
  server and client authentication (extended key usages `serverAuth` and `clientAuth`, or no
  extended key usage at all). The PKCS#12 file must not be password-protected.
- Use a CA that issues certificates for this cluster's nodes only. Any certificate it signed is
  accepted as a peer.
- The Kommander and Kahuna gRPC services (`Rafter`, `KeyValuer`, `Locker`, `Sequencer`, `Cluster`
  and `Backups`) then answer only a verified peer on the Raft port. This is true whether user
  authentication is on or off. A user token never opens them.
- A new certificate takes a restart: the node loads its client certificate once.

**It is required for a multi-node cluster with authentication on.** The Raft and Kahuna services
do not carry a user token. With authentication on, a verified peer certificate is the only credential
they can show. A node does not start if `CAMUSDB_AUTH_ENABLED=true` is set, `peers` has at least one
entry, and this setting is off. A single-node cluster is not affected.

### Inline functions

An inline function is a SQL function whose body is C# source in a `CREATE FUNCTION` statement; the
server compiles it in memory. For the full description, see
[csharp-extensions.md](csharp-extensions.md#inline-functions). These settings are all restart-class: change the file on the node and restart it.

| Key | Default | Scope | Meaning |
| --- | --- | --- | --- |
| `inline_functions_enabled` | `false` | cluster | Accept `CREATE FUNCTION`. With `false` the statement returns `CADB0841`. Functions that exist continue to work. This means that a restart with the setting off does not break a view that calls one. |
| `inline_function_call_timeout_ms` | `1000` | cluster | The longest one call may run. `0` or less disables the limit. |
| `inline_function_allocation_limit_bytes` | `16777216` | cluster | The managed memory one call may allocate, counted from the start of the call. `0` or less disables the limit. |
| `inline_function_array_limit` | `1048576` | cluster | The largest array, in elements, a body may create with `new T[n]`. `0` or less disables the limit. |
| `inline_function_source_limit_bytes` | `65536` | cluster | The largest body, in UTF-8 bytes. Must be greater than 0. |
| `inline_function_compile_timeout_ms` | `10000` | node | The longest one compile may take, the wait for a compile slot included. Must be greater than 0. |
| `inline_function_compile_concurrency` | `2` | node | How many functions the node compiles at the same time. Each compile uses one core. Must be greater than 0. |
| `max_inline_functions_per_database` | `256` | cluster | The most inline functions in one database. `0` or less disables the limit. |
| `max_inline_definitions_per_user_per_minute` | `60` | cluster | The most `CREATE [OR REPLACE] FUNCTION` and `CREATE TRIGGER ... LANGUAGE csharp` statements that one user who is not a superuser can execute per minute on a node. The next one returns `CADB0848`. A runtime setting. `0` or less disables the limit. See [Compile rate](csharp-extensions.md#compile-rate). |
| `inline_triggers_enabled` | `false` | cluster | Accept `CREATE TRIGGER ... LANGUAGE csharp`. It is separate from `inline_functions_enabled`. A trigger body can execute SQL, and a function body cannot. With `false` the statement returns `CADB0841`. Triggers that exist continue to fire. One fire of a trigger body for one row has the time, memory and array limits above. See [Triggers in C#](csharp-extensions.md#triggers-in-c). |

Give the cluster-scope settings the same value on all nodes. A statement must pass or fail the
same way on any node. A call over its time, memory or array limit makes its statement return
`CADB0840`.

**The heap hard limit is the backstop for memory.** The memory limit is checked between two
framework calls. This means that one call can go over it before the next check. Run the server with
`DOTNET_GCHeapHardLimit` or `DOTNET_GCHeapHardLimitPercent` (a container memory limit gives a
default of 75%, see [Memory-proportional cache defaults](#memory-proportional-cache-defaults)).
Under a hard limit, a call that asks for more memory than the process may have gets an error that
the server catches, and the statement returns `CADB0840`. If no hard limit is set, the operating
system can stop the process.

## Kahuna engine section

The nested `kahuna:` map is an allow-listed passthrough to `EmbeddedKahunaOptions`, used for the one
embedded Kahuna node of a process, in both standalone and cluster mode (`Program.cs`).
Unset keys keep the CamusDB baseline for that mode. Unknown keys fail validation at startup.

The authoritative allow-list is `KahunaOptionsConfig.AllowedYamlKeys`; the commented `kahuna:` block
in `CamusDB/Config/config.yml` documents each key with its meaning. The keys fall into six main groups.

- Storage and WAL backends.
- Transaction timeouts and admission control.
- Worker and IO thread counts.
- Raft election and heartbeat timings.
- The cache and eviction knobs described below, and RocksDB shared memory.
- Backup and PITR settings.

The error message for a key that is not allowed lists all valid keys.

`storage_revision` names the directory Kahuna opens under `{data_dir}/kv`. The CamusDB baseline
pins it, together with the WAL revision, to the revision of the current key layout (`v2`). Do not
set it to an older revision to reach older data. The key layout of that data is not readable by the
current version, and all tables would appear empty. A server that finds an older revision's
directory logs a warning at start and leaves it untouched; move the data with a logical dump and
reimport, see [logical-dump-and-reimport.md](logical-dump-and-reimport.md).

Entry eviction is governed by two mechanisms: **size-based** caps (`max_entries_per_actor`,
`max_bytes_per_actor`) that bound how much an actor caches in memory, and a **time-based**
collection sweep (`collection_interval_ms`) that evicts up to `cache_entries_to_remove` entries
older than `cache_entry_ttl_ms` each pass. Raft-log compaction is governed together by
`compact_every_operations` (how often), `compact_number_entries` (trailing entries kept), and
`max_entries_per_compaction` (per-pass removal cap).

`compact_every_operations` counts persisted WAL **batches**: under load one batch carries on the order
of a hundred log entries. With a RocksDB Raft log, the log is reclaimed by dropping complete SST
files below a persisted compaction floor, and each pass advances that floor to the true floor. This
means that the cadence decides how often a pass happens. It does not limit how much a pass reclaims.

Storage backends: `memory`, `sqlite`, `rocksdb`, `solarium`.

### SQLite is outside the durability contract

`sqlite` as `storage` or `wal_storage` carries no guarantee against storage corruption. SQLite does
not checksum its pages. This means that a flipped bit in a value reads back as data, and a misplaced
page goes unnoticed. Its `-wal` replay stops at the first bad frame, and no error is returned. The SQLite
Raft log also writes its metadata at `synchronous=NORMAL`, whatever `wal_sync_writes` says. The server
defaults to `rocksdb` in standalone and cluster mode. SQLite is used only if you configure it.

### Storage damage

On `rocksdb`, a node stops serving from a store it finds damaged. The damage shows in the failed
statement, in the health check, and in the node's Raft role.

- The statement that reached the damage returns `CADB0837` (HTTP 500, gRPC `DataLoss`). This error
  is not retryable. It never means "no rows".
- `/v1/cluster/health` answers 503 with `ready: false`. With authentication on, only a superuser sees
  `storageFault` (the engine's message, which names the file) and `partitionStorageFaults` (one line
  for each hosted partition with a storage verdict).
- The node withholds its candidacy and steps down where it leads. It never closes, reopens or rewrites
  the damaged files.

`kahuna.raft_lost_acknowledged_entries_policy` decides what a partition does after a restart that
finds its log lost entries it had acknowledged. By default, the replica is quarantined.

| Value | Effect |
|---|---|
| `quarantine` (default) | The replica neither votes nor campaigns until its leader repairs it. A single-replica partition does not have a peer. It remains leaderless and writes a Critical log line. |
| `accept_and_log` | The node acknowledges the loss and logs the range. **This disables a safety fence.** The entries are gone for good on this node, and with one replica they are gone for good. |

A change to this key needs a restart.

`kahuna.raft_snapshot_max_pending_bytes` caps the snapshot bytes a node stages while receiving, on disk and in
memory together. A partition whose snapshot is larger cannot seed a new replica. This means that replacing a node does
not finish. Set it on all nodes to about twice the largest partition's snapshot, and restart. The node logs a
warning naming both sizes when it exports a snapshot above its cap.

`kahuna.raft_snapshot_transfer_step_timeout_ms` bounds one step of sending a snapshot. Building the complete export is
one step. Exports take longer with a partition's size and with branches that hold many distinct revisions, and a node
logs a warning with the time and size of any export of 30 s or more. Set it on all nodes above the longest one, and restart.

These protections do not yet cover `solarium`: a Solarium node has block and record checksums, but it
does not fail-stop on damage. The health response does not show a storage fault for it.

### Solarium storage engine

`solarium` selects [Solarium](https://solariumdb.github.io/docs/integrations/kahuna/), a managed
key-value store with the RocksDB API, for the key-value store (`storage`), the Raft WAL
(`wal_storage`), or both. A node on Solarium needs no native RocksDB library. CamusDB ships the
`SolariumDb.Kahuna` package and registers its two engines with the embedded node. This means that no
extension assembly is necessary.

```yaml
kahuna:
  storage: solarium
  wal_storage: solarium
  solarium_environment: default         # default | io_uring (Linux only)
  storage_compaction_rate_mbps: 50      # optional pacing, see below
```

Solarium and RocksDB use different file formats, and some RocksDB keys do not apply to Solarium.

- Solarium does not read RocksDB files, and RocksDB does not read Solarium files. A change of engine
  on a node with data needs a logical dump and reimport, see
  [logical-dump-and-reimport.md](logical-dump-and-reimport.md). All nodes of a cluster must use the
  same `storage` and `wal_storage`.
- The `rocksdb_*` tuning keys and the `wal_shard_*` keys do not apply to Solarium. The exceptions are
  `rocksdb_shared_memory_budget_mb` and `rocksdb_shared_memtable_budget_mb`. They size the one cache
  and the one memtable budget that a Solarium store and a Solarium WAL share. Solarium shares them
  whenever a size is above 0, and `rocksdb_shared_memory` is ignored for it.
- **Solarium's memtables and cached data blocks are native memory, as RocksDB's.** Since SolariumDb
  0.1.4 the engine stores its memtables in native arenas and its cached data blocks in native memory,
  outside the .NET heap. This means that the two shared budgets are sized from the machine memory on
  both engines (see [memory-proportional sizing](#memory-proportional-cache-defaults)), and a heap
  limit chosen for RocksDB fits a Solarium node. The part of Solarium on the heap is the index and
  filter partitions in the cache, about 2% of the table bytes (4% with LZ4), and their handles.
  SolariumDb 0.1.3 stored the complete cache and the memtables as managed objects; on a 4,096 MiB container with explicit 768 / 256 MiB
  budgets under a 60% heap limit (2,458 MiB) the leader of a 2-hour fault soak then died of a managed
  `OutOfMemoryException`, and the same node passed under a 3,072 MiB limit. A node on 0.1.3 needs that
  larger heap limit; a node on 0.1.4 does not. The node logs its budgets once at startup
  (`Solarium shared memory: ...`, see the sizing section), and `gc_memory_limit_bytes` in the
  [node stats](node-stats.md) shows the heap limit in force.
- `solarium_environment` selects the file I/O of both layers. `io_uring` needs Linux with io_uring,
  and the node does not start where the kernel cannot create a ring. Solarium ignores the key unless
  one layer is `solarium`.
- A backup bootstrap (`pitr_bootstrap_from`) needs RocksDB for the store and the WAL, see
  [backups-and-point-in-time-recovery.md](backups-and-point-in-time-recovery.md).

Two keys pace the flush and compaction writes of the key-value store. Both engines read them.

| Key | Effect |
|-----|--------|
| `storage_compaction_rate_mbps` | Rate limit in MB/s (10^6 bytes) for flush and compaction writes. 0 or unset = no limit. |
| `storage_max_background_compactions` | The number of compactions that run at once. 0 or unset = the engine default. |

If neither key is set, Solarium does one compaction at a time, and it does not pace writes. RocksDB
does max(2, CPUs / 2) at a time. With a rate, Solarium also does max(2, CPUs / 2), unless
`storage_max_background_compactions` sets the number. Several unpaced compactions take the disk from
the synced writes of a large store. Choose a rate of about a third to half of what the disk commits in
synced writes. A rate that is too low lets level 0 grow until writes slow down.

### One-phase apply-time validation

`kahuna.one_phase_apply_time_validation` (default **on**) lets a read-modify-write or read-carrying
transaction commit in **one** durable round, where it would otherwise take two. The bundled commit
carries its on-partition read dependencies, and each replica judges them at apply time, in log order, against the
partition's replicated committed-head ledger. With it off, those transactions pay one extra Raft
proposal plus the replica-fence exchange per commit. The flag does not change which
transactions are eligible. A transaction is eligible with only one participant partition, the anchor on
that partition, and a read set the gate can decide there. A cross-partition transaction uses
two-phase commit either way. CamusDB's grouped key layout makes it pay. A table row and its index
entries hash to the same partition. This means that a single-row update is a one-partition transaction. Measured on a three-partition `accounts` workload,
turning it on is worth about **+24% throughput** at a 99.8% one-phase rate.

`kahuna.staged_base_fence_retention_ms` (default 600,000) is the horizon the fence — and the ledger the
gate judges against — remembers a key's last transactionally committed head for. It must comfortably
exceed the longest transaction lifetime the deployment allows. A transaction that began before the
horizon returns an error. Pruned memory cannot be told apart from "no commit happened".

Three operating rules apply. Kahuna enforces all of them, and CamusDB does not check them.

- **Same value on all nodes of the group** — for both keys. The gate is a property of the replicated
  command, and nodes with different horizons would judge the same bundled commit differently.
- **Never enable it across mixed Kahuna versions.** A node too old to know the check skips it and
  commits where a current node returns an error. This forks the state machine. Set
  `one_phase_apply_time_validation: false` for the duration of a rolling upgrade and remove the override
  once the last old node is gone.
- **Start once with it off.** A node that starts with it on over a prepared-intent snapshot written
  before the ledger existed fails at startup. It also does not install a partition snapshot that was
  exported without a ledger.

Because the default is on, the last rule is an **upgrade step**. Bringing a
data directory written by a build that predates the committed-head ledger into a build that defaults the
gate on will fail to start. Upgrading such a deployment takes one extra restart.

1. Set `one_phase_apply_time_validation: false` in the `kahuna:` block before starting the new build.
2. Start the cluster and let a checkpoint complete. The checkpoint rewrites the snapshot of each partition with its ledger.
3. Remove the override and restart.

A cluster created on a build that defaults it on needs none of this — its first snapshot already carries
a ledger.

### Materialize on resolve

`kahuna.durable_materialize_on_resolve` (default **on** in CamusDB, and in Kahuna since 1.19.0) sets the record shape of
a committed durable transaction. The commit's settle installs each committed value from the prepared
intent on each replica. This means that Kahuna writes no materialization record per key. On the anchor
partition, a two-row transaction then costs about four Raft entries where it would cost six (the
one-phase bundle's three plus one settle). It also takes about two scheduler submissions, down from four.
Followers apply fewer entries, and a device writes fewer bytes per commit. CamusDB turns it on in both
baselines. The shape passed two consecutive clean two-hour fault soaks on two released Kahuna versions
(conservation exact, no divergence, all faults recovered). It is a per-group property. This means that
the baseline decides it for all nodes. An
explicit value, including `false`, always wins.

Two operating rules apply. The record shape enforces both, and CamusDB does not check them.

- **Upgrade all nodes before you turn on the flag.** All nodes must run a Kahuna build that installs
  on a materializing settle (Kahuna.Core 1.10.1 or later) before any node has this on. An older node
  resolves the intent but does not install the value, and it loses that write locally. Turning it off
  is safe at any time. During a rolling upgrade from an older build, pin
  `durable_materialize_on_resolve: false` on all nodes until the last one is upgraded. Then remove the
  key.
- **Same value on all nodes of the group**, for the same reason as the one-phase gate above.

### Log entries per commit

Two keys reduce the Raft entries that a committed transaction writes on its partition. Both are off by
default and need Kahuna.Core 1.19.0 or later. With materialize on resolve, a two-row transaction that
commits in one phase writes four entries: three for the bundle (record init, prepare, decision) and one
for the settle.

`kahuna.durable_merge_adjacent_settles` (default **off**) writes one entry for each run of adjacent
settles in a batch of the partition's write scheduler. The commands keep their order in the log, and
each build that installs on a settle reads the entry. The key needs no upgrade order, it can differ
between nodes, and turning it off is safe at any time. A merged entry carries the settles of several
transactions, so the durability floor of the partition moves in larger steps.

`kahuna.durable_one_phase_bundle_entry` (default **off**) writes the bundle of a one-phase commit as one
entry. Two operating rules apply:

- **Upgrade all nodes before you turn on the key.** All nodes of the group must run Kahuna.Core 1.19.0
  or later before any node has this on. An older node skips the entry with no error, and it then holds
  no record and no intent of the transaction. A leader asks its replicas whether they apply the entry,
  and it writes three entries until all of them confirm. This check catches a key that is set too
  early. It does not cover an older node that joins later, or a node that starts again on an older
  build and reads a log that holds bundle entries.
- **Turning it off is safe at any time.** A node of a current build applies the entry whatever its own
  value, so the key can differ between nodes while you change it.

With both keys on, a two-row transaction writes between one and two entries: one for the bundle and a
share of a merged settle entry.

### Persistent MVCC revision retention

Each version of a row is a physical row in the KV store. If history is not pruned, a hot table's
history and the disk use behind it grow without limit for the life of the store.

By default (both keys unset), CamusDB bounds the persisted history **by age, aligned with the effective
PITR window** (`kahuna.pitr_window_seconds`, default 3600): Kahuna's background writer prunes revision
rows older than the window. An as-of restore within the window always finds the revisions it needs. A key that was not changed
since before the window restores from its current row, which pruning never removes.
Snapshot reads older than the retention age are **not guaranteed**; readers that must reach further
back (branch forks) pin their own snapshot floors, which pruning honors.

A snapshot read that needs history that pruning already removed returns `CADB0847`
(`SnapshotTooOld`, HTTP 409, gRPC `FailedPrecondition`). It never returns the key as
absent or with an older value. Examples are an `AS OF SYSTEM TIME` read older than the retention age,
or a statement that executes for longer than the retention age. The engine does not retry it. A read
at the same timestamp would get the same answer. Start a new transaction, or read as of a newer time.

- `kahuna.persistent_revision_retention_age_seconds` — explicit age bound. Must be ≥ the effective
  PITR window (pruning below the window deletes history an in-window restore may need — the backup
  machinery would then fail closed). `0` disables age pruning and, together with an unset count,
  restores the unbounded-history behavior.
- `kahuna.persistent_revision_retention_count` — hard per-key revision cap, `0` (default) disabled.
  A count cap can cut below the PITR window on a hot key; prefer the age bound unless a per-key cap
  is the goal.

### Memory profile

`memory_profile` (`--memory-profile`) selects *how* the four cache-sizing knobs below are defaulted,
and under `dev` it also selects workstation GC (see below). It does not change worker counts or
durability. The only behavior an application can observe is how often a read is served from cache
and how often from disk.

| Profile | Block cache | Memtable sub-budget | Actor caches | Total |
|---------|-------------|---------------------|--------------|-------|
| `prod` (default) | 10% of machine memory, ≤ 2 GiB | ¼ of it, raised to the Raft-log flush unit (≤ half the cache), ≤ 1 GiB — see below | 6.25% of the managed heap budget, ≥ 64 MiB; up to four times that when every actor fills its cap (from a 2 GiB heap budget) | ~16% of memory (~1.5 GiB on an 8 GiB box), up to ~37% when every actor fills its cap |
| `dev` | 64 MiB | 16 MiB | 32 MiB | ~96 MiB, on any machine |

`dev` is for a node sharing a developer machine with the application being built against it. The
budgets are fixed. This means that the same node is the same size on a 64 GiB workstation and in a
2 GiB container. The cost is throughput once the working set outgrows the cache — the TPC-C run that
motivated proportional sizing was ~5x slower against a 320 MiB block cache — so do not use it on a
server. The caches are ceilings filled lazily. A burst of large writes can push
process RSS well above the cache total. This is because transient managed allocations and the .NET GC dominate the
peak. Server GC (the build default, one heap per core) also does not release the space a burst grew
after the burst ends: an idle dev node on a 10-core machine was measured at 2.1 GB committed for a
~160 MB live heap.

For this reason, `dev` also uses workstation GC for the node. The GC mode is fixed by the runtime
before any configuration is read. The server starts itself again as a child process with
`DOTNET_gcServer=0` and the original arguments. The first process waits for the child and passes its
exit code through. SIGINT, SIGTERM and SIGQUIT sent to the first process are forwarded to the child.
This means that `docker stop`, systemd and `kill` shut the node down gracefully. A SIGKILL cannot be
forwarded and leaves the child running. If you set `DOTNET_gcServer` (either value) yourself, the
relaunch is turned off and the GC mode you chose is used. The startup banner states which GC the node
uses.

An explicit `kahuna.*` budget always beats the profile. For example, `dev` plus one raised budget is a
valid combination.

### Transaction state and the heap limit

For 60 to 90 seconds, Kahuna retains a record of each committed write transaction. It also retains a
receipt for each key the transaction wrote and the identity of each settled key. This state lives in the managed heap and grows
with the write commit rate. No cache budget covers it and no CamusDB setting shortens it. With two
keys of about 30 characters in each transaction it is about 0.10 MiB for each write commit a second.

Plan a managed heap limit of 0.25 MiB for each write commit a second at the peak rate: 1,500 MiB for
6,000 write commits a second. The node logs a warning (`Retained transaction state is ...`) when the
state passes 40% of the heap limit. A node that continues above that share can end with an
out-of-memory exception. Kahuna ends the process on an out-of-memory exception.

This was measured on a 2,048 MiB node with a heap limit of 1,229 MiB, at 64 workers and about 12,000
transactions a second with half of them writes (Serializable). The warning appeared 70 seconds into
the load at 41% to 43% of the limit in six of six tests, and two of the six died of an out-of-memory
exception in the next minute. The four others ran to the end at the heap limit, with 8% to 11% of the
time in GC pauses.

### Garbage collector

The server is built with server GC (one heap per core) and with the GC's dynamic adaptation mode
off (`System.GC.DynamicAdaptationMode` = 0 in the runtime configuration the build writes). The
runtime's own default for that mode is on.

Under dynamic adaptation the GC sizes the young generation from the share of memory that survives a
collection. A node with frequent Raft rounds leaves little alive at each collection. This means that the
young generation shrinks and collections come more often. With the write aggregator's exact hold
(`kahuna.key_value_write_precise_wake`), a busy leader settled into that state in some tests and
did not leave it: 13.6% of its time in GC pauses on one node at 64 workers, and 24% with throughput at 0.80x on
the leader of three nodes. With the mode off no measured run did, and a 2-hour fault soak passed.

The mode off has a cost in one measured configuration: three nodes at 128 workers ran 0.96x to 0.97x
of the adaptive GC. An idle node also uses 100 to 250 MiB more memory. On nodes of 4 GiB or more,
`DOTNET_GCgen0size=3000000` (hexadecimal, 48 MiB per heap) in the environment removed that cost in
the same measurement. The young generation is that size times the heap count, 768 MiB on 16 cores. Do not set it on a
smaller node, because it does not fit the 1,229 MiB heap limit of a 2 GiB node. The runtime
reads this value from the environment only.

`DOTNET_GCDynamicAdaptationMode=1` in the environment turns the mode back on. The node then logs a
warning at startup when the write aggregator uses short cycles (`kahuna.key_value_write_precise_wake`
on, or `kahuna.key_value_write_post_completion_hold_ms` 0, which is the standalone default). To run with the mode on, set
`kahuna.key_value_write_precise_wake: false` and keep the hold at 1 ms or more. Workstation GC, which
the `dev` profile selects, does not use dynamic adaptation.

### Memory-proportional cache defaults

Under `memory_profile: prod`, most unset keys keep Kahuna's own default, but the four cache-sizing
knobs are an exception: when left unset they are computed at startup from the node's memory.
A fixed 320 MB block cache was measured forcing a 1.2 GB TPC-C working set
through disk reads on nearly all statements; sizing it to the machine took the same workload from
24.5 to 119.6 tx/s at 8 clients.

Two different memory sizes feed the computation, one for each kind of cache.

- **Machine memory** sizes the RocksDB pair. It is the container's cgroup memory limit when one is
  set, else the machine's physical RAM. RocksDB's block cache and memtables are native allocations.
  This means that they are not sized from the .NET heap limit.
- **Managed heap budget** sizes the actor caches, which are in the managed heap. It is the value that
  the GC gives: the heap hard limit when one is in force, else the machine memory. The heap hard limit
  is set by `DOTNET_GCHeapHardLimit` or `DOTNET_GCHeapHardLimitPercent`. In a memory-limited container,
  the runtime sets a default limit of 75% when neither variable is set.

If no heap limit is set, the two sizes are the same. With a heap limit they differ: the shared budgets sit **outside** the heap limit and count against the container beside it,
on RocksDB and on Solarium alike ([Solarium's are native memory too](#solarium-storage-engine)). On a
4,096 MiB container with `DOTNET_GCHeapHardLimitPercent=3C` (60%, the value is hexadecimal), the heap
budget is 2,457 MiB and both engines size the budgets from 4,096 MiB: RocksDB gets a 409 MiB cache and
192 MiB of memtables, Solarium 409 MiB and 204 MiB (its Raft log's flush-unit floor, capped at half the
cache). Leave room for the budgets beside the heap when you pick the heap percentage.

| Key | Computed when unset | Clamp |
|-----|---------------------|-------|
| `rocksdb_shared_memory_budget_mb` | 10% of machine memory | 64 MiB – 2 GiB (320 MiB floor if 10% reaches it) |
| `rocksdb_shared_memtable_budget_mb` | a quarter of the block cache, raised to the Raft-log flush unit (at most half the cache) | 16 MiB – 1 GiB (128 MiB floor if a quarter reaches it) |
| `max_bytes_per_actor` | 6.25% of the managed heap budget (≥ 64 MiB for the layer) ÷ the key/value actor count, times 4 from a 2 GiB heap budget (at most the whole layer) | 1 MiB – 2 GiB per actor (8 MiB floor if the share reaches it) |
| `max_entries_per_actor` | `max_bytes_per_actor` ÷ ~512 B | 2k – 4M (10k floor if the share reaches it) |

A floor applies only if the computed value reaches it.

The floors in parentheses yield on a small node: below them the percentage governs, down to the lower
bound of the clamp. A fixed floor on a small container used to claim several times its share of memory.

**The memtable flush-unit floor.** When the KV store and the Raft WAL share one
WriteBufferManager (`rocksdb_shared_memory` on, `storage` and `wal_storage` both `rocksdb`), the Raft
log flushes at its flush unit plus headroom: `wal_shard_write_buffer_size_mb` ×
(`wal_shard_min_write_buffer_number_to_merge` + 1), which is 192 MiB at Kommander's defaults. Kommander
checks the same figure when it opens the WAL. Below it, RocksDB flushes the Raft log whenever the
shared budget fills, where it would otherwise flush at its 128 MiB unit. RocksDB then logs a warning
that names both sizes.

```text
the shared memtable budget (61 MB) is below the shard flush unit plus headroom (192 MB)
```

In a 10-minute write probe at a 61 MiB budget, all 349 Raft-log flushes were forced by the budget, at
about 6 MiB each. CamusDB therefore raises the computed memtable budget to that unit, taking at most
**half** of the block-cache budget. The memtables are charged against that budget, and reads need the
rest. For example, a 4,096 MiB node gets a 409 MiB cache and a 192 MiB memtable budget. Below roughly 3.75 GiB of
machine memory the unit does not fit in half the cache, the budget stops at that half, and the warning
above is expected; a node that wants the unit covered there sets `rocksdb_shared_memtable_budget_mb`
explicitly. An explicit `rocksdb_shared_memtable_budget_mb` always wins. The `dev` profile does not
apply the floor.

The floor needs Kommander 1.6.6 or later and is the `RaftWalFlushUnitFloorEnabled` constant in
`EmbeddedKahunaOptionsBuilder`. It shipped off on Kommander 1.6.5. On that version, an unstarved budget let the Raft-log store retain
all write-ahead `.log` files for the life of the process. A rarely written column family (the meta
partition's shard) flushed only under budget starvation, and no other mechanism released the files.
The files grew from 443 MiB to 2,709 MiB in ten minutes per node, against ~230 MB of live tables, and
a follower restart took 4 s longer. Kommander 1.6.6 bounds the write-ahead files.

At startup the node logs the values it uses, once, before RocksDB opens.

```text
RocksDB shared memory: block cache 409 MiB, memtable budget 192 MiB; Raft-log flush unit plus headroom 192 MiB (covered). Native budgets sit outside the managed heap: machine memory 4096 MiB (cgroup), managed heap budget 2457 MiB.
```

A Solarium node logs the same figures with a `Solarium` prefix. They are checked against the fixed
flush unit of the Solarium WAL.

```text
Solarium shared memory: cache 409 MiB, memtable budget 204 MiB; Raft-log flush unit plus headroom 256 MiB (NOT covered: Raft-log flushes will be budget-forced). Native budgets sit outside the managed heap, as RocksDB's: machine memory 4096 MiB (cgroup), managed heap budget 2457 MiB.
```

The machine-memory source is `cgroup`, `meminfo` (Linux RAM), `sysctl` (macOS RAM), or `gc`. The `gc`
source means that the machine size was not readable and the GC figure is used.

That is roughly 16% of memory across both cache layers, and at most 4 GiB in total however large the
machine is. The fractions and the ceilings are deliberately modest: an unconfigured node
is far more often a developer workstation or a CI container sharing the box with a compiler and an
IDE than a dedicated database server. An explicit value always wins over the computed one. Take an
8 GiB, 8-core machine where none of them is set and no heap limit applies. It gets an 819 MiB block
cache, a 204 MiB memtable sub-budget (above the 192 MiB flush unit before any floor applies), and
a 512 MiB actor-cache layer over 32 actors, with a cap of 64 MiB per actor (four shares of 16 MiB).
That is about 1.5 GiB while the data sits on a few actors, and up to about 3 GiB when all 32 actors
fill their cap.

A dedicated server should raise all four explicitly. The sizing above is a floor to build from.

The 6.25% share and its 64 MiB floor bound the *complete* actor-cache layer, and are
then divided by the key/value actor count (`kahuna.key_value_workers`, or max(32, 4 × CPU cores)
when unset); only the 8 MiB per-actor minimum is per actor. Adding cores
therefore splits the same budget more ways and does not grow it — a machine with many cores
relative to its RAM does not end up with a multiple of the intended share. From a 2 GiB heap budget
one actor may hold four of those shares (see `max_bytes_per_actor` below), so the layer is the
expected size of the caches and four times the layer is their upper bound.

The RocksDB pair is shared: `rocksdb_shared_memory` (default on, and a no-op unless `storage` and
`wal_storage` are both `rocksdb`) makes one block cache and one write-buffer manager serve both the
KV store and the Raft WAL. The memtable sub-budget is charged **against** the total block-cache
budget and is not added to it. It must be ≤ the total. That comparison is made against the *effective*
post-merge pair. This means that overriding only one of the two can produce an inconsistent pair, such
as a 100 MiB total against an explicit 512 MiB memtable. Startup then fails with `InvalidConfig`. The
computed memtable default is at most the total, whether or not the flush-unit floor applies. Set both
together.

A Solarium WAL (`wal_storage: solarium`) always charges its memtables to the shared memtable budget.
The memory-proportional sizing therefore raises an unset budget to 256 MiB, the Solarium WAL's
128 MiB flush unit plus one memtable of headroom, up to at most half the cache. Below that size, the
Solarium WAL writes a warning at open and flushes the Raft log in small pieces. The 256 MiB fits in
half the cache only from 5 GiB of machine memory; a smaller Solarium node that wants the unit covered
sets both budgets explicitly.

`max_bytes_per_actor` is **per actor**: multiply by the actor count (`key_value_workers`,
default max(32, 4 × CPU cores)) to get the total.

All rows of a table are cached by one actor, and each of its indexes by one more, so the actors fill
unevenly. From a 2 GiB heap budget the computed cap is therefore four even shares of the layer: with
a 2,458 MiB heap limit and 64 actors, 9.6 MiB per actor where the even share is 2.4 MiB. The cap is
enforced per actor, so the actor caches together can reach four times the layer, a quarter of the
heap budget, on a node where every actor fills its cap. Below a 2 GiB heap budget the cap stays at
the even share. Set `max_bytes_per_actor` and `max_entries_per_actor` to choose another size.

`kahuna.key_value_write_max_in_flight_batches_per_partition` (default **1**, Kahuna 1.7.1) is how many
write-aggregator batches a partition may have waiting on Raft at once. At 1, the next batch is dispatched
after the previous one completes. This means that a partition's batch rate is bounded by
1 / (Raft round latency), and throughput scales only with items per batch. Higher values pipeline batches in FIFO order; leave it at 1
until an interleaved A/B on the soak harness shows the gain for your shape. Measured 2026-09-09 on the
`bank` candidate arm with the device held constant: 2 and 4 in flight lowered throughput 15-20% — the
Raft round stretched with the in-flight count while batches got smaller.

`kahuna.key_value_write_linger_ms` (default **1** in cluster mode, **0** on a standalone node) and
`kahuna.key_value_write_max_batch_items` (default **512**) shape the write aggregator's batches. The linger is how long the oldest queued item may wait before
a batch is dispatched while no batch is in flight. The cap bounds a batch's items. The measured round
costs ~1.4 ms fixed plus ~18 µs per item. This means that denser batches are the lever at one
partition. A linger above the ~2.6 ms that an item already waits trades write latency for density.
This linger belongs to the aggregator. It is separate from `wal_group_commit_linger_ms` (Kommander's
cross-partition WAL group commit, inert at one partition). Measured 2026-09-09: the linger itself is
inert at full occupancy (1, 3 and 5 ms all gave the same batches). A completing batch re-dispatches
whatever is queued.

`kahuna.key_value_write_post_completion_hold_ms` (default **1** in cluster mode and **0** on a
standalone node; Kahuna's own default is 0) adds density at full occupancy. After a batch completes, the aggregator waits this long before
dispatching the next one. This means that more items accumulate per batch. The aggregator has one Raft
round in flight per partition. At full load, a round carries only what arrived during the previous
round. The hold
applies after a completion only: a write that arrives at an idle partition waits for the linger. Set
`0` to dispatch each round as soon as the one before it completes; an explicit value always wins over
the shipped default.

A standalone node runs with no hold and no linger. It has one voter, so a round is a local log write
and a partition sends a batch as soon as its log is free. Measured on the bank shape in standalone
mode on a memory-backed store against a 1 ms hold and a 1 ms linger, Serializable, three runs each:
at 8 workers the commit call fell from 0.87 to 0.35 ms and throughput rose 14.5% for 26% more CPU
per transaction; at 64 and 256 workers throughput stayed level and CPU per transaction rose 7% and
2%. On an NVMe drive a round is a device sync of 3 to 20 ms and the two settings did not differ
beyond the drive's variation between runs. A standalone node that is short of CPU at high concurrency
can set both keys to `1`. A cluster of one node keeps the cluster defaults, because its configuration
stays in force when other nodes join it.

`kahuna.key_value_write_precise_wake` (default **true**) fires the aggregator's hold, linger and
queue-age wakes from one dedicated timer thread per node. The .NET timer queue measures a delay with a
clock that advances once per kernel tick on Linux (4 ms on a 250 Hz kernel). On the timer queue, a
1 ms hold therefore ends at the next tick, and a write cycle lasts a full tick. The dedicated thread
sleeps the integer-millisecond part of the delay in a kernel wait and spins the sub-millisecond remainder. This
means that the hold ends within about 0.1 ms of its deadline. `false` returns the wakes to the timer queue.

`kahuna.key_value_write_fill_target_trigger` (default **true** in CamusDB; Kahuna's own default is
false) adds fill-or-time dispatch on top of the hold: a held or lingering partition buffer is dispatched
as soon as it reaches the learned fill target, the upper median of its last 16 timer-dispatched batch
sizes. One in eight fill-triggered dispatches waits for the timer as a probe. This means that the target
follows the load; the learned target is exported as the `kahuna.kv.write.fill_target` histogram.

The table shows throughput ratios measured on the bank shape (Serializable isolation, optimistic
locking, GC adaptation off with a 48 MiB gen0 budget per heap). Each ratio is relative to the 1 ms hold
on the timer queue.

| Nodes, workers | Storage | Exact hold | Exact hold and fill trigger | Hold 0, linger 0 |
|---|---|---|---|---|
| 1, 8 | tmpfs | 1.75x | 1.89x | 2.26x |
| 1, 64 | tmpfs | 1.14x | 1.15x | 1.18x |
| 3, 8 | tmpfs | 1.32x | 1.38x | 1.29x |
| 3, 64 | tmpfs | 1.02x | 1.03x | 1.01x |
| 3, 256 | tmpfs | 0.99x | 0.99x | 0.98x |
| 1, 64 | NVMe | 1.07x | 1.10x | 1.09x |
| 3, 8 | NVMe | 0.98x | 1.20x | 1.29x |
| 3, 64 | NVMe | 1.14x | 1.18x | 1.02x |

The shipped pair is the exact hold with the fill trigger. Its cost is device headroom: about 20% more
device writes per transaction than the timer-queue hold, and a drive that ran near 63% utilisation
where the timer-queue hold ran near 35%. A hold of 0 with a linger of 0 is faster on one node and is
not the default. On three nodes at 8 workers it used 2.2 times the CPU per transaction, it wrote 16%
to 47% more to the device per transaction, and on a three-node NVMe cluster four runs ranged from
6,526 to 9,850 transactions a second.

A shorter write cycle means more Raft rounds per second, and the garbage collector has to be set for
that: see [Garbage collector](#garbage-collector).

`kahuna.rocksdb_direct_reads` (default **off** in CamusDB; Kahuna's own default is on) selects how the
RocksDB key/value backend reads SST files. With direct I/O, the block cache is the only in-RAM read
cache, and each miss is a physical device read. On a node whose disk is already carrying the Raft
WAL's fsyncs, those misses queue behind the flushes. The key/value actor performs its as-of-timestamp
history read synchronously. This means that one device-bound miss stalls all keys on that actor. In
the 45-minute `bank` soaks this took leader reads from 1 ms to 40-50 ms with the CPU idle. Buffered
reads serve misses from the page cache; the page cache is charged to the container's memory
cgroup and shows in `docker stats`, but it is reclaimable and does not threaten an OOM kill. Set it
`true` to restore the block-cache-only footprint.

### Raft WAL shard column families

Eight `kahuna.wal_shard_*` keys size the RocksDB column families that hold the Raft log. **CamusDB sets
none of them.** Each key is unset by default, and unset leaves Kommander's own default for that field in
force, field for field. These keys override a default that is tuned and measured in Kommander. They
take effect if `kahuna.wal_storage` is `rocksdb`, and are ignored otherwise.

A Raft log row dies when a compaction pass covers it with a range tombstone. A row that meets its
tombstone within one flush unit is dropped at flush and does not reach L0. A row flushed earlier has to be
rewritten down the levels before it can die, and that rewrite is the write amplification these knobs cut.

| Key | Kommander default | What it does |
|-----|-------------------|--------------|
| `wal_shard_write_buffer_size_mb` | 64 | Size of one shard memtable. Widens the flush unit — and the restart replay unit. |
| `wal_shard_min_write_buffer_number_to_merge` | 2 | Immutable memtables merged into one flush. The cheaper way to widen the flush unit. |
| `wal_shard_max_write_buffer_number` | 4 | Memtables per shard, mutable plus immutable. Must exceed the merge count. |
| `wal_shard_level0_file_num_compaction_trigger` | 8 | L0 files that trigger compaction into the base level. |
| `wal_shard_level0_slowdown_writes_trigger` | 28 | L0 files at which writers are slowed. |
| `wal_shard_level0_stop_writes_trigger` | 44 | L0 files at which writers are stopped. |
| `wal_shard_max_bytes_for_level_base_mb` | RocksDB's 256 | Base-level size. Sized above the retained log, the live log remains in the base level, and one compaction annihilates a tombstone's rows. |
| `wal_shard_universal_compaction` | `false` (leveled) | Universal compaction in place of leveled: few large sorted runs, no L0-to-Lmax cascade. |

Three cautions apply to all keys in the group.

- **Memory.** The worst case per actively-written shard is `wal_shard_write_buffer_size_mb` ×
  `wal_shard_max_write_buffer_number` — 256 MB at the defaults. That competes with
  `kahuna.rocksdb_shared_memory_budget_mb` on a small node. Under the shared WriteBufferManager, an
  over-budget node flushes early. This quietly returns the flush unit to its old size, and memory does
  not grow. This means that raising the memtable knobs and leaving the budget as it is can have no
  effect. The
  computed memtable budget follows these two keys through the flush-unit floor above, but only up to
  half the block cache; past that, raise `kahuna.rocksdb_shared_memtable_budget_mb` alongside them.
- **Restart.** A wider flush unit is a wider WAL-replay unit. A restart re-reads more before the node
  is ready. Measure that as well as the bytes written.
- **Universal makes the level size inert.** Setting `wal_shard_universal_compaction: true` together with
  `wal_shard_max_bytes_for_level_base_mb` is allowed. However, RocksDB does not consult
  level sizing under universal compaction. The second key is then ignored.

Validation returns an error for a size outside 1..65536 MiB, a count outside 1..64, a
`wal_shard_max_write_buffer_number` that does not exceed `wal_shard_min_write_buffer_number_to_merge`, and
L0 triggers that are not strictly increasing. The last two are checked against the **effective** values,
substituting Kommander's defaults for the keys left unset. This means that a one-sided override cannot
form an invalid combination silently.

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
| `kahuna.raft_lost_acknowledged_entries_policy` other than `quarantine` / `accept_and_log` | `InvalidConfig` |
| `kahuna.raft_snapshot_max_pending_bytes` ≤ 0 | `InvalidConfig` |
| `kahuna.raft_snapshot_transfer_step_timeout_ms` ≤ 0 | `InvalidConfig` |
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

See `CamusDB/Config/config.yml` for inline documentation of each field.
