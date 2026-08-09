# Configuration

CamusDB reads `CamusDB/Config/config.yml` at startup and merges CLI flags and environment
variables into a single resolved configuration object.

To see what a running node actually resolved — including which layer supplied each value — run
[`SHOW VARIABLES`](show-variables.md) against it rather than reconstructing the merge by hand.

## Precedence

Highest wins:

1. **CLI flags** — only flags you explicitly pass override YAML (nullable options; no sentinel defaults).
2. **Environment variables** — currently `CAMUS_KEY_RANGE_SHARDING` overrides `key_range_sharding`.
3. **`config.yml`**
4. **Built-in defaults** in `ConfigDefinition` / `CamusDBConfig`.

Example: YAML `mode: cluster` with `--mode standalone` starts in standalone mode. YAML
`mode: cluster` with no `--mode` flag stays in cluster mode.

## CLI ↔ YAML mapping

| YAML field | CLI flag | Default |
|------------|----------|---------|
| `data_dir` | `--data-dir` | `Data` (process cwd) |
| `mode` | `--mode` | `standalone` |
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
| `kahuna.*` | — | mode-specific baseline |

Parser-cache, lock/isolation, spill, and query-result-cache knobs are YAML-only (operational tuning,
not per-node startup flags). The result cache is **on by default** (opt-in per query via a
`{cache=…}` hint); set `query_result_cache_enabled: false` to turn it off entirely. See
[query-result-cache.md](query-result-cache.md) for what each knob does and operator guidance.

## Kahuna engine section

The nested `kahuna:` map is an allow-listed passthrough to `EmbeddedKahunaOptions`, used for
both the cluster node (`Program.cs`) and standalone per-database nodes (`DatabaseOpener`).
Unset keys keep the CamusDB baseline for that mode. Unknown keys fail validation at startup.

The authoritative allow-list is `KahunaOptionsConfig.AllowedYamlKeys`; the commented `kahuna:` block
in `CamusDB/Config/config.yml` documents each key with its meaning. Broadly it covers storage and
WAL backends, transaction timeouts and admission control, worker/IO-thread counts, Raft election and
heartbeat timings, the cache and eviction knobs described below, RocksDB shared memory, and backup /
PITR settings. A rejected key's error message lists every accepted one.

Entry eviction is governed by two mechanisms: **size-based** caps (`max_entries_per_actor`,
`max_bytes_per_actor`) that bound how much an actor holds in memory, and a **time-based**
collection sweep (`collection_interval_ms`) that evicts up to `cache_entries_to_remove` entries
older than `cache_entry_ttl_ms` each pass. Raft-log compaction is governed together by
`compact_every_operations` (how often), `compact_number_entries` (trailing entries kept), and
`max_entries_per_compaction` (per-pass removal cap).

Storage backends: `memory`, `sqlite`, `rocksdb`.

### Memory-proportional cache defaults

Most unset keys keep Kahuna's own default, but the four cache-sizing knobs are an exception: when
left unset they are computed at startup from the machine's available memory (container limits
respected) rather than from a fixed constant. A fixed 320 MB block cache was measured forcing a
1.2 GB TPC-C working set through disk reads on nearly every statement; sizing it to the machine
took the same workload from 24.5 to 119.6 tx/s at 8 clients.

| Key | Computed when unset | Clamp |
|-----|---------------------|-------|
| `rocksdb_shared_memory_budget_mb` | 25% of RAM | 320 MiB – 8 GiB |
| `rocksdb_shared_memtable_budget_mb` | a quarter of the block cache | 128 MiB – 1 GiB |
| `max_bytes_per_actor` | 12.5% of RAM ÷ `key_value_workers` | 64 MiB – 4 GiB per actor |
| `max_entries_per_actor` | `max_bytes_per_actor` ÷ ~512 B | 50k – 4M |

That is roughly 40% of RAM across both cache layers. An explicit value always wins over the
computed one. On an 8 GiB machine with none of them set: 2048 MiB block cache, 512 MiB memtable
sub-budget, and 1 GiB of actor caches in total — about 3.5 GiB.

The RocksDB pair is shared: `rocksdb_shared_memory` (default on, and a no-op unless `storage` and
`wal_storage` are both `rocksdb`) makes one block cache and one write-buffer manager serve both the
KV store and the Raft WAL. The memtable sub-budget is charged **inside** the total block-cache
budget, not added to it, and must be ≤ it. That comparison is made against the *effective*
post-merge pair, so overriding only one of the two can produce an inconsistent pair — a 100 MiB
total against a computed 512 MiB memtable — and fails startup with `InvalidConfig`. Set both
together. Likewise `max_bytes_per_actor` is **per actor**: multiply by `key_value_workers` (default:
one per CPU) to get the total.

## Validation errors

| Condition | Error |
|-----------|-------|
| Unknown `mode` | `InvalidConfig` |
| Port outside 1..65535 | `InvalidConfig` |
| `http_peers` count ≠ `peers` count | `InvalidConfig` |
| Invalid `default_isolation_level` | `InvalidConfig` |
| `range_lock_heartbeat_interval_ms` ≥ `range_lock_expires_ms` (when expiry > 0) | `InvalidConfig` |
| `spill_threshold_rows` ≤ 0 | `InvalidConfig` |
| `spill_merge_fan_in` ≤ 0 | `InvalidConfig` |
| Unknown `kahuna` key | `InvalidConfig` |
| Unknown `kahuna.storage` / `kahuna.wal_storage` | `InvalidConfig` |
| `kahuna.start_election_timeout_ms` ≥ `kahuna.end_election_timeout_ms` | `InvalidConfig` |

See `CamusDB/Config/config.yml` for inline documentation of every field.
