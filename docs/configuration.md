# Configuration

CamusDB reads `CamusDB/Config/config.yml` at startup and merges CLI flags and environment
variables into a single resolved configuration object.

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
| `default_isolation_level` | — | `serializable` |
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

Allowed keys: `storage`, `storage_revision`, `wal_storage`, `wal_revision`, `wal_sync_writes`,
`default_transaction_timeout_ms`, `locks_workers`, `key_value_workers`,
`background_writer_workers`, `read_io_threads`, `write_io_threads`, `start_election_timeout_ms`,
`end_election_timeout_ms`, `start_election_timeout_increment_ms`,
`end_election_timeout_increment_ms`, `heartbeat_interval_ms`, `voting_timeout_ms`,
`max_entries_per_actor`, `max_bytes_per_actor`, `compact_every_operations`.

Storage backends: `memory`, `sqlite`, `rocksdb`.

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
