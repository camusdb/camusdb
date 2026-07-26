# Standalone performance diagnostics

CamusDB can emit low-overhead, **opt-in** OpenTelemetry metrics and sampled traces from a standalone
node, so a workload run can be attributed to real server stages (request handler, SQL execution,
commit/WAL) and correlated with Kahuna, Kommander-WAL, and .NET runtime signals. This document is the
reference for what is emitted and how to collect it.

Diagnostics are **off by default** and are wired only for a standalone node (`!IsClusterMode`) with
`diagnostics.enabled: true`. When disabled the server registers no exporter, endpoint, or collector and
emits nothing — an unconfigured or cluster node pays no diagnostics cost.

## Enabling

Add a `diagnostics:` block to `config.yml` (see the commented reference in
`CamusDB/Config/config.yml`):

```yaml
diagnostics:
  enabled: true
  prometheus_enabled: true        # bind the /metrics scrape endpoint on the HTTP listener
  prometheus_path: /metrics
  otlp_endpoint:                  # optional OTLP collector URL; presence enables the OTLP push exporter
  trace_sample_ratio: 0.01        # head sampling ratio in [0,1]
  include_runtime_metrics: true   # .NET GC / thread-pool metrics
```

Validation is enforced at startup: a sample ratio outside `[0,1]`, a Prometheus path that is not
rooted, or a malformed OTLP URL fails fast.

An orchestration script may point the server at an alternate config without editing the tracked file
via `CAMUS_CONFIG_PATH=/path/to/config.yml`. A run id supplied via `CAMUS_DIAGNOSTICS_RUN_ID` is
attached as an OpenTelemetry **resource attribute** (`camus.run_id`), never as a per-request tag.

> **Security:** the Prometheus endpoint exposes operational metadata. Protect it or bind it to a
> trusted interface; there is no built-in authentication.

## Metrics

Meter name: **`CamusDB.Server`** (version `1.0.0`). Prometheus names shown as exported (the OTel
Prometheus exporter lowercases, replaces `.` with `_`, appends the unit, and adds `_total` to counters).

| Instrument | Prometheus name | Type | Tags |
|---|---|---|---|
| `camus.request.count` | `camus_request_count_total` | counter | operation, transport, outcome |
| `camus.request.duration` | `camus_request_duration_milliseconds` | histogram | operation, transport, outcome |
| `camus.request.in_flight` | `camus_request_in_flight` | up-down | transport |
| `camus.execute.duration` | `camus_execute_duration_milliseconds` | histogram | operation, statement |
| `camus.sql.parse.cache` | `camus_sql_parse_cache_total` | counter | result (hit/miss) |
| `camus.sql.parse.duration` | `camus_sql_parse_duration_milliseconds` | histogram | — (miss path only) |
| `camus.query.rows` | `camus_query_rows_total` | counter | scan, stage (scanned/returned) |
| `camus.query.scan.duration` | `camus_query_scan_duration_milliseconds` | histogram | scan |
| `camus.query_cache.requests` | `camus_query_cache_requests_total` | counter | result |
| `camus.transaction.count` | `camus_transaction_count_total` | counter | operation, outcome |
| `camus.transaction.active` | `camus_transaction_active` | up-down | transaction_mode |
| `camus.transaction.commit.duration` | `camus_transaction_commit_duration_milliseconds` | histogram | outcome |
| `camus.transaction.staged_mutations` | `camus_transaction_staged_mutations` | histogram | — |

### Bounded tag vocabulary

Tag values are a small, reviewed set — never SQL text, ids, messages, or user values — so an instrument
can never explode into unbounded time series. A unit test (`ServerDiagnosticsTests`) enumerates the
allowed values.

- `operation`: `query`, `non_query`, `ddl`, `begin`, `commit`, `rollback`
- `statement`: `select`, `insert`, `update`, `delete`, `other`
- `outcome`: `ok`, `domain_error`, `conflict`, `canceled`, `internal_error`
- `transport`: `grpc_unary`, `grpc_batch`, `http`
- `transaction_mode`: `read_only`, `read_write`
- `scan`: `point`, `primary_range`, `index_range`, `full`
- `stage`: `scanned`, `returned`

### Where each stage is measured

- **request** — per logical operation. On the multiplexed `BatchExecute` path each op is measured
  individually (tagged `grpc_batch`), *not* the duplex stream lifetime; unary/server-streaming calls
  are measured by `MetricsServerInterceptor` (tagged `grpc_unary`).
- **execute** — the command-executor body for non-query/DDL (`CommandExecutor`), exclusive of transport
  and commit-transport time.
- **parse** — the SQL parser AST cache boundary (`SQLParserProcessor.Parse`); duration is recorded only
  on a miss (a hit is ~free).
- **query scan** — the full-table and index scan iterators (`QueryScanner`); rows scanned vs returned,
  and scan+decode duration by scan kind.
- **commit** — the durable 2PC finalize span (WAL-dominated) inside `KvTransactionsManager.CommitAsync`;
  staged KV mutation count is recorded per committed transaction.

## Dependency and runtime metrics

The exporter also subscribes, by meter name, to the embedded dependencies and the runtime:

- **`Kahuna`** (meter version hardcoded `1.0`): `kahuna_kv_write_batches_total`,
  `kahuna_kv_write_entries_total`, `kahuna_durable_tx_outstanding`,
  `kahuna_durable_tx_resident_prepared_intents`, and related KV/durable-tx series.
- **`Kommander`** (meter version = assembly version): `raft_wal_batches_total`,
  `raft_wal_operations_total`, `raft_wal_batch_size`, `raft_executor_operation_duration_ms`,
  `raft_executor_client_queue_depth`, and related Raft/WAL series.
- **.NET runtime** (`OpenTelemetry.Instrumentation.Runtime`): GC collections/allocations/heap,
  thread-pool thread count and queue length. ASP.NET Core request metrics are included too.

> Kahuna's meter version is a fixed `1.0` and does not track the package version. Record the actual
> Kahuna/Kommander **package** versions from the build (`CamusDB.Core.csproj`) in any comparison.

> **WAL fsync latency split** (leader-propose vs leader-commit p50/p99) comes only from Kommander's
> `WalPhaseInstrumentation`, which is process-global and **not concurrent-run safe**. It is not part of
> the always-on exporter; capture it in a separate single-writer measurement window if you need it.

## Traces

Activity source **`CamusDB.Server`**, sampled at `trace_sample_ratio` (head sampling). One root span
per logical operation, with child spans that parent automatically via `Activity.Current`:

```
camus.request
├── camus.sql.parse        (via the executor)
├── camus.execute
│   └── camus.storage.read
└── camus.transaction.commit
```

Spans are created only while a listener is active (`ActivitySource.StartActivity` returns null
otherwise), so unsampled tracing allocates no tags or closures. Failure records a stable error code and
outcome — never exception messages, SQL text, row keys, or values. Traces export via OTLP only (set
`otlp_endpoint`); the Prometheus endpoint is metrics-only.

## One-command snapshot

`scripts/bottleneck-snapshot.sh <output-dir> [--workers N --duration 5m --rows N --mode closed|open]`
starts a Release standalone server with diagnostics enabled against a temp data dir, waits for
readiness, seeds the dataset, runs the mixed workload under a shared run id, scrapes `/metrics`, and
generates a self-contained evidence bundle:

```
<output-dir>/
├── server-command.txt        # exact server invocation
├── config-used.yml           # exact config (with diagnostics enabled)
├── server.log                # server output (preserved on failure)
├── server-metrics.txt        # scraped /metrics snapshot
└── workload/
    ├── manifest.json summary.json summary.md intervals.csv errors.json reconciliation.json
    └── bottleneck-report.md  # client+server attribution, top-3 candidate stages
```

The script stops only the server it started and cleans up the temp data dir on interruption; it
refuses to overwrite an existing output directory.

### Reading `bottleneck-report.md`

The report aligns client offered-vs-completed throughput and latency percentiles with server stage
means (handler, executor, commit, scan), runtime/GC/thread-pool signals, and Kahuna/Kommander
storage/WAL batch density, then lists the top candidate limiting stages each backed by a named
measurement. It is **diagnostic evidence, not a prescription**: it does not declare a root cause from
the largest inclusive duration alone, because awaited durability (commit/WAL) can dominate latency
while using little CPU, and overlapping stage durations are not additive.

## Overhead

`scripts/diagnostics-overhead.sh <output-dir> [--runs 5 --duration 60s]` alternates
diagnostics-disabled and diagnostics-enabled Release workload runs and reports the median completed
ops/s of each plus the enabled-vs-disabled delta. Targets (from the spec): enabled overhead below **5%**
median throughput regression at the default 1% trace sample ratio. Measuring the *disabled* overhead
against a pre-instrumentation build (target **< 2%**) requires building from a commit before this
change; the harness prints that caveat. Overhead is environment-specific — record the raw runs, and do
not treat a single comparison as definitive.
