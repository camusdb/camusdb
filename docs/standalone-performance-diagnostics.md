# Standalone performance diagnostics

CamusDB can emit low-overhead, **opt-in** OpenTelemetry metrics and sampled traces from a standalone
node. With them, a workload run can be attributed to real server stages (request handler, SQL execution,
commit/WAL) and correlated with Kahuna, Kommander-WAL, and .NET runtime signals. This document is the
reference for what is emitted and how to collect it.

Diagnostics are **off by default** and are wired only for a standalone node (`!IsClusterMode`) with
`diagnostics.enabled: true`. When diagnostics are disabled, the server does not register a diagnostics
exporter or endpoint. It does not create a collector or emit any data. This means that an unconfigured
or cluster node pays no diagnostics cost.

## Enabling

Diagnostics are enabled with a `diagnostics:` block in `config.yml`. For a commented reference, see
`CamusDB/Config/config.yml`.

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

An orchestration script may point the server at an alternate config via
`CAMUS_CONFIG_PATH=/path/to/config.yml`. This leaves the tracked file unchanged. A run id supplied via
`CAMUS_DIAGNOSTICS_RUN_ID` is attached as an OpenTelemetry **resource attribute** (`camus.run_id`). It
is not added as a per-request tag.

> **Security:** the Prometheus endpoint exposes operational metadata. Protect it or bind it to a
> trusted interface; there is no built-in authentication.

## Metrics

Meter name: **`CamusDB.Server`** (version `1.0.0`). Prometheus names are shown as exported. The OTel
Prometheus exporter lowercases names and replaces `.` with `_`. It also appends the unit and adds
`_total` to counters.

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

Tag values come from a small, reviewed set. SQL text and ids are never used as tag values, and neither
are messages or user values. This means that an instrument can't explode into unbounded time series. A
unit test (`ServerDiagnosticsTests`) enumerates the
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
  individually (tagged `grpc_batch`). The request metric does not cover the duplex stream lifetime.
  Unary/server-streaming calls are measured by `MetricsServerInterceptor` (tagged `grpc_unary`).
- **execute** — the command-executor body for non-query/DDL (`CommandExecutor`), exclusive of transport
  and commit-transport time.
- **parse** — the SQL parser AST cache boundary (`SQLParserProcessor.Parse`); duration is recorded only
  on a miss (a hit is ~free).
- **query scan** — the full-table and index scan iterators (`QueryScanner`); rows scanned vs returned,
  and scan+decode duration by scan kind.
- **commit** — the durable 2PC finalize span (WAL-dominated) in `KvTransactionsManager.CommitAsync`;
  staged KV mutation count is recorded per committed transaction.

## Dependency and runtime metrics

The exporter also subscribes, by meter name, to the embedded dependencies and the runtime.

| Meter | Meter version | Series |
|---|---|---|
| **`Kahuna`** | hardcoded `1.0` | KV and durable-tx series, including `kahuna_kv_write_batches_total`, `kahuna_kv_write_entries_total`, `kahuna_durable_tx_outstanding`, `kahuna_durable_tx_resident_prepared_intents` |
| **`Kommander`** | assembly version | Raft and WAL series, including `raft_wal_batches_total`, `raft_wal_operations_total`, `raft_wal_batch_size`, `raft_executor_operation_duration_ms`, `raft_executor_client_queue_depth` |

The **.NET runtime** metrics (`OpenTelemetry.Instrumentation.Runtime`) cover GC collections/allocations/heap,
thread-pool thread count and queue length. ASP.NET Core request metrics are included too.

> Kahuna's meter version is a fixed `1.0` and does not track the package version. Record the
> Kahuna/Kommander **package** versions from the build (`CamusDB.Core.csproj`) in any comparison.

> **WAL fsync latency split** (leader-propose vs leader-commit p50/p99) comes only from Kommander's
> `WalPhaseInstrumentation`, which is process-global and **not concurrent-run safe**. It is not part of
> the always-on exporter; capture it in a separate single-writer measurement window if you need it.

## Traces

Activity source **`CamusDB.Server`**, sampled at `trace_sample_ratio` (head sampling). Each logical
operation produces one root span, and its child spans are parented automatically via `Activity.Current`.

```
camus.request
├── camus.sql.parse        (via the executor)
├── camus.execute
│   └── camus.storage.read
└── camus.transaction.commit
```

Spans are created only while a listener is active (`ActivitySource.StartActivity` returns null
otherwise). This means that unsampled tracing allocates no tags or closures. Failure records a stable
error code and outcome. Exception messages and SQL text are not recorded, and neither are row keys or
values. Traces export via OTLP only (set
`otlp_endpoint`); the Prometheus endpoint is metrics-only.

## One-command snapshot

`scripts/bottleneck-snapshot.sh <output-dir> [--workers N --duration 5m --rows N --mode closed|open]`
produces a self-contained evidence bundle in six steps.

1. Starts a Release standalone server with diagnostics enabled against a temp data dir.
2. Waits for readiness.
3. Seeds the dataset.
4. Runs the mixed workload under a shared run id.
5. Scrapes `/metrics`.
6. Generates the evidence bundle in the output directory, with the layout below.

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

The script stops only the server it started and cleans up the temp data dir on interruption. It does
not overwrite an existing output directory.

### Reading `bottleneck-report.md`

The report aligns client offered-vs-completed throughput and latency percentiles with server stage
means (handler, executor, commit, scan), runtime/GC/thread-pool signals, and Kahuna/Kommander
storage/WAL batch density, then lists the top candidate limiting stages each backed by a named
measurement. It is **diagnostic evidence** and does not declare a root cause from the largest
inclusive duration alone. Awaited durability (commit/WAL) can dominate latency while using little CPU,
and overlapping stage durations are not additive.

## Overhead

`scripts/diagnostics-overhead.sh <output-dir> [--runs 5 --duration 60s]` repeats a Release workload
with diagnostics disabled and enabled, alternating between the two. It prints the median completed
ops/s of each plus the enabled-vs-disabled delta. Target: enabled overhead below **5%**
median throughput regression at the default 1% trace sample ratio. Measuring the *disabled* overhead
against a pre-instrumentation build (target **< 2%**) requires building from a commit before this
change; the harness prints that caveat. Overhead is environment-specific. Record the raw result of each
run, and do not treat a single comparison as definitive.
