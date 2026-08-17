
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;
using Kahuna.Shared.KeyValue;
using YamlDotNet.Serialization;

namespace CamusDB.Core.Config.Models;

public class ConfigDefinition
{
    /// <summary>
    /// Root keys the operator actually supplied, as underscored YAML names — populated by
    /// <see cref="ConfigReader"/> from the parsed document and extended by
    /// <see cref="ConfigResolver.ApplyCliOverrides"/> for every flag that took effect.
    /// <para>
    /// This exists because several settings need a default that depends on the rest of the
    /// configuration (see <see cref="ConfigResolver.ApplyEffectiveDefaults"/>), and a property
    /// initializer cannot tell "the operator chose this value" from "nobody said anything". Reading
    /// the value alone would silently override an explicit choice that happens to equal the default.
    /// </para>
    /// </summary>
    [YamlIgnore]
    public HashSet<string> ProvidedKeys { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Which layer supplied each key, as underscored YAML names (nested sections use their dotted
    /// form, e.g. <c>kahuna.wal_sync_writes</c>). Keys absent from the map were never overridden and
    /// are <see cref="ConfigValueSource.Default"/>.
    /// <para>
    /// This is finer-grained than <see cref="ProvidedKeys"/> and answers a different question.
    /// <see cref="ProvidedKeys"/> asks "did the operator say anything about this key?" so a
    /// context-dependent default can avoid overriding an explicit choice; it records only the handful
    /// of keys those defaults consult, and it cannot tell a file value from a flag. This map records
    /// every override and which layer won, purely so the value can be reported back.
    /// </para>
    /// <para>
    /// Writers record themselves as they apply their layer — <see cref="ConfigReader"/> for the
    /// document, <see cref="ConfigResolver.ApplyCliOverrides"/> for flags, <see cref="ConfigResolver.Resolve"/>
    /// and the host for environment variables — so a later layer overwriting a value also overwrites
    /// its provenance, and the recorded source stays the one that actually took effect.
    /// </para>
    /// </summary>
    [YamlIgnore]
    public Dictionary<string, ConfigValueSource> KeySources { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Records that <paramref name="source"/> supplied <paramref name="key"/>, replacing any layer
    /// recorded earlier. Call from the code that applies the value, not from a later pass, so the two
    /// cannot disagree.
    /// </summary>
    public void RecordSource(string key, ConfigValueSource source) => KeySources[key] = source;

    public string DataDir { get; set; } = "";

    public string Mode { get; set; } = "standalone";

    public string NodeName { get; set; } = "";

    public string RaftHost { get; set; } = "localhost";

    public int RaftPort { get; set; } = 7070;

    public int InitialPartitions { get; set; } = 3;

    public List<string> Peers { get; set; } = [];

    /// <summary>
    /// Per-peer HTTP base addresses, parallel to <see cref="Peers"/>.
    /// Entry i is the HTTP URL for the node whose Raft endpoint is Peers[i].
    /// When populated and Peers.Count == HttpPeers.Count, the endpoint map uses
    /// these explicit addresses instead of the uniform-port fallback.
    /// Format: "host:httpPort" (e.g. "192.168.1.10:5095").
    /// </summary>
    public List<string> HttpPeers { get; set; } = [];

    /// <summary>
    /// How long a DDL proposer waits for every live node to ack the previous schema
    /// version before giving up (the two-version gate). Milliseconds; must be &gt; 0.
    /// Maps to <c>EmbeddedKahuna.SchemaAckWaitTimeout</c>.
    /// </summary>
    public int SchemaAckWaitTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// How long since the schema leader last heard from a live member (via Raft activity)
    /// before the gate presumes it down and stops blocking on it. Milliseconds, or <c>-1</c>
    /// for an infinite lease (strict — wait on every configured member, no DDL liveness under a
    /// node failure). Must be &gt; 0 or exactly -1. Default 30 s, so a node death does not freeze
    /// subsequent DDL while a healthy-but-idle follower is never false-evicted.
    /// Maps to <c>EmbeddedKahuna.SchemaAckLiveNodeLease</c>.
    /// </summary>
    public int SchemaAckLiveNodeLeaseMs { get; set; } = 30_000;

    /// <summary>
    /// Minimum interval between background flushes of advisory table statistics to
    /// durable storage, in milliseconds, per table. <c>0</c> flushes after every change;
    /// <c>-1</c> disables auto-flush (persist only on explicit flush / close); a positive
    /// value caps flush frequency. Maps to <c>CamusDBOptions.StatsFlushIntervalMs</c>.
    /// </summary>
    public int StatsFlushIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Row-count threshold below which manual <c>ANALYZE</c> full-scans a table; above it, the first N
    /// rows in storage order are sampled. <c>0</c> = always full scan. Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBOptions.StatsAnalyzeSampleRows</c>.
    /// </summary>
    public int StatsAnalyzeSampleRows { get; set; } = 100_000;

    /// <summary>
    /// Number of equi-depth histogram buckets <c>ANALYZE</c> builds per column. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.StatsHistogramBuckets</c>.
    /// </summary>
    public int StatsHistogramBuckets { get; set; } = 100;

    // ── Automatic (background) ANALYZE ────────────────────────────────────────────────────────────
    // Keeps optimizer statistics fresh without a user running ANALYZE, while staying low-priority:
    // lock-free snapshot reads, bounded-memory sampling, a throttled scan, and load backoff. On by
    // default. See docs/automatic-analyze.md.

    /// <summary>Master switch for automatic background <c>ANALYZE</c>. Maps to <c>CamusDBOptions.AutoAnalyzeEnabled</c>.</summary>
    public bool AutoAnalyzeEnabled { get; set; } = true;

    /// <summary>
    /// Interval between auto-analyze staleness sweeps, in milliseconds. Only the registry leader
    /// sweeps (once per cluster). <c>&lt;= 0</c> also disables the loop.
    /// Maps to <c>CamusDBOptions.AutoAnalyzeCheckIntervalMs</c>.
    /// </summary>
    public int AutoAnalyzeCheckIntervalMs { get; set; } = 60_000;

    /// <summary>
    /// Proportional staleness trigger: a table is stale once mutations since the last ANALYZE reach
    /// <c>fraction · row_count + min_stale_rows</c>. Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBOptions.AutoAnalyzeFractionStaleRows</c>.
    /// </summary>
    public double AutoAnalyzeFractionStaleRows { get; set; } = 0.20;

    /// <summary>
    /// Absolute mutation floor before a table is ever considered stale. Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBOptions.AutoAnalyzeMinStaleRows</c>.
    /// </summary>
    public long AutoAnalyzeMinStaleRows { get; set; } = 500;

    /// <summary>
    /// Maximum background analyses running at once on a node. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.AutoAnalyzeMaxConcurrent</c>.
    /// </summary>
    public int AutoAnalyzeMaxConcurrent { get; set; } = 1;

    /// <summary>
    /// Scan-rate cap for a background analyze, in rows/second (the CPU/IO throttle). <c>&lt;= 0</c>
    /// disables throttling. Maps to <c>CamusDBOptions.AutoAnalyzeMaxRowsPerSecond</c>.
    /// </summary>
    public int AutoAnalyzeMaxRowsPerSecond { get; set; } = 50_000;

    /// <summary>
    /// Reservoir sample size per column for background histograms — the memory bound. Must be
    /// <c>&gt;= 1</c>. Maps to <c>CamusDBOptions.AutoAnalyzeHistogramSampleRows</c>.
    /// </summary>
    public int AutoAnalyzeHistogramSampleRows { get; set; } = 10_000;

    /// <summary>
    /// HyperLogLog precision (index bits) for background NDV sketches; register count is
    /// <c>2^precision</c>. Must be in <c>4..16</c>. Maps to <c>CamusDBOptions.AutoAnalyzeHllPrecision</c>.
    /// </summary>
    public int AutoAnalyzeHllPrecision { get; set; } = 11;

    /// <summary>
    /// Foreground-load surge protector: when in-flight foreground work exceeds this, the sweep skips
    /// starting (and cancels a running) background analyze. <c>&lt;= 0</c> disables load backoff.
    /// Maps to <c>CamusDBOptions.AutoAnalyzeLoadPauseThreshold</c>.
    /// </summary>
    public int AutoAnalyzeLoadPauseThreshold { get; set; } = 16;

    /// <summary>
    /// Rows the background scan processes between successive mid-scan re-checks of ownership and load.
    /// Must be <c>&gt;= 1</c>. Maps to <c>CamusDBOptions.AutoAnalyzeOwnershipCheckRows</c>.
    /// </summary>
    public int AutoAnalyzeOwnershipCheckRows { get; set; } = 1000;

    /// <summary>
    /// Master switch for the row-level TTL sweep. Maps to <c>CamusDBOptions.TtlEnabled</c>.
    /// </summary>
    public bool TtlEnabled { get; set; } = true;

    /// <summary>
    /// Fallback for the per-table <c>ttl_job_cron</c> storage parameter. Must be a supported
    /// <c>@macro</c> form. Maps to <c>CamusDBOptions.TtlDefaultJobCron</c>.
    /// </summary>
    public string TtlDefaultJobCron { get; set; } = "@daily";

    /// <summary>
    /// Fallback for <c>ttl_select_batch_size</c>. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.TtlDefaultSelectBatchSize</c>.
    /// </summary>
    public int TtlDefaultSelectBatchSize { get; set; } = 500;

    /// <summary>
    /// Fallback for <c>ttl_delete_batch_size</c>. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.TtlDefaultDeleteBatchSize</c>.
    /// </summary>
    public int TtlDefaultDeleteBatchSize { get; set; } = 100;

    /// <summary>
    /// Fallback for <c>ttl_select_rate_limit</c>, rows/second; <c>0</c> is unlimited. Must be
    /// <c>&gt;= 0</c>. Maps to <c>CamusDBOptions.TtlDefaultSelectRateLimit</c>.
    /// </summary>
    public int TtlDefaultSelectRateLimit { get; set; } = 0;

    /// <summary>
    /// Fallback for <c>ttl_delete_rate_limit</c>, rows/second; <c>0</c> is unlimited. Must be
    /// <c>&gt;= 0</c>. Maps to <c>CamusDBOptions.TtlDefaultDeleteRateLimit</c>.
    /// </summary>
    public int TtlDefaultDeleteRateLimit { get; set; } = 100;

    /// <summary>
    /// Spans a TTL run divides a table's keyspace into. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.TtlSpansPerTable</c>.
    /// </summary>
    public int TtlSpansPerTable { get; set; } = 64;

    /// <summary>
    /// Spans one node processes at once. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.TtlMaxConcurrentSpansPerNode</c>.
    /// </summary>
    public int TtlMaxConcurrentSpansPerNode { get; set; } = 1;

    /// <summary>
    /// Foreground-load threshold above which the sweep pauses. <c>&lt;= 0</c> disables load backoff.
    /// Maps to <c>CamusDBOptions.TtlLoadPauseThreshold</c>.
    /// </summary>
    public int TtlLoadPauseThreshold { get; set; } = 16;

    /// <summary>
    /// Span claim lease in milliseconds. Must be <c>&gt;= 1</c>.
    /// Maps to <c>CamusDBOptions.TtlSpanLeaseMs</c>.
    /// </summary>
    public int TtlSpanLeaseMs { get; set; } = 30_000;

    /// <summary>
    /// Span lease renewal interval in milliseconds. Must be <c>&gt;= 1</c> and strictly less than
    /// <c>ttl_span_lease_ms</c>. Maps to <c>CamusDBOptions.TtlSpanLeaseRenewIntervalMs</c>.
    /// </summary>
    public int TtlSpanLeaseRenewIntervalMs { get; set; } = 10_000;

    /// <summary>
    /// Sliding TTL for the SQL parser AST cache, in seconds.
    /// Each cache hit extends the deadline by this interval.
    /// <c>0</c> disables the cache entirely (every parse re-lexes from scratch).
    /// Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBOptions.SqlParserCacheTtlSeconds</c>.
    /// </summary>
    public int SqlParserCacheTtlSeconds { get; set; } = 300;

    /// <summary>
    /// Maximum number of distinct SQL texts the parser AST cache may hold.
    /// When the cap is reached, new statements are silently skipped until the background
    /// sweep reclaims expired entries. <c>0</c> = unbounded (no cap).
    /// Must be <c>&gt;= 0</c>.
    /// Maps to <c>CamusDBOptions.SqlParserCacheMaxEntries</c>.
    /// </summary>
    public int SqlParserCacheMaxEntries { get; set; } = 2048;

    /// <summary>
    /// How often, in seconds, the background sweep task removes expired SQL parser cache
    /// entries. Must be <c>&gt; 0</c>.
    /// Maps to <c>CamusDBOptions.SqlParserCacheSweepSeconds</c>.
    /// </summary>
    public int SqlParserCacheSweepSeconds { get; set; } = 60;

    /// <summary>
    /// Retention window, in milliseconds, for orphaned (deferred-dropped) databases/tables before the
    /// garbage collector may physically reclaim them. <c>&lt;= 0</c> keeps orphans until an explicit
    /// FORCE drop / manual purge. Maps to <c>CamusDBOptions.OrphanRetentionMs</c> (yml
    /// <c>orphan_retention_ms</c>). Default 7 days.
    /// </summary>
    public long OrphanRetentionMs { get; set; } = 7L * 24 * 60 * 60 * 1000;

    /// <summary>
    /// Observes the embedded Kommander/Kahuna meters in-process so <c>SHOW ENGINE STATS</c> can
    /// report them. Maps to <c>CamusDBOptions.EngineMetricsEnabled</c> (yml
    /// <c>engine_metrics_enabled</c>). Default on.
    /// </summary>
    public bool EngineMetricsEnabled { get; set; } = true;

    /// <summary>
    /// Interval, in milliseconds, of the background orphan-reclamation sweep. <c>&lt;= 0</c> disables
    /// the loop. Maps to <c>CamusDBOptions.OrphanReclaimIntervalMs</c> (yml
    /// <c>orphan_reclaim_interval_ms</c>). Default 5 minutes.
    /// </summary>
    public int OrphanReclaimIntervalMs { get; set; } = 5 * 60 * 1000;

    /// <summary>
    /// How long a database may sit unused before this node releases its descriptor, in milliseconds.
    /// <c>&lt;= 0</c> disables idle eviction. Maps to <c>CamusDBOptions.DatabaseIdleEvictionMs</c>
    /// (yml <c>database_idle_eviction_ms</c>). Default 15 minutes.
    /// </summary>
    public int DatabaseIdleEvictionMs { get; set; } = 15 * 60 * 1000;

    /// <summary>
    /// Enables cost-based access-path selection in the query planner.
    /// When <c>true</c> (default), the planner costs all viable index steps for ANALYZEd tables
    /// and picks the cheapest. When <c>false</c>, the rule-based (score-based) path is used
    /// unchanged. Maps to <c>CamusDBOptions.CostBasedAccessPathEnabled</c>.
    /// </summary>
    public bool CostBasedAccessPathEnabled { get; set; } = true;

    /// <summary>
    /// Enables cost-based join-order enumeration (System-R–style DP).
    /// When <c>true</c> (default), the planner prices all left-deep orderings and picks the
    /// cheapest. When <c>false</c>, the rule-based heuristic is used unchanged.
    /// Maps to <c>CamusDBOptions.CostBasedJoinOrderEnabled</c>.
    /// </summary>
    public bool CostBasedJoinOrderEnabled { get; set; } = true;

    /// <summary>
    /// Enables the query plan cache.
    /// When <c>false</c> (default), the cache is built but never consulted —
    /// consistent with the opt-in convention for all cost-based optimizer features.
    /// Maps to <c>CamusDBOptions.PlanCacheEnabled</c>.
    /// </summary>
    public bool PlanCacheEnabled { get; set; } = false;

    /// <summary>
    /// Maximum LRU entries in the plan cache (0 = effectively disabled even when
    /// <c>plan_cache_enabled</c> is true). Maps to <c>CamusDBOptions.PlanCacheMaxEntries</c>.
    /// </summary>
    public int PlanCacheMaxEntries { get; set; } = 512;

    /// <summary>
    /// Per-match timeout in milliseconds for the regex operators <c>~</c> / <c>~*</c> / <c>!~</c> /
    /// <c>!~*</c>; a match that exceeds it is rejected rather than allowed to run unbounded (ReDoS
    /// guard). Must be &gt; 0. Maps to <c>CamusDBOptions.RegexMatchTimeoutMs</c>.
    /// </summary>
    public int RegexMatchTimeoutMs { get; set; } = 250;

    /// <summary>
    /// Maximum number of compiled regex patterns cached (keyed by pattern + case-sensitivity).
    /// 0 disables caching — patterns are still compiled and evaluated, just never retained.
    /// Maps to <c>CamusDBOptions.RegexCacheMaxEntries</c>.
    /// </summary>
    public int RegexCacheMaxEntries { get; set; } = 1024;

    /// <summary>Numeric Raft node id for cluster mode. Maps to <c>EmbeddedKahunaOptions.NodeId</c>.</summary>
    public int RaftNodeId { get; set; } = 1;

    /// <summary>HTTP API listen port. Default 5095.</summary>
    public int HttpPort { get; set; } = 5095;

    /// <summary>HTTPS API listen port when a certificate is configured. Default 7141.</summary>
    public int HttpsPort { get; set; } = 7141;

    /// <summary>Path to a PFX certificate for the HTTPS API port; empty disables HTTPS.</summary>
    public string HttpsCertificate { get; set; } = "";

    /// <summary>Path to a PFX certificate for the Raft gRPC port in cluster mode.</summary>
    public string RaftCertificate { get; set; } = "";

    /// <summary>
    /// When authentication is enabled, refuse credential-bearing requests arriving over a plaintext
    /// (non-TLS) connection — a bearer token or password on the wire is trivially stolen. Loopback
    /// peers are exempt either way, so single-host development needs no certificate. Default
    /// <c>true</c> (secure by default); set <c>false</c> only where TLS terminates in front of the
    /// node (a sidecar, ingress, or service mesh), because that hop is invisible to the server and it
    /// would otherwise reject every forwarded request.
    ///
    /// <para>Unlike the token key and bootstrap credentials — which are secrets and are read only from
    /// the environment so they never land in a config file — this is a deployment-topology policy flag
    /// with no secret value, so it is configurable from YAML and the CLI.</para>
    ///
    /// <para>Maps to <c>CamusDBOptions.RequireTlsWhenAuthEnabled</c>. Inert while authentication is
    /// disabled.</para>
    /// </summary>
    public bool RequireTlsWhenAuthEnabled { get; set; } = true;

    /// <summary>
    /// When <c>true</c>, binds a dedicated client-facing gRPC HTTP/2 port (<see cref="GrpcPort"/>)
    /// and registers the <c>CamusSql</c> and <c>CamusRows</c> services plus gRPC reflection.
    /// Defaults to <c>true</c> — the operator must consciously disable the gRPC endpoint.
    /// </summary>
    public bool GrpcEnabled { get; set; } = true;

    /// <summary>
    /// Port for the client-facing gRPC listener (HTTP/2 only). Ignored when
    /// <see cref="GrpcEnabled"/> is <c>false</c>. Default 5096.
    /// When <see cref="RaftCertificate"/> is set the same cert is reused for TLS here; otherwise
    /// the listener binds plaintext HTTP/2 (<c>h2c</c>), which is fine for local/dev use.
    /// </summary>
    public int GrpcPort { get; set; } = 5096;

    /// <summary>
    /// Maximum number of concurrently in-flight operations the server will execute per
    /// <c>CamusSql.BatchExecute</c> duplex stream before applying backpressure (the read loop stops
    /// pulling new requests until a slot frees). Bounds a single client's fan-out. Must be &gt; 0.
    /// Default 64. Maps to <c>CamusDBOptions.GrpcBatchMaxInFlight</c>.
    /// </summary>
    public int GrpcBatchMaxInFlight { get; set; } = 64;

    /// <summary>
    /// Cluster-wide default isolation when a transaction omits an explicit level:
    /// <c>serializable</c> or <c>read_committed</c>.
    /// </summary>
    public string DefaultIsolationLevel { get; set; } = "serializable";

    /// <summary>
    /// Cluster-wide default locking strategy when a transaction omits an explicit locking selection:
    /// <c>pessimistic</c> (acquire-then-write, block on conflict) or <c>optimistic</c> (defer conflict
    /// detection to read-set validation at commit). Default is <c>pessimistic</c>.
    /// </summary>
    public string DefaultTransactionLocking { get; set; } = "pessimistic";

    /// <summary>
    /// Cluster-wide default admission priority when a transaction omits one: <c>background</c>,
    /// <c>low</c>, <c>normal</c>, <c>high</c>, or <c>critical</c>. Default is <c>normal</c>.
    ///
    /// <para>Consulted only by the Kahuna node's admission gate, which is off while
    /// <c>kahuna.max_concurrent_sessions</c> is zero (its default) — so changing this has no effect
    /// until a concurrency ceiling is configured.</para>
    /// </summary>
    public string DefaultTransactionPriority { get; set; } = "normal";

    /// <summary>
    /// Milliseconds a transaction queues at the node's admission gate before being refused with a
    /// retryable error. <c>0</c> (the default) leaves the node's own default budget in force.
    ///
    /// <para>Distinct from <c>max_serializable_transaction_lifetime_ms</c>, which bounds how long an
    /// admitted transaction may live. Inert while <c>kahuna.max_concurrent_sessions</c> is zero, and
    /// clamped by the node to <c>kahuna.max_admission_wait_ms</c>.</para>
    /// </summary>
    public int TransactionAdmissionWaitMs { get; set; }

    /// <summary>
    /// Initial range-lock TTL in milliseconds. The coordinator renews a live session's range locks on
    /// its collection-interval tick, so this must exceed that interval (60 s by default) or a lock
    /// lapses before the first renewal. &lt;= 0 disables expiry. Default 150 000.
    /// </summary>
    public int RangeLockExpiresMs { get; set; } = 150_000;

    /// <summary>Absolute Serializable+RW transaction lifetime cap in milliseconds. &lt;= 0 disables.</summary>
    public int MaxSerializableTransactionLifetimeMs { get; set; } = 3_600_000;

    /// <summary>
    /// Total wall-clock budget in milliseconds for retrying one commit/rollback/close against the
    /// coordinator's non-terminal <c>MustRetry</c> before surfacing CADB0509. &lt;= 0 attempts the
    /// finalize once. Default 15 000 (15 s).
    /// </summary>
    public int TransactionFinalizeRetryBudgetMs { get; set; } = 15_000;

    /// <summary>
    /// Total wall-clock budget in milliseconds for retrying a monotonic id counter (database id, table
    /// id, registry generation) against <c>MustRetry</c> before failing the statement with CADB0535.
    /// &lt;= 0 attempts the call once. Default 10 000 (10 s).
    /// </summary>
    public int SequenceRetryBudgetMs { get; set; } = 10_000;

    /// <summary>
    /// Idle timeout in milliseconds for explicit (client-driven) transactions before the background
    /// reaper rolls them back. &lt;= 0 disables the reaper. Default 300 000 (5 min).
    /// </summary>
    public int TransactionIdleTimeoutMs { get; set; } = 300_000;

    /// <summary>Reaper sweep interval in milliseconds. Must be &gt; 0. Default 30 000 (30 s).</summary>
    public int TransactionReaperIntervalMs { get; set; } = 30_000;

    /// <summary>
    /// Idle timeout in milliseconds for a REST prepared statement before the reaper drops it.
    /// &lt;= 0 disables reaping, leaving handles alive until closed or the process exits. gRPC handles
    /// are unaffected — they die with their stream. Default 600 000 (10 min).
    /// </summary>
    public int PreparedStatementIdleTimeoutMs { get; set; } = 600_000;

    /// <summary>Prepared-statement reaper sweep interval in milliseconds. Default 60 000 (1 min).</summary>
    public int PreparedStatementSweepIntervalMs { get; set; } = 60_000;

    /// <summary>Live prepared statements one BatchExecute stream may hold. 0 = unbounded. Default 512.</summary>
    public int GrpcMaxPreparedStatementsPerStream { get; set; } = 512;

    /// <summary>Live REST prepared statements one principal may hold. 0 = unbounded. Default 512.</summary>
    public int RestMaxPreparedStatementsPerPrincipal { get; set; } = 512;

    /// <summary>Live REST prepared statements a node holds in total. 0 = unbounded. Default 8192.</summary>
    public int RestMaxPreparedStatements { get; set; } = 8192;

    /// <summary>
    /// Largest single statement (database + SQL + parameter names, UTF-16 bytes) that may be
    /// prepared on either transport. 0 = unlimited. This is what makes the count caps bound memory.
    /// Default 65536 (64 KiB).
    /// </summary>
    public int MaxPreparedStatementBytes { get; set; } = 65_536;

    /// <summary>Retained REST prepared-statement bytes per node. 0 = unlimited. Default 64 MiB.</summary>
    public long RestMaxPreparedStatementBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>Retained REST prepared-statement bytes per principal. 0 = unlimited. Default 8 MiB.</summary>
    public long RestMaxPreparedStatementBytesPerPrincipal { get; set; } = 8L * 1024 * 1024;

    /// <summary>Retained prepared-statement bytes per BatchExecute stream. 0 = unlimited. Default 8 MiB.</summary>
    public long GrpcMaxPreparedStatementBytesPerStream { get; set; } = 8L * 1024 * 1024;

    /// <summary>Per-bucket shared-point-lock count before whole-bucket escalation. Default 50.</summary>
    public int LockEscalationThreshold { get; set; } = 50;

    /// <summary>Wall-clock cap per lock-acquire retry loop for Serializable conflicts. Default 500 ms.</summary>
    public int LockWaitDeadlineMs { get; set; } = 500;

    /// <summary>
    /// Opt tables into Kahuna key-range routing. Overridden by <c>CAMUS_KEY_RANGE_SHARDING</c> when set.
    /// </summary>
    public bool KeyRangeSharding { get; set; }

    /// <summary>
    /// Planner fragmentation of eligible read-only scans into per-span Gather fragments.
    /// Default false. Maps to <c>CamusDBOptions.DistributedQueryExecutionEnabled</c>.
    /// </summary>
    public bool DistributedQueryExecution { get; set; }

    /// <summary>Max UTF-16 length for any identifier (db, table, column, index). &lt;= 0 disables. Default 64.</summary>
    public int MaxIdentifierLength { get; set; } = 64;

    /// <summary>Max user-declared columns per table. &lt;= 0 disables. Default 512.</summary>
    public int MaxColumnsPerTable { get; set; } = 512;

    /// <summary>Max user-visible secondary indexes per table (PK exempt). &lt;= 0 disables. Default 64.</summary>
    public int MaxIndexesPerTable { get; set; } = 64;

    /// <summary>Max tables per database. &lt;= 0 disables. Default 10 000.</summary>
    public int MaxTablesPerDatabase { get; set; } = 10_000;

    /// <summary>
    /// Max key + stored/payload (<c>INCLUDE</c>) columns in one index. Guards a covering index from
    /// duplicating unbounded row data into every entry. &lt;= 0 disables. Default 32.
    /// Maps to <c>CamusDBOptions.MaxIndexColumns</c>.
    /// </summary>
    public int MaxIndexColumns { get; set; } = 32;

    /// <summary>
    /// Max encoded byte size of one index entry's key + INCLUDE tuple, checked at write time. &lt;= 0
    /// disables. Default 4096 (4 KiB). Maps to <c>CamusDBOptions.MaxIndexIncludeTupleBytes</c>.
    /// </summary>
    public int MaxIndexIncludeTupleBytes { get; set; } = 4096;

    /// <summary>
    /// Max rows one user transaction may mutate before it is rejected. &lt;= 0 disables (unlimited);
    /// DDL/backfill always run unlimited. Default 20 000. Maps to
    /// <c>CamusDBOptions.MaxMutationsPerTransaction</c>.
    /// </summary>
    public int MaxMutationsPerTransaction { get; set; } = 20_000;

    /// <summary>
    /// Max nesting depth for view-over-view expansion; a backstop behind the DDL-time cycle check.
    /// Default 32. Maps to <c>CamusDBOptions.MaxViewExpansionDepth</c>.
    /// </summary>
    public int MaxViewExpansionDepth { get; set; } = 32;

    /// <summary>
    /// Rows written per transaction while a materialized view is refreshed. Must stay well below
    /// <see cref="MaxMutationsPerTransaction"/>. Default 10 000. Maps to
    /// <c>CamusDBOptions.MaterializedViewRefreshChunkRows</c>.
    /// </summary>
    public int MaterializedViewRefreshChunkRows { get; set; } = 10_000;

    /// <summary>
    /// Whether this node may execute materialized-view refresh work. Default true. Maps to
    /// <c>CamusDBOptions.MaterializedViewRefreshEnabled</c>.
    /// </summary>
    public bool MaterializedViewRefreshEnabled { get; set; } = true;

    /// <summary>
    /// How many times the background sweep may restart an interrupted materialized-view refresh
    /// before giving up; 0 disables takeover and only reclaims the abandoned staging storage.
    /// Default 3. Maps to <c>CamusDBOptions.MaterializedViewRefreshTakeoverAttempts</c>.
    /// </summary>
    public int MaterializedViewRefreshTakeoverAttempts { get; set; } = 3;

    /// <summary>
    /// Lease window, in milliseconds, for a branch's snapshot-floor hold on its parent's MVCC history;
    /// the leader-owned renewer must renew well inside it for as long as the branch exists. Must be
    /// &gt; 0. Default 300 000 (5 min). Maps to <c>CamusDBOptions.BranchSnapshotHoldLeaseMs</c>.
    /// </summary>
    public int BranchSnapshotHoldLeaseMs { get; set; } = 300_000;

    /// <summary>
    /// Enables spill-to-disk for blocking query operators (sort, GROUP BY, DISTINCT,
    /// hash join, derived-table materialization, DELETE/UPDATE row buffers).
    /// When <c>false</c> (default), every operator keeps its in-memory path; when <c>true</c>,
    /// each operator spills sorted runs or partitioned rows to temp files once its buffer exceeds
    /// <see cref="SpillThresholdRows"/>. Maps to <c>CamusDBOptions.SpillEnabled</c>.
    /// </summary>
    public bool SpillEnabled { get; set; } = false;

    /// <summary>
    /// Per-operator in-memory row cap before the operator begins spilling to disk.
    /// Must be &gt; 0. Ignored when <see cref="SpillEnabled"/> is <c>false</c>.
    /// Maps to <c>CamusDBOptions.SpillThresholdRows</c>.
    /// </summary>
    public int SpillThresholdRows { get; set; } = 500_000;

    /// <summary>
    /// Maximum number of simultaneously-open spill-run readers during a k-way merge pass.
    /// When the number of spilled runs exceeds this value, a multi-pass merge is performed.
    /// Must be &gt; 0. Ignored when <see cref="SpillEnabled"/> is <c>false</c>.
    /// Maps to <c>CamusDBOptions.SpillMergeFanIn</c>.
    /// </summary>
    public int SpillMergeFanIn { get; set; } = 16;

    /// <summary>
    /// Enables per-step query-execution trace log lines. Off by default — each query emits one line
    /// per plan step executed. Maps to <c>CamusDBOptions.QueryTracingEnabled</c>.
    /// </summary>
    public bool QueryTracingEnabled { get; set; } = false;

    /// <summary>
    /// Enables per-lock-acquisition debug log lines. Off by default — a busy workload emits one line
    /// per lock acquired. Maps to <c>CamusDBOptions.LockTracingEnabled</c>.
    /// </summary>
    public bool LockTracingEnabled { get; set; } = false;

    /// <summary>
    /// Drop-intent fence lease, in milliseconds. Must be &gt; 0 and comfortably above
    /// <see cref="FenceLeaseRenewIntervalMs"/>. Default 30 000. Maps to <c>CamusDBOptions.FenceLeaseMs</c>.
    /// </summary>
    public int FenceLeaseMs { get; set; } = 30_000;

    /// <summary>
    /// How often a fence holder renews its drop-intent lease, in milliseconds. Must be &gt; 0 and
    /// strictly under <see cref="FenceLeaseMs"/> — a renew interval at or above the lease lets a live
    /// holder's lease lapse under it. Default 10 000. Maps to
    /// <c>CamusDBOptions.FenceLeaseRenewIntervalMs</c>.
    /// </summary>
    public int FenceLeaseRenewIntervalMs { get; set; } = 10_000;

    /// <summary>
    /// Max keys the non-transactional DROP DATABASE keyspace purge scans/deletes per batch. Must be
    /// &gt;= 1. Default 512. Maps to <c>CamusDBOptions.KeyspacePurgeBatchSize</c>.
    /// </summary>
    public int KeyspacePurgeBatchSize { get; set; } = 512;

    /// <summary>
    /// Row ids buffered from a non-covering index scan before one batched row fetch. Must be &gt;= 1
    /// (1 degrades to per-entry fetching). Default 64. Maps to
    /// <c>CamusDBOptions.IndexScanFetchBatchSize</c>.
    /// </summary>
    public int IndexScanFetchBatchSize { get; set; } = 64;

    /// <summary>
    /// Concurrent span workers per full primary-row table scan. Must be &gt;= 1; 1 disables
    /// parallel scanning. Default 1. Maps to <c>CamusDBOptions.MaxQueryParallelism</c>.
    /// </summary>
    public int MaxQueryParallelism { get; set; } = 1;

    /// <summary>
    /// Rows the hash-join build phase may materialise before degrading to nested-loop, applied only
    /// while spill is disabled. Must be &gt;= 1. Default 1 000 000. Maps to
    /// <c>CamusDBOptions.HashJoinMaxBuildRows</c>.
    /// </summary>
    public int HashJoinMaxBuildRows { get; set; } = 1_000_000;

    /// <summary>
    /// Weight per byte shipped over the network in the planner's network cost model; 0 disables
    /// network costing. Must be &gt;= 0. Default 0.01. Maps to <c>CamusDBOptions.NetWeight</c>.
    /// </summary>
    public double NetWeight { get; set; } = 0.01;

    /// <summary>
    /// Slot-backed (lazy) query decode; <c>false</c> is the eager kill-switch / A/B baseline.
    /// Default true. Maps to <c>CamusDBOptions.SlotBackedDecode</c>.
    /// </summary>
    public bool SlotBackedDecode { get; set; } = true;

    /// <summary>
    /// Borrowed (zero-copy) decode policy: <c>adaptive</c>, <c>force_eager</c>, or
    /// <c>force_borrowed</c>. Default <c>adaptive</c>. Maps to <c>CamusDBOptions.BorrowedDecode</c>.
    /// </summary>
    public string BorrowedDecode { get; set; } = "adaptive";

    /// <summary>
    /// Upper bound on a single spill-run record's declared payload length, in bytes, checked before
    /// the reader allocates for it. Must be &gt; 0. Default 268 435 456 (256 MiB). Maps to
    /// <c>CamusDBOptions.SpillMaxFrameBytes</c>.
    /// </summary>
    public int SpillMaxFrameBytes { get; set; } = 256 * 1024 * 1024;

    /// <summary>
    /// Default read-set validation when a transaction omits one: <c>none</c> or
    /// <c>track_and_validate</c>. Default <c>none</c>. Maps to
    /// <c>CamusDBOptions.DefaultReadValidation</c>.
    /// </summary>
    public string DefaultReadValidation { get; set; } = "none";

    /// <summary>
    /// Default commit-decision durability when a transaction omits one: <c>best_effort</c> or
    /// <c>durable</c>. Default <c>best_effort</c>. Maps to
    /// <c>CamusDBOptions.DefaultDecisionDurability</c>.
    /// </summary>
    public string DefaultDecisionDurability { get; set; } = "best_effort";

    /// <summary>
    /// PBKDF2-HMAC-SHA256 iteration count for hashing user passwords. The value in force is stored
    /// with each credential, so raising it later never invalidates existing hashes. Must be &gt;= 1.
    /// Default 600 000 (OWASP's floor). Maps to <c>CamusDBOptions.PasswordHashIterations</c>.
    /// </summary>
    public int PasswordHashIterations { get; set; } = 600_000;

    /// <summary>
    /// Concurrent password-verification (PBKDF2) operations allowed across all logins. Must be
    /// &gt;= 1. Default 8. Maps to <c>CamusDBOptions.LoginKdfMaxConcurrency</c>.
    /// </summary>
    public int LoginKdfMaxConcurrency { get; set; } = 8;

    /// <summary>
    /// Login attempts per normalized account per rolling minute before rejection. Must be &gt;= 1.
    /// Default 20. Maps to <c>CamusDBOptions.LoginMaxAttemptsPerMinute</c>.
    /// </summary>
    public int LoginMaxAttemptsPerMinute { get; set; } = 20;

    /// <summary>
    /// Upper bound on the login rate-limiter's tracked (account, source) keys. Must be &gt;= 1.
    /// Default 100 000. Maps to <c>CamusDBOptions.LoginRateLimitMaxEntries</c>.
    /// </summary>
    public int LoginRateLimitMaxEntries { get; set; } = 100_000;

    /// <summary>
    /// Maximum staleness, in milliseconds, of a per-node authorization cache hit; 0 forces an
    /// authoritative lookup on every request. Must be &gt;= 0. Default 1 000. Maps to
    /// <c>CamusDBOptions.AuthenticationCacheTtl</c>.
    /// </summary>
    public long AuthenticationCacheTtl { get; set; } = 1_000;

    /// <summary>
    /// Upper bound on the per-node authenticated-principal cache size. Must be &gt;= 1.
    /// Default 10 000. Maps to <c>CamusDBOptions.AuthenticationCacheMaxEntries</c>.
    /// </summary>
    public int AuthenticationCacheMaxEntries { get; set; } = 10_000;

    /// <summary>
    /// Absolute access-token lifetime, in milliseconds. Must be &gt; 0. Default 900 000 (15 min).
    /// Maps to <c>CamusDBOptions.AccessTokenTtl</c>.
    /// </summary>
    public long AccessTokenTtl { get; set; } = 15 * 60 * 1000;

    /// <summary>Allow-listed Kahuna engine tunables for cluster and standalone nodes.</summary>
    public KahunaOptionsConfig Kahuna { get; set; } = new();

    /// <summary>Opt-in observability settings; effective only for a standalone node when enabled.</summary>
    public DiagnosticsConfig Diagnostics { get; set; } = new();

    // ──────────────────────────────────────────────────────────────────────────
    // Query result cache
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>Enables the per-node in-memory query result cache. Default: true. Maps to <c>CamusDBOptions.QueryResultCacheEnabled</c>.</summary>
    public bool QueryResultCacheEnabled { get; set; } = true;

    /// <summary>Default TTL in milliseconds for cache entries without a per-hint override. Default: 5 000 ms. Maps to <c>CamusDBOptions.QueryResultCacheDefaultTtlMs</c>.</summary>
    public int QueryResultCacheDefaultTtlMs { get; set; } = 5_000;

    /// <summary>Maximum entries held by the cache (LRU eviction when exceeded). Default: 1 024. Maps to <c>CamusDBOptions.QueryResultCacheMaxEntries</c>.</summary>
    public int QueryResultCacheMaxEntries { get; set; } = 1_024;

    /// <summary>Maximum total bytes across all cached entries. Default: 64 MiB. Maps to <c>CamusDBOptions.QueryResultCacheMaxBytes</c>.</summary>
    public long QueryResultCacheMaxBytes { get; set; } = 64 * 1024 * 1024;

    /// <summary>Maximum bytes for a single cache entry; oversized results are bypassed. Default: 1 MiB. Maps to <c>CamusDBOptions.QueryResultCacheMaxEntryBytes</c>.</summary>
    public long QueryResultCacheMaxEntryBytes { get; set; } = 1 * 1024 * 1024;

    /// <summary>Maximum rows in a single cache entry; oversized results are bypassed. Default: 10 000. Maps to <c>CamusDBOptions.QueryResultCacheMaxEntryRows</c>.</summary>
    public int QueryResultCacheMaxEntryRows { get; set; } = 10_000;

    /// <summary>Maximum combined dep facts (range + point + schema) per entry before bypass. Default: 4 096. Maps to <c>CamusDBOptions.QueryResultCacheMaxDeps</c>.</summary>
    public int QueryResultCacheMaxDeps { get; set; } = 4_096;

    /// <summary>Maximum point-key deps per entry before promotion or bypass. Default: 2 048. Maps to <c>CamusDBOptions.QueryResultCacheMaxPointDeps</c>.</summary>
    public int QueryResultCacheMaxPointDeps { get; set; } = 2_048;

    /// <summary>Maximum range deps per entry before promotion or bypass. Default: 256. Maps to <c>CamusDBOptions.QueryResultCacheMaxRanges</c>.</summary>
    public int QueryResultCacheMaxRanges { get; set; } = 256;

    /// <summary>
    /// Single-flight waiter timeout in milliseconds before independent execution. Default: 250 ms.
    /// Property spelling is deliberately <c>Singleflight</c> (one word) so the underscored YAML key
    /// binds as <c>query_result_cache_singleflight_wait_ms</c> — the documented key — rather than
    /// <c>..._single_flight_...</c>. Maps to <c>CamusDBOptions.QueryResultCacheSingleFlightWaitMs</c>.
    /// </summary>
    public int QueryResultCacheSingleflightWaitMs { get; set; } = 250;

    /// <summary>Max keys probed during strict validation before treating the entry as invalid. Default: 10 000. Maps to <c>CamusDBOptions.QueryResultCacheStrictValidationMaxKeys</c>.</summary>
    public int QueryResultCacheStrictValidationMaxKeys { get; set; } = 10_000;

    /// <summary>Background sweep interval in milliseconds for TTL expiry. Default: 10 000 ms. Maps to <c>CamusDBOptions.QueryResultCacheSweepIntervalMs</c>.</summary>
    public int QueryResultCacheSweepIntervalMs { get; set; } = 10_000;

    public bool IsClusterMode => Mode == "cluster" || Peers.Count > 0;

    /// <summary>Parses <see cref="DefaultIsolationLevel"/> to the engine enum.</summary>
    public CamusIsolationLevel ParseDefaultIsolationLevel()
    {
        return DefaultIsolationLevel switch
        {
            "serializable" => CamusIsolationLevel.Serializable,
            "read_committed" => CamusIsolationLevel.ReadCommitted,
            _ => throw Invalid(
                "'default_isolation_level' must be 'serializable' or 'read_committed', got '" +
                DefaultIsolationLevel + "'"),
        };
    }

    /// <summary>Parses <see cref="DefaultTransactionLocking"/> to the Kahuna enum.</summary>
    public KeyValueTransactionLocking ParseDefaultTransactionLocking()
    {
        return DefaultTransactionLocking switch
        {
            "pessimistic" => KeyValueTransactionLocking.Pessimistic,
            "optimistic" => KeyValueTransactionLocking.Optimistic,
            _ => throw Invalid(
                "'default_transaction_locking' must be 'pessimistic' or 'optimistic', got '" +
                DefaultTransactionLocking + "'"),
        };
    }

    /// <summary>Parses <see cref="DefaultTransactionPriority"/> to the Kahuna enum.</summary>
    public TransactionPriority ParseDefaultTransactionPriority()
    {
        return DefaultTransactionPriority switch
        {
            "background" => TransactionPriority.Background,
            "low" => TransactionPriority.Low,
            "normal" => TransactionPriority.Normal,
            "high" => TransactionPriority.High,
            "critical" => TransactionPriority.Critical,
            _ => throw Invalid(
                "'default_transaction_priority' must be one of 'background', 'low', 'normal', 'high', " +
                "'critical', got '" + DefaultTransactionPriority + "'"),
        };
    }

    /// <summary>Parses <see cref="BorrowedDecode"/> to the engine enum.</summary>
    public BorrowedDecodePolicy ParseBorrowedDecode()
    {
        return BorrowedDecode switch
        {
            "adaptive" => BorrowedDecodePolicy.Adaptive,
            "force_eager" => BorrowedDecodePolicy.ForceEager,
            "force_borrowed" => BorrowedDecodePolicy.ForceBorrowed,
            _ => throw Invalid(
                "'borrowed_decode' must be 'adaptive', 'force_eager' or 'force_borrowed', got '" +
                BorrowedDecode + "'"),
        };
    }

    /// <summary>Parses <see cref="DefaultReadValidation"/> to the Kahuna enum.</summary>
    public ReadValidation ParseDefaultReadValidation()
    {
        return DefaultReadValidation switch
        {
            "none" => ReadValidation.None,
            "track_and_validate" => ReadValidation.TrackAndValidate,
            _ => throw Invalid(
                "'default_read_validation' must be 'none' or 'track_and_validate', got '" +
                DefaultReadValidation + "'"),
        };
    }

    /// <summary>Parses <see cref="DefaultDecisionDurability"/> to the Kahuna enum.</summary>
    public DecisionDurability ParseDefaultDecisionDurability()
    {
        return DefaultDecisionDurability switch
        {
            "best_effort" => DecisionDurability.BestEffort,
            "durable" => DecisionDurability.Durable,
            _ => throw Invalid(
                "'default_decision_durability' must be 'best_effort' or 'durable', got '" +
                DefaultDecisionDurability + "'"),
        };
    }

    /// <summary>
    /// Validates the configuration, throwing <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.InvalidConfig"/> on the first problem found.
    /// Called by <c>ConfigReader.Read</c> so a malformed config fails fast at startup
    /// rather than producing confusing behaviour later (e.g. a zero ack timeout that
    /// makes the two-version gate give up instantly).
    /// </summary>
    public void Validate()
    {
        if (Mode is not ("standalone" or "cluster"))
            throw Invalid($"'mode' must be 'standalone' or 'cluster', got '{Mode}'");

        if (RaftPort is <= 0 or > 65535)
            throw Invalid($"'raft_port' must be in 1..65535, got {RaftPort}");

        if (InitialPartitions < 1)
            throw Invalid($"'initial_partitions' must be >= 1, got {InitialPartitions}");

        if (SchemaAckWaitTimeoutMs <= 0)
            throw Invalid(
                $"'schema_ack_wait_timeout_ms' must be > 0, got {SchemaAckWaitTimeoutMs}");

        if (SchemaAckLiveNodeLeaseMs is not (-1 or > 0))
            throw Invalid(
                "'schema_ack_live_node_lease_ms' must be > 0 or -1 (infinite), got " +
                SchemaAckLiveNodeLeaseMs);

        if (StatsFlushIntervalMs < -1)
            throw Invalid(
                "'stats_flush_interval_ms' must be >= 0 (interval), 0 (immediate), or -1 " +
                $"(disabled), got {StatsFlushIntervalMs}");

        if (StatsAnalyzeSampleRows < 0)
            throw Invalid($"'stats_analyze_sample_rows' must be >= 0 (0 = always full scan), got {StatsAnalyzeSampleRows}");

        if (StatsHistogramBuckets < 1)
            throw Invalid($"'stats_histogram_buckets' must be >= 1, got {StatsHistogramBuckets}");

        if (AutoAnalyzeFractionStaleRows < 0)
            throw Invalid($"'auto_analyze_fraction_stale_rows' must be >= 0, got {AutoAnalyzeFractionStaleRows}");

        if (AutoAnalyzeMinStaleRows < 0)
            throw Invalid($"'auto_analyze_min_stale_rows' must be >= 0, got {AutoAnalyzeMinStaleRows}");

        if (AutoAnalyzeMaxConcurrent < 1)
            throw Invalid($"'auto_analyze_max_concurrent' must be >= 1, got {AutoAnalyzeMaxConcurrent}");

        if (AutoAnalyzeHistogramSampleRows < 1)
            throw Invalid($"'auto_analyze_histogram_sample_rows' must be >= 1, got {AutoAnalyzeHistogramSampleRows}");

        if (AutoAnalyzeHllPrecision is < 4 or > 16)
            throw Invalid($"'auto_analyze_hll_precision' must be in 4..16, got {AutoAnalyzeHllPrecision}");

        if (AutoAnalyzeOwnershipCheckRows < 1)
            throw Invalid($"'auto_analyze_ownership_check_rows' must be >= 1, got {AutoAnalyzeOwnershipCheckRows}");

        if (TtlDefaultSelectBatchSize < 1)
            throw Invalid($"'ttl_default_select_batch_size' must be >= 1, got {TtlDefaultSelectBatchSize}");

        if (TtlDefaultDeleteBatchSize < 1)
            throw Invalid($"'ttl_default_delete_batch_size' must be >= 1, got {TtlDefaultDeleteBatchSize}");

        if (TtlDefaultSelectRateLimit < 0)
            throw Invalid(
                $"'ttl_default_select_rate_limit' must be >= 0 (0 = unlimited), got {TtlDefaultSelectRateLimit}");

        if (TtlDefaultDeleteRateLimit < 0)
            throw Invalid(
                $"'ttl_default_delete_rate_limit' must be >= 0 (0 = unlimited), got {TtlDefaultDeleteRateLimit}");

        if (TtlSpansPerTable < 1)
            throw Invalid($"'ttl_spans_per_table' must be >= 1, got {TtlSpansPerTable}");

        if (TtlMaxConcurrentSpansPerNode < 1)
            throw Invalid($"'ttl_max_concurrent_spans_per_node' must be >= 1, got {TtlMaxConcurrentSpansPerNode}");

        if (TtlSpanLeaseMs < 1)
            throw Invalid($"'ttl_span_lease_ms' must be >= 1, got {TtlSpanLeaseMs}");

        if (TtlSpanLeaseRenewIntervalMs < 1)
            throw Invalid($"'ttl_span_lease_renew_interval_ms' must be >= 1, got {TtlSpanLeaseRenewIntervalMs}");

        // A renew interval at or above the lease lets a live owner's lease lapse under it: the span
        // becomes reclaimable while its owner is still deleting, and two workers process it at once.
        if (TtlSpanLeaseRenewIntervalMs >= TtlSpanLeaseMs)
            throw Invalid(
                $"'ttl_span_lease_renew_interval_ms' ({TtlSpanLeaseRenewIntervalMs}) must be < " +
                $"'ttl_span_lease_ms' ({TtlSpanLeaseMs})");

        if (!Catalogs.Models.TtlCron.IsSupported(TtlDefaultJobCron))
            throw Invalid(
                $"'ttl_default_job_cron' must be one of {Catalogs.Models.TtlCron.SupportedForMessage}, " +
                $"got '{TtlDefaultJobCron}'");

        if (BranchSnapshotHoldLeaseMs <= 0)
            throw Invalid($"'branch_snapshot_hold_lease_ms' must be > 0, got {BranchSnapshotHoldLeaseMs}");

        if (SqlParserCacheTtlSeconds < 0)
            throw Invalid(
                $"'sql_parser_cache_ttl_seconds' must be >= 0 (0 = disabled), got {SqlParserCacheTtlSeconds}");

        if (SqlParserCacheMaxEntries < 0)
            throw Invalid(
                $"'sql_parser_cache_max_entries' must be >= 0 (0 = unbounded), got {SqlParserCacheMaxEntries}");

        if (SqlParserCacheSweepSeconds <= 0)
            throw Invalid(
                $"'sql_parser_cache_sweep_seconds' must be > 0, got {SqlParserCacheSweepSeconds}");

        if (RegexMatchTimeoutMs <= 0)
            throw Invalid(
                $"'regex_match_timeout_ms' must be > 0, got {RegexMatchTimeoutMs}");

        if (RegexCacheMaxEntries < 0)
            throw Invalid(
                $"'regex_cache_max_entries' must be >= 0 (0 = no caching), got {RegexCacheMaxEntries}");

        if (HttpPort is <= 0 or > 65535)
            throw Invalid($"'http_port' must be in 1..65535, got {HttpPort}");

        if (HttpsPort is <= 0 or > 65535)
            throw Invalid($"'https_port' must be in 1..65535, got {HttpsPort}");

        if (GrpcPort is <= 0 or > 65535)
            throw Invalid($"'grpc_port' must be in 1..65535, got {GrpcPort}");

        if (RaftNodeId <= 0)
            throw Invalid($"'raft_node_id' must be > 0, got {RaftNodeId}");

        if (DefaultIsolationLevel is not ("serializable" or "read_committed"))
            throw Invalid(
                "'default_isolation_level' must be 'serializable' or 'read_committed', got '" +
                DefaultIsolationLevel + "'");

        if (DefaultTransactionLocking is not ("pessimistic" or "optimistic"))
            throw Invalid(
                "'default_transaction_locking' must be 'pessimistic' or 'optimistic', got '" +
                DefaultTransactionLocking + "'");

        if (DefaultTransactionPriority is not ("background" or "low" or "normal" or "high" or "critical"))
            throw Invalid(
                "'default_transaction_priority' must be one of 'background', 'low', 'normal', 'high', " +
                "'critical', got '" + DefaultTransactionPriority + "'");

        if (TransactionAdmissionWaitMs < 0)
            throw Invalid(
                "'transaction_admission_wait_ms' must be >= 0 (0 leaves the node default in force), got " +
                TransactionAdmissionWaitMs);

        if (LockEscalationThreshold <= 0)
            throw Invalid($"'lock_escalation_threshold' must be > 0, got {LockEscalationThreshold}");

        if (LockWaitDeadlineMs <= 0)
            throw Invalid($"'lock_wait_deadline_ms' must be > 0, got {LockWaitDeadlineMs}");

        if (TransactionReaperIntervalMs <= 0)
            throw Invalid($"'transaction_reaper_interval_ms' must be > 0, got {TransactionReaperIntervalMs}");

        // Prepared-statement knobs. The sweep interval must be positive: the reaper would otherwise
        // have to invent a value, and a typo like -1 would silently become a scan loop rather than the
        // startup error it is. Caps and budgets use 0 for "unbounded", so a negative value is a typo
        // that would read as unbounded and silently disable the limit the operator meant to set.
        if (PreparedStatementSweepIntervalMs <= 0)
            throw Invalid($"'prepared_statement_sweep_interval_ms' must be > 0, got {PreparedStatementSweepIntervalMs}");

        if (GrpcMaxPreparedStatementsPerStream < 0)
            throw Invalid($"'grpc_max_prepared_statements_per_stream' must be >= 0 (0 = unbounded), got {GrpcMaxPreparedStatementsPerStream}");

        if (RestMaxPreparedStatementsPerPrincipal < 0)
            throw Invalid($"'rest_max_prepared_statements_per_principal' must be >= 0 (0 = unbounded), got {RestMaxPreparedStatementsPerPrincipal}");

        if (RestMaxPreparedStatements < 0)
            throw Invalid($"'rest_max_prepared_statements' must be >= 0 (0 = unbounded), got {RestMaxPreparedStatements}");

        if (MaxPreparedStatementBytes < 0)
            throw Invalid($"'max_prepared_statement_bytes' must be >= 0 (0 = unlimited), got {MaxPreparedStatementBytes}");

        if (RestMaxPreparedStatementBytes < 0)
            throw Invalid($"'rest_max_prepared_statement_bytes' must be >= 0 (0 = unlimited), got {RestMaxPreparedStatementBytes}");

        if (RestMaxPreparedStatementBytesPerPrincipal < 0)
            throw Invalid($"'rest_max_prepared_statement_bytes_per_principal' must be >= 0 (0 = unlimited), got {RestMaxPreparedStatementBytesPerPrincipal}");

        if (GrpcMaxPreparedStatementBytesPerStream < 0)
            throw Invalid($"'grpc_max_prepared_statement_bytes_per_stream' must be >= 0 (0 = unlimited), got {GrpcMaxPreparedStatementBytesPerStream}");

        if (SpillThresholdRows <= 0)
            throw Invalid($"'spill_threshold_rows' must be > 0, got {SpillThresholdRows}");

        if (SpillMergeFanIn <= 0)
            throw Invalid($"'spill_merge_fan_in' must be > 0, got {SpillMergeFanIn}");

        if (SpillMaxFrameBytes <= 0)
            throw Invalid($"'spill_max_frame_bytes' must be > 0, got {SpillMaxFrameBytes}");

        if (FenceLeaseMs <= 0)
            throw Invalid($"'fence_lease_ms' must be > 0, got {FenceLeaseMs}");

        if (FenceLeaseRenewIntervalMs <= 0)
            throw Invalid($"'fence_lease_renew_interval_ms' must be > 0, got {FenceLeaseRenewIntervalMs}");

        // A renew interval at or above the lease lets a live fence holder's lease lapse under it: the
        // drop-intent fence frees while its owner is still purging, and a concurrent RELINK/GC can act
        // on an id whose keyspace is mid-destruction.
        if (FenceLeaseRenewIntervalMs >= FenceLeaseMs)
            throw Invalid(
                $"'fence_lease_renew_interval_ms' ({FenceLeaseRenewIntervalMs}) must be < " +
                $"'fence_lease_ms' ({FenceLeaseMs})");

        if (KeyspacePurgeBatchSize < 1)
            throw Invalid($"'keyspace_purge_batch_size' must be >= 1, got {KeyspacePurgeBatchSize}");

        if (IndexScanFetchBatchSize < 1)
            throw Invalid($"'index_scan_fetch_batch_size' must be >= 1, got {IndexScanFetchBatchSize}");

        if (MaxQueryParallelism < 1)
            throw Invalid($"'max_query_parallelism' must be >= 1, got {MaxQueryParallelism}");

        if (HashJoinMaxBuildRows < 1)
            throw Invalid($"'hash_join_max_build_rows' must be >= 1, got {HashJoinMaxBuildRows}");

        if (NetWeight < 0)
            throw Invalid($"'net_weight' must be >= 0 (0 disables network costing), got {NetWeight}");

        if (PasswordHashIterations < 1)
            throw Invalid($"'password_hash_iterations' must be >= 1, got {PasswordHashIterations}");

        if (LoginKdfMaxConcurrency < 1)
            throw Invalid($"'login_kdf_max_concurrency' must be >= 1, got {LoginKdfMaxConcurrency}");

        if (LoginMaxAttemptsPerMinute < 1)
            throw Invalid($"'login_max_attempts_per_minute' must be >= 1, got {LoginMaxAttemptsPerMinute}");

        if (LoginRateLimitMaxEntries < 1)
            throw Invalid($"'login_rate_limit_max_entries' must be >= 1, got {LoginRateLimitMaxEntries}");

        if (AuthenticationCacheTtl < 0)
            throw Invalid(
                "'authentication_cache_ttl' must be >= 0 ms (0 = authoritative lookup on every request), got " +
                AuthenticationCacheTtl);

        if (AuthenticationCacheMaxEntries < 1)
            throw Invalid($"'authentication_cache_max_entries' must be >= 1, got {AuthenticationCacheMaxEntries}");

        if (AccessTokenTtl <= 0)
            throw Invalid($"'access_token_ttl' must be > 0 ms, got {AccessTokenTtl}");

        // Each refreshed row costs one mutation per index plus the row itself, so a chunk size at
        // or near the mutation cap fails on any indexed materialized view — and it fails mid-
        // refresh, at a distance from the setting that caused it. Require a 2x margin.
        if (MaxMutationsPerTransaction > 0 && (long)MaterializedViewRefreshChunkRows * 2 > MaxMutationsPerTransaction)
            throw Invalid(
                $"'materialized_view_refresh_chunk_rows' ({MaterializedViewRefreshChunkRows}) must be at most half of " +
                $"'max_mutations_per_transaction' ({MaxMutationsPerTransaction}); each refreshed row costs one mutation " +
                "per index, so a chunk near the cap fails on any indexed materialized view");

        // The storage layer sheds a participant write that ages past ~1 s in its pre-dispatch
        // queue and answers MustRetry; a finalize budget that cannot absorb at least two shed
        // rounds reports a recoverable in-doubt commit as an error. Non-positive disables retrying
        // and is exempt (an explicit single-attempt choice).
        const int StorageTransientSheddingThresholdMs = 1_000;
        if (TransactionFinalizeRetryBudgetMs > 0 && TransactionFinalizeRetryBudgetMs < StorageTransientSheddingThresholdMs * 2)
            throw Invalid(
                $"'transaction_finalize_retry_budget_ms' ({TransactionFinalizeRetryBudgetMs}) must be >= " +
                $"{StorageTransientSheddingThresholdMs * 2} ms (2x the storage layer's ~{StorageTransientSheddingThresholdMs} ms " +
                "transient-shedding threshold), or a single shed round consumes the whole budget; " +
                "set it <= 0 to disable retrying entirely");

        // The three enum-valued strings validate by parsing, so file and statement reject the same
        // spellings for the same reason.
        ParseBorrowedDecode();
        ParseDefaultReadValidation();
        ParseDefaultDecisionDurability();

        Kahuna.Validate();
        Diagnostics.Validate();

        // Compose the Kahuna session-timeout cap with the engine's serializable lifetime. The engine
        // starts every session with Timeout = MaxSerializableTransactionLifetimeMs and Kahuna clamps it
        // to the node's MaxTransactionTimeout. An explicit 'max_transaction_timeout_ms' below the
        // lifetime makes that lifetime unreachable (sessions are silently reaped early), so reject it
        // rather than let the two knobs disagree. When the cap is left unset the option builder derives
        // it from the lifetime, so there is nothing to check here.
        if (Kahuna.MaxTransactionTimeoutMs is int kahunaMaxTimeout
            && MaxSerializableTransactionLifetimeMs > 0
            && kahunaMaxTimeout < MaxSerializableTransactionLifetimeMs)
            throw Invalid(
                $"'kahuna.max_transaction_timeout_ms' ({kahunaMaxTimeout}) must be >= " +
                $"'max_serializable_transaction_lifetime_ms' ({MaxSerializableTransactionLifetimeMs}); " +
                "a smaller cap silently truncates the serializable transaction lifetime");

        // Cross-check the range-lock TTL against the coordinator's renewal cadence. The coordinator
        // renews a live session's range locks on its collection-interval tick (Kahuna's default is
        // 60 s), so the TTL must comfortably outlast one tick or a lock lapses before its first renewal
        // and a concurrent writer can slip into the scanned range. Require a 2x margin over the
        // effective interval. A non-positive TTL disables expiry (server-side renewal owns the lease),
        // so it is exempt.
        if (RangeLockExpiresMs > 0)
        {
            const int RangeLockRenewalMarginFactor = 2;
            int effectiveCollectionIntervalMs = Kahuna.CollectionIntervalMs ?? 60_000;
            long minSafeRangeLockTtl = (long)effectiveCollectionIntervalMs * RangeLockRenewalMarginFactor;

            if (RangeLockExpiresMs < minSafeRangeLockTtl)
                throw Invalid(
                    $"'range_lock_expires_ms' ({RangeLockExpiresMs}) must be >= {RangeLockRenewalMarginFactor}x the " +
                    $"effective Kahuna collection interval ({effectiveCollectionIntervalMs} ms => {minSafeRangeLockTtl} ms); " +
                    "the coordinator renews range locks on that tick, so a smaller TTL lapses before the first renewal " +
                    "(raise 'range_lock_expires_ms', lower 'kahuna.collection_interval_ms', or set the TTL <= 0 to disable expiry)");
        }

        // Forwarding endpoints: http_peers, when supplied, must be parallel to peers so
        // the raft-endpoint → HTTP base-URI map in Program.cs is unambiguous. An entry
        // count mismatch silently disables the explicit map and falls back to the
        // uniform-port heuristic, which is a misconfiguration we should surface here.
        if (HttpPeers.Count > 0 && HttpPeers.Count != Peers.Count)
            throw Invalid(
                $"'http_peers' has {HttpPeers.Count} entr{(HttpPeers.Count == 1 ? "y" : "ies")} " +
                $"but 'peers' has {Peers.Count}; they must be parallel (one http address per peer)");

        foreach (string peer in Peers)
            ValidateHostPort(peer, "peers");

        foreach (string httpPeer in HttpPeers)
            ValidateHostPort(httpPeer, "http_peers");
    }

    private static void ValidateHostPort(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw Invalid($"'{field}' contains an empty entry");

        int colon = value.LastIndexOf(':');
        if (colon <= 0 || colon == value.Length - 1)
            throw Invalid($"'{field}' entry '{value}' must be in 'host:port' format");

        string portPart = value[(colon + 1)..];
        if (!int.TryParse(portPart, out int port) || port is <= 0 or > 65535)
            throw Invalid($"'{field}' entry '{value}' has an invalid port '{portPart}'");
    }

    private static CamusDBException Invalid(string message)
        => new(CamusDBErrorCodes.InvalidConfig, message);
}
