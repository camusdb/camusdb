/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The single process-wide holder for CamusDB server metrics and traces. It lives in
/// <c>CamusDB.Core</c> so engine code (command executor, transaction manager) can record at the exact
/// stage boundary, while the OpenTelemetry exporter — and its package dependency — stay in the host.
/// The host subscribes to the meter/activity source by name (<see cref="MeterName"/>).
///
/// Two invariants: (1) <b>disabled means silent.</b> When <see cref="Enabled"/> is false every record
/// helper returns before touching an instrument, so a node that has not opted in does no diagnostics
/// work and makes no diagnostics allocation. (2) <b>bounded cardinality.</b> Tag values come only from
/// the <see cref="Tags"/> vocabulary — never SQL text, ids, messages, or user values — so an instrument
/// can never explode into unbounded time series. A test enumerates the allowed values.
/// </summary>
public static class ServerDiagnostics
{
    public const string MeterName = "CamusDB.Server";
    private const string Version = "1.0.0";

    /// <summary>Master gate. Set once at startup for a standalone node with diagnostics enabled.</summary>
    public static bool Enabled { get; set; }

    public static readonly ActivitySource ActivitySource = new(MeterName, Version);

    private static readonly Meter Meter = new(MeterName, Version);

    // ── Request / transport ────────────────────────────────────────────────────
    private static readonly Counter<long> RequestCount =
        Meter.CreateCounter<long>("camus.request.count", unit: "{request}", description: "Server requests handled.");

    private static readonly Histogram<double> RequestDuration =
        Meter.CreateHistogram<double>("camus.request.duration", unit: "ms", description: "End-to-end server handler duration.");

    private static readonly UpDownCounter<long> RequestInFlight =
        Meter.CreateUpDownCounter<long>("camus.request.in_flight", unit: "{request}", description: "Requests currently being handled.");

    // ── KV retry waits ─────────────────────────────────────────────────────────
    // One count per backoff sleep inside a KvTableStore retry loop, tagged by call site. Localizes
    // which retry loop a latency tail is spending its time in — added while chasing the back-to-back
    // statement stall (execute p99 ~2s under client pipelining, p50 normal).
    private static readonly Counter<long> KvRetryWaits =
        Meter.CreateCounter<long>("camus.kv.retry_waits", unit: "{wait}", description: "Backoff sleeps in KV retry loops, by site.");

    public static void AddKvRetryWait(string site)
    {
        if (!Enabled)
            return;
        KvRetryWaits.Add(1, new TagList { { "site", site } });
    }

    // ── SQL execution ───────────────────────────────────────────────────────────
    private static readonly Histogram<double> ExecuteDuration =
        Meter.CreateHistogram<double>("camus.execute.duration", unit: "ms", description: "Command executor duration by statement family.");

    // ── SQL parse + parser cache ─────────────────────────────────────────────────
    private static readonly Histogram<double> ParseDuration =
        Meter.CreateHistogram<double>("camus.sql.parse.duration", unit: "ms", description: "SQL lex+parse duration (miss path); ~0 on a cache hit.");

    private static readonly Counter<long> ParseCache =
        Meter.CreateCounter<long>("camus.sql.parse.cache", unit: "{lookup}", description: "SQL parser AST cache lookups by result.");

    // ── Query scan / rows ────────────────────────────────────────────────────────
    private static readonly Counter<long> QueryRows =
        Meter.CreateCounter<long>("camus.query.rows", unit: "{row}", description: "Rows crossing a query stage, by scan kind and stage.");

    private static readonly Histogram<double> ScanDuration =
        Meter.CreateHistogram<double>("camus.query.scan.duration", unit: "ms", description: "Row scan+decode duration by scan kind.");

    // ── Query result cache ───────────────────────────────────────────────────────
    private static readonly Counter<long> QueryCache =
        Meter.CreateCounter<long>("camus.query_cache.requests", unit: "{lookup}", description: "Query-result-cache lookups by result.");

    // ── Transactions ────────────────────────────────────────────────────────────
    // Both instruments below are recorded from the one place that owns the transaction manager's
    // tracking map, so a transaction is counted once on entry and once on exit and the gauge equals
    // the map's size. Two consequences worth knowing when reading them: a read-only snapshot that
    // opens no server-side transaction (the zero-identity fast path) is not tracked and so does not
    // appear, and a transaction that leaks in the map shows up as a gauge that does not return to
    // zero — which is the truth, not an instrumentation defect.
    private static readonly Counter<long> TransactionCount =
        Meter.CreateCounter<long>("camus.transaction.count", unit: "{transaction}", description: "Transaction lifecycle events.");

    private static readonly UpDownCounter<long> TransactionActive =
        Meter.CreateUpDownCounter<long>("camus.transaction.active", unit: "{transaction}", description: "Active transactions by mode.");

    private static readonly Histogram<double> CommitDuration =
        Meter.CreateHistogram<double>("camus.transaction.commit.duration", unit: "ms", description: "Durable 2PC finalize duration (WAL-dominated) within commit.");

    private static readonly Histogram<long> StagedMutations =
        Meter.CreateHistogram<long>("camus.transaction.staged_mutations", unit: "{mutation}", description: "Staged KV mutations per committed transaction.");

    private static readonly Counter<long> CoordinatorUnknownFinalize =
        Meter.CreateCounter<long>("camus.transaction.coordinator_unknown", unit: "{transaction}", description: "Finalizes the coordinator answered with an unknown transaction, by what the reap did about it.");

    private static readonly Counter<long> MirroredKeysReleased =
        Meter.CreateCounter<long>("camus.transaction.mirrored_keys_released", unit: "{key}", description: "Keys released from a coordinator-unknown transaction's client-side key mirror.");

    // ── Record helpers (all no-ops when disabled) ───────────────────────────────

    public static void RecordRequest(string operation, string transport, string outcome, double milliseconds)
    {
        if (!Enabled)
            return;
        TagList tags = new() { { "operation", operation }, { "transport", transport }, { "outcome", outcome } };
        RequestCount.Add(1, tags);
        RequestDuration.Record(milliseconds, tags);
    }

    public static void AddRequestInFlight(string transport, long delta)
    {
        if (!Enabled)
            return;
        RequestInFlight.Add(delta, new TagList { { "transport", transport } });
    }

    public static void RecordExecute(string operation, string statement, double milliseconds)
    {
        if (!Enabled)
            return;
        ExecuteDuration.Record(milliseconds,
            new TagList { { "operation", operation }, { "statement", statement } });
    }

    /// <summary>
    /// Scoped timer for a command-executor stage: <c>using var _ = ServerDiagnostics.MeasureExecute(...)</c>
    /// records the elapsed duration on dispose, covering every return path of the method without manual
    /// try/finally. A no-op struct when disabled (captures no timestamp, records nothing).
    /// </summary>
    public static ExecuteScope MeasureExecute(string operation, string statement) => new(operation, statement);

    public readonly struct ExecuteScope : IDisposable
    {
        private readonly bool _enabled;
        private readonly long _start;
        private readonly string _operation;
        private readonly string _statement;

        internal ExecuteScope(string operation, string statement)
        {
            _enabled = Enabled;
            _operation = operation;
            _statement = statement;
            _start = _enabled ? Stopwatch.GetTimestamp() : 0;
        }

        public void Dispose()
        {
            if (!_enabled)
                return;
            RecordExecute(_operation, _statement, Stopwatch.GetElapsedTime(_start).TotalMilliseconds);
        }
    }

    public static void RecordTransaction(string operation, string outcome)
    {
        if (!Enabled)
            return;
        TransactionCount.Add(1, new TagList { { "operation", operation }, { "outcome", outcome } });
    }

    public static void AddActiveTransaction(string transactionMode, long delta)
    {
        if (!Enabled)
            return;
        TransactionActive.Add(delta, new TagList { { "transaction_mode", transactionMode } });
    }

    public static void RecordCommitDuration(string outcome, double milliseconds)
    {
        if (!Enabled)
            return;
        CommitDuration.Record(milliseconds, new TagList { { "outcome", outcome } });
    }

    public static void RecordStagedMutations(long count)
    {
        if (!Enabled)
            return;
        StagedMutations.Record(count);
    }

    /// <summary>
    /// Records a finalize that resolved to the coordinator-unknown outcome, tagged by what followed:
    /// <c>released</c> (the transaction was old enough, so its mirrored keys were replayed as
    /// releases), <c>deferred</c> (parked until it reaches the release age), <c>dropped</c> (parking
    /// refused because the deferred-release budget is full), <c>disabled</c> (the release is switched
    /// off, or the transaction never opened a session), or <c>no_keys</c> (nothing was ever written,
    /// so there is nothing to release).
    ///
    /// <para>Counted separately from an ordinary rollback on purpose: the two are indistinguishable in
    /// the transaction counter, yet one released the transaction's holdings and the other left them
    /// behind. Without this, a reap that leaves a wedge is invisible in a run's artifacts.</para>
    /// </summary>
    public static void RecordCoordinatorUnknownFinalize(string disposition, long keysReleased)
    {
        if (!Enabled)
            return;
        CoordinatorUnknownFinalize.Add(1, new TagList { { "disposition", disposition } });
        if (keysReleased > 0)
            MirroredKeysReleased.Add(keysReleased);
    }

    public static void RecordParse(bool cacheHit, double milliseconds)
    {
        if (!Enabled)
            return;
        ParseCache.Add(1, new TagList { { "result", cacheHit ? "hit" : "miss" } });
        if (!cacheHit)
            ParseDuration.Record(milliseconds);
    }

    /// <summary>Records rows crossing a query stage (e.g. scanned, returned) tagged by scan kind.</summary>
    public static void RecordQueryRows(string scan, string stage, long rows)
    {
        if (!Enabled || rows == 0)
            return;
        QueryRows.Add(rows, new TagList { { "scan", scan }, { "stage", stage } });
    }

    public static void RecordScanDuration(string scan, double milliseconds)
    {
        if (!Enabled)
            return;
        ScanDuration.Record(milliseconds, new TagList { { "scan", scan } });
    }

    public static void RecordQueryCache(string result)
    {
        if (!Enabled)
            return;
        QueryCache.Add(1, new TagList { { "result", result } });
    }

    /// <summary>
    /// Starts a trace span only when a listener is active; returns null otherwise so unsampled tracing
    /// allocates no tags or closures. Child spans parent automatically to the ambient
    /// <see cref="Activity.Current"/> (which flows across awaits), so callers deep in the engine need no
    /// explicit context handle. Always used with <c>using</c> so the span ends on every path.
    /// </summary>
    public static Activity? StartSpan(string name) => ActivitySource.StartActivity(name);

    /// <summary>Trace span names for the request → stage tree.</summary>
    public static class Spans
    {
        public const string Request = "camus.request";
        public const string Parse = "camus.sql.parse";
        public const string Execute = "camus.execute";
        public const string StorageRead = "camus.storage.read";
        public const string Commit = "camus.transaction.commit";
    }

    /// <summary>The bounded tag vocabulary. These are the ONLY values any instrument tag may take.</summary>
    public static class Tags
    {
        public static class Operation
        {
            public const string Query = "query";
            public const string NonQuery = "non_query";
            public const string Ddl = "ddl";
            public const string Begin = "begin";
            public const string Commit = "commit";
            public const string Rollback = "rollback";
            /// <summary>Registering a prepared statement — one per statement, not per execution.</summary>
            public const string Prepare = "prepare";
            /// <summary>Releasing a prepared statement.</summary>
            public const string Close = "close";
            public static readonly IReadOnlyList<string> All = new[] { Query, NonQuery, Ddl, Begin, Commit, Rollback, Prepare, Close };
        }

        public static class Statement
        {
            public const string Select = "select";
            public const string Insert = "insert";
            public const string Update = "update";
            public const string Delete = "delete";
            public const string Other = "other";
            public static readonly IReadOnlyList<string> All = new[] { Select, Insert, Update, Delete, Other };
        }

        public static class Outcome
        {
            public const string Ok = "ok";
            public const string DomainError = "domain_error";
            public const string Conflict = "conflict";
            public const string Canceled = "canceled";
            public const string InternalError = "internal_error";
            public static readonly IReadOnlyList<string> All = new[] { Ok, DomainError, Conflict, Canceled, InternalError };
        }

        /// <summary>
        /// What a rollback did about a transaction the coordinator did not know. One value per exit of
        /// the release-by-mirror path, so a run's artifacts distinguish a reap that cleared the
        /// transaction's holdings from one that left them behind.
        /// </summary>
        public static class CoordinatorUnknown
        {
            /// <summary>The mirrored keys were replayed as releases.</summary>
            public const string Released = "released";

            /// <summary>Too young to release; the key mirror is parked until it reaches the age.</summary>
            public const string Deferred = "deferred";

            /// <summary>Parking was refused — the deferred-release budget is full.</summary>
            public const string Dropped = "dropped";

            /// <summary>The release is switched off, or the transaction never opened a session.</summary>
            public const string Disabled = "disabled";

            /// <summary>The transaction wrote nothing, so it planted nothing to release.</summary>
            public const string NoKeys = "no_keys";

            public static readonly IReadOnlyList<string> All = new[] { Released, Deferred, Dropped, Disabled, NoKeys };
        }

        public static class Transport
        {
            public const string GrpcUnary = "grpc_unary";
            public const string GrpcBatch = "grpc_batch";
            public const string Http = "http";
            public static readonly IReadOnlyList<string> All = new[] { GrpcUnary, GrpcBatch, Http };
        }

        public static class TransactionMode
        {
            public const string ReadOnly = "read_only";
            public const string ReadWrite = "read_write";
            public static readonly IReadOnlyList<string> All = new[] { ReadOnly, ReadWrite };
        }

        public static class Scan
        {
            public const string Point = "point";
            public const string PrimaryRange = "primary_range";
            public const string IndexRange = "index_range";
            public const string Full = "full";
            public static readonly IReadOnlyList<string> All = new[] { Point, PrimaryRange, IndexRange, Full };
        }

        public static class Stage
        {
            public const string Scanned = "scanned";
            public const string Returned = "returned";
            public static readonly IReadOnlyList<string> All = new[] { Scanned, Returned };
        }
    }
}
