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
/// The stages a query's wall time is split into by <see cref="QueryStageProfile"/>. Some nest: the
/// <c>prepare</c> stage contains <c>parse</c>, <c>authorize</c> and <c>db_open</c>, and <c>fetch</c>
/// contains <c>index_lookup</c>, <c>row_get</c> and <c>decode</c>. The report is a set of totals per
/// stage, and a reader subtracts the inner stages from the outer one to get the remainder.
/// </summary>
public enum QueryStage
{
    /// <summary>The op's admission on the stream: principal resolution and the read-buffer slot.</summary>
    Admit,
    /// <summary>Waiting for the previous op of the same transaction handle.</summary>
    ChainWait,
    /// <summary>Waiting for the stream's execution slot.</summary>
    SlotWait,
    /// <summary>The whole op, the same span as <c>camus.request.duration</c>.</summary>
    Request,
    /// <summary>Resolving the SQL text and parameters (prepared-statement lookup).</summary>
    Resolve,
    /// <summary>The autocommit read's <c>BeginReadOnlyAsync</c>.</summary>
    Begin,
    /// <summary>Everything the executor does before it hands back a cursor.</summary>
    Prepare,
    Parse,
    Authorize,
    DbOpen,
    /// <summary>Moving the cursor: every <c>MoveNextAsync</c> of the result.</summary>
    Fetch,
    /// <summary>The unique-index probe that resolves a key to its row id.</summary>
    IndexLookup,
    /// <summary>The row read by row id.</summary>
    RowGet,
    /// <summary>Row decode and the plan filter.</summary>
    Decode,
    /// <summary>Writing the schema and the rows onto the response stream.</summary>
    Write,
    /// <summary>The autocommit read's commit or release.</summary>
    Commit,
    /// <summary>Building the terminator (routing advice, causal token) and writing it.</summary>
    Complete,
}

/// <summary>The kind of Kahuna call a <see cref="QueryStageProfile.MeasureKahuna"/> interval timed.</summary>
public enum KahunaCallKind
{
    /// <summary>A key-value call through <c>KahunaRetryPolicy</c> (a point get on the query path).</summary>
    KeyValue,
    /// <summary><c>LocateAndStartTransaction</c>, including a deferred START run by a statement.</summary>
    Start,
    /// <summary><c>LocateAndCommitTransaction</c>.</summary>
    Commit,
    /// <summary><c>LocateAndRollbackTransaction</c>.</summary>
    Rollback,
}

/// <summary>
/// Opt-in wall-clock profile of the query path, per stage, for attributing point-read latency. A
/// sampled CPU trace cannot split a request's latency, because most of it is
/// awaiting another component. This splits it by stage, and names how much of each stage was spent
/// inside Kahuna calls.
///
/// <para>The op's handler opens a <see cref="QueryStageClock"/> with <see cref="Begin"/>, and it flows
/// to the storage layer through an <see cref="AsyncLocal{T}"/>, so no signature on the way down
/// changes. <see cref="Measure"/> times a stage. <see cref="MeasureKahuna"/> times a Kahuna call and
/// charges it to the innermost open stage. The stages of one op run one after another, so the clock
/// needs no synchronization.</para>
///
/// <para>Off unless <see cref="Enabled"/> is set (the host sets it from
/// <c>CAMUS_QUERY_STAGE_PROFILE=1</c>, and only when diagnostics are on). While it is off, every
/// helper returns before it reads a timestamp or allocates.</para>
///
/// <para>Output, per <c>path</c> (<c>autocommit</c> or <c>txn</c>): <c>camus.query.profile.queries</c>
/// counts profiled queries, and <c>camus.query.profile.time</c> (ms) and
/// <c>camus.query.profile.calls</c> are keyed by <c>stage</c> and by <c>part</c>. The <c>total</c>
/// part is the stage's wall time. The <c>kahuna_*</c> parts are the time inside Kahuna calls of that
/// kind that the stage made directly (not inside a nested stage). A stage's mean per query is its
/// time divided by the query count.</para>
/// </summary>
public static class QueryStageProfile
{
    public static bool Enabled { get; set; }

    public static class Paths
    {
        public const string Autocommit = "autocommit";
        public const string Txn = "txn";
        public static readonly IReadOnlyList<string> All = new[] { Autocommit, Txn };
    }

    public static class Parts
    {
        public const string Total = "total";
        public static readonly IReadOnlyList<string> All = new[] { Total, "kahuna_kv", "kahuna_start", "kahuna_commit", "kahuna_rollback" };
    }

    /// <summary>Part names of the Kahuna call kinds, indexed by <see cref="KahunaCallKind"/>.</summary>
    internal static readonly string[] KahunaPartNames = { "kahuna_kv", "kahuna_start", "kahuna_commit", "kahuna_rollback" };

    internal static readonly string[] StageNames =
    {
        "admit", "chain_wait", "slot_wait", "request", "resolve", "begin", "prepare", "parse", "authorize",
        "db_open", "fetch", "index_lookup", "row_get", "decode", "write", "commit", "complete",
    };

    public static IReadOnlyList<string> AllStageNames => StageNames;

    private static readonly AsyncLocal<QueryStageClock?> current = new();

    private static readonly Meter Meter = new(ServerDiagnostics.MeterName + ".QueryProfile", "1.0.0");

    private static readonly Counter<long> Queries =
        Meter.CreateCounter<long>("camus.query.profile.queries", unit: "{query}", description: "Queries profiled by stage.");

    private static readonly Counter<double> Time =
        Meter.CreateCounter<double>("camus.query.profile.time", unit: "ms", description: "Query wall time by stage and part.");

    private static readonly Counter<long> Calls =
        Meter.CreateCounter<long>("camus.query.profile.calls", unit: "{call}", description: "Timed intervals by query stage and part.");

    /// <summary>The name of the meter, which the host subscribes to only when the profile is on.</summary>
    public static string MeterName => Meter.Name;

    /// <summary>The clock of the op in progress, or null when the profile is off or no op opened one.</summary>
    public static QueryStageClock? Current => Enabled ? current.Value : null;

    /// <summary>
    /// Opens a clock for the calling async flow and its callees. Returns null when the profile is off.
    /// Call it from the async method that owns the op, so the value never leaks to its caller.
    /// </summary>
    public static QueryStageClock? Begin()
    {
        if (!Enabled)
            return null;

        QueryStageClock clock = new();
        current.Value = clock;
        return clock;
    }

    public static StageScope Measure(QueryStage stage) => new(Live(), stage);

    public static KahunaScope MeasureKahuna(KahunaCallKind kind) => new(Live(), kind);

    /// <summary>
    /// The current clock while its op is still running. Work that an op started and that outlives it
    /// (a background task that inherited the async flow) finds the clock published and records nothing.
    /// </summary>
    private static QueryStageClock? Live() => Current is { Published: false } clock ? clock : null;

    public readonly struct StageScope : IDisposable
    {
        private readonly QueryStageClock? clock;
        private readonly QueryStage stage;
        private readonly QueryStage? outer;
        private readonly long start;

        internal StageScope(QueryStageClock? clock, QueryStage stage)
        {
            this.clock = clock;
            this.stage = stage;
            if (clock is null)
            {
                outer = null;
                start = 0;
                return;
            }

            outer = clock.Open;
            clock.Open = stage;
            start = Stopwatch.GetTimestamp();
        }

        public void Dispose()
        {
            if (clock is null)
                return;

            clock.Add(stage, Stopwatch.GetTimestamp() - start);
            clock.Open = outer;
        }
    }

    public readonly struct KahunaScope : IDisposable
    {
        private readonly QueryStageClock? clock;
        private readonly KahunaCallKind kind;
        private readonly long start;

        internal KahunaScope(QueryStageClock? clock, KahunaCallKind kind)
        {
            this.clock = clock;
            this.kind = kind;
            start = clock is null ? 0 : Stopwatch.GetTimestamp();
        }

        public void Dispose()
        {
            clock?.AddKahuna(kind, Stopwatch.GetTimestamp() - start);
        }
    }

    internal static void Publish(QueryStageClock clock, string path)
    {
        Queries.Add(1, new TagList { { "path", path } });

        double msPerTick = 1000.0 / Stopwatch.Frequency;
        for (int i = 0; i < StageNames.Length; i++)
        {
            if (clock.Calls[i] > 0)
            {
                TagList tags = new() { { "path", path }, { "stage", StageNames[i] }, { "part", Parts.Total } };
                Time.Add(clock.Ticks[i] * msPerTick, tags);
                Calls.Add(clock.Calls[i], tags);
            }

            for (int k = 0; k < KahunaPartNames.Length; k++)
            {
                int slot = i * KahunaPartNames.Length + k;
                if (clock.KahunaCalls[slot] == 0)
                    continue;

                TagList tags = new() { { "path", path }, { "stage", StageNames[i] }, { "part", KahunaPartNames[k] } };
                Time.Add(clock.KahunaTicks[slot] * msPerTick, tags);
                Calls.Add(clock.KahunaCalls[slot], tags);
            }
        }
    }
}

/// <summary>
/// One op's stage totals, in <see cref="Stopwatch"/> ticks. Owned by one async flow at a time.
/// </summary>
public sealed class QueryStageClock
{
    private static readonly int StageCount = QueryStageProfile.StageNames.Length;

    internal readonly long[] Ticks = new long[StageCount];
    internal readonly int[] Calls = new int[StageCount];
    private static readonly int KahunaSlots = StageCount * QueryStageProfile.KahunaPartNames.Length;

    // Indexed by stage * (number of Kahuna call kinds) + kind.
    internal readonly long[] KahunaTicks = new long[KahunaSlots];
    internal readonly int[] KahunaCalls = new int[KahunaSlots];

    /// <summary>The innermost stage open right now; a Kahuna call is charged to it.</summary>
    internal QueryStage? Open;

    /// <summary>Set once the op is known to be a query, and names its path; an op left unset is not published.</summary>
    public string? Path { get; set; }

    internal void Add(QueryStage stage, long ticks)
    {
        Ticks[(int)stage] += ticks;
        Calls[(int)stage]++;
    }

    internal void AddKahuna(KahunaCallKind kind, long ticks)
    {
        if (Open is not { } stage)
            return;

        int slot = (int)stage * QueryStageProfile.KahunaPartNames.Length + (int)kind;
        KahunaTicks[slot] += ticks;
        KahunaCalls[slot]++;
    }

    /// <summary>Adds an interval measured by the caller, for a stage that has no single scope to wrap.</summary>
    public void AddElapsed(QueryStage stage, long startTimestamp) => Add(stage, Stopwatch.GetTimestamp() - startTimestamp);

    internal bool Published { get; private set; }

    /// <summary>Publishes the totals when the op was a profiled query. Call once, when the op ends.</summary>
    public void Publish()
    {
        if (Published)
            return;

        Published = true;
        if (Path is { } path)
            QueryStageProfile.Publish(this, path);
    }
}
