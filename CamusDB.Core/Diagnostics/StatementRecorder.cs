/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Times statements and hands each finished one to the two per-statement diagnostics: the slow query
/// log and statement statistics.
///
/// <para><b>One recording, two consumers.</b> Both features need the same facts about a statement —
/// its duration, the rows it read, whether it scanned a whole relation or spilled — and those facts
/// come from one <see cref="StatementProbe"/> that the scans and the blocking operators already write
/// into. A second probe would double the per-row cost and could disagree with the first, so there is
/// one recording per statement, created when either consumer is on, and its single finish point feeds
/// both. A statement with both off gets no recording: no probe, no clock, no allocation.</para>
///
/// <para><b>Slow query log.</b> The ring is built only when the log is first enabled — at construction,
/// or by the first published options snapshot that turns <see cref="CamusDBOptions.SlowQueryLogEnabled"/>
/// on — so an engine that never enables the log never holds the ring's array. The threshold and the
/// enabled flag are re-read per statement; the ring's capacity is latched from the construction-time
/// options. Turning the log off stops new entries and keeps the ring and what it holds.</para>
///
/// <para><b>Statement statistics.</b> The table follows the same rules: it is built when
/// <see cref="CamusDBOptions.StatementStatsEnabled"/> is first on, its capacity
/// (<see cref="CamusDBOptions.StatementStatsMaxShapes"/>) is latched from the construction-time
/// options, and turning the feature off stops recording and keeps the totals.</para>
/// </summary>
public sealed class StatementRecorder
{
    /// <summary>
    /// Ring capacity, latched from the construction-time options. A ring created later by a live
    /// enable uses this value rather than the live snapshot's, because the setting is restart-class.
    /// </summary>
    private readonly int slowLogCapacity;

    /// <summary>Statement statistics capacity, latched for the same reason as <see cref="slowLogCapacity"/>.</summary>
    private readonly int statsCapacity;

    /// <summary>
    /// Null until the log is first enabled, then fixed. Written before the enabling snapshot is
    /// published to <see cref="options"/>, so a statement that sees the log enabled also sees the ring.
    /// </summary>
    private SlowQueryLog? log;

    /// <summary>Null until statement statistics are first enabled, then fixed; published like <see cref="log"/>.</summary>
    private StatementStatsTable? stats;

    private CamusDBOptions options;

    /// <summary>
    /// The log this recorder writes into, read by <c>SHOW SLOW QUERIES</c>; null while the log has
    /// never been enabled on this engine.
    /// </summary>
    public SlowQueryLog? Log => Volatile.Read(ref log);

    /// <summary>
    /// The statistics this recorder writes into, read by <c>SHOW STATEMENT STATS</c>; null while they
    /// have never been enabled on this engine.
    /// </summary>
    public StatementStatsTable? Stats => Volatile.Read(ref stats);

    public StatementRecorder(CamusDBOptions options)
    {
        this.options = options;
        slowLogCapacity = options.SlowQueryLogMaxEntries;
        statsCapacity = options.StatementStatsMaxShapes;

        if (options.SlowQueryLogEnabled)
            log = new SlowQueryLog(slowLogCapacity, options.SlowQueryLogMaxSqlLength);

        if (options.StatementStatsEnabled)
            stats = new StatementStatsTable(statsCapacity, options.SlowQueryLogMaxSqlLength);
    }

    /// <summary>
    /// Adopts a newly published configuration snapshot. The enabled flags and the threshold take
    /// effect on the next statement; the truncation length is handed to the log and the table, which
    /// apply it to the next entry they create. Capacities are deliberately not adopted.
    ///
    /// <para>The first snapshot that enables a consumer creates its store, and the store is published
    /// before the snapshot is: <see cref="Begin"/> reads the snapshot first, so it never sees a
    /// consumer enabled with no store behind it. Each create is a compare-exchange, so two concurrent
    /// publishes cannot leave two stores with entries split between them.</para>
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        SlowQueryLog? currentLog = Volatile.Read(ref log);

        if (currentLog is null && next.SlowQueryLogEnabled)
        {
            SlowQueryLog created = new(slowLogCapacity, next.SlowQueryLogMaxSqlLength);
            currentLog = Interlocked.CompareExchange(ref log, created, null) ?? created;
        }

        currentLog?.ApplyOptions(next);

        StatementStatsTable? currentStats = Volatile.Read(ref stats);

        if (currentStats is null && next.StatementStatsEnabled)
        {
            StatementStatsTable created = new(statsCapacity, next.SlowQueryLogMaxSqlLength);
            currentStats = Interlocked.CompareExchange(ref stats, created, null) ?? created;
        }

        currentStats?.ApplyOptions(next);

        Volatile.Write(ref options, next);
    }

    /// <summary>
    /// Starts timing a statement, or returns null when both consumers are currently off.
    ///
    /// <para>Called before the statement is parsed, so parsing, authorization, opening the database
    /// and planning are all inside the reported duration. An operator chasing a slow statement needs
    /// them there: a statement that spends two seconds waiting for a schema catch-up is slow, and a
    /// clock started after that wait would report it as fast.</para>
    /// </summary>
    public StatementRecording? Begin(string sql, string database, string? user)
    {
        CamusDBOptions current = Volatile.Read(ref options);

        SlowQueryLog? targetLog = current.SlowQueryLogEnabled ? Volatile.Read(ref log) : null;
        StatementStatsTable? targetStats = current.StatementStatsEnabled ? Volatile.Read(ref stats) : null;

        if (targetLog is null && targetStats is null)
            return null;

        return new StatementRecording(targetLog, current.SlowQueryLogThresholdMs, targetStats, sql, database, user);
    }
}

/// <summary>
/// One statement being timed. It owns the clock, the probe the operators write into, and the single
/// point at which the statement is handed to the slow query log, to statement statistics, to both or
/// to neither.
///
/// <para><b>Finishing is idempotent.</b> A row-returning statement can reach its end more than once
/// — the wrapped cursor completes and is then disposed, or it throws and is then disposed — and a
/// statement must be counted once. The first call to <see cref="Finish"/> wins and later calls do
/// nothing, so the recorded outcome is the one that actually ended the statement rather than the
/// disposal that followed it.</para>
/// </summary>
public sealed class StatementRecording
{
    private readonly SlowQueryLog? log;

    private readonly int thresholdMs;

    private readonly StatementStatsTable? stats;

    private readonly string sql;

    private readonly string database;

    private readonly string? user;

    private readonly DateTime startedAtUtc;

    private readonly long startedAtTicks;

    private int finished;

    /// <summary>
    /// The statement's shape id, set by <see cref="Describe(NodeAst?)"/> when statement statistics are
    /// on and the statement is of a kind they record; null otherwise, which keeps it out of them.
    /// </summary>
    private string? shapeId;

    /// <summary>The kind statement statistics record the statement under; null alongside <see cref="shapeId"/>.</summary>
    private string? statsKind;

    /// <summary>The accumulator the scans and the blocking operators of this statement write into.</summary>
    public StatementProbe Probe { get; } = new();

    /// <summary>
    /// The statement's kind, set once the parse has identified it. It stays <c>unknown</c> when the
    /// statement failed to parse, which is itself worth seeing: a parse that is slow enough to be
    /// recorded points at a pathological statement text rather than at the data.
    /// </summary>
    public string Kind { get; private set; } = "unknown";

    internal StatementRecording(
        SlowQueryLog? log, int thresholdMs, StatementStatsTable? stats, string sql, string database, string? user)
    {
        this.log = log;
        this.thresholdMs = thresholdMs;
        this.stats = stats;
        this.sql = sql;
        this.database = database;
        this.user = user;

        startedAtUtc = DateTime.UtcNow;
        startedAtTicks = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Names the statement from its parsed root: the kind, spelled the way the configuration file
    /// spells its enum values (<c>select</c>, <c>show_tables</c>, <c>create_table</c>), and — when
    /// statement statistics are on and the statement is one they record — its shape.
    /// </summary>
    /// <param name="ast">
    /// The parsed root, or null when the statement could not be parsed. Null leaves the kind at
    /// <c>unknown</c> rather than inventing one, and keeps the statement out of statement statistics.
    /// </param>
    public void Describe(NodeAst? ast)
    {
        if (ast is null)
            return;

        Kind = UnderscoredNamingConvention.Instance.Apply(ast.nodeType.ToString());

        if (stats is not null && StatementShape.KindOf(ast.nodeType) is { } kind)
        {
            statsKind = kind;
            shapeId = StatementShape.Of(ast);
        }
    }

    /// <summary>
    /// Ends the statement: records it in the slow query log if it reached the threshold, and adds it
    /// to statement statistics if it is a recorded kind. Safe to call more than once; only the first
    /// call has an effect.
    /// </summary>
    public void Finish(long rowsReturned, SlowQueryOutcome outcome, string? errorCode = null)
    {
        if (Interlocked.Exchange(ref finished, 1) != 0)
            return;

        double elapsedMs = Stopwatch.GetElapsedTime(startedAtTicks).TotalMilliseconds;

        if (log is not null && elapsedMs >= thresholdMs)
        {
            log.Record(
                startedAtUtc,
                elapsedMs,
                database,
                user,
                Kind,
                sql,
                rowsReturned,
                Probe.RowsRead,
                Probe.FullScan,
                Probe.Spilled,
                outcome,
                errorCode);
        }

        if (stats is not null && shapeId is not null)
        {
            bool failed = outcome == SlowQueryOutcome.Failed;

            stats.Record(
                database,
                shapeId,
                statsKind!,
                sql,
                DateTime.UtcNow,
                elapsedMs,
                Probe.RowsRead,
                rowsReturned,
                Probe.FullScan,
                Probe.Spilled,
                failed,
                failed && errorCode is not null && SerializableRetryHelper.RetryableCodes.Contains(errorCode),
                Probe.PlanFingerprint);
        }
    }

    /// <summary>
    /// Abandons this recording: the statement is timed but never stored.
    ///
    /// <para>It exists for the statement that reads the log. <c>SHOW SLOW QUERIES</c> recording
    /// itself would mean every read evicts an entry, so anything polling the log — the operator
    /// dashboard does, every few seconds — would erase the history it exists to display. Reading a
    /// diagnostic must not change it.</para>
    ///
    /// <para>Implemented by claiming the finish flag, so the cursor wrapper's later
    /// <see cref="Finish"/> and <see cref="FinishFailed"/> both become the no-ops they already are on
    /// a second call.</para>
    /// </summary>
    public void Discard() => Interlocked.Exchange(ref finished, 1);

    /// <summary>
    /// Ends a statement that raised, taking the error code from the exception when it is one of the
    /// engine's own.
    /// </summary>
    public void FinishFailed(Exception exception, long rowsReturned = 0)
        => Finish(
            rowsReturned,
            SlowQueryOutcome.Failed,
            exception is CamusDBException camusException ? camusException.Code : exception.GetType().Name);

    /// <summary>
    /// Wraps a result cursor so the statement is recorded when the cursor ends, whichever way it
    /// ends.
    ///
    /// <para><b>This wrapper is the only correct place to stop the clock for a row-returning
    /// statement.</b> <c>ExecuteSQLQuery</c> returns a lazy cursor: when it returns, the statement
    /// has been planned and nothing has been read. A duration taken at that point measures planning
    /// and reports every query as fast.</para>
    ///
    /// <para>The <c>finally</c> covers all three endings. A drained cursor is
    /// <see cref="SlowQueryOutcome.Completed"/>; a cursor the caller stops reading — a client that
    /// disconnected, or one that took the first page and left — is
    /// <see cref="SlowQueryOutcome.Abandoned"/>; a cursor that raises is
    /// <see cref="SlowQueryOutcome.Failed"/>.</para>
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> Wrap(
        IAsyncEnumerable<QueryResultRow> cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long rows = 0;
        bool drained = false;
        Exception? failure = null;

        try
        {
            IAsyncEnumerator<QueryResultRow> enumerator = cursor.GetAsyncEnumerator(cancellationToken);

            try
            {
                while (true)
                {
                    // Stepping the enumerator by hand rather than with `await foreach` is what lets a
                    // failure be caught here: a `yield return` cannot sit inside a try/catch block.
                    try
                    {
                        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                        {
                            drained = true;
                            break;
                        }
                    }
                    catch (Exception exception)
                    {
                        failure = exception;
                        throw;
                    }

                    rows++;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            if (failure is not null)
                FinishFailed(failure, rows);
            else
                Finish(rows, drained ? SlowQueryOutcome.Completed : SlowQueryOutcome.Abandoned);
        }
    }
}
