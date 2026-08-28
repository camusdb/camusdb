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
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Decides which statements the slow query log keeps, and times them.
///
/// <para>The engine builds one of these per instance when
/// <see cref="CamusDBOptions.SlowQueryLogEnabled"/> is set at construction, and holds null otherwise.
/// A caller asks <see cref="Begin"/> for a recording at the top of a statement; a null answer means
/// the log is off right now and the caller runs the statement with no probe, no clock, and no
/// allocation.</para>
///
/// <para>The threshold and the enabled flag are re-read per statement from the current options
/// snapshot, which is what lets both be honestly classified as runtime-mutable. The ring's capacity
/// is not, because the ring allocated its array when this recorder was built.</para>
/// </summary>
public sealed class SlowQueryRecorder
{
    private readonly SlowQueryLog log;

    private CamusDBOptions options;

    /// <summary>The log this recorder writes into, read by <c>SHOW SLOW QUERIES</c>.</summary>
    public SlowQueryLog Log => log;

    public SlowQueryRecorder(CamusDBOptions options)
    {
        this.options = options;
        log = new SlowQueryLog(options.SlowQueryLogMaxEntries, options.SlowQueryLogMaxSqlLength);
    }

    /// <summary>
    /// Adopts a newly published configuration snapshot. The enabled flag and the threshold take
    /// effect on the next statement; the truncation length is handed to the log, which applies it to
    /// the next entry. Capacity is deliberately not adopted — see
    /// <see cref="CamusDBOptions.SlowQueryLogMaxEntries"/>.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        options = next;
        log.ApplyOptions(next);
    }

    /// <summary>
    /// Starts timing a statement, or returns null when the log is currently disabled.
    ///
    /// <para>Called before the statement is parsed, so parsing, authorization, opening the database
    /// and planning are all inside the reported duration. An operator chasing a slow statement needs
    /// them there: a statement that spends two seconds waiting for a schema catch-up is slow, and a
    /// clock started after that wait would report it as fast.</para>
    /// </summary>
    public SlowQueryRecording? Begin(string sql, string database, string? user)
    {
        CamusDBOptions current = options;

        return current.SlowQueryLogEnabled
            ? new SlowQueryRecording(log, current.SlowQueryLogThresholdMs, sql, database, user)
            : null;
    }
}

/// <summary>
/// One statement being timed. It owns the clock, the probe the operators write into, and the single
/// point at which an entry is either kept or discarded.
///
/// <para><b>Finishing is idempotent.</b> A row-returning statement can reach its end more than once
/// — the wrapped cursor completes and is then disposed, or it throws and is then disposed — and an
/// entry must be written once. The first call to <see cref="Finish"/> wins and later calls do
/// nothing, so the recorded outcome is the one that actually ended the statement rather than the
/// disposal that followed it.</para>
/// </summary>
public sealed class SlowQueryRecording
{
    private readonly SlowQueryLog log;

    private readonly int thresholdMs;

    private readonly string sql;

    private readonly string database;

    private readonly string? user;

    private readonly DateTime startedAtUtc;

    private readonly long startedAtTicks;

    private int finished;

    /// <summary>The accumulator the scans and the blocking operators of this statement write into.</summary>
    public StatementProbe Probe { get; } = new();

    /// <summary>
    /// The statement's kind, set once the parse has identified it. It stays <c>unknown</c> when the
    /// statement failed to parse, which is itself worth seeing: a parse that is slow enough to be
    /// recorded points at a pathological statement text rather than at the data.
    /// </summary>
    public string Kind { get; private set; } = "unknown";

    internal SlowQueryRecording(SlowQueryLog log, int thresholdMs, string sql, string database, string? user)
    {
        this.log = log;
        this.thresholdMs = thresholdMs;
        this.sql = sql;
        this.database = database;
        this.user = user;

        startedAtUtc = DateTime.UtcNow;
        startedAtTicks = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Names the statement's kind from its parsed root, spelled the way the configuration file
    /// spells its enum values: <c>select</c>, <c>show_tables</c>, <c>create_table</c>.
    /// </summary>
    /// <param name="nodeType">
    /// The parsed root, or null when the statement could not be parsed. Null leaves the kind at
    /// <c>unknown</c> rather than inventing one.
    /// </param>
    public void Describe(NodeType? nodeType)
    {
        if (nodeType is { } parsed)
            Kind = UnderscoredNamingConvention.Instance.Apply(parsed.ToString());
    }

    /// <summary>
    /// Ends the statement and records it if it reached the threshold. Safe to call more than once;
    /// only the first call has an effect.
    /// </summary>
    public void Finish(long rowsReturned, SlowQueryOutcome outcome, string? errorCode = null)
    {
        if (Interlocked.Exchange(ref finished, 1) != 0)
            return;

        double elapsedMs = Stopwatch.GetElapsedTime(startedAtTicks).TotalMilliseconds;

        if (elapsedMs < thresholdMs)
            return;

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
