/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Auth;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Per-statement-shape counters, read by <c>SHOW STATEMENT STATS</c>: the CamusDB counterpart of
/// PostgreSQL's <c>pg_stat_statements</c>. One entry per (database, shape) — see
/// <see cref="StatementShape"/> — accumulates every finished execution of that shape on this node.
///
/// <para><b>Totals, not averages.</b> Every additive value is kept and reported as a running total
/// since the shape was first seen: calls, errors, milliseconds, squared milliseconds, rows. A reader
/// takes two readings and subtracts them to get any window it wants, including the variance of the
/// window (from the sum and the sum of squares). A table of means could not be windowed at all.</para>
///
/// <para><b>Bounded.</b> At most <see cref="Capacity"/> shapes are held. The capacity is restart-class
/// because it is latched at construction. A shape that arrives when the table is full is not admitted
/// and <see cref="Dropped"/> counts it; existing entries keep counting. Nothing is evicted, because an
/// eviction would silently reset a shape's totals, and a reader subtracting two readings would see a
/// negative window. The drop count is published in <c>SHOW ENGINE STATS</c> as
/// <c>statement_stats.dropped</c> so a reader can tell that the view is incomplete.</para>
///
/// <para><b>Concurrency.</b> Each entry has its own lock, held for a handful of field updates, and the
/// table itself is a concurrent dictionary, so two statements of different shapes never contend and
/// the record path takes no global lock. The entry count is kept in its own interlocked counter
/// because <see cref="ConcurrentDictionary{TKey,TValue}.Count"/> takes every bucket lock.</para>
///
/// <para>Node-local and volatile, like the slow query log: it describes what this process ran, it is
/// never gathered from peers, and it does not survive a restart.</para>
/// </summary>
public sealed class StatementStatsTable
{
    private readonly ConcurrentDictionary<(string Database, string ShapeId), StatementStatsEntry> entries = new();

    private int count;

    private long dropped;

    /// <summary>
    /// SQL text length kept as a shape's sample. Runtime-mutable through <see cref="ApplyOptions"/>; it
    /// shares the slow query log's setting so one knob bounds the statement text both features hold.
    /// </summary>
    private volatile int maxSqlLength;

    /// <summary>Shapes this table holds at most.</summary>
    public int Capacity { get; }

    /// <summary>Shapes refused because the table was full, since this node started.</summary>
    public long Dropped => Interlocked.Read(ref dropped);

    /// <summary>Shapes held now.</summary>
    public int Count => Volatile.Read(ref count);

    public StatementStatsTable(int capacity, int maxSqlLength)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSqlLength, 1);
        Capacity = capacity;
        this.maxSqlLength = maxSqlLength;
    }

    /// <summary>Adopts the sample truncation length of a newly published configuration snapshot.</summary>
    internal void ApplyOptions(CamusDBOptions next)
    {
        if (next.SlowQueryLogMaxSqlLength >= 1)
            maxSqlLength = next.SlowQueryLogMaxSqlLength;
    }

    /// <summary>
    /// Adds one finished execution to its shape, creating the entry on the shape's first execution
    /// when there is room.
    ///
    /// <para>The sample text is redacted and truncated only when an entry is created, so a statement
    /// of an already-known shape pays neither.</para>
    /// </summary>
    /// <param name="planFingerprint">
    /// The probe's plan hash, or zero when the execution built no plan; zero never counts as a plan
    /// change, so a statement that failed before planning does not look like a plan flip.
    /// </param>
    public void Record(
        string database,
        string shapeId,
        string kind,
        string sql,
        DateTime finishedAtUtc,
        double durationMs,
        long rowsRead,
        long rowsReturned,
        bool fullScan,
        bool spilled,
        bool failed,
        bool retryable,
        ulong planFingerprint)
    {
        if (!entries.TryGetValue((database, shapeId), out StatementStatsEntry? entry))
        {
            if (Interlocked.Increment(ref count) > Capacity)
            {
                Interlocked.Decrement(ref count);

                // A second thread may have added this very shape a moment ago; it still counts.
                if (!entries.TryGetValue((database, shapeId), out entry))
                {
                    Interlocked.Increment(ref dropped);
                    return;
                }
            }
            else
            {
                StatementStatsEntry created = new(database, shapeId, kind, Sample(sql), finishedAtUtc);
                entry = entries.GetOrAdd((database, shapeId), created);

                // Lost the race to another thread that added the same shape: give the slot back.
                if (!ReferenceEquals(entry, created))
                    Interlocked.Decrement(ref count);
            }
        }

        entry.Add(finishedAtUtc, durationMs, rowsRead, rowsReturned, fullScan, spilled, failed, retryable, planFingerprint);
    }

    /// <summary>A consistent copy of each entry, taken entry by entry under that entry's lock.</summary>
    public List<StatementStatsSnapshot> Snapshot()
    {
        List<StatementStatsSnapshot> result = new(Count);

        foreach (KeyValuePair<(string, string), StatementStatsEntry> pair in entries)
            result.Add(pair.Value.Snapshot());

        return result;
    }

    private string Sample(string sql)
    {
        sql = SqlCredentialRedactor.Redact(sql);
        int limit = maxSqlLength;
        return sql.Length > limit ? sql[..limit] : sql;
    }
}

/// <summary>The running totals of one statement shape. Mutated only under its own lock.</summary>
internal sealed class StatementStatsEntry
{
    private readonly object sync = new();

    private readonly string database;

    private readonly string shapeId;

    private readonly string kind;

    private readonly string sampleSql;

    private readonly DateTime firstSeenAt;

    private DateTime lastSeenAt;

    private long calls;

    private long errors;

    private double totalMs;

    private double sumSqMs;

    private double minMs = double.MaxValue;

    private double maxMs;

    private long rowsRead;

    private long rowsReturned;

    private long fullScans;

    private long spills;

    private long retries;

    private long planChanges;

    private ulong lastPlan;

    internal StatementStatsEntry(string database, string shapeId, string kind, string sampleSql, DateTime firstSeenAt)
    {
        this.database = database;
        this.shapeId = shapeId;
        this.kind = kind;
        this.sampleSql = sampleSql;
        this.firstSeenAt = firstSeenAt;
        lastSeenAt = firstSeenAt;
    }

    internal void Add(
        DateTime finishedAtUtc,
        double durationMs,
        long read,
        long returned,
        bool fullScan,
        bool spilled,
        bool failed,
        bool retryable,
        ulong planFingerprint)
    {
        lock (sync)
        {
            calls++;
            totalMs += durationMs;
            sumSqMs += durationMs * durationMs;

            if (durationMs < minMs)
                minMs = durationMs;

            if (durationMs > maxMs)
                maxMs = durationMs;

            rowsRead += read;
            rowsReturned += returned;

            if (fullScan)
                fullScans++;

            if (spilled)
                spills++;

            if (failed)
                errors++;

            if (retryable)
                retries++;

            if (planFingerprint != 0)
            {
                if (lastPlan != 0 && lastPlan != planFingerprint)
                    planChanges++;

                lastPlan = planFingerprint;
            }

            if (finishedAtUtc > lastSeenAt)
                lastSeenAt = finishedAtUtc;
        }
    }

    internal StatementStatsSnapshot Snapshot()
    {
        lock (sync)
        {
            return new StatementStatsSnapshot(
                database, shapeId, kind, sampleSql, calls, errors, totalMs, sumSqMs,
                calls == 0 ? 0 : minMs, maxMs, rowsRead, rowsReturned, fullScans, spills, retries,
                planChanges, lastPlan, firstSeenAt, lastSeenAt);
        }
    }
}

/// <summary>
/// One shape's totals as read at one instant. The mean, the standard deviation and the coefficient
/// of variation are derived here, at read time, from the totals; they are never stored.
/// </summary>
public sealed record StatementStatsSnapshot(
    string Database,
    string ShapeId,
    string Kind,
    string SampleSql,
    long Calls,
    long Errors,
    double TotalMs,
    double SumSqMs,
    double MinMs,
    double MaxMs,
    long RowsRead,
    long RowsReturned,
    long FullScans,
    long Spills,
    long Retries,
    long PlanChanges,
    ulong LastPlan,
    DateTime FirstSeenAt,
    DateTime LastSeenAt)
{
    /// <summary>Mean duration, or zero before the first call.</summary>
    public double MeanMs => Calls == 0 ? 0 : TotalMs / Calls;

    /// <summary>
    /// Population standard deviation from the sum and the sum of squares. Clamped at zero: the
    /// subtraction can go a hair negative for identical durations because of rounding.
    /// </summary>
    public double StddevMs => Calls == 0 ? 0 : Math.Sqrt(Math.Max(0, (SumSqMs / Calls) - (MeanMs * MeanMs)));

    /// <summary>Coefficient of variation, stddev over mean; zero when the mean is zero.</summary>
    public double Cv => MeanMs == 0 ? 0 : StddevMs / MeanMs;
}
