/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// The execution facts one statement accumulates while it runs, so the slow query log can explain a
/// duration instead of only reporting it.
///
/// <para><b>It exists only when a consumer is on.</b> The engine creates one per statement when
/// <see cref="CamusDBOptions.SlowQueryLogEnabled"/> or <see cref="CamusDBOptions.StatementStatsEnabled"/>
/// is set and leaves the reference null otherwise, so every write site is a null-conditional call and
/// a node with both off pays one null check per scanned row and nothing more. Both consumers read the
/// same probe; see <see cref="StatementRecording"/>. That is why the type is not folded into
/// <see cref="Models.Plans.PlanNodeStats"/>, which is per plan node and allocated per node; this is
/// per statement and spans a plan, its subqueries, and its joins.</para>
///
/// <para><b>It travels on the ticket and the execution context.</b> Both already reach the places
/// that know these facts: <c>QueryPlan.Ticket</c> is reachable from every scan, and
/// <c>QueryExecutionContext</c> is what the blocking operators consult before they spill. Neither
/// carrier is new, so the probe adds no threading of its own.</para>
///
/// <para><b>Written from several threads.</b> A join reads its two sides concurrently and a
/// distributed plan runs fragments in parallel, so the counters are interlocked and the flags are
/// set-once ints rather than bools. Reads are meaningful only after the statement is finished,
/// which is the single point where the recording site reads them.</para>
/// </summary>
public sealed class StatementProbe
{
    private long rowsRead;

    private int fullScan;

    private int spilled;

    /// <summary>
    /// Distinct hashes of the plans the statement built, guarded by its own monitor. A statement
    /// builds one plan per relation it reads — a join, a subquery, a derived table — and a correlated
    /// subquery may build the same plan once per outer row, so the set keeps each plan once.
    /// </summary>
    private List<ulong>? planHashes;

    /// <summary>Rows fetched from storage before filtering, across every scan the statement ran.</summary>
    public long RowsRead => Interlocked.Read(ref rowsRead);

    /// <summary>True when any part of the statement scanned a whole relation.</summary>
    public bool FullScan => Volatile.Read(ref fullScan) != 0;

    /// <summary>True when any blocking operator wrote to a spill file.</summary>
    public bool Spilled => Volatile.Read(ref spilled) != 0;

    /// <summary>
    /// A hash of every distinct plan the statement built, or zero when it built none (an
    /// <c>INSERT … VALUES</c> plans nothing). Order-independent, because the plans of a join's two
    /// sides can be built concurrently, and duplicate-insensitive, because a plan built once per outer
    /// row must not make two executions of the same statement look different.
    /// </summary>
    public ulong PlanFingerprint
    {
        get
        {
            List<ulong>? hashes = Volatile.Read(ref planHashes);
            if (hashes is null)
                return 0;

            lock (hashes)
            {
                ulong combined = 0;
                foreach (ulong hash in hashes)
                    combined = unchecked(combined + ((hash * 0x9E3779B97F4A7C15UL) ^ (hash >> 29)));

                return combined == 0 ? 1 : combined;
            }
        }
    }

    /// <summary>
    /// Records that the statement built a plan with this hash. Called once per plan, where the planner
    /// has just settled the access paths; see <c>QueryPlanStepAdapter</c>.
    /// </summary>
    public void NotePlan(ulong hash)
    {
        List<ulong> hashes = Volatile.Read(ref planHashes)
            ?? Interlocked.CompareExchange(ref planHashes, [], null)
            ?? Volatile.Read(ref planHashes)!;

        lock (hashes)
        {
            if (!hashes.Contains(hash))
                hashes.Add(hash);
        }
    }

    /// <summary>Counts one row fetched from storage.</summary>
    public void AddRowRead() => Interlocked.Increment(ref rowsRead);

    /// <summary>
    /// Counts a batch of rows fetched from storage in one call, for a scan that reads in blocks
    /// rather than one row at a time.
    /// </summary>
    public void AddRowsRead(long count)
    {
        if (count > 0)
            Interlocked.Add(ref rowsRead, count);
    }

    /// <summary>
    /// Marks the statement as having scanned a whole relation. Called from the planner, where the
    /// scan strategy is decided, rather than from the scan loops: the planner settles the question
    /// once, and a loop that inferred the same fact from its own inputs would have to agree with the
    /// planner about what counts as a full scan.
    /// </summary>
    public void NoteFullScan() => Volatile.Write(ref fullScan, 1);

    /// <summary>
    /// Marks the statement as having spilled to disk. Called where a spill scope is created, which
    /// is the one path every blocking operator takes to get spill storage — so a new operator that
    /// spills is reported without touching this type.
    /// </summary>
    public void NoteSpill() => Volatile.Write(ref spilled, 1);
}
