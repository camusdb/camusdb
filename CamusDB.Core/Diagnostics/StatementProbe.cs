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
/// <para><b>It exists only when the log is on.</b> The engine creates one per statement when
/// <see cref="CamusDBOptions.SlowQueryLogEnabled"/> is set and leaves the reference null otherwise,
/// so every write site is a null-conditional call and a node with the log off pays one null check
/// per scanned row and nothing more. That is why the type is not folded into
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

    /// <summary>Rows fetched from storage before filtering, across every scan the statement ran.</summary>
    public long RowsRead => Interlocked.Read(ref rowsRead);

    /// <summary>True when any part of the statement scanned a whole relation.</summary>
    public bool FullScan => Volatile.Read(ref fullScan) != 0;

    /// <summary>True when any blocking operator wrote to a spill file.</summary>
    public bool Spilled => Volatile.Read(ref spilled) != 0;

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
