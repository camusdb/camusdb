
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Runtime counters collected during EXPLAIN ANALYZE execution.
/// All fields are updated by the executor while rows flow through the operator;
/// they are read-only after the cursor has been drained.
///
/// <para><b>Join nodes.</b> A join node has one plan input, its left side; the right side is a
/// table or derived table the node names, not a child node. The storage counters of a join node
/// (<see cref="RowsRead"/>, <see cref="KvPointLookups"/>, <see cref="KvScanEntries"/>) therefore
/// count the reads of its right side, while <see cref="RowsEmitted"/> counts the rows the join
/// produced. A merge join's ordered right input is a node of its own
/// (<c>MergeJoinNode.RightPhysicalNode</c>) that the plan walk does not render, so EXPLAIN ANALYZE
/// adds that subtree's storage counters to the join row.</para>
///
/// <para><b>Threading.</b> The fields are plain, not interlocked: every writer runs on the one
/// thread that consumes the node's cursor. Operators that read on worker threads (the broadcast
/// probe) count on the consumer thread when a row arrives, never in the worker.</para>
///
/// <para><b>Repeated runs.</b> A node can run more than once in one statement — a hash join whose
/// build outgrows its cap reads its build input again in the fallback. The counters add up over
/// every run, which is the work the statement actually did; <see cref="RanAs"/> names the
/// fallback that explains it.</para>
/// </summary>
public sealed class PlanNodeStats
{
    /// <summary>
    /// Rows fetched and decoded from storage by a scan operator (pre-filter count).
    /// Always ≥ <see cref="RowsEmitted"/>. For unique-index point lookups this is 0 or 1.
    /// Zero for non-scan operators (they read from an upstream cursor, not from storage).
    /// On a join node, the rows read from its right side (see the class summary).
    /// </summary>
    public long RowsRead;

    /// <summary>Rows emitted (yielded) by this operator to its parent (post-filter count).</summary>
    public long RowsEmitted;

    /// <summary>
    /// Wall-clock milliseconds of a whole run, set from outside the operator's cursor: on the plan
    /// root, the time to drain the plan; on the root of a derived table's plan and on its derived-table
    /// line, the time of the derived query's runs. When it is set, EXPLAIN (ANALYZE) prints it in place
    /// of <see cref="CursorMs"/>.
    /// </summary>
    public double? ElapsedMs;

    /// <summary>
    /// <see cref="System.Diagnostics.Stopwatch"/> ticks spent inside the operator's row cursor: from each
    /// request for a row until the row (or the end) arrives. A request runs the inputs of the operator, so
    /// the time includes theirs, and the time a parent spends on a row it got is left out. Written by
    /// <see cref="Controllers.Queries.PlanNodeCursor.CountAndTime"/>, which only EXPLAIN (ANALYZE) puts on a
    /// cursor; a plain query keeps no timer.
    /// </summary>
    public long CursorTicks;

    /// <summary>True when the cursor of the node ran under a timer, so <see cref="CursorTicks"/> is a measurement.</summary>
    public bool CursorTimed;

    /// <summary>The inclusive time of the operator's cursor in milliseconds, or null when it ran without a timer.</summary>
    public double? CursorMs => CursorTimed ? TicksToMs(CursorTicks) : null;

    /// <summary>The time EXPLAIN (ANALYZE) prints for the node: <see cref="ElapsedMs"/> when set, else <see cref="CursorMs"/>.</summary>
    public double? ReportedMs => ElapsedMs ?? CursorMs;

    /// <summary>
    /// Left rows for which a nested-loop join ran its <c>LATERAL</c> body. A left row that read the rows
    /// that an earlier run with the same left values kept counts in <see cref="LateralKeptReads"/> instead.
    /// </summary>
    public long LateralRuns;

    /// <summary>Left rows for which a nested-loop join read the kept rows of an earlier run of its <c>LATERAL</c> body.</summary>
    public long LateralKeptReads;

    /// <summary>
    /// <see cref="System.Diagnostics.Stopwatch"/> ticks the <c>LATERAL</c> body of a nested-loop join took over all
    /// left rows: the runs, the reads of kept rows and the right filter. Part of the join's
    /// <see cref="CursorTicks"/>.
    /// </summary>
    public long LateralTicks;

    /// <summary>Converts <see cref="System.Diagnostics.Stopwatch"/> ticks to milliseconds.</summary>
    public static double TicksToMs(long ticks) => ticks * 1000.0 / System.Diagnostics.Stopwatch.Frequency;

    /// <summary>KV point-lookups issued (unique-index lookups).</summary>
    public long KvPointLookups;

    /// <summary>KV scan entries visited (table or range-index scans).</summary>
    public long KvScanEntries;

    /// <summary>
    /// Probes of an index nested-loop join that the join answered from its memo of earlier probes
    /// instead of the store: an outer row whose key an earlier outer row already probed. Each hit
    /// is a probe that read nothing, so <see cref="KvPointLookups"/>, <see cref="KvScanEntries"/>
    /// and <see cref="RowsRead"/> count the distinct keys only. EXPLAIN ANALYZE prints it as
    /// <c>probe-memo-hits=</c> when it is above zero.
    /// </summary>
    public long ProbeMemoHits;

    /// <summary>
    /// Batched probes an index nested-loop join over a unique index issued: each one answers the
    /// distinct keys of a page of outer rows with one index read and one row read, instead of two reads
    /// per outer row. <see cref="KvPointLookups"/> still counts the keys, so the ratio of the two is the
    /// keys per batch. EXPLAIN ANALYZE prints it as <c>probe-batches=</c> when it is above zero.
    /// </summary>
    public long ProbeBatches;

    /// <summary>
    /// The strategy the operator actually ran, when it is not the one the plan names; null when
    /// the operator ran as planned. A hash join decides two things only after its build exists: a
    /// build that outgrows its cap runs as a Grace hash join (<c>grace-hash</c>) or a nested-loop
    /// join (<c>nested-loop-fallback</c>), and a build small enough to ship runs as a broadcast
    /// probe (<c>broadcast</c>). The plan cannot show either, so EXPLAIN ANALYZE prints this value
    /// next to the node.
    /// </summary>
    public string? RanAs;

    /// <summary>
    /// Per-span actuals of a broadcast probe, index-aligned with the placement spans it probed;
    /// null unless the node ran as a broadcast probe. Rendered as <c>broadcast-span</c> rows after
    /// the join row. As in a gather, a probe row this node decodes counts in <see cref="RowsRead"/>
    /// of the node that owns the probe input, local or shipped; the rows a remote span scanned and
    /// did not ship are visible only here.
    /// </summary>
    public GatherSpanExecution[]? BroadcastSpans;

    /// <summary>
    /// Row locks that the lock step of a locking read (<see cref="LockRowsNode"/>) got: one per
    /// candidate row it sent, less the rows SKIP LOCKED left out. It includes the rows that
    /// <see cref="RowsDroppedByRecheck"/> counts, because their locks stay until the transaction ends.
    /// So <c>locked − recheck-dropped</c> is the rows the step emitted. EXPLAIN ANALYZE prints it as
    /// <c>locked=</c> on the lock step.
    /// </summary>
    public long RowsLocked;

    /// <summary>
    /// Candidate rows that SKIP LOCKED left out because another transaction held them. Always zero for
    /// the other wait policies, which fail the statement on a held row. Printed as
    /// <c>skipped-held=</c>.
    /// </summary>
    public long RowsSkippedHeld;

    /// <summary>
    /// Rows the lock step locked and then did not emit, because the read after the lock found the row
    /// deleted or no longer matching the WHERE clause (a concurrent commit changed it after the scan
    /// chose it). Printed as <c>recheck-dropped=</c>.
    /// </summary>
    public long RowsDroppedByRecheck;
}
