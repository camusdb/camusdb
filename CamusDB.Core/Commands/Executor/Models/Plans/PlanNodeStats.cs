
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
    /// Wall-clock milliseconds spent executing this operator and all operators below it.
    /// Populated only on the plan root after the cursor is fully drained; null on all other
    /// nodes (not measured — emitted as NULL in EXPLAIN ANALYZE output rather than 0.0 which
    /// would imply zero cost).
    /// </summary>
    public double? ElapsedMs;

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
}
