
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Exchange operator: executes its <see cref="PhysicalPlanNode.Input"/> scan subtree once per
/// placement span — each execution bounded to that span's row-id range — and concatenates the
/// results in span order.
///
/// <para><b>Why concatenation is enough:</b> the spans of a <see cref="TablePlacement"/> form a
/// complete, contiguous, ascending partition of the row keyspace (the first starts unbounded,
/// the last ends unbounded, adjacent spans share an endpoint). Ordered concatenation therefore
/// reproduces exactly the row stream the unbounded scan would produce, for every plan shape —
/// LIMIT, ORDER BY ties, and streaming operators included. This also makes the node immune to
/// placement staleness: a stale snapshot's spans still partition the keyspace completely, so
/// every row is scanned exactly once; a split that happened after capture only costs routing
/// hops inside Kahuna's locator.</para>
///
/// <para>The gather runs span fetches concurrently (each span's pages stream from its
/// partition leader), which is where the parallelism comes from. Until the remote fragment
/// service exists, span execution stays in-process and remote pages arrive via locator
/// routing; relocating fragment execution to the data-owning node changes the transport, not
/// this operator's semantics. Runs normally under EXPLAIN ANALYZE — the gather's stats
/// bookkeeping is consumer-side and single-threaded — except in partial-aggregation mode,
/// which ANALYZE disables (it replaces instrumented scanning, so ANALYZE reports the
/// row-gather strategy it actually executed).</para>
/// </summary>
/// <summary>
/// Execution actuals for one gather span, filled in while the gather runs and rendered by
/// <c>EXPLAIN (ANALYZE)</c> as a <c>gather-span</c> row. Thread-safety is by ownership, not
/// locks: every field except <see cref="RowsDelivered"/> is written only by that span's
/// worker, <see cref="RowsDelivered"/> only by the single consumer thread, and the whole
/// object is read only after the cursor has fully drained.
/// </summary>
public sealed class GatherSpanExecution
{
    /// <summary>The Kahuna partition that owned the span in the placement snapshot.</summary>
    public required int PartitionId { get; init; }

    /// <summary>True when the span was sent to a peer as a fragment (even if it later fell back).</summary>
    public bool DispatchedRemotely { get; set; }

    /// <summary>The peer the fragment was sent to; null for spans executed locally from the start.</summary>
    public string? RemoteEndpoint { get; set; }

    /// <summary>True when the remote fragment failed and the span resumed locally.</summary>
    public bool FellBackToLocal { get; set; }

    /// <summary>
    /// Rows this span contributed across the exchange. Not uniform on purpose: a remotely
    /// executed span contributes only filter survivors (the residual filter ran at the data),
    /// while a locally scanned span contributes every scanned row (its filter runs above the
    /// exchange, on the consumer). Comparing the two is exactly what shows the shipping win.
    /// </summary>
    public long RowsDelivered { get; set; }

    /// <summary>Survivor rows received from the remote fragment before it completed or failed.</summary>
    public long RemoteRowsShipped { get; set; }

    /// <summary>
    /// Rows the remote fragment scanned, from its terminal stats frame; null when the peer did
    /// not report (older build, or the stream failed before the frame arrived).
    /// </summary>
    public long? RemoteRowsScanned { get; set; }
}

public sealed class GatherNode : PhysicalPlanNode
{
    /// <summary>The placement snapshot the planner sliced against (advisory; see class doc).</summary>
    public TablePlacement Placement { get; }

    /// <summary>
    /// Per-span execution actuals, index-aligned with <see cref="TablePlacement.Spans"/>.
    /// Populated only when the plan collects runtime stats (<c>EXPLAIN (ANALYZE)</c>) — plan
    /// trees are rebuilt per execution, so this never leaks across statements — and read only
    /// after the gather's cursor has fully drained.
    /// </summary>
    public GatherSpanExecution[]? SpanExecutions { get; set; }

    /// <summary>
    /// Per-span cap on filter-surviving rows (<c>limit + offset</c>), when the plan shape
    /// permits: since span-order concatenation preserves global row order, the final LIMIT cut
    /// can never require more than this many rows from any one span. Applied remotely by the
    /// fragment (survivors stop shipping) — local spans rely on the pull pipeline's laziness.
    /// Null = unbounded.
    /// </summary>
    public long? MaxSurvivors { get; init; }

    /// <summary>Gather output lands on the coordinator regardless of input distribution.</summary>
    public override bool CanDecomposeToLocalPlusMerge => false;

    public GatherNode(TablePlacement placement)
    {
        Placement = placement;
        Distribution = DataDistribution.Gathered;
    }

    /// <summary>
    /// Extracts the row-id bound encoded in a placement-span boundary key, which is a full KV
    /// key (<c>{rowKeyPrefix}{rowIdHex24}</c> — split points are chosen from real row keys).
    /// Null boundary = unbounded. Returns false for a key that does not carry a well-formed
    /// 24-hex row-id suffix; the planner then declines to fragment rather than guess.
    /// </summary>
    public static bool TryParseRowIdBound(string? boundKey, out ObjectIdValue? rowId)
    {
        rowId = null;

        if (boundKey is null)
            return true;

        int slash = boundKey.LastIndexOf('/');
        if (slash < 0 || boundKey.Length - slash - 1 != 24)
            return false;

        ReadOnlySpan<char> hex = boundKey.AsSpan(slash + 1);

        foreach (char c in hex)
        {
            if (c is (< '0' or > '9') and (< 'a' or > 'f'))
                return false;
        }

        rowId = ObjectId.ToValue(hex);
        return true;
    }
}
