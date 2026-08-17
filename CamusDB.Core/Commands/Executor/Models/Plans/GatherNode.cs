
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
/// this operator's semantics. Degrades to a single unbounded execution of the input under
/// EXPLAIN ANALYZE (per-node runtime stats are not thread-safe across span workers).</para>
/// </summary>
public sealed class GatherNode : PhysicalPlanNode
{
    /// <summary>The placement snapshot the planner sliced against (advisory; see class doc).</summary>
    public TablePlacement Placement { get; }

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
