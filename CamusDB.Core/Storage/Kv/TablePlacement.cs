
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// One contiguous slice of a key space and where it lives right now: the owning partition and
/// this node's current best-effort view of that partition's leadership and replica set.
///
/// <para><see cref="StartKey"/> is inclusive and <see cref="EndKey"/> exclusive, both relative
/// to the key space (null = unbounded on that side), matching Kahuna's range-descriptor
/// contract. Hash-routed key spaces produce exactly one span with both bounds null and
/// <see cref="Generation"/> 0 — hash spaces never split, so they carry no fence.</para>
///
/// <para><see cref="LeaderEndpoint"/> is a gossip/local-belief hint, never a correctness gate:
/// null means "unknown", not "no leader", and a stale value costs a routed hop at execution
/// time, never a wrong answer. <see cref="ReplicaEndpoints"/> empty means legacy full
/// replication — every roster node hosts the partition; a non-empty list (the planned
/// replication-factor regime) is the complete set of nodes that host this span at all, and
/// fragment dispatch may target only those.</para>
/// </summary>
public sealed record PlacementSpan(
    string? StartKey,
    string? EndKey,
    int PartitionId,
    long Generation,
    string? LeaderEndpoint,
    IReadOnlyList<string> ReplicaEndpoints,
    bool LeaderIsLocal,
    bool HostedLocally);

/// <summary>
/// A point-in-time view of where one key space's data lives across the cluster, composed from
/// Kahuna's node-local range map (<c>IKahuna.GetRangeMap</c>) and Kommander's partition
/// placement (<c>IRaft.GetPartitionReplicas</c> / <c>GetPartitionLeaderHint</c> /
/// <c>HostsPartition</c>). Produced and cached by <see cref="EmbeddedKahuna.GetPlacement"/>.
///
/// <para>Advisory by design: the range map is the local applied view (a lagging follower
/// reports an older generation) and leadership hints are best-effort. Planning may act on
/// this snapshot freely because execution always re-resolves through the Kahuna locator —
/// staleness costs a network hop or a retry, never correctness.</para>
/// </summary>
public sealed class TablePlacement
{
    /// <summary>The bucket prefix this placement describes (no trailing slash).</summary>
    public string KeySpace { get; }

    /// <summary>
    /// True when the key space is key-range routed with at least one seeded descriptor;
    /// false for hash routing (single span) and for a registered-but-unseeded range space.
    /// </summary>
    public bool IsKeyRange { get; }

    /// <summary>Spans in ascending key order; always at least one.</summary>
    public IReadOnlyList<PlacementSpan> Spans { get; }

    /// <summary>
    /// Fraction of spans whose leader is not this node. A span with an unknown leader counts
    /// as remote — pessimistic on purpose, so the cost model never undercharges shipping.
    /// </summary>
    public double RemoteLeaderFraction { get; }

    /// <summary>True when every span's leader is (believed to be) this node.</summary>
    public bool AllLeadersLocal => RemoteLeaderFraction <= 0.0;

    /// <summary>Monotonic capture instant (<see cref="Environment.TickCount64"/>) for TTL checks.</summary>
    internal long CapturedAtTicks { get; }

    public TablePlacement(string keySpace, bool isKeyRange, IReadOnlyList<PlacementSpan> spans, long capturedAtTicks)
    {
        if (spans.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "A placement must have at least one span");

        KeySpace = keySpace;
        IsKeyRange = isKeyRange;
        Spans = spans;
        CapturedAtTicks = capturedAtTicks;

        int remote = 0;
        foreach (PlacementSpan span in spans)
        {
            if (!span.LeaderIsLocal)
                remote++;
        }

        RemoteLeaderFraction = remote == 0 ? 0.0 : (double)remote / spans.Count;
    }

    /// <summary>
    /// The standalone-mode placement: one unbounded span, local by definition. Built without
    /// touching Kahuna or Raft — a single node has nothing to ask.
    /// </summary>
    internal static TablePlacement Local(string keySpace) =>
        new(
            keySpace,
            isKeyRange: false,
            spans:
            [
                new PlacementSpan(
                    StartKey: null,
                    EndKey: null,
                    PartitionId: 1,
                    Generation: 0,
                    LeaderEndpoint: null,
                    ReplicaEndpoints: [],
                    LeaderIsLocal: true,
                    HostedLocally: true)
            ],
            capturedAtTicks: Environment.TickCount64);
}
