/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Readiness probe answer for the local node. <c>Ready</c> requires cluster initialization to have
/// completed AND a serving roster role — a node can answer HTTP long before it can serve a single
/// key/value request. <c>HostedPartitions</c> is informational only: under a replication factor a
/// node hosting zero data partitions is still ready (it serves every key by forwarding), so probes
/// must never gate on it.
///
/// <para><c>CommitStalled</c> is the replication-liveness signal readiness deliberately is not: it
/// is true while this node leads a partition with an open backfill-refusal episode or a failing
/// snapshot rescue. Such a partition may still commit through its healthy quorum, so this must not
/// flip <c>Ready</c> (and does not change the HTTP status) — but it is exactly the degraded state
/// that container liveness and <c>Ready</c> alone cannot see, and the state a monitor should alarm
/// on before the next fault turns it into an outage. Details per partition are in
/// <c>StalledPartitions</c>; the full per-peer picture is at <c>/v1/cluster/backfill-status</c> and
/// <c>/v1/cluster/snapshot-status</c>.</para>
/// </summary>
public sealed class ClusterHealthResponse
{
    public bool Ready { get; set; }

    public bool Initialized { get; set; }

    public string LocalRole { get; set; } = "";

    public int HostedPartitions { get; set; }

    public bool CommitStalled { get; set; }

    public List<ClusterStalledPartitionModel> StalledPartitions { get; set; } = new();

    /// <summary>
    /// The storage engine's message when this node's key-value store is fail-stopped on damage, or the
    /// notice that the store was found empty at start while the Raft log holds history. Null while the
    /// store is intact as far as the node can tell. Unlike <see cref="CommitStalled"/>, this clears
    /// <see cref="Ready"/>: the node does not serve from that store. The text names files and paths, so
    /// only a superuser sees it when authentication is on.
    /// </summary>
    public string? StorageFault { get; set; }

    /// <summary>
    /// One line for each hosted partition whose Raft storage verdict is not healthy: lost acknowledged
    /// entries, an application floor below the compaction floor, a fail-stopped log engine, or a failed
    /// restore. Null when there is none. Any entry clears <see cref="Ready"/>. A replica with a repairable
    /// verdict clears this by itself when its leader repairs it.
    /// </summary>
    public List<string>? PartitionStorageFaults { get; set; }
}
