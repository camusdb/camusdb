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
}
