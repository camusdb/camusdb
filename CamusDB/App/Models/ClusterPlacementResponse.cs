/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// The committed partition placement table: which nodes host each partition and in what role, the
/// effective replication factor per partition, and which partitions the answering node hosts
/// locally. Under full replication (factor 0) every replica set is empty and everything is hosted
/// locally. Every node returns the same committed map; only the local-perspective fields differ.
/// </summary>
public sealed class ClusterPlacementResponse
{
    public int ReplicationFactor { get; set; }

    public bool RebalancerEnabled { get; set; }

    public bool Initialized { get; set; }

    public string LocalEndpoint { get; set; } = "";

    public int HostedPartitionCount { get; set; }

    public List<ClusterPartitionPlacementModel> Partitions { get; set; } = new();
}

/// <summary>
/// One partition's placement. <c>Generation</c> bumps on every placement or split/merge change, so
/// two snapshots with the same generation describe the same placement. An empty <c>Replicas</c>
/// list means legacy full replication (every voter hosts it).
/// </summary>
public sealed class ClusterPartitionPlacementModel
{
    public int PartitionId { get; set; }

    public string State { get; set; } = "";

    public long Generation { get; set; }

    public int EffectiveReplicationFactor { get; set; }

    public bool HostedLocally { get; set; }

    /// <summary>Whether the answering node believes it is the Raft leader of this partition. Each
    /// partition has exactly one leader, so polling every node's placement and collecting the
    /// partitions each reports <c>LeaderLocal</c> for gives the whole leader distribution — the
    /// signal for observing the leader balancer. Local belief only (see <c>AmILeaderQuick</c>).</summary>
    public bool LeaderLocal { get; set; }

    public List<ClusterPartitionReplicaModel> Replicas { get; set; } = new();
}

/// <summary>One replica in a partition's committed replica set: hosting node and role
/// (Voter counts toward quorum; Learner is catching up; Removing is on its way out).</summary>
public sealed class ClusterPartitionReplicaModel
{
    public string Endpoint { get; set; } = "";

    public string Role { get; set; } = "";
}
