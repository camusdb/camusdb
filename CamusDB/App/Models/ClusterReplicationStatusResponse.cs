/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// The answering node's leader-side view of backfill refusals, per hosted partition. An entry
/// exists while this node, as a partition's leader, cannot ship an anchored backfill batch to a
/// follower because no committed entry exists at the follower's anchor — a healthy cluster
/// returns an empty list. This is the REST form of <c>IRaft.GetBackfillStatuses</c>, the query
/// the refusal log line tells the operator to run. Each node reports only the partitions it
/// leads, so a full-cluster picture is the union across nodes.
/// </summary>
public sealed class ClusterBackfillStatusResponse
{
    public string LocalEndpoint { get; set; } = "";

    public bool Initialized { get; set; }

    public List<ClusterPartitionBackfillModel> Partitions { get; set; } = new();
}

public sealed class ClusterPartitionBackfillModel
{
    public int PartitionId { get; set; }

    /// <summary>The partition's committed frontier on this node at the time of the read.</summary>
    public long CommitIndex { get; set; }

    public List<ClusterPeerBackfillModel> Peers { get; set; } = new();
}

public sealed class ClusterPeerBackfillModel
{
    public string FollowerEndpoint { get; set; } = "";

    /// <summary>The index the refused batch would have been anchored at — the follower's next needed entry.</summary>
    public long AnchorIndex { get; set; }

    /// <summary>First committed entry the leader can actually read at or above the anchor.</summary>
    public long FirstAvailableIndex { get; set; }

    /// <summary>
    /// The partition's last checkpoint when the episode opened. At or above the first available
    /// index means the anchor was compacted away and only a snapshot install can seed the
    /// follower; below the anchor means the entries exist but are uncommitted.
    /// </summary>
    public long LastCheckpoint { get; set; }

    /// <summary>Refusals since the episode opened; only the first logs at Warning.</summary>
    public int Occurrences { get; set; }

    public DateTimeOffset FirstRefusedAt { get; set; }

    public DateTimeOffset LastRefusedAt { get; set; }
}

/// <summary>
/// The answering node's leader-side view of snapshot transfers, per hosted partition. An entry
/// exists only while a transfer to a follower is in flight or has recorded failures — a healthy
/// cluster returns an empty list. This is the REST form of <c>IRaft.GetSnapshotStatuses</c>: it
/// answers "the backfill refusal escalated; what happened to the rescue?" from a live cluster.
/// </summary>
public sealed class ClusterSnapshotStatusResponse
{
    public string LocalEndpoint { get; set; } = "";

    public bool Initialized { get; set; }

    public List<ClusterPartitionSnapshotModel> Partitions { get; set; } = new();
}

public sealed class ClusterPartitionSnapshotModel
{
    public int PartitionId { get; set; }

    /// <summary>The partition's committed frontier on this node at the time of the read.</summary>
    public long CommitIndex { get; set; }

    public List<ClusterPeerSnapshotModel> Peers { get; set; } = new();
}

public sealed class ClusterPeerSnapshotModel
{
    public string FollowerEndpoint { get; set; } = "";

    /// <summary>Consecutive failed transfer attempts in the current episode.</summary>
    public int FailedAttempts { get; set; }

    public string? LastError { get; set; }

    /// <summary>
    /// True when no snapshot can be produced for this configuration (no transfer registered, or
    /// the application rejected the export). The follower cannot catch up until that changes.
    /// </summary>
    public bool Unproducible { get; set; }

    public bool InFlight { get; set; }

    /// <summary>How long the in-flight transfer has been running, in milliseconds; null when none.</summary>
    public double? InFlightForMs { get; set; }

    public DateTimeOffset? FirstFailureAt { get; set; }

    public DateTimeOffset? LastFailureAt { get; set; }

    /// <summary>Milliseconds until the next transfer attempt is allowed; 0 when not backing off.</summary>
    public double RetryBackoffRemainingMs { get; set; }
}

/// <summary>
/// One hosted partition whose replication is stalled, as reported inside
/// <see cref="ClusterHealthResponse"/>: this node leads it and either refuses backfill to at
/// least one replica or cannot complete a snapshot rescue. The partition may still be serving
/// (a quorum of healthy replicas commits without the stalled one), but it is degraded and it is
/// one more fault away from stopping — the exact condition container-level monitoring cannot
/// see (the Caraxes soaks watched a wedged cluster report "3/3 up, healthy" for 83 minutes).
/// </summary>
public sealed class ClusterStalledPartitionModel
{
    public int PartitionId { get; set; }

    /// <summary>Replicas this node currently refuses to backfill (open refusal episodes).</summary>
    public int OpenBackfillRefusals { get; set; }

    /// <summary>True when a snapshot rescue is required but cannot be produced.</summary>
    public bool SnapshotUnproducible { get; set; }

    /// <summary>Highest consecutive-failure count across this partition's snapshot transfers.</summary>
    public int SnapshotFailedAttempts { get; set; }

    /// <summary>True while a snapshot transfer to one of the replicas is in flight.</summary>
    public bool SnapshotInFlight { get; set; }

    /// <summary>The partition's committed frontier on this node at the time of the read.</summary>
    public long CommitIndex { get; set; }
}
