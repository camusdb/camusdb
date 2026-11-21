/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander;
using Kommander.Data;
using Kommander.System;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// This node's view of one Raft partition: one row of <c>SHOW PARTITIONS</c>.
///
/// <para>A null member renders as SQL NULL and means "this node does not know". It never means zero,
/// "no leader" or "no replicas". Kommander reports "not hosted here" and "nothing committed" as -1;
/// the reader turns each -1 into null so that a client cannot take the sentinel for a real index or
/// term.</para>
///
/// <para>An empty <see cref="Voters"/> string means legacy full replication: every roster voter hosts
/// the partition. The system partition is always in this state.</para>
/// </summary>
internal sealed record ShowPartitionsRow(
    int PartitionId,
    bool IsSystem,
    string State,
    string? Routing,
    long? HashStart,
    long? HashEnd,
    long? Generation,
    string? Leader,
    string? LeaderSource,
    bool LeaderIsLocal,
    bool HostedLocally,
    long? Term,
    long? CommitIndex,
    long? ReplicationFactor,
    string Voters,
    string Learners,
    string Removing,
    bool AwaitingPlacement,
    double? LogOpsPerSecond,
    long? WalQueueDepth,
    double? CommitWaitMs,
    long? MaxFollowerLag,
    long? StalledFollowers);

/// <summary>
/// Builds the rows of <c>SHOW PARTITIONS</c> from the in-memory state of the local
/// <see cref="IRaft"/>: the system partition first, then every partition of the committed map that is
/// not removed, in ascending id order.
///
/// <para><b>Node-local.</b> The reader calls no peer and does no WAL or KV read. Every node holds the
/// same committed map, so every node lists the same partitions. The leader, the term, the commit
/// index and the follower health are this node's view. During an election two nodes can show
/// different values; that difference is the information, not an error.</para>
///
/// <para><b>The leader is a best-effort hint.</b> When this node believes that it leads, the leader is
/// the local endpoint. Otherwise the value comes from <see cref="IRaft.GetPartitionLeaderHint"/>: the
/// node's own belief for a hosted partition, and the newest gossiped load report for a partition it
/// does not host. Kommander also answers from gossip for a hosted partition that has no local belief
/// yet. A null leader means "unknown". Load-report gossip can be off, and then a node that does not
/// host a partition never knows its leader.</para>
///
/// <para><b>Follower health exists on the leader only.</b> Only the leader records what each follower
/// acknowledged, so the follower columns are null on every other node. A follower that has not
/// reported is left out of the lag maximum, because an unknown frontier does not mean "caught up".
/// Under full replication the followers are the nodes this node sees (<see cref="IRaft.GetNodes"/>),
/// because the replica list is empty.</para>
///
/// <para><b>One partition cannot fail the statement.</b> A replica move or a removal can land between
/// the map read and a per-partition call, which then throws <see cref="PartitionNotHostedException"/>.
/// Each per-partition read catches its own failure and leaves only the cells it feeds null.</para>
///
/// <para><b>Standalone node.</b> A single node hosts and leads every partition, and it can have no
/// leader hint yet. The reader therefore reports the local endpoint as the leader, the same rule as
/// <c>EmbeddedKahuna.BuildSpan</c>. Its Raft roster also holds two phantom witnesses that hold no
/// data, so the reader reports no follower for it: the lag is null and no follower is stalled.</para>
///
/// <para><b>No cache.</b> Each call reads live values. The planner's placement cache plays no part.</para>
/// </summary>
internal static class ShowPartitionsReader
{
    /// <summary>The leader value is this node's own belief: the node hosts the partition.</summary>
    internal const string LeaderSourceLocal = "local";

    /// <summary>The leader value is a peer's claim, from a gossiped load report.</summary>
    internal const string LeaderSourceGossip = "gossip";

    /// <summary>
    /// Reads one row for each partition. A cluster node that is not initialized yet (a node that is
    /// still joining) returns the system partition row only, and does not wait for the map.
    /// </summary>
    internal static async Task<List<ShowPartitionsRow>> ReadAsync(EmbeddedKahuna kahuna)
    {
        IRaft raft = kahuna.Raft;
        bool clusterMode = kahuna.IsClusterMode;
        string? localEndpoint = ReadLocalEndpoint(raft);

        List<RaftPartitionRange> ranges;

        if (raft.IsInitialized)
        {
            // GetPartitionMap returns a copy, so reading it once is enough for the whole statement.
            IReadOnlyList<RaftPartitionRange> map = raft.GetPartitionMap();
            ranges = new(map.Count);

            foreach (RaftPartitionRange range in map)
            {
                // A removed entry stays in the map only as a tombstone. The system partition is not
                // a map entry, but skip it here too so that it cannot appear twice.
                if (range.State != RaftPartitionState.Removed && range.PartitionId != RaftSystemConfig.SystemPartition)
                    ranges.Add(range);
            }

            ranges.Sort(static (a, b) => a.PartitionId.CompareTo(b.PartitionId));
        }
        else
        {
            ranges = [];
        }

        List<ShowPartitionsRow> rows = new(ranges.Count + 1)
        {
            await ReadPartitionAsync(raft, RaftSystemConfig.SystemPartition, range: null, clusterMode, localEndpoint).ConfigureAwait(false)
        };

        foreach (RaftPartitionRange range in ranges)
            rows.Add(await ReadPartitionAsync(raft, range.PartitionId, range, clusterMode, localEndpoint).ConfigureAwait(false));

        return rows;
    }

    /// <summary>
    /// Builds one row. <paramref name="range"/> is null for the system partition, which has no map
    /// entry: it has no hash range, no generation and no replica list, and every roster voter hosts it.
    /// </summary>
    private static async ValueTask<ShowPartitionsRow> ReadPartitionAsync(
        IRaft raft, int partitionId, RaftPartitionRange? range, bool clusterMode, string? localEndpoint)
    {
        bool hosted;
        bool leads;
        string? leader;
        string? leaderSource;

        if (!clusterMode)
        {
            hosted = true;
            leads = true;
            leader = localEndpoint;
            leaderSource = leader is null ? null : LeaderSourceLocal;
        }
        else
        {
            hosted = ReadHosted(raft, partitionId);

            // Only a node that hosts a partition can lead it, and AmILeaderQuick throws for a
            // partition that is not hosted.
            leads = hosted && await ReadLeadsAsync(raft, partitionId).ConfigureAwait(false);

            if (leads && localEndpoint is not null)
            {
                leader = localEndpoint;
                leaderSource = LeaderSourceLocal;
            }
            else
            {
                leader = ReadLeaderHint(raft, partitionId);
                leaderSource = leader is null ? null : hosted ? LeaderSourceLocal : LeaderSourceGossip;
            }
        }

        long? commitIndex = ReadCommitIndex(raft, partitionId);

        long? maxFollowerLag = null;
        long? stalledFollowers = null;

        if (leads)
        {
            // The Raft roster of a standalone node holds two phantom witnesses that grant it a
            // majority. They hold no data and never report a durable frontier, so their progress is
            // not a follower lag: a standalone node has no follower.
            if (clusterMode)
                (maxFollowerLag, stalledFollowers) = ReadFollowerHealth(raft, partitionId, range, localEndpoint, commitIndex);
            else
                stalledFollowers = 0;
        }

        string voters = "";
        string learners = "";
        string removing = "";
        bool awaitingPlacement = false;
        long? replicationFactor;

        if (range is null)
        {
            // The system partition lives on every roster voter: full replication.
            replicationFactor = 0;
        }
        else
        {
            (voters, learners, removing) = JoinReplicas(range.Replicas);
            replicationFactor = ReadReplicationFactor(raft, partitionId);

            // The rule of IRaft.GetPartitionsAwaitingPlacement: an active range with no replica set
            // yet, and a factor that asks for one.
            awaitingPlacement = range.State == RaftPartitionState.Active
                && range.Replicas.Count == 0
                && replicationFactor > 0;
        }

        bool unrouted = range is null || range.RoutingMode == RaftRoutingMode.Unrouted;

        return new ShowPartitionsRow(
            PartitionId: partitionId,
            IsSystem: range is null,
            State: range is null ? "active" : StateName(range.State),
            Routing: range is null ? null : RoutingName(range.RoutingMode),
            HashStart: unrouted ? null : range!.StartRange,
            HashEnd: unrouted ? null : range!.EndRange,
            Generation: range?.Generation,
            Leader: leader,
            LeaderSource: leaderSource,
            LeaderIsLocal: leads,
            HostedLocally: hosted,
            Term: ReadTerm(raft, partitionId),
            CommitIndex: commitIndex,
            ReplicationFactor: replicationFactor,
            Voters: voters,
            Learners: learners,
            Removing: removing,
            AwaitingPlacement: awaitingPlacement,
            LogOpsPerSecond: ReadLogOpsPerSecond(raft, partitionId),
            WalQueueDepth: ReadWalQueueDepth(raft, partitionId),
            CommitWaitMs: ReadCommitWaitMs(raft, partitionId),
            MaxFollowerLag: maxFollowerLag,
            StalledFollowers: stalledFollowers);
    }

    /// <summary>
    /// The largest lag of a follower behind <paramref name="leaderCommitIndex"/>, and the count of
    /// followers that the leader holds in a WAL-stall episode. Call it only on the leader. The lag is
    /// null when no follower has reported. Under full replication (an empty replica list, and always
    /// for the system partition) the followers are the nodes this node sees.
    /// </summary>
    private static (long? maxLag, long? stalled) ReadFollowerHealth(
        IRaft raft, int partitionId, RaftPartitionRange? range, string? localEndpoint, long? leaderCommitIndex)
    {
        long? maxLag = null;
        long stalled = 0;

        // A partition that has committed nothing has no lag to report: EntriesBehind floors at 0.
        long commit = leaderCommitIndex ?? -1;

        try
        {
            if (range is { Replicas.Count: > 0 })
            {
                foreach (RaftReplica replica in range.Replicas)
                {
                    // A removing replica no longer counts toward the quorum.
                    if (replica.Role != RaftReplicaRole.Removing)
                        Fold(raft.GetFollowerProgress(partitionId, replica.Endpoint), replica.Endpoint);
                }
            }
            else
            {
                foreach (RaftNode node in raft.GetNodes())
                    Fold(raft.GetFollowerProgress(partitionId, node.Endpoint), node.Endpoint);
            }
        }
        catch (Exception)
        {
            // A leadership or placement change while the replica list was read. The columns this
            // read feeds become NULL; the rest of the row stays.
            return (null, null);
        }

        return (maxLag, stalled);

        void Fold(RaftFollowerProgress? progress, string endpoint)
        {
            // A null progress is "unknown", never "caught up", so it stays out of the maximum.
            if (progress is null || string.Equals(endpoint, localEndpoint, StringComparison.Ordinal))
                return;

            long lag = progress.EntriesBehind(commit);
            if (maxLag is null || lag > maxLag)
                maxLag = lag;

            if (progress.WalStalled)
                stalled++;
        }
    }

    /// <summary>The replica endpoints of each role, comma-joined in map order. Empty when none.</summary>
    private static (string voters, string learners, string removing) JoinReplicas(List<RaftReplica> replicas)
    {
        if (replicas.Count == 0)
            return ("", "", "");

        List<string>? voters = null;
        List<string>? learners = null;
        List<string>? removing = null;

        foreach (RaftReplica replica in replicas)
        {
            switch (replica.Role)
            {
                case RaftReplicaRole.Voter:
                    (voters ??= new(replicas.Count)).Add(replica.Endpoint);
                    break;

                case RaftReplicaRole.Learner:
                    (learners ??= new(1)).Add(replica.Endpoint);
                    break;

                case RaftReplicaRole.Removing:
                    (removing ??= new(1)).Add(replica.Endpoint);
                    break;
            }
        }

        return (Join(voters), Join(learners), Join(removing));

        static string Join(List<string>? endpoints) => endpoints is null ? "" : string.Join(",", endpoints);
    }

    private static string StateName(RaftPartitionState state) => state switch
    {
        RaftPartitionState.Active => "active",
        RaftPartitionState.Splitting => "splitting",
        RaftPartitionState.Draining => "draining",
        RaftPartitionState.Removed => "removed",
        _ => state.ToString().ToLowerInvariant(),
    };

    private static string RoutingName(RaftRoutingMode mode) => mode switch
    {
        RaftRoutingMode.HashRange => "hash_range",
        RaftRoutingMode.Unrouted => "unrouted",
        _ => mode.ToString().ToLowerInvariant(),
    };

    // -----------------------------------------------------------------------
    // Per-partition reads. Each one catches its own failure and gives null, so that one partition
    // that moves or goes away during the statement empties only the cells that read feeds.
    // -----------------------------------------------------------------------

    private static string? ReadLocalEndpoint(IRaft raft)
    {
        try
        {
            string endpoint = raft.GetLocalEndpoint();
            return string.IsNullOrEmpty(endpoint) ? null : endpoint;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static bool ReadHosted(IRaft raft, int partitionId)
    {
        try
        {
            return raft.HostsPartition(partitionId);
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static async ValueTask<bool> ReadLeadsAsync(IRaft raft, int partitionId)
    {
        try
        {
            return await raft.AmILeaderQuick(partitionId).ConfigureAwait(false);
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static string? ReadLeaderHint(IRaft raft, int partitionId)
    {
        try
        {
            string? hint = raft.GetPartitionLeaderHint(partitionId);
            return string.IsNullOrEmpty(hint) ? null : hint;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>The term on this node, or null when the node does not host the partition (-1).</summary>
    private static long? ReadTerm(IRaft raft, int partitionId)
    {
        try
        {
            long term = raft.GetPartitionTerm(partitionId);
            return term < 0 ? null : term;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// The commit frontier on this node, or null when the node does not host the partition or the
    /// partition has committed nothing (-1 for both).
    /// </summary>
    private static long? ReadCommitIndex(IRaft raft, int partitionId)
    {
        try
        {
            long index = raft.GetCommitIndex(partitionId);
            return index < 0 ? null : index;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static long? ReadReplicationFactor(IRaft raft, int partitionId)
    {
        try
        {
            return raft.GetEffectiveReplicationFactor(partitionId);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static double? ReadLogOpsPerSecond(IRaft raft, int partitionId)
    {
        try
        {
            return raft.GetPartitionLogOpsPerSecond(partitionId);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static long? ReadWalQueueDepth(IRaft raft, int partitionId)
    {
        try
        {
            return raft.GetPartitionWalQueueDepth(partitionId);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static double? ReadCommitWaitMs(IRaft raft, int partitionId)
    {
        try
        {
            return raft.GetPartitionCommitWaitMs(partitionId);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
