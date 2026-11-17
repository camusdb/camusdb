/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander;
using Kommander.Data;
using Kommander.System;
using Microsoft.Extensions.Hosting;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.App.Services;

/// <summary>
/// Hands the node's partition leadership to its peers when the host starts to stop.
///
/// <para>A stopping node otherwise keeps leading until its process ends, and its peers learn it is
/// gone only when its heartbeats stop: each partition it led then waits one election timeout, 2 to 4
/// seconds, with no leader. While the partition holding the authentication catalog has none, every
/// node refuses every authenticated call with CADB0504. A rolling restart did that once for each node
/// that led.</para>
///
/// <para>The hand-off runs in <see cref="IHostedLifecycleService.StoppingAsync"/>, which the host calls
/// before it stops any hosted service: the web server is still listening and every peer stream is
/// still up. It is bounded by <see cref="Budget"/>, and what it could not hand off is left to an
/// election, as before. The system partition cannot be handed off and is left to an election too.</para>
///
/// <para>Once the hand-off is over, whatever its outcome, the service closes the
/// <see cref="PeerConnectionGate"/>, which ends the streams peers hold to the node. Without that the web
/// server would wait for them until the host shutdown timeout. It closes the gate only once the
/// node's client connections have gone: the web server stops accepting and closes them after
/// <see cref="StoppingAsync"/> returns, and a request still running on one may need the peers. A
/// commit cut off from them was left unresolved, and its client waited 15 seconds for an answer.</para>
/// </summary>
public sealed class LeadershipHandoffService : IHostedLifecycleService
{
    /// <summary>
    /// The longest the hand-off may take. A transfer to a replica that is level returns in
    /// milliseconds; one to a lagging replica waits up to one election timeout for it to catch up.
    /// </summary>
    public static readonly TimeSpan Budget = TimeSpan.FromSeconds(8);

    /// <summary>
    /// The longest the node waits for its client connections to go before it ends its peers'
    /// streams anyway. The host shutdown timeout aborts whatever is still open after that.
    /// </summary>
    public static readonly TimeSpan ClientDrainBudget = TimeSpan.FromSeconds(10);

    private readonly EmbeddedKahuna kahuna;

    private readonly CommandExecutor executor;

    private readonly PeerConnectionGate peers;

    private readonly ILogger<LeadershipHandoffService> logger;

    private Task peersClosed = Task.CompletedTask;

    public LeadershipHandoffService(
        EmbeddedKahuna kahuna,
        CommandExecutor executor,
        PeerConnectionGate peers,
        ILogger<LeadershipHandoffService> logger)
    {
        this.kahuna = kahuna;
        this.executor = executor;
        this.peers = peers;
        this.logger = logger;
    }

    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await HandOffAsync(kahuna.Raft, Budget, logger, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OutOfMemoryException)
        {
            // A failed hand-off costs an election, which is what every stop cost before; it must not
            // keep the node from stopping.
            logger.LogWarning(e, "Leadership hand-off failed; the partitions this node leads will elect new leaders");
        }
        finally
        {
            // Not awaited: the web server starts closing the client connections only once this method
            // has returned.
            ClientConnectionRegistry clients = executor.QueryActivity.Connections;
            peersClosed = Task.Run(() => ClosePeersOnceClientsHaveGoneAsync(
                () => clients.Count, peers, ClientDrainBudget, logger));
        }
    }

    // Runs after the web server has stopped, by which time the peer streams have ended.
    public Task StopAsync(CancellationToken cancellationToken) => peersClosed;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Transfers leadership of every data partition this node leads to one of its peers, all
    /// partitions at once, and returns how many it handed off and how many it led.
    ///
    /// <para>The successor is a voter replica of the partition, or, when the partition lists no
    /// replicas because every node holds every partition, a voter of the cluster. The candidates are
    /// tried in turn, starting at a different one for each partition so that the leadership spreads
    /// over the peers instead of all landing on one.</para>
    /// </summary>
    public static async Task<(int HandedOff, int Led)> HandOffAsync(
        IRaft raft,
        TimeSpan budget,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!raft.IsInitialized)
            return (0, 0);

        string local = raft.GetLocalEndpoint();

        List<string> voters = [];
        foreach (ClusterMember member in raft.GetMembership().Members)
        {
            if (member.Role == ClusterMemberRole.Voter && member.Endpoint != local)
                voters.Add(member.Endpoint);
        }
        voters.Sort(StringComparer.Ordinal);

        using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bound.CancelAfter(budget);

        List<Task<bool>> transfers = [];
        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            // Consensus refuses to transfer the system partition.
            if (range.PartitionId == RaftSystemConfig.SystemPartition
                || range.State == RaftPartitionState.Removed
                || !raft.HostsPartition(range.PartitionId))
                continue;

            if (!await raft.AmILeaderQuick(range.PartitionId).ConfigureAwait(false))
                continue;

            List<string> candidates = range.Replicas.Count == 0
                ? [.. voters]
                : [.. range.Replicas
                    .Where(r => r.Role == RaftReplicaRole.Voter && r.Endpoint != local && voters.Contains(r.Endpoint))
                    .Select(r => r.Endpoint)
                    .Order(StringComparer.Ordinal)];

            transfers.Add(TransferAsync(raft, range.PartitionId, Rotate(candidates, range.PartitionId), logger, bound.Token));
        }

        if (transfers.Count == 0)
            return (0, 0);

        bool[] outcomes = await Task.WhenAll(transfers).ConfigureAwait(false);
        int handedOff = outcomes.Count(o => o);

        logger.LogInformation("Handed off leadership of {HandedOff} of {Led} partitions before stopping", handedOff, outcomes.Length);

        return (handedOff, outcomes.Length);
    }

    /// <summary>
    /// Offers one partition to each candidate in turn until one takes it, the leadership moves
    /// elsewhere on its own, or the budget runs out.
    /// </summary>
    private static async Task<bool> TransferAsync(
        IRaft raft,
        int partitionId,
        IReadOnlyList<string> candidates,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        foreach (string target in candidates)
        {
            RaftOperationStatus status;
            try
            {
                status = await raft.TransferLeadershipAsync(partitionId, target, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (RaftException e)
            {
                logger.LogDebug("Could not hand partition {PartitionId} to {Target}: {Reason}", partitionId, target, e.Message);
                continue;
            }

            switch (status)
            {
                // Success: the target leads. Pending: it was told to, and the hand-off is out of this
                // node's hands. LeaderAlreadyElected and NodeIsNotLeader: another replica leads already.
                case RaftOperationStatus.Success:
                case RaftOperationStatus.Pending:
                case RaftOperationStatus.LeaderAlreadyElected:
                case RaftOperationStatus.NodeIsNotLeader:
                    return true;
            }

            logger.LogDebug("Could not hand partition {PartitionId} to {Target}: {Status}", partitionId, target, status);
        }

        logger.LogWarning("Could not hand off leadership of partition {PartitionId}; it will elect a new leader", partitionId);
        return false;
    }

    /// <summary>
    /// Waits until no client connection is open, or <paramref name="budget"/> has passed, then
    /// closes <paramref name="peers"/>. Returns how many peer connections it aborted.
    /// </summary>
    public static async Task<int> ClosePeersOnceClientsHaveGoneAsync(
        Func<int> openClients,
        PeerConnectionGate peers,
        TimeSpan budget,
        ILogger logger)
    {
        DateTime deadline = DateTime.UtcNow + budget;
        while (openClients() > 0 && DateTime.UtcNow < deadline)
            await Task.Delay(20).ConfigureAwait(false);

        int stillOpen = openClients();
        int aborted = peers.Close();

        if (stillOpen > 0)
            logger.LogWarning("Closed {Connections} peer connections before stopping with {Clients} client connections still open",
                aborted, stillOpen);
        else
            logger.LogInformation("Closed {Connections} peer connections before stopping", aborted);

        return aborted;
    }

    /// <summary>Starts the candidate list at a position that depends on the partition.</summary>
    private static List<string> Rotate(List<string> candidates, int partitionId)
    {
        if (candidates.Count < 2)
            return candidates;

        int start = partitionId % candidates.Count;
        return [.. candidates.Skip(start), .. candidates.Take(start)];
    }
}
