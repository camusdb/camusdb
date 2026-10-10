/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Answers the activity statements: <c>SHOW [CLUSTER] QUERIES</c>, <c>SHOW [CLUSTER]
/// CONNECTIONS</c> and <c>CANCEL QUERY</c>. The node-local forms read
/// <see cref="QueryActivityRegistry"/> directly; the cluster forms add the rows of every peer,
/// gathered through <see cref="IClusterActivityTransport"/>.
///
/// <para><b>A peer that fails does not fail the statement.</b> Each peer call has its own timeout
/// (<see cref="CamusDBOptions.ClusterActivityPeerTimeoutMs"/>), the calls run in parallel, and a peer
/// that fails or does not answer in time gives one row that names it and carries the error. An
/// operator runs these statements most often while something is wrong, and a list that fails
/// because one node is down fails exactly then.</para>
///
/// <para><b>Membership is Raft's view at the moment of the statement.</b> A node that joined a
/// moment ago may be missing and a node that left may still be asked; the second case gives an error
/// row, not a wrong answer.</para>
/// </summary>
public sealed class QueryActivityService
{
    private readonly QueryActivityRegistry registry;

    private readonly IClusterActivityTransport? transport;

    private readonly Func<IReadOnlyList<string>> peers;

    private CamusDBOptions options;

    /// <param name="registry">This node's running statements and connections.</param>
    /// <param name="transport">The channel to peers, or null on an engine that has none.</param>
    /// <param name="peers">
    /// Returns the Raft endpoints of the other cluster members, not including this node. Returns an
    /// empty list in standalone mode, where Raft's node list holds only quorum witnesses.
    /// </param>
    /// <param name="options">The configuration snapshot in force when the engine is built.</param>
    public QueryActivityService(
        QueryActivityRegistry registry,
        IClusterActivityTransport? transport,
        Func<IReadOnlyList<string>> peers,
        CamusDBOptions options)
    {
        this.registry = registry;
        this.transport = transport;
        this.peers = peers;
        this.options = options;
    }

    /// <summary>This node's registry.</summary>
    public QueryActivityRegistry Registry => registry;

    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// The running statements <paramref name="viewer"/> may see, on this node or on every member,
    /// oldest first.
    /// </summary>
    public async Task<List<QueryActivityRow>> ListQueriesAsync(bool cluster, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        List<QueryActivityRow> rows = registry.SnapshotQueries(viewer);

        if (!cluster)
            return rows;

        IReadOnlyList<string> targets = peers();
        if (targets.Count == 0)
            return rows;

        List<QueryActivityRow>[] fromPeers = await Task.WhenAll(
            targets.Select(target => FetchQueriesAsync(target, viewer, cancellationToken))).ConfigureAwait(false);

        foreach (List<QueryActivityRow> peerRows in fromPeers)
            rows.AddRange(peerRows);

        // Error rows have no start time and sort first, where an operator sees them before the list.
        rows.Sort(static (a, b) => a.StartedAt.CompareTo(b.StartedAt));
        return rows;
    }

    /// <summary>
    /// The open connections <paramref name="viewer"/> may see, on this node or on every member,
    /// oldest first.
    /// </summary>
    public async Task<List<ConnectionActivityRow>> ListConnectionsAsync(bool cluster, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        List<ConnectionActivityRow> rows = registry.SnapshotConnections(viewer);

        if (!cluster)
            return rows;

        IReadOnlyList<string> targets = peers();
        if (targets.Count == 0)
            return rows;

        List<ConnectionActivityRow>[] fromPeers = await Task.WhenAll(
            targets.Select(target => FetchConnectionsAsync(target, viewer, cancellationToken))).ConfigureAwait(false);

        foreach (List<ConnectionActivityRow> peerRows in fromPeers)
            rows.AddRange(peerRows);

        rows.Sort(static (a, b) => a.OpenedAt.CompareTo(b.OpenedAt));
        return rows;
    }

    /// <summary>
    /// Cancels the statement <paramref name="queryId"/> on whichever member owns it, or raises.
    ///
    /// <para>The id names its owner: its first six characters are the tag of the owner's Raft
    /// endpoint. An id this process minted is cancelled here; any other id goes to the peers whose
    /// tag matches. Two members can share a tag, so each match is asked until one owns the id.</para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// <see cref="CamusDBErrorCodes.QueryNotFound"/>, <see cref="CamusDBErrorCodes.QueryNotCancellable"/>,
    /// or <see cref="CamusDBErrorCodes.QueryOwnerUnreachable"/> when a peer that may own the id did
    /// not answer.
    /// </exception>
    public async Task CancelAsync(string queryId, ActivityViewer viewer)
    {
        QueryCancelOutcome outcome = QueryCancelOutcome.NotFound;
        string? unreachable = null;

        if (registry.IsLocalId(queryId))
        {
            outcome = registry.TryCancel(queryId, viewer);
        }
        else if (transport is not null && ActivityIdMinter.TryGetNodeTag(queryId, out string tag))
        {
            foreach (string target in peers())
            {
                if (!string.Equals(ActivityIdMinter.NodeTagOf(target), tag, StringComparison.Ordinal))
                    continue;

                // The cancel itself must not be abandoned because the caller went away: the timeout
                // is its only bound.
                using CancellationTokenSource timeout = new(options.ClusterActivityPeerTimeoutMs);

                try
                {
                    outcome = await transport.CancelAsync(target, queryId, viewer, timeout.Token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    unreachable = target;
                    continue;
                }

                if (outcome != QueryCancelOutcome.NotFound)
                    break;
            }
        }

        switch (outcome)
        {
            case QueryCancelOutcome.Cancelled:
                return;

            case QueryCancelOutcome.NotCancellable:
                throw new CamusDBException(
                    CamusDBErrorCodes.QueryNotCancellable,
                    $"Query '{queryId}' cannot be cancelled: it is a write or schema change, which a cancel cannot stop once it runs, or its parse has not ended yet");

            default:
                if (unreachable is not null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.QueryOwnerUnreachable,
                        $"Node '{unreachable}', which may own query '{queryId}', did not answer; the query may still run");

                throw new CamusDBException(CamusDBErrorCodes.QueryNotFound, $"No running query has id '{queryId}'");
        }
    }

    private async Task<List<QueryActivityRow>> FetchQueriesAsync(string target, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        if (transport is null)
            return [QueryErrorRow(target, "This node has no channel to its peers")];

        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ClusterActivityPeerTimeoutMs);

        try
        {
            return await transport.ListQueriesAsync(target, viewer, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return [QueryErrorRow(target, Describe(exception, timeout.IsCancellationRequested))];
        }
    }

    private async Task<List<ConnectionActivityRow>> FetchConnectionsAsync(string target, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        if (transport is null)
            return [ConnectionErrorRow(target, "This node has no channel to its peers")];

        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ClusterActivityPeerTimeoutMs);

        try
        {
            return await transport.ListConnectionsAsync(target, viewer, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return [ConnectionErrorRow(target, Describe(exception, timeout.IsCancellationRequested))];
        }
    }

    private string Describe(Exception exception, bool timedOut)
        => timedOut
            ? $"No answer within {options.ClusterActivityPeerTimeoutMs} ms"
            : exception.Message;

    private static QueryActivityRow QueryErrorRow(string node, string error)
        => new("", node, null, null, "", null, "", null, null, "", "", default, 0, 0, false, false, "", error);

    private static ConnectionActivityRow ConnectionErrorRow(string node, string error)
        => new("", node, null, null, "", null, default, 0, 0, 0, 0, 0, error);
}
