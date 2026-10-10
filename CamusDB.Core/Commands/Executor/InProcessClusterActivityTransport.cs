/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// <see cref="IClusterActivityTransport"/> for a cluster whose members all live in one process: it
/// calls the target member's own <see cref="QueryActivityService"/> instead of its HTTP endpoint.
/// It does what the host's <c>/internal/activity/*</c> endpoints do — the node-local list, filtered
/// by the viewer the caller sent — so a test of the cluster statements exercises the same rule the
/// wire does.
///
/// <para>A member that is not registered fails the way an unreachable host fails, with a thrown
/// exception, so the caller's error-row path is exercised too.</para>
/// </summary>
public sealed class InProcessClusterActivityTransport : IClusterActivityTransport
{
    private readonly InProcessClusterNodes nodes;

    public InProcessClusterActivityTransport(InProcessClusterNodes nodes)
    {
        this.nodes = nodes;
    }

    public Task<List<QueryActivityRow>> ListQueriesAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken)
        => nodes.Get(targetRaftEndpoint).Executor.QueryActivityService.ListQueriesAsync(cluster: false, viewer, cancellationToken);

    public Task<List<ConnectionActivityRow>> ListConnectionsAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken)
        => nodes.Get(targetRaftEndpoint).Executor.QueryActivityService.ListConnectionsAsync(cluster: false, viewer, cancellationToken);

    public Task<QueryCancelOutcome> CancelAsync(string targetRaftEndpoint, string queryId, ActivityViewer viewer, CancellationToken cancellationToken)
        => Task.FromResult(nodes.Get(targetRaftEndpoint).Executor.QueryActivity.TryCancel(queryId, viewer));
}
