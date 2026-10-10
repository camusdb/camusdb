/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Node-to-node channel for the cluster forms of the activity statements: it asks one peer for its
/// running statements or its connections, and asks it to cancel one of its statements. The target is
/// addressed by its Raft endpoint, as the other node-to-node channels do; each implementation owns
/// the mapping to its own wire address.
///
/// <para><b>The peer applies the visibility rule, not the caller.</b> The caller sends the
/// <see cref="ActivityViewer"/> it resolved, and the peer filters its own rows with it, so another
/// user's SQL text never leaves the node that holds it. The channel is authenticated by the cluster's
/// node secret, which is what lets the peer trust the viewer it receives.</para>
///
/// <para>Contract for implementations: no transparent retries, and every failure surfaces as a
/// thrown exception. The caller decides what a failure means: a list reports it as an error row, a
/// cancel as <see cref="CamusDBErrorCodes.QueryOwnerUnreachable"/>.</para>
/// </summary>
public interface IClusterActivityTransport
{
    /// <summary>The running statements of <paramref name="targetRaftEndpoint"/> that <paramref name="viewer"/> may see.</summary>
    Task<List<QueryActivityRow>> ListQueriesAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken);

    /// <summary>The open connections of <paramref name="targetRaftEndpoint"/> that <paramref name="viewer"/> may see.</summary>
    Task<List<ConnectionActivityRow>> ListConnectionsAsync(string targetRaftEndpoint, ActivityViewer viewer, CancellationToken cancellationToken);

    /// <summary>Asks <paramref name="targetRaftEndpoint"/> to cancel its statement <paramref name="queryId"/>.</summary>
    Task<QueryCancelOutcome> CancelAsync(string targetRaftEndpoint, string queryId, ActivityViewer viewer, CancellationToken cancellationToken);
}
