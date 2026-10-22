/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Asks a peer node which extension builds it loaded at boot. The proposer of <c>CREATE EXTENSION</c>
/// and <c>ALTER EXTENSION ... UPDATE</c> asks every live peer before it proposes, and refuses unless
/// each one loads the target build (<see cref="CamusDBErrorCodes.ExtensionClusterMismatch"/>).
///
/// <para><b>Why before the proposal.</b> A committed schema delta must apply on every node, so the
/// apply cannot refuse a node that lacks the file. The check therefore runs on the proposer, against
/// the nodes that answer now. A node that is down when the statement runs is not asked; if it comes
/// back without the file, it still opens the database and reports
/// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> for the statements that use the
/// extension.</para>
///
/// <para>An implementation must not throw on a transport failure; it answers null (unreachable).</para>
/// </summary>
internal interface IExtensionPeerProbe
{
    /// <param name="nodeEndpoint">Raft endpoint of the node to ask.</param>
    /// <returns>The builds the node loaded, or null when it did not answer.</returns>
    Task<IReadOnlyList<LoadedExtensionProbeEntry>?> ProbeLoadedExtensionsAsync(string nodeEndpoint, CancellationToken cancellationToken);
}
