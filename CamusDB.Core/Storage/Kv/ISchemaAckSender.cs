/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Sends a schema-apply acknowledgement from a follower to the current schema-partition
/// leader. The leader records the ack in its <see cref="SchemaAckTracker"/> so that
/// <c>WaitForAllLiveAsync</c> observes real follower progress rather than only the
/// local node's own apply. Sending is best-effort: implementations must not throw on
/// transport failure; the gate's own timeout is the correctness backstop.
/// </summary>
internal interface ISchemaAckSender
{
    /// <param name="leaderEndpoint">Raft endpoint of the current schema-partition leader.</param>
    /// <param name="database">Database name whose schema log this ack belongs to.</param>
    /// <param name="nodeEndpoint">Raft endpoint of the node that applied the version.</param>
    /// <param name="schemaVersion">Schema version that was applied.</param>
    Task SendSchemaAckAsync(
        string leaderEndpoint,
        string database,
        string nodeEndpoint,
        long schemaVersion,
        CancellationToken cancellationToken
    );
}
