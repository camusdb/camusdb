/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// What the node an acknowledgement was sent to answered. The sender
/// (<c>EmbeddedKahuna.PublishSchemaAckWithRetryAsync</c>) acts on it: only
/// <see cref="Accepted"/> ends the delivery.
/// </summary>
internal enum SchemaAckDelivery
{
    /// <summary>The target leads the schema partition and recorded the ack. Nothing more to send.</summary>
    Accepted,

    /// <summary>
    /// The target does not lead the schema partition. It may have kept the ack for a term it wins
    /// later, but the leader's gate has not seen it: the sender must reach the leader through another
    /// replica. This is the normal answer when a node outside the replica set named the wrong replica,
    /// which it does whenever no gossiped leader hint has formed yet.
    /// </summary>
    NotLeader,

    /// <summary>The ack did not reach the target, or the target failed to process it. The sender resolves the leader again and retries.</summary>
    Failed,
}

/// <summary>
/// Sends a schema-apply acknowledgement from a follower to the schema-partition leader. The
/// leader records the ack in its <see cref="SchemaAckTracker"/> so that
/// <c>WaitForAllLiveAsync</c> observes real follower progress rather than only the local node's
/// own apply.
///
/// <para>An implementation must not throw on a transport failure; it answers
/// <see cref="SchemaAckDelivery.Failed"/> instead, and the sender retries. It must answer
/// <see cref="SchemaAckDelivery.NotLeader"/> when the target refused the ack as a non-leader, so
/// the sender can walk the replica set: a node that does not host the schema partition has no Raft
/// view of its leader and names a replica from the placement view, which is wrong until a gossiped
/// hint forms. A silently recorded ack on the wrong replica never reaches the leader's gate.</para>
/// </summary>
internal interface ISchemaAckSender
{
    /// <param name="leaderEndpoint">Raft endpoint of the node the sender believes leads the schema partition.</param>
    /// <param name="database">Database id whose schema log this ack belongs to.</param>
    /// <param name="nodeEndpoint">Raft endpoint of the node that applied the version.</param>
    /// <param name="schemaVersion">Schema version that was applied.</param>
    Task<SchemaAckDelivery> SendSchemaAckAsync(
        string leaderEndpoint,
        string database,
        string nodeEndpoint,
        long schemaVersion,
        CancellationToken cancellationToken
    );
}
