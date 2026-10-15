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
/// Carries the schema-ack conversation between nodes in both directions: a follower pushes its
/// applied version to the schema-partition leader, and a DDL proposer pulls the applied version
/// from a node it has not heard from. The leader records the pushed ack in its
/// <see cref="SchemaAckTracker"/> so that <c>WaitForAllLiveAsync</c> observes real follower
/// progress rather than only the local node's own apply; the pull is
/// <see cref="ProbeSchemaAppliedAsync"/>.
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

    /// <summary>
    /// Asks <paramref name="nodeEndpoint"/> which schema version of <paramref name="database"/> it
    /// has applied, or whether it holds the database open at all. The DDL proposer's gate uses this
    /// pull path next to the pushed acks: it is how the gate learns about a node whose ack never
    /// arrived (a lost send, a leader change that redirected the acks, a node that opened the
    /// database a moment ago) and about a node that Raft activity on the proposer never shows
    /// (under replica placement a node that shares no partition with the proposer). An
    /// implementation must not throw on a transport failure; it answers
    /// <see cref="SchemaAppliedProbeOutcome.Unreachable"/> instead.
    /// </summary>
    /// <param name="nodeEndpoint">Raft endpoint of the node to ask.</param>
    /// <param name="database">Database id whose applied schema version is asked for.</param>
    Task<SchemaAppliedProbe> ProbeSchemaAppliedAsync(
        string nodeEndpoint,
        string database,
        CancellationToken cancellationToken
    );
}

/// <summary>
/// What a node answered when asked for its applied schema version of a database.
/// </summary>
internal enum SchemaAppliedProbeOutcome
{
    /// <summary>The node holds the database open; <see cref="SchemaAppliedProbe.Version"/> is the version it has applied.</summary>
    Open,

    /// <summary>
    /// The node does not hold the database open. It cannot be behind on a schema it does not serve,
    /// so the gate stops waiting for it; a record the gate still held for it is stale and is dropped.
    /// </summary>
    NotOpen,

    /// <summary>The node did not answer. The gate keeps whatever it already knows about the node.</summary>
    Unreachable,
}

/// <summary>
/// The answer to <see cref="ISchemaAckSender.ProbeSchemaAppliedAsync"/>. <see cref="Version"/> is
/// meaningful only when <see cref="Outcome"/> is <see cref="SchemaAppliedProbeOutcome.Open"/>.
/// </summary>
internal readonly record struct SchemaAppliedProbe(SchemaAppliedProbeOutcome Outcome, long Version)
{
    public static SchemaAppliedProbe OpenAt(long version) => new(SchemaAppliedProbeOutcome.Open, version);

    public static readonly SchemaAppliedProbe NotOpen = new(SchemaAppliedProbeOutcome.NotOpen, 0);

    public static readonly SchemaAppliedProbe Unreachable = new(SchemaAppliedProbeOutcome.Unreachable, 0);
}
