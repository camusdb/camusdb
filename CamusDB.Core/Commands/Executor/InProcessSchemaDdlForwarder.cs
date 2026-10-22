/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Picks the executor a forwarded DDL ticket runs on.
/// </summary>
/// <param name="leaderEndpoint">The Raft endpoint the calling node believes leads the schema log.</param>
/// <param name="databaseName">The database the ticket names.</param>
public delegate ValueTask<CommandExecutor> InProcessDdlLeaderResolver(
    string leaderEndpoint,
    string databaseName,
    CancellationToken cancellationToken
);

/// <summary>
/// The in-process twin of <see cref="HttpSchemaDdlForwarder"/>: a follower's DDL ticket is run on
/// the leader's own <see cref="CommandExecutor"/> instead of posted to its HTTP endpoints. It is
/// what a cluster whose members share one process — the cluster test harness, and the browser
/// playground — forwards DDL through.
///
/// <para>It implements <see cref="ISchemaAckSender"/> as well, for the same reason the HTTP
/// forwarder does: a node needs one object for both directions of the schema conversation, and
/// <see cref="EmbeddedKahuna.SetSchemaAckForwarder"/> takes the forwarder and casts it. The ack is
/// delivered by calling the leader's <see cref="EmbeddedKahuna.RecordRemoteSchemaAck"/> before this
/// method returns, so the leader's tracker is already updated when its next ack poll runs. The pull
/// probe reads the asked node's <see cref="EmbeddedKahuna.GetLocalSchemaApplied"/> the same way.</para>
///
/// <para>There is no operation-id de-duplication here, because nothing retries: the call never
/// crosses a wire, so it cannot be delivered twice. The leader executor's own schema-leader check
/// is what stops a forward loop if leadership moves while the ticket is in flight.</para>
/// </summary>
public sealed class InProcessSchemaDdlForwarder : ISchemaDdlForwarder, ISchemaAckSender, IExtensionPeerProbe
{
    private readonly InProcessClusterNodes nodes;

    private readonly InProcessDdlLeaderResolver resolveLeader;

    /// <summary>
    /// Builds a forwarder that runs each ticket on the member holding the Raft endpoint the caller
    /// passed, which is what the HTTP forwarder does with that endpoint.
    /// </summary>
    /// <param name="leaderResolver">
    /// Replaces that lookup. The cluster test harness passes one so that a forwarded ticket waits
    /// for a settled schema leader instead of trusting the endpoint the caller resolved, which can
    /// already be stale under an election.
    /// </param>
    public InProcessSchemaDdlForwarder(InProcessClusterNodes nodes, InProcessDdlLeaderResolver? leaderResolver = null)
    {
        ArgumentNullException.ThrowIfNull(nodes);

        this.nodes = nodes;
        resolveLeader = leaderResolver ?? ((endpoint, _, _) => ValueTask.FromResult(this.nodes.Get(endpoint).Executor));
    }

    public async Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return (await executor.CreateTable(ticket).ConfigureAwait(false)).Success;
    }

    public async Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.AlterTable(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.AlterIndex(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.DropTable(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.TruncateTable(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.RelinkTable(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return await executor.RenameTable(ticket).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return (await executor.AlterConstraint(ticket).ConfigureAwait(false)).Success;
    }

    public async Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        CommandExecutor executor = await LeaderAsync(leader, ticket.DatabaseName, cancellationToken).ConfigureAwait(false);
        return (await executor.Comment(ticket).ConfigureAwait(false)).Success;
    }

    /// <summary>
    /// Hands the follower's applied version to the tracker of the node the caller named, then answers
    /// whether that node leads the schema partition, exactly as the HTTP endpoint does. The record
    /// is kept on a non-leader too: should it win the next election its gate starts with the
    /// follower's progress. A target that is no longer registered, because it stopped while the ack
    /// was on its way, answers <see cref="SchemaAckDelivery.Failed"/> so the sender resolves again.
    /// </summary>
    async Task<SchemaAckDelivery> ISchemaAckSender.SendSchemaAckAsync(
        string leaderEndpoint,
        string database,
        string nodeEndpoint,
        long schemaVersion,
        CancellationToken cancellationToken)
    {
        if (!nodes.TryGet(leaderEndpoint, out InProcessClusterNode? target))
            return SchemaAckDelivery.Failed;

        target.Node.RecordRemoteSchemaAck(database, nodeEndpoint, schemaVersion);

        return await target.Node.AmISchemaLeaderAsync(database, cancellationToken).ConfigureAwait(false)
            ? SchemaAckDelivery.Accepted
            : SchemaAckDelivery.NotLeader;
    }

    /// <summary>
    /// Reads the asked node's own applied version straight from its tracker, exactly what the HTTP
    /// endpoint answers from. A node that is no longer registered counts as unreachable.
    /// </summary>
    Task<SchemaAppliedProbe> ISchemaAckSender.ProbeSchemaAppliedAsync(
        string nodeEndpoint,
        string database,
        CancellationToken cancellationToken)
    {
        if (!nodes.TryGet(nodeEndpoint, out InProcessClusterNode? target))
            return Task.FromResult(SchemaAppliedProbe.Unreachable);

        long? applied = target.Node.GetLocalSchemaApplied(database);

        return Task.FromResult(applied is null ? SchemaAppliedProbe.NotOpen : SchemaAppliedProbe.OpenAt(applied.Value));
    }

    /// <summary>
    /// Reads the asked node's loaded extensions straight from its engine, exactly what the HTTP
    /// endpoint answers from. A node that is no longer registered counts as unreachable.
    /// </summary>
    Task<IReadOnlyList<LoadedExtensionProbeEntry>?> IExtensionPeerProbe.ProbeLoadedExtensionsAsync(
        string nodeEndpoint,
        CancellationToken cancellationToken)
    {
        if (!nodes.TryGet(nodeEndpoint, out InProcessClusterNode? target))
            return Task.FromResult<IReadOnlyList<LoadedExtensionProbeEntry>?>(null);

        return Task.FromResult<IReadOnlyList<LoadedExtensionProbeEntry>?>(target.Executor.DescribeLoadedExtensions());
    }

    private ValueTask<CommandExecutor> LeaderAsync(string leaderEndpoint, string databaseName, CancellationToken cancellationToken)
        => resolveLeader(leaderEndpoint, databaseName, cancellationToken);
}
