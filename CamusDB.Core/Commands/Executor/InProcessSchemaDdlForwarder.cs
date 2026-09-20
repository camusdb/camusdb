/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// method returns, so the leader's tracker is already updated when its next ack poll runs.</para>
///
/// <para>There is no operation-id de-duplication here, because nothing retries: the call never
/// crosses a wire, so it cannot be delivered twice. The leader executor's own schema-leader check
/// is what stops a forward loop if leadership moves while the ticket is in flight.</para>
/// </summary>
public sealed class InProcessSchemaDdlForwarder : ISchemaDdlForwarder, ISchemaAckSender
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
    /// Hands the follower's applied version to the leader's tracker. A leader that is no longer
    /// registered — it stopped while the ack was on its way — is ignored, because the interface
    /// makes sending best-effort and the gate's timeout is the backstop.
    /// </summary>
    public Task SendSchemaAckAsync(
        string leaderEndpoint,
        string database,
        string nodeEndpoint,
        long schemaVersion,
        CancellationToken cancellationToken)
    {
        if (nodes.TryGet(leaderEndpoint, out InProcessClusterNode? leader))
            leader.Node.RecordRemoteSchemaAck(database, nodeEndpoint, schemaVersion);

        return Task.CompletedTask;
    }

    private ValueTask<CommandExecutor> LeaderAsync(string leaderEndpoint, string databaseName, CancellationToken cancellationToken)
        => resolveLeader(leaderEndpoint, databaseName, cancellationToken);
}
