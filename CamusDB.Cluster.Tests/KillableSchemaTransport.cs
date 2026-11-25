/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The schema transport of one start of one node: the shared <see cref="InProcessSchemaDdlForwarder"/> with a
/// switch. A killed node runs on in the test process for as long as something in it waits, and without the
/// switch its late schema statements, deltas and acks would still reach the cluster, which those of a dead
/// process never do. A call after the kill answers as a call that cannot leave the node.
/// </summary>
internal sealed class KillableSchemaTransport : ISchemaDdlForwarder, ISchemaAckSender, IExtensionPeerProbe
{
    private readonly InProcessSchemaDdlForwarder inner;

    private volatile bool killed;

    internal KillableSchemaTransport(InProcessSchemaDdlForwarder inner) => this.inner = inner;

    /// <summary>Set by <see cref="InProcessSchemaCluster.KillNodeAsync"/>; never cleared, because a restart builds a new transport.</summary>
    internal bool IsKilled
    {
        get => killed;
        set => killed = value;
    }

    public Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardCreateTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardAlterTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardAlterIndexAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardDropTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardRelinkTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardTruncateTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardRenameTableAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardAlterConstraintAsync(leader, ticket, operationId, cancellationToken);

    public Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<bool?>(null) : inner.ForwardCommentAsync(leader, ticket, operationId, cancellationToken);

    public Task<long?> ForwardSchemaDeltaAsync(string leader, SchemaDeltaForward delta, string operationId, CancellationToken cancellationToken) =>
        killed ? Task.FromResult<long?>(null) : inner.ForwardSchemaDeltaAsync(leader, delta, operationId, cancellationToken);

    Task<SchemaAckDelivery> ISchemaAckSender.SendSchemaAckAsync(
        string leaderEndpoint, string database, string nodeEndpoint, long schemaVersion, CancellationToken cancellationToken) =>
        killed
            ? Task.FromResult(SchemaAckDelivery.Failed)
            : ((ISchemaAckSender)inner).SendSchemaAckAsync(leaderEndpoint, database, nodeEndpoint, schemaVersion, cancellationToken);

    Task<SchemaAppliedProbe> ISchemaAckSender.ProbeSchemaAppliedAsync(
        string nodeEndpoint, string database, CancellationToken cancellationToken) =>
        killed
            ? Task.FromResult(SchemaAppliedProbe.Unreachable)
            : ((ISchemaAckSender)inner).ProbeSchemaAppliedAsync(nodeEndpoint, database, cancellationToken);

    Task<IReadOnlyList<LoadedExtensionProbeEntry>?> IExtensionPeerProbe.ProbeLoadedExtensionsAsync(
        string nodeEndpoint, CancellationToken cancellationToken) =>
        killed
            ? Task.FromResult<IReadOnlyList<LoadedExtensionProbeEntry>?>(null)
            : ((IExtensionPeerProbe)inner).ProbeLoadedExtensionsAsync(nodeEndpoint, cancellationToken);
}
