
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Routes a DDL statement that arrived on a follower to the database's schema leader, and does not
/// return until this node can actually see the result.
///
/// <para><b>Why the wait matters.</b> Forwarding is only half the job: the leader commits the change
/// through the schema log, and this node applies it asynchronously when the entry replicates. A
/// forward that returned as soon as the leader acknowledged would let the caller's very next
/// statement run against a schema that does not yet contain what it just created — a read-your-own-
/// writes violation visible to any client that does DDL then DML. Each operation therefore supplies
/// its own <c>wasApplied</c> predicate, and the forward blocks until the local schema both advances
/// past the pre-forward version <b>and</b> satisfies that predicate.</para>
///
/// <para>The predicates are deliberately specific rather than "the version moved": a concurrent DDL
/// on another table also advances the version, so a version-only check would return on someone
/// else's change. They also encode what "applied" means per operation — an <c>ADD COLUMN</c> counts
/// only once the column reaches <see cref="SchemaElementState.Public"/>, because the intermediate
/// staged states are not yet visible to queries.</para>
///
/// <para>Every method answers <c>null</c> for "not forwarded — this node should execute it locally",
/// which is the standalone case and the this-node-is-leader case alike.</para>
/// </summary>
internal sealed class DdlForwardingCoordinator
{
    private readonly ISchemaDdlForwarder? schemaDdlForwarder;

    private readonly bool isClusterMode;

    internal DdlForwardingCoordinator(ISchemaDdlForwarder? schemaDdlForwarder, bool isClusterMode)
    {
        this.schemaDdlForwarder = schemaDdlForwarder;
        this.isClusterMode = isClusterMode;
    }

    internal Task<bool?> TryForwardCreateTableAsync(DatabaseDescriptor database, CreateTableTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardCreateTableAsync(leader, ticket, opId, ct),
            () => ForwardedCreateTableApplied(database, ticket)
        );

    internal Task<bool?> TryForwardAlterTableAsync(DatabaseDescriptor database, AlterTableTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterTableAsync(leader, ticket, opId, ct),
            () => ForwardedAlterTableApplied(database, ticket)
        );

    internal Task<bool?> TryForwardAlterIndexAsync(DatabaseDescriptor database, AlterIndexTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterIndexAsync(leader, ticket, opId, ct),
            () => ForwardedAlterIndexApplied(database, ticket)
        );

    internal Task<bool?> TryForwardDropTableAsync(DatabaseDescriptor database, DropTableTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardDropTableAsync(leader, ticket, opId, ct),
            () => ForwardedDropTableApplied(database, ticket)
        );

    internal Task<bool?> TryForwardRenameTableAsync(DatabaseDescriptor database, RenameTableTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardRenameTableAsync(leader, ticket, opId, ct),
            () => ForwardedRenameTableApplied(database, ticket)
        );

    internal Task<bool?> TryForwardRelinkTableAsync(DatabaseDescriptor database, RelinkTableTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardRelinkTableAsync(leader, ticket, opId, ct),
            () => ForwardedRelinkTableApplied(database, ticket)
        );

    internal Task<bool?> TryForwardAlterConstraintAsync(DatabaseDescriptor database, AlterConstraintTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardAlterConstraintAsync(leader, ticket, opId, ct),
            () => ForwardedAlterConstraintApplied(database, ticket)
        );

    internal Task<bool?> TryForwardCommentAsync(DatabaseDescriptor database, CommentTicket ticket)
        => TryForwardDdlAsync(
            database,
            (leader, opId, ct) => schemaDdlForwarder!.ForwardCommentAsync(leader, ticket, opId, ct),
            () => ForwardedCommentApplied(database, ticket)
        );

    /// <summary>
    /// Forwards one DDL operation to the schema leader and waits for it to become visible locally.
    /// Returns <c>null</c> when the caller should execute the operation itself: standalone mode, or
    /// this node already being the schema leader.
    /// </summary>
    private async Task<bool?> TryForwardDdlAsync(
        DatabaseDescriptor database,
        Func<string, string, CancellationToken, Task<bool?>> forward,
        Func<bool> wasApplied
    )
    {
        if (!isClusterMode)
            return null;

        // Degraded nodes must not propose or forward DDL — reject immediately so the
        // caller gets a typed "degraded" error rather than a generic "not leader" error.
        if (database.SchemaSubsystemDegraded)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema subsystem for database '{database.Name}' is degraded; DDL proposals are rejected until the node recovers"
            );

        if (await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            return null;

        if (schemaDdlForwarder is null)
        {
            string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"DDL must be executed by schema leader '{leader}' for database '{database.Name}'"
            );
        }

        // One stable id for all retry attempts so a dedup receiver can
        // recognise retransmissions of the same logical operation.
        string operationId = Guid.NewGuid().ToString("N");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            long fromVersion = database.Schema.SchemaVersion;

            for (int attempt = 0; attempt < 3; attempt++)
            {
                string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
                bool? result = await forward(leader, operationId, CancellationToken.None).ConfigureAwait(false);
                if (result is not null)
                {
                    if (result.Value)
                        await WaitForForwardedSchemaApplyAsync(database, fromVersion, wasApplied).ConfigureAwait(false);

                    return result;
                }
            }
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Failed to forward DDL to schema leader for database '{database.Name}'"
        );
    }

    /// <summary>
    /// Blocks until the leader's committed change has replicated into this node's in-memory schema.
    /// Both conditions are required: the version must move (so a predicate that was already true
    /// before the forward cannot report success) and the predicate must hold (so an unrelated
    /// concurrent DDL that moved the version cannot either).
    /// </summary>
    private static async Task WaitForForwardedSchemaApplyAsync(DatabaseDescriptor database, long fromVersion, Func<bool> wasApplied)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            if (database.Schema.SchemaVersion > fromVersion && wasApplied())
                return;

            await Task.Delay(25).ConfigureAwait(false);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Timed out waiting for forwarded schema apply for database '{database.Name}' after version {fromVersion}"
        );
    }

    private static bool ForwardedCreateTableApplied(DatabaseDescriptor database, CreateTableTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        // Also wait for all index constraints to be replicated via the schema log.
        // ApplyAddIndex runs after ApplyCreateTable (separate Raft entries), so the table
        // may exist before all its constraints are visible.
        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            if (constraint.Type is ConstraintType.PrimaryKey or ConstraintType.IndexUnique or ConstraintType.IndexMulti)
            {
                if (tableSchema.Indexes?.Any(ix => string.Equals(ix.Name, constraint.Name, StringComparison.OrdinalIgnoreCase)) != true)
                    return false;
            }
        }

        return true;
    }

    private static bool ForwardedAlterTableApplied(DatabaseDescriptor database, AlterTableTicket ticket)
    {
        if (ticket.Operation == AlterTableOperation.RenameColumn)
        {
            // After rename, new name present in any table (table name unchanged).
            return database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts) &&
                   ts.Columns?.Any(c => string.Equals(c.Name, ticket.NewName, StringComparison.OrdinalIgnoreCase)) == true;
        }

        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema))
            return false;

        return ticket.Operation switch
        {
            // A forwarded AddColumn is complete only when the column is Public — intermediate
            // staged states (DeleteOnly, WriteOnly) are not yet visible to queries.
            AlterTableOperation.AddColumn =>
                tableSchema.Columns?.Any(c => string.Equals(c.Name, ticket.Column.Name, StringComparison.OrdinalIgnoreCase) && c.State == SchemaElementState.Public) == true,
            AlterTableOperation.DropColumn =>
                tableSchema.Columns?.Any(c => string.Equals(c.Name, ticket.Column.Name, StringComparison.OrdinalIgnoreCase)) != true,
            _ => false
        };
    }

    private static bool ForwardedAlterIndexApplied(DatabaseDescriptor database, AlterIndexTicket ticket)
    {
        if (ticket.Operation == AlterIndexOperation.RenameIndex)
        {
            return database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts) &&
                   ts.Indexes?.Any(ix => string.Equals(ix.Name, ticket.NewName, StringComparison.OrdinalIgnoreCase)) == true;
        }

        // Check TableSchema.Indexes (the source of truth). Fall back to SystemSchema
        // for nodes that haven't yet applied the migration (legacy path).
        bool existsInSchema = database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? tableSchema) &&
                              tableSchema.Indexes is not null &&
                              tableSchema.Indexes.Any(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.OrdinalIgnoreCase));

        return ticket.Operation switch
        {
            AlterIndexOperation.AddIndex or AlterIndexOperation.AddUniqueIndex or AlterIndexOperation.AddPrimaryKey => existsInSchema,
            AlterIndexOperation.DropIndex or AlterIndexOperation.DropPrimaryKey => !existsInSchema,
            _ => false
        };
    }

    private static bool ForwardedDropTableApplied(DatabaseDescriptor database, DropTableTicket ticket)
    {
        return !database.Schema.Tables.ContainsKey(ticket.TableName)
            && !database.TableDescriptors.ContainsKey(ticket.TableName);
    }

    private static bool ForwardedRenameTableApplied(DatabaseDescriptor database, RenameTableTicket ticket)
    {
        return database.Schema.Tables.ContainsKey(ticket.NewName)
            && !database.Schema.Tables.ContainsKey(ticket.TableName);
    }

    private static bool ForwardedRelinkTableApplied(DatabaseDescriptor database, RelinkTableTicket ticket)
    {
        return database.Schema.Tables.ContainsKey(ticket.NewTableName);
    }

    private static bool ForwardedAlterConstraintApplied(DatabaseDescriptor database, AlterConstraintTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName, out TableSchema? ts))
            return false;

        if (ticket.Operation == AlterConstraintOperation.SetNotNull)
        {
            TableColumnSchema? col = ts.Columns?.FirstOrDefault(c => string.Equals(c.Name, ticket.ColumnName, StringComparison.OrdinalIgnoreCase));
            return col?.NotNull == true;
        }

        if (ticket.Operation == AlterConstraintOperation.DropNotNull)
        {
            TableColumnSchema? col = ts.Columns?.FirstOrDefault(c => string.Equals(c.Name, ticket.ColumnName, StringComparison.OrdinalIgnoreCase));
            return col?.NotNull == false;
        }

        bool constraintExists = ts.CheckConstraints?.Any(c => string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase)) == true;
        return ticket.Operation == AlterConstraintOperation.AddCheck ? constraintExists : !constraintExists;
    }

    /// <summary>
    /// Whether a forwarded <c>COMMENT ON</c> is visible in this node's in-memory schema yet.
    /// Comparison is ordinal and null-aware on purpose: <c>IS NULL</c> (null) and <c>IS ''</c> (empty)
    /// are different outcomes, so treating them as equal would report "applied" for the wrong one.
    /// </summary>
    private static bool ForwardedCommentApplied(DatabaseDescriptor database, CommentTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(ticket.TableName ?? "", out TableSchema? ts))
            return false;

        string? current = ticket.Target switch
        {
            CommentTarget.Table => ts.Comment,
            CommentTarget.Column => ts.Columns?.FirstOrDefault(
                c => string.Equals(c.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase))?.Comment,
            CommentTarget.Index => ts.Indexes?.FirstOrDefault(
                ix => string.Equals(ix.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase))?.Comment,
            _ => null
        };

        return string.Equals(current, ticket.Comment, StringComparison.Ordinal);
    }
}
