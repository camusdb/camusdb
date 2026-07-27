/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Applies <c>COMMENT ON TABLE|COLUMN|INDEX</c> against an opened table, splitting on cluster vs
/// standalone the same way <see cref="TableConstraintAlterer"/> does: cluster proposes a schema-log
/// delta, standalone mutates and rewrites the table blob directly.
///
/// <para>Target existence is resolved here, against the opened schema, rather than at validation
/// time — so a comment on a missing column or index fails with the same errors the rest of the DDL
/// surface produces. The one target that is rejected outright is the internal primary-key index:
/// it has no inline <c>KEY … COMMENT</c> form, so a comment on it could never be rendered by
/// <c>SHOW CREATE TABLE</c> and would be silently invisible.</para>
///
/// <para>Serialization with other DDL is the caller's responsibility: it holds
/// <c>DatabaseDescriptor.SchemaDdlSemaphore</c> across both the table resolution and the mutation, so
/// the identity check below cannot be invalidated between validating and applying.</para>
/// </summary>
internal sealed class CommentSetter
{
    internal async Task<bool> Set(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        CommentTicket ticket,
        bool isClusterMode)
    {
        ValidateTargetIsCurrent(database, table, ticket);
        ValidateTargetExists(table, ticket);

        if (isClusterMode)
            await catalogs.ReplicateSetCommentAsync(
                database, table.Name, ticket.Target, ticket.ElementName, ticket.Comment
            ).ConfigureAwait(false);
        else
            await catalogs.SetTableCommentAsync(
                database, table, ticket.Target, ticket.ElementName, ticket.Comment
            ).ConfigureAwait(false);

        // An index comment lives on a copy inside TableDescriptor.Indexes, so a cached descriptor
        // would keep rendering the stale value. The replicated path evicts via SchemaReplicator;
        // standalone has no such hook, so evict here. Table/column comments share the mutated
        // instances and need no eviction.
        if (ticket.Target == CommentTarget.Index)
            database.TableDescriptors.TryRemove(table.Name, out _);

        return true;
    }

    /// <summary>
    /// Confirms the descriptor still names the table the live schema currently holds under that name.
    ///
    /// <para>The replicated payload identifies its target by table <em>name</em>, so a descriptor that
    /// has since been replaced — dropped and recreated, or relinked — would send the comment to a
    /// different physical table that merely reuses the name. Comparing the immutable table id is what
    /// makes the target an object rather than a string. Callers hold the DDL gate, so a pass here
    /// stays true through the mutation.</para>
    /// </summary>
    private static void ValidateTargetIsCurrent(DatabaseDescriptor database, TableDescriptor table, CommentTicket ticket)
    {
        if (!database.Schema.Tables.TryGetValue(table.Name, out TableSchema? live))
            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"Table '{ticket.TableName}' does not exist");

        if (!string.Equals(live.Id, table.Id, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"Table '{ticket.TableName}' was replaced by a different table while the comment was being applied");
    }

    private static void ValidateTargetExists(TableDescriptor table, CommentTicket ticket)
    {
        switch (ticket.Target)
        {
            case CommentTarget.Table:
                return;

            case CommentTarget.Column:
                {
                    bool exists = table.Schema.Columns?.Any(
                        c => string.Equals(c.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase)) == true;

                    if (!exists)
                        throw new CamusDBException(
                            CamusDBErrorCodes.UnknownColumn,
                            $"Column '{ticket.ElementName}' does not exist on table '{table.Name}'");

                    return;
                }

            case CommentTarget.Index:
                {
                    if (string.Equals(ticket.ElementName, CamusDBConfig.PrimaryKeyInternalName, StringComparison.OrdinalIgnoreCase))
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "The primary key index cannot carry a comment: PRIMARY KEY has no inline COMMENT form, " +
                            "so the comment would never appear in SHOW CREATE TABLE. Comment the table instead.");

                    bool exists = table.Schema.Indexes?.Any(
                        ix => string.Equals(ix.Name, ticket.ElementName, StringComparison.OrdinalIgnoreCase)) == true;

                    if (!exists)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            $"Index '{ticket.ElementName}' does not exist on table '{table.Name}'");

                    return;
                }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Comment target '{ticket.Target}' is not handled by the table path");
        }
    }
}
