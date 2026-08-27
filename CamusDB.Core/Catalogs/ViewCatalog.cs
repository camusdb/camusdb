
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using Kommander.Time;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Creates, replaces, drops, renames and redefines a view, and publishes the state of a
/// materialized view after a refresh.
///
/// <para><b>A rename carries the rewritten bodies of dependent views in the same delta.</b> A view
/// body is bound to what it reads by immutable id, so the rewrite is mechanical — but it has to
/// commit atomically with the rename, or a dependent body is left resolving a name that no longer
/// exists.</para>
///
/// <para><b>Publishing a refreshed materialized view is a compare-and-swap.</b> The delta carries
/// the metadata generation the refresh started against, and apply refuses it if the live generation
/// has moved. Without that guard a slow refresh could adopt its stale contents over a newer
/// one.</para>
///
/// <para>A view owns no rows and no index key-space, so dropping one removes the whole object —
/// there is nothing to detach or retain for recovery, unlike a table.</para>
/// </summary>
internal sealed class ViewCatalog
{
    private readonly SchemaChangePublisher publisher;

    public ViewCatalog(SchemaChangePublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        this.publisher = publisher;
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.SetComment"/> delta and replicates it, so every node's
    /// in-memory schema carries the new comment and the KV checkpoint is rewritten. Advances the
    /// database schema version but not <see cref="TableSchema.Version"/>. Cluster mode only —
    /// standalone goes through <see cref="SetTableCommentAsync"/>.
    ///
    /// <para><paramref name="comment"/> is null to remove the comment; the empty string stores a
    /// present-but-empty one.</para>
    /// </summary>
    /// <summary>Whether a non-materialized view with this name currently exists.</summary>
    internal static bool ViewExists(DatabaseDescriptor database, string viewName)
        => database.Schema.Views.ContainsKey(viewName);

    /// <summary>
    /// Proposes a <see cref="SchemaOp.CreateView"/> (or <see cref="SchemaOp.ReplaceView"/>) delta and
    /// waits for it to apply locally. The view id is allocated by the caller and carried in the
    /// payload so every node assigns the same one.
    /// </summary>
    /// <remarks>
    /// The name-availability check runs <b>under the schema lock, here</b>, in the same critical
    /// section that builds the delta — not in the caller. Checking outside the lock would be a
    /// check-then-act that two concurrent creations both pass.
    /// </remarks>
    internal async Task CreateViewAsync(
        DatabaseDescriptor database,
        string viewId,
        string viewName,
        ViewDefinition definition,
        string? comment,
        bool replace)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (replace)
            {
                if (!database.Schema.Views.ContainsKey(viewName))
                    throw new CamusDBException(
                        CamusDBErrorCodes.ViewDoesntExist,
                        $"View '{viewName}' does not exist");
            }
            else
            {
                database.Schema.RequireRelationNameAvailable(viewName);
            }

            entry = SchemaChangeEntryFactory.CreateViewEntry(database, viewId, viewName, definition, comment, replace);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>Proposes a <see cref="SchemaOp.DropView"/> delta and waits for it to apply locally.</summary>
    internal async Task DropViewAsync(DatabaseDescriptor database, string viewName)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (!database.Schema.Views.ContainsKey(viewName))
                throw new CamusDBException(CamusDBErrorCodes.ViewDoesntExist, $"View '{viewName}' does not exist");

            entry = SchemaChangeEntryFactory.DropViewEntry(database, viewName);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>Proposes a <see cref="SchemaOp.RenameView"/> delta and waits for it to apply locally.</summary>
    internal async Task RenameViewAsync(
        DatabaseDescriptor database, string viewName, string newName,
        Dictionary<string, ViewDefinition>? dependentViews = null)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (!database.Schema.Views.ContainsKey(viewName))
                throw new CamusDBException(CamusDBErrorCodes.ViewDoesntExist, $"View '{viewName}' does not exist");

            database.Schema.RequireRelationNameAvailable(newName);

            entry = SchemaChangeEntryFactory.RenameViewEntry(database, viewName, newName, dependentViews);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.SetViewDefinition"/> delta: overwrites one view's stored body
    /// without the user having issued a <c>CREATE OR REPLACE</c>. Used by the dependent-view rewrite
    /// that follows a base table or column rename.
    /// </summary>
    internal async Task SetViewDefinitionAsync(DatabaseDescriptor database, string viewName, ViewDefinition definition)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.SetViewDefinitionEntry(database, viewName, definition);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.SetMaterializedViewState"/> delta recording a refresh outcome.
    ///
    /// <para>When <paramref name="swapToTableId"/> is given this is the <b>swap</b> half of a
    /// build-and-swap refresh: the materialized view adopts the freshly built key-space in one
    /// replicated change, so no node ever observes a half-built materialized view and no reader is
    /// blocked while the rebuild runs. Passing null instead only updates the flags.</para>
    /// </summary>
    /// <param name="tableId">The materialized view's own relation id. It is unchanged by a swap —
    /// only the storage it addresses moves — so this identifies the view in both cases.</param>
    /// <param name="publishHlc">When this change is being made — not when the rebuild read its
    /// source. It becomes the entry timestamp, and with it the <c>DroppedAt</c> of the storage the
    /// swap retires, so that storage gets its full retention window starting from the moment it
    /// actually stopped being read.</param>
    internal async Task SetMaterializedViewStateAsync(
        DatabaseDescriptor database,
        string tableId,
        bool isPopulated,
        HLCTimestamp? refreshedAt,
        string? swapToTableId = null,
        HLCTimestamp publishHlc = default,
        long? expectedMetadataGeneration = null)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            entry = SchemaChangeEntryFactory.SetMaterializedViewStateEntry(database, tableId, isPopulated, refreshedAt, swapToTableId, publishHlc, expectedMetadataGeneration);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }
}
