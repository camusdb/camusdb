
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
/// Applies a COMMENT ON to a relation, a column or an index on the single-node path, and persists it.
///
/// <para><b>The three helpers here are a compensating rollback, and only make sense as a set.</b>
/// The comment is written into the in-memory schema first, then persisted. If the persist fails, the
/// captured snapshot is put back, because the in-memory schema has already been mutated and nothing
/// else will undo it. Reading any one of the three alone hides that pairing, which is why the
/// summary says it here.</para>
///
/// <para>The cluster path does not come through this class: there the comment travels as a
/// replicated delta and is applied on every node by the delta applier. The two must produce the same
/// result, which is why the in-memory mutation is expressed once, in
/// <see cref="ApplyCommentToSchema"/>.</para>
/// </summary>
internal static class TableCommentWriter
{
    /// <summary>
    /// Standalone-mode counterpart of <see cref="ReplicateSetCommentAsync"/>: mutates the in-memory
    /// schema and rewrites the table blob directly, with no schema-log entry. It rides the blob without
    /// bumping <see cref="TableSchema.Version"/>, and does not advance the database schema version
    /// either; only the replicated path does.
    ///
    /// <para><b>Known divergence between the modes.</b> The same statement advances the database schema
    /// version in cluster mode (its delta lands in the central apply) and not here. Nothing currently
    /// keys off the version for comments, so this is latent rather than broken — but it is the same
    /// shape of bug that made table settings invisible to background sweeps, and the fix is the same:
    /// propose the delta in both modes rather than shortcutting one.</para>
    ///
    /// <para>Ordering matters and is not incidental: the mutation must happen before the persist
    /// (which serializes the in-memory schema), the schema lock must be released before the persist
    /// (no replicated write may run under it), and a failed persist/commit must roll the in-memory
    /// value back so memory never runs ahead of what is durable.</para>
    ///
    /// <para>Returns the previous comment's presence so the caller can tell a genuine change from a
    /// no-op; a null <paramref name="comment"/> removes the comment.</para>
    /// </summary>
    internal static async Task SetTableCommentAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        CommentTarget target,
        string? elementName,
        string? comment)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);

        TableSchema? revertSnapshot = null;
        bool mutated = false;
        try
        {
            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                revertSnapshot = CaptureCommentState(table.Schema, target, elementName);

                ApplyCommentToSchema(table.Schema, target, elementName, comment);
                mutated = true;
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            await SchemaMetaStore.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            mutated = false;
        }
        finally
        {
            if (mutated && revertSnapshot is not null)
            {
                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try { RestoreCommentState(table.Schema, target, elementName, revertSnapshot); }
                finally { database.Schema.ReleaseLock(); }
            }
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Captures just enough of <paramref name="tableSchema"/> to undo a comment mutation: the prior
    /// comment value for the targeted element, carried on a throwaway <see cref="TableSchema"/> so
    /// the revert path needs no extra type.
    /// </summary>
    private static TableSchema CaptureCommentState(TableSchema tableSchema, CommentTarget target, string? elementName)
    {
        TableSchema snapshot = new();

        switch (target)
        {
            case CommentTarget.Table:
                snapshot.Comment = tableSchema.Comment;
                break;

            case CommentTarget.Column:
                {
                    TableColumnSchema? col = tableSchema.Columns?.FirstOrDefault(
                        c => string.Equals(c.Name, elementName, StringComparison.OrdinalIgnoreCase));
                    snapshot.Comment = col?.Comment;
                    break;
                }

            case CommentTarget.Index:
                {
                    TableIndexSchema? ix = tableSchema.Indexes?.FirstOrDefault(
                        i => string.Equals(i.Name, elementName, StringComparison.OrdinalIgnoreCase));
                    snapshot.Comment = ix?.Comment;
                    break;
                }
        }

        return snapshot;
    }

    private static void RestoreCommentState(TableSchema tableSchema, CommentTarget target, string? elementName, TableSchema snapshot)
        => ApplyCommentToSchema(tableSchema, target, elementName, snapshot.Comment);

    /// <summary>
    /// The single in-memory comment mutation, shared by the standalone write path and its revert so
    /// the two can never drift. Mirrors <c>TableDeltaApplier.ApplySetComment</c>, which does the same work on the
    /// replicated path.
    /// </summary>
    private static void ApplyCommentToSchema(TableSchema tableSchema, CommentTarget target, string? elementName, string? comment)
    {
        switch (target)
        {
            case CommentTarget.Table:
                tableSchema.Comment = comment;
                break;

            case CommentTarget.Column:
                {
                    int idx = tableSchema.Columns?.FindIndex(
                        c => string.Equals(c.Name, elementName, StringComparison.OrdinalIgnoreCase)) ?? -1;

                    if (idx < 0)
                        break;

                    TableColumnSchema old = tableSchema.Columns![idx];
                    tableSchema.Columns[idx] = new TableColumnSchema(
                        id: old.Id,
                        name: old.Name,
                        type: old.Type,
                        notNull: old.NotNull,
                        defaultValue: old.DefaultValue,
                        state: old.State,
                        maxLength: old.MaxLength,
                        arrayElementType: old.ArrayElementType,
                        defaultFunction: old.DefaultFunction,
                        notNullConstraintName: old.NotNullConstraintName,
                        comment: comment
                    );
                    break;
                }

            case CommentTarget.Index:
                {
                    int idx = tableSchema.Indexes?.FindIndex(
                        i => string.Equals(i.Name, elementName, StringComparison.OrdinalIgnoreCase)) ?? -1;

                    if (idx < 0)
                        break;

                    TableIndexSchema old = tableSchema.Indexes![idx];
                    tableSchema.Indexes[idx] = new TableIndexSchema(
                        id: old.Id,
                        name: old.Name,
                        columnIds: old.ColumnIds,
                        type: old.Type,
                        state: old.State,
                        startOffset: old.StartOffset,
                        columnDirections: old.ColumnDirections,
                        includeColumnIds: old.IncludeColumnIds,
                        comment: comment
                    );
                    break;
                }
        }
    }
}
