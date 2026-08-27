
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that add, drop or rename a secondary index.
///
/// <para><b>An index is identified by its immutable id, never by its name.</b> The key-space of its
/// entries is derived from that id, so a rename touches the schema only — no entry is rewritten and
/// no scan changes shape. Resolving an index by name during apply would break the moment two nodes
/// disagreed about a rename still in flight.</para>
///
/// <para>Dropping an index removes it from the schema but does <b>not</b> free its key-space here.
/// The id stays in the grow-only keyspace catalog so a later database drop can still find and purge
/// entries belonging to an index that no longer appears in any schema.</para>
/// </summary>
internal static class IndexDeltaApplier
{
    /// <summary>
    /// Applies an AddIndex delta. Idempotent: if an entry with the same name already exists
    /// (e.g. the proposer already applied it locally before proposing), it is replaced.
    /// TableSchema.Version is intentionally NOT bumped — see TableSchema.Version doc.
    /// </summary>
    internal static TableSchema ApplyAddIndex(Schema schema, SchemaIndexPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (payload.Index is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"AddIndex payload for '{payload.IndexName}' carries no index definition");

        tableSchema.Indexes ??= [];
        tableSchema.Indexes.RemoveAll(ix => string.Equals(ix.Name, payload.IndexName, StringComparison.OrdinalIgnoreCase));
        tableSchema.Indexes.Add(payload.Index);
        return tableSchema;
    }

    /// <summary>
    /// Applies a DropIndex delta. Idempotent: if the index is already absent the operation
    /// is a no-op. Returns the table even when the index was absent, because the schema
    /// version must still advance and the checkpoint must still be persisted.
    /// </summary>
    internal static TableSchema? ApplyDropIndex(Schema schema, SchemaIndexPayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            return null;

        tableSchema.Indexes?.RemoveAll(ix => string.Equals(ix.Name, payload.IndexName, StringComparison.OrdinalIgnoreCase));
        return tableSchema;
    }

    /// <summary>
    /// Replaces an index entry with a renamed copy, preserving the immutable <c>Id</c>,
    /// <c>ColumnIds</c>, <c>Type</c>, <c>State</c>, and <c>StartOffset</c>.
    /// Does NOT bump <c>TableSchema.Version</c> — indexes are not part of row encoding.
    /// </summary>
    internal static TableSchema ApplyRenameIndex(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (string.IsNullOrEmpty(payload.ElementName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Index name is required for RenameIndex");

        if (tableSchema.Indexes is null || tableSchema.Indexes.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no indexes");

        int idx = tableSchema.Indexes.FindIndex(ix => string.Equals(ix.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{payload.ElementName}' does not exist on table '{payload.TableName}'");

        if (tableSchema.Indexes.Any(ix => string.Equals(ix.Name, payload.NewName, StringComparison.OrdinalIgnoreCase)))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Index '{payload.NewName}' already exists on table '{payload.TableName}'");

        TableIndexSchema current = tableSchema.Indexes[idx];

        if (current.State != SchemaElementState.Public && current.State != SchemaElementState.Absent)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Cannot rename index '{payload.ElementName}': it has an in-flight schema change (state: {current.State}). Wait for it to reach Public before renaming.");

        tableSchema.Indexes[idx] = new TableIndexSchema(
            id: current.Id,
            name: payload.NewName,
            columnIds: current.ColumnIds,
            type: current.Type,
            state: current.State,
            startOffset: current.StartOffset,
            columnDirections: current.ColumnDirections,
            includeColumnIds: current.IncludeColumnIds,
            comment: current.Comment
        );

        // TableSchema.Version is intentionally NOT bumped: indexes are not part of row encoding.
        return tableSchema;
    }
}
