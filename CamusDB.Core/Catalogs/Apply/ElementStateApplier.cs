
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
/// Applies a staged element-state transition, driving a column or an index through
/// <c>Absent -> DeleteOnly -> WriteOnly -> Public</c>, and a foreign key through its shorter
/// ladder (see <see cref="ApplyForeignKeyElementState"/>).
///
/// <para><b>The staging exists so the cluster is never in a state where one node writes an element
/// another node cannot read.</b> A node at <c>DeleteOnly</c> removes entries for the element but
/// creates none; a node at <c>WriteOnly</c> maintains it but no query may use it; only at
/// <c>Public</c> is it visible to readers. Skipping a state, or moving two at once, produces exactly
/// the divergence the ladder is built to prevent, which is what
/// <see cref="ValidateElementStateTransition"/> refuses.</para>
///
/// <para>The transition is applied to in-memory schema only. Backfilling the element's data is a
/// separate, committed step that the proposer performs between states — never from here.</para>
/// </summary>
internal static class ElementStateApplier
{
    internal static TableSchema ApplyElementState(Schema schema, SchemaElementStatePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (payload.ElementKind == SchemaElementKind.Index)
            return ApplyIndexElementState(tableSchema, payload);

        if (payload.ElementKind == SchemaElementKind.ForeignKey)
            return ApplyForeignKeyElementState(tableSchema, payload);

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int columnIndex = tableSchema.Columns.FindIndex(column => string.Equals(column.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
        if (columnIndex < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{payload.ElementName}'");

        TableColumnSchema current = tableSchema.Columns[columnIndex];
        ValidateElementStateTransition(current.State, payload.State, payload.ElementName);

        if (current.State == payload.State)
            return tableSchema;

        List<TableColumnSchema> tableColumns = [.. tableSchema.Columns];

        if (payload.State == SchemaElementState.Absent)
        {
            tableColumns.RemoveAt(columnIndex);
        }
        else
        {
            tableColumns[columnIndex] = new(
                current.Id,
                current.Name,
                current.Type,
                current.NotNull,
                current.DefaultValue,
                payload.State,
                maxLength: current.MaxLength,
                arrayElementType: current.ArrayElementType,
                defaultFunction: current.DefaultFunction,
                notNullConstraintName: current.NotNullConstraintName,
                comment: current.Comment,
                storage: current.Storage,
                defaultSequenceId: current.DefaultSequenceId,
                identityAlways: current.IdentityAlways
            );
        }

        tableSchema.Version++;
        tableSchema.Columns = tableColumns;
        tableSchema.SchemaHistory ??= [];
        tableSchema.SchemaHistory.Add(new()
        {
            Version = tableSchema.Version,
            Columns = tableSchema.Columns
        });

        return tableSchema;
    }

    /// <summary>
    /// Applies a <c>SetElementState</c> delta that targets an index. Unlike the column
    /// variant, this does NOT bump <c>tableSchema.Version</c> or write schema history —
    /// indexes are not part of the row encoding so their state changes are invisible to
    /// the row decoder.
    /// </summary>
    internal static TableSchema ApplyIndexElementState(TableSchema tableSchema, SchemaElementStatePayload payload)
    {
        if (tableSchema.Indexes is null || tableSchema.Indexes.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Table '{tableSchema.Name}' has no indexes — cannot apply state transition for '{payload.ElementName}'"
            );

        int indexIdx = tableSchema.Indexes.FindIndex(ix => string.Equals(ix.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
        if (indexIdx < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unknown index '{payload.ElementName}' on table '{tableSchema.Name}'"
            );

        TableIndexSchema current = tableSchema.Indexes[indexIdx];
        ValidateElementStateTransition(current.State, payload.State, payload.ElementName);

        if (current.State == payload.State)
            return tableSchema;

        if (payload.State == SchemaElementState.Absent)
        {
            tableSchema.Indexes.RemoveAt(indexIdx);
        }
        else
        {
            tableSchema.Indexes[indexIdx] = new TableIndexSchema(
                current.Id!,
                current.Name,
                current.ColumnIds,
                current.Type,
                payload.State,
                current.StartOffset,
                columnDirections: current.ColumnDirections,
                includeColumnIds: current.IncludeColumnIds,
                comment: current.Comment,
                ownerConstraintId: current.OwnerConstraintId
            );
        }

        // TableSchema.Version is intentionally NOT bumped: indexes are not part of the
        // row encoding, so index state changes are invisible to the row decoder.
        return tableSchema;
    }

    /// <summary>
    /// Applies a <c>SetElementState</c> delta that targets a foreign key, found by constraint name.
    /// Does not bump <c>tableSchema.Version</c>, for the same reason as the index variant: a
    /// constraint is not part of the row encoding. The published <see cref="ForeignKeyGraph"/> is
    /// rebuilt by the caller after every delta, so the DML of both tables sees the new state at once.
    ///
    /// <para><b>Absent removes the constraint and releases the index it owned.</b> The owned index
    /// stays, as an ordinary index, with <see cref="TableIndexSchema.OwnerConstraintId"/> cleared in
    /// the same delta. An owner id must always name a live constraint; a DROP CONSTRAINT that also
    /// wants the index gone drops it by name in a later step.</para>
    ///
    /// <para>The list is replaced, never edited in place, so a lock-free reader that holds the old
    /// list keeps a consistent one.</para>
    /// </summary>
    internal static TableSchema ApplyForeignKeyElementState(TableSchema tableSchema, SchemaElementStatePayload payload)
    {
        int position = tableSchema.ForeignKeys?.FindIndex(fk => string.Equals(fk.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase)) ?? -1;
        if (position < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unknown foreign key '{payload.ElementName}' on table '{tableSchema.Name}'"
            );

        ForeignKeySchema current = tableSchema.ForeignKeys![position];
        ValidateForeignKeyStateTransition(current.State, payload.State, payload.ElementName);

        if (current.State == payload.State)
            return tableSchema;

        List<ForeignKeySchema> foreignKeys = [.. tableSchema.ForeignKeys];

        if (payload.State == SchemaElementState.Absent)
        {
            foreignKeys.RemoveAt(position);
            ReleaseOwnedIndexes(tableSchema, current.Id);
        }
        else
        {
            foreignKeys[position] = current.WithState(payload.State);
        }

        tableSchema.ForeignKeys = foreignKeys.Count == 0 ? null : foreignKeys;
        return tableSchema;
    }

    /// <summary>
    /// The foreign-key ladder: <c>WriteOnly → Public</c> after validation, and <c>WriteOnly</c> or
    /// <c>Public → Absent</c> in one step. A constraint is never demoted from Public to WriteOnly, and
    /// never passes through <c>DeleteOnly</c>: enforcement either covers every write or none.
    /// </summary>
    internal static void ValidateForeignKeyStateTransition(SchemaElementState current, SchemaElementState next, string constraintName)
    {
        if (current == next)
            return;

        bool valid = (current, next) switch
        {
            (SchemaElementState.WriteOnly, SchemaElementState.Public) => true,
            (SchemaElementState.WriteOnly, SchemaElementState.Absent) => true,
            (SchemaElementState.Public, SchemaElementState.Absent) => true,
            _ => false
        };

        if (!valid)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid state transition for foreign key '{constraintName}': {current} -> {next}"
            );
    }

    private static void ReleaseOwnedIndexes(TableSchema tableSchema, string constraintId)
    {
        if (tableSchema.Indexes is null)
            return;

        for (int i = 0; i < tableSchema.Indexes.Count; i++)
        {
            TableIndexSchema index = tableSchema.Indexes[i];
            if (!string.Equals(index.OwnerConstraintId, constraintId, StringComparison.Ordinal))
                continue;

            tableSchema.Indexes[i] = new TableIndexSchema(
                index.Id,
                index.Name,
                index.ColumnIds,
                index.Type,
                index.State,
                index.StartOffset,
                columnDirections: index.ColumnDirections,
                includeColumnIds: index.IncludeColumnIds,
                comment: index.Comment,
                ownerConstraintId: null
            );
        }
    }

    internal static void ValidateElementStateTransition(
        SchemaElementState current,
        SchemaElementState next,
        string elementName
    )
    {
        if (current == next)
            return;

        bool valid = (current, next) switch
        {
            (SchemaElementState.Absent, SchemaElementState.DeleteOnly) => true,
            (SchemaElementState.DeleteOnly, SchemaElementState.WriteOnly) => true,
            (SchemaElementState.WriteOnly, SchemaElementState.Public) => true,
            (SchemaElementState.Public, SchemaElementState.WriteOnly) => true,
            (SchemaElementState.WriteOnly, SchemaElementState.DeleteOnly) => true,
            (SchemaElementState.DeleteOnly, SchemaElementState.Absent) => true,
            _ => false
        };

        if (!valid)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid state transition for schema element '{elementName}': {current} -> {next}"
            );
    }
}
