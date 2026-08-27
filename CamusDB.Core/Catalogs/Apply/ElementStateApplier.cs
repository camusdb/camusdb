
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
/// <c>Absent -> DeleteOnly -> WriteOnly -> Public</c>.
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
                notNullConstraintName: current.NotNullConstraintName
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
                comment: current.Comment
            );
        }

        // TableSchema.Version is intentionally NOT bumped: indexes are not part of the
        // row encoding, so index state changes are invisible to the row decoder.
        return tableSchema;
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
