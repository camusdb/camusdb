
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that add, drop, rename or alter a column.
///
/// <para><b>Every column change appends a schema-history entry.</b> Rows already on disk were
/// encoded under the previous layout, and the history entry for that version is the only thing
/// that can decode them afterwards. Dropping a column without recording the layout that preceded
/// it makes every older row undecodable — the data is intact and unreadable, which is worse than
/// an error.</para>
///
/// <para>A column is addressed by its immutable id wherever a stored reference points at it, so a
/// rename updates the name and leaves every index, check constraint and view body pointing at the
/// same column.</para>
/// </summary>
internal static class ColumnDeltaApplier
{
    internal static TableSchema ApplyAlterColumn(Schema schema, SchemaAlterColumnPayload payload, SchemaOp op)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        tableSchema.Version++;

        switch (op)
        {
            case SchemaOp.AddColumn:
                AddColumn(tableSchema, payload.Column);
                break;

            case SchemaOp.DropColumn:
                DropColumn(tableSchema, payload.Column.Name);
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{op}'");
        }

        TableSchemaHistory schemaHistory = new()
        {
            Version = tableSchema.Version,
            Columns = tableSchema.Columns,
        };

        tableSchema.SchemaHistory ??= [];
        tableSchema.SchemaHistory.Add(schemaHistory);

        return tableSchema;
    }

    /// <summary>
    /// Replaces a column record in the table's column list, preserving the immutable <c>Id</c>.
    /// Bumps <c>TableSchema.Version</c> and records a history entry so pins re-validate and
    /// the row decoder picks up the new name on next access.
    /// </summary>
    internal static TableSchema ApplyRenameColumn(Schema schema, SchemaRenamePayload payload)
    {
        if (!schema.Tables.TryGetValue(payload.TableName, out TableSchema? tableSchema))
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{payload.TableName}' does not exist");

        if (string.IsNullOrEmpty(payload.ElementName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Column name is required for RenameColumn");

        if (tableSchema.Columns is null)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Table '{payload.TableName}' has no columns");

        int idx = tableSchema.Columns.FindIndex(c => string.Equals(c.Name, payload.ElementName, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Column '{payload.ElementName}' does not exist in table '{payload.TableName}'");

        if (tableSchema.Columns.Any(c => string.Equals(c.Name, payload.NewName, StringComparison.OrdinalIgnoreCase)))
            throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, $"Column '{payload.NewName}' already exists in table '{payload.TableName}'");

        TableColumnSchema current = tableSchema.Columns[idx];

        if (current.State != SchemaElementState.Public && current.State != SchemaElementState.Absent)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"Cannot rename column '{payload.ElementName}': it has an in-flight schema change (state: {current.State}). Wait for it to reach Public before renaming.");

        List<TableColumnSchema> columns = [.. tableSchema.Columns];
        columns[idx] = new TableColumnSchema(
            id: current.Id,
            name: payload.NewName,
            type: current.Type,
            notNull: current.NotNull,
            defaultValue: current.DefaultValue,
            state: current.State,
            maxLength: current.MaxLength,
            arrayElementType: current.ArrayElementType,
            defaultFunction: current.DefaultFunction,
            notNullConstraintName: current.NotNullConstraintName,
            comment: current.Comment
        );

        tableSchema.Version++;
        tableSchema.Columns = columns;
        tableSchema.SchemaHistory ??= [];

        // Rename is purely a label change: positional encoding means every historical snapshot
        // must also reflect the new name so rows written under any previous version decode correctly.
        foreach (TableSchemaHistory history in tableSchema.SchemaHistory)
        {
            if (history.Columns is null) continue;
            int hIdx = history.Columns.FindIndex(c => c.Id == current.Id);
            if (hIdx < 0) continue;
            TableColumnSchema hCol = history.Columns[hIdx];
            history.Columns[hIdx] = new TableColumnSchema(
                id: hCol.Id, name: payload.NewName, type: hCol.Type,
                notNull: hCol.NotNull, defaultValue: hCol.DefaultValue, state: hCol.State,
                maxLength: hCol.MaxLength, arrayElementType: hCol.ArrayElementType,
                defaultFunction: hCol.DefaultFunction,
                notNullConstraintName: hCol.NotNullConstraintName,
                comment: hCol.Comment);
        }

        tableSchema.SchemaHistory.Add(new() { Version = tableSchema.Version, Columns = tableSchema.Columns });

        // Storage parameters that name a column store the NAME, not the immutable column id, so a
        // rename would otherwise leave ttl_expiration_expression pointing at a column that no longer
        // exists — and a dangling TTL configuration fails only in the background sweep, silently.
        // Carrying it across keeps the rename a pure label change from the user's point of view.
        if (tableSchema.Settings is not null &&
            tableSchema.Settings.TryGetValue(TableSettings.TtlExpirationExpressionKey, out string? ttlColumn) &&
            string.Equals(ttlColumn, payload.ElementName, StringComparison.OrdinalIgnoreCase))
        {
            Dictionary<string, string> settings = new(tableSchema.Settings, StringComparer.Ordinal)
            {
                [TableSettings.TtlExpirationExpressionKey] = payload.NewName
            };
            tableSchema.Settings = settings;
        }

        return tableSchema;
    }

    internal static void AddColumn(TableSchema tableSchema, SchemaColumnPayload newColumn)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (string.Equals(newColumn.Name, column.Name, StringComparison.OrdinalIgnoreCase))
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.DuplicateColumn, $"Duplicate column '{newColumn.Name}'");

        tableColumns.Add(
            new TableColumnSchema(
                id: string.IsNullOrWhiteSpace(newColumn.Id) ? ObjectIdGenerator.Generate().ToString() : newColumn.Id,
                name: newColumn.Name,
                type: newColumn.Type,
                notNull: newColumn.NotNull,
                defaultValue: newColumn.DefaultValue,
                state: newColumn.State,
                maxLength: newColumn.MaxLength,
                arrayElementType: newColumn.ArrayElementType,
                defaultFunction: newColumn.DefaultFunction,
                notNullConstraintName: newColumn.NotNullConstraintName,
                comment: newColumn.Comment
            )
        );

        tableSchema.Columns = tableColumns;
    }

    internal static void DropColumn(TableSchema tableSchema, string columnName)
    {
        bool hasColumn = false;

        List<TableColumnSchema> tableColumns = new(tableSchema.Columns!.Count);

        foreach (TableColumnSchema column in tableSchema.Columns!)
        {
            if (columnName == column.Name)
                hasColumn = true;
            else
                tableColumns.Add(column);
        }

        if (!hasColumn)
            throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{columnName}'");

        tableSchema.Columns = tableColumns;
    }
}
