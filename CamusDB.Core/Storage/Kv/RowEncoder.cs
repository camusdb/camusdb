
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Thin wrapper over <see cref="Serializator"/> that encodes/decodes a full row
/// (all columns + embedded rowId) as a <c>byte[]</c> for storage as a Kahuna KV value.
///
/// Wire format is identical to the one produced by <c>RowSerializer</c> and consumed
/// by <c>RowDeserializer</c> so existing data written by the old storage layer can be
/// read back without migration.
///
/// Layout:
///   [TypeInteger32 marker][4-byte schema version]
///   [TypeInteger32 marker][12-byte ObjectId rowId]
///   per column in schema.Columns order:
///     TypeNull (1 byte)                     — null / absent
///     TypeId   + 12-byte ObjectId           — ColumnType.Id
///     TypeInteger64 + 8-byte int64          — ColumnType.Integer64
///     TypeString32  + 4-byte len + UTF-16   — ColumnType.String
///     TypeDouble    + 8-byte double         — ColumnType.Float64
///     TypeBool low-nibble in same byte      — ColumnType.Bool
/// </summary>
public static class RowEncoder
{
    private enum ColumnVisibility
    {
        PublicOnly,
        Writable
    }

    public static byte[] Encode(TableSchema schema, Dictionary<string, ColumnValue> row, ObjectIdValue rowId)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(row);

        int length = CalculateBufferLength(schema, row);
        byte[] buffer = new byte[length];
        int pointer = 0;

        Serializator.WriteType(buffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(buffer, schema.Version, ref pointer);

        Serializator.WriteType(buffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteObjectId(buffer, rowId, ref pointer);

        List<TableColumnSchema> columns = schema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!SchemaElementStateRules.IsWritable(column))
            {
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                continue;
            }

            if (!row.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                continue;
            }

            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeId, ref pointer);
                    Serializator.WriteObjectId(buffer, ObjectId.ToValue(columnValue.StrValue!), ref pointer);
                    break;

                case ColumnType.Integer64:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeInteger64, ref pointer);
                    Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                    break;

                case ColumnType.String:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeString32, ref pointer);
                    Serializator.WriteString(buffer, columnValue.StrValue!, ref pointer);
                    break;

                case ColumnType.Float64:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeDouble, ref pointer);
                    Serializator.WriteDouble(buffer, columnValue.FloatValue, ref pointer);
                    break;

                case ColumnType.Bool:
                    Serializator.WriteBool(buffer, columnValue.BoolValue, ref pointer);
                    break;

                case ColumnType.Null:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type);
            }
        }

        return buffer;
    }

    public static Dictionary<string, ColumnValue> Decode(
        TableSchema schema,
        ObjectIdValue rowId,
        byte[] data,
        IReadOnlySet<string>? requiredColumns = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer);                    // schema type marker
        int schemaVersion = Serializator.ReadInt32(data, ref pointer);

        Serializator.ReadType(data, ref pointer);                    // rowId type marker
        Serializator.ReadObjectId(data, ref pointer);                // rowId (it's the KV key — discard)

        List<TableColumnSchema> columns = schema.GetSchemaHistory(schemaVersion).Columns!;
        return DecodeColumns(columns, schema.Columns, data, ref pointer, requiredColumns, ColumnVisibility.PublicOnly);
    }

    public static async ValueTask<Dictionary<string, ColumnValue>> DecodeAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        byte[] data,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer);                    // schema type marker
        int schemaVersion = Serializator.ReadInt32(data, ref pointer);

        Serializator.ReadType(data, ref pointer);                    // rowId type marker
        Serializator.ReadObjectId(data, ref pointer);                // rowId (it's the KV key — discard)

        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = await GetVisibilityColumnsAsync(
            schema,
            txId,
            visibilitySchemaVersion).ConfigureAwait(false);
        return DecodeColumns(columns, visibilityColumns, data, ref pointer, requiredColumns, ColumnVisibility.PublicOnly);
    }

    public static async ValueTask<Dictionary<string, ColumnValue>> DecodeWritableAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        byte[] data,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer);
        int schemaVersion = Serializator.ReadInt32(data, ref pointer);

        Serializator.ReadType(data, ref pointer);
        Serializator.ReadObjectId(data, ref pointer);

        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = await GetVisibilityColumnsAsync(
            schema,
            txId,
            visibilitySchemaVersion).ConfigureAwait(false);
        return DecodeColumns(columns, visibilityColumns, data, ref pointer, requiredColumns, ColumnVisibility.Writable);
    }

    private static async ValueTask<List<TableColumnSchema>?> GetVisibilityColumnsAsync(
        TableSchema schema,
        HLCTimestamp txId,
        long? visibilitySchemaVersion
    )
    {
        if (visibilitySchemaVersion is null)
            return schema.Columns;

        if (visibilitySchemaVersion.Value > int.MaxValue || visibilitySchemaVersion.Value < int.MinValue)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Invalid table schema visibility version {visibilitySchemaVersion.Value}"
            );

        return (await schema.GetSchemaHistoryAsync(txId, (int)visibilitySchemaVersion.Value).ConfigureAwait(false)).Columns;
    }

    private static Dictionary<string, ColumnValue> DecodeColumns(
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        byte[] data,
        ref int pointer,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility)
    {
        bool decodeAll = requiredColumns is null;

        Dictionary<string, ColumnValue> result = new(decodeAll ? columns.Count : requiredColumns!.Count);

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!IsVisible(column, currentColumns, visibility))
            {
                SkipColumnValue(column.Type, data, ref pointer);
                continue;
            }

            if (decodeAll || requiredColumns!.Contains(column.Name))
                result.Add(column.Name, ReadColumnValue(column, data, ref pointer));
            else
                SkipColumnValue(column.Type, data, ref pointer);
        }

        // Columns added after this row was written are absent from the byte stream.
        // Inject their default value (or a typed null) so callers see consistent output.
        if (currentColumns is not null)
        {
            foreach (TableColumnSchema current in currentColumns)
            {
                if (result.ContainsKey(current.Name))
                    continue;

                if (FindCurrentColumn(current, columns) is not null)
                    continue; // present in row schema — was filtered by visibility or requiredColumns, not a new column

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };

                if (!visible)
                    continue;

                if (!decodeAll && !requiredColumns!.Contains(current.Name))
                    continue;

                result[current.Name] = current.DefaultValue ?? new(ColumnType.Null, 0L);
            }
        }

        return result;
    }

    private static bool IsVisible(
        TableColumnSchema column,
        List<TableColumnSchema>? currentColumns,
        ColumnVisibility visibility
    )
    {
        TableColumnSchema? current = FindCurrentColumn(column, currentColumns);
        if (current is null)
            return false;

        return visibility switch
        {
            ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
            ColumnVisibility.Writable => SchemaElementStateRules.IsWritable(current),
            _ => false
        };
    }

    private static TableColumnSchema? FindCurrentColumn(TableColumnSchema historyColumn, List<TableColumnSchema>? currentColumns)
    {
        if (currentColumns is null)
            return null;

        foreach (TableColumnSchema current in currentColumns)
        {
            if (current.Id == historyColumn.Id)
                return current;
        }

        if (string.IsNullOrWhiteSpace(historyColumn.Id))
        {
            foreach (TableColumnSchema current in currentColumns)
            {
                if (current.Name == historyColumn.Name)
                    return current;
            }
        }

        return null;
    }

    private static ColumnValue ReadColumnValue(TableColumnSchema column, byte[] data, ref int pointer)
    {
        switch (column.Type)
        {
            case ColumnType.Id:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeId =>
                        new(ColumnType.Id, Serializator.ReadObjectId(data, ref pointer).ToString()),
                    SerializatorTypes.TypeNull =>
                        new(ColumnType.Null, ""),
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Integer64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeInteger64 =>
                        new(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        new(ColumnType.Null, 0L),
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.String:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeString8 or
                    SerializatorTypes.TypeString16 or
                    SerializatorTypes.TypeString32 =>
                        new(ColumnType.String, Serializator.ReadString(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        new(ColumnType.Null, 0L),
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Float64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDouble =>
                        new(ColumnType.Float64, Serializator.ReadDouble(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        new(ColumnType.Null, 0L),
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Bool:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeBool =>
                        new(ColumnType.Bool, Serializator.ReadBool(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        new(ColumnType.Null, 0L),
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + column.Type);
        }
    }

    private static void SkipColumnValue(ColumnType columnType, byte[] data, ref int pointer)
    {
        switch (columnType)
        {
            case ColumnType.Id:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeId)
                    Serializator.ReadObjectId(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Integer64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeInteger64)
                    Serializator.ReadInt64(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.String:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t is SerializatorTypes.TypeString8 or SerializatorTypes.TypeString16 or SerializatorTypes.TypeString32)
                    Serializator.ReadString(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Float64:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDouble)
                    Serializator.ReadDouble(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Bool:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t is not (SerializatorTypes.TypeBool or SerializatorTypes.TypeNull))
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnType);
        }
    }

    private static int CalculateBufferLength(TableSchema schema, Dictionary<string, ColumnValue> row)
    {
        int length = 20; // header: 1+4 (schema version) + 1+12 (rowId) + 2 padding

        List<TableColumnSchema> columns = schema.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!SchemaElementStateRules.IsWritable(column))
            {
                length += SerializatorTypeSizes.TypeNull;
                continue;
            }

            if (!row.TryGetValue(column.Name, out ColumnValue? columnValue) || columnValue.Type == ColumnType.Null)
            {
                length += SerializatorTypeSizes.TypeNull;
                continue;
            }

            if (column.Type != columnValue.Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownType,
                    $"Type {columnValue.Type} cannot be assigned to {column.Name} ({column.Type})"
                );

            length += columnValue.Type switch
            {
                ColumnType.Id =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeObjectId,
                ColumnType.Integer64 =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.Float64 =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeDouble,
                ColumnType.String =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32
                    + Encoding.Unicode.GetByteCount(columnValue.StrValue!),
                ColumnType.Bool =>
                    SerializatorTypeSizes.TypeBool,
                _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type)
            };
        }

        return length;
    }
}
