
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Serializer;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Thin wrapper over <see cref="Serializator"/> that encodes/decodes a full row
/// (all columns + embedded rowId) as a <c>byte[]</c> for storage as a Kahuna KV value.
///
/// Wire format is identical to the one produced by the original storage layer so existing
/// data written before this codec can be read back without migration.
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

    public static byte[] Encode(TableSchema schema, IReadOnlyDictionary<string, ColumnValue> row, ObjectIdValue rowId)
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

                case ColumnType.Float32:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeFloat, ref pointer);
                    Serializator.WriteFloat(buffer, (float)columnValue.FloatValue, ref pointer);
                    break;

                case ColumnType.Date:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeDate, ref pointer);
                    Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                    break;

                case ColumnType.DateTime:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeDateTime, ref pointer);
                    Serializator.WriteInt64(buffer, columnValue.LongValue, ref pointer);
                    break;

                case ColumnType.Bytes:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeBytes, ref pointer);
                    Serializator.WriteBytesPayload(buffer, columnValue.BytesValue ?? [], ref pointer);
                    break;

                case ColumnType.Uuid:
                    Serializator.WriteType(buffer, SerializatorTypes.TypeUuid, ref pointer);
                    Serializator.WriteUuid(buffer, columnValue.UuidHigh, columnValue.LongValue, ref pointer);
                    break;

                case ColumnType.Array:
                {
                    IReadOnlyList<ColumnValue> elements = columnValue.ArrayValues ?? [];
                    Serializator.WriteType(buffer, SerializatorTypes.TypeArray32, ref pointer);
                    Serializator.WriteInt32(buffer, elements.Count, ref pointer);
                    buffer[pointer++] = (byte)columnValue.ArrayElementType;
                    foreach (ColumnValue el in elements)
                        WriteArrayElement(buffer, el, ref pointer);
                    break;
                }

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
        return DecodeColumns(
            columns,
            schema.Columns,
            data,
            ref pointer,
            requiredColumns,
            ColumnVisibility.PublicOnly,
            injectMissingCurrentColumns: false);
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
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
        return DecodeColumns(
            columns,
            visibilityColumns,
            data,
            ref pointer,
            requiredColumns,
            ColumnVisibility.PublicOnly,
            injectMissingCurrentColumns: visibilitySchemaVersion is not null);
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
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);
        return DecodeColumns(
            columns,
            visibilityColumns,
            data,
            ref pointer,
            requiredColumns,
            ColumnVisibility.Writable,
            injectMissingCurrentColumns: visibilitySchemaVersion is not null);
    }

    /// <summary>
    /// Decodes a stored row byte buffer into a layout-backed <see cref="QueryRow"/> rather than
    /// a plain dictionary. Values are stored in an ordinal <c>ColumnValue[]</c>, allowing O(1)
    /// access by position.
    ///
    /// <para>
    /// <paramref name="layoutCache"/> is a per-scan memoisation table keyed by the stored schema
    /// version embedded in each row's bytes. Because <paramref name="requiredColumns"/> and
    /// <paramref name="visibilitySchemaVersion"/> are constant for the life of a single scan, every
    /// row written under the same stored version produces the same <see cref="RowLayout"/>. Passing
    /// a non-null dictionary causes the layout to be built once and reused for all subsequent rows
    /// with that stored version — the common case is one entry, built on the first decoded row.
    /// Pass <see langword="null"/> when a stable per-scan dictionary is unavailable (e.g. one-off
    /// decodes outside a scan loop).
    /// </para>
    ///
    /// <para>
    /// The <see cref="QueryRow"/> implements <see cref="IReadOnlyDictionary{TKey,TValue}"/>, so
    /// unmigrated consumers that access values by string key continue to work correctly during the
    /// incremental pipeline migration.
    /// </para>
    ///
    /// <para>
    /// Output is value-identical to <see cref="DecodeAsync"/>: same column names, same values, for
    /// the same byte buffer and schema parameters.
    /// </para>
    /// </summary>
    public static async ValueTask<QueryRow> DecodeToQueryRowAsync(
        TableSchema schema,
        HLCTimestamp txId,
        ObjectIdValue rowId,
        byte[] data,
        IReadOnlySet<string>? requiredColumns = null,
        long? visibilitySchemaVersion = null,
        Dictionary<int, RowLayout>? layoutCache = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(data);

        int pointer = 0;

        Serializator.ReadType(data, ref pointer);                    // schema type marker
        int schemaVersion = Serializator.ReadInt32(data, ref pointer);

        Serializator.ReadType(data, ref pointer);                    // rowId type marker
        Serializator.ReadObjectId(data, ref pointer);                // rowId (it's the KV key — discard)

        List<TableColumnSchema> columns = (await schema.GetSchemaHistoryAsync(txId, schemaVersion).ConfigureAwait(false)).Columns!;
        List<TableColumnSchema>? visibilityColumns = visibilitySchemaVersion is null
            ? columns
            : await GetVisibilityColumnsAsync(schema, txId, visibilitySchemaVersion.Value).ConfigureAwait(false);

        bool injectMissing = visibilitySchemaVersion is not null;

        // Resolve or build the layout for this stored schema version. The layout is identical for
        // every row at the same stored version when requiredColumns and visibilitySchemaVersion
        // are constant (which they always are within a single scan).
        RowLayout layout;
        if (layoutCache is null || !layoutCache.TryGetValue(schemaVersion, out layout!))
        {
            layout = BuildRowLayout(columns, visibilityColumns, requiredColumns, ColumnVisibility.PublicOnly, injectMissing);
            layoutCache?.Add(schemaVersion, layout);
        }

        return DecodeColumnsToQueryRow(
            layout,
            columns,
            visibilityColumns,
            data,
            ref pointer,
            requiredColumns,
            ColumnVisibility.PublicOnly,
            injectMissing,
            rowId);
    }

    /// <summary>
    /// Builds the <see cref="RowLayout"/> for a given combination of history columns, visibility
    /// columns, required-column filter, and visibility mode. This is the first pass of the decode:
    /// it determines which column names will appear in the output and in what order, without
    /// touching the byte buffer. The result can be cached and reused for every row that shares the
    /// same stored schema version within a scan.
    /// </summary>
    private static RowLayout BuildRowLayout(
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns)
    {
        bool decodeAll = requiredColumns is null;

        List<string> outputNames = new(columns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];
            if (IsVisible(column, currentColumns, visibility) &&
                (decodeAll || requiredColumns!.Contains(column.Name)))
                outputNames.Add(column.Name);
        }

        if (injectMissingCurrentColumns && currentColumns is not null)
        {
            // Use a set over outputNames to avoid O(n²) Contains checks.
            HashSet<string> included = new(outputNames, StringComparer.Ordinal);

            foreach (TableColumnSchema current in currentColumns)
            {
                if (included.Contains(current.Name)) continue;
                if (FindCurrentColumn(current, columns) is not null) continue;

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable   => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };
                if (!visible) continue;
                if (!decodeAll && !requiredColumns!.Contains(current.Name)) continue;

                outputNames.Add(current.Name);
            }
        }

        return RowLayout.ForColumns(outputNames);
    }

    private static QueryRow DecodeColumnsToQueryRow(
        RowLayout layout,
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        byte[] data,
        ref int pointer,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns,
        ObjectIdValue rowId)
    {
        bool decodeAll = requiredColumns is null;
        ColumnValue[] values = new ColumnValue[layout.Count];

        // Decode the byte stream, filling each included column's ordinal slot.
        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!IsVisible(column, currentColumns, visibility))
            {
                SkipColumnValue(column.Type, data, ref pointer);
                continue;
            }

            if (decodeAll || requiredColumns!.Contains(column.Name))
            {
                int ord = layout.IndexOf(column.Name);
                if (ord >= 0)
                    values[ord] = ReadColumnValue(column, data, ref pointer);
                else
                    SkipColumnValue(column.Type, data, ref pointer);
            }
            else
                SkipColumnValue(column.Type, data, ref pointer);
        }

        // Inject default values for columns added after this row was written (absent from bytes).
        if (injectMissingCurrentColumns && currentColumns is not null)
        {
            foreach (TableColumnSchema current in currentColumns)
            {
                int ord = layout.IndexOf(current.Name);
                if (ord < 0) continue;
                if (values[ord] is not null) continue; // already decoded from bytes
                if (FindCurrentColumn(current, columns) is not null) continue; // present in history, not a new column

                bool visible = visibility switch
                {
                    ColumnVisibility.PublicOnly => SchemaElementStateRules.IsReadable(current),
                    ColumnVisibility.Writable   => SchemaElementStateRules.IsWritable(current),
                    _ => false
                };
                if (!visible) continue;
                if (!decodeAll && !requiredColumns!.Contains(current.Name)) continue;

                values[ord] = current.DefaultValue ?? ColumnValue.Null;
            }
        }

        return new QueryRow(rowId, layout, values);
    }

    private static async ValueTask<List<TableColumnSchema>?> GetVisibilityColumnsAsync(
        TableSchema schema,
        HLCTimestamp txId,
        long visibilitySchemaVersion
    )
    {
        if (visibilitySchemaVersion > int.MaxValue || visibilitySchemaVersion < int.MinValue)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Invalid table schema visibility version {visibilitySchemaVersion}"
            );

        return (await schema.GetSchemaHistoryAsync(txId, (int)visibilitySchemaVersion).ConfigureAwait(false)).Columns;
    }

    private static Dictionary<string, ColumnValue> DecodeColumns(
        List<TableColumnSchema> columns,
        List<TableColumnSchema>? currentColumns,
        byte[] data,
        ref int pointer,
        IReadOnlySet<string>? requiredColumns,
        ColumnVisibility visibility,
        bool injectMissingCurrentColumns)
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
        if (injectMissingCurrentColumns && currentColumns is not null)
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

                result[current.Name] = current.DefaultValue ?? ColumnValue.Null;
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
                        ColumnValue.Null,
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
                        ColumnValue.Null,
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
                        ColumnValue.Null,
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
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Bool:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeBool =>
                        ColumnValue.FromBool(Serializator.ReadBool(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Float32:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeFloat =>
                        new(ColumnType.Float32, (double)Serializator.ReadFloat(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Date:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDate =>
                        new(ColumnType.Date, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.DateTime:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeDateTime =>
                        new(ColumnType.DateTime, Serializator.ReadInt64(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Bytes:
            {
                int t = Serializator.ReadType(data, ref pointer);
                return t switch
                {
                    SerializatorTypes.TypeBytes =>
                        new(Serializator.ReadBytesPayload(data, ref pointer)),
                    SerializatorTypes.TypeNull =>
                        ColumnValue.Null,
                    _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString())
                };
            }

            case ColumnType.Uuid:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeUuid)
                {
                    (long high, long low) = Serializator.ReadUuid(data, ref pointer);
                    return new ColumnValue(ColumnType.Uuid, high, low);
                }
                if (t == SerializatorTypes.TypeNull)
                    return ColumnValue.Null;
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
            }

            case ColumnType.Array:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeNull)
                    return ColumnValue.Null;
                if (t != SerializatorTypes.TypeArray32)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());

                int count = Serializator.ReadInt32(data, ref pointer);
                ColumnType elementType = (ColumnType)Serializator.ReadInt8(data, ref pointer);
                List<ColumnValue> elements = new(count);
                for (int j = 0; j < count; j++)
                    elements.Add(ReadArrayElement(elementType, data, ref pointer));
                return ColumnValue.FromArray(elementType, elements);
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
                    Serializator.SkipLengthPrefixedPayload(data, ref pointer);
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

            case ColumnType.Float32:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeFloat)
                    pointer += SerializatorTypeSizes.TypeFloat32;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Date:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDate)
                    pointer += SerializatorTypeSizes.TypeInteger64;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.DateTime:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeDateTime)
                    pointer += SerializatorTypeSizes.TypeInteger64;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Bytes:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeBytes)
                    Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Uuid:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeUuid)
                    pointer += SerializatorTypeSizes.TypeUuid;
                else if (t != SerializatorTypes.TypeNull)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                break;
            }

            case ColumnType.Array:
            {
                int t = Serializator.ReadType(data, ref pointer);
                if (t == SerializatorTypes.TypeNull)
                    break;
                if (t != SerializatorTypes.TypeArray32)
                    throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, t.ToString());
                int count = Serializator.ReadInt32(data, ref pointer);
                Serializator.ReadInt8(data, ref pointer); // element type byte — discard
                for (int j = 0; j < count; j++)
                    SkipArrayElement(data, ref pointer);
                break;
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnType);
        }
    }

    // -------------------------------------------------------------------------
    // Array element helpers
    // -------------------------------------------------------------------------

    private static int ArrayElementsSize(IReadOnlyList<ColumnValue> elements)
    {
        int total = 0;
        foreach (ColumnValue el in elements)
        {
            total += el.Type switch
            {
                ColumnType.Null      => 1,
                ColumnType.Bool      => 1,
                ColumnType.Integer64 => 1 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.Float32   => 1 + SerializatorTypeSizes.TypeFloat32,
                ColumnType.Float64   => 1 + SerializatorTypeSizes.TypeDouble,
                ColumnType.Date or ColumnType.DateTime => 1 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.String    => 1 + SerializatorTypeSizes.TypeInteger32 + Encoding.Unicode.GetByteCount(el.StrValue!),
                ColumnType.Bytes     => 1 + SerializatorTypeSizes.TypeInteger32 + (el.BytesValue?.Length ?? 0),
                ColumnType.Uuid      => 1 + SerializatorTypeSizes.TypeUuid,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported in size: " + el.Type)
            };
        }
        return total;
    }

    private static void WriteArrayElement(byte[] buffer, ColumnValue el, ref int pointer)
    {
        switch (el.Type)
        {
            case ColumnType.Null:
                Serializator.WriteType(buffer, SerializatorTypes.TypeNull, ref pointer);
                break;
            case ColumnType.Integer64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeInteger64, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            case ColumnType.Float32:
                Serializator.WriteType(buffer, SerializatorTypes.TypeFloat, ref pointer);
                Serializator.WriteFloat(buffer, (float)el.FloatValue, ref pointer);
                break;
            case ColumnType.Float64:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDouble, ref pointer);
                Serializator.WriteDouble(buffer, el.FloatValue, ref pointer);
                break;
            case ColumnType.Bool:
                Serializator.WriteBool(buffer, el.BoolValue, ref pointer);
                break;
            case ColumnType.String:
                Serializator.WriteType(buffer, SerializatorTypes.TypeString32, ref pointer);
                Serializator.WriteString(buffer, el.StrValue!, ref pointer);
                break;
            case ColumnType.Bytes:
                Serializator.WriteType(buffer, SerializatorTypes.TypeBytes, ref pointer);
                Serializator.WriteBytesPayload(buffer, el.BytesValue ?? [], ref pointer);
                break;
            case ColumnType.Uuid:
                Serializator.WriteType(buffer, SerializatorTypes.TypeUuid, ref pointer);
                Serializator.WriteUuid(buffer, el.UuidHigh, el.LongValue, ref pointer);
                break;
            case ColumnType.Date:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDate, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            case ColumnType.DateTime:
                Serializator.WriteType(buffer, SerializatorTypes.TypeDateTime, ref pointer);
                Serializator.WriteInt64(buffer, el.LongValue, ref pointer);
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Array element type not supported in write: " + el.Type);
        }
    }

    private static ColumnValue ReadArrayElement(ColumnType elementType, byte[] data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        if (t == SerializatorTypes.TypeNull)
            return ColumnValue.Null;

        return elementType switch
        {
            ColumnType.Integer64 => new(ColumnType.Integer64, Serializator.ReadInt64(data, ref pointer)),
            ColumnType.Float32   => new(ColumnType.Float32,   (double)Serializator.ReadFloat(data, ref pointer)),
            ColumnType.Float64   => new(ColumnType.Float64,   Serializator.ReadDouble(data, ref pointer)),
            ColumnType.Bool      => ColumnValue.FromBool(Serializator.ReadBool(data, ref pointer)),
            ColumnType.String    => new(ColumnType.String,    Serializator.ReadString(data, ref pointer)),
            ColumnType.Bytes     => new(Serializator.ReadBytesPayload(data, ref pointer)),
            ColumnType.Uuid      => ReadUuidElement(data, ref pointer),
            ColumnType.Date      => new(ColumnType.Date,      Serializator.ReadInt64(data, ref pointer)),
            ColumnType.DateTime  => new(ColumnType.DateTime,  Serializator.ReadInt64(data, ref pointer)),
            _ => throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Unknown array element type: " + elementType)
        };
    }

    private static ColumnValue ReadUuidElement(byte[] data, ref int pointer)
    {
        (long high, long low) = Serializator.ReadUuid(data, ref pointer);
        return new ColumnValue(ColumnType.Uuid, high, low);
    }

    private static void SkipArrayElement(byte[] data, ref int pointer)
    {
        int t = Serializator.ReadType(data, ref pointer);
        switch (t)
        {
            case SerializatorTypes.TypeNull:
                break;
            case SerializatorTypes.TypeBool:
                break; // value is in the same byte as the type tag — already consumed
            case SerializatorTypes.TypeInteger64:
                pointer += SerializatorTypeSizes.TypeInteger64;
                break;
            case SerializatorTypes.TypeFloat:
                pointer += SerializatorTypeSizes.TypeFloat32;
                break;
            case SerializatorTypes.TypeDouble:
                pointer += SerializatorTypeSizes.TypeDouble;
                break;
            case SerializatorTypes.TypeDate:
            case SerializatorTypes.TypeDateTime:
                pointer += SerializatorTypeSizes.TypeInteger64;
                break;
            case SerializatorTypes.TypeString8:
            case SerializatorTypes.TypeString16:
            case SerializatorTypes.TypeString32:
                Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                break;
            case SerializatorTypes.TypeBytes:
                Serializator.SkipLengthPrefixedPayload(data, ref pointer);
                break;
            case SerializatorTypes.TypeUuid:
                pointer += SerializatorTypeSizes.TypeUuid;
                break;
            default:
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Unknown array element disk type: " + t);
        }
    }

    private static int CalculateBufferLength(TableSchema schema, IReadOnlyDictionary<string, ColumnValue> row)
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
                ColumnType.Float32 =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeFloat32,
                ColumnType.String =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32
                    + Encoding.Unicode.GetByteCount(columnValue.StrValue!),
                ColumnType.Bool =>
                    SerializatorTypeSizes.TypeBool,
                ColumnType.Date or ColumnType.DateTime =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger64,
                ColumnType.Bytes =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32
                    + (columnValue.BytesValue?.Length ?? 0),
                ColumnType.Uuid =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeUuid,
                ColumnType.Array =>
                    SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32 + 1  // type + count + element-type byte
                    + ArrayElementsSize(columnValue.ArrayValues ?? []),
                _ => throw new CamusDBException(CamusDBErrorCodes.UnknownType, "Unknown type " + columnValue.Type)
            };
        }

        return length;
    }
}
