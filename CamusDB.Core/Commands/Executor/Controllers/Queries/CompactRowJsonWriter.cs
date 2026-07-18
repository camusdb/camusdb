
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Streams query result rows to a <see cref="Utf8JsonWriter"/> in the same compact-raw positional
/// form as <see cref="CompactRowEncoder"/>, but without building the intermediate <c>object?[]</c>
/// graph or boxing any scalar. Each row is written as a JSON array aligned to the response column
/// schema; the token produced for a given <see cref="ColumnValue"/> is byte-identical to serializing
/// the object returned by <see cref="CompactRowEncoder.EncodeValue"/> — that method remains the
/// canonical reference the codec tests assert against.
///
/// <para>
/// Values are resolved by schema-column name. When every row in a set shares one
/// <see cref="RowLayout"/> (the common case for a scan or join cursor), the schema-name→ordinal
/// mapping is resolved once via <see cref="WriteRow(Utf8JsonWriter, QueryResultRow, IReadOnlyList{DerivedColumnSchema}, ref RowLayout?, ref int[]?)"/>
/// so the per-cell path is a direct array index rather than a dictionary lookup.
/// </para>
/// </summary>
public static class CompactRowJsonWriter
{
    /// <summary>
    /// Writes one row as a positional JSON array aligned to <paramref name="schema"/>, reusing a
    /// prebound schema-name→ordinal map for as long as consecutive rows share the same
    /// <see cref="RowLayout"/>. <paramref name="boundLayout"/> / <paramref name="ordinals"/> carry that
    /// binding across calls; pass <see langword="null"/> refs on the first call. A row that is not a
    /// layout-backed <see cref="QueryRow"/> falls back to a per-cell dictionary lookup.
    /// </summary>
    public static void WriteRow(
        Utf8JsonWriter writer,
        QueryResultRow row,
        IReadOnlyList<DerivedColumnSchema> schema,
        ref RowLayout? boundLayout,
        ref int[]? ordinals)
    {
        writer.WriteStartArray();

        if (row.Row is QueryRow queryRow)
        {
            // Rebind ordinals only when the layout identity changes (usually never within a set).
            if (!ReferenceEquals(boundLayout, queryRow.Layout))
            {
                boundLayout = queryRow.Layout;
                ordinals ??= new int[schema.Count];
                if (ordinals.Length != schema.Count)
                    ordinals = new int[schema.Count];
                for (int i = 0; i < schema.Count; i++)
                    ordinals[i] = queryRow.Layout.IndexOf(schema[i].Name);
            }

            // Per-cell access (not queryRow.Values): a slot-backed row materializes only the projected
            // cells it is asked for, never the whole row.
            for (int i = 0; i < schema.Count; i++)
            {
                int ord = ordinals![i];
                WriteValue(writer, ord >= 0 ? queryRow.GetColumnValue(ord) : null);
            }
        }
        else
        {
            IReadOnlyDictionary<string, ColumnValue> dict = row.Row;
            for (int i = 0; i < schema.Count; i++)
            {
                dict.TryGetValue(schema[i].Name, out ColumnValue? value);
                WriteValue(writer, value);
            }
        }

        writer.WriteEndArray();
    }

    /// <summary>
    /// Writes a single <see cref="ColumnValue"/> as its compact-raw JSON token. A null value or a
    /// <see cref="ColumnType.Null"/> cell is always JSON null. Kept token-for-token equivalent to
    /// serializing <see cref="CompactRowEncoder.EncodeValue"/>'s result.
    /// </summary>
    public static void WriteValue(Utf8JsonWriter writer, ColumnValue? value)
    {
        if (value is null || value.Type == ColumnType.Null)
        {
            writer.WriteNullValue();
            return;
        }

        switch (value.Type)
        {
            case ColumnType.Id:
            case ColumnType.String:
                writer.WriteStringValue(value.StrValue);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                writer.WriteNumberValue(value.LongValue);
                break;

            case ColumnType.Bool:
                writer.WriteBooleanValue(value.BoolValue);
                break;

            case ColumnType.Float64:
                writer.WriteNumberValue(value.FloatValue);
                break;

            case ColumnType.Float32:
                writer.WriteNumberValue((float)value.FloatValue);
                break;

            case ColumnType.Bytes:
                // Base64 as a STRING value (not WriteBase64StringValue): a plain string routes through
                // the writer's JSON encoder exactly as the previous object-graph path did — the default
                // web encoder escapes a base64 '+' to the + escape. WriteBase64StringValue emits the
                // base64 alphabet raw, which would silently change the wire bytes. This keeps it identical.
                if (value.BytesValue is null)
                    writer.WriteNullValue();
                else
                    writer.WriteStringValue(Convert.ToBase64String(value.BytesValue));
                break;

            case ColumnType.Uuid:
                writer.WriteStartArray();
                writer.WriteNumberValue(value.UuidHigh);
                writer.WriteNumberValue(value.LongValue);
                writer.WriteEndArray();
                break;

            case ColumnType.Array:
                WriteArray(writer, value);
                break;

            default:
                writer.WriteNullValue();
                break;
        }
    }

    private static void WriteArray(Utf8JsonWriter writer, ColumnValue array)
    {
        writer.WriteStartArray();
        if (array.ArrayValues is not null)
        {
            for (int i = 0; i < array.ArrayValues.Count; i++)
                WriteValue(writer, array.ArrayValues[i]);
        }
        writer.WriteEndArray();
    }
}
