
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
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

            // Per-cell access (not queryRow.Values): a slot- or view-backed row serializes each projected
            // cell straight from its ValueSlot — the sink reads a cell exactly once, so materializing a
            // ColumnValue for it would be a pure allocation. Cells the slot seam declines (eager rows,
            // already-cached cells, injected defaults) fall back to the ColumnValue path.
            for (int i = 0; i < schema.Count; i++)
            {
                int ord = ordinals![i];
                if (ord >= 0 && queryRow.TryGetSlot(ord, out ValueSlot slot))
                    WriteSlot(writer, in slot);
                else
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
                if (value.BytesValue is null)
                    writer.WriteNullValue();
                else
                    WriteBase64String(writer, value.BytesValue);
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

    /// <summary>
    /// Maximum base64 character count encoded into a stack buffer. 256 chars (512 bytes of stack)
    /// covers every payload up to 192 bytes, which is the common case for a Bytes cell.
    /// </summary>
    private const int Base64StackScratchChars = 256;

    /// <summary>
    /// Writes a byte payload as a base64 JSON string, encoding into short-lived scratch storage
    /// instead of allocating an intermediate <see cref="string"/> per cell — a stack buffer for a
    /// small payload, a pooled buffer (returned in a <c>finally</c>) for a large one. The scratch is
    /// owned by this call and never escapes it, so concurrent responses cannot share it.
    /// <para>
    /// The span overload of <see cref="Utf8JsonWriter.WriteStringValue(ReadOnlySpan{char})"/> is
    /// deliberate: like the <see cref="string"/> overload it routes the base64 text through the
    /// writer's JSON encoder, so the default web encoder escapes a base64 <c>'+'</c> exactly as the
    /// original object-graph path did. <c>Utf8JsonWriter.WriteBase64StringValue</c> must not be used
    /// here — it emits the base64 alphabet raw and would silently change the wire bytes.
    /// </para>
    /// </summary>
    private static void WriteBase64String(Utf8JsonWriter writer, ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            writer.WriteStringValue(ReadOnlySpan<char>.Empty);
            return;
        }

        // Computed in 64 bits: the char count of a payload larger than about 1.6 GB does not fit in an
        // int. Such a payload cannot be encoded into a span at all, so it falls back to the string form,
        // which reports the failure the same way the previous code did.
        long charCount = (((long)bytes.Length + 2) / 3) * 4;

        if (charCount > int.MaxValue)
        {
            writer.WriteStringValue(Convert.ToBase64String(bytes));
            return;
        }

        if (charCount <= Base64StackScratchChars)
        {
            // Sized to the payload, not to the cap: the stack buffer is zero-initialized, so asking
            // for the maximum would clear bytes this cell never writes. The cap still bounds it.
            Span<char> scratch = stackalloc char[(int)charCount];

            if (Convert.TryToBase64Chars(bytes, scratch, out int stackWritten))
            {
                writer.WriteStringValue(scratch[..stackWritten]);
                return;
            }

            writer.WriteStringValue(Convert.ToBase64String(bytes));
            return;
        }

        char[] rented = ArrayPool<char>.Shared.Rent((int)charCount);

        try
        {
            if (Convert.TryToBase64Chars(bytes, rented, out int written))
                writer.WriteStringValue(rented.AsSpan(0, written));
            else
                writer.WriteStringValue(Convert.ToBase64String(bytes));
        }
        finally
        {
            ArrayPool<char>.Shared.Return(rented);
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

    /// <summary>
    /// Writes a single <see cref="ValueSlot"/> as its compact-raw JSON token, allocating no
    /// <see cref="ColumnValue"/> for the cell. Token-for-token identical to
    /// <see cref="WriteValue(Utf8JsonWriter, ColumnValue?)"/> over <see cref="ValueSlot.ToColumnValue"/>'s
    /// result — every case below must stay in lockstep with that method, including the shared
    /// <see cref="WriteBase64String"/> path for a Bytes cell and its escaping contract.
    /// </summary>
    internal static void WriteSlot(Utf8JsonWriter writer, in ValueSlot slot)
    {
        switch (slot.Type)
        {
            case ColumnType.Null:
                writer.WriteNullValue();
                break;

            case ColumnType.Id:
            case ColumnType.String:
                writer.WriteStringValue(slot.AsString);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                writer.WriteNumberValue(slot.AsLong);
                break;

            case ColumnType.Bool:
                writer.WriteBooleanValue(slot.AsBool);
                break;

            case ColumnType.Float64:
                writer.WriteNumberValue(slot.AsDouble);
                break;

            case ColumnType.Float32:
                writer.WriteNumberValue((float)slot.AsDouble);
                break;

            case ColumnType.Bytes:
                if (slot.AsBytes is { } bytes)
                    WriteBase64String(writer, bytes);
                else
                    writer.WriteNullValue();
                break;

            case ColumnType.Uuid:
                writer.WriteStartArray();
                writer.WriteNumberValue(slot.UuidHigh);
                writer.WriteNumberValue(slot.UuidLow);
                writer.WriteEndArray();
                break;

            case ColumnType.Array:
            {
                writer.WriteStartArray();
                ValueSlot[] elements = slot.ArrayElements;
                for (int i = 0; i < elements.Length; i++)
                    WriteSlot(writer, in elements[i]);
                writer.WriteEndArray();
                break;
            }

            default:
                writer.WriteNullValue();
                break;
        }
    }
}
