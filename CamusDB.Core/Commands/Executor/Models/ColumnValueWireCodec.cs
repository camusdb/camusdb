
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Faithful JSON round-trip for a <see cref="ColumnValue"/>, used when decoded values (rather
/// than raw row bytes) must cross the node boundary — partial aggregation states are the
/// first consumer. Every <see cref="ColumnType"/> is covered; per-type payload fields keep
/// frames compact: <c>{"t":type, "s"|"l"|"f"|"b"|"y"|"u"+"l"|"a"+"e": payload}</c>.
/// Doubles round-trip exactly (System.Text.Json uses shortest round-trippable formatting; ±infinity
/// and NaN, which JSON has no number for, travel as the strings Infinity, -Infinity and NaN) and
/// bytes travel as base64. Arrays recurse. A Uuid and a NUMERIC both travel as their two signed
/// 64-bit halves (<c>"u"</c> high, <c>"l"</c> low); the type tag tells them apart, and the read
/// path range-checks a NUMERIC, so a corrupt frame cannot carry a value past ±(10³⁸−1) unscaled.
/// A jsonb value travels as its cell bytes in base64 (<c>"j"</c>); the read path checks the format
/// version.
/// </summary>
public static class ColumnValueWireCodec
{
    public static void Write(Utf8JsonWriter writer, ColumnValue value)
    {
        writer.WriteStartObject();
        writer.WriteNumber("t", (int)value.Type);

        switch (value.Type)
        {
            case ColumnType.Null:
                break;

            case ColumnType.Bool:
                writer.WriteBoolean("b", value.BoolValue);
                break;

            case ColumnType.Integer64:
            case ColumnType.Date:
            case ColumnType.DateTime:
                writer.WriteNumber("l", value.LongValue);
                break;

            case ColumnType.Float64:
            case ColumnType.Float32:
                // JSON has no number for ±infinity or NaN (WriteNumber throws for them), so they travel
                // as their names; a finite value, -0 included, is an exact JSON number.
                if (double.IsFinite(value.FloatValue))
                    writer.WriteNumber("f", value.FloatValue);
                else
                    writer.WriteString("f", FloatMath.NonFiniteText(value.FloatValue));
                break;

            case ColumnType.String:
            case ColumnType.Id:
                writer.WriteString("s", value.StrValue);
                break;

            case ColumnType.Bytes:
                writer.WriteBase64String("y", value.BytesValue ?? []);
                break;

            case ColumnType.Jsonb:
                writer.WriteBase64String("j", value.BytesValue ?? []);
                break;

            case ColumnType.Uuid:
            case ColumnType.Numeric:
                writer.WriteNumber("u", value.UuidHigh);
                writer.WriteNumber("l", value.LongValue);
                break;

            case ColumnType.Array:
                writer.WriteNumber("e", (int)value.ArrayElementType);
                writer.WriteStartArray("a");
                foreach (ColumnValue element in value.ArrayValues ?? [])
                    Write(writer, element);
                writer.WriteEndArray();
                break;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Column type {value.Type} has no wire encoding");
        }

        writer.WriteEndObject();
    }

    public static ColumnValue Read(JsonElement element)
    {
        ColumnType type = (ColumnType)element.GetProperty("t").GetInt32();

        return type switch
        {
            ColumnType.Null => ColumnValue.Null,
            ColumnType.Bool => ColumnValue.FromBool(element.GetProperty("b").GetBoolean()),
            ColumnType.Integer64 or ColumnType.Date or ColumnType.DateTime =>
                new ColumnValue(type, element.GetProperty("l").GetInt64()),
            ColumnType.Float64 or ColumnType.Float32 =>
                new ColumnValue(type, ReadFloat(element.GetProperty("f"))),
            ColumnType.String or ColumnType.Id =>
                new ColumnValue(type, element.GetProperty("s").GetString()!),
            ColumnType.Bytes =>
                new ColumnValue(element.GetProperty("y").GetBytesFromBase64()),
            ColumnType.Jsonb =>
                ColumnValue.FromJsonbCell(JsonbValidator.ValidateCell(
                    element.GetProperty("j").GetBytesFromBase64(), CamusDBErrorCodes.JsonbValueCorrupt)),
            ColumnType.Uuid or ColumnType.Numeric =>
                new ColumnValue(type, element.GetProperty("u").GetInt64(), element.GetProperty("l").GetInt64()),
            ColumnType.Array => ReadArray(element),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Column type {type} has no wire decoding"),
        };
    }

    /// <summary>
    /// A float payload: a JSON number, or the name of a non-finite value as <see cref="Write"/> sends
    /// it. Any other string is a corrupt frame.
    /// </summary>
    private static double ReadFloat(JsonElement payload)
    {
        if (payload.ValueKind == JsonValueKind.Number)
            return payload.GetDouble();

        return payload.GetString() switch
        {
            "Infinity" => double.PositiveInfinity,
            "-Infinity" => double.NegativeInfinity,
            "NaN" => double.NaN,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A float wire value is neither a number nor Infinity, -Infinity or NaN"),
        };
    }

    private static ColumnValue ReadArray(JsonElement element)
    {
        ColumnType elementType = (ColumnType)element.GetProperty("e").GetInt32();

        List<ColumnValue> values = [];
        foreach (JsonElement item in element.GetProperty("a").EnumerateArray())
            values.Add(Read(item));

        return new ColumnValue(
            ColumnType.Array,
            strValue: null,
            longValue: 0,
            floatValue: 0,
            boolValue: false,
            bytesValue: null,
            arrayValues: values,
            arrayElementType: elementType);
    }
}
