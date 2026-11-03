
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

/// <summary>
/// Reads the <c>values</c> object of a typed row request (<c>/insert</c>, <c>/update</c>) and refuses
/// a JSON object that holds one key twice.
///
/// <para><b>Why a converter.</b> The default dictionary reader keeps the last value of a repeated key,
/// so <c>{"a": 1, "a": 2}</c> reached the engine as one assignment of 2 and the first value was lost
/// without a trace. The repeat is gone before any engine check can run, so it must be caught here,
/// while the JSON is read. A repeat is refused with <see cref="CamusDBErrorCodes.DuplicateColumn"/>.</para>
///
/// <para>Keys are compared ordinally here, as the JSON text spells them. Two keys that differ only in
/// case both survive this reader and are refused by the engine, which resolves a name to its column
/// case-insensitively.</para>
/// </summary>
public sealed class ColumnValueMapConverter : JsonConverter<Dictionary<string, ColumnValue>>
{
    public override Dictionary<string, ColumnValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected a JSON object of column values");

        Dictionary<string, ColumnValue> values = new();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return values;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected a column name");

            string column = reader.GetString()!;

            if (!reader.Read())
                break;

            ColumnValue? value = JsonSerializer.Deserialize<ColumnValue>(ref reader, options);

            if (!values.TryAdd(column, value!))
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateColumn,
                    $"Column '{column}' specified more than once in the request values");
        }

        throw new JsonException("Unterminated JSON object of column values");
    }

    public override void Write(Utf8JsonWriter writer, Dictionary<string, ColumnValue> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        foreach (KeyValuePair<string, ColumnValue> entry in value)
        {
            writer.WritePropertyName(entry.Key);
            JsonSerializer.Serialize(writer, entry.Value, options);
        }

        writer.WriteEndObject();
    }
}
