/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Writes a <c>jsonb</c> item onto a <see cref="Utf8JsonWriter"/> as native JSON tokens, so a REST
/// response carries the document itself, not a string that holds it. Strings go through the writer's
/// own encoder, as every other string of the response does; numbers keep their exact digits (written
/// raw), so <c>12345678901234567890.50</c> reaches the client unchanged.
///
/// <para><see cref="ToElement"/> gives the same tokens as a <see cref="JsonElement"/> for the
/// object-graph encoder (<c>CompactRowEncoder</c>), which a serializer writes back through the same
/// writer calls, so the two REST paths stay byte-identical.</para>
///
/// <para>The walk is iterative, with an explicit stack.</para>
/// </summary>
internal static class JsonbJsonWriter
{
    public static void Write(Utf8JsonWriter writer, ReadOnlySpan<byte> item)
    {
        // Each entry: items still to write in an open container, and whether it is an object.
        Stack<(int Remaining, bool IsObject)> open = new();
        int pos = 0;

        while (true)
        {
            JsonbKind kind = JsonbCbor.KindAt(item, pos);
            switch (kind)
            {
                case JsonbKind.Array:
                case JsonbKind.Object:
                {
                    int count = JsonbCbor.ReadContainerHeader(item, ref pos, out bool isObject);
                    if (isObject)
                        writer.WriteStartObject();
                    else
                        writer.WriteStartArray();
                    open.Push((count, isObject));
                    break;
                }

                case JsonbKind.String:
                    writer.WriteStringValue(JsonbCbor.ReadText(item, ref pos));
                    break;

                case JsonbKind.Boolean:
                    writer.WriteBooleanValue(JsonbCbor.ReadBoolean(item, ref pos));
                    break;

                case JsonbKind.Null:
                    JsonbCbor.ReadHeader(item, ref pos, out _, out _);
                    writer.WriteNullValue();
                    break;

                default:
                    WriteNumber(writer, item, ref pos);
                    break;
            }

            // Close finished containers, then position at the next value (after its key).
            while (true)
            {
                if (open.Count == 0)
                    return;

                (int remaining, bool isObject) = open.Pop();
                if (remaining == 0)
                {
                    if (isObject)
                        writer.WriteEndObject();
                    else
                        writer.WriteEndArray();
                    continue;
                }

                open.Push((remaining - 1, isObject));
                if (isObject)
                    writer.WritePropertyName(JsonbCbor.ReadText(item, ref pos));
                break;
            }
        }
    }

    private static void WriteNumber(Utf8JsonWriter writer, ReadOnlySpan<byte> item, ref int pos)
    {
        if (JsonbCbor.TryReadSmallInteger(item, pos, out long small, out int end))
        {
            writer.WriteNumberValue(small);
            pos = end;
            return;
        }

        StringBuilder text = new();
        JsonbCbor.ReadNumber(item, ref pos).AppendText(text);
        writer.WriteRawValue(Encoding.ASCII.GetBytes(text.ToString()), skipInputValidation: true);
    }

    /// <summary>The document as a <see cref="JsonElement"/> with the tokens <see cref="Write"/> writes.</summary>
    public static JsonElement ToElement(ReadOnlySpan<byte> item)
    {
        ArrayBufferWriter<byte> buffer = new(item.Length * 2 + 16);
        using (Utf8JsonWriter writer = new(buffer))
            Write(writer, item);

        using JsonDocument document = JsonDocument.Parse(buffer.WrittenMemory);
        return document.RootElement.Clone();
    }
}
