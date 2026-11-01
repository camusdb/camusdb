/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Formats.Cbor;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Turns JSON text into a <c>jsonb</c> cell: the format-version byte
/// (<see cref="JsonbLimits.FormatVersion"/>) followed by one CBOR (RFC 8949) item written by
/// <see cref="CborWriter"/> in <see cref="CborConformanceMode.Canonical"/>.
///
/// <para>The canonical mode is what gives the PostgreSQL <c>jsonb</c> semantics for free. It writes
/// definite lengths and the shortest encodings, and it sorts map keys by their encoding, which for
/// text keys is shorter key first, then bytes: the PostgreSQL key order. It refuses duplicate keys, so
/// this encoder removes them first and keeps the last value, as PostgreSQL does. Two texts that differ
/// only in whitespace, key order or a repeated key therefore give the same bytes, and every nested item
/// is itself a complete canonical item, so <c>-&gt;</c> can return a slice.</para>
///
/// <para>Numbers are exact (<see cref="JsonbNumber"/>): an integer with no fraction or exponent that
/// fits a long is a CBOR integer, a wider integer a bignum (tags 2 and 3), and any other number a
/// decimal fraction (tag 4) <c>[-scale, mantissa]</c>. CBOR floats are never written.</para>
///
/// <para>The text is parsed by <see cref="JsonDocument"/> with the depth limit
/// <see cref="JsonbLimits.MaxNestingDepth"/>, and the document is written by an iterative walk with an
/// explicit stack, so a deep document is refused before anything recurses over it.</para>
/// </summary>
internal static class JsonbEncoder
{
    private static readonly JsonDocumentOptions ParseOptions = new()
    {
        MaxDepth = JsonbLimits.MaxNestingDepth,
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    /// <summary>Parses <paramref name="text"/> into a cell.</summary>
    public static byte[] FromText(string text) => FromUtf8(Encoding.UTF8.GetBytes(text));

    /// <summary>Parses UTF-8 JSON text into a cell.</summary>
    public static byte[] FromUtf8(ReadOnlyMemory<byte> utf8)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(utf8, ParseOptions);
        }
        catch (JsonException ex)
        {
            if (ExceedsDepth(utf8.Span))
                throw TooDeep();

            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText, "invalid input syntax for type json: " + ex.Message);
        }

        using (document)
        {
            CborWriter writer = NewWriter();
            WriteElement(writer, document.RootElement);
            return ToCell(writer);
        }
    }

    public static CborWriter NewWriter() =>
        new(CborConformanceMode.Canonical, convertIndefiniteLengthEncodings: false, allowMultipleRootLevelValues: false);

    /// <summary>The cell for what <paramref name="writer"/> holds: the version byte, then the CBOR item.</summary>
    public static byte[] ToCell(CborWriter writer)
    {
        byte[] cell = new byte[1 + writer.BytesWritten];
        cell[0] = JsonbLimits.FormatVersion;
        writer.Encode(cell.AsSpan(1));
        return cell;
    }

    /// <summary>The cell that holds a copy of one CBOR item (for example a slice of another cell).</summary>
    public static byte[] ToCell(ReadOnlySpan<byte> item)
    {
        byte[] cell = new byte[1 + item.Length];
        cell[0] = JsonbLimits.FormatVersion;
        item.CopyTo(cell.AsSpan(1));
        return cell;
    }

    public static CamusDBException TooDeep() =>
        new(CamusDBErrorCodes.JsonNestingTooDeep,
            $"jsonb document nests deeper than {JsonbLimits.MaxNestingDepth} levels");

    /// <summary>
    /// Runs only after a parse failed, to tell a document that nests too deep from text that is not
    /// JSON. <see cref="Utf8JsonReader"/> is iterative, so this is safe for any depth.
    /// </summary>
    private static bool ExceedsDepth(ReadOnlySpan<byte> utf8)
    {
        Utf8JsonReader reader = new(utf8, new JsonReaderOptions { MaxDepth = int.MaxValue / 2 });
        try
        {
            while (reader.Read())
            {
                if (reader.TokenType is JsonTokenType.StartArray or JsonTokenType.StartObject &&
                    reader.CurrentDepth >= JsonbLimits.MaxNestingDepth)
                    return true;
            }
        }
        catch (JsonException)
        {
            // The text is malformed before it gets too deep.
        }

        return false;
    }

    /// <summary>One open container of the walk: its remaining array items or object pairs.</summary>
    private sealed class Frame
    {
        public bool IsObject;
        public JsonElement.ArrayEnumerator Items;
        public KeyValuePair<string, JsonElement>[]? Pairs;
        public int Next;
    }

    private static void WriteElement(CborWriter writer, JsonElement root)
    {
        Stack<Frame>? stack = null;
        JsonElement element = root;

        while (true)
        {
            // Write one element. A container opens a frame instead of recursing.
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                {
                    KeyValuePair<string, JsonElement>[] pairs = DistinctPairs(element);
                    writer.WriteStartMap(pairs.Length);
                    (stack ??= new()).Push(new Frame { IsObject = true, Pairs = pairs });
                    break;
                }

                case JsonValueKind.Array:
                    writer.WriteStartArray(element.GetArrayLength());
                    (stack ??= new()).Push(new Frame { IsObject = false, Items = element.EnumerateArray() });
                    break;

                case JsonValueKind.String:
                    writer.WriteTextString(ReadString(element));
                    break;

                case JsonValueKind.Number:
                    WriteNumber(writer, JsonbNumber.ParseJsonText(JsonMarshal.GetRawUtf8Value(element)));
                    break;

                case JsonValueKind.True:
                    writer.WriteBoolean(true);
                    break;

                case JsonValueKind.False:
                    writer.WriteBoolean(false);
                    break;

                default:
                    writer.WriteNull();
                    break;
            }

            // Find the next element: the next item of the innermost open container, closing every
            // container that has none left.
            while (true)
            {
                if (stack is null || stack.Count == 0)
                    return;

                Frame frame = stack.Peek();
                if (frame.IsObject)
                {
                    if (frame.Next < frame.Pairs!.Length)
                    {
                        KeyValuePair<string, JsonElement> pair = frame.Pairs[frame.Next++];
                        writer.WriteTextString(pair.Key);
                        element = pair.Value;
                        break;
                    }

                    writer.WriteEndMap();
                    stack.Pop();
                }
                else
                {
                    if (frame.Items.MoveNext())
                    {
                        element = frame.Items.Current;
                        break;
                    }

                    writer.WriteEndArray();
                    stack.Pop();
                }
            }
        }
    }

    /// <summary>
    /// The pairs of an object with duplicate keys removed. The last value of a key wins, as in
    /// PostgreSQL. The order does not matter: the canonical writer sorts the keys.
    /// </summary>
    private static KeyValuePair<string, JsonElement>[] DistinctPairs(JsonElement element)
    {
        List<KeyValuePair<string, JsonElement>> pairs = [];
        Dictionary<string, int>? positions = null;

        foreach (JsonProperty property in element.EnumerateObject())
        {
            string name = ReadName(property);

            int existing = -1;
            if (positions is not null)
            {
                if (positions.TryGetValue(name, out int position))
                    existing = position;
            }
            else
            {
                for (int i = 0; i < pairs.Count; i++)
                {
                    if (string.Equals(pairs[i].Key, name, StringComparison.Ordinal))
                    {
                        existing = i;
                        break;
                    }
                }
            }

            if (existing >= 0)
            {
                pairs[existing] = new(name, property.Value);
                continue;
            }

            pairs.Add(new(name, property.Value));

            // A linear search is cheapest for the small objects most documents hold.
            if (positions is not null)
                positions[name] = pairs.Count - 1;
            else if (pairs.Count > 16)
            {
                positions = new(pairs.Count * 2, StringComparer.Ordinal);
                for (int i = 0; i < pairs.Count; i++)
                    positions[pairs[i].Key] = i;
            }
        }

        return pairs.ToArray();
    }

    private static string ReadString(JsonElement element)
    {
        string value;
        try
        {
            value = element.GetString()!;
        }
        catch (InvalidOperationException)
        {
            throw InvalidUnicode();
        }

        return CheckString(value);
    }

    private static string ReadName(JsonProperty property)
    {
        string name;
        try
        {
            name = property.Name;
        }
        catch (InvalidOperationException)
        {
            throw InvalidUnicode();
        }

        return CheckString(name);
    }

    /// <summary>
    /// PostgreSQL text cannot hold U+0000, so <c>jsonb</c> refuses the <c>\u0000</c> escape. A lone
    /// surrogate is refused by the reader before this point.
    /// </summary>
    public static string CheckString(string value)
    {
        if (value.Contains('\0'))
            throw new CamusDBException(CamusDBErrorCodes.InvalidJsonText,
                "unsupported Unicode escape sequence: \\u0000 cannot be converted to text");

        return value;
    }

    private static CamusDBException InvalidUnicode() =>
        new(CamusDBErrorCodes.InvalidJsonText, "invalid input syntax for type json: invalid Unicode surrogate");

    /// <summary>Writes an exact number in the canonical form described on the class.</summary>
    public static void WriteNumber(CborWriter writer, in JsonbNumber number)
    {
        if (number.Scale == 0)
        {
            WriteInteger(writer, number.Mantissa);
            return;
        }

        writer.WriteTag(CborTag.DecimalFraction);
        writer.WriteStartArray(2);
        writer.WriteInt64(-number.Scale);
        WriteInteger(writer, number.Mantissa);
        writer.WriteEndArray();
    }

    private static void WriteInteger(CborWriter writer, BigInteger value)
    {
        if (value >= long.MinValue && value <= long.MaxValue)
            writer.WriteInt64((long)value);
        else
            writer.WriteBigInteger(value);
    }
}
