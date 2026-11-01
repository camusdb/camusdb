/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Writes the text of a <c>jsonb</c> item in the two PostgreSQL forms: the compact form of
/// <c>jsonb_out</c> (<c>{"a": 1, "b": [1, 2]}</c>: a space after every <c>:</c> and <c>,</c>) and the
/// indented form of <c>jsonb_pretty</c> (four spaces per level). Keys come out in the stored order,
/// which is the PostgreSQL order.
///
/// <para>The rules copy PostgreSQL <c>JsonbToCStringWorker</c> exactly, including two details that
/// look odd: in the indented form an empty container still breaks the line (<c>[]</c> prints as
/// <c>"[\n]"</c>), and a container that is the value of a key starts on the key's line. Strings are
/// escaped as PostgreSQL <c>escape_json</c> does: <c>\"</c>, <c>\\</c>, <c>\b \f \n \r \t</c>, and
/// <c>\u00xx</c> (lower-case hex) for other control characters; every other character is written
/// as is.</para>
///
/// <para>The walk is iterative, with an explicit stack, so the output of a deep document cannot
/// overflow the stack.</para>
/// </summary>
internal static class JsonbText
{
    private struct Frame
    {
        public bool IsObject;
        public int Remaining;
        public bool First;
    }

    public static string ToText(ReadOnlySpan<byte> item, bool pretty = false)
    {
        StringBuilder output = new(Math.Max(16, item.Length * 2));
        Append(item, output, pretty);
        return output.ToString();
    }

    public static void Append(ReadOnlySpan<byte> item, StringBuilder output, bool pretty)
    {
        Frame[] stack = new Frame[8];
        int depth = 0;
        int pos = 0;

        // PostgreSQL turns the line breaks on only after the first token, so the text never starts
        // with a newline.
        bool startOfValue = true;
        bool afterKey = false;

        while (true)
        {
            JsonbKind kind = JsonbCbor.KindAt(item, pos);
            if (kind is JsonbKind.Array or JsonbKind.Object)
            {
                int count = JsonbCbor.ReadContainerHeader(item, ref pos, out bool isObject);
                if (pretty && !startOfValue && !afterKey)
                    NewLine(output, depth);

                output.Append(isObject ? '{' : '[');

                if (depth == stack.Length)
                    Array.Resize(ref stack, stack.Length * 2);

                stack[depth++] = new Frame { IsObject = isObject, Remaining = count, First = true };
            }
            else
            {
                AppendScalar(item, ref pos, output);
            }

            startOfValue = false;
            afterKey = false;

            // Move to the next value, closing every finished container.
            while (true)
            {
                if (depth == 0)
                    return;

                ref Frame frame = ref stack[depth - 1];
                if (frame.Remaining == 0)
                {
                    depth--;
                    if (pretty)
                        NewLine(output, depth);

                    output.Append(frame.IsObject ? '}' : ']');
                    continue;
                }

                frame.Remaining--;
                if (!frame.First)
                    output.Append(pretty ? "," : ", ");
                frame.First = false;

                if (frame.IsObject)
                {
                    if (pretty)
                        NewLine(output, depth);

                    AppendString(JsonbCbor.ReadText(item, ref pos), output);
                    output.Append(": ");
                    afterKey = true;
                }
                else
                {
                    JsonbKind next = JsonbCbor.KindAt(item, pos);
                    if (pretty && next is not (JsonbKind.Array or JsonbKind.Object))
                        NewLine(output, depth);
                }

                break;
            }
        }
    }

    private static void NewLine(StringBuilder output, int level)
    {
        output.Append('\n');
        output.Append(' ', level * 4);
    }

    private static void AppendScalar(ReadOnlySpan<byte> item, ref int pos, StringBuilder output)
    {
        switch (JsonbCbor.KindAt(item, pos))
        {
            case JsonbKind.Null:
                JsonbCbor.ReadHeader(item, ref pos, out _, out _);
                output.Append("null");
                break;

            case JsonbKind.Boolean:
                output.Append(JsonbCbor.ReadBoolean(item, ref pos) ? "true" : "false");
                break;

            case JsonbKind.String:
                AppendString(JsonbCbor.ReadText(item, ref pos), output);
                break;

            default:
                if (JsonbCbor.TryReadSmallInteger(item, pos, out long small, out int end))
                {
                    output.Append(small);
                    pos = end;
                }
                else
                {
                    JsonbCbor.ReadNumber(item, ref pos).AppendText(output);
                }
                break;
        }
    }

    /// <summary>Writes a quoted, escaped JSON string from its UTF-8 bytes.</summary>
    public static void AppendString(ReadOnlySpan<byte> utf8, StringBuilder output)
    {
        output.Append('"');
        AppendEscaped(Encoding.UTF8.GetString(utf8), output);
        output.Append('"');
    }

    public static void AppendEscaped(string value, StringBuilder output)
    {
        foreach (char c in value)
        {
            switch (c)
            {
                case '"': output.Append("\\\""); break;
                case '\\': output.Append("\\\\"); break;
                case '\b': output.Append("\\b"); break;
                case '\f': output.Append("\\f"); break;
                case '\n': output.Append("\\n"); break;
                case '\r': output.Append("\\r"); break;
                case '\t': output.Append("\\t"); break;
                default:
                    if (c < 0x20)
                        output.Append("\\u").Append(((int)c).ToString("x4"));
                    else
                        output.Append(c);
                    break;
            }
        }
    }
}
