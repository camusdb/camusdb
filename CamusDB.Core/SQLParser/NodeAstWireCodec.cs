
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Compact JSON round-trip for a <see cref="NodeAst"/> tree, used to ship a residual WHERE
/// filter to the node executing a query fragment. The AST is a pure tree (no cycles, no
/// back-references), so the encoding is a direct recursive walk:
/// <c>{"t": nodeType, "y": yytext?, "c": [child…]}</c> with the child array trimmed of
/// trailing nulls and omitted entirely for leaves.
///
/// <para>This serializes structure only. Whether a given tree is <i>safe</i> to evaluate on a
/// peer (no subqueries, no parameter placeholders, no volatile functions) is a separate
/// judgment made by the coordinator before shipping — the codec neither checks nor cares.</para>
/// </summary>
public static class NodeAstWireCodec
{
    public static string Serialize(NodeAst ast)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
            Write(writer, ast);

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    public static NodeAst Deserialize(string json)
    {
        using JsonDocument doc = JsonDocument.Parse(json);
        return Read(doc.RootElement);
    }

    private static void Write(Utf8JsonWriter writer, NodeAst node)
    {
        writer.WriteStartObject();
        writer.WriteNumber("t", (int)node.nodeType);

        if (node.yytext is not null)
            writer.WriteString("y", node.yytext);

        NodeAst?[] children =
        [
            node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo,
            node.extendedThree, node.extendedFour, node.extendedFive,
            node.extendedSix, node.extendedSeven,
        ];

        int last = children.Length - 1;
        while (last >= 0 && children[last] is null)
            last--;

        if (last >= 0)
        {
            writer.WriteStartArray("c");

            for (int i = 0; i <= last; i++)
            {
                if (children[i] is { } child)
                    Write(writer, child);
                else
                    writer.WriteNullValue();
            }

            writer.WriteEndArray();
        }

        writer.WriteEndObject();
    }

    private static NodeAst Read(JsonElement element)
    {
        NodeType nodeType = (NodeType)element.GetProperty("t").GetInt32();

        string? yytext = element.TryGetProperty("y", out JsonElement y) ? y.GetString() : null;

        NodeAst?[] children = new NodeAst?[9];

        if (element.TryGetProperty("c", out JsonElement childArray))
        {
            int i = 0;

            foreach (JsonElement child in childArray.EnumerateArray())
            {
                if (i >= children.Length)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "Serialized AST node has more children than NodeAst can hold");

                if (child.ValueKind != JsonValueKind.Null)
                    children[i] = Read(child);

                i++;
            }
        }

        return new NodeAst(
            nodeType,
            children[0], children[1], children[2], children[3], children[4],
            children[5], children[6],
            yytext,
            children[7], children[8]);
    }
}
