/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The AST wire codec must round-trip real parser output structurally intact — node types,
/// literal text, and child positions — because a fragment's remote filter evaluation runs on
/// the deserialized tree. A drift here would silently change which rows a remote span keeps.
/// </summary>
public sealed class TestNodeAstWireCodec
{
    private static void AssertStructurallyEqual(NodeAst? expected, NodeAst? actual, string path)
    {
        if (expected is null || actual is null)
        {
            Assert.AreEqual(expected is null, actual is null, $"Null mismatch at {path}");
            return;
        }

        Assert.AreEqual(expected.nodeType, actual.nodeType, $"nodeType at {path}");
        Assert.AreEqual(expected.yytext, actual.yytext, $"yytext at {path}");

        AssertStructurallyEqual(expected.leftAst, actual.leftAst, path + ".left");
        AssertStructurallyEqual(expected.rightAst, actual.rightAst, path + ".right");
        AssertStructurallyEqual(expected.extendedOne, actual.extendedOne, path + ".e1");
        AssertStructurallyEqual(expected.extendedTwo, actual.extendedTwo, path + ".e2");
        AssertStructurallyEqual(expected.extendedThree, actual.extendedThree, path + ".e3");
        AssertStructurallyEqual(expected.extendedFour, actual.extendedFour, path + ".e4");
        AssertStructurallyEqual(expected.extendedFive, actual.extendedFive, path + ".e5");
        AssertStructurallyEqual(expected.extendedSix, actual.extendedSix, path + ".e6");
        AssertStructurallyEqual(expected.extendedSeven, actual.extendedSeven, path + ".e7");
    }

    [Test]
    public void RoundTrip_ParsedWhereClause_IsStructurallyIdentical()
    {
        NodeAst select = SQLParserProcessor.Parse(
            "SELECT id, num FROM readings WHERE (num >= 100 AND num < 250) OR name = 'a''b' AND enabled = TRUE");

        Assert.IsNotNull(select.extendedOne, "Parsed SELECT must carry a WHERE clause");
        NodeAst where = select.extendedOne!;

        string json = NodeAstWireCodec.Serialize(where);
        NodeAst restored = NodeAstWireCodec.Deserialize(json);

        AssertStructurallyEqual(where, restored, "where");

        // Serialization is deterministic: a second pass over the restored tree is identical.
        Assert.AreEqual(json, NodeAstWireCodec.Serialize(restored));
    }

    [Test]
    public void RoundTrip_LeafAndUnicodeText_Survive()
    {
        NodeAst leaf = NodeAst.FromLong(-42);
        AssertStructurallyEqual(leaf, NodeAstWireCodec.Deserialize(NodeAstWireCodec.Serialize(leaf)), "leaf");

        NodeAst unicode = new(
            NodeType.String, null, null, null, null, null, null, null, "café — 数据库 'quoted'");
        AssertStructurallyEqual(unicode, NodeAstWireCodec.Deserialize(NodeAstWireCodec.Serialize(unicode)), "unicode");
    }
}
