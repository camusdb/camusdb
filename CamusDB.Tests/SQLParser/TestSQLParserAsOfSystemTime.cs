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
/// Parser coverage for the <c>AS OF SYSTEM TIME</c> time-travel clause. The scanner recognises the
/// whole phrase as one token, so these tests also guard that it does not collide with the table-alias
/// production (<c>FROM t AS x</c>) nor with ordinary SELECTs.
/// </summary>
public sealed class TestSQLParserAsOfSystemTime
{
    [Test]
    public void RelativeOffset_ParsesOntoExtendedSeven()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard AS OF SYSTEM TIME '-10s'");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedSeven);
        Assert.AreEqual(NodeType.String, ast.extendedSeven!.nodeType);
        Assert.AreEqual("'-10s'", ast.extendedSeven!.yytext);
    }

    [Test]
    public void AbsoluteTimestampString_Parses()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM leaderboard AS OF SYSTEM TIME '2026-07-19 20:00:00+00:00'");

        Assert.AreEqual(NodeType.String, ast.extendedSeven!.nodeType);
        Assert.AreEqual("'2026-07-19 20:00:00+00:00'", ast.extendedSeven!.yytext);
    }

    [Test]
    public void IntegerEpoch_Parses()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard AS OF SYSTEM TIME 1721420000000");

        Assert.AreEqual(NodeType.Integer, ast.extendedSeven!.nodeType);
        Assert.AreEqual("1721420000000", ast.extendedSeven!.yytext);
    }

    [Test]
    public void Placeholder_Parses()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard AS OF SYSTEM TIME @ts");

        Assert.AreEqual(NodeType.Placeholder, ast.extendedSeven!.nodeType);
        Assert.AreEqual("@ts", ast.extendedSeven!.yytext);
    }

    [Test]
    public void AsOf_BeforeWhereOrderAndLimit_Parses()
    {
        // Standard placement: AS OF SYSTEM TIME sits right after the FROM table, before WHERE.
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT score FROM leaderboard AS OF SYSTEM TIME '-1m' WHERE score > 5 ORDER BY score DESC LIMIT 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne); // WHERE preserved
        Assert.IsNotNull(ast.extendedTwo); // ORDER BY preserved
        Assert.IsNotNull(ast.extendedThree); // LIMIT preserved
        Assert.AreEqual("'-1m'", ast.extendedSeven!.yytext);
    }

    [Test]
    public void AsOf_BeforeWhere_DoubleQuoted_Parses()
    {
        // The exact user shape: double-quoted value, AS OF before WHERE.
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM accounts AS OF SYSTEM TIME \"-10s\" WHERE id = 9910");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("\"-10s\"", ast.extendedSeven!.yytext);
        Assert.IsNotNull(ast.extendedOne); // WHERE id = 9910 preserved
    }

    [Test]
    public void CaseInsensitiveAndNewlines_Parse()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard\nas\tof   system\ntime '-5m'");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("'-5m'", ast.extendedSeven!.yytext);
    }

    [Test]
    public void PlainSelect_HasNoAsOf()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNull(ast.extendedSeven);
    }

    [Test]
    public void TableAlias_NotShadowedByAsOf()
    {
        // "AS x" must still parse as a table alias, not be swallowed by the AS OF phrase token.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard AS x");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNull(ast.extendedSeven);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("x", ast.rightAst!.rightAst!.yytext);
    }

    [Test]
    public void TableAliasNamedOf_NotShadowed()
    {
        // "AS of" (alias literally "of") must not be mistaken for the AS OF phrase, because the
        // full "AS OF SYSTEM TIME" phrase is not present.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM leaderboard AS of");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNull(ast.extendedSeven);
        Assert.AreEqual("of", ast.rightAst!.rightAst!.yytext);
    }
}
