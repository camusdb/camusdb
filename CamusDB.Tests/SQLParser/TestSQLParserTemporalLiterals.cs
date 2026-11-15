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
/// The SQL standard typed literals <c>DATE '…'</c>, <c>TIMESTAMP '…'</c> and <c>DATETIME '…'</c>. Each
/// parses to the cast node that <c>CAST('…' AS DATE)</c> builds, so the cast checks the text and the
/// rest of the engine sees no new node shape.
/// </summary>
public sealed class TestSQLParserTemporalLiterals
{
    /// <summary>Returns the right-hand side of the WHERE comparison, the literal under test.</summary>
    private static NodeAst Literal(string literal)
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM t WHERE x = " + literal);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        return ast.extendedOne!.rightAst!;
    }

    [TestCase("DATE '2018-03-01'", "'2018-03-01'", NodeType.TypeDate)]
    [TestCase("date '2018-03-01'", "'2018-03-01'", NodeType.TypeDate)]
    [TestCase("TIMESTAMP '2018-03-01 10:00:00'", "'2018-03-01 10:00:00'", NodeType.TypeDateTime)]
    [TestCase("DATETIME '2018-03-01T10:00:00Z'", "'2018-03-01T10:00:00Z'", NodeType.TypeDateTime)]
    public void TypedLiteral_ParsesAsACast(string literal, string text, NodeType type)
    {
        NodeAst node = Literal(literal);

        Assert.AreEqual(NodeType.ExprCast, node.nodeType, literal);
        Assert.AreEqual(NodeType.String, node.leftAst!.nodeType, literal);
        Assert.AreEqual(text, node.leftAst.yytext, literal);
        Assert.AreEqual(type, node.rightAst!.nodeType, literal);
    }

    [Test]
    public void TypedLiteral_InInsertValues()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO employees VALUES (1, 'Ana', 10, NULL, 9000, DATE '2018-03-01'), " +
            "(2, 'Luis', 10, 1, 6000, DATE '2020-07-15')");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);
    }

    [Test]
    public void TypedLiteral_AsColumnDefault()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (id int64 PRIMARY KEY, hired date DEFAULT(DATE '2020-01-01'), at datetime DEFAULT(TIMESTAMP '2020-01-01T10:00:00Z'))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
    }

    [Test]
    public void DateTypeWithoutText_IsAParseError()
    {
        Assert.Catch(() => SQLParserProcessor.Parse("SELECT * FROM t WHERE x = DATE"));
    }
}
