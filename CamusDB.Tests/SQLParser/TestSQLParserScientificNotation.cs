/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using NUnit.Framework;

using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Float literals may carry an exponent (<c>1e10</c>, <c>1.5E-3</c>, <c>-2.5e+8</c>). The mantissa
/// may omit the fractional part, so <c>1e10</c> is a float even though it has no decimal point —
/// without the exponent rule the lexer would split it into the integer <c>1</c> followed by the
/// identifier <c>e10</c>, which then fails to parse. These tests pin that the literal is lexed as a
/// single float token and that its text round-trips through invariant <c>double</c> parsing, which
/// is how the executor turns it into a <c>Float64</c> column value.
/// </summary>
public sealed class TestSQLParserScientificNotation
{
    /// <summary>Returns the right-hand literal of the WHERE comparison, i.e. the token under test.</summary>
    private static NodeAst Literal(string literal)
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM t WHERE x = " + literal);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        return ast.extendedOne!.rightAst!;
    }

    private static void AssertFloatLiteral(string literal, double expected)
    {
        NodeAst node = Literal(literal);

        Assert.AreEqual(NodeType.Float, node.nodeType, literal);
        Assert.AreEqual(literal, node.yytext, literal);
        Assert.IsTrue(double.TryParse(node.yytext!, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed), literal);
        Assert.AreEqual(expected, parsed, literal);
    }

    [Test]
    public void ExponentWithoutFractionalPart_IsFloat()
    {
        AssertFloatLiteral("1e10", 1e10);
        AssertFloatLiteral("1E10", 1E10);
        AssertFloatLiteral("3e0", 3d);
    }

    [Test]
    public void ExponentWithFractionalPart_IsFloat()
    {
        AssertFloatLiteral("1.5e3", 1.5e3);
        AssertFloatLiteral("1.5E3", 1.5E3);
    }

    [Test]
    public void SignedExponent_IsFloat()
    {
        AssertFloatLiteral("1.5e-3", 1.5e-3);
        AssertFloatLiteral("1.5e+3", 1.5e+3);
        AssertFloatLiteral("2e-8", 2e-8);
    }

    [Test]
    public void NegativeMantissa_IsFloat()
    {
        AssertFloatLiteral("-2.5e+8", -2.5e+8);
        AssertFloatLiteral("-4e2", -4e2);
    }

    [Test]
    public void PlainLiteralsStillLexAsBefore()
    {
        Assert.AreEqual(NodeType.Integer, Literal("10").nodeType);
        Assert.AreEqual(NodeType.Float, Literal("10.5").nodeType);
        Assert.AreEqual(NodeType.Integer, Literal("0x1E5").nodeType);
    }

    [Test]
    public void ExponentInWhereClause_IsFloat()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM t WHERE x > 1.5e-3");
        NodeAst where = ast.extendedOne!;

        Assert.AreEqual(NodeType.ExprGreaterThan, where.nodeType);
        Assert.AreEqual(NodeType.Float, where.rightAst!.nodeType);
        Assert.AreEqual("1.5e-3", where.rightAst!.yytext);
    }
}
