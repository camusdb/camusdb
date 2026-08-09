
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
/// BETWEEN binds tighter than AND/OR, so a trailing boolean operator must group outside the BETWEEN:
/// <c>x BETWEEN a AND b AND c</c> is <c>(x BETWEEN a AND b) AND c</c>, not
/// <c>x BETWEEN (a AND b) AND c</c>. The grammar gets that grouping by construction: a bound is an
/// arithmetic-level expression that cannot contain a bare AND/OR, so no other reading exists. These
/// tests pin the grouping and the shape of what a bound may contain.
/// </summary>
public sealed class TestSQLParserBetweenPrecedence
{
    /// <summary>Returns the WHERE condition of a SELECT.</summary>
    private static NodeAst Where(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        return ast.extendedOne!;
    }

    [Test]
    public void BetweenThenAnd_GroupsBetweenTighter()
    {
        NodeAst where = Where("SELECT x FROM t WHERE x BETWEEN a AND b AND c");

        // Top node is AND, whose left operand is the whole BETWEEN and right operand is c.
        Assert.AreEqual(NodeType.ExprAnd, where.nodeType);
        Assert.AreEqual(NodeType.ExprBetween, where.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, where.rightAst!.nodeType);
        Assert.AreEqual("c", where.rightAst.yytext);

        // The BETWEEN keeps a as its lower bound and b as its upper bound (not `a AND b`).
        NodeAst between = where.leftAst;
        Assert.AreEqual("x", between.leftAst!.yytext);
        Assert.AreEqual("a", between.extendedOne!.yytext);
        Assert.AreEqual("b", between.extendedTwo!.yytext);
    }

    [Test]
    public void BetweenThenOr_GroupsBetweenTighter()
    {
        NodeAst where = Where("SELECT x FROM t WHERE x BETWEEN a AND b OR c");

        Assert.AreEqual(NodeType.ExprOr, where.nodeType);
        Assert.AreEqual(NodeType.ExprBetween, where.leftAst!.nodeType);
        Assert.AreEqual("c", where.rightAst!.yytext);
    }

    [Test]
    public void AndThenBetween_GroupsBetweenTighter()
    {
        // Leading AND: `a AND x BETWEEN b AND c` is `a AND (x BETWEEN b AND c)`.
        NodeAst where = Where("SELECT x FROM t WHERE a AND x BETWEEN b AND c");

        Assert.AreEqual(NodeType.ExprAnd, where.nodeType);
        Assert.AreEqual("a", where.leftAst!.yytext);
        Assert.AreEqual(NodeType.ExprBetween, where.rightAst!.nodeType);
    }

    [Test]
    public void Bounds_AcceptArithmetic()
    {
        NodeAst where = Where("SELECT x FROM t WHERE x BETWEEN a + 1 AND b * 2");

        Assert.AreEqual(NodeType.ExprBetween, where.nodeType);
        Assert.AreEqual(NodeType.ExprAdd, where.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprMult, where.extendedTwo!.nodeType);
    }

    [Test]
    public void Bounds_AcceptNegativeLiteralsFunctionsAndSubqueries()
    {
        NodeAst where = Where("SELECT x FROM t WHERE x BETWEEN -5 AND abs(y)");
        Assert.AreEqual(NodeType.ExprBetween, where.nodeType);
        Assert.AreEqual(NodeType.Integer, where.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprFuncCall, where.extendedTwo!.nodeType);

        where = Where("SELECT x FROM t WHERE x BETWEEN (SELECT MIN(v) FROM u) AND 10");
        Assert.AreEqual(NodeType.ExprBetween, where.nodeType);
        Assert.AreEqual(NodeType.ExprScalarSubquery, where.extendedOne!.nodeType);
    }

    [Test]
    public void Bounds_AcceptBooleanExpressionOnlyWhenParenthesised()
    {
        // A bare AND inside a bound is not a bound at all — it closes the BETWEEN. Parentheses
        // delimit the boolean expression, so this form still reaches the lower bound.
        NodeAst where = Where("SELECT x FROM t WHERE x BETWEEN (a AND b) AND c");

        Assert.AreEqual(NodeType.ExprBetween, where.nodeType);
        Assert.AreEqual(NodeType.ExprAnd, where.extendedOne!.nodeType);
        Assert.AreEqual("c", where.extendedTwo!.yytext);
    }
}
