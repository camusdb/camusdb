
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
/// <c>x BETWEEN (a AND b) AND c</c>. The grammar has an inherent reduce/reduce conflict on that
/// trailing AND/OR; declaring <c>between_expr</c> before <c>and_expr</c> makes gppg resolve it toward
/// BETWEEN. These tests pin that resolution.
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
}
