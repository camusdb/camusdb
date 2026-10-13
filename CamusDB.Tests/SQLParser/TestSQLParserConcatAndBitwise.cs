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
/// Parser tests for <c>||</c> and the bitwise operators <c>&amp; | # ~ &lt;&lt; &gt;&gt;</c>. They pin the
/// tokens (<c>||</c> against <c>|</c>, <c>&lt;&lt;</c> against <c>&lt;</c>), the precedence level (above
/// every comparison, below <c>+</c> and <c>-</c>, left-associative), and the two readings of <c>~</c>:
/// prefix bitwise NOT where an operand starts, binary regex match after an operand.
/// </summary>
public sealed class TestSQLParserConcatAndBitwise
{
    private static NodeAst Condition(string sql) => SQLParserProcessor.ParseCondition(sql);

    [TestCase("a || b", NodeType.ExprConcat)]
    [TestCase("a & b", NodeType.ExprBitAnd)]
    [TestCase("a | b", NodeType.ExprBitOr)]
    [TestCase("a # b", NodeType.ExprBitXor)]
    [TestCase("a << b", NodeType.ExprShiftLeft)]
    [TestCase("a >> b", NodeType.ExprShiftRight)]
    [TestCase("a||b", NodeType.ExprConcat)]
    [TestCase("a|b", NodeType.ExprBitOr)]
    [TestCase("a<<2", NodeType.ExprShiftLeft)]
    [TestCase("a>>2", NodeType.ExprShiftRight)]
    public void BinaryOperator_ParsesToItsNode(string sql, NodeType expected)
    {
        NodeAst ast = Condition(sql);

        Assert.AreEqual(expected, ast.nodeType, sql);
        Assert.AreEqual("a", ast.leftAst!.yytext);
    }

    [Test]
    public void ShiftTokens_DoNotBreakTheComparisons()
    {
        Assert.AreEqual(NodeType.ExprLessThan, Condition("a < b").nodeType);
        Assert.AreEqual(NodeType.ExprLessEqualsThan, Condition("a <= b").nodeType);
        Assert.AreEqual(NodeType.ExprNotEquals, Condition("a <> b").nodeType);
        Assert.AreEqual(NodeType.ExprGreaterThan, Condition("a > b").nodeType);
        Assert.AreEqual(NodeType.ExprGreaterEqualsThan, Condition("a >= b").nodeType);
    }

    [Test]
    public void ShiftWithNegativeCount_LexesTheCountAsOneLiteral()
    {
        NodeAst ast = Condition("a << -1");

        Assert.AreEqual(NodeType.ExprShiftLeft, ast.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.rightAst!.nodeType);
        Assert.AreEqual("-1", ast.rightAst.yytext);
    }

    [Test]
    public void Concat_BindsTighterThanComparison()
    {
        // a || b = c is (a || b) = c.
        NodeAst ast = Condition("a || b = c");

        Assert.AreEqual(NodeType.ExprEquals, ast.nodeType);
        Assert.AreEqual(NodeType.ExprConcat, ast.leftAst!.nodeType);
        Assert.AreEqual("c", ast.rightAst!.yytext);
    }

    [Test]
    public void Bitwise_BindsLooserThanAddition()
    {
        // 1 + 2 & 3 is (1 + 2) & 3.
        NodeAst ast = Condition("1 + 2 & 3");

        Assert.AreEqual(NodeType.ExprBitAnd, ast.nodeType);
        Assert.AreEqual(NodeType.ExprAdd, ast.leftAst!.nodeType);
        Assert.AreEqual("3", ast.rightAst!.yytext);

        // 1 << 2 + 3 is 1 << (2 + 3).
        NodeAst shift = Condition("1 << 2 + 3");
        Assert.AreEqual(NodeType.ExprShiftLeft, shift.nodeType);
        Assert.AreEqual(NodeType.ExprAdd, shift.rightAst!.nodeType);
    }

    [Test]
    public void SameLevelOperators_GroupLeftToRight()
    {
        // a & b | c is (a & b) | c, and a | b & c is (a | b) & c: one level, as in PostgreSQL.
        NodeAst andOr = Condition("a & b | c");
        Assert.AreEqual(NodeType.ExprBitOr, andOr.nodeType);
        Assert.AreEqual(NodeType.ExprBitAnd, andOr.leftAst!.nodeType);

        NodeAst orAnd = Condition("a | b & c");
        Assert.AreEqual(NodeType.ExprBitAnd, orAnd.nodeType);
        Assert.AreEqual(NodeType.ExprBitOr, orAnd.leftAst!.nodeType);

        NodeAst concat = Condition("a || b || c");
        Assert.AreEqual(NodeType.ExprConcat, concat.nodeType);
        Assert.AreEqual(NodeType.ExprConcat, concat.leftAst!.nodeType);
        Assert.AreEqual("c", concat.rightAst!.yytext);
    }

    [Test]
    public void Bitwise_BindsTighterThanLogicalAndLike()
    {
        NodeAst and = Condition("a & 1 = 1 AND b | 2 = 3");
        Assert.AreEqual(NodeType.ExprAnd, and.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, and.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprBitAnd, and.leftAst.leftAst!.nodeType);

        NodeAst like = Condition("a || b LIKE 'x%'");
        Assert.AreEqual(NodeType.ExprLike, like.nodeType);
        Assert.AreEqual(NodeType.ExprConcat, like.leftAst!.nodeType);
    }

    [Test]
    public void PrefixTilde_IsBitNot()
    {
        NodeAst ast = Condition("~a");

        Assert.AreEqual(NodeType.ExprBitNot, ast.nodeType);
        Assert.AreEqual("a", ast.leftAst!.yytext);
    }

    [Test]
    public void BinaryTilde_IsStillRegexMatch()
    {
        NodeAst ast = Condition("a ~ 'x'");
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.nodeType);

        // A prefix ~ on the right operand of a regex match: a ~ (~b).
        NodeAst both = Condition("a ~ ~b");
        Assert.AreEqual(NodeType.ExprRegexMatch, both.nodeType);
        Assert.AreEqual(NodeType.ExprBitNot, both.rightAst!.nodeType);

        Assert.AreEqual(NodeType.ExprRegexMatchCi, Condition("a ~* 'x'").nodeType);
        Assert.AreEqual(NodeType.ExprRegexNotMatch, Condition("a !~ 'x'").nodeType);
    }

    [Test]
    public void NotOverRegexMatch_NegatesTheWholeMatch()
    {
        // NOT a ~ b is NOT (a ~ b).
        NodeAst ast = Condition("NOT a ~ b");

        Assert.AreEqual(NodeType.ExprNot, ast.nodeType);
        Assert.AreEqual(NodeType.ExprRegexMatch, ast.leftAst!.nodeType);
    }

    [Test]
    public void PrefixTilde_BindsLikeUnaryMinus()
    {
        // ~a + 1 is (~a) + 1.
        NodeAst plus = Condition("~a + 1");
        Assert.AreEqual(NodeType.ExprAdd, plus.nodeType);
        Assert.AreEqual(NodeType.ExprBitNot, plus.leftAst!.nodeType);

        // ~a & b is (~a) & b.
        NodeAst and = Condition("~a & b");
        Assert.AreEqual(NodeType.ExprBitAnd, and.nodeType);
        Assert.AreEqual(NodeType.ExprBitNot, and.leftAst!.nodeType);

        // The postfix cast binds tighter: ~a::int64 is ~(a::int64).
        NodeAst cast = Condition("~a::int64");
        Assert.AreEqual(NodeType.ExprBitNot, cast.nodeType);
        Assert.AreEqual(NodeType.ExprCast, cast.leftAst!.nodeType);

        NodeAst twice = Condition("~~a");
        Assert.AreEqual(NodeType.ExprBitNot, twice.nodeType);
        Assert.AreEqual(NodeType.ExprBitNot, twice.leftAst!.nodeType);
    }

    [Test]
    public void BetweenBounds_AcceptTheNewOperators()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT x FROM t WHERE x BETWEEN a & 3 AND ~b AND c")!.extendedOne!;

        Assert.AreEqual(NodeType.ExprAnd, ast.nodeType);
        NodeAst between = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprBetween, between.nodeType);
        Assert.AreEqual(NodeType.ExprBitAnd, between.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprBitNot, between.extendedTwo!.nodeType);

        NodeAst concat = SQLParserProcessor.Parse("SELECT x FROM t WHERE x BETWEEN 'a' || 'b' AND 'c' || 'd'")!.extendedOne!;
        Assert.AreEqual(NodeType.ExprConcat, concat.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprConcat, concat.extendedTwo!.nodeType);
    }

    [Test]
    public void Caret_IsNotAnOperator()
    {
        Assert.Throws(Is.InstanceOf<System.Exception>(), () => Condition("a ^ b"));
    }
}
