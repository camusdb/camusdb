
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Tests.Queries;

/// <summary>
/// Unit tests for QueryExpressionClassifier classification methods.
/// These tests build NodeAst nodes directly to avoid any dependency on the full
/// query pipeline — they verify classifier behaviour in isolation.
/// </summary>
[TestFixture]
public class TestQueryExpressionClassifier
{
    // ── AST construction helpers ─────────────────────────────────────────────

    private static NodeAst Ident(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst IntLit(long value) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, value.ToString());

    private static NodeAst FloatLit(double value) =>
        new(NodeType.Float, null, null, null, null, null, null, null, value.ToString());

    /// <summary>Mirrors the grammar production: identifier LPAREN arg RPAREN.</summary>
    private static NodeAst FuncCall(string name, NodeAst? args = null) =>
        new(NodeType.ExprFuncCall, Ident(name), args, null, null, null, null, null, null);

    /// <summary>Two-arg ExprArgumentList matching the grammar's left-recursive form.</summary>
    private static NodeAst ArgList(NodeAst left, NodeAst right) =>
        new(NodeType.ExprArgumentList, left, right, null, null, null, null, null, null);

    private static NodeAst Alias(NodeAst expr, string alias) =>
        new(NodeType.ExprAlias, expr, Ident(alias), null, null, null, null, null, null);

    private static NodeAst Add(NodeAst left, NodeAst right) =>
        new(NodeType.ExprAdd, left, right, null, null, null, null, null, null);

    private static NodeAst Div(NodeAst left, NodeAst right) =>
        new(NodeType.ExprDiv, left, right, null, null, null, null, null, null);

    // ── IsAggregateProjection ────────────────────────────────────────────────

    [Test]
    public void IsAggregateProjection_BareSum_ReturnsTrue()
    {
        NodeAst ast = FuncCall("sum", Ident("x"));
        Assert.IsTrue(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    [Test]
    public void IsAggregateProjection_BareCount_ReturnsTrue()
    {
        NodeAst ast = FuncCall("count", Ident("x"));
        Assert.IsTrue(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    [Test]
    public void IsAggregateProjection_BareAggregateWithAlias_ReturnsTrue()
    {
        NodeAst ast = Alias(FuncCall("avg", Ident("score")), "a");
        Assert.IsTrue(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    [Test]
    public void IsAggregateProjection_PlainIdentifier_ReturnsFalse()
    {
        NodeAst ast = Ident("year");
        Assert.IsFalse(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    [Test]
    public void IsAggregateProjection_ScalarFuncCall_ReturnsFalse()
    {
        NodeAst ast = FuncCall("coalesce", ArgList(Ident("col"), IntLit(0)));
        Assert.IsFalse(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    [Test]
    public void IsAggregateProjection_CompoundSumPlusOne_ReturnsFalse()
    {
        NodeAst ast = Add(FuncCall("sum", Ident("x")), IntLit(1));
        Assert.IsFalse(QueryExpressionClassifier.IsAggregateProjection(ast));
    }

    // ── ContainsAggregate ────────────────────────────────────────────────────

    [Test]
    public void ContainsAggregate_BareSum_ReturnsTrue()
    {
        NodeAst ast = FuncCall("sum", Ident("x"));
        Assert.IsTrue(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_CoalesceOfSum_ReturnsTrue()
    {
        // COALESCE(SUM(x), 0)
        NodeAst ast = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("x")), IntLit(0)));
        Assert.IsTrue(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_SumPlusOne_ReturnsTrue()
    {
        // SUM(x) + 1
        NodeAst ast = Add(FuncCall("sum", Ident("x")), IntLit(1));
        Assert.IsTrue(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_SumDivSum_ReturnsTrue()
    {
        // SUM(a) / SUM(b)
        NodeAst ast = Div(FuncCall("sum", Ident("a")), FuncCall("sum", Ident("b")));
        Assert.IsTrue(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_PlainIdentifier_ReturnsFalse()
    {
        NodeAst ast = Ident("year");
        Assert.IsFalse(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_ScalarFuncWithoutAggregate_ReturnsFalse()
    {
        // abs(x)
        NodeAst ast = FuncCall("abs", Ident("x"));
        Assert.IsFalse(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    [Test]
    public void ContainsAggregate_LiteralInt_ReturnsFalse()
    {
        NodeAst ast = IntLit(42);
        Assert.IsFalse(QueryExpressionClassifier.ContainsAggregate(ast));
    }

    // ── IsCompoundAggregateProjection ────────────────────────────────────────

    [Test]
    public void IsCompoundAggregateProjection_BareSum_ReturnsFalse()
    {
        // Bare aggregate is NOT compound — it has its own fast path.
        NodeAst ast = FuncCall("sum", Ident("x"));
        Assert.IsFalse(QueryExpressionClassifier.IsCompoundAggregateProjection(ast));
    }

    [Test]
    public void IsCompoundAggregateProjection_CoalesceOfSum_ReturnsTrue()
    {
        NodeAst ast = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("x")), IntLit(0)));
        Assert.IsTrue(QueryExpressionClassifier.IsCompoundAggregateProjection(ast));
    }

    [Test]
    public void IsCompoundAggregateProjection_SumPlusOne_ReturnsTrue()
    {
        NodeAst ast = Add(FuncCall("sum", Ident("x")), IntLit(1));
        Assert.IsTrue(QueryExpressionClassifier.IsCompoundAggregateProjection(ast));
    }

    [Test]
    public void IsCompoundAggregateProjection_PlainIdentifier_ReturnsFalse()
    {
        NodeAst ast = Ident("year");
        Assert.IsFalse(QueryExpressionClassifier.IsCompoundAggregateProjection(ast));
    }

    // ── ValidateNoNestedAggregate ─────────────────────────────────────────────

    [Test]
    public void ValidateNoNestedAggregate_BareSum_DoesNotThrow()
    {
        NodeAst ast = FuncCall("sum", Ident("x"));
        Assert.DoesNotThrow(() => QueryExpressionClassifier.ValidateNoNestedAggregate(ast));
    }

    [Test]
    public void ValidateNoNestedAggregate_CoalesceOfSum_DoesNotThrow()
    {
        NodeAst ast = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("x")), IntLit(0)));
        Assert.DoesNotThrow(() => QueryExpressionClassifier.ValidateNoNestedAggregate(ast));
    }

    [Test]
    public void ValidateNoNestedAggregate_SumOfSum_Throws()
    {
        // SUM(SUM(x)) — aggregate nested inside aggregate argument
        NodeAst inner = FuncCall("sum", Ident("x"));
        NodeAst outer = FuncCall("sum", inner);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => QueryExpressionClassifier.ValidateNoNestedAggregate(outer))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("nested", ex.Message.ToLowerInvariant());
    }

    [Test]
    public void ValidateNoNestedAggregate_SumOfCount_Throws()
    {
        // SUM(COUNT(*)) — different aggregate functions, still invalid
        NodeAst inner = FuncCall("count", Ident("*"));
        NodeAst outer = FuncCall("sum", inner);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => QueryExpressionClassifier.ValidateNoNestedAggregate(outer))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public void ValidateNoNestedAggregate_SumPlusOne_DoesNotThrow()
    {
        // SUM(x) + 1 is compound but not aggregate-of-aggregate
        NodeAst ast = Add(FuncCall("sum", Ident("x")), IntLit(1));
        Assert.DoesNotThrow(() => QueryExpressionClassifier.ValidateNoNestedAggregate(ast));
    }
}
