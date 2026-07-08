
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Queries;

/// <summary>
/// Unit tests for QueryAggregateExtractor. Tests verify:
///   - correct aggregate extraction and placeholder assignment
///   - deduplication of structurally identical aggregates
///   - the rewritten tree evaluates to the expected value when placeholders are resolved
///   - non-aggregate expressions pass through unchanged
/// </summary>
[TestFixture]
public class TestQueryAggregateExtractor
{
    // ── AST helpers ──────────────────────────────────────────────────────────

    private static NodeAst Ident(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst IntLit(long v) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, v.ToString());

    private static NodeAst FloatLit(double v) =>
        new(NodeType.Float, null, null, null, null, null, null, null, v.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static NodeAst FuncCall(string name, NodeAst? args = null) =>
        new(NodeType.ExprFuncCall, Ident(name), args, null, null, null, null, null, null);

    private static NodeAst ArgList(NodeAst left, NodeAst right) =>
        new(NodeType.ExprArgumentList, left, right, null, null, null, null, null, null);

    private static NodeAst Alias(NodeAst expr, string alias) =>
        new(NodeType.ExprAlias, expr, Ident(alias), null, null, null, null, null, null);

    private static NodeAst Add(NodeAst l, NodeAst r) =>
        new(NodeType.ExprAdd, l, r, null, null, null, null, null, null);

    private static NodeAst Div(NodeAst l, NodeAst r) =>
        new(NodeType.ExprDiv, l, r, null, null, null, null, null, null);

    // Evaluates the rewritten tree against a synthetic row of placeholder→value.
    private static ColumnValue Eval(NodeAst rewritten, Dictionary<string, ColumnValue> row) =>
        SqlExecutor.EvalExpr(rewritten, row, parameters: null);

    // ── Basic extraction ──────────────────────────────────────────────────────

    [Test]
    public void Extract_BareSum_ProducesOneEntry()
    {
        // SUM(x) — bare aggregate; IsCompoundAggregateProjection is false, but Extract
        // still works (returns one aggregate, rewritten tree is just the placeholder).
        NodeAst ast = FuncCall("sum", Ident("x"));

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(1, aggregates.Count);
        Assert.AreEqual("sum", aggregates[0].AggregateNode.leftAst!.yytext);
        Assert.AreEqual(NodeType.Identifier, rewritten.nodeType);
        Assert.AreEqual(aggregates[0].Placeholder, rewritten.yytext);
    }

    [Test]
    public void Extract_PlainIdentifier_ProducesNoAggregates_RewrittenUnchanged()
    {
        NodeAst ast = Ident("year");

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(0, aggregates.Count);
        Assert.AreEqual(NodeType.Identifier, rewritten.nodeType);
        Assert.AreEqual("year", rewritten.yytext);
    }

    [Test]
    public void Extract_Literal_ProducesNoAggregates()
    {
        NodeAst ast = IntLit(42);

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(0, aggregates.Count);
        Assert.AreEqual(NodeType.Integer, rewritten.nodeType);
    }

    // ── COALESCE(SUM(x), 0) ──────────────────────────────────────────────────

    [Test]
    public void Extract_CoalesceOfSum_ProducesOneAggregate()
    {
        // COALESCE(SUM(x), 0)
        NodeAst sumX = FuncCall("sum", Ident("x"));
        NodeAst ast = FuncCall("coalesce", ArgList(sumX, IntLit(0)));

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(1, aggregates.Count);
        Assert.AreEqual("sum", aggregates[0].AggregateNode.leftAst!.yytext);

        // The rewritten tree must be COALESCE(placeholder, 0).
        Assert.AreEqual(NodeType.ExprFuncCall, rewritten.nodeType);
        Assert.AreEqual("coalesce", rewritten.leftAst!.yytext);

        // First arg of the argument list is the placeholder identifier.
        NodeAst firstArg = rewritten.rightAst!; // ExprArgumentList
        Assert.AreEqual(NodeType.ExprArgumentList, firstArg.nodeType);
        Assert.AreEqual(NodeType.Identifier, firstArg.leftAst!.nodeType);
        Assert.AreEqual(aggregates[0].Placeholder, firstArg.leftAst.yytext);
    }

    [Test]
    public void Extract_CoalesceOfSum_EvaluatesCorrectly_NonNull()
    {
        NodeAst sumX = FuncCall("sum", Ident("x"));
        NodeAst ast = FuncCall("coalesce", ArgList(sumX, IntLit(0)));
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        // Simulate: SUM finalized to 42.
        Dictionary<string, ColumnValue> syntheticRow = new()
        {
            { aggregates[0].Placeholder, new ColumnValue(ColumnType.Integer64, 42L) }
        };

        ColumnValue result = Eval(rewritten, syntheticRow);
        Assert.AreEqual(42L, result.LongValue);
    }

    [Test]
    public void Extract_CoalesceOfSum_EvaluatesCorrectly_NullToZero()
    {
        // Empty-set SUM returns NULL; COALESCE must yield 0.
        NodeAst sumX = FuncCall("sum", Ident("x"));
        NodeAst ast = FuncCall("coalesce", ArgList(sumX, IntLit(0)));
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Dictionary<string, ColumnValue> syntheticRow = new()
        {
            { aggregates[0].Placeholder, ColumnValue.Null }
        };

        ColumnValue result = Eval(rewritten, syntheticRow);
        Assert.AreEqual(ColumnType.Integer64, result.Type);
        Assert.AreEqual(0L, result.LongValue);
    }

    // ── SUM(x) + 1 ──────────────────────────────────────────────────────────

    [Test]
    public void Extract_SumPlusOne_EvaluatesCorrectly()
    {
        // SUM(x) + 1
        NodeAst ast = Add(FuncCall("sum", Ident("x")), IntLit(1));
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(1, aggregates.Count);
        Assert.AreEqual(NodeType.ExprAdd, rewritten.nodeType);

        Dictionary<string, ColumnValue> syntheticRow = new()
        {
            { aggregates[0].Placeholder, new ColumnValue(ColumnType.Integer64, 10L) }
        };

        ColumnValue result = Eval(rewritten, syntheticRow);
        Assert.AreEqual(11L, result.LongValue);
    }

    // ── SUM(a) / SUM(b) — two distinct aggregates ────────────────────────────

    [Test]
    public void Extract_SumDivSum_TwoDistinctAggregates()
    {
        // SUM(a) / SUM(b) — different columns → two separate accumulators.
        NodeAst ast = Div(FuncCall("sum", Ident("a")), FuncCall("sum", Ident("b")));
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(2, aggregates.Count);
        Assert.AreNotEqual(aggregates[0].Placeholder, aggregates[1].Placeholder);
        Assert.AreEqual(NodeType.ExprDiv, rewritten.nodeType);
    }

    [Test]
    public void Extract_SumDivSum_EvaluatesCorrectly()
    {
        NodeAst ast = Div(FuncCall("sum", Ident("a")), FuncCall("sum", Ident("b")));
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        // SUM(a)=10, SUM(b)=4 → 10/4=2 (integer truncation)
        Dictionary<string, ColumnValue> syntheticRow = new()
        {
            { aggregates[0].Placeholder, new ColumnValue(ColumnType.Integer64, 10L) },
            { aggregates[1].Placeholder, new ColumnValue(ColumnType.Integer64, 4L) },
        };

        ColumnValue result = Eval(rewritten, syntheticRow);
        Assert.AreEqual(ColumnType.Integer64, result.Type);
        Assert.AreEqual(2L, result.LongValue);
    }

    // ── Deduplication: SUM(x) / SUM(x) ──────────────────────────────────────

    [Test]
    public void Extract_SumXDivSumX_DeduplicatesToOneAccumulator()
    {
        // SUM(x) / SUM(x) — identical aggregate nodes → one placeholder, one accumulator.
        NodeAst sumX1 = FuncCall("sum", Ident("x"));
        NodeAst sumX2 = FuncCall("sum", Ident("x"));
        NodeAst ast = Div(sumX1, sumX2);

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(1, aggregates.Count, "identical aggregates must share one accumulator");

        // Both sides of the division must reference the same placeholder.
        Assert.AreEqual(NodeType.ExprDiv, rewritten.nodeType);
        Assert.AreEqual(NodeType.Identifier, rewritten.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, rewritten.rightAst!.nodeType);
        Assert.AreEqual(rewritten.leftAst.yytext, rewritten.rightAst.yytext);
    }

    [Test]
    public void Extract_SumXDivSumX_EvaluatesCorrectly()
    {
        NodeAst sumX1 = FuncCall("sum", Ident("x"));
        NodeAst sumX2 = FuncCall("sum", Ident("x"));
        NodeAst ast = Div(sumX1, sumX2);
        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        Dictionary<string, ColumnValue> syntheticRow = new()
        {
            { aggregates[0].Placeholder, new ColumnValue(ColumnType.Integer64, 7L) }
        };

        ColumnValue result = Eval(rewritten, syntheticRow);
        Assert.AreEqual(1L, result.LongValue); // 7/7 = 1
    }

    // ── Alias wrapping is preserved ───────────────────────────────────────────

    [Test]
    public void Extract_AliasedSum_PreservesAliasAndExtractsInner()
    {
        // SUM(x) AS total
        NodeAst ast = Alias(FuncCall("sum", Ident("x")), "total");

        var (aggregates, rewritten) = QueryAggregateExtractor.Extract(ast);

        // The alias wrapper must be preserved; the inner expression becomes a placeholder.
        Assert.AreEqual(1, aggregates.Count);
        Assert.AreEqual(NodeType.ExprAlias, rewritten.nodeType);
        Assert.AreEqual(NodeType.Identifier, rewritten.leftAst!.nodeType);
        Assert.AreEqual(aggregates[0].Placeholder, rewritten.leftAst.yytext);
    }

    // ── Case-distinct column names are NOT deduplicated ──────────────────────

    [Test]
    public void Extract_SumColAMinusSumCola_TwoAccumulators()
    {
        // SUM(colA) - SUM(cola): colA and cola are distinct columns in a case-sensitive engine.
        // They must NOT be deduplicated, otherwise both placeholders resolve to the same sum
        // and the subtraction always returns 0 regardless of the real column values.
        NodeAst ast = new(NodeType.ExprSub,
            FuncCall("sum", Ident("colA")),
            FuncCall("sum", Ident("cola")),
            null, null, null, null, null, null);

        var (aggregates, _) = QueryAggregateExtractor.Extract(ast);

        Assert.AreEqual(2, aggregates.Count,
            "SUM(colA) and SUM(cola) reference distinct columns and must use separate accumulators");
        Assert.AreNotEqual(aggregates[0].Placeholder, aggregates[1].Placeholder);
    }

    // ── Placeholder collision guard ───────────────────────────────────────────

    [Test]
    public void Extract_PlaceholderPrefix_IsNotValidSqlIdentifier()
    {
        NodeAst ast = FuncCall("sum", Ident("x"));
        var (aggregates, _) = QueryAggregateExtractor.Extract(ast);

        // The first character of the placeholder must not be a letter, digit, or underscore
        // so it cannot be produced by the SQL lexer (identifier = [a-zA-Z_][a-zA-Z0-9_]*).
        char first = aggregates[0].Placeholder[0];
        Assert.IsFalse(char.IsLetterOrDigit(first) || first == '_',
            $"Placeholder '{aggregates[0].Placeholder}' starts with a lexable char '{first}'");
    }
}
