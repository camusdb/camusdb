/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Parser-level coverage for the two defenses against a statement tree deep enough to overflow the
/// stack: <see cref="ExpressionChains"/>, which rebalances long OR/AND chains, IN lists and ARRAY
/// literals, and <see cref="StatementDepthGuard"/>, which refuses whatever is still too deep.
///
/// <para>The rebalance is only correct if nothing observable changes except depth, so these tests
/// pin source order — operands, IN values, ARRAY elements and placeholder names — as well as depth,
/// and pin that short chains keep the exact shape the grammar gives them.</para>
/// </summary>
public sealed class TestExpressionChains
{
    private const int LongChain = 5_000;

    [Test]
    public void Balance_PreservesOrder_AndDepthIsLogarithmic()
    {
        for (int count = 1; count <= 200; count++)
        {
            List<NodeAst> operands = Enumerable.Range(0, count).Select(Leaf).ToList();

            NodeAst tree = ExpressionChains.Balance(NodeType.ExprOr, operands);

            List<NodeAst> flattened = [];
            ExpressionChains.Flatten(tree, NodeType.ExprOr, flattened);

            CollectionAssert.AreEqual(operands, flattened, $"order changed for {count} operands");
            Assert.AreEqual((int)Math.Ceiling(Math.Log2(count)), Depth(tree) - 1, $"depth for {count} operands");
        }
    }

    [Test]
    public void Combine_KeepsShortChainsLeftDeep_AndBalancesLongOnes()
    {
        List<NodeAst> shortList = Enumerable.Range(0, ExpressionChains.BalanceThreshold).Select(Leaf).ToList();
        NodeAst shortTree = ExpressionChains.Combine(NodeType.ExprAnd, shortList)!;
        Assert.AreEqual(ExpressionChains.BalanceThreshold, Depth(shortTree), "a short chain keeps its left-deep shape");

        List<NodeAst> longList = Enumerable.Range(0, ExpressionChains.BalanceThreshold + 1).Select(Leaf).ToList();
        NodeAst longTree = ExpressionChains.Combine(NodeType.ExprAnd, longList)!;
        Assert.Less(Depth(longTree), 10, "a long chain is balanced");

        Assert.IsNull(ExpressionChains.Combine(NodeType.ExprAnd, []));
    }

    [Test]
    public void LongOrChain_IsBalanced_AndKeepsSourceOrder()
    {
        NodeAst where = SQLParserProcessor.ParseCondition(Chain(LongChain, " OR ", i => $"v = {i}"));

        Assert.AreEqual(NodeType.ExprOr, where.nodeType);
        Assert.LessOrEqual(Depth(where), 20, "a 5,000-term chain must be about log2(5,000) deep, not 5,000");

        List<NodeAst> terms = [];
        ExpressionChains.Flatten(where, NodeType.ExprOr, terms);

        Assert.AreEqual(LongChain, terms.Count);

        for (int i = 0; i < LongChain; i++)
        {
            Assert.AreEqual(NodeType.ExprEquals, terms[i].nodeType);
            Assert.AreEqual(i.ToString(), terms[i].rightAst!.yytext, $"term {i} moved");
        }
    }

    [Test]
    public void ShortOrChain_KeepsTheGrammarShape()
    {
        int terms = ExpressionChains.BalanceThreshold;
        NodeAst where = SQLParserProcessor.ParseCondition(Chain(terms, " OR ", i => $"v = {i}"));

        // Left-deep: the last term is the root's right child, and the left spine is terms - 1 long.
        Assert.AreEqual((terms - 1).ToString(), where.rightAst!.rightAst!.yytext);

        int spine = 0;
        for (NodeAst? node = where; node is { nodeType: NodeType.ExprOr }; node = node.leftAst)
            spine++;

        Assert.AreEqual(terms - 1, spine);
    }

    [Test]
    public void LongOrOfAnds_KeepsEachConjunctionIntact()
    {
        NodeAst where = SQLParserProcessor.ParseCondition(
            Chain(1_000, " OR ", i => $"a = {i} AND b = {i}"));

        List<NodeAst> disjuncts = [];
        ExpressionChains.Flatten(where, NodeType.ExprOr, disjuncts);

        Assert.AreEqual(1_000, disjuncts.Count);

        for (int i = 0; i < disjuncts.Count; i++)
        {
            NodeAst disjunct = disjuncts[i];
            Assert.AreEqual(NodeType.ExprAnd, disjunct.nodeType, $"disjunct {i}");
            Assert.AreEqual("a", disjunct.leftAst!.leftAst!.yytext);
            Assert.AreEqual(i.ToString(), disjunct.leftAst!.rightAst!.yytext);
            Assert.AreEqual("b", disjunct.rightAst!.leftAst!.yytext);
        }
    }

    [Test]
    public void LongAndChain_IsBalanced()
    {
        NodeAst where = SQLParserProcessor.ParseCondition(Chain(LongChain, " AND ", i => $"v <> {i}"));

        Assert.AreEqual(NodeType.ExprAnd, where.nodeType);
        Assert.LessOrEqual(Depth(where), 20);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void LongInList_IsBalanced_AndKeepsValueOrder(bool negated)
    {
        NodeAst where = SQLParserProcessor.ParseCondition(
            (negated ? "v NOT IN (" : "v IN (") + Chain(LongChain, ", ", i => i.ToString()) + ")");

        Assert.AreEqual(negated ? NodeType.ExprNotInMembership : NodeType.ExprInMembership, where.nodeType);
        Assert.LessOrEqual(Depth(where), 20);

        List<NodeAst> values = [];
        ExpressionChains.Flatten(where.rightAst!, NodeType.ExprList, values);

        CollectionAssert.AreEqual(
            Enumerable.Range(0, LongChain).Select(i => i.ToString()).ToList(),
            values.Select(v => v.yytext).ToList());
    }

    [Test]
    public void LongArrayLiteral_IsBalanced_AndKeepsElementOrder()
    {
        NodeAst array = SQLParserProcessor.ParseCondition("ARRAY[" + Chain(LongChain, ", ", i => i.ToString()) + "]");

        Assert.AreEqual(NodeType.ArrayLiteral, array.nodeType);
        Assert.LessOrEqual(Depth(array), 20);

        ColumnValue value = SQLExecutorBaseCreator.EvalExpr(array, new Dictionary<string, ColumnValue>(), null);

        CollectionAssert.AreEqual(
            Enumerable.Range(0, LongChain).Select(i => (long)i).ToList(),
            value.ArrayValues!.Select(v => v.LongValue).ToList());
    }

    [Test]
    public void LongOrChain_KeepsPlaceholderOrder()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM t WHERE " + Chain(1_000, " OR ", i => $"v = @p{i}"));

        CollectionAssert.AreEqual(
            Enumerable.Range(0, 1_000).Select(i => $"@p{i}").ToArray(),
            PlaceholderCollector.Collect(ast));
    }

    [Test]
    public void DeepArithmetic_BelowTheLimit_IsAccepted()
    {
        SQLParserProcessor.Parse(
            "SELECT id FROM t WHERE v" + Repeat(" + 1", StatementDepthGuard.MaxDepth - 10) + " > 0");
    }

    [Test]
    public void DeepArithmetic_AboveTheLimit_IsRefused()
    {
        AssertTooDeep("SELECT id FROM t WHERE v" + Repeat(" + 1", StatementDepthGuard.MaxDepth + 1) + " > 0");
    }

    [Test]
    public void DeepFunctionNesting_IsRefused()
    {
        int levels = StatementDepthGuard.MaxDepth + 1;
        AssertTooDeep("SELECT id FROM t WHERE " + Repeat("abs(", levels) + "v" + Repeat(")", levels) + " > 0");
    }

    [Test]
    public void DeepParenthesizedNesting_IsRefused()
    {
        int levels = StatementDepthGuard.MaxDepth + 1;
        AssertTooDeep("SELECT id FROM t WHERE v" + Repeat(" + (1", levels) + Repeat(")", levels) + " > 0");
    }

    [Test]
    public void DeepNotChain_IsRefused()
    {
        AssertTooDeep("SELECT id FROM t WHERE " + Repeat("NOT ", StatementDepthGuard.MaxDepth + 1) + "v > 0");
    }

    [Test]
    public void CaseWithTooManyBranches_IsRefused()
    {
        // The WHEN list stays left-deep (its evaluator depends on that shape), so the guard bounds it.
        AssertTooDeep(
            "SELECT id FROM t WHERE CASE " + Repeat("WHEN v = 1 THEN 1 ", StatementDepthGuard.MaxDepth + 1) + "ELSE 0 END = 1");
    }

    [Test]
    public void ManyRowInsert_IsNotCountedAsDepth()
    {
        const int Rows = 10_000;

        StringBuilder sb = new("INSERT INTO t (id, v) VALUES ");
        for (int i = 0; i < Rows; i++)
        {
            if (i > 0)
                sb.Append(", ");
            sb.Append('(').Append(i).Append(", ").Append(i).Append(')');
        }

        NodeAst ast = SQLParserProcessor.Parse(sb.ToString());

        Assert.AreEqual(NodeType.Insert, ast.nodeType);
    }

    [Test]
    public void TooDeeplyNested_IsAClientError()
    {
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.StatementTooDeeplyNested));
    }

    private static void AssertTooDeep(string sql)
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));
        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, ex!.Code);
        StringAssert.Contains(StatementDepthGuard.MaxDepth.ToString(), ex.Message);
    }

    private static NodeAst Leaf(int i) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, i.ToString());

    private static string Chain(int count, string separator, Func<int, string> term) =>
        string.Join(separator, Enumerable.Range(0, count).Select(term));

    private static string Repeat(string text, int count) => new StringBuilder(text.Length * count).Insert(0, text, count).ToString();

    /// <summary>Depth of the tree through <c>leftAst</c>/<c>rightAst</c>, measured iteratively.</summary>
    private static int Depth(NodeAst root)
    {
        int max = 0;
        Stack<(NodeAst Node, int Depth)> pending = new();
        pending.Push((root, 1));

        while (pending.Count > 0)
        {
            (NodeAst node, int depth) = pending.Pop();
            max = Math.Max(max, depth);

            if (node.leftAst is not null) pending.Push((node.leftAst, depth + 1));
            if (node.rightAst is not null) pending.Push((node.rightAst, depth + 1));
        }

        return max;
    }
}
