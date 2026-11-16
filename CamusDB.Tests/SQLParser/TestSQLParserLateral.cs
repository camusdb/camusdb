/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The parse forms of <c>LATERAL</c>: the keyword marks a derived table, a function in <c>FROM</c> whose
/// arguments name a column becomes a marked derived table around the call with or without the keyword, and
/// <c>LATERAL</c> is a reserved word.
/// </summary>
public sealed class TestSQLParserLateral
{
    private static List<NodeAst> Find(NodeAst root, NodeType type)
    {
        List<NodeAst> found = [];
        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == type)
                found.Add(node);

            foreach (NodeAst? child in new[] { node.extendedSeven, node.extendedSix, node.extendedFive, node.extendedFour,
                         node.extendedThree, node.extendedTwo, node.extendedOne, node.rightAst, node.leftAst })
            {
                if (child is not null)
                    pending.Push(child);
            }
        }

        return found;
    }

    [TestCase("SELECT a.id FROM a, LATERAL (SELECT a.v AS w) s")]
    [TestCase("SELECT a.id FROM a CROSS JOIN LATERAL (SELECT a.v AS w) s")]
    [TestCase("SELECT a.id FROM a JOIN LATERAL (SELECT a.v AS w) s ON s.w > 1")]
    [TestCase("SELECT a.id FROM a LEFT JOIN LATERAL (SELECT a.v AS w) AS s ON true")]
    [TestCase("SELECT a.id FROM a, lateral (SELECT a.v, a.v) AS s (x, y)")]
    public void TheKeywordMarksTheDerivedTable(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        List<NodeAst> derived = Find(ast, NodeType.DerivedTableReference);
        Assert.That(derived, Has.Count.EqualTo(1));
        Assert.That(LateralAst.IsLateral(derived[0]), Is.True);
    }

    [Test]
    public void ADerivedTableWithoutTheKeywordIsNotMarked()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT a.id FROM a, (SELECT 1 AS w) s");

        List<NodeAst> derived = Find(ast, NodeType.DerivedTableReference);
        Assert.That(derived, Has.Count.EqualTo(1));
        Assert.That(LateralAst.IsLateral(derived[0]), Is.False);
    }

    [TestCase("SELECT * FROM a, generate_series(1, a.n) AS g")]
    [TestCase("SELECT * FROM a, LATERAL generate_series(1, a.n) AS g")]
    [TestCase("SELECT * FROM a CROSS JOIN generate_series(1, n + 1) g")]
    [TestCase("SELECT * FROM a, generate_series(1, CAST(a.n AS int64)) AS g (k)")]
    public void AFunctionThatNamesAColumnIsWrappedInAMarkedDerivedTable(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        List<NodeAst> derived = Find(ast, NodeType.DerivedTableReference);
        Assert.That(derived, Has.Count.EqualTo(1));
        Assert.That(LateralAst.IsLateral(derived[0]), Is.True);

        NodeAst? call = LateralAst.WrappedFunction(derived[0]);
        Assert.That(call, Is.Not.Null);
        Assert.That(LateralAst.IsLateralFunction(call!), Is.True);
        Assert.That(derived[0].rightAst!.yytext, Is.EqualTo("g"));
    }

    [TestCase("SELECT * FROM a, generate_series(1, 10) AS g")]
    [TestCase("SELECT * FROM a, generate_series(1, upper('x') || @p) AS g")]
    [TestCase("SELECT * FROM a, LATERAL generate_series(1, 3) AS g")]
    public void AFunctionThatNamesNoColumnStaysACall(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.That(Find(ast, NodeType.DerivedTableReference), Is.Empty);

        List<NodeAst> calls = Find(ast, NodeType.TableFunctionReference);
        Assert.That(calls, Has.Count.EqualTo(1));
        Assert.That(LateralAst.IsLateralFunction(calls[0]), Is.False);
    }

    [Test]
    public void AnUnaliasedFunctionWrapIsNamedAfterTheFunction()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM a, generate_series(1, a.n)");

        List<NodeAst> derived = Find(ast, NodeType.DerivedTableReference);
        Assert.That(derived[0].rightAst!.yytext, Is.EqualTo("generate_series"));
    }

    [TestCase("SELECT lateral FROM a")]
    [TestCase("CREATE TABLE lateral (id int64 PRIMARY KEY)")]
    [TestCase("SELECT * FROM a, LATERAL b")]
    public void LateralIsReserved(string sql)
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));
    }

    [Test]
    public void AnEscapedLateralIsAName()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `lateral` FROM a");

        Assert.That(Find(ast, NodeType.Identifier).Exists(n => n.yytext == "lateral"), Is.True);
    }
}
