/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Text;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// The parse forms of a <c>VALUES</c> list in <c>FROM</c>: a marked derived table over a <c>UNION ALL</c> of
/// FROM-less SELECTs whose first row names the columns, the <c>LATERAL</c> mark next to the <c>VALUES</c>
/// mark, and the rules that the parser checks.
/// </summary>
public sealed class TestSQLParserValues
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

    private static NodeAst OnlyValues(string sql)
    {
        List<NodeAst> derived = Find(SQLParserProcessor.Parse(sql), NodeType.DerivedTableReference);
        Assert.That(derived, Has.Count.EqualTo(1), sql);
        Assert.That(ValuesAst.IsValues(derived[0]), Is.True, sql);
        return derived[0];
    }

    [TestCase("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v (id, k)")]
    [TestCase("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v (id, k)")]
    [TestCase("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v")]
    [TestCase("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v")]
    [TestCase("SELECT * FROM t JOIN (values (1), (2)) AS v (id) ON v.id = t.id")]
    [TestCase("UPDATE d SET v = x.v FROM (VALUES (1, 5)) AS x (id, v) WHERE x.id = d.id")]
    [TestCase("DELETE FROM d USING (VALUES (1)) AS x (id) WHERE x.id = d.id")]
    public void EachFormIsAMarkedDerivedTable(string sql)
    {
        NodeAst values = OnlyValues(sql);

        Assert.That(LateralAst.IsLateral(values), Is.False);
    }

    [Test]
    public void TheRowsBecomeAUnionAllWhoseFirstRowNamesTheColumns()
    {
        NodeAst values = OnlyValues("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v (id, k)");

        Assert.That(values.rightAst!.yytext, Is.EqualTo("v"));
        Assert.That(values.extendedThree, Is.Not.Null);
        Assert.That(SetOperationAst.IsSetOperationSelect(values.leftAst!), Is.True);
        Assert.That(ValuesAst.RowCount(values), Is.EqualTo(3));

        List<List<NodeAst>> rows = ValuesAst.Rows(values);
        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows[0][0].yytext, Is.EqualTo("1"));
        Assert.That(rows[2][1].yytext, Is.EqualTo("'c'"));

        List<NodeAst> aliases = Find(values, NodeType.ExprAlias);
        Assert.That(aliases.ConvertAll(a => a.rightAst!.yytext), Is.EquivalentTo(new[] { "column1", "column2" }));
        Assert.That(Find(values, NodeType.SetOperationReference).TrueForAll(SetOperationAst.IsAll), Is.True);
    }

    [Test]
    public void OneRowIsOneSelect()
    {
        NodeAst values = OnlyValues("SELECT * FROM (VALUES (1)) AS v");

        Assert.That(values.leftAst!.nodeType, Is.EqualTo(NodeType.Select));
        Assert.That(values.leftAst.rightAst, Is.Null);
        Assert.That(ValuesAst.RowCount(values), Is.EqualTo(1));
    }

    [TestCase("SELECT * FROM a, LATERAL (VALUES (a.x), (a.y)) AS s (n)")]
    [TestCase("SELECT * FROM a CROSS JOIN LATERAL (VALUES (a.x)) s")]
    [TestCase("SELECT * FROM a LEFT JOIN LATERAL (VALUES (a.x)) AS s (n) ON true")]
    public void TheLateralKeywordAddsItsMark(string sql)
    {
        NodeAst values = OnlyValues(sql);

        Assert.That(LateralAst.IsLateral(values), Is.True);
    }

    [Test]
    public void ALongListIsRebalanced()
    {
        StringBuilder sql = new("SELECT * FROM (VALUES ");
        for (int i = 0; i < 1000; i++)
            sql.Append(i == 0 ? "" : ", ").Append('(').Append(i).Append(')');
        sql.Append(") AS v");

        NodeAst values = OnlyValues(sql.ToString());

        Assert.That(ValuesAst.RowCount(values), Is.EqualTo(1000));

        List<List<NodeAst>> rows = ValuesAst.Rows(values);
        for (int i = 0; i < rows.Count; i++)
            Assert.That(rows[i][0].yytext, Is.EqualTo(i.ToString()));
    }

    [TestCase("SELECT * FROM (VALUES (1, 2), (3)) AS v", "VALUES lists must all be the same length")]
    [TestCase("SELECT * FROM (VALUES (SUM(1))) AS v", "Aggregate functions are not allowed in VALUES")]
    [TestCase("SELECT * FROM (VALUES (1 + COUNT(*))) AS v", "Aggregate functions are not allowed in VALUES")]
    [TestCase("SELECT * FROM (VALUES (rank() OVER ())) AS v", "Window functions are not allowed in VALUES")]
    [TestCase("SELECT * FROM (VALUES (DEFAULT)) AS v", "DEFAULT is not allowed in a VALUES list in FROM")]
    public void TheRulesOfAListAreChecked(string sql, string message)
    {
        CamusDBException? error = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        StringAssert.Contains(message, error.Message);
    }

    [Test]
    public void AnAggregateInsideASubqueryItemIsAccepted()
    {
        OnlyValues("SELECT * FROM (VALUES ((SELECT COUNT(*) FROM t))) AS v");
    }

    [TestCase("SELECT * FROM (VALUES (1))")]
    [TestCase("VALUES (1), (2)")]
    [TestCase("WITH c AS (VALUES (1)) SELECT * FROM c")]
    public void FormsThatAreNotSupportedAreSyntaxErrors(string sql)
    {
        CamusDBException? error = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.SqlSyntaxError));
    }
}
