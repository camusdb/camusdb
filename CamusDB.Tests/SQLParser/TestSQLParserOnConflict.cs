/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Parser-level tests for <c>INSERT … ON CONFLICT</c> and <c>UPSERT INTO</c>: every INSERT form puts
/// the clause in <see cref="NodeAst.extendedThree"/> as an <see cref="NodeType.OnConflict"/> node and
/// keeps RETURNING in <see cref="NodeAst.extendedTwo"/>, and UPSERT parses into a plain INSERT.
/// </summary>
public sealed class TestSQLParserOnConflict
{
    [TestCase("INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT DO NOTHING RETURNING a", NodeType.Insert)]
    [TestCase("INSERT INTO t VALUES (1, 2) ON CONFLICT DO NOTHING RETURNING a", NodeType.Insert)]
    [TestCase("INSERT INTO t (a, b) SELECT x, y FROM s ON CONFLICT DO NOTHING RETURNING a", NodeType.InsertSelect)]
    [TestCase("INSERT INTO t SELECT x, y FROM s ON CONFLICT DO NOTHING RETURNING a", NodeType.InsertSelect)]
    [TestCase("INSERT INTO t SELECT x, y FROM s JOIN u ON s.x = u.x ON CONFLICT DO NOTHING RETURNING a", NodeType.InsertSelect)]
    [TestCase("INSERT INTO t (a) OVERRIDING SYSTEM VALUE VALUES (1) ON CONFLICT DO NOTHING RETURNING a", NodeType.Insert)]
    [TestCase("INSERT INTO t OVERRIDING USER VALUE VALUES (1) ON CONFLICT DO NOTHING RETURNING a", NodeType.Insert)]
    [TestCase("INSERT INTO t (a) OVERRIDING SYSTEM VALUE SELECT x FROM s ON CONFLICT DO NOTHING RETURNING a", NodeType.InsertSelect)]
    [TestCase("INSERT INTO t OVERRIDING USER VALUE SELECT x FROM s ON CONFLICT DO NOTHING RETURNING a", NodeType.InsertSelect)]
    public void EveryInsertFormCarriesTheClauseInExtendedThree(string sql, NodeType expected)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, ast.nodeType);
        Assert.AreEqual(NodeType.OnConflict, ast.extendedThree!.nodeType);
        Assert.AreEqual("nothing", ast.extendedThree.yytext);
        Assert.IsNull(ast.extendedThree.leftAst);
        Assert.AreEqual("a", StatementScope.GetReturningList(ast)!.yytext);
    }

    [Test]
    public void ColumnTargetAndDoUpdateWithExcludedAndWhere()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "insert into t (k, n) values (1, 2) on conflict (k, n) do update set n = t.n + excluded.n, m = 3 where t.n < 10");

        NodeAst clause = ast.extendedThree!;
        Assert.AreEqual(NodeType.OnConflict, clause.nodeType);
        Assert.AreEqual("update", clause.yytext);
        Assert.AreEqual(NodeType.IdentifierList, clause.leftAst!.nodeType);
        Assert.AreEqual(NodeType.UpdateList, clause.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprLessThan, clause.extendedOne!.nodeType);
        Assert.AreEqual("t.n", clause.extendedOne.leftAst!.yytext);

        NodeAst firstItem = clause.rightAst.leftAst!;
        Assert.AreEqual(NodeType.UpdateItem, firstItem.nodeType);
        Assert.AreEqual(NodeType.ExprAdd, firstItem.rightAst!.nodeType);
        Assert.AreEqual("excluded.n", firstItem.rightAst.rightAst!.yytext);
        Assert.IsNull(ast.extendedTwo);
    }

    [Test]
    public void OnConstraintTarget()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO t (k) VALUES (1) ON CONFLICT ON CONSTRAINT t_k_key DO NOTHING");

        NodeAst target = ast.extendedThree!.leftAst!;
        Assert.AreEqual(NodeType.OnConflictConstraint, target.nodeType);
        Assert.AreEqual("t_k_key", target.yytext);
    }

    [Test]
    public void DoUpdateWithoutTargetIsRefused()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("INSERT INTO t (k) VALUES (1) ON CONFLICT DO UPDATE SET k = 2"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("requires a conflict target", ex.Message);
    }

    [TestCase("INSERT INTO t (k) VALUES (1) ON CONFLICTS DO NOTHING")]
    [TestCase("INSERT INTO t (k) VALUES (1) ON CONFLICT DONT NOTHING")]
    [TestCase("INSERT INTO t (k) VALUES (1) ON CONFLICT DO NOTHINGS")]
    [TestCase("INSERT INTO t (k) VALUES (1) ON CONFLICT (k) MAKE UPDATE SET k = 1")]
    [TestCase("UPSERTS INTO t (k) VALUES (1)")]
    public void MisspelledWordsAreRefused(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [TestCase("UPSERT INTO t (k, v) VALUES (1, 2) RETURNING k", NodeType.Insert, true)]
    [TestCase("upsert into t values (1, 2) returning k", NodeType.Insert, false)]
    [TestCase("UPSERT INTO t (k, v) SELECT x, y FROM s RETURNING k", NodeType.InsertSelect, true)]
    [TestCase("UPSERT INTO t SELECT x, y FROM s RETURNING k", NodeType.InsertSelect, false)]
    public void UpsertParsesIntoAnInsertWithAnUpsertClause(string sql, NodeType expected, bool hasColumns)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, ast.nodeType);
        Assert.AreEqual(NodeType.OnConflict, ast.extendedThree!.nodeType);
        Assert.AreEqual("upsert", ast.extendedThree.yytext);
        Assert.AreEqual(hasColumns, ast.rightAst is not null);
        Assert.AreEqual("k", StatementScope.GetReturningList(ast)!.yytext);
    }

    [Test]
    public void UpsertAndConflictWordsStayUsableAsNames()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO upsert (conflict, nothing, excluded) VALUES (1, 2, 3)");

        Assert.AreEqual("upsert", ast.leftAst!.yytext);
        Assert.IsNull(ast.extendedThree);

        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT conflict, nothing, excluded FROM upsert"));
    }

    [Test]
    public void PlainInsertHasNoClause()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1) RETURNING a");
        Assert.IsNull(ast.extendedThree);
    }
}
