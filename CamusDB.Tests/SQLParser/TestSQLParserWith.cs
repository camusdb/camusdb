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
/// The parser writes out every WITH clause: each reference to a CTE becomes a derived table over a copy
/// of the CTE's query, so no tree that leaves the parser holds a WITH node. A CTE at the start of the
/// statement that is read two or more times, or written AS MATERIALIZED, gets one marker instance on
/// every reference; a CTE in parentheses never does.
/// </summary>
public sealed class TestSQLParserWith
{
    private static List<NodeAst> DerivedTables(NodeAst root)
    {
        List<NodeAst> found = [];
        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();
            Assert.AreNotEqual(NodeType.With, node.nodeType, "no WITH node leaves the parser");

            if (node.nodeType == NodeType.DerivedTableReference)
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

    private static List<string> Integers(NodeAst root)
    {
        List<string> found = [];
        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();
            if (node.nodeType == NodeType.Integer)
                found.Add(node.yytext!);

            foreach (NodeAst? child in new[] { node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo, node.extendedThree,
                         node.extendedFour, node.extendedFive, node.extendedSix, node.extendedSeven })
            {
                if (child is not null)
                    pending.Push(child);
            }
        }

        return found;
    }

    [Test]
    public void AReferenceBecomesADerivedTableNamedAfterTheCte()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT id FROM a WHERE id > 1");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.DerivedTableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("a", ast.rightAst.rightAst!.yytext);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst.leftAst!.rightAst!.nodeType);
        Assert.AreEqual("t", ast.rightAst.leftAst.rightAst.leftAst!.yytext);
        Assert.IsNull(ast.rightAst.extendedFour, "a CTE read once is inlined");
    }

    [Test]
    public void TheAliasOfTheReferenceNamesTheDerivedTable()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT x.id FROM a AS x");

        Assert.AreEqual("x", ast.rightAst!.rightAst!.yytext);
    }

    [Test]
    public void ACteReadTwiceHasOneMarkerAndTwoCopies()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT * FROM a x JOIN a y ON x.id = y.id");

        List<NodeAst> derived = DerivedTables(ast);
        Assert.AreEqual(2, derived.Count);
        Assert.IsNotNull(derived[0].extendedFour);
        Assert.AreEqual(NodeType.CteMaterialization, derived[0].extendedFour!.nodeType);
        Assert.AreEqual("a", derived[0].extendedFour!.yytext);
        Assert.AreSame(derived[0].extendedFour, derived[1].extendedFour, "every reference holds the same marker");
        Assert.AreNotSame(derived[0].leftAst, derived[1].leftAst, "each reference has its own copy of the query");
    }

    [Test]
    public void AReadInASubqueryCountsAsAReference()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT id FROM a WHERE id IN (SELECT id FROM a)");

        List<NodeAst> derived = DerivedTables(ast);
        Assert.AreEqual(2, derived.Count);
        Assert.AreSame(derived[0].extendedFour, derived[1].extendedFour);
        Assert.IsNotNull(derived[0].extendedFour);
    }

    [Test]
    public void NotMaterializedInlinesAndMaterializedMarks()
    {
        List<NodeAst> twice = DerivedTables(SQLParserProcessor.Parse(
            "WITH a AS NOT MATERIALIZED (SELECT id FROM t) SELECT * FROM a x, a y"));
        Assert.AreEqual(2, twice.Count);
        Assert.IsNull(twice[0].extendedFour);
        Assert.IsNull(twice[1].extendedFour);

        List<NodeAst> once = DerivedTables(SQLParserProcessor.Parse(
            "WITH a AS MATERIALIZED (SELECT id FROM t) SELECT * FROM a"));
        Assert.AreEqual(1, once.Count);
        Assert.IsNotNull(once[0].extendedFour);
    }

    [Test]
    public void ALaterCteReadsAnEarlierOne()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a) SELECT id FROM b");

        NodeAst b = ast.rightAst!;
        Assert.AreEqual("b", b.rightAst!.yytext);
        NodeAst a = b.leftAst!.rightAst!;
        Assert.AreEqual(NodeType.DerivedTableReference, a.nodeType);
        Assert.AreEqual("a", a.rightAst!.yytext);
        Assert.AreEqual("t", a.leftAst!.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void ACteDoesNotSeeItselfOrALaterCte()
    {
        // The name in a CTE's own query, and a name of a later CTE, read the table of that name.
        NodeAst self = SQLParserProcessor.Parse("WITH t AS (SELECT id FROM t) SELECT id FROM t");
        Assert.AreEqual(NodeType.DerivedTableReference, self.rightAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, self.rightAst.leftAst!.rightAst!.nodeType);

        NodeAst forward = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM b), b AS (SELECT 1 AS id) SELECT id FROM a");
        Assert.AreEqual(NodeType.TableReference, forward.rightAst!.leftAst!.rightAst!.nodeType);
        Assert.AreEqual("b", forward.rightAst.leftAst.rightAst.leftAst!.yytext);
    }

    [Test]
    public void AWithInParenthesesIsInlinedAndHidesAnOuterName()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "WITH a AS (SELECT 1 AS id) SELECT * FROM (WITH a AS (SELECT 2 AS id) SELECT id FROM a, a b) d");

        List<NodeAst> derived = DerivedTables(ast);

        // d, and the two inner references; the outer a is not read at all.
        Assert.AreEqual(3, derived.Count);
        foreach (NodeAst node in derived)
            Assert.IsNull(node.extendedFour, "a CTE in parentheses is never computed once");

        NodeAst inner = ast.rightAst!.leftAst!;
        NodeAst firstA = inner.rightAst!.leftAst!;
        CollectionAssert.AreEqual(new[] { "2" }, Integers(firstA), "the inner a hides the outer a");
    }

    [Test]
    public void TheColumnListRidesOnTheDerivedTable()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH a (x, y) AS (SELECT id, v FROM t) SELECT x FROM a");

        NodeAst columns = ast.rightAst!.extendedThree!;
        Assert.AreEqual(NodeType.IdentifierList, columns.nodeType);
        Assert.AreEqual("x", columns.leftAst!.yytext);
        Assert.AreEqual("y", columns.rightAst!.yytext);
    }

    [Test]
    public void ADerivedTableTakesAColumnList()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT a, b FROM (SELECT 1, 2) AS d (a, b)");
        Assert.AreEqual("d", ast.rightAst!.rightAst!.yytext);
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst.extendedThree!.nodeType);

        NodeAst bare = SQLParserProcessor.Parse("SELECT a FROM (SELECT 1) d (a)");
        Assert.AreEqual("a", bare.rightAst!.extendedThree!.yytext);
    }

    [Test]
    public void TheTargetOfAWriteIsNeverACte()
    {
        NodeAst update = SQLParserProcessor.Parse("WITH t AS (SELECT 1 AS id) UPDATE t SET v = 1 FROM t AS s WHERE s.id = t.id");
        Assert.AreEqual(NodeType.Update, update.nodeType);
        Assert.AreEqual("t", update.leftAst!.yytext);
        Assert.AreEqual(NodeType.DerivedTableReference, DmlJoinSlots.RelationList(update)!.nodeType);

        NodeAst delete = SQLParserProcessor.Parse("WITH s AS (SELECT 1 AS id) DELETE FROM t USING s WHERE s.id = t.id");
        Assert.AreEqual(NodeType.Delete, delete.nodeType);
        Assert.AreEqual(NodeType.DerivedTableReference, DmlJoinSlots.RelationList(delete)!.nodeType);

        NodeAst insert = SQLParserProcessor.Parse("WITH t AS (SELECT 1 AS id) INSERT INTO t SELECT id FROM t");
        Assert.AreEqual(NodeType.InsertSelect, insert.nodeType);
        Assert.AreEqual("t", insert.leftAst!.yytext);
        Assert.AreEqual(NodeType.DerivedTableReference, insert.extendedOne!.rightAst!.nodeType);

        NodeAst source = SQLParserProcessor.Parse("INSERT INTO t (id) WITH s AS (SELECT 1 AS id) SELECT id FROM s");
        Assert.AreEqual(NodeType.InsertSelect, source.nodeType);
        Assert.AreEqual(NodeType.DerivedTableReference, source.extendedOne!.rightAst!.nodeType);
    }

    [Test]
    public void AWithKeepsItsOrderByLimitAndSetOperation()
    {
        NodeAst ordered = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT id FROM a ORDER BY id LIMIT 3");
        Assert.IsNotNull(ordered.extendedTwo, "ORDER BY");
        Assert.IsNotNull(ordered.extendedThree, "LIMIT");

        NodeAst union = SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT id FROM a UNION ALL SELECT id FROM a");
        Assert.IsTrue(SetOperationAst.IsSetOperationSelect(union));
        Assert.AreEqual(2, DerivedTables(union).Count);

        NodeAst operand = SQLParserProcessor.Parse("(WITH a AS (SELECT 1 AS id) SELECT id FROM a) UNION SELECT 2");
        Assert.IsTrue(SetOperationAst.IsSetOperationSelect(operand));
    }

    [Test]
    public void AWithWorksInEveryQueryPosition()
    {
        string[] statements =
        [
            "SELECT * FROM t WHERE id IN (WITH a AS (SELECT 1 AS id) SELECT id FROM a)",
            "SELECT * FROM t WHERE EXISTS (WITH a AS (SELECT 1 AS id) SELECT id FROM a)",
            "SELECT (WITH a AS (SELECT 1 AS id) SELECT id FROM a)",
            "EXPLAIN WITH a AS (SELECT 1 AS id) SELECT id FROM a",
            "WITH a AS (SELECT id FROM t) SELECT id FROM a FOR UPDATE",
            "CREATE VIEW v AS WITH a AS (SELECT id FROM t) SELECT id FROM a",
            "CREATE TABLE c AS WITH a AS (SELECT id FROM t) SELECT id FROM a",
            "WITH a AS (SELECT id FROM t) UPSERT INTO t SELECT id FROM a",
            "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a) DELETE FROM t WHERE id IN (SELECT id FROM b)",
        ];

        foreach (string sql in statements)
            Assert.IsNotEmpty(DerivedTables(SQLParserProcessor.Parse(sql)), sql);
    }

    [Test]
    public void ADuplicateNameARecursiveWithAndAnIndexHintAreRefused()
    {
        CamusDBException duplicate = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("WITH a AS (SELECT 1), A AS (SELECT 2) SELECT * FROM a"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, duplicate.Code);
        StringAssert.Contains("more than once", duplicate.Message);

        CamusDBException recursive = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("WITH RECURSIVE a AS (SELECT 1) SELECT * FROM a"))!;
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, recursive.Code);

        CamusDBException hint = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("WITH a AS (SELECT id FROM t) SELECT * FROM a@{FORCE_INDEX=i}"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, hint.Code);

        CamusDBException qualified = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("WITH d.a AS (SELECT 1) SELECT * FROM t"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, qualified.Code);
    }

    [Test]
    public void ACteNamedLikeAKeywordWordStillWorks()
    {
        NodeAst ast = SQLParserProcessor.Parse("WITH recursive AS (SELECT 1 AS id) SELECT id FROM recursive");
        Assert.AreEqual(NodeType.DerivedTableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void AChainThatDoublesAtEachStepIsRefused()
    {
        StringBuilder sql = new("WITH c0 AS (SELECT 1 AS id)");
        for (int i = 1; i <= 24; i++)
            sql.Append($", c{i} AS (SELECT x.id FROM c{i - 1} x, c{i - 1} y)");
        sql.Append(" SELECT id FROM c24");

        // Inlined, the chain would copy the query of c0 2^24 times. Only the CTE that each step reads
        // twice is computed once, but each reference still holds a copy of its query.
        CamusDBException refused = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql.ToString()))!;
        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, refused.Code);
    }
}
