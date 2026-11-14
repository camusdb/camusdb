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
/// Parser-level tests for <c>UPDATE … FROM</c>, <c>DELETE … USING</c> and the target alias. The two
/// new parts sit in the slots after RETURNING, so every older consumer of the node reads it as before;
/// every check reads them through <see cref="DmlJoinSlots"/>, as the engine does.
/// </summary>
public sealed class TestSQLParserUpdateFromDeleteUsing
{
    [Test]
    public void UpdateFromKeepsTheOlderSlotsAndAddsTheListAndTheAlias()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE accounts a SET balance = d.amount FROM deltas d WHERE d.id = a.id RETURNING a.id");

        Assert.AreEqual(NodeType.Update, ast.nodeType);
        Assert.AreEqual("accounts", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.UpdateItem, ast.rightAst!.nodeType);
        Assert.IsNotNull(ast.extendedOne, "the WHERE");
        Assert.IsNull(ast.extendedTwo, "no LIMIT");
        Assert.AreEqual("a.id", StatementScope.GetReturningList(ast)!.yytext);
        Assert.AreEqual(NodeType.TableReference, DmlJoinSlots.RelationList(ast)!.nodeType);
        Assert.AreEqual("a", DmlJoinSlots.TargetAlias(ast)!.yytext);
        Assert.IsTrue(DmlJoinSlots.IsJoined(ast));
    }

    [Test]
    public void DeleteUsingKeepsTheOlderSlotsAndAddsTheListAndTheAlias()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM orders AS o USING customers c, regions r WHERE c.id = o.customer_id RETURNING o.id");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);
        Assert.AreEqual("orders", ast.leftAst!.yytext);
        Assert.IsNotNull(ast.rightAst, "the WHERE");
        Assert.IsNull(ast.extendedOne, "no LIMIT");
        Assert.AreEqual("o.id", StatementScope.GetReturningList(ast)!.yytext);
        Assert.AreEqual(NodeType.CommaJoin, DmlJoinSlots.RelationList(ast)!.nodeType);
        Assert.AreEqual("o", DmlJoinSlots.TargetAlias(ast)!.yytext);
    }

    [TestCase("UPDATE t SET a = 1 WHERE b = 2")]
    [TestCase("UPDATE t SET a = 1 WHERE b = 2 LIMIT 3 RETURNING a")]
    [TestCase("DELETE FROM t WHERE b = 2")]
    [TestCase("DELETE FROM t WHERE b = 2 LIMIT 3 RETURNING a")]
    public void APlainStatementIsNotJoined(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.IsFalse(DmlJoinSlots.IsJoined(ast));
        Assert.IsNull(DmlJoinSlots.RelationList(ast));
        Assert.IsNull(DmlJoinSlots.TargetAlias(ast));
    }

    [TestCase("UPDATE t a SET v = 1 FROM s JOIN r ON r.k = s.k WHERE s.id = a.id", NodeType.Join)]
    [TestCase("UPDATE t a SET v = 1 FROM (SELECT id FROM s) q WHERE q.id = a.id", NodeType.DerivedTableReference)]
    [TestCase("UPDATE t a SET v = g FROM generate_series(1, 3) g WHERE g = a.id", NodeType.TableFunctionReference)]
    [TestCase("UPDATE t SET v = 1 FROM s, r WHERE s.id = t.id", NodeType.CommaJoin)]
    [TestCase("DELETE FROM t USING s LEFT JOIN r ON r.k = s.k WHERE s.id = t.id", NodeType.Join)]
    public void TheListTakesEveryFromForm(string sql, NodeType expected)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, DmlJoinSlots.RelationList(ast)!.nodeType);
    }

    [TestCase("UPDATE t a SET v = 1 WHERE a.id = 1", "a")]
    [TestCase("UPDATE t AS a SET v = 1 WHERE a.id = 1", "a")]
    [TestCase("UPDATE t `al` SET v = 1 WHERE `al`.id = 1", "al")]
    [TestCase("DELETE FROM t a WHERE a.id = 1", "a")]
    [TestCase("DELETE FROM t AS a WHERE a.id = 1", "a")]
    public void TheTargetTakesAnAliasWithoutAList(string sql, string alias)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.IsNull(DmlJoinSlots.RelationList(ast));
        Assert.AreEqual(alias, DmlJoinSlots.TargetAlias(ast)!.yytext);
    }

    [TestCase("UPDATE t a SET v = 1 FROM s", "UPDATE requires a WHERE clause")]
    [TestCase("DELETE FROM t a USING s", "DELETE requires a WHERE clause")]
    public void AStatementWithoutWhereNamesTheRule(string sql, string message)
    {
        CamusDBException error = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;

        StringAssert.Contains(message, error.Message);
    }

    [TestCase("CREATE INDEX g ON docs USING GIN (doc)")]
    [TestCase("CREATE INDEX IF NOT EXISTS g ON docs USING btree (name)")]
    public void CreateIndexUsingStillParses(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.That(ast.nodeType, Is.EqualTo(NodeType.AlterTableAddIndex).Or.EqualTo(NodeType.AlterTableAddIndexIfNotExists));
    }

    [TestCase("SELECT using FROM t")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, using INT64)")]
    public void UsingIsAReservedWord(string sql)
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));
    }

    [TestCase("SELECT `using` FROM t")]
    [TestCase("SELECT usings, using_id FROM t")]
    public void UsingInBackticksOrInALongerNameStillParses(string sql)
    {
        Assert.AreEqual(NodeType.Select, SQLParserProcessor.Parse(sql).nodeType);
    }
}
