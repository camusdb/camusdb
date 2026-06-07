
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
/// Parser tests for IN / NOT IN with literal value lists (ExprInMembership / ExprNotInMembership).
///
/// Tree layout produced by the grammar:
///   ExprInMembership
///     leftAst  = column identifier
///     rightAst = ExprList chain (left-deep) of literal nodes
///
/// For a single-element list the rightAst is the literal itself (no ExprList wrapper).
/// For N elements: ExprList(ExprList(...ExprList(v1, v2)..., vN-1), vN)
/// </summary>
public class TestSQLParserInMembership
{
    // ── AST navigation helpers ─────────────────────────────────────────────────

    private static NodeAst SelectWhere(NodeAst ast)
    {
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne, "Expected a WHERE clause on the SELECT node");
        return ast.extendedOne!;
    }

    private static NodeAst UpdateWhere(NodeAst ast)
    {
        Assert.AreEqual(NodeType.Update, ast.nodeType);
        // Update: leftAst=table, rightAst=update-list, extendedOne=WHERE, extendedTwo=LIMIT
        Assert.IsNotNull(ast.extendedOne, "Expected a WHERE clause on the UPDATE node");
        return ast.extendedOne!;
    }

    private static NodeAst DeleteWhere(NodeAst ast)
    {
        Assert.AreEqual(NodeType.Delete, ast.nodeType);
        // Delete: leftAst=table, rightAst=WHERE, extendedOne=LIMIT
        Assert.IsNotNull(ast.rightAst, "Expected a WHERE clause on the DELETE node");
        return ast.rightAst!;
    }

    /// <summary>
    /// Flattens the left-deep ExprList chain produced by the grammar into a flat list of leaf nodes.
    /// </summary>
    private static List<NodeAst> FlattenList(NodeAst? node)
    {
        var result = new List<NodeAst>();
        Collect(node, result);
        return result;
    }

    private static void Collect(NodeAst? node, List<NodeAst> result)
    {
        if (node is null) return;
        if (node.nodeType == NodeType.ExprList)
        {
            Collect(node.leftAst, result);
            Collect(node.rightAst, result);
        }
        else
        {
            result.Add(node);
        }
    }

    // ── ExprInMembership — node type and column name ───────────────────────────

    [Test]
    public void ParseInMembership_ProducesCorrectNodeType()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE user_inventory_item_id IN ('id1', 'id2', 'id3')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);
    }

    [Test]
    public void ParseInMembership_ColumnNameIsNormalized()
    {
        // IdentifierNormalizer lower-cases identifiers before the tree is returned.
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM t WHERE UserInventoryItemId IN ('a', 'b')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);
        Assert.AreEqual(NodeType.Identifier, where.leftAst!.nodeType);
        Assert.AreEqual("userinventoryitemid", where.leftAst.yytext);
    }

    // ── ExprInMembership — list contents ──────────────────────────────────────

    [Test]
    public void ParseInMembership_SingleString_ValueCorrect()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM orders WHERE status IN ('active')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(1, items.Count);
        Assert.AreEqual(NodeType.String, items[0].nodeType);
        Assert.AreEqual("'active'", items[0].yytext);  // scanner keeps surrounding quotes
    }

    [Test]
    public void ParseInMembership_TwoStrings_ValuesCorrect()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE user_inventory_item_id IN ('id1', 'id2')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(2, items.Count);
        Assert.AreEqual(NodeType.String, items[0].nodeType);
        Assert.AreEqual("'id1'", items[0].yytext);
        Assert.AreEqual(NodeType.String, items[1].nodeType);
        Assert.AreEqual("'id2'", items[1].yytext);
    }

    [Test]
    public void ParseInMembership_ThreeStrings_ValuesCorrect()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE user_inventory_item_id IN ('id1', 'id2', 'id3')");

        NodeAst where = SelectWhere(ast);
        List<NodeAst> items = FlattenList(where.rightAst);

        Assert.AreEqual(3, items.Count);
        Assert.AreEqual("'id1'", items[0].yytext);
        Assert.AreEqual("'id2'", items[1].yytext);
        Assert.AreEqual("'id3'", items[2].yytext);

        foreach (NodeAst item in items)
            Assert.AreEqual(NodeType.String, item.nodeType);
    }

    [Test]
    public void ParseInMembership_FiveStrings_AllValuesPresent()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM items WHERE id IN ('a', 'b', 'c', 'd', 'e')");

        NodeAst where = SelectWhere(ast);
        List<NodeAst> items = FlattenList(where.rightAst);

        Assert.AreEqual(5, items.Count);
        string[] expected = ["'a'", "'b'", "'c'", "'d'", "'e'"];
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.AreEqual(NodeType.String, items[i].nodeType);
            Assert.AreEqual(expected[i], items[i].yytext);
        }
    }

    [Test]
    public void ParseInMembership_IntegerList_NodeTypesAndValues()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM items WHERE quantity IN (1, 2, 3)");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(3, items.Count);

        foreach (NodeAst item in items)
            Assert.AreEqual(NodeType.Integer, item.nodeType);

        Assert.AreEqual("1", items[0].yytext);
        Assert.AreEqual("2", items[1].yytext);
        Assert.AreEqual("3", items[2].yytext);
    }

    [Test]
    public void ParseInMembership_Placeholders_NodeTypesCorrect()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM items WHERE id IN (@p1, @p2, @p3)");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(3, items.Count);

        foreach (NodeAst item in items)
            Assert.AreEqual(NodeType.Placeholder, item.nodeType);

        // Placeholder yytext holds the full token text (e.g. "@p1")
        Assert.That(items[0].yytext, Does.Contain("p1"));
        Assert.That(items[1].yytext, Does.Contain("p2"));
        Assert.That(items[2].yytext, Does.Contain("p3"));
    }

    // ── ExprNotInMembership ────────────────────────────────────────────────────

    [Test]
    public void ParseNotInMembership_ProducesCorrectNodeType()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE status NOT IN ('deleted', 'banned')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprNotInMembership, where.nodeType);
    }

    [Test]
    public void ParseNotInMembership_ColumnAndValues()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE status NOT IN ('deleted', 'banned')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual("status", where.leftAst!.yytext);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(2, items.Count);
        Assert.AreEqual("'deleted'", items[0].yytext);
        Assert.AreEqual("'banned'", items[1].yytext);
    }

    [Test]
    public void ParseNotInMembership_SingleValue()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM orders WHERE status NOT IN ('archived')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprNotInMembership, where.nodeType);
        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(1, items.Count);
        Assert.AreEqual("'archived'", items[0].yytext);
    }

    // ── Combined predicates ────────────────────────────────────────────────────

    [Test]
    public void ParseInMembership_AfterAnd_CorrectSide()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE active = true AND role IN ('admin', 'editor')");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprAnd, where.nodeType);
        Assert.AreEqual(NodeType.ExprInMembership, where.rightAst!.nodeType);
        Assert.AreEqual("role", where.rightAst.leftAst!.yytext);
    }

    [Test]
    public void ParseInMembership_BeforeAnd_CorrectSide()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM users WHERE role IN ('admin', 'editor') AND active = true");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprAnd, where.nodeType);
        Assert.AreEqual(NodeType.ExprInMembership, where.leftAst!.nodeType);
    }

    // ── DML statements ─────────────────────────────────────────────────────────

    [Test]
    public void ParseUpdateWhereIn_StringList()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "UPDATE orders SET status = 'closed' WHERE id IN ('id1', 'id2', 'id3')");

        NodeAst where = UpdateWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);
        Assert.AreEqual("id", where.leftAst!.yytext);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(3, items.Count);
        Assert.AreEqual("'id1'", items[0].yytext);
        Assert.AreEqual("'id2'", items[1].yytext);
        Assert.AreEqual("'id3'", items[2].yytext);
    }

    [Test]
    public void ParseDeleteWhereIn_StringList()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "DELETE FROM sessions WHERE session_id IN ('sess1', 'sess2')");

        NodeAst where = DeleteWhere(ast);
        Assert.AreEqual(NodeType.ExprInMembership, where.nodeType);
        Assert.AreEqual("session_id", where.leftAst!.yytext);

        List<NodeAst> items = FlattenList(where.rightAst);
        Assert.AreEqual(2, items.Count);
        Assert.AreEqual("'sess1'", items[0].yytext);
        Assert.AreEqual("'sess2'", items[1].yytext);
    }

    // ── IN (SELECT ...) still produces ExprInSubquery ─────────────────────────

    [Test]
    public void ParseInSubquery_ProducesExprInSubquery_NotMembership()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = true)");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprInSubquery, where.nodeType,
            "IN (SELECT ...) must still produce ExprInSubquery");
    }

    [Test]
    public void ParseNotInSubquery_ProducesExprNotInSubquery_NotMembership()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM blocked_customers)");

        NodeAst where = SelectWhere(ast);
        Assert.AreEqual(NodeType.ExprNotInSubquery, where.nodeType,
            "NOT IN (SELECT ...) must still produce ExprNotInSubquery");
    }
}
