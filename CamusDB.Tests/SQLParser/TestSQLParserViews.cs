
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
/// Parser-level tests for view and materialized-view DDL: the AST shape each statement produces,
/// that the optional clauses (column-alias list, WITH CHECK OPTION, CASCADE/RESTRICT, WITH NO DATA,
/// CONCURRENTLY) are captured and validated, and — the part most likely to regress silently — that
/// adding these statements did not turn the surrounding words into reserved words. Only VIEW, VIEWS,
/// MATERIALIZED and REFRESH are reserved; REPLACE, CASCADE, RESTRICT, LOCAL, CASCADED, OPTION,
/// OWNER and CONCURRENTLY must all stay usable as ordinary identifiers.
/// </summary>
public sealed class TestSQLParserViews
{
    [Test]
    public void CreateView_Simple()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.AreEqual(NodeType.CreateView, ast.nodeType);
        Assert.AreEqual("open_orders", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.rightAst!.nodeType);
        Assert.AreEqual("orders", ast.rightAst.rightAst!.leftAst!.yytext);
        Assert.IsNull(ast.extendedOne, "no column-alias list was given");
        Assert.IsNull(ast.yytext, "no WITH CHECK OPTION was given");
    }

    [Test]
    public void CreateView_WithColumnAliasList()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE VIEW v (order_id, amount) AS SELECT id, total FROM orders");

        Assert.AreEqual(NodeType.CreateView, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedOne!.nodeType);
        Assert.AreEqual("order_id", ast.extendedOne.leftAst!.yytext);
        Assert.AreEqual("amount", ast.extendedOne.rightAst!.yytext);
    }

    [Test]
    public void CreateView_SingleColumnAliasListIsNotAList()
    {
        // A one-element list reduces to the bare identifier, matching how every other
        // comma-list in this grammar degenerates.
        NodeAst ast = SQLParserProcessor.Parse("CREATE VIEW v (only_col) AS SELECT id FROM orders");

        Assert.AreEqual(NodeType.Identifier, ast.extendedOne!.nodeType);
        Assert.AreEqual("only_col", ast.extendedOne.yytext);
    }

    [Test]
    public void CreateOrReplaceView()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE OR REPLACE VIEW v AS SELECT id FROM orders");

        Assert.AreEqual(NodeType.CreateOrReplaceView, ast.nodeType);
        Assert.AreEqual("v", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.rightAst!.nodeType);
    }

    [Test]
    public void CreateOrReplaceView_RejectsWrongWord()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("CREATE OR REPLASE VIEW v AS SELECT id FROM orders"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("REPLACE", ex.Message);
    }

    [Test]
    public void CreateView_CheckOptionDefaultsToCascaded()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE VIEW v AS SELECT id FROM orders WITH CHECK OPTION");

        Assert.AreEqual("cascaded", ast.yytext, "bare WITH CHECK OPTION means CASCADED, as in PostgreSQL");
    }

    [Test]
    public void CreateView_CheckOptionLocalAndCascaded()
    {
        Assert.AreEqual("local",
            SQLParserProcessor.Parse("CREATE VIEW v AS SELECT id FROM orders WITH LOCAL CHECK OPTION").yytext);

        Assert.AreEqual("cascaded",
            SQLParserProcessor.Parse("CREATE VIEW v AS SELECT id FROM orders WITH CASCADED CHECK OPTION").yytext);
    }

    [Test]
    public void CreateView_RejectsUnknownCheckOptionScope()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("CREATE VIEW v AS SELECT id FROM orders WITH GLOBAL CHECK OPTION"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public void CreateView_RejectsCheckWithoutOption()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("CREATE VIEW v AS SELECT id FROM orders WITH CHECK CONSTRAINTS"));
    }

    [Test]
    public void CreateView_BodyKeepsEveryClause()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE VIEW v AS SELECT DISTINCT o.customer, SUM(o.total) AS s FROM orders o " +
            "INNER JOIN customers c ON o.customer = c.id WHERE o.total > 10 " +
            "GROUP BY o.customer HAVING SUM(o.total) > 100 ORDER BY s DESC LIMIT 5 OFFSET 2");

        NodeAst select = ast.rightAst!;
        Assert.AreEqual(NodeType.Select, select.nodeType);
        Assert.AreEqual("1", select.yytext, "DISTINCT must survive into the stored body");
        Assert.IsNotNull(select.extendedOne, "WHERE must survive");
        Assert.IsNotNull(select.extendedTwo, "ORDER BY must survive");
        Assert.IsNotNull(select.extendedThree, "LIMIT must survive");
        Assert.IsNotNull(select.extendedFour, "OFFSET must survive");
        Assert.IsNotNull(select.extendedFive, "GROUP BY must survive");
        Assert.IsNotNull(select.extendedSix, "HAVING must survive");
    }

    [Test]
    public void DropView_Forms()
    {
        NodeAst plain = SQLParserProcessor.Parse("DROP VIEW v");
        Assert.AreEqual(NodeType.DropView, plain.nodeType);
        Assert.AreEqual("v", plain.leftAst!.yytext);
        Assert.IsNull(plain.yytext, "no behavior given means RESTRICT");

        NodeAst ifExists = SQLParserProcessor.Parse("DROP VIEW IF EXISTS v");
        Assert.AreEqual(NodeType.DropViewIfExists, ifExists.nodeType);

        Assert.AreEqual("cascade", SQLParserProcessor.Parse("DROP VIEW v CASCADE").yytext);
        Assert.AreEqual("restrict", SQLParserProcessor.Parse("DROP VIEW v RESTRICT").yytext);
        Assert.AreEqual("cascade", SQLParserProcessor.Parse("DROP VIEW IF EXISTS v CASCADE").yytext);
    }

    [Test]
    public void DropView_MultipleNames()
    {
        NodeAst ast = SQLParserProcessor.Parse("DROP VIEW a, b, c CASCADE");

        Assert.AreEqual(NodeType.DropView, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual("cascade", ast.yytext);
    }

    [Test]
    public void DropView_RejectsUnknownBehavior()
    {
        CamusDBException? ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("DROP VIEW v CASCADING"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public void AlterView_RenameAndOwner()
    {
        NodeAst rename = SQLParserProcessor.Parse("ALTER VIEW v RENAME TO w");
        Assert.AreEqual(NodeType.AlterViewRenameTo, rename.nodeType);
        Assert.AreEqual("v", rename.leftAst!.yytext);
        Assert.AreEqual("w", rename.rightAst!.yytext);

        NodeAst owner = SQLParserProcessor.Parse("ALTER VIEW v OWNER TO alice");
        Assert.AreEqual(NodeType.AlterViewOwnerTo, owner.nodeType);
        Assert.AreEqual("alice", owner.rightAst!.yytext);
    }

    [Test]
    public void CreateMaterializedView_WithAndWithoutData()
    {
        NodeAst plain = SQLParserProcessor.Parse("CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders");
        Assert.AreEqual(NodeType.CreateMaterializedView, plain.nodeType);
        Assert.AreEqual("mv", plain.leftAst!.yytext);
        Assert.IsNull(plain.yytext, "absent WITH clause means WITH DATA");

        Assert.AreEqual("no data",
            SQLParserProcessor.Parse("CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders WITH NO DATA").yytext);

        Assert.AreEqual("data",
            SQLParserProcessor.Parse("CREATE MATERIALIZED VIEW mv AS SELECT id FROM orders WITH DATA").yytext);

        NodeAst ifNotExists = SQLParserProcessor.Parse(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv (a) AS SELECT id FROM orders WITH NO DATA");
        Assert.AreEqual(NodeType.CreateMaterializedViewIfNotExists, ifNotExists.nodeType);
        Assert.AreEqual("mv", ifNotExists.leftAst!.yytext);
        Assert.AreEqual("a", ifNotExists.extendedOne!.yytext);
        Assert.AreEqual("no data", ifNotExists.yytext);
    }

    [Test]
    public void RefreshMaterializedView_Forms()
    {
        NodeAst plain = SQLParserProcessor.Parse("REFRESH MATERIALIZED VIEW mv");
        Assert.AreEqual(NodeType.RefreshMaterializedView, plain.nodeType);
        Assert.AreEqual("mv", plain.leftAst!.yytext);
        Assert.IsNull(plain.yytext);

        Assert.AreEqual("no data", SQLParserProcessor.Parse("REFRESH MATERIALIZED VIEW mv WITH NO DATA").yytext);
        Assert.AreEqual("concurrently", SQLParserProcessor.Parse("REFRESH MATERIALIZED VIEW CONCURRENTLY mv").yytext);
        Assert.AreEqual("concurrently,data",
            SQLParserProcessor.Parse("REFRESH MATERIALIZED VIEW CONCURRENTLY mv WITH DATA").yytext);
    }

    [Test]
    public void DropAndAlterMaterializedView()
    {
        Assert.AreEqual(NodeType.DropMaterializedView, SQLParserProcessor.Parse("DROP MATERIALIZED VIEW mv").nodeType);
        Assert.AreEqual(NodeType.DropMaterializedViewIfExists,
            SQLParserProcessor.Parse("DROP MATERIALIZED VIEW IF EXISTS mv").nodeType);
        Assert.AreEqual("cascade", SQLParserProcessor.Parse("DROP MATERIALIZED VIEW mv CASCADE").yytext);

        NodeAst rename = SQLParserProcessor.Parse("ALTER MATERIALIZED VIEW mv RENAME TO mv2");
        Assert.AreEqual(NodeType.AlterMaterializedViewRenameTo, rename.nodeType);
        Assert.AreEqual("mv2", rename.rightAst!.yytext);
    }

    [Test]
    public void ShowStatements()
    {
        Assert.AreEqual(NodeType.ShowViews, SQLParserProcessor.Parse("SHOW VIEWS").nodeType);
        Assert.AreEqual(NodeType.ShowMaterializedViews, SQLParserProcessor.Parse("SHOW MATERIALIZED VIEWS").nodeType);
        Assert.AreEqual(NodeType.ShowCreateView, SQLParserProcessor.Parse("SHOW CREATE VIEW v").nodeType);
        Assert.AreEqual(NodeType.ShowCreateMaterializedView,
            SQLParserProcessor.Parse("SHOW CREATE MATERIALIZED VIEW mv").nodeType);

        NodeAst filtered = SQLParserProcessor.Parse("SHOW VIEWS LIKE 'open%'");
        Assert.AreEqual(NodeType.ShowViews, filtered.nodeType);
        Assert.IsNotNull(filtered.leftAst, "the LIKE pattern must reach the executor");
    }

    /// <summary>
    /// The regression that would otherwise be found by a user, not by us: the words the view
    /// statements are spelled with must not become reserved. Each of these parses only if the word
    /// is still an ordinary identifier.
    /// </summary>
    [Test]
    public void ViewKeywordsDoNotBecomeReservedWords()
    {
        foreach (string word in new[] { "replace", "cascade", "restrict", "local", "cascaded", "option", "owner", "concurrently" })
        {
            Assert.DoesNotThrow(
                () => SQLParserProcessor.Parse($"SELECT {word} FROM {word} WHERE {word} = 1"),
                $"'{word}' must remain usable as a table and column name");

            Assert.DoesNotThrow(
                () => SQLParserProcessor.Parse($"CREATE TABLE {word} ({word} INT64 PRIMARY KEY)"),
                $"'{word}' must remain usable in CREATE TABLE");
        }
    }

    /// <summary>
    /// The flip side: VIEW/VIEWS/MATERIALIZED/REFRESH <i>are</i> reserved, and that is a deliberate,
    /// documented cost. Asserting it here means a future change that tries to unreserve one of them
    /// has to come past this test rather than silently altering the dialect.
    /// </summary>
    [Test]
    public void ViewViewsMaterializedRefreshAreReserved()
    {
        foreach (string word in new[] { "view", "views", "materialized", "refresh" })
            Assert.Throws<CamusDBException>(
                () => SQLParserProcessor.Parse($"SELECT {word} FROM t"),
                $"'{word}' is reserved and must not parse as a bare column name");

        // The escape hatch works: backtick-quoting still lets a user reach such a name.
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT `view` FROM `refresh`"));
    }
}
