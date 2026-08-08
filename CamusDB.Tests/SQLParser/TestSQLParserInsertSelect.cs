
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
/// Parser-level tests for <c>INSERT INTO … SELECT</c> and <c>CREATE TABLE … AS SELECT</c>: the AST
/// shape both forms produce, that the source query keeps every clause the SELECT grammar admits
/// (joins, aggregates, subqueries, AS OF SYSTEM TIME), and that <c>WITH [NO] DATA</c> is validated
/// without turning <c>data</c> or <c>no</c> into reserved words.
/// </summary>
public sealed class TestSQLParserInsertSelect
{
    [Test]
    public void InsertSelect_WithColumnList()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO dest (a, b) SELECT x, y FROM src");

        Assert.AreEqual(NodeType.InsertSelect, ast.nodeType);
        Assert.AreEqual("dest", ast.leftAst!.yytext);

        // Target column list, same shape the VALUES form produces.
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst!.nodeType);
        Assert.AreEqual("a", ast.rightAst.leftAst!.yytext);
        Assert.AreEqual("b", ast.rightAst.rightAst!.yytext);

        // The source query hangs off extendedOne — the same slot the VALUES batch list uses.
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.nodeType);
        Assert.AreEqual("src", ast.extendedOne.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void InsertSelect_WithoutColumnList()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO dest SELECT * FROM src");

        Assert.AreEqual(NodeType.InsertSelect, ast.nodeType);
        Assert.AreEqual("dest", ast.leftAst!.yytext);
        Assert.IsNull(ast.rightAst, "no explicit column list");
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprAllFields, ast.extendedOne.leftAst!.nodeType);
    }

    [Test]
    public void InsertSelect_KeepsEveryClauseOfTheSourceQuery()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO dest (customer, total) " +
            "SELECT c.name, SUM(o.total) FROM orders o JOIN customers c ON o.cid = c.id " +
            "WHERE o.total > 10 GROUP BY c.name HAVING SUM(o.total) > 100 " +
            "ORDER BY c.name DESC LIMIT 5 OFFSET 2");

        Assert.AreEqual(NodeType.InsertSelect, ast.nodeType);

        NodeAst select = ast.extendedOne!;
        Assert.AreEqual(NodeType.Select, select.nodeType);
        Assert.IsNotNull(select.extendedOne, "WHERE survives");
        Assert.IsNotNull(select.extendedTwo, "ORDER BY survives");
        Assert.IsNotNull(select.extendedThree, "LIMIT survives");
        Assert.IsNotNull(select.extendedFour, "OFFSET survives");
        Assert.IsNotNull(select.extendedFive, "GROUP BY survives");
        Assert.IsNotNull(select.extendedSix, "HAVING survives");
    }

    [Test]
    public void InsertSelect_SourceMayCarryAsOfSystemTime()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO dest SELECT * FROM src AS OF SYSTEM TIME '-2h'");

        Assert.AreEqual(NodeType.InsertSelect, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne!.extendedSeven, "AS OF SYSTEM TIME value is preserved");
        Assert.AreEqual(NodeType.String, ast.extendedOne.extendedSeven!.nodeType);
    }

    [Test]
    public void InsertValues_StillParsesUnchanged()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO dest (a, b) VALUES (1, 2), (3, 4)");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);
        Assert.AreEqual(NodeType.InsertBatchList, ast.extendedOne!.nodeType);
    }

    // ── CREATE TABLE … AS SELECT ─────────────────────────────────────────────

    [Test]
    public void CreateTableAsSelect_Basic()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE summary AS SELECT a, b FROM src");

        Assert.AreEqual(NodeType.CreateTableAsSelect, ast.nodeType);
        Assert.AreEqual("summary", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.rightAst!.nodeType);
        Assert.IsNull(ast.yytext, "no WITH clause means the default (WITH DATA)");
    }

    [Test]
    public void CreateTableAsSelect_IfNotExists()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE IF NOT EXISTS summary AS SELECT a FROM src");

        Assert.AreEqual(NodeType.CreateTableAsSelectIfNotExists, ast.nodeType);
        Assert.AreEqual("summary", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.rightAst!.nodeType);
    }

    [Test]
    public void CreateTableAsSelect_WithData()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE summary AS SELECT a FROM src WITH DATA");

        Assert.AreEqual(NodeType.CreateTableAsSelect, ast.nodeType);
        Assert.AreEqual("data", ast.yytext);
    }

    [Test]
    public void CreateTableAsSelect_WithNoData()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE summary AS SELECT a FROM src WITH NO DATA");

        Assert.AreEqual(NodeType.CreateTableAsSelect, ast.nodeType);
        Assert.AreEqual("no data", ast.yytext);
    }

    [Test]
    public void CreateTableAsSelect_WithNoData_IsCaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse("create table summary as select a from src with no data");

        Assert.AreEqual(NodeType.CreateTableAsSelect, ast.nodeType);
        Assert.AreEqual("no data", ast.yytext);
    }

    [Test]
    public void CreateTableAsSelect_RejectsUnknownWithOption()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("CREATE TABLE summary AS SELECT a FROM src WITH BOGUS"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("WITH DATA", ex.Message);
    }

    [Test]
    public void CreateTableAsSelect_RejectsMalformedNoData()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse("CREATE TABLE summary AS SELECT a FROM src WITH NO ROWS"));
    }

    /// <summary>
    /// DATA and NO are matched as ordinary identifiers, so adding WITH [NO] DATA must not have
    /// turned them into reserved words that break existing schemas.
    /// </summary>
    [Test]
    public void DataAndNo_AreStillUsableAsIdentifiers()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT data, no FROM data");
        Assert.AreEqual(NodeType.Select, ast.nodeType);

        NodeAst created = SQLParserProcessor.Parse("CREATE TABLE data (no INT64 PRIMARY KEY)");
        Assert.AreEqual(NodeType.CreateTable, created.nodeType);
        Assert.AreEqual("data", created.leftAst!.yytext);
    }

    [Test]
    public void CreateTable_ClassicFormStillParsesUnchanged()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (id OID PRIMARY KEY, name STRING)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        Assert.AreEqual(NodeType.CreateTableItemList, ast.rightAst!.nodeType);
    }

    /// <summary>
    /// Placeholders inside the source query must be published by PlaceholderCollector so a prepared
    /// INSERT … SELECT binds its parameters — the collector walks every child slot, and this pins
    /// the fact that the source hangs off one of them.
    /// </summary>
    [Test]
    public void InsertSelect_PublishesSourcePlaceholders()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO dest (a) SELECT x FROM src WHERE y = @yv AND z = @zv");

        Assert.AreEqual(new[] { "@yv", "@zv" }, PlaceholderCollector.Collect(ast));
    }

    [Test]
    public void CreateTableAsSelect_PublishesSourcePlaceholders()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t AS SELECT x FROM src WHERE y = @yv");

        Assert.AreEqual(new[] { "@yv" }, PlaceholderCollector.Collect(ast));
    }
}
