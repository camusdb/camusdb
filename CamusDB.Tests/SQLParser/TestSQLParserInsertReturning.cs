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
/// Parser-level tests for <c>INSERT … RETURNING</c>: all four INSERT forms put the list in
/// <see cref="NodeAst.extendedTwo"/>, an INSERT … SELECT attaches the list to the INSERT and not to
/// its source query, and <see cref="StatementScope"/> classifies the statement from the AST.
/// </summary>
public sealed class TestSQLParserInsertReturning
{
    [TestCase("INSERT INTO t (a, b) VALUES (1, 2) RETURNING a, b", NodeType.Insert)]
    [TestCase("INSERT INTO t VALUES (1, 2) RETURNING a, b", NodeType.Insert)]
    [TestCase("INSERT INTO t (a, b) SELECT x, y FROM s RETURNING a, b", NodeType.InsertSelect)]
    [TestCase("INSERT INTO t SELECT x, y FROM s RETURNING a, b", NodeType.InsertSelect)]
    public void EveryInsertFormCarriesTheListInExtendedTwo(string sql, NodeType expected)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, ast.nodeType);
        Assert.IsNotNull(ast.extendedTwo);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
        Assert.AreEqual("a", ast.extendedTwo.leftAst!.yytext);
        Assert.AreEqual("b", ast.extendedTwo.rightAst!.yytext);
        Assert.IsTrue(StatementScope.IsWriteReturningRows(ast), sql);
    }

    [TestCase("INSERT INTO t (a) VALUES (1)")]
    [TestCase("INSERT INTO t VALUES (1)")]
    [TestCase("INSERT INTO t (a) SELECT x FROM s")]
    [TestCase("INSERT INTO t SELECT x FROM s")]
    public void AnInsertWithoutReturningHasNoList(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.IsNull(ast.extendedTwo);
        Assert.IsFalse(StatementScope.IsWriteReturningRows(ast), sql);
    }

    /// <summary>
    /// The source query of an INSERT … SELECT ends with its own optional clauses. RETURNING must end
    /// the INSERT, so the source keeps its WHERE, ORDER BY and LIMIT, and the list is not mistaken
    /// for part of them.
    /// </summary>
    [Test]
    public void InsertSelectAttachesTheListToTheInsertNotTheSource()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO t (a) SELECT x FROM s WHERE x > 1 ORDER BY x LIMIT 5 RETURNING a");

        Assert.AreEqual(NodeType.InsertSelect, ast.nodeType);
        Assert.AreEqual("a", ast.extendedTwo!.yytext);

        NodeAst source = ast.extendedOne!;
        Assert.AreEqual(NodeType.Select, source.nodeType);
        Assert.IsNotNull(source.extendedOne, "the source keeps its WHERE");
        Assert.IsNotNull(source.extendedThree, "the source keeps its LIMIT");
    }

    [Test]
    public void TheListAcceptsStarExpressionsAndAliases()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1) RETURNING *, a + @p AS shifted");

        NodeAst list = ast.extendedTwo!;
        Assert.AreEqual(NodeType.ExprAllFields, list.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprAlias, list.rightAst!.nodeType);
    }

    [Test]
    public void ReturningIsCaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse("insert into t (a) values (1) returning a");

        Assert.IsTrue(StatementScope.IsWriteReturningRows(ast));
    }

    /// <summary>
    /// <c>RETURNING</c> is reserved, as in PostgreSQL. A column with that name is still reachable
    /// with backticks.
    /// </summary>
    [Test]
    public void ReturningIsReservedButUsableEscaped()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT returning FROM t"));

        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO t (`returning`) VALUES (1) RETURNING `returning`");
        Assert.AreEqual("returning", ast.extendedTwo!.yytext);
    }

    [Test]
    public void AnEmptyListIsASyntaxError()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1) RETURNING"));
    }

    [Test]
    public void TheAstFormOfReturnsRowsSeesReturning()
    {
        Assert.IsTrue(StatementScope.ReturnsRows(SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1) RETURNING a")));
        Assert.IsFalse(StatementScope.ReturnsRows(SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1)")));
        Assert.IsTrue(StatementScope.ReturnsRows(SQLParserProcessor.Parse("SELECT a FROM t")));

        // The root type alone cannot see the list, which is why the AST form exists.
        Assert.IsFalse(StatementScope.ReturnsRows(SQLParserProcessor.Parse("INSERT INTO t (a) VALUES (1) RETURNING a").nodeType));
    }
}
