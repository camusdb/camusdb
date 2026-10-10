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
/// Parser-level tests for <c>UPDATE … RETURNING</c> and <c>DELETE … RETURNING</c>. The two statements
/// keep the list in different slots (UPDATE in <see cref="NodeAst.extendedThree"/>, because its
/// <see cref="NodeAst.extendedTwo"/> holds the LIMIT; DELETE in <see cref="NodeAst.extendedTwo"/>), so
/// every check reads the list through <see cref="StatementScope.GetReturningList"/>, as the engine does.
/// </summary>
public sealed class TestSQLParserUpdateDeleteReturning
{
    [TestCase("UPDATE t SET a = 1 WHERE b = 2 RETURNING a, b", NodeType.Update)]
    [TestCase("DELETE FROM t WHERE b = 2 RETURNING a, b", NodeType.Delete)]
    public void TheListIsReadThroughTheScopeHelper(string sql, NodeType expected)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.AreEqual(expected, ast.nodeType);
        NodeAst? list = StatementScope.GetReturningList(ast);
        Assert.IsNotNull(list);
        Assert.AreEqual(NodeType.IdentifierList, list!.nodeType);
        Assert.AreEqual("a", list.leftAst!.yytext);
        Assert.AreEqual("b", list.rightAst!.yytext);
        Assert.IsTrue(StatementScope.IsWriteReturningRows(ast), sql);
        Assert.IsTrue(StatementScope.ReturnsRows(ast), sql);
    }

    [Test]
    public void UpdateKeepsItsLimitAndPutsTheListInExtendedThree()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE t SET a = 1 WHERE b = 2 LIMIT 5 RETURNING a");

        Assert.IsNotNull(ast.extendedOne, "the WHERE");
        Assert.IsNotNull(ast.extendedTwo, "the LIMIT");
        Assert.AreEqual("a", ast.extendedThree!.yytext);
        Assert.AreSame(ast.extendedThree, StatementScope.GetReturningList(ast));
    }

    [Test]
    public void DeleteKeepsItsLimitAndPutsTheListInExtendedTwo()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM t WHERE b = 2 LIMIT 5 RETURNING a");

        Assert.IsNotNull(ast.rightAst, "the WHERE");
        Assert.IsNotNull(ast.extendedOne, "the LIMIT");
        Assert.AreEqual("a", ast.extendedTwo!.yytext);
        Assert.AreSame(ast.extendedTwo, StatementScope.GetReturningList(ast));
    }

    [TestCase("UPDATE t SET a = 1 WHERE b = 2")]
    [TestCase("UPDATE t SET a = 1 WHERE b = 2 LIMIT 3")]
    [TestCase("DELETE FROM t WHERE b = 2")]
    [TestCase("DELETE FROM t WHERE b = 2 LIMIT 3")]
    public void AStatementWithoutReturningHasNoList(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);

        Assert.IsNull(StatementScope.GetReturningList(ast));
        Assert.IsFalse(StatementScope.IsWriteReturningRows(ast), sql);
        Assert.IsFalse(StatementScope.ReturnsRows(ast), sql);
    }

    [TestCase("UPDATE t SET a = 1 WHERE b = 2 RETURNING *, a + @p AS shifted")]
    [TestCase("DELETE FROM t WHERE b = 2 RETURNING *, a + @p AS shifted")]
    public void TheListAcceptsStarExpressionsAndAliases(string sql)
    {
        NodeAst list = StatementScope.GetReturningList(SQLParserProcessor.Parse(sql))!;

        Assert.AreEqual(NodeType.ExprAllFields, list.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprAlias, list.rightAst!.nodeType);
    }

    [TestCase("UPDATE t SET a = 1 WHERE b = 2 RETURNING")]
    [TestCase("DELETE FROM t WHERE b = 2 RETURNING")]
    [TestCase("DELETE FROM t WHERE b = 2 RETURNING a LIMIT 3")]
    public void MalformedListsAreSyntaxErrors(string sql)
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql));
    }

    /// <summary>
    /// A transport that knows only the root type decides from it that a statement on a row-returning
    /// endpoint writes; every statement that can carry a RETURNING list is in that set, and a read is not.
    /// </summary>
    [Test]
    public void TheRootTypeCheckCoversEveryWriteThatCanReturnRows()
    {
        Assert.IsTrue(StatementScope.CanReturnWrittenRows(NodeType.Insert));
        Assert.IsTrue(StatementScope.CanReturnWrittenRows(NodeType.InsertSelect));
        Assert.IsTrue(StatementScope.CanReturnWrittenRows(NodeType.Update));
        Assert.IsTrue(StatementScope.CanReturnWrittenRows(NodeType.Delete));
        Assert.IsFalse(StatementScope.CanReturnWrittenRows(NodeType.Select));
        Assert.IsFalse(StatementScope.CanReturnWrittenRows(NodeType.TruncateTable));
    }
}
