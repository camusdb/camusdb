
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
/// Parser-level tests for the CASE conditional expression: the searched and simple forms, the WHEN
/// chain shape, the optional ELSE, nesting, use as a function argument, and the syntax errors
/// (missing WHEN) plus the reserved-word consequence of the new keywords.
/// </summary>
public sealed class TestSQLParserCaseExpression
{
    /// <summary>Returns the single-projection expression of a FROM-less SELECT.</summary>
    private static NodeAst Projection(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        return ast.leftAst!;
    }

    [Test]
    public void SearchedCase_SingleWhen_WithElse()
    {
        NodeAst c = Projection("SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END");

        Assert.AreEqual(NodeType.ExprCase, c.nodeType);
        Assert.IsNull(c.leftAst, "searched CASE has no operand");

        // Single WHEN collapses to a bare ExprCaseWhen (no list wrapper).
        Assert.AreEqual(NodeType.ExprCaseWhen, c.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, c.rightAst.leftAst!.nodeType);
        Assert.AreEqual(NodeType.String, c.rightAst.rightAst!.nodeType);

        // ELSE result is carried in extendedOne.
        Assert.AreEqual(NodeType.String, c.extendedOne!.nodeType);
    }

    [Test]
    public void SearchedCase_MultiWhen_NoElse()
    {
        NodeAst c = Projection("SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' END");

        Assert.AreEqual(NodeType.ExprCase, c.nodeType);
        Assert.IsNull(c.leftAst);
        Assert.IsNull(c.extendedOne, "no ELSE");

        // Two WHENs form a left-recursive list: leftAst is the first clause, rightAst the second.
        Assert.AreEqual(NodeType.ExprCaseWhenList, c.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprCaseWhen, c.rightAst.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprCaseWhen, c.rightAst.rightAst!.nodeType);
    }

    [Test]
    public void SimpleCase_HasOperand()
    {
        NodeAst c = Projection("SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'closed' ELSE 'unknown' END");

        Assert.AreEqual(NodeType.ExprCase, c.nodeType);
        Assert.AreEqual(NodeType.Identifier, c.leftAst!.nodeType, "simple CASE operand");
        Assert.AreEqual("status", c.leftAst.yytext);
        Assert.AreEqual(NodeType.ExprCaseWhenList, c.rightAst!.nodeType);
        Assert.AreEqual(NodeType.String, c.extendedOne!.nodeType);
    }

    [Test]
    public void Case_NestedInThen()
    {
        NodeAst c = Projection("SELECT CASE WHEN a = 1 THEN CASE WHEN b = 2 THEN 'x' END ELSE 'y' END");

        Assert.AreEqual(NodeType.ExprCase, c.nodeType);
        Assert.AreEqual(NodeType.ExprCaseWhen, c.rightAst!.nodeType);
        // The THEN result is itself a CASE.
        Assert.AreEqual(NodeType.ExprCase, c.rightAst.rightAst!.nodeType);
    }

    [Test]
    public void Case_AsFunctionArgument()
    {
        NodeAst c = Projection("SELECT upper(CASE WHEN a = 1 THEN 'x' ELSE 'y' END)");

        Assert.AreEqual(NodeType.ExprFuncCall, c.nodeType);
        // The single function argument is the CASE expression.
        Assert.AreEqual(NodeType.ExprCase, c.rightAst!.nodeType);
    }

    [Test]
    public void Case_WithBooleanConditionInWhen()
    {
        // The WHEN condition is a full boolean expression terminated by THEN.
        NodeAst c = Projection("SELECT CASE WHEN a = 1 AND b = 2 THEN 'x' END");

        Assert.AreEqual(NodeType.ExprCase, c.nodeType);
        Assert.AreEqual(NodeType.ExprCaseWhen, c.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprAnd, c.rightAst.leftAst!.nodeType);
    }

    [Test]
    public void Case_MissingWhen_IsSyntaxError()
    {
        // CASE ELSE x END and CASE END both require at least one WHEN.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT CASE ELSE 'x' END"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT CASE END"));
    }

    [Test]
    public void EndIsReserved_EscapedIdentifierStillParses()
    {
        // `end` as a bare identifier now lexes as the TEND keyword and cannot be a column name;
        // the escaped-identifier form remains available.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT end FROM t"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT `end` FROM t"));
    }
}
