
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Parser tests for CHECK constraint syntax: column-level, table-level (named and unnamed),
/// and ALTER TABLE ADD/DROP CONSTRAINT CHECK.
/// </summary>
public class TestSQLParserCheckConstraints
{
    // ---- column-level CHECK ----

    [Test]
    public void TestColumnCheckConstraintSimple()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE products (price int64 CHECK (price > 0))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        // single item (no list wrapper)
        NodeAst item = ast.rightAst!;
        Assert.AreEqual(NodeType.CreateTableItem, item.nodeType);

        // a single field constraint reduces directly to the constraint node (no list wrapper)
        NodeAst check = item.extendedOne!;
        Assert.AreEqual(NodeType.ConstraintCheck, check.nodeType);

        // leftAst = the condition (price > 0)
        Assert.AreEqual(NodeType.ExprGreaterThan, check.leftAst!.nodeType);
    }

    [Test]
    public void TestColumnCheckConstraintCaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (salary int64 check (salary > 0))");

        NodeAst item = ast.rightAst!;
        Assert.AreEqual(NodeType.CreateTableItem, item.nodeType);
        // single constraint reduces directly
        NodeAst check = item.extendedOne!;
        Assert.AreEqual(NodeType.ConstraintCheck, check.nodeType);
    }

    [Test]
    public void TestColumnCheckWithMultipleConstraints()
    {
        // CHECK combined with NOT NULL
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (price int64 NOT NULL CHECK (price > 0))");

        NodeAst item = ast.rightAst!;
        Assert.AreEqual(NodeType.CreateTableItem, item.nodeType);

        // constraint list has two items: NOT NULL and CHECK
        NodeAst constraintList = item.extendedOne!;
        Assert.AreEqual(NodeType.CreateTableFieldConstraintList, constraintList.nodeType);
        Assert.AreEqual(NodeType.ConstraintCheck, constraintList.rightAst!.nodeType);
    }

    [Test]
    public void TestColumnCheckWithAndCondition()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (price int64 CHECK (price > 0 AND price < 1000))");

        NodeAst item = ast.rightAst!;
        // single constraint reduces directly
        NodeAst check = item.extendedOne!;
        Assert.AreEqual(NodeType.ConstraintCheck, check.nodeType);
        Assert.AreEqual(NodeType.ExprAnd, check.leftAst!.nodeType);
    }

    // ---- table-level CHECK (unnamed) ----

    [Test]
    public void TestTableLevelUnnamedCheck()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE products (price int64, discounted_price int64, CHECK (price > discounted_price))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        // The item list is a list; the last node in the list should be CreateTableConstraintCheck
        NodeAst itemList = ast.rightAst!;
        // Walk to the rightmost item which holds the inline constraint
        NodeAst constraintNode = itemList.rightAst!;
        Assert.AreEqual(NodeType.CreateTableConstraintCheck, constraintNode.nodeType);
        Assert.IsNull(constraintNode.yytext); // no name
        Assert.IsNotNull(constraintNode.leftAst); // condition present
        Assert.AreEqual(NodeType.ExprGreaterThan, constraintNode.leftAst!.nodeType);
    }

    // ---- table-level CHECK (named) ----

    [Test]
    public void TestTableLevelNamedCheck()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE products (" +
            "  price int64, discounted_price int64, " +
            "  CONSTRAINT valid_discount CHECK (price > discounted_price))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        NodeAst itemList = ast.rightAst!;
        NodeAst constraintNode = itemList.rightAst!;
        Assert.AreEqual(NodeType.CreateTableConstraintCheck, constraintNode.nodeType);
        Assert.AreEqual("valid_discount", constraintNode.yytext);
        Assert.AreEqual(NodeType.ExprGreaterThan, constraintNode.leftAst!.nodeType);
    }

    [Test]
    public void TestTableLevelNamedCheckMixedCase()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (x int64, CONSTRAINT positive_x CHECK (x > 0))");

        NodeAst itemList = ast.rightAst!;
        NodeAst constraintNode = itemList.rightAst!;
        Assert.AreEqual(NodeType.CreateTableConstraintCheck, constraintNode.nodeType);
        Assert.AreEqual("positive_x", constraintNode.yytext);
    }

    // ---- ALTER TABLE ADD CONSTRAINT CHECK ----

    [Test]
    public void TestAlterTableAddConstraintCheck()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "ALTER TABLE employees ADD CONSTRAINT positive_salary CHECK (salary > 0)");

        Assert.AreEqual(NodeType.AlterTableAddConstraintCheck, ast.nodeType);

        // leftAst = table name node
        Assert.AreEqual("employees", ast.leftAst!.yytext);

        // yytext = constraint name
        Assert.AreEqual("positive_salary", ast.yytext);

        // rightAst = condition
        Assert.AreEqual(NodeType.ExprGreaterThan, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestAlterTableAddConstraintCheckComplexCondition()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "ALTER TABLE orders ADD CONSTRAINT valid_dates CHECK (ship_date >= order_date)");

        Assert.AreEqual(NodeType.AlterTableAddConstraintCheck, ast.nodeType);
        Assert.AreEqual("orders", ast.leftAst!.yytext);
        Assert.AreEqual("valid_dates", ast.yytext);
        Assert.AreEqual(NodeType.ExprGreaterEqualsThan, ast.rightAst!.nodeType);
    }

    // ---- ALTER TABLE DROP CONSTRAINT ----

    [Test]
    public void TestAlterTableDropConstraint()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "ALTER TABLE employees DROP CONSTRAINT positive_salary");

        Assert.AreEqual(NodeType.AlterTableDropConstraint, ast.nodeType);
        Assert.AreEqual("employees", ast.leftAst!.yytext);
        Assert.AreEqual("positive_salary", ast.yytext);
    }

    [Test]
    public void TestAlterTableDropConstraintCaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "alter table employees drop constraint positive_salary");

        Assert.AreEqual(NodeType.AlterTableDropConstraint, ast.nodeType);
        Assert.AreEqual("employees", ast.leftAst!.yytext);
        Assert.AreEqual("positive_salary", ast.yytext);
    }

    // ---- multi-column check ----

    [Test]
    public void TestColumnCheckReferencingOtherColumn()
    {
        // A column-level CHECK may reference other columns
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE employees (" +
            "  birth_date string, " +
            "  joined_date string CHECK (joined_date > birth_date))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        // The item list; rightAst holds the second item (joined_date with check)
        NodeAst itemList = ast.rightAst!;
        NodeAst secondItem = itemList.rightAst!;
        Assert.AreEqual(NodeType.CreateTableItem, secondItem.nodeType);

        // single constraint reduces directly
        NodeAst check = secondItem.extendedOne!;
        Assert.AreEqual(NodeType.ConstraintCheck, check.nodeType);
        Assert.AreEqual(NodeType.ExprGreaterThan, check.leftAst!.nodeType);
    }

    // ---- parse doesn't throw: the four target examples from the spec ----

    [Test]
    public void TestSpecExample1ParsesWithoutError()
    {
        Assert.DoesNotThrow(() =>
            SQLParserProcessor.Parse(
                "CREATE TABLE products (product_no int64, name string, price int64 CHECK (price > 0))"));
    }

    [Test]
    public void TestSpecExample2ParsesWithoutError()
    {
        Assert.DoesNotThrow(() =>
            SQLParserProcessor.Parse(
                "CREATE TABLE products (" +
                "  product_no int64, name string, price int64, discounted_price int64, " +
                "  CONSTRAINT valid_discount CHECK (price > discounted_price))"));
    }

    [Test]
    public void TestSpecExample3ParsesWithoutError()
    {
        Assert.DoesNotThrow(() =>
            SQLParserProcessor.Parse(
                "ALTER TABLE employees ADD CONSTRAINT positive_salary CHECK (salary > 0)"));
    }

    [Test]
    public void TestSpecExample4ParsesWithoutError()
    {
        Assert.DoesNotThrow(() =>
            SQLParserProcessor.Parse(
                "CREATE TABLE employees (" +
                "  id object_id, " +
                "  birth_date string CHECK (birth_date > '1900-01-01'), " +
                "  joined_date string CHECK (joined_date > birth_date), " +
                "  salary int64 CHECK (salary > 0))"));
    }
}
