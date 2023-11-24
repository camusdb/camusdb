﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */


using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

public class TestSQLParser
{
    [Test]
    public void TestParseSimpleSelect()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);

        Assert.AreEqual("some_field", ast.leftAst!.yytext);
        Assert.AreEqual("some_table", ast.rightAst!.yytext);
    }

    [Test]
    public void TestParseSimpleSelect2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\"");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" AND yy = 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table ORDER BY xx");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC, yy DESC");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectAll()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.ExprAllFields, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
        
        Assert.AreEqual("some_table", ast.rightAst!.yytext);
    }

    [Test]
    public void TestParseSimpleSelectAll2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT *, * FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.rightAst!.yytext);
    }
}