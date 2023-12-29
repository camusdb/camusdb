
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
    public void TestParseSimpleSelect3()
    {
        NodeAst ast = SQLParserProcessor.Parse("select some_field, another_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelect4()
    {
        NodeAst ast = SQLParserProcessor.Parse("select some_field, another_field from some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelect5()
    {
        NodeAst ast = SQLParserProcessor.Parse("Select some_field, another_field From some_table");

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
    public void TestParseSimpleSelectWhere5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE (xx = 100)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere6()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = 100 OR x != 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere7()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx > 100 AND x < 200");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere8()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx >= 100 AND x <= 200");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere9()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE (xx >= 100) AND (x <= 200)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere10()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE ((xx >= 100) AND (x <= 200)) OR x = 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere11()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE enabled OR enabled");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere12()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE enabled=true OR enabled=true");

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
        Assert.AreEqual(NodeType.Identifier, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
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

    [Test]
    public void TestParseSimpleUpdate()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE some_table SET some_field = some_value WHERE TRUE");

        Assert.AreEqual(NodeType.Update, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.UpdateItem, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);        
    }

    [Test]
    public void TestParseUpdateMultiSet()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE some_table SET some_field = some_value, some_other_field = 100 WHERE TRUE");

        Assert.AreEqual(NodeType.Update, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.UpdateList, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseUpdateMultiSet2()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE some_table SET some_field = some_value, some_other_field = 100, bool_field = false WHERE TRUE");

        Assert.AreEqual(NodeType.Update, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.UpdateList, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO some_table (y) VALUES (x)");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedOne!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert2()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO some_table (y, z) VALUES (x, p)");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert3()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO some_table (id, z) VALUES (GEN_ID(), \"aaaa\")");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableOneField()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id STRING )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);        

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableOneField1()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id OBJECT_ID )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableOneField2()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id OID )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFields()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id STRING, name STRING )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFieldsNotNull()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id STRING, name STRING NOT NULL )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFieldsBothNotNull()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id STRING NOT NULL, name STRING NOT NULL )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFieldsBothNotNull2()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id INT64 NOT NULL, name INT64 NOT NULL )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFieldsBothNotNull3()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table (\nid INT64 NOT NULL,\nname INT64 NOT NULL, year INT64 )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableTwoFieldsBothNotNull4()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table (\nid INT64 NOT NULL,\nname INT64 NOT NULL, year INT64 )");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }
}