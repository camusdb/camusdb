/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The syntax of the function statements: the dollar-quoted body, which is raw text, and the words
/// that the grammar checks without reserving them.
/// </summary>
[TestFixture]
public sealed class TestSQLParserInlineFunctions
{
    [Test]
    public void CreateFunction_ParsesEveryPart()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE FUNCTION Slugify(s STRING, n INT64, xs ARRAY(FLOAT64)) RETURNS STRING LANGUAGE csharp AS $$ return s; $$");

        Assert.AreEqual(NodeType.CreateFunction, ast.nodeType);
        Assert.AreEqual("Slugify", ast.leftAst!.yytext, "the name keeps the case the statement wrote");
        Assert.AreEqual(NodeType.TypeString, ast.extendedOne!.nodeType);
        Assert.AreEqual(" return s; ", ast.extendedTwo!.yytext, "the body is the text between the quotes, with its spaces");
        Assert.AreEqual("csharp", ast.extendedThree!.yytext);
        Assert.IsNull(ast.yytext, "no NULL rule written: the default");

        // The parameters are a left-deep list.
        Assert.AreEqual(NodeType.FunctionParameterList, ast.rightAst!.nodeType);
        Assert.AreEqual("xs", ast.rightAst.rightAst!.leftAst!.yytext);
        Assert.AreEqual(NodeType.TypeArray, ast.rightAst.rightAst.rightAst!.nodeType);
        Assert.AreEqual("s", ast.rightAst.leftAst!.leftAst!.leftAst!.yytext);
    }

    [Test]
    public void NullRulesAndOrReplace()
    {
        Assert.AreEqual("called", SQLParserProcessor.Parse(
            "CREATE FUNCTION f() RETURNS INT64 CALLED ON NULL INPUT LANGUAGE csharp AS $$ return 1; $$").yytext);

        Assert.IsNull(SQLParserProcessor.Parse(
            "CREATE FUNCTION f() RETURNS INT64 RETURNS NULL ON NULL INPUT LANGUAGE csharp AS $$ return 1; $$").yytext);

        NodeAst replace = SQLParserProcessor.Parse(
            "create or replace function f(a bool) returns array(string) language CSharp as $$ return null; $$;");

        Assert.AreEqual(NodeType.CreateOrReplaceFunction, replace.nodeType);
        Assert.AreEqual(NodeType.FunctionParameter, replace.rightAst!.nodeType, "one parameter is not a list");
        Assert.AreEqual(NodeType.TypeArray, replace.extendedOne!.nodeType);
        Assert.AreEqual("CSharp", replace.extendedThree!.yytext, "the language is checked when the statement runs");

        Assert.IsNull(SQLParserProcessor.Parse("CREATE FUNCTION f() RETURNS INT64 LANGUAGE csharp AS $$$$").rightAst, "no parameters");
    }

    [Test]
    public void TheBody_IsRawText()
    {
        // Quotes, a semicolon, both comment forms, a placeholder and a line break: all text.
        const string body = "\n  var a = 'x'; var b = \"it's\"; // a -- comment\n  /* block ; */ return @name + $\"{a}\";\n";

        NodeAst ast = SQLParserProcessor.Parse("CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE csharp AS $$" + body + "$$");
        Assert.AreEqual(body, ast.extendedTwo!.yytext);

        // A tagged quote lets the body hold the plain one, and another tag.
        NodeAst tagged = SQLParserProcessor.Parse(
            "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE csharp AS $fn$ return \"$$\" + \"$other$\" + \"$\"; $fn$");
        Assert.AreEqual(" return \"$$\" + \"$other$\" + \"$\"; ", tagged.extendedTwo!.yytext);

        // A dollar just before the closing quote belongs to the body.
        NodeAst edge = SQLParserProcessor.Parse("CREATE FUNCTION f() RETURNS STRING LANGUAGE csharp AS $a$ x$$a$");
        Assert.AreEqual(" x$", edge.extendedTwo!.yytext);

        // Tags compare exactly.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("CREATE FUNCTION f() RETURNS STRING LANGUAGE csharp AS $a$ return 1; $A$"));
    }

    [Test]
    public void ABodyThatDoesNotClose_IsASyntaxError()
    {
        CamusDBException open = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("CREATE FUNCTION f() RETURNS STRING LANGUAGE csharp AS $$ return 1;"))!;

        StringAssert.Contains("does not close", open.Message);

        // A lone dollar is still no token.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT $1"));
    }

    [Test]
    public void TheOtherStatements()
    {
        NodeAst drop = SQLParserProcessor.Parse("DROP FUNCTION slugify");
        Assert.AreEqual(NodeType.DropFunction, drop.nodeType);
        Assert.AreEqual("slugify", drop.leftAst!.yytext);
        Assert.IsNull(drop.yytext);

        Assert.AreEqual("if exists", SQLParserProcessor.Parse("DROP FUNCTION IF EXISTS slugify").yytext);

        NodeAst owner = SQLParserProcessor.Parse("ALTER FUNCTION slugify OWNER TO alice");
        Assert.AreEqual(NodeType.AlterFunctionOwnerTo, owner.nodeType);
        Assert.AreEqual("alice", owner.rightAst!.yytext);

        Assert.AreEqual(NodeType.ShowFunctions, SQLParserProcessor.Parse("SHOW FUNCTIONS").nodeType);
        Assert.AreEqual(NodeType.ShowCreateFunction, SQLParserProcessor.Parse("SHOW CREATE FUNCTION slugify").nodeType);

        // The extension statements that share their tokens still parse as before.
        Assert.AreEqual(NodeType.DropExtension, SQLParserProcessor.Parse("DROP EXTENSION geo").nodeType);
        Assert.AreEqual(NodeType.CreateExtension, SQLParserProcessor.Parse("CREATE EXTENSION geo").nodeType);
        Assert.AreEqual(NodeType.AlterExtensionUpdate, SQLParserProcessor.Parse("ALTER EXTENSION geo UPDATE").nodeType);
    }

    [TestCase("CREATE PROCEDURE f() RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$")]
    [TestCase("CREATE FUNCTION f() GIVES INT64 LANGUAGE csharp AS $$ return 1; $$")]
    [TestCase("CREATE FUNCTION f() RETURNS INT64 DIALECT csharp AS $$ return 1; $$")]
    [TestCase("CREATE FUNCTION f() RETURNS INT64 CALLED ON NULL OUTPUT LANGUAGE csharp AS $$ return 1; $$")]
    [TestCase("CREATE FUNCTION f() RETURNS INT64 IGNORES NULL ON NULL INPUT LANGUAGE csharp AS $$ return 1; $$")]
    [TestCase("CREATE OR UPDATE FUNCTION f() RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$")]
    [TestCase("ALTER FUNCTION f RENAME TO g")]
    [TestCase("ALTER PROCEDURE f OWNER TO g")]
    [TestCase("SHOW CREATE PROCEDURE f")]
    [TestCase("DROP PROCEDURE f")]
    public void AWrongWord_IsRefusedWithTheExpectedForm(string sql)
    {
        CamusDBException wrong = Assert.Catch<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;

        // A word that is a name in SQL gets the message of the statement; a word that is a keyword
        // (UPDATE, RENAME) gets the message of the grammar.
        Assert.That(wrong.Message, Does.Contain("Expected").Or.Contain("Syntax error"));
    }

    [Test]
    public void TheWordsOfTheStatements_StayUsableAsNames()
    {
        foreach (string sql in new[]
        {
            "CREATE TABLE function (id INT64 PRIMARY KEY, language STRING, returns STRING, called STRING, input STRING, owner STRING, replace STRING)",
            "SELECT language, returns, called, input, owner, replace FROM function",
            "SELECT functions.id FROM functions",
        })
        {
            Assert.DoesNotThrow(() => SQLParserProcessor.Parse(sql), sql);
        }
    }

    [Test]
    public void TheScriptSplitter_KeepsABodyInOneStatement()
    {
        const string script =
            "CREATE FUNCTION f(s STRING) RETURNS STRING LANGUAGE csharp AS $$ var a = 1; return s + \";\"; $$;\n" +
            "CREATE FUNCTION g() RETURNS STRING LANGUAGE csharp AS $x$ return \"$$;\"; $x$;\n" +
            "SELECT f('a;b'); SELECT 1";

        List<string> statements = SqlScriptSplitter.Split(script);

        Assert.AreEqual(4, statements.Count);
        StringAssert.EndsWith("return s + \";\"; $$", statements[0]);
        StringAssert.EndsWith("$x$ return \"$$;\"; $x$", statements[1]);
        Assert.AreEqual("SELECT f('a;b')", statements[2]);

        // With no closing quote the rest of the script is one statement, and the parser reports it.
        Assert.AreEqual(1, SqlScriptSplitter.Split("CREATE FUNCTION f() RETURNS STRING LANGUAGE csharp AS $$ return 1; SELECT 2;").Count);

        // A dollar that starts no quote is one character.
        Assert.AreEqual(2, SqlScriptSplitter.Split("SELECT 'a$'; SELECT 2").Count);
    }

    [Test]
    public void TheDollarQuoteOfARenderedBody_AlwaysClosesAtTheEnd()
    {
        foreach (string body in new[] { "return 1;", "return \"$$\";", "x$", "$fn$ and $$", "$fn", "$fn$ $fn1$ $$ $fn", "" })
        {
            InlineFunctionDefinition function = new() { Name = "f", Source = body, ReturnType = ColumnType.String };
            NodeAst parsed = SQLParserProcessor.Parse(InlineFunctionSignature.RenderCreate(function));

            Assert.AreEqual(body, parsed.extendedTwo!.yytext, InlineFunctionSignature.DollarQuoteFor(body));
        }
    }
}
