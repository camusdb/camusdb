/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The syntax of the inline trigger statements: <c>CREATE TRIGGER ... LANGUAGE csharp AS $$ body $$</c>,
/// <c>SHOW TRIGGERS [ON table]</c> and <c>SHOW CREATE TRIGGER name ON table</c>, with the words the
/// grammar checks without reserving them.
/// </summary>
[TestFixture]
public sealed class TestSQLParserInlineTriggers
{
    [Test]
    public void CreateInlineTrigger_ParsesEveryPart()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TRIGGER Slug ON posts BEFORE INSERT, UPDATE LANGUAGE csharp AS $$ NEW!.slug = \"x\"; $$");

        Assert.AreEqual(NodeType.CreateTrigger, ast.nodeType);
        Assert.AreEqual("Slug", ast.leftAst!.yytext);
        Assert.AreEqual("posts", ast.rightAst!.yytext);
        Assert.AreEqual("before", ast.yytext);
        Assert.AreEqual("insert,update", ast.extendedOne!.yytext!.ToLowerInvariant());
        Assert.AreEqual(NodeType.FunctionBody, ast.extendedTwo!.nodeType);
        Assert.AreEqual(" NEW!.slug = \"x\"; ", ast.extendedTwo.yytext, "the body is the raw text between the quotes");
        Assert.AreEqual("csharp", ast.extendedThree!.yytext);

        NodeAst tagged = SQLParserProcessor.Parse("create trigger t on posts after delete language CSharp as $fn$ var s = \"$$\"; $fn$;");
        Assert.AreEqual(" var s = \"$$\"; ", tagged.extendedTwo!.yytext, "a tagged quote lets the body hold $$");
        Assert.AreEqual("after", tagged.yytext);
    }

    [Test]
    public void CreateInlineTrigger_Refusals()
    {
        static string Code(string sql) => Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!.Code;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Code("CREATE TRIGGER t ON posts BEFORE INSERT LANGUAGE java AS $$ x(); $$"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Code("CREATE TRIGGER t ON posts BEFORE INSERT DIALECT csharp AS $$ x(); $$"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Code("CREATE TRIGGER t ON posts DURING INSERT LANGUAGE csharp AS $$ x(); $$"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Code("CREATE TRIGGER t ON posts BEFORE INSERT, INSERT LANGUAGE csharp AS $$ x(); $$"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Code("CREATE TRIGGERS t ON posts BEFORE INSERT LANGUAGE csharp AS $$ x(); $$"));
    }

    [Test]
    public void ShowTriggers_AndShowCreateTrigger()
    {
        NodeAst all = SQLParserProcessor.Parse("SHOW TRIGGERS");
        Assert.AreEqual(NodeType.ShowTriggers, all.nodeType);
        Assert.IsNull(all.leftAst);

        NodeAst one = SQLParserProcessor.Parse("show triggers on posts");
        Assert.AreEqual(NodeType.ShowTriggers, one.nodeType);
        Assert.AreEqual("posts", one.leftAst!.yytext);

        NodeAst create = SQLParserProcessor.Parse("SHOW CREATE TRIGGER slug ON posts");
        Assert.AreEqual(NodeType.ShowCreateTrigger, create.nodeType);
        Assert.AreEqual("slug", create.leftAst!.yytext);
        Assert.AreEqual("posts", create.rightAst!.yytext);

        Assert.AreEqual(NodeType.ShowCreateFunction, SQLParserProcessor.Parse("SHOW CREATE FUNCTION slugify").nodeType, "the shorter form is still a function");

        NodeAst quoted = SQLParserProcessor.Parse("SHOW CREATE TRIGGER `slug` ON `posts`");
        Assert.AreEqual("slug", quoted.leftAst!.yytext, "the backquoted names a dump writes");
        Assert.AreEqual("posts", quoted.rightAst!.yytext);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW TRIGGER ON posts"))!.Code);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW CREATE TRIGGERS slug ON posts"))!.Code);
    }
}
