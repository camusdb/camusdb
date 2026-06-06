
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.SQLParser;
using NUnit.Framework.Internal;

namespace CamusDB.Tests.SQLParser;

public class TestSQLParser
{
    private static NodeAst SelectFromClause(NodeAst selectAst)
    {
        Assert.AreEqual(NodeType.Select, selectAst.nodeType);
        return selectAst.rightAst!;
    }

    private static NodeAst SelectFromTable(NodeAst selectAst)
    {
        NodeAst from = SelectFromClause(selectAst);
        Assert.AreEqual(NodeType.TableReference, from.nodeType);
        return from.leftAst!;
    }

    private static string SelectFromTableName(NodeAst selectAst) =>
        SelectFromTable(selectAst).yytext!;

    [Test]
    public void TestParseSimpleSelect()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);

        Assert.AreEqual("some_field", ast.leftAst!.yytext);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleSelect2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelect3()
    {
        NodeAst ast = SQLParserProcessor.Parse("select some_field, another_field FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelect4()
    {
        NodeAst ast = SQLParserProcessor.Parse("select some_field, another_field from some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelect5()
    {
        NodeAst ast = SQLParserProcessor.Parse("Select some_field, another_field From some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseNormalizesMixedCaseIdentifiers()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT Some_Field, Another_Field FROM Some_Table WHERE UserId = 1 ORDER BY CreatedAt DESC");

        Assert.AreEqual("some_field", ast.leftAst!.leftAst!.yytext);
        Assert.AreEqual("another_field", ast.leftAst!.rightAst!.yytext);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual("userid", ast.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("createdat", ast.extendedTwo!.leftAst!.yytext);
    }

    [Test]
    public void TestParseNormalizesEscapedIdentifiers()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `UserName` FROM `Users`");

        Assert.AreEqual("username", ast.leftAst!.yytext);
        Assert.AreEqual("users", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleSelectWhere()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\"");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" AND yy = 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE (xx = 100)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere6()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = 100 OR x != 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere7()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx > 100 AND x < 200");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere8()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx >= 100 AND x <= 200");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere9()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE (xx >= 100) AND (x <= 200)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprAnd, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere10()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE ((xx >= 100) AND (x <= 200)) OR x = 100");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprOr, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere11()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE enabled OR enabled");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprOr, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere12()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE enabled=true OR enabled=true");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprOr, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere13()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE ((xx >= @xx) AND (x <= @x)) OR x = @x");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprOr, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere14()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx LIKE \"prefix%\"");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprLike, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere15()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx ILIKE \"prefix%\"");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprILike, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere16()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx IS NULL");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprIsNull, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhere17()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx IS NOT NULL");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprIsNotNull, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhereBetween()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT year FROM robots WHERE year BETWEEN 2001 AND 2004");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.ExprBetween, ast.extendedOne!.nodeType);
        Assert.AreEqual("year", ast.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("2001", ast.extendedOne!.extendedOne!.yytext);
        Assert.AreEqual("2004", ast.extendedOne!.extendedTwo!.yytext);
    }

    [Test]
    public void TestParseSelectDistinct()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT DISTINCT code FROM teams");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("1", ast.yytext);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("code", ast.leftAst.yytext);
        Assert.AreEqual("teams", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSelectDistinctMultiColumnWithOrderBy()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT DISTINCT code, name FROM teams ORDER BY code");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual("1", ast.yytext);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedTwo!.nodeType);
        Assert.AreEqual("code", ast.extendedTwo.yytext);
    }

    [Test]
    public void TestParseSelectWithoutDistinctHasNullFlag()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT code FROM teams");

        Assert.IsNull(ast.yytext);
    }

    [Test]
    public void TestParseCountDistinctFunction_throwsParseError()
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(
            () => SQLParserProcessor.Parse("SELECT COUNT(DISTINCT code) FROM teams"))!;

        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, exception.Code);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table ORDER BY xx");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC, yy");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC, yy DESC");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy6()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx ASC");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.SortAsc, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectOrderBy7()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field, another_field FROM some_table WHERE xx = \"100\" ORDER BY xx DESC");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.SortDesc, ast.extendedTwo!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectAll()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.ExprAllFields, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleSelectAll2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT *, * FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleEscapedIdentifiers()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `someField`, `someData`, `someOtherField` FROM `some_table`");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleAggregate()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT COUNT(*) FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.ExprFuncCall, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType`,`author` FROM `some_table` WHERE `status`= @status_0 LIMIT 1000");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimit2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType`,`author` FROM `some_table` LIMIT 1000");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);        
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimit3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType`,`author` FROM `some_table` LIMIT @limit");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Placeholder, ast.extendedThree!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimit4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType`,`author` FROM `some_table` ORDER BY `id` LIMIT @limit");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Placeholder, ast.extendedThree!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimit5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType`,`author` FROM `some_table` WHERE `status`= @status_0 ORDER BY `id` LIMIT 1000");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimitOffset()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType` FROM some_table WHERE `status`= @status_0 LIMIT 20 OFFSET 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedFour!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimitOffset2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType` FROM some_table LIMIT 20 OFFSET 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedFour!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimitOffset3()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType` FROM some_table LIMIT @limit OFFSET @offset");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Placeholder, ast.extendedThree!.nodeType);
        Assert.AreEqual(NodeType.Placeholder, ast.extendedFour!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimitOffset4()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType` FROM some_table ORDER by `id` LIMIT 20 OFFSET 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedFour!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionLimitOffset5()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `id`,`branch`,`jobType` FROM some_table WHERE `status`= @status_0 ORDER by `id` LIMIT 20 OFFSET 10");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedFour!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectProjectionAliases()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT year + 100 AS y FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.ExprAlias, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));        
    }

    [Test]
    public void TestParseSimpleSelectProjectionAliases2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `year` + 100 AS `y` FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.ExprAlias, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));        
    }

    [Test]
    public void TestParseSelectForceIndex()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT some_field FROM some_table@{FORCE_INDEX=pk}");

        Assert.AreEqual(NodeType.Select, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.nodeType);

        Assert.AreEqual("some_field", ast.leftAst!.yytext);
        Assert.AreEqual("some_table", ast.rightAst!.leftAst!.yytext);
        Assert.IsNotNull(ast.rightAst!.extendedOne);
        Assert.AreEqual("force_index", ast.rightAst!.extendedOne!.rightAst!.yytext);
        Assert.AreEqual("pk", ast.rightAst!.extendedOne!.extendedOne!.yytext);
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
    public void TestParseUpdateMultiSet3()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE `some_table` SET `some_field` = `some_value`, `some_other_field` = 100, `bool_field` = false, str_field = null WHERE TRUE");

        Assert.AreEqual(NodeType.Update, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.UpdateList, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleDelete()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM some_table WHERE 1=1");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, ast.rightAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleDelete1()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM `some_table` WHERE `name` = `other_field`");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, ast.rightAst!.nodeType);

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
    public void TestParseSimpleInsert4()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO some_table (id, z) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"aaaa\")");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprFuncCall, ast.extendedOne!.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert5()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO `some_table` (`id`, `z`) VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"aaaa\")");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprFuncCall, ast.extendedOne!.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert6()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO `some_table` VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"aaaa\")");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.Null(ast.rightAst);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprFuncCall, ast.extendedOne!.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleInsert7()
    {
        NodeAst ast = SQLParserProcessor.Parse("INSERT INTO `some_table` VALUES (STR_ID(\"507f1f77bcf86cd799439011\"), \"aaaa\"), (STR_ID(\"507f1f77bcf86cd799439013\"), \"aaaaaaa\")");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.Null(ast.rightAst);
        Assert.AreEqual(NodeType.InsertBatchList, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedOne!.leftAst!.nodeType);

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

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table (\nid INT64 PRIMARY KEY NOT NULL,\nname INT64 UNIQUE NOT NULL, year INT64 NULL)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints2()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE `some_table` (\n`id` INT64 PRIMARY KEY NOT NULL,\n`name` INT64 UNIQUE NOT NULL, `year` INT64 NULL)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints3()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE `some_table` (\n`id` INT64 NOT NULL,\n`name` INT64 UNIQUE NOT NULL, `year` INT64 NULL) PRIMARY KEY (`id`)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints4()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE `some_table` (\n`id` INT64 NOT NULL,\n`name` INT64 UNIQUE NOT NULL, `year` INT64 NULL) PRIMARY KEY (`id`, `name`)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints5()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE `some_table` (\n`id` INT64 NOT NULL,\n`name` INT64 UNIQUE NOT NULL, `year` INT64 NULL) PRIMARY KEY (`id` DESC)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableMultiConstraints6()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE `some_table` (\n`id` INT64 NOT NULL,\n`name` INT64 UNIQUE NOT NULL, `year` INT64 NULL) PRIMARY KEY (`id` ASC, `name` ASC)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableDefault1()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE some_table ( id INT64 NOT NULL, name INT64 DEFAULT (100))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleCreateTableIfNotExists()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE IF NOT EXISTS some_table ( id INT64 NOT NULL, name INT64 DEFAULT (100))");

        Assert.AreEqual(NodeType.CreateTableIfNotExists, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseCreateTableWithInlineConstraint()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE IF NOT EXISTS drivers (\n" +
            "    id OID NOT NULL,\n" +
            "    city STRING NOT NULL,\n" +
            "    name STRING,\n" +
            "    dl STRING UNIQUE,\n" +
            "    address STRING,\n" +
            "    CONSTRAINT \"primary\" PRIMARY KEY (city ASC, id ASC)\n" +
            ")"
        );

        Assert.AreEqual(NodeType.CreateTableIfNotExists, ast.nodeType);
        Assert.AreEqual("drivers", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE some_table ADD COLUMN year INT64 NULL");

        Assert.AreEqual(NodeType.AlterTableAddColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.TypeInteger64, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable1()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD COLUMN `year` INT64 NULL");

        Assert.AreEqual(NodeType.AlterTableAddColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.TypeInteger64, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable2()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE some_table ADD COLUMN year INT64");

        Assert.AreEqual(NodeType.AlterTableAddColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.TypeInteger64, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable3()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE some_table ADD COLUMN enabled BOOL NULL");

        Assert.AreEqual(NodeType.AlterTableAddColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("enabled", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.TypeBool, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable4()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE some_table DROP COLUMN year");

        Assert.AreEqual(NodeType.AlterTableDropColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year", ast.rightAst!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable5()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` DROP COLUMN `year`");

        Assert.AreEqual(NodeType.AlterTableDropColumn, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year", ast.rightAst!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable6()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD INDEX `year_index` (`year`)");

        Assert.AreEqual(NodeType.AlterTableAddIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year_index", ast.rightAst!.yytext);
        Assert.AreEqual("year", ast.extendedOne!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable7()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` DROP INDEX `year_index`");

        Assert.AreEqual(NodeType.AlterTableDropIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable8()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD PRIMARY KEY (`id`)");

        Assert.AreEqual(NodeType.AlterTableAddPrimaryKey, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("id", ast.rightAst!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable9()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` DROP PRIMARY KEY");

        Assert.AreEqual(NodeType.AlterTableDropPrimaryKey, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);        
    }

    [Test]
    public void TestParseSimpleAlterTable10()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD UNIQUE `year_index` (`year`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year_index", ast.rightAst!.yytext);
        Assert.AreEqual("year", ast.extendedOne!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable11()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD UNIQUE INDEX `year_index` (`year`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);

        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("year_index", ast.rightAst!.yytext);
        Assert.AreEqual("year", ast.extendedOne!.yytext);
    }

    [Test]
    public void TestParseSimpleAlterTable12()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD PRIMARY KEY (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddPrimaryKey, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable13()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD INDEX `usersid_idx` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable14()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD UNIQUE INDEX `usersid_idx` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseSimpleAlterTable15()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE `some_table` ADD UNIQUE `usersid_idx` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseCreateIndex1()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE INDEX `usersid_idx` ON `some_table` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseCreateIndex2()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE UNIQUE INDEX `usersid_idx` ON `some_table` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndex, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseCreateIndexIfNotExists()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE INDEX IF NOT EXISTS `usersid_idx` ON `some_table` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddIndexIfNotExists, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseCreateUniqueIndexIfNotExists()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE UNIQUE INDEX IF NOT EXISTS `usersid_idx` ON `some_table` (`usersId`, `id`)");

        Assert.AreEqual(NodeType.AlterTableAddUniqueIndexIfNotExists, ast.nodeType);

        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", ast.leftAst!.yytext);
        Assert.AreEqual("usersid_idx", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.IndexIdentifierList, ast.extendedOne!.nodeType);
    }

    [Test]
    public void TestParseShowDatabase()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW DATABASE");

        Assert.AreEqual(NodeType.ShowDatabase, ast.nodeType);
    }

    [Test]
    public void TestParseShowTables()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW TABLES");

        Assert.AreEqual(NodeType.ShowTables, ast.nodeType);
    }

    [Test]
    public void TestParseShowColumns()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW COLUMNS FROM robots");

        Assert.AreEqual(NodeType.ShowColumns, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseShowColumns2()
    {
        NodeAst ast = SQLParserProcessor.Parse("DESC robots");

        Assert.AreEqual(NodeType.ShowColumns, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseShowColumns3()
    {
        NodeAst ast = SQLParserProcessor.Parse("DESCRIBE robots");

        Assert.AreEqual(NodeType.ShowColumns, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseShowIndexes()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW INDEXES FROM robots");

        Assert.AreEqual(NodeType.ShowIndexes, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseShowIndexes2()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW INDEX FROM robots");

        Assert.AreEqual(NodeType.ShowIndexes, ast.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.nodeType);
    }

    #region QP0.2 pending query feature acceptance fixtures

    [Test]
    public void TestParseSelectGroupBySingleColumn()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT role, COUNT(*) FROM app_users GROUP BY role");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedFive);
        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedFive.leftAst!.nodeType);
        Assert.AreEqual("role", ast.extendedFive.leftAst.yytext);
    }

    [Test]
    public void TestParseSelectGroupByMultipleColumns()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT role, department, COUNT(*) FROM app_users GROUP BY role, department");

        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.ExprList, ast.extendedFive.leftAst!.nodeType);
        Assert.AreEqual("role", ast.extendedFive.leftAst.leftAst!.yytext);
        Assert.AreEqual("department", ast.extendedFive.leftAst.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectGroupByWithWhereOrderByLimitOffset()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT role FROM app_users WHERE enabled = true GROUP BY role ORDER BY role LIMIT 10 OFFSET 5");

        Assert.AreEqual(NodeType.ExprEquals, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedTwo!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
        Assert.AreEqual(10, long.Parse(ast.extendedThree.yytext!));
        Assert.AreEqual(NodeType.Integer, ast.extendedFour!.nodeType);
        Assert.AreEqual(5, long.Parse(ast.extendedFour.yytext!));
    }

    [Test]
    public void TestParseSelectGroupByExpression()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT year + 100 AS y FROM robots GROUP BY year + 100");

        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.ExprAdd, ast.extendedFive.leftAst!.nodeType);
    }

    [Test]
    public void TestParseSelectGroupByHaving()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT role, COUNT(*) AS x FROM app_users GROUP BY role HAVING x > 0");

        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.Having, ast.extendedSix!.nodeType);
        Assert.AreEqual(NodeType.ExprGreaterThan, ast.extendedSix.leftAst!.nodeType);
        Assert.AreEqual("x", ast.extendedSix.leftAst.leftAst!.yytext);
    }

    [Test]
    public void TestParseSelectGroupByHavingGroupKey()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT role FROM app_users GROUP BY role HAVING role = 'admin'");

        Assert.AreEqual(NodeType.Having, ast.extendedSix!.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, ast.extendedSix.leftAst!.nodeType);
    }

    [Test]
    public void TestParseSelectAggregateOnlyHaving()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT COUNT(*) AS x FROM robots HAVING x > 0");

        Assert.IsNull(ast.extendedFive);
        Assert.AreEqual(NodeType.Having, ast.extendedSix!.nodeType);
    }

    [Test]
    public void TestParseSelectGroupByHavingOrderByLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT role, COUNT(*) AS x FROM app_users WHERE enabled = true GROUP BY role HAVING x > 0 ORDER BY role LIMIT 10");

        Assert.AreEqual(NodeType.ExprEquals, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.GroupBy, ast.extendedFive!.nodeType);
        Assert.AreEqual(NodeType.Having, ast.extendedSix!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedTwo!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.extendedThree!.nodeType);
    }

    [Test]
    public void TestParseSelectInnerJoin()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.Join, ast.rightAst!.nodeType);

        NodeAst join = ast.rightAst!;
        Assert.AreEqual(NodeType.TableReference, join.leftAst!.nodeType);
        Assert.AreEqual("app_users", join.leftAst!.leftAst!.yytext);
        Assert.AreEqual("u", join.leftAst!.rightAst!.yytext);

        Assert.AreEqual(NodeType.TableReference, join.rightAst!.nodeType);
        Assert.AreEqual("posts", join.rightAst!.leftAst!.yytext);
        Assert.AreEqual("p", join.rightAst!.rightAst!.yytext);

        Assert.AreEqual(NodeType.ExprEquals, join.extendedOne!.nodeType);
        Assert.AreEqual("p.user_id", join.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("u.id", join.extendedOne!.rightAst!.yytext);

        Assert.AreEqual("u.email", ast.leftAst!.leftAst!.yytext);
        Assert.AreEqual("p.title", ast.leftAst!.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectJoinDerivedTable()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT u.email, d.post_count FROM app_users u "
            + "JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "ON d.user_id = u.id ORDER BY u.email");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.Join, ast.rightAst!.nodeType);

        NodeAst join = ast.rightAst!;
        Assert.AreEqual(NodeType.TableReference, join.leftAst!.nodeType);
        Assert.AreEqual(NodeType.DerivedTableReference, join.rightAst!.nodeType);
        Assert.AreEqual("d", join.rightAst!.rightAst!.yytext);
        Assert.AreEqual(NodeType.Select, join.rightAst!.leftAst!.nodeType);
        Assert.AreEqual(NodeType.GroupBy, join.rightAst!.leftAst!.extendedFive!.nodeType);
        Assert.AreEqual("d.user_id", join.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("u.id", join.extendedOne!.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectCommaJoinTwoSources()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT r.id, u.amount FROM robots r, user_robots u WHERE r.id = u.robots_id");

        Assert.AreEqual(NodeType.CommaJoin, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.leftAst!.nodeType);
        Assert.AreEqual("robots", ast.rightAst!.leftAst!.leftAst!.yytext);
        Assert.AreEqual("r", ast.rightAst!.leftAst!.rightAst!.yytext);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.rightAst!.nodeType);
        Assert.AreEqual("user_robots", ast.rightAst!.rightAst!.leftAst!.yytext);
        Assert.AreEqual("u", ast.rightAst!.rightAst!.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectCommaJoinThreeSources()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT r.id, u.amount, p.title FROM robots r, user_robots u, posts p "
            + "WHERE r.id = u.robots_id AND u.amount > 0");

        Assert.AreEqual(NodeType.CommaJoin, ast.rightAst!.nodeType);
        Assert.AreEqual(NodeType.CommaJoinTableList, ast.rightAst!.rightAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.rightAst!.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.rightAst!.rightAst!.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSelectMixedCommaAndExplicitJoin_throws()
    {
        Assert.Throws<CamusDBException>(() =>
            SQLParserProcessor.Parse(
                "SELECT r.id FROM robots r, user_robots u JOIN posts p ON p.user_id = u.id"));
    }

    [Test]
    public void TestParseSelectTableAliasWithAs()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM robots AS r");

        NodeAst from = SelectFromClause(ast);
        Assert.AreEqual(NodeType.TableReference, from.nodeType);
        Assert.AreEqual("robots", from.leftAst!.yytext);
        Assert.AreEqual("r", from.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectTableAliasWithoutAs()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT id FROM robots r");

        NodeAst from = SelectFromClause(ast);
        Assert.AreEqual(NodeType.TableReference, from.nodeType);
        Assert.AreEqual("robots", from.leftAst!.yytext);
        Assert.AreEqual("r", from.rightAst!.yytext);
    }

    [Test]
    public void TestParseSelectInnerJoinExplicit()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT u.email FROM app_users u INNER JOIN posts p ON p.user_id = u.id");

        Assert.AreEqual(NodeType.Join, ast.rightAst!.nodeType);
    }

    [Test]
    public void TestParseSelectQualifiedColumnsInWhere()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT u.email FROM app_users u WHERE u.enabled = true");

        Assert.AreEqual("u.enabled", ast.extendedOne!.leftAst!.yytext);
    }

    [Test]
    public void TestParseSelectScalarSubqueryInWhere()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE year = (SELECT MAX(year) FROM robots)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne);
        Assert.AreEqual(NodeType.ExprEquals, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprScalarSubquery, ast.extendedOne!.rightAst!.nodeType);
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.rightAst!.leftAst!.nodeType);
    }

    [Test]
    public void TestParseSelectInSubquery()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM app_users WHERE id IN (SELECT user_id FROM posts WHERE published = true)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne, "WHERE should capture the IN (subquery) predicate");
        Assert.AreEqual(NodeType.ExprInSubquery, ast.extendedOne!.nodeType);
        Assert.AreEqual("id", ast.extendedOne!.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.rightAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.extendedOne!.rightAst!.rightAst!.nodeType);
        Assert.AreEqual("posts", ast.extendedOne!.rightAst!.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void TestParseSelectNotInSubquery()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE id NOT IN (SELECT robots_id FROM user_robots)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne);
        Assert.AreEqual(NodeType.ExprNotInSubquery, ast.extendedOne!.nodeType);
        Assert.AreEqual("id", ast.extendedOne!.leftAst!.yytext);
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.rightAst!.nodeType);
        Assert.AreEqual("user_robots", ast.extendedOne!.rightAst!.rightAst!.leftAst!.yytext);
    }

    [Test]
    public void TestParseSelectExistsSubquery()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT * FROM app_users WHERE EXISTS (SELECT * FROM posts WHERE posts.user_id = app_users.id)");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne, "WHERE should capture the EXISTS (subquery) predicate");
        Assert.AreEqual(NodeType.ExprExistsSubquery, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.Select, ast.extendedOne!.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TableReference, ast.extendedOne!.leftAst!.rightAst!.nodeType);
        Assert.AreEqual("posts", ast.extendedOne!.leftAst!.rightAst!.leftAst!.yytext);
        Assert.AreEqual(NodeType.ExprEquals, ast.extendedOne!.leftAst!.extendedOne!.nodeType);
        Assert.AreEqual("posts.user_id", ast.extendedOne!.leftAst!.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("app_users.id", ast.extendedOne!.leftAst!.extendedOne!.rightAst!.yytext);
    }

    [Test]
    public void TestParseCastAsString()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(name AS string) FROM robots");

        Assert.AreEqual(NodeType.ExprCast, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.leftAst!.leftAst!.nodeType);
        Assert.AreEqual("name", ast.leftAst!.leftAst!.yytext);
        Assert.AreEqual(NodeType.TypeString, ast.leftAst!.rightAst!.nodeType);
    }

    [Test]
    public void TestParseCastAsFloat64()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(1 AS float64) FROM robots");

        Assert.AreEqual(NodeType.ExprCast, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.Integer, ast.leftAst!.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TypeFloat64, ast.leftAst!.rightAst!.nodeType);
    }

    [Test]
    public void TestParseNestedCast()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(CAST(1 AS string) AS int64) FROM robots");

        Assert.AreEqual(NodeType.ExprCast, ast.leftAst!.nodeType);
        Assert.AreEqual(NodeType.ExprCast, ast.leftAst!.leftAst!.nodeType);
        Assert.AreEqual(NodeType.TypeInteger64, ast.leftAst!.rightAst!.nodeType);
    }

    #endregion

    #region DELETE/UPDATE LIMIT parsing

    [Test]
    public void TestParseDeleteWithLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM teams WHERE code = 'BEL' LIMIT 1");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);
        Assert.AreEqual("teams", ast.leftAst!.yytext);
        Assert.IsNotNull(ast.rightAst);
        Assert.IsNotNull(ast.extendedOne, "limit node must land in extendedOne");
        Assert.AreEqual(NodeType.Integer, ast.extendedOne!.nodeType);
        Assert.AreEqual("1", ast.extendedOne!.yytext);
    }

    [Test]
    public void TestParseDeleteWithLimitNoLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM teams WHERE code = 'BEL'");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);
        Assert.IsNull(ast.extendedOne, "opt_limit must be null when absent");
    }

    [Test]
    public void TestParseUpdateWithLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE teams SET score = 0 WHERE code = 'BEL' LIMIT 2");

        Assert.AreEqual(NodeType.Update, ast.nodeType);
        Assert.AreEqual("teams", ast.leftAst!.yytext);
        Assert.IsNotNull(ast.extendedOne, "where node must land in extendedOne");
        Assert.IsNotNull(ast.extendedTwo, "limit node must land in extendedTwo");
        Assert.AreEqual(NodeType.Integer, ast.extendedTwo!.nodeType);
        Assert.AreEqual("2", ast.extendedTwo!.yytext);
    }

    [Test]
    public void TestParseUpdateWithLimitNoLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("UPDATE teams SET score = 0 WHERE code = 'BEL'");

        Assert.AreEqual(NodeType.Update, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne);
        Assert.IsNull(ast.extendedTwo, "opt_limit must be null when absent");
    }

    [Test]
    public void TestParseDeleteWithParameterizedLimit()
    {
        NodeAst ast = SQLParserProcessor.Parse("DELETE FROM teams WHERE code = 'BEL' LIMIT @max");

        Assert.AreEqual(NodeType.Delete, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne);
        Assert.AreEqual(NodeType.Placeholder, ast.extendedOne!.nodeType);
        Assert.AreEqual("@max", ast.extendedOne!.yytext);
    }

    #endregion

    #region EXPLAIN

    [Test]
    public void TestParseExplainSelect()
    {
        NodeAst ast = SQLParserProcessor.Parse("EXPLAIN SELECT * FROM users");

        Assert.AreEqual(NodeType.Explain, ast.nodeType);
        Assert.IsNotNull(ast.leftAst, "Explain must carry the wrapped SELECT as its left child");
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseExplainLogical()
    {
        NodeAst ast = SQLParserProcessor.Parse("EXPLAIN (LOGICAL) SELECT * FROM users");

        Assert.AreEqual(NodeType.ExplainLogical, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseExplainPhysical()
    {
        NodeAst ast = SQLParserProcessor.Parse("EXPLAIN (PHYSICAL) SELECT * FROM users");

        Assert.AreEqual(NodeType.ExplainPhysical, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseExplainLowercase()
    {
        NodeAst ast = SQLParserProcessor.Parse("explain select id from robots");

        Assert.AreEqual(NodeType.Explain, ast.nodeType);
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseExplainLogicalLowercase()
    {
        NodeAst ast = SQLParserProcessor.Parse("explain (logical) select * from robots");

        Assert.AreEqual(NodeType.ExplainLogical, ast.nodeType);
        Assert.AreEqual(NodeType.Select, ast.leftAst!.nodeType);
    }

    [Test]
    public void TestParseExplainWrapsFullSelect()
    {
        // The wrapped SELECT AST must carry the table and WHERE clause intact.
        NodeAst ast = SQLParserProcessor.Parse(
            "EXPLAIN SELECT id, name FROM robots WHERE year = 2000 ORDER BY year LIMIT 5");

        Assert.AreEqual(NodeType.Explain, ast.nodeType);

        NodeAst inner = ast.leftAst!;
        Assert.AreEqual(NodeType.Select, inner.nodeType);
        // FROM clause (rightAst of Select) must be a table reference.
        Assert.AreEqual(NodeType.TableReference, inner.rightAst!.nodeType);
    }

    [Test]
    public void TestParseExplainUnknownOptionThrows()
    {
        // Unrecognised option words are rejected so typos don't silently degrade.
        Assert.Throws<CamusDB.Core.CamusDBException>(() =>
            SQLParserProcessor.Parse("EXPLAIN (VERBOOSE) SELECT * FROM users"));
    }

    [Test]
    public void TestParseExplainAnalyzeParsesAsExplainAnalyzeNode()
    {
        // R5: ANALYZE is now a valid EXPLAIN option.
        NodeAst? ast = SQLParserProcessor.Parse("EXPLAIN (ANALYZE) SELECT * FROM users");
        Assert.That(ast, Is.Not.Null);
        Assert.That(ast!.nodeType, Is.EqualTo(NodeType.ExplainAnalyze));
    }

    #endregion
}
