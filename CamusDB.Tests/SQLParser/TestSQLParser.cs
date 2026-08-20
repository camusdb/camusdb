
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
    public void TestParsePreservesMixedCaseIdentifiers()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT Some_Field, Another_Field FROM Some_Table WHERE UserId = 1 ORDER BY CreatedAt DESC");

        // Identifiers keep the exact case the user wrote; case-insensitive matching happens at lookup time.
        Assert.AreEqual("Some_Field", ast.leftAst!.leftAst!.yytext);
        Assert.AreEqual("Another_Field", ast.leftAst!.rightAst!.yytext);
        Assert.AreEqual("Some_Table", SelectFromTableName(ast));
        Assert.AreEqual("UserId", ast.extendedOne!.leftAst!.yytext);
        Assert.AreEqual("CreatedAt", ast.extendedTwo!.leftAst!.yytext);
    }

    [Test]
    public void TestParsePreservesEscapedIdentifierCase()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT `UserName` FROM `Users`");

        // Backtick quoting strips the backticks but keeps the original case.
        Assert.AreEqual("UserName", ast.leftAst!.yytext);
        Assert.AreEqual("Users", SelectFromTableName(ast));
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
    public void TestParseSimpleSelectWhereNotBoolColumn()
    {
        // Unary prefix NOT applied to a boolean column.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM robots WHERE NOT enabled");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.ExprNot, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.Identifier, ast.extendedOne!.leftAst!.nodeType);
        Assert.AreEqual("enabled", ast.extendedOne!.leftAst!.yytext);
    }

    [Test]
    public void TestParseSimpleSelectWhereNotBindsLooserThanComparison()
    {
        // NOT a = b parses as NOT (a = b) — comparison binds tighter than NOT.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM robots WHERE NOT year = 2001");

        Assert.AreEqual(NodeType.ExprNot, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprEquals, ast.extendedOne!.leftAst!.nodeType);
    }

    [Test]
    public void TestParseSimpleSelectWhereNotBindsTighterThanAnd()
    {
        // NOT enabled AND ready parses as (NOT enabled) AND ready — NOT binds tighter than AND.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM robots WHERE NOT enabled AND ready");

        Assert.AreEqual(NodeType.ExprAnd, ast.extendedOne!.nodeType);
        Assert.AreEqual(NodeType.ExprNot, ast.extendedOne!.leftAst!.nodeType);
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

    // ── Reserved keywords as backtick-quoted identifiers (gap 3) ──────────────

    [Test]
    public void TestReservedWordAsColumnName_Select()
    {
        // `key`, `select`, `from`, `index` are all reserved tokens; backtick-quoting
        // must allow them as column references in SELECT.
        NodeAst ast = SQLParserProcessor.Parse("SELECT `key`, `select`, `from`, `index` FROM some_table");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.AreEqual(NodeType.IdentifierList, ast.leftAst!.nodeType);
        Assert.AreEqual("some_table", SelectFromTableName(ast));
    }

    [Test]
    public void TestReservedWordAsColumnName_CreateTable()
    {
        // Reserved words used as column names inside CREATE TABLE must parse successfully.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (`key` STRING NOT NULL, `select` INT64, `index` STRING)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        Assert.AreEqual("t", ast.leftAst!.yytext);
    }

    [Test]
    public void TestSmallIntAndFloatTypeAliases()
    {
        // `smallint` is an alias for INT64 and `float` is an alias for FLOAT64.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (`a` SMALLINT, `b` FLOAT)");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);

        NodeAst itemList = ast.rightAst!;
        Assert.AreEqual(NodeType.CreateTableItemList, itemList.nodeType);

        NodeAst firstItem = itemList.leftAst!;
        NodeAst secondItem = itemList.rightAst!;

        Assert.AreEqual(NodeType.TypeInteger64, firstItem.rightAst!.nodeType);
        Assert.AreEqual(NodeType.TypeFloat64, secondItem.rightAst!.nodeType);
    }

    [Test]
    public void TestReservedWordAsColumnName_CreateTableWithConstraints()
    {
        // Full round-trip form: column named `key` with PRIMARY KEY and KEY constraints.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (`id` OID, `key` STRING NOT NULL, PRIMARY KEY (`id`), KEY `key_idx` (`key`))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
    }

    [Test]
    public void TestReservedWordAsColumnName_Where()
    {
        // Reserved-word column referenced in WHERE must parse correctly.
        NodeAst ast = SQLParserProcessor.Parse(
            "SELECT `key` FROM some_table WHERE `key` = 'value'");

        Assert.AreEqual(NodeType.Select, ast.nodeType);
        Assert.IsNotNull(ast.extendedOne, "WHERE clause must be present");
    }

    [Test]
    public void TestReservedWordAsColumnName_Insert()
    {
        // INSERT with a reserved-word column name in the field list.
        NodeAst ast = SQLParserProcessor.Parse(
            "INSERT INTO t (`key`, `select`) VALUES ('k1', 1)");

        Assert.AreEqual(NodeType.Insert, ast.nodeType);
    }

    [Test]
    public void TestReservedWordAsColumnName_PreservesCase()
    {
        // Backticks let a reserved word be used as an identifier; the case is preserved verbatim
        // (only the backticks are stripped).
        NodeAst ast = SQLParserProcessor.Parse("SELECT `KEY` FROM t");

        Assert.AreEqual("KEY", ast.leftAst!.yytext,
            "Backtick-quoted identifiers keep their original case");
    }

    // ── SHOW CREATE TABLE round-trip: KEY / UNIQUE KEY inside CREATE TABLE ────

    [Test]
    public void TestCreateTable_InlineKeyConstraint()
    {
        // Grammar must accept KEY name (...) inline — this is what SHOW CREATE TABLE emits.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE `robots` (`id` OID NOT NULL, `name` STRING, PRIMARY KEY (`id`), KEY `name_idx` (`name`))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
    }

    [Test]
    public void TestCreateTable_InlineUniqueKeyConstraint()
    {
        // Grammar must accept UNIQUE KEY name (...) inline.
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE `robots` (`id` OID NOT NULL, `code` STRING NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `code_uk` (`code`))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
    }

    [Test]
    public void TestCreateTable_InlineMixedConstraints()
    {
        // Multiple constraint types together (what SHOW CREATE TABLE produces for a fully
        // indexed table).
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE `t` (`id` OID NOT NULL, `code` STRING NOT NULL, `name` STRING, " +
            "PRIMARY KEY (`id`), UNIQUE KEY `code_uk` (`code`), KEY `name_idx` (`name`))");

        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
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
        Assert.AreEqual("FORCE_INDEX", ast.rightAst!.extendedOne!.rightAst!.yytext);
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
    public void TestParseCreateDatabase()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE DATABASE mydb");

        Assert.AreEqual(NodeType.CreateDatabase, ast.nodeType);
        Assert.AreEqual("mydb", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseCreateDatabaseIfNotExists()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE DATABASE IF NOT EXISTS mydb");

        Assert.AreEqual(NodeType.CreateDatabaseIfNotExists, ast.nodeType);
        Assert.AreEqual("mydb", ast.leftAst!.yytext);
    }

    [Test]
    public void TestParseShowTables()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW TABLES");

        Assert.AreEqual(NodeType.ShowTables, ast.nodeType);
        Assert.IsNull(ast.leftAst);
    }

    [Test]
    public void TestParseShowTablesLike()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW TABLES LIKE 'orders_%'");

        Assert.AreEqual(NodeType.ShowTables, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("'orders_%'", ast.leftAst.yytext);
    }

    [Test]
    public void TestParseShowDatabasesLike()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW DATABASES LIKE 'prod_%'");

        Assert.AreEqual(NodeType.ShowDatabases, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("'prod_%'", ast.leftAst.yytext);
    }

    [Test]
    public void TestParseShowEngineStats()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW ENGINE STATS");

        Assert.AreEqual(NodeType.ShowEngineStats, ast.nodeType);
        Assert.IsNull(ast.leftAst);
    }

    [Test]
    public void TestParseShowEngineStatsLowercase()
    {
        Assert.AreEqual(NodeType.ShowEngineStats, SQLParserProcessor.Parse("show engine stats").nodeType);
    }

    [Test]
    public void TestParseShowEngineStatsLike()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW ENGINE STATS LIKE 'raft.wal%'");

        Assert.AreEqual(NodeType.ShowEngineStats, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("'raft.wal%'", ast.leftAst.yytext);
    }

    [Test]
    public void TestParseShowEngineStatsRejectsOtherWords()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW ENGINE FOO"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW FOO STATS"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW ENGINE STATS LIKE 'x' EXTRA"));
    }

    /// <summary>
    /// ENGINE and STATS are matched as identifiers rather than reserved as keywords, precisely so DDL
    /// that already uses those words keeps parsing. Reserving them would have been a silent breaking
    /// change to existing schemas.
    /// </summary>
    [Test]
    public void TestEngineAndStatsRemainOrdinaryIdentifiers()
    {
        Assert.AreEqual(
            NodeType.CreateTable,
            SQLParserProcessor.Parse("CREATE TABLE stats (id oid PRIMARY KEY, engine string(64))").nodeType);

        Assert.AreEqual(NodeType.Select, SQLParserProcessor.Parse("SELECT engine FROM stats").nodeType);
        Assert.AreEqual(NodeType.Select, SQLParserProcessor.Parse("SELECT * FROM engine WHERE stats = 1").nodeType);
        Assert.AreEqual(NodeType.Insert, SQLParserProcessor.Parse("INSERT INTO stats (engine) VALUES ('raft')").nodeType);
    }

    [Test]
    public void TestParseShowVariables()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW VARIABLES");

        Assert.AreEqual(NodeType.ShowVariables, ast.nodeType);
        Assert.IsNull(ast.leftAst);

        Assert.AreEqual(NodeType.ShowVariables, SQLParserProcessor.Parse("show variables").nodeType);
    }

    [Test]
    public void TestParseShowVariablesLike()
    {
        NodeAst ast = SQLParserProcessor.Parse("SHOW VARIABLES LIKE '%cache%'");

        Assert.AreEqual(NodeType.ShowVariables, ast.nodeType);
        Assert.IsNotNull(ast.leftAst);
        Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType);
        Assert.AreEqual("'%cache%'", ast.leftAst.yytext);
    }

    /// <summary>
    /// Both quoting styles and the escape-processing form reach the pattern, so an operator does not
    /// have to know which one this statement happens to accept.
    /// </summary>
    [Test]
    public void TestParseShowVariablesLikeAcceptsEveryLiteralForm()
    {
        foreach (string literal in new[] { "'ttl_%'", "\"ttl_%\"", "E'ttl_%'" })
        {
            NodeAst ast = SQLParserProcessor.Parse($"SHOW VARIABLES LIKE {literal}");

            Assert.AreEqual(NodeType.ShowVariables, ast.nodeType, literal);
            Assert.AreEqual(NodeType.String, ast.leftAst!.nodeType, literal);
        }
    }

    [Test]
    public void TestParseShowVariablesRejectsOtherWords()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW FOO"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW FOO LIKE 'x'"));
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SHOW VARIABLES LIKE 'x' EXTRA"));
    }

    /// <summary>
    /// VARIABLES is matched as an identifier rather than reserved as a keyword, so schemas that already
    /// use the word keep parsing. Reserving it would have been a silent breaking change.
    /// </summary>
    [Test]
    public void TestVariablesRemainsAnOrdinaryIdentifier()
    {
        Assert.AreEqual(
            NodeType.CreateTable,
            SQLParserProcessor.Parse("CREATE TABLE variables (id oid PRIMARY KEY, variables string(64))").nodeType);

        Assert.AreEqual(NodeType.Select, SQLParserProcessor.Parse("SELECT variables FROM variables").nodeType);
        Assert.AreEqual(NodeType.Insert, SQLParserProcessor.Parse("INSERT INTO variables (variables) VALUES ('x')").nodeType);
    }

    /// <summary>
    /// SHOW VARIABLES and SHOW ENGINE STATS share a <c>SHOW &lt;identifier&gt;</c> prefix, so the
    /// one-token and two-token forms must stay distinguishable — including when a LIKE clause follows,
    /// which is where a grammar that resolved the prefix too early would break.
    /// </summary>
    [Test]
    public void TestShowVariablesAndEngineStatsDoNotShadowEachOther()
    {
        Assert.AreEqual(NodeType.ShowVariables, SQLParserProcessor.Parse("SHOW VARIABLES").nodeType);
        Assert.AreEqual(NodeType.ShowEngineStats, SQLParserProcessor.Parse("SHOW ENGINE STATS").nodeType);
        Assert.AreEqual(NodeType.ShowVariables, SQLParserProcessor.Parse("SHOW VARIABLES LIKE 'a%'").nodeType);
        Assert.AreEqual(NodeType.ShowEngineStats, SQLParserProcessor.Parse("SHOW ENGINE STATS LIKE 'a%'").nodeType);
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

    #region Pending query feature acceptance fixtures

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
        // ANALYZE is now a valid EXPLAIN option.
        NodeAst? ast = SQLParserProcessor.Parse("EXPLAIN (ANALYZE) SELECT * FROM users");
        Assert.That(ast, Is.Not.Null);
        Assert.That(ast!.nodeType, Is.EqualTo(NodeType.ExplainAnalyze));
    }

    #endregion

    // ── T5 data-type keywords ─────────────────────────────────────────────────
    #region T5 — new type keyword parsing

    // Walk a CreateTableItemList/CreateTableItem tree and collect (columnName → typeNode) pairs.
    private static Dictionary<string, NodeAst> CollectColumnTypes(NodeAst node)
    {
        var result = new Dictionary<string, NodeAst>(StringComparer.Ordinal);
        CollectColumnTypesInto(node, result);
        return result;
    }

    private static void CollectColumnTypesInto(NodeAst? node, Dictionary<string, NodeAst> result)
    {
        if (node is null)
            return;
        if (node.nodeType == NodeType.CreateTableItem)
        {
            if (node.leftAst?.yytext is string name && node.rightAst is NodeAst typeNode)
                result[name] = typeNode;
            return;
        }
        if (node.nodeType == NodeType.CreateTableItemList)
        {
            CollectColumnTypesInto(node.leftAst, result);
            CollectColumnTypesInto(node.rightAst, result);
        }
    }

    [Test]
    public void CreateTable_Float32_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (v float32)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeFloat32, cols["v"].nodeType);
    }

    [Test]
    public void CreateTable_Float32_AliasReal_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (v real)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeFloat32, cols["v"].nodeType);
    }

    [Test]
    public void CreateTable_Int_AliasForInt64_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (v int)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeInteger64, cols["v"].nodeType);
    }

    [Test]
    public void CreateTable_Int64_StillParsesAfterIntAlias()
    {
        // Longest-match must keep `int64` distinct from the new `int` alias,
        // and an int-prefixed identifier must remain an identifier.
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (a int64, internal int)");
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeInteger64, cols["a"].nodeType);
        Assert.AreEqual(NodeType.TypeInteger64, cols["internal"].nodeType);
    }

    [Test]
    public void CreateTable_Bytes_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (b bytes)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeBytes, cols["b"].nodeType);
    }

    [Test]
    public void CreateTable_Bytes_AliasBlob_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (b blob)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeBytes, cols["b"].nodeType);
    }

    [Test]
    public void CreateTable_Date_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (d date)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeDate, cols["d"].nodeType);
    }

    [Test]
    public void CreateTable_DateTime_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (dt datetime)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeDateTime, cols["dt"].nodeType);
    }

    [Test]
    public void CreateTable_DateTime_AliasTimestamp_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (ts timestamp)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeDateTime, cols["ts"].nodeType);
    }

    [Test]
    public void CreateTable_ArrayOfInt64_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (tags array(int64))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        NodeAst arrNode = cols["tags"];
        Assert.AreEqual(NodeType.TypeArray, arrNode.nodeType);
        Assert.AreEqual(NodeType.TypeInteger64, arrNode.leftAst!.nodeType);
    }

    [Test]
    public void CreateTable_StringSized_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (name string(32))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        NodeAst sizedNode = cols["name"];
        Assert.AreEqual(NodeType.TypeStringSized, sizedNode.nodeType);
        Assert.AreEqual("32", sizedNode.yytext);
    }

    [Test]
    public void CreateTable_AllNewTypes_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (a float32, b bytes, c date, d datetime, e array(int64), nm string(32))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);

        Assert.AreEqual(NodeType.TypeFloat32,    cols["a"].nodeType);
        Assert.AreEqual(NodeType.TypeBytes,      cols["b"].nodeType);
        Assert.AreEqual(NodeType.TypeDate,       cols["c"].nodeType);
        Assert.AreEqual(NodeType.TypeDateTime,   cols["d"].nodeType);

        Assert.AreEqual(NodeType.TypeArray,      cols["e"].nodeType);
        Assert.AreEqual(NodeType.TypeInteger64,  cols["e"].leftAst!.nodeType);

        Assert.AreEqual(NodeType.TypeStringSized, cols["nm"].nodeType);
        Assert.AreEqual("32",                    cols["nm"].yytext);
    }

    [Test]
    public void CreateTable_ArrayWithStringElement_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (tags array(string))");
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeArray,  cols["tags"].nodeType);
        Assert.AreEqual(NodeType.TypeString, cols["tags"].leftAst!.nodeType);
    }

    [Test]
    public void Cast_Float32_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(v AS float32) FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        NodeAst castNode = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprCast, castNode.nodeType);
        Assert.AreEqual(NodeType.TypeFloat32, castNode.rightAst!.nodeType);
    }

    [Test]
    public void Cast_Date_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(ts AS date) FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        NodeAst castNode = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprCast, castNode.nodeType);
        Assert.AreEqual(NodeType.TypeDate, castNode.rightAst!.nodeType);
    }

    [Test]
    public void Cast_DateTime_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(ts AS datetime) FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        NodeAst castNode = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprCast, castNode.nodeType);
        Assert.AreEqual(NodeType.TypeDateTime, castNode.rightAst!.nodeType);
    }

    [Test]
    public void Cast_Bytes_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(v AS bytes) FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        NodeAst castNode = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprCast, castNode.nodeType);
        Assert.AreEqual(NodeType.TypeBytes, castNode.rightAst!.nodeType);
    }

    [Test]
    public void CreateTable_BytesSized_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (embedding bytes(3072))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        NodeAst sizedNode = cols["embedding"];
        Assert.AreEqual(NodeType.TypeBytesSized, sizedNode.nodeType);
        Assert.AreEqual("3072", sizedNode.yytext);
    }

    [Test]
    public void CreateTable_BareBytesAndSizedBytes_AreDistinctNodes()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (payload bytes, embedding bytes(3072))");
        var cols = CollectColumnTypes(ast.rightAst!);

        Assert.AreEqual(NodeType.TypeBytes, cols["payload"].nodeType);
        Assert.IsNull(cols["payload"].yytext);

        Assert.AreEqual(NodeType.TypeBytesSized, cols["embedding"].nodeType);
        Assert.AreEqual("3072", cols["embedding"].yytext);
    }

    [Test]
    public void AlterTableAddColumn_BytesSized_CarriesSizeOnTheTypeNode()
    {
        NodeAst ast = SQLParserProcessor.Parse("ALTER TABLE t ADD COLUMN embedding bytes(3072)");
        Assert.AreEqual(NodeType.AlterTableAddColumn, ast.nodeType);
        Assert.AreEqual("embedding", ast.rightAst!.yytext);
        Assert.AreEqual(NodeType.TypeBytesSized, ast.extendedOne!.nodeType);
        Assert.AreEqual("3072", ast.extendedOne!.yytext);
    }

    [Test]
    public void Cast_BytesSized_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("SELECT CAST(v AS bytes(3072)) FROM t");
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        NodeAst castNode = ast.leftAst!;
        Assert.AreEqual(NodeType.ExprCast, castNode.nodeType);
        Assert.AreEqual(NodeType.TypeBytesSized, castNode.rightAst!.nodeType);
    }

    [Test]
    public void CreateTable_Char_AliasForString_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (name char)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeString, cols["name"].nodeType);
    }

    [Test]
    public void CreateTable_Varchar_AliasForString_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (name varchar)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeString, cols["name"].nodeType);
    }

    [Test]
    public void CreateTable_VarcharSized_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (name varchar(255))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeStringSized, cols["name"].nodeType);
        Assert.AreEqual("255", cols["name"].yytext);
    }

    [Test]
    public void CreateTable_CharSized_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (code char(10))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeStringSized, cols["code"].nodeType);
        Assert.AreEqual("10", cols["code"].yytext);
    }

    [Test]
    public void CreateTable_CharAndVarchar_MixedWithOtherTypes_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse(
            "CREATE TABLE t (id int64, name varchar(64), code char, tag char(8))");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeInteger64,   cols["id"].nodeType);
        Assert.AreEqual(NodeType.TypeStringSized, cols["name"].nodeType);
        Assert.AreEqual("64",                     cols["name"].yytext);
        Assert.AreEqual(NodeType.TypeString,      cols["code"].nodeType);
        Assert.AreEqual(NodeType.TypeStringSized, cols["tag"].nodeType);
        Assert.AreEqual("8",                      cols["tag"].yytext);
    }

    [Test]
    public void CreateTable_VarcharNotMistakenForChar_ParsesCorrectly()
    {
        // Longest-match must resolve `varchar` as a whole, not `char` + trailing text.
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (a varchar, b char)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeString, cols["a"].nodeType);
        Assert.AreEqual(NodeType.TypeString, cols["b"].nodeType);
    }

    [Test]
    public void CreateTable_Text_AliasForString_ParsesCorrectly()
    {
        NodeAst ast = SQLParserProcessor.Parse("CREATE TABLE t (body text)");
        Assert.AreEqual(NodeType.CreateTable, ast.nodeType);
        var cols = CollectColumnTypes(ast.rightAst!);
        Assert.AreEqual(NodeType.TypeString, cols["body"].nodeType);
    }

    #endregion
}
