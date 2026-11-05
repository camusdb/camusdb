/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end rules for the places where a PostgreSQL user could get a surprise: a character the
/// scanner does not know, the <c>^</c> operator, the logarithm functions, and ORDER BY on an aggregate
/// with no GROUP BY. Expected values were measured on PostgreSQL 16 and written here by hand, so this
/// fixture does not compare the engine with itself.
/// </summary>
[NonParallelizable]
public sealed class TestSqlCompatibilityRules : SharedNodeBaseTest
{
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, a INT64, b INT64, f FLOAT64)");
        await Dml(database, executor, dbname, "INSERT INTO t (id, a, b, f) VALUES (1, 2, 3, 2.5), (2, 20, 30, 4.0)");
        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Dml(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> Query(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static CamusDBException Refused(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await Query(database, executor, dbname, sql))!;

    private async Task<ColumnValue> Scalar(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        return (await Query(database, executor, dbname, sql))[0].Row.First().Value;
    }

    // ── A character no token starts with is a syntax error, never a character to skip ──

    [TestCase("SELECT a \\ - b FROM t", TestName = "Scanner_Backslash_DoesNotBecomeASubtraction")]
    [TestCase("SELECT 1 $ + 1", TestName = "Scanner_Dollar_DoesNotBecomeAnAddition")]
    [TestCase("SELECT $1", TestName = "Scanner_DollarDigit_IsNotTheLiteral")]
    [TestCase("SELECT id ! FROM t", TestName = "Scanner_LoneBang_IsRefused")]
    [TestCase("SELECT aé FROM t", TestName = "Scanner_NonAsciiLetter_DoesNotShortenTheName")]
    [TestCase("SELECT id FROM t", TestName = "Scanner_NoBreakSpace_IsNotWhitespace")]
    [TestCase("SELECT 1 \U0001F600", TestName = "Scanner_SupplementaryCharacter_IsRefused")]
    [TestCase("﻿SELECT 1", TestName = "Scanner_ByteOrderMark_IsRefused")]
    [TestCase("SELECT 'abc", TestName = "Scanner_UnclosedSingleQuote_IsRefused")]
    [TestCase("SELECT \"abc", TestName = "Scanner_UnclosedDoubleQuote_IsRefused")]
    [TestCase("SELECT `abc", TestName = "Scanner_UnclosedBacktick_IsRefused")]
    [TestCase("SELECT 'a\tb'", TestName = "Scanner_RawTabInString_IsRefused")]
    [TestCase("UPDATE t SET a = 5 $ WHERE id = 1", TestName = "Scanner_StrayCharacterInUpdate_IsRefused")]
    public void UnknownCharacter_IsASyntaxError(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [Test]
    public void UnknownCharacter_MessageNamesItAndItsPosition()
    {
        StringAssert.Contains("(line 2, col 3) Syntax error, unexpected character '$'",
            Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT\n  $"))!.Message);

        StringAssert.Contains("unexpected character U+00A0",
            Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT id FROM t"))!.Message);

        StringAssert.Contains("unexpected character U+1F600",
            Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT 1 \U0001F600"))!.Message);

        StringAssert.Contains("does not close",
            Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("SELECT 'abc"))!.Message);
    }

    [Test]
    public async Task StrayCharacterInUpdate_WritesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Assert.ThrowsAsync<CamusDBException>(() => Dml(database, executor, dbname, "UPDATE t SET a = 5 $ WHERE id = 1"));

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT a FROM t WHERE id = 1");
        Assert.AreEqual(2L, rows[0].Row["a"].LongValue);
    }

    [TestCase("SELECT\n1\r\n,\t2\f\v", TestName = "Scanner_EveryWhitespace_IsSkipped")]
    [TestCase("/* $ ^ é */ SELECT 1", TestName = "Scanner_AnyCharacterInABlockComment_IsSkipped")]
    [TestCase("-- $ é\nSELECT 1", TestName = "Scanner_AnyCharacterInALineComment_IsSkipped")]
    [TestCase("SELECT '$ é ^ ;'", TestName = "Scanner_AnyPrintableCharacterInAString_IsText")]
    [TestCase("SELECT 1;", TestName = "Semicolon_ClosesAStatement")]
    [TestCase("SELECT 1 ; ;\n", TestName = "Semicolon_CanRepeat")]
    public void ValidText_StillParses(string sql)
    {
        Assert.IsNotNull(SQLParserProcessor.Parse(sql));
    }

    [TestCase(";", TestName = "Semicolon_AloneIsNotAStatement")]
    [TestCase("SELECT 1; SELECT 2", TestName = "Semicolon_DoesNotAdmitASecondStatement")]
    [TestCase("SELECT;1", TestName = "Semicolon_CannotSitInsideAStatement")]
    public void SemicolonInTheWrongPlace_IsASyntaxError(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [Test]
    public async Task Semicolon_RunsEndToEnd()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE TABLE s (id INT64 PRIMARY KEY);");
        await Dml(database, executor, dbname, "INSERT INTO s (id) VALUES (1);");

        Assert.AreEqual(1, (await Query(database, executor, dbname, "SELECT id FROM s;")).Count);
    }

    // ── a ^ b is pow(a, b), with the PostgreSQL precedence ──

    [TestCase("SELECT 2 ^ 3", 8.0, TestName = "Power_Value")]
    [TestCase("SELECT 2 ^ 3 ^ 2", 64.0, TestName = "Power_GroupsToTheLeft")]
    [TestCase("SELECT -2 ^ 2", 4.0, TestName = "Power_NegativeLiteralIsTheBase")]
    [TestCase("SELECT - 2 ^ 2", 4.0, TestName = "Power_UnaryMinusBindsTighter")]
    [TestCase("SELECT -1 ^ 0", 1.0, TestName = "Power_UnaryMinusOfOneToTheZero")]
    [TestCase("SELECT 2 ^ -1", 0.5, TestName = "Power_NegativeExponent")]
    [TestCase("SELECT 2 * 3 ^ 2", 18.0, TestName = "Power_BindsTighterThanMultiply")]
    [TestCase("SELECT 2 ^ 3 * 2", 16.0, TestName = "Power_BindsTighterThanMultiply_OnTheLeft")]
    [TestCase("SELECT 1 + 2 ^ 3", 9.0, TestName = "Power_BindsTighterThanAdd")]
    [TestCase("SELECT 7 % 2 ^ 2", 3.0, TestName = "Power_BindsTighterThanModulo")]
    public async Task Power_HasTheValueOfPostgreSql(string sql, double expected)
    {
        ColumnValue value = await Scalar(sql);
        Assert.AreEqual(ColumnType.Float64, value.Type);
        Assert.AreEqual(expected, value.FloatValue);
    }

    [TestCase("SELECT 0 ^ -1", CamusDBErrorCodes.InvalidInput, TestName = "Power_ZeroToANegativePower_IsRefused")]
    [TestCase("SELECT (-8) ^ 0.5", CamusDBErrorCodes.InvalidInput, TestName = "Power_ComplexResult_IsRefused")]
    [TestCase("SELECT 10 ^ 400", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Power_Overflow_IsRefused")]
    [TestCase("SELECT 10 ^ -400", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Power_Underflow_IsRefused")]
    [TestCase("SELECT 2 ^ 'x'", CamusDBErrorCodes.InvalidInput, TestName = "Power_StringOperand_IsRefused")]
    [TestCase("SELECT TRUE ^ 2", CamusDBErrorCodes.InvalidInput, TestName = "Power_BoolOperand_IsRefused")]
    public async Task Power_ErrorsMatchThePowFunction(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        Assert.AreEqual(code, Refused(database, executor, dbname, sql).Code);
    }

    [Test]
    public async Task Power_SpecialValuesFollowIeee()
    {
        Assert.AreEqual(1.0, (await Scalar("SELECT CAST('NaN' AS float64) ^ 0")).FloatValue);
        Assert.IsTrue(double.IsPositiveInfinity((await Scalar("SELECT 2 ^ CAST('Infinity' AS float64)")).FloatValue));
    }

    [Test]
    public async Task Power_NullOperandGivesNull()
    {
        Assert.AreEqual(ColumnType.Null, (await Scalar("SELECT NULL ^ 2")).Type);
        Assert.AreEqual(ColumnType.Null, (await Scalar("SELECT 2 ^ NULL")).Type);
    }

    [Test]
    public async Task Power_WorksOnColumnsAndInEveryExpressionPosition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT a ^ b AS x, -a ^ 2 AS y, f ^ 2 AS z FROM t WHERE id = 1");
        Assert.AreEqual(8.0, rows[0].Row["x"].FloatValue);
        Assert.AreEqual(4.0, rows[0].Row["y"].FloatValue);
        Assert.AreEqual(6.25, rows[0].Row["z"].FloatValue);

        rows = await Query(database, executor, dbname, "SELECT id FROM t WHERE a ^ 2 > 100");
        Assert.AreEqual(2L, rows.Single().Row["id"].LongValue);

        rows = await Query(database, executor, dbname, "SELECT id FROM t WHERE a BETWEEN 2 ^ 1 AND 2 ^ 2");
        Assert.AreEqual(1L, rows.Single().Row["id"].LongValue);

        rows = await Query(database, executor, dbname, "SELECT id FROM t ORDER BY a ^ 2 DESC");
        Assert.AreEqual(2L, rows[0].Row["id"].LongValue);

        await Dml(database, executor, dbname, "UPDATE t SET f = f ^ 2 WHERE id = 1");
        rows = await Query(database, executor, dbname, "SELECT f FROM t WHERE id = 1");
        Assert.AreEqual(6.25, rows[0].Row["f"].FloatValue);

        rows = await Query(database, executor, dbname, "SELECT @p ^ 2 AS x", new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 3L) });
        Assert.AreEqual(9.0, rows[0].Row["x"].FloatValue);
    }

    [Test]
    public async Task Power_AssignedToAnIntegerColumn_IsNotARoundingPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        Assert.ThrowsAsync<CamusDBException>(() => Dml(database, executor, dbname, "UPDATE t SET a = 2 ^ 3 WHERE id = 1"));

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT a FROM t WHERE id = 1");
        Assert.AreEqual(2L, rows[0].Row["a"].LongValue);
    }

    [Test]
    public async Task Power_SurvivesAViewBodyRoundTrip()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        await Ddl(database, executor, dbname, "CREATE VIEW squares AS SELECT id, 2 * a ^ 2 AS x FROM t");

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT x FROM squares WHERE id = 1");
        Assert.AreEqual(8.0, rows[0].Row["x"].FloatValue);
    }

    [Test]
    public async Task Power_LongChain_HitsTheNestingLimitNotTheStack()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        StringBuilder chain = new("SELECT 1");
        for (int i = 0; i < 3_000; i++)
            chain.Append(" ^ 1");

        Assert.AreEqual(CamusDBErrorCodes.StatementTooDeeplyNested, Refused(database, executor, dbname, chain.ToString()).Code);
    }

    [Test]
    public async Task CaretInsideAStringOrARegex_IsText()
    {
        Assert.AreEqual("a^b", (await Scalar("SELECT 'a^b'")).StrValue);
        Assert.IsTrue((await Scalar("SELECT 'abc' ~ '^a'")).BoolValue);
    }

    // ── ln, log, log10, exp ──

    [TestCase("SELECT ln(1)", 0.0, TestName = "Ln_OfOne")]
    [TestCase("SELECT ln(2)", 0.6931471805599453, TestName = "Ln_OfTwo")]
    [TestCase("SELECT LN(2.5)", 0.9162907318741551, TestName = "Ln_IsCaseInsensitive")]
    [TestCase("SELECT log(1000)", 3.0, TestName = "Log_OneArgumentIsBaseTen")]
    [TestCase("SELECT log10(1000)", 3.0, TestName = "Log10_ExactPower")]
    [TestCase("SELECT log(2, 8)", 3.0, TestName = "Log_BaseComesFirst")]
    [TestCase("SELECT log(10, 1000)", 3.0, TestName = "Log_BaseTenIsExact")]
    [TestCase("SELECT log(3, 81)", 4.0, TestName = "Log_OtherBase")]
    [TestCase("SELECT exp(0)", 1.0, TestName = "Exp_OfZero")]
    [TestCase("SELECT exp(1)", 2.718281828459045, TestName = "Exp_OfOne")]
    public async Task Logarithm_HasTheValueOfPostgreSql(string sql, double expected)
    {
        ColumnValue value = await Scalar(sql);
        Assert.AreEqual(ColumnType.Float64, value.Type);
        Assert.AreEqual(expected, value.FloatValue);
    }

    [TestCase("SELECT ln(0)", CamusDBErrorCodes.InvalidInput, TestName = "Ln_OfZero_IsRefused")]
    [TestCase("SELECT ln(-1)", CamusDBErrorCodes.InvalidInput, TestName = "Ln_OfNegative_IsRefused")]
    [TestCase("SELECT log10(0)", CamusDBErrorCodes.InvalidInput, TestName = "Log10_OfZero_IsRefused")]
    [TestCase("SELECT log(-1)", CamusDBErrorCodes.InvalidInput, TestName = "Log_OfNegative_IsRefused")]
    [TestCase("SELECT log(1, 5)", CamusDBErrorCodes.InvalidInput, TestName = "Log_BaseOne_IsADivisionByZero")]
    [TestCase("SELECT log(0, 5)", CamusDBErrorCodes.InvalidInput, TestName = "Log_BaseZero_IsRefused")]
    [TestCase("SELECT log(-2, 8)", CamusDBErrorCodes.InvalidInput, TestName = "Log_NegativeBase_IsRefused")]
    [TestCase("SELECT log(2, 0)", CamusDBErrorCodes.InvalidInput, TestName = "Log_ValueZero_IsRefused")]
    [TestCase("SELECT ln('x')", CamusDBErrorCodes.InvalidInput, TestName = "Ln_OfAString_IsRefused")]
    [TestCase("SELECT log()", CamusDBErrorCodes.InvalidInput, TestName = "Log_NoArgument_IsRefused")]
    [TestCase("SELECT log(1, 2, 3)", CamusDBErrorCodes.InvalidInput, TestName = "Log_ThreeArguments_IsRefused")]
    [TestCase("SELECT exp(1000)", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Exp_Overflow_IsRefused")]
    [TestCase("SELECT exp(-1000)", CamusDBErrorCodes.FloatValueOutOfRange, TestName = "Exp_Underflow_IsRefused")]
    public async Task Logarithm_ErrorsMatchPostgreSql(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        Assert.AreEqual(code, Refused(database, executor, dbname, sql).Code);
    }

    [Test]
    public async Task Logarithm_SpecialValuesFollowIeee()
    {
        Assert.IsTrue(double.IsPositiveInfinity((await Scalar("SELECT ln(CAST('Infinity' AS float64))")).FloatValue));
        Assert.IsTrue(double.IsNaN((await Scalar("SELECT ln(CAST('NaN' AS float64))")).FloatValue));
        Assert.AreEqual(0.0, (await Scalar("SELECT exp(CAST('-Infinity' AS float64))")).FloatValue);
        Assert.IsTrue(double.IsPositiveInfinity((await Scalar("SELECT exp(CAST('Infinity' AS float64))")).FloatValue));
    }

    [TestCase("SELECT ln(NULL)", TestName = "Ln_OfNull_IsNull")]
    [TestCase("SELECT log10(NULL)", TestName = "Log10_OfNull_IsNull")]
    [TestCase("SELECT log(NULL)", TestName = "Log_OfNull_IsNull")]
    [TestCase("SELECT log(NULL, 8)", TestName = "Log_NullBase_IsNull")]
    [TestCase("SELECT log(2, NULL)", TestName = "Log_NullValue_IsNull")]
    [TestCase("SELECT exp(NULL)", TestName = "Exp_OfNull_IsNull")]
    public async Task Logarithm_NullArgumentGivesNull(string sql)
    {
        Assert.AreEqual(ColumnType.Null, (await Scalar(sql)).Type);
    }

    [Test]
    public async Task Logarithm_ReadsColumnsAndEveryNumericType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT ln(a) AS x, log10(f) AS y, exp(0 * a) AS z FROM t WHERE id = 1");
        Assert.AreEqual(0.6931471805599453, rows[0].Row["x"].FloatValue);
        Assert.AreEqual(0.3979400086720376, rows[0].Row["y"].FloatValue);
        Assert.AreEqual(1.0, rows[0].Row["z"].FloatValue);

        Assert.AreEqual(0.9162907318741551, (await Scalar("SELECT ln(CAST('2.5' AS numeric))")).FloatValue);
    }

    // ── ORDER BY of an aggregate with no GROUP BY ──

    [TestCase("SELECT COUNT(*) FROM t ORDER BY a", TestName = "AggregateOrder_BareColumn_IsRefused")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY a LIMIT 0", TestName = "AggregateOrder_BareColumn_IsRefusedUnderLimitZero")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY a + 1", TestName = "AggregateOrder_ColumnExpression_IsRefused")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY COUNT(*) + a", TestName = "AggregateOrder_ColumnBesideAnAggregate_IsRefused")]
    [TestCase("SELECT SUM(a) + 1 AS s FROM t ORDER BY b", TestName = "AggregateOrder_CompoundAggregate_BareColumn_IsRefused")]
    [TestCase("SELECT COUNT(*) FROM t JOIN t AS u ON t.id = u.id ORDER BY t.a", TestName = "AggregateOrder_ColumnOfAJoin_IsRefused")]
    public async Task AggregateOrder_ColumnOutsideAnAggregate_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException ex = Refused(database, executor, dbname, sql);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("must appear in the GROUP BY clause or be used in an aggregate function", ex.Message);
    }

    [Test]
    public async Task AggregateOrder_BareColumn_IsRefusedOnAnEmptyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE e (id INT64 PRIMARY KEY, a INT64)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Refused(database, executor, dbname, "SELECT COUNT(*) FROM e ORDER BY a").Code);
    }

    [TestCase("SELECT COUNT(*) AS c FROM t ORDER BY c", TestName = "AggregateOrder_Alias_IsAccepted")]
    [TestCase("SELECT COUNT(*) AS a FROM t ORDER BY a", TestName = "AggregateOrder_AliasOutranksTheColumn")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY COUNT(*)", TestName = "AggregateOrder_SameAggregate_IsAccepted")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY SUM(a)", TestName = "AggregateOrder_OtherAggregate_IsAccepted")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY 1", TestName = "AggregateOrder_Position_IsAccepted")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY COUNT(*) DESC, SUM(a) ASC", TestName = "AggregateOrder_TwoKeys_AreAccepted")]
    public async Task AggregateOrder_ValidKey_ReturnsTheOneRow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname, sql);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2L, rows[0].Row.First().Value.LongValue);
    }

    [Test]
    public async Task AggregateOrder_CompoundAlias_ReturnsItsValue()
    {
        Assert.AreEqual(3L, (await Scalar("SELECT COUNT(*) + 1 AS x FROM t ORDER BY x")).LongValue);
    }

    [Test]
    public async Task AggregateOrder_LimitZero_ReturnsNoRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        Assert.AreEqual(0, (await Query(database, executor, dbname, "SELECT COUNT(*) AS c FROM t ORDER BY c LIMIT 0")).Count);
    }

    [TestCase("SELECT COUNT(*) FROM t ORDER BY nope", CamusDBErrorCodes.UnknownColumn, TestName = "AggregateOrder_UnknownColumn_IsReportedFirst")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY SUM(nope)", CamusDBErrorCodes.UnknownColumn, TestName = "AggregateOrder_UnknownColumnInAnAggregate_IsReported")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY 2", CamusDBErrorCodes.InvalidInput, TestName = "AggregateOrder_PositionOutOfRange_IsRefused")]
    [TestCase("SELECT COUNT(*) FROM t ORDER BY (SELECT 1)", CamusDBErrorCodes.InvalidInput, TestName = "AggregateOrder_Subquery_IsRefused")]
    [TestCase("SELECT SUM(a) FROM t ORDER BY SUM(SUM(a))", CamusDBErrorCodes.InvalidInput, TestName = "AggregateOrder_NestedAggregate_IsRefused")]
    public async Task AggregateOrder_InvalidKey_IsRefused(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        Assert.AreEqual(code, Refused(database, executor, dbname, sql).Code);
    }

    [Test]
    public async Task AggregateOrder_PlanHasNoSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        List<QueryResultRow> rows = await Query(database, executor, dbname, "EXPLAIN SELECT COUNT(*) AS c FROM t ORDER BY c");

        Assert.IsEmpty(rows.Where(row => row.Row["node"].StrValue == "sort"));
    }

    [Test]
    public async Task OrderByKeepsItsRules_ForTheOtherShapes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // A window function is not an aggregate of the whole query: it keeps one row per input row.
        List<QueryResultRow> window = await Query(database, executor, dbname, "SELECT a, COUNT(*) OVER () AS c FROM t ORDER BY a DESC");
        CollectionAssert.AreEqual(new[] { 20L, 2L }, window.Select(row => row.Row["a"].LongValue));

        // A grouped query orders its groups.
        List<QueryResultRow> grouped = await Query(database, executor, dbname, "SELECT a, COUNT(*) AS c FROM t GROUP BY a ORDER BY a DESC");
        CollectionAssert.AreEqual(new[] { 20L, 2L }, grouped.Select(row => row.Row["a"].LongValue));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, Refused(database, executor, dbname, "SELECT a FROM t ORDER BY COUNT(*)").Code);
    }

    // ── One string order through every plan ──

    private static readonly string[] StringValues =
    [
        "", " x", "A", "B", "Z", "_aa", "a", "aa", "b", "c", "z ", "z", "~",
        "é", "퟿", "\U0001F600", "", "ａ", "�", "a", "B",
    ];

    /// <summary>
    /// The rows as (value, id), ordered by .NET's ordinal comparison. It is the reference for every plan
    /// below, so the expected order does not come from the engine. A duplicate value breaks its tie by id.
    /// </summary>
    private static List<long> ExpectedIdsByValue(bool descending, Func<string, bool>? filter = null)
    {
        IEnumerable<(string Value, long Id)> rows = StringValues.Select((value, index) => (value, (long)index + 1));

        if (filter is not null)
            rows = rows.Where(row => filter(row.Value));

        List<(string Value, long Id)> sorted = rows.ToList();
        sorted.Sort((x, y) =>
        {
            int order = string.CompareOrdinal(x.Value, y.Value);
            if (order != 0)
                return descending ? -order : order;

            return x.Id.CompareTo(y.Id);
        });

        return sorted.Select(row => row.Id).ToList();
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SeedStrings()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE TABLE plain (id INT64 PRIMARY KEY, v STRING)");
        await Ddl(database, executor, dbname, "CREATE TABLE indexed (id INT64 PRIMARY KEY, v STRING)");
        await Ddl(database, executor, dbname, "CREATE INDEX by_v ON indexed (v)");

        for (int i = 0; i < StringValues.Length; i++)
        {
            await Dml(database, executor, dbname, $"INSERT INTO plain (id, v) VALUES ({i + 1}, '{StringValues[i]}')");
            await Dml(database, executor, dbname, $"INSERT INTO indexed (id, v) VALUES ({i + 1}, '{StringValues[i]}')");
        }

        return (dbname, database, executor);
    }

    private static async Task<List<long>> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        (await Query(database, executor, dbname, sql)).Select(row => row.Row["id"].LongValue).ToList();

    [Test]
    public async Task StringOrder_IsOneOrderThroughEveryPlan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedStrings();

        foreach (string source in new[] { "plain", "indexed", "indexed@{FORCE_INDEX=by_v}" })
        {
            List<long> ascending = ExpectedIdsByValue(descending: false);
            List<long> descending = ExpectedIdsByValue(descending: true);

            CollectionAssert.AreEqual(ascending, await Ids(database, executor, dbname, $"SELECT id FROM {source} ORDER BY v, id"), source);
            CollectionAssert.AreEqual(ascending.Take(5), await Ids(database, executor, dbname, $"SELECT id FROM {source} ORDER BY v, id LIMIT 5"), source);
            CollectionAssert.AreEqual(ascending.Skip(3).Take(5), await Ids(database, executor, dbname, $"SELECT id FROM {source} ORDER BY v, id LIMIT 5 OFFSET 3"), source);
            CollectionAssert.AreEqual(descending, await Ids(database, executor, dbname, $"SELECT id FROM {source} ORDER BY v DESC, id"), source);
            CollectionAssert.AreEqual(descending.Take(5), await Ids(database, executor, dbname, $"SELECT id FROM {source} ORDER BY v DESC, id LIMIT 5"), source);
        }
    }

    [Test]
    public async Task StringOrder_RangePredicateAgreesWithTheSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedStrings();

        foreach (string source in new[] { "plain", "indexed@{FORCE_INDEX=by_v}" })
        {
            foreach (string bound in new[] { "a", "B", "_", "", "\U0001F600", "�" })
            {
                List<long> below = ExpectedIdsByValue(false, value => string.CompareOrdinal(value, bound) < 0);
                List<long> above = ExpectedIdsByValue(false, value => string.CompareOrdinal(value, bound) >= 0);

                CollectionAssert.AreEqual(below, await Ids(database, executor, dbname, $"SELECT id FROM {source} WHERE v < '{bound}' ORDER BY v, id"), $"{source} < {bound}");
                CollectionAssert.AreEqual(above, await Ids(database, executor, dbname, $"SELECT id FROM {source} WHERE v >= '{bound}' ORDER BY v, id"), $"{source} >= {bound}");
            }
        }
    }

    [Test]
    public async Task StringOrder_MinAndMaxAgreeWithTheSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedStrings();

        foreach (string table in new[] { "plain", "indexed" })
        {
            List<QueryResultRow> rows = await Query(database, executor, dbname, $"SELECT MIN(v) AS lo, MAX(v) AS hi FROM {table}");
            Assert.AreEqual("", rows[0].Row["lo"].StrValue);
            Assert.AreEqual("�", rows[0].Row["hi"].StrValue);
        }
    }

    [TestCase("SELECT 'a' COLLATE \"C\"", TestName = "Collate_OnALiteral_IsRefused")]
    [TestCase("SELECT id FROM t ORDER BY a COLLATE \"C\"", TestName = "Collate_InOrderBy_IsRefused")]
    [TestCase("CREATE TABLE c1 (id INT64 PRIMARY KEY, v STRING COLLATE \"C\")", TestName = "Collate_OnAColumn_IsRefused")]
    [TestCase("CREATE INDEX c2 ON t (a COLLATE \"C\")", TestName = "Collate_OnAnIndexKey_IsRefused")]
    public void Collate_IsNeverAcceptedAndIgnored(string sql)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse(sql))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [Test]
    public async Task PrimaryKeyUpdate_MessageNamesTheColumnAndTheWayOut()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Dml(database, executor, dbname, "UPDATE t SET id = 5 WHERE id = 2"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("Cannot update primary key column 'id'", ex.Message);
        StringAssert.Contains("delete the row", ex.Message);
    }
}
