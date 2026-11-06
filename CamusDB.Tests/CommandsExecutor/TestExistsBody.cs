/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// EXISTS asks only whether a row exists, so a clause that cannot change the answer must not fail the
/// statement and must not run. Each expected result was measured on PostgreSQL 16.11 with the same tables.
/// <c>r</c> has one row with <c>v = 0</c>, <c>em</c> has no rows, and <c>r2</c> has one row. A row of the form
/// <c>SELECT id FROM r ...</c> expects <c>"1"</c> for the row and <c>""</c> for no rows. An expected
/// value that starts with <c>CADB</c> is an error code.
///
/// <para>The exceptions are the refusals of a correlated body that must run, which are on
/// <c>DecidedDivergences</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestExistsBody : BaseTest
{
    private const string Invalid = CamusDBErrorCodes.InvalidInput;

    private const string UnknownColumn = CamusDBErrorCodes.UnknownColumn;

    private const string NotSupported = CamusDBErrorCodes.FeatureNotSupported;

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY)");
        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (1, 0, 'x')");
        await Commit(database, executor, dbname, "INSERT INTO r2 (id) VALUES (1)");

        return (dbname, database, executor);
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.Bool => value.BoolValue.ToString(),
        ColumnType.Null => "NULL",
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task<string> Run(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return string.Join(";", rows.Select(row => string.Join("|", row.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private async Task AssertResult(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    private static Dictionary<string, ColumnValue> Limit(long value) => new() { ["@p"] = new ColumnValue(ColumnType.Integer64, value) };

    // ── The output is discarded, so a value error in it does not fail ────────────────────────────

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r)", "1", TestName = "ConstantDivision_WithRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r)", "1", TestName = "ColumnDivision_WithRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT id, id / 0 FROM r)", "1", TestName = "SecondOutputColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT *, 1 / 0 FROM r)", "1", TestName = "StarAndADivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT * FROM r)", "1", TestName = "Star")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1, 2 FROM r)", "1", TestName = "TwoColumns")]
    [TestCase("SELECT id FROM r WHERE NOT EXISTS (SELECT 1 / 0 FROM r)", "", TestName = "NotExists_WithRows")]
    [TestCase("SELECT id FROM r WHERE NOT EXISTS (SELECT 1 / 0 FROM em)", "1", TestName = "NotExists_NoRows")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 / 0 FROM em)", "", TestName = "ConstantDivision_NoOuterRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em)", "", TestName = "ConstantDivision_NoInnerRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT sqrt(-1) FROM r)", "1", TestName = "DomainError")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT CAST(s AS BIGINT) FROM r)", "1", TestName = "CastError")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 'a' || 1 / 0 FROM r)", "1", TestName = "DivisionInAConcat")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT CASE WHEN v = 0 THEN 1 / v END FROM r)", "1", TestName = "DivisionInACase")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r WHERE id = 1)", "1", TestName = "WithAWhereThatPasses")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r WHERE v = 99)", "", TestName = "WithAWhereThatFails")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r) AND id = 99", "", TestName = "BesideAnotherCondition")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT q / 0 FROM (SELECT v AS q FROM r) d)", "1", TestName = "OverADerivedTable")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT r.v / 0 FROM r JOIN r2 ON r.id = r2.id)", "1", TestName = "OverAJoin")]
    public Task Discarded_Output(string sql, string expected) => AssertResult(sql, expected);

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r LIMIT 1)", "1", TestName = "Limit_ConstantDivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r LIMIT 1)", "1", TestName = "Limit_ColumnDivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r ORDER BY v)", "1", TestName = "OrderBy_AColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v FROM r ORDER BY v / 0)", "1", TestName = "OrderBy_ADivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 AS q FROM r ORDER BY q)", "1", TestName = "OrderBy_AnAlias")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 AS q FROM r ORDER BY q)", "1", TestName = "OrderBy_AnAliasOfAConstant")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 AS q FROM r ORDER BY 1)", "1", TestName = "OrderBy_APosition")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 AS q FROM r ORDER BY q LIMIT 1)", "1", TestName = "OrderBy_AndLimit")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT DISTINCT v / 0 FROM r)", "1", TestName = "Distinct_AColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT DISTINCT 1 / 0 FROM r)", "1", TestName = "Distinct_AConstant")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r GROUP BY v)", "1", TestName = "GroupBy_AGroupedColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r GROUP BY v)", "1", TestName = "GroupBy_AConstant")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 / 0 FROM em GROUP BY v)", "", TestName = "GroupBy_NoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em GROUP BY v)", "", TestName = "GroupBy_NoInnerRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM em GROUP BY v)", "", TestName = "GroupBy_AColumnWithNoInnerRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v + 1 FROM r GROUP BY v)", "1", TestName = "GroupBy_AnExpressionOverAGroupedColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v + 1 FROM r GROUP BY v + 1)", "1", TestName = "GroupBy_TheSameExpression")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v FROM r GROUP BY r.v)", "1", TestName = "GroupBy_AQualifiedKey")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT r.v / 0 FROM r GROUP BY v)", "1", TestName = "GroupBy_AQualifiedColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r GROUP BY 1)", "1", TestName = "GroupBy_APosition")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v FROM r GROUP BY 1)", "1", TestName = "GroupBy_APositionOfAColumn")]
    public Task Discarded_Clauses(string sql, string expected) => AssertResult(sql, expected);

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT (SELECT v / 0 FROM r) FROM r)", "1", TestName = "Subquery_AColumnDivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT (SELECT 1 / 0 FROM r2) FROM r)", "1", TestName = "Subquery_AConstantDivision")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT (SELECT r2.id FROM r2) FROM r)", "1", TestName = "Subquery_AValid")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT EXISTS (SELECT 1 / 0 FROM r2) FROM r)", "1", TestName = "Subquery_AnExists")]
    public Task Discarded_Subquery(string sql, string expected) => AssertResult(sql, expected);

    // ── Every position of an EXISTS ──────────────────────────────────────────────────────────────

    [TestCase("SELECT EXISTS (SELECT 1 / 0 FROM r)", "True", TestName = "Position_NoFrom")]
    [TestCase("SELECT EXISTS (SELECT 1 / 0 FROM em)", "False", TestName = "Position_NoFrom_NoRows")]
    [TestCase("SELECT CASE WHEN EXISTS (SELECT 1 / 0 FROM r) THEN 1 ELSE 2 END", "1", TestName = "Position_Case")]
    [TestCase("SELECT id, EXISTS (SELECT v / 0 FROM r) FROM r", "1|True", TestName = "Position_SelectList")]
    [TestCase("SELECT v, COUNT(*) FROM r GROUP BY v HAVING EXISTS (SELECT 1 / 0 FROM r)", "0|1", TestName = "Position_Having")]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r))", "1", TestName = "Position_InsideAnIn")]
    [TestCase("SELECT id FROM (SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r)) d", "1", TestName = "Position_InsideADerivedTable")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r))", "1", TestName = "Position_InsideAnExists")]
    public Task EveryPosition(string sql, string expected) => AssertResult(sql, expected);

    [Test]
    public async Task Writes_DoNotEvaluateTheOutput()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "UPDATE r SET v = 5 WHERE EXISTS (SELECT 1 / 0 FROM r)");
        Assert.That(await Run(database, executor, dbname, "SELECT v FROM r"), Is.EqualTo("5"));

        await Commit(database, executor, dbname, "INSERT INTO r2 (id) SELECT id + 100 FROM r WHERE EXISTS (SELECT v / 0 FROM r)");
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r2 ORDER BY id"), Is.EqualTo("1;101"));

        await Commit(database, executor, dbname, "DELETE FROM r2 WHERE EXISTS (SELECT 1 / 0 FROM em)");
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r2 ORDER BY id"), Is.EqualTo("1;101"));

        await Commit(database, executor, dbname, "DELETE FROM r2 WHERE EXISTS (SELECT v / 0 FROM r)");
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r2"), Is.EqualTo(""));
    }

    [Test]
    public async Task View_DoesNotEvaluateTheOutput()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW w AS SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r)");

        Assert.That(await Run(database, executor, dbname, "SELECT id FROM w"), Is.EqualTo("1"));
    }

    // ── A sequence in the output does not draw a value ───────────────────────────────────────────

    [Test]
    public async Task Sequence_DoesNotDrawAValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE SEQUENCE sq");

        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r WHERE EXISTS (SELECT nextval('sq') FROM r)"), Is.EqualTo("1"));
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r o WHERE EXISTS (SELECT nextval('sq') FROM r i WHERE i.id = o.id)"), Is.EqualTo("1"));
        Assert.That(await Run(database, executor, dbname, "SELECT nextval('sq')"), Is.EqualTo("1"));
    }

    // ── A body that must run still runs ──────────────────────────────────────────────────────────

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT COUNT(*) FROM r)", "1", TestName = "MustRun_Count")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT MAX(v) FROM em)", "1", TestName = "MustRun_AnAggregateGivesARowFromNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT COUNT(*) / 0 FROM r)", Invalid, TestName = "MustRun_AnAggregateIsEvaluated")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT COUNT(*) / 0 FROM em)", Invalid, TestName = "MustRun_AnAggregateIsEvaluatedWithNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0, COUNT(*) FROM r)", Invalid, TestName = "MustRun_BesideAnAggregate")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT COUNT(*) FROM r HAVING 1 / 0 = 1)", Invalid, TestName = "MustRun_Having")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r OFFSET 1)", Invalid, TestName = "MustRun_Offset")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r OFFSET 0)", Invalid, TestName = "MustRun_OffsetZero")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM em OFFSET 1)", "", TestName = "MustRun_OffsetNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em OFFSET 1)", Invalid, TestName = "MustRun_OffsetKeepsTheConstantCheck")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em OFFSET 0)", Invalid, TestName = "MustRun_OffsetZeroKeepsTheConstantCheck")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r LIMIT 0)", "", TestName = "MustRun_LimitZero")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM em LIMIT 0)", "", TestName = "MustRun_LimitZeroNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r LIMIT 0)", Invalid, TestName = "MustRun_LimitZeroKeepsTheConstantCheck")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em LIMIT 0)", Invalid, TestName = "MustRun_LimitZeroKeepsTheConstantCheckNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r WHERE 1 / 0 = 1)", Invalid, TestName = "MustRun_WhereConstant")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r WHERE v / 0 = 1)", Invalid, TestName = "MustRun_WhereColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r UNION SELECT 1 FROM r)", Invalid, TestName = "MustRun_Union")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM em UNION SELECT 1 FROM em)", Invalid, TestName = "MustRun_UnionKeepsTheConstantCheck")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r UNION ALL SELECT 1 FROM r LIMIT 0)", "", TestName = "MustRun_UnionLimitZero")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r UNION ALL SELECT 1 FROM r LIMIT 1)", Invalid, TestName = "MustRun_UnionLimitOne")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r UNION ALL SELECT v / 0 FROM r)", "1", TestName = "MustRun_UnionAllStopsAtTheFirstRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r UNION ALL SELECT 1 FROM r)", Invalid, TestName = "MustRun_UnionAllFirstOperand")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r WHERE v = 99 UNION ALL SELECT v / 0 FROM r)", Invalid, TestName = "MustRun_UnionAllSecondOperand")]
    public Task MustRun(string sql, string expected) => AssertResult(sql, expected);

    [Test]
    public async Task Limit_FromAParameter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        const string Uncorrelated = "SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r LIMIT @p)";
        const string Correlated = "SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id LIMIT @p)";

        // One text with other values: the parse cache shares the tree, so each run classifies again.
        Assert.That(await Run(database, executor, dbname, Uncorrelated, Limit(1)), Is.EqualTo("1"));
        Assert.That(await Run(database, executor, dbname, Uncorrelated, Limit(0)), Is.EqualTo(""));
        Assert.That(await Run(database, executor, dbname, Uncorrelated, Limit(5)), Is.EqualTo("1"));

        Assert.That(await Run(database, executor, dbname, Correlated, Limit(1)), Is.EqualTo("1"));
        Assert.That(await Run(database, executor, dbname, Correlated, Limit(0)), Is.EqualTo(""));
        Assert.That(await Run(database, executor, dbname, Correlated, Limit(5)), Is.EqualTo("1"));
    }

    [Test]
    public async Task Limit_ANegativeParameter_IsRefusedWhenCorrelated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(
            await Run(database, executor, dbname, "SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id LIMIT @p)", Limit(-1)),
            Is.EqualTo(NotSupported));
    }

    // ── A statement mistake in a discarded clause still fails, on any rows ───────────────────────

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT nosuchcol FROM r)", UnknownColumn, TestName = "Mistake_UnknownColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT nosuchcol FROM em)", UnknownColumn, TestName = "Mistake_UnknownColumnNoRows")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT nosuchfn(1) FROM r)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Mistake_UnknownFunction")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 + 'x' FROM r)", Invalid, TestName = "Mistake_LiteralType")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT s + 1 FROM r)", Invalid, TestName = "Mistake_ColumnType")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT CASE WHEN v = 0 THEN 1 ELSE 'a' END FROM r)", Invalid, TestName = "Mistake_CaseType")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r ORDER BY nosuchcol)", UnknownColumn, TestName = "Mistake_OrderByUnknownColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v FROM r ORDER BY 2)", Invalid, TestName = "Mistake_OrderByPosition")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT s + 1 FROM r ORDER BY s + 1)", Invalid, TestName = "Mistake_OrderByType")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT DISTINCT v FROM r ORDER BY s)", Invalid, TestName = "Mistake_DistinctOrderBy")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r GROUP BY nosuchcol)", UnknownColumn, TestName = "Mistake_GroupByUnknownColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r GROUP BY COUNT(*))", Invalid, TestName = "Mistake_GroupByAggregate")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v FROM r GROUP BY s)", Invalid, TestName = "Mistake_UngroupedColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v, s FROM r GROUP BY v)", Invalid, TestName = "Mistake_OneUngroupedColumn")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v + 2 FROM r GROUP BY v + 1)", Invalid, TestName = "Mistake_AnotherExpression")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT * FROM r GROUP BY v)", Invalid, TestName = "Mistake_StarWithGroupBy")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT v / 0 FROM r GROUP BY v ORDER BY COUNT(*))", Invalid, TestName = "Mistake_AggregateOnlyInOrderBy")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r WHERE COUNT(*) = 1)", Invalid, TestName = "Mistake_AggregateInWhere")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT SUM(COUNT(*)) FROM r)", Invalid, TestName = "Mistake_NestedAggregate")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT (SELECT nosuchcol FROM r2) FROM r)", UnknownColumn, TestName = "Mistake_InAScalarSubquery")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 IN (SELECT nosuchcol FROM r2) FROM r)", UnknownColumn, TestName = "Mistake_InAnInSubquery")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r ORDER BY (SELECT nosuchcol FROM r2))", UnknownColumn, TestName = "Mistake_InAnOrderBySubquery")]
    public Task Mistake(string sql, string expected) => AssertResult(sql, expected);

    // ── A correlated body reads only FROM and WHERE ──────────────────────────────────────────────

    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 / 0 FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_ConstantDivision")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 / 0 FROM r i WHERE i.id = o.id + 100)", "", TestName = "Correlated_ConstantDivisionNoInnerRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_InnerColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT v / 0 FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_BareNameOfBothQueries")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.v / 0 FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_OuterColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT s FROM r2 i WHERE i.id = o.id)", "1", TestName = "Correlated_BareNameOfTheOuterQuery")]
    [TestCase("SELECT id FROM r o WHERE NOT EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id)", "", TestName = "Correlated_NotExists")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.v > o.v)", "", TestName = "Correlated_NoIndex")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id AND i.v = 0)", "1", TestName = "Correlated_AnotherCondition")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT * FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_Star")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id LIMIT 1)", "1", TestName = "Correlated_Limit")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id LIMIT 2)", "1", TestName = "Correlated_LimitAboveTheRows")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM em i WHERE i.id = o.id LIMIT 1)", "", TestName = "Correlated_LimitNoRows")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT DISTINCT i.v FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_Distinct")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v FROM r i WHERE i.id = o.id ORDER BY i.v / 0)", "1", TestName = "Correlated_OrderBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v FROM r i WHERE i.id = o.id ORDER BY s)", "1", TestName = "Correlated_OrderByBareName")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v FROM r i WHERE i.id = o.id GROUP BY i.v)", "1", TestName = "Correlated_GroupBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id GROUP BY i.v)", "1", TestName = "Correlated_GroupByExpression")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.v / 0 FROM r2 i WHERE i.id = o.id GROUP BY o.v)", "1", TestName = "Correlated_GroupByOuterColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT (SELECT r2.id FROM r2) FROM r i WHERE i.id = o.id)", "1", TestName = "Correlated_ASubquery")]
    public Task Correlated(string sql, string expected) => AssertResult(sql, expected);

    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.v FROM r i)", "1", TestName = "OuterInOutput_Select")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.v / 0 FROM r i)", "1", TestName = "OuterInOutput_Division")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.v FROM em i)", "", TestName = "OuterInOutput_NoInnerRow")]
    [TestCase("SELECT id FROM r o WHERE NOT EXISTS (SELECT o.v FROM em i)", "1", TestName = "OuterInOutput_NotExists")]
    [TestCase("SELECT id FROM em o WHERE EXISTS (SELECT o.v / 0 FROM r i)", "", TestName = "OuterInOutput_NoOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i ORDER BY o.v)", "1", TestName = "OuterInOutput_OrderBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i GROUP BY o.v)", "1", TestName = "OuterInOutput_GroupBy")]
    public Task OuterColumnInTheOutput(string sql, string expected) => AssertResult(sql, expected);

    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id LIMIT 0)", "", TestName = "CorrelatedLimitZero_NoRows")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id LIMIT 0)", "", TestName = "CorrelatedLimitZero_ADivisionDoesNotRun")]
    [TestCase("SELECT id FROM r o WHERE NOT EXISTS (SELECT i.v / 0 FROM r i WHERE i.id = o.id LIMIT 0)", "1", TestName = "CorrelatedLimitZero_NotExists")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 / 0 FROM r i WHERE i.id = o.id LIMIT 0)", Invalid, TestName = "CorrelatedLimitZero_KeepsTheConstantCheck")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT nosuchcol FROM r i WHERE i.id = o.id LIMIT 0)", UnknownColumn, TestName = "CorrelatedLimitZero_KeepsTheNameCheck")]
    public Task CorrelatedLimitZero(string sql, string expected) => AssertResult(sql, expected);

    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT nosuchcol FROM r i WHERE i.id = o.id)", UnknownColumn, TestName = "CorrelatedMistake_UnknownColumn")]
    [TestCase("SELECT id FROM em o WHERE EXISTS (SELECT nosuchcol FROM r i WHERE i.id = o.id)", UnknownColumn, TestName = "CorrelatedMistake_UnknownColumnNoOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.nosuchcol FROM r i WHERE i.id = o.id)", UnknownColumn, TestName = "CorrelatedMistake_UnknownOuterColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.nosuchcol FROM r i)", UnknownColumn, TestName = "CorrelatedMistake_UnknownOuterColumnOnlyInOutput")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT nosuchfn(1) FROM r i WHERE i.id = o.id)", CamusDBErrorCodes.InvalidAstStmt, TestName = "CorrelatedMistake_UnknownFunction")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT s + 1 FROM r i WHERE i.id = o.id)", Invalid, TestName = "CorrelatedMistake_InnerType")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT s + 1 FROM r2 i WHERE i.id = o.id)", Invalid, TestName = "CorrelatedMistake_BareOuterType")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.s + 1 FROM r i WHERE i.id = o.id)", Invalid, TestName = "CorrelatedMistake_OuterType")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT o.s + 1 FROM r2 i)", Invalid, TestName = "CorrelatedMistake_OuterTypeOnlyInOutput")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id ORDER BY nosuchcol)", UnknownColumn, TestName = "CorrelatedMistake_OrderBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id ORDER BY o.s + 1)", Invalid, TestName = "CorrelatedMistake_OrderByOuterType")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id GROUP BY nosuchcol)", UnknownColumn, TestName = "CorrelatedMistake_GroupBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v FROM r i WHERE i.id = o.id GROUP BY i.s)", Invalid, TestName = "CorrelatedMistake_UngroupedColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT (SELECT nosuchcol FROM r2) FROM r i WHERE i.id = o.id)", UnknownColumn, TestName = "CorrelatedMistake_InASubquery")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id AND 1 / 0 = 1)", Invalid, TestName = "CorrelatedMistake_WhereConstantDivision")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id + 100 AND 1 / 0 = 1)", Invalid, TestName = "CorrelatedMistake_WhereConstantDivisionNoInnerRow")]
    public Task CorrelatedMistake(string sql, string expected) => AssertResult(sql, expected);

    // ── A correlated body that must run is refused: it once ignored these clauses ────────────────

    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT COUNT(*) FROM r i WHERE i.id = o.id + 100)", TestName = "Refused_Aggregate")]
    [TestCase("SELECT id FROM r o WHERE NOT EXISTS (SELECT COUNT(*) FROM r i WHERE i.id = o.id + 100)", TestName = "Refused_AggregateInNotExists")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT COUNT(*) / 0 FROM r i WHERE i.id = o.id)", TestName = "Refused_AggregateDivision")]
    [TestCase("SELECT id FROM em o WHERE EXISTS (SELECT COUNT(*) FROM r i WHERE i.id = o.id)", TestName = "Refused_AggregateNoOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id OFFSET 1)", TestName = "Refused_Offset")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id OFFSET 0)", TestName = "Refused_OffsetZero")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT i.v FROM r i WHERE i.id = o.id GROUP BY i.v HAVING i.v = 99)", TestName = "Refused_GroupByHaving")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT COUNT(*) FROM r i WHERE i.id = o.id HAVING COUNT(*) > 5)", TestName = "Refused_Having")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT COUNT(*) FROM r i HAVING COUNT(*) > o.v)", TestName = "Refused_OuterColumnOnlyInHaving")]
    public Task Refused(string sql) => AssertResult(sql, NotSupported);

    [Test]
    public async Task Correlated_EachOuterRowHasItsOwnAnswer()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (2, 0, 'y'), (3, 7, 'z')");
        await Commit(database, executor, dbname, "INSERT INTO r2 (id) VALUES (3)");

        Assert.That(
            await Run(database, executor, dbname, "SELECT id FROM r o WHERE EXISTS (SELECT i.id / 0 FROM r2 i WHERE i.id = o.id) ORDER BY id"),
            Is.EqualTo("1;3"));

        Assert.That(
            await Run(database, executor, dbname, "SELECT id FROM r o WHERE NOT EXISTS (SELECT o.v / 0 FROM r2 i WHERE i.id = o.id) ORDER BY id"),
            Is.EqualTo("2"));
    }

    [Test]
    public async Task Discarded_AnIndexedColumnStillFiltersTheBody()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE INDEX r_v ON r (v)");

        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r WHERE EXISTS (SELECT s FROM r WHERE v = 0 ORDER BY v / 0)"), Is.EqualTo("1"));
        Assert.That(await Run(database, executor, dbname, "SELECT id FROM r WHERE EXISTS (SELECT 1 / 0 FROM r WHERE v = 99 ORDER BY v)"), Is.EqualTo(""));
    }
}
