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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A division by zero that the statement alone shows fails before any row is read, so it does not
/// depend on the rows, a LIMIT or a <c>WHERE FALSE</c>. Each expected result was measured on PostgreSQL
/// 16.11. A constant condition removes a branch, and a condition that reads a row does not, so a
/// constant division in a live branch fails even when no row selects that branch. A column, a parameter,
/// a subquery or a function call is not constant, so a division over one stays a row-time error. A
/// stored definition is accepted at DDL, and the statement that uses it fails.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestConstantDivisionByZero : BaseTest
{
    private const string Refused = "CADB0400: Division by zero";

    private static string Accepted(int rows) => "accepted: " + rows + " rows";

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal) || sql.StartsWith("ALTER", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs one statement in a transaction that is rolled back, and returns its outcome as a text.</summary>
    private static async Task<string> Outcome(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            ExecuteSQLTicket ticket = new(tx, dbname, sql, parameters);

            if (sql.StartsWith("SELECT", StringComparison.Ordinal) || sql.StartsWith("EXPLAIN", StringComparison.Ordinal))
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
                return Accepted((await cursor.ToListAsync()).Count);
            }

            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            {
                await executor.ExecuteDDLSQL(ticket);
                return Accepted(0);
            }

            return Accepted((int)(await executor.ExecuteNonSQLQuery(ticket)).ModifiedRows);
        }
        catch (CamusDBException ex)
        {
            return ex.Code + ": " + ex.Message;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary><c>em</c> is empty. <c>r</c> has one row, with <c>v = 0</c>.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (1, 0, 'x')");

        return (dbname, database, executor);
    }

    // ── Refused before any row is read ───────────────────────────────────────────────────────────

    [TestCase("SELECT 1 / 0 FROM em", TestName = "Refused_EmptyTable")]
    [TestCase("SELECT 1 / 0 FROM em WHERE FALSE", TestName = "Refused_WhereFalse")]
    [TestCase("SELECT 1 / 0 FROM em LIMIT 0", TestName = "Refused_LimitZero")]
    [TestCase("SELECT 1 / 0 FROM r LIMIT 0", TestName = "Refused_LimitZero_TableWithRows")]
    [TestCase("SELECT 1 / 0 FROM r", TestName = "Refused_TableWithRows")]
    [TestCase("SELECT 1 / 0 AS x FROM em", TestName = "Refused_UnderAnAlias")]
    [TestCase("SELECT 1 / 0", TestName = "Refused_FromlessSelect")]
    [TestCase("SELECT 1 / 0 AS x WHERE FALSE", TestName = "Refused_FromlessSelect_WhereFalse")]
    [TestCase("SELECT 5 WHERE 1 / 0 = 1", TestName = "Refused_FromlessSelect_InTheWhere")]
    [TestCase("SELECT 1 / (1 - 1) FROM em", TestName = "Refused_ANestedConstantDivisor")]
    [TestCase("SELECT 0 / 0 FROM em", TestName = "Refused_ZeroOverZero")]
    [TestCase("SELECT 1.5 / 0 FROM em", TestName = "Refused_FloatDividend")]
    [TestCase("SELECT 1 / 0.0 FROM em", TestName = "Refused_FloatDivisor")]
    [TestCase("SELECT 0.0 / 0 FROM em", TestName = "Refused_FloatZeroOverZero")]
    [TestCase("SELECT CAST(1.5 AS FLOAT64) / 0 FROM em", TestName = "Refused_CastToFloat")]
    [TestCase("SELECT CAST(1 AS BIGINT) / CAST(0 AS BIGINT) FROM em", TestName = "Refused_CastOperands")]
    [TestCase("SELECT NUMERIC '1.5' / 0 FROM em", TestName = "Refused_Numeric")]
    [TestCase("SELECT -1 / 0 FROM em", TestName = "Refused_NegativeDividend")]
    [TestCase("SELECT 1 / CASE WHEN TRUE THEN 0 END FROM em", TestName = "Refused_DivisorIsAConstantCase")]
    [TestCase("SELECT 1 / COALESCE(0, 1) FROM em", TestName = "Refused_DivisorIsAConstantCoalesce")]
    [TestCase("SELECT id FROM em WHERE id = 1 / 0", TestName = "Refused_InWhere")]
    [TestCase("SELECT id FROM em WHERE id BETWEEN 1 / 0 AND 2", TestName = "Refused_InBetween")]
    [TestCase("SELECT id FROM em ORDER BY 1 / 0", TestName = "Refused_InOrderBy")]
    [TestCase("SELECT COUNT(*) FROM em GROUP BY v HAVING COUNT(*) = 1 / 0", TestName = "Refused_InHaving")]
    [TestCase("SELECT SUM(1 / 0) FROM em", TestName = "Refused_InAnAggregateArgument")]
    [TestCase("SELECT SUM(1 / 0) OVER () FROM em", TestName = "Refused_InAWindowArgument")]
    [TestCase("SELECT abs(1 / 0) FROM em", TestName = "Refused_InAFunctionArgument")]
    [TestCase("SELECT NULLIF(1 / 0, 1) FROM em", TestName = "Refused_InNullif")]
    [TestCase("SELECT e1.id FROM em e1 JOIN em e2 ON e1.id = 1 / 0", TestName = "Refused_InJoinOn")]
    [TestCase("SELECT g FROM generate_series(1, 1 / 0) AS g LIMIT 0", TestName = "Refused_InATableFunctionArgument_LimitZero")]
    [TestCase("EXPLAIN SELECT 1 / 0 FROM em", TestName = "Refused_InExplain")]
    public async Task Select_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, sql));
    }

    [TestCase("SELECT (SELECT 1 / 0 FROM em)", TestName = "Subquery_ScalarInAFromlessSelect")]
    [TestCase("SELECT id FROM em WHERE id = (SELECT 1 / 0 FROM em)", TestName = "Subquery_ScalarInWhere")]
    [TestCase("SELECT id FROM em WHERE id IN (SELECT 1 / 0 FROM em)", TestName = "Subquery_InSelectList")]
    [TestCase("SELECT id FROM em WHERE id IN (SELECT id FROM em WHERE 1 / 0 = 1)", TestName = "Subquery_SemiJoinInnerFilter")]
    [TestCase("SELECT id FROM em WHERE id NOT IN (SELECT id FROM em WHERE 1 / 0 = 1)", TestName = "Subquery_AntiJoinInnerFilter")]
    [TestCase("SELECT id FROM em WHERE (1 / 0) IN (SELECT id FROM em)", TestName = "Subquery_TestedValueOfIn")]
    [TestCase("SELECT d.x FROM (SELECT 1 / 0 AS x FROM em) AS d", TestName = "Subquery_DerivedTable")]
    [TestCase("SELECT 1 / 0 FROM em UNION ALL SELECT 1 FROM em", TestName = "Subquery_FirstSetOperand")]
    [TestCase("SELECT 1 FROM em UNION ALL SELECT 1 / 0 FROM em", TestName = "Subquery_SecondSetOperand")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT id FROM em WHERE 1 / 0 = 1)", TestName = "Exists_InnerWhere")]
    [TestCase("SELECT id FROM em e1 WHERE EXISTS (SELECT 1 FROM em e2 WHERE e2.id = e1.id AND 1 / 0 = 1)", TestName = "Exists_InnerWhere_Correlated")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT SUM(1 / 0) FROM em)", TestName = "Exists_AnAggregateRuns")]
    public async Task NestedSelect_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, sql));
    }

    [TestCase("UPDATE em SET v = 1 / 0 WHERE FALSE", TestName = "Write_UpdateSet_WhereFalse")]
    [TestCase("UPDATE em SET v = 1 / 0 WHERE TRUE", TestName = "Write_UpdateSet_EmptyTable")]
    [TestCase("UPDATE r SET v = 1 / 0 WHERE FALSE", TestName = "Write_UpdateSet_NoRowMatches")]
    [TestCase("UPDATE em SET v = 1 WHERE id = 1 / 0", TestName = "Write_UpdateWhere")]
    [TestCase("UPDATE em SET v = CASE WHEN id > 0 THEN 1 / 0 ELSE 1 END WHERE FALSE", TestName = "Write_UpdateSet_LiveBranch")]
    [TestCase("DELETE FROM em WHERE id = 1 / 0", TestName = "Write_DeleteWhere")]
    [TestCase("DELETE FROM em WHERE FALSE RETURNING 1 / 0", TestName = "Write_DeleteReturning")]
    [TestCase("UPDATE em SET v = 1 WHERE FALSE RETURNING 1 / 0", TestName = "Write_UpdateReturning")]
    [TestCase("INSERT INTO em (id, v) VALUES (1, 1 / 0)", TestName = "Write_InsertValues")]
    [TestCase("INSERT INTO em (id, v) SELECT id, 1 / 0 FROM em", TestName = "Write_InsertSelect_NoSourceRow")]
    [TestCase("INSERT INTO em (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = 1 / 0", TestName = "Write_OnConflictSet_NoConflict")]
    [TestCase("CREATE TABLE cc AS SELECT 1 / 0 AS x FROM em", TestName = "Write_CreateTableAs_NoSourceRow")]
    [TestCase("CREATE MATERIALIZED VIEW mv AS SELECT 1 / 0 AS x FROM em", TestName = "Write_MaterializedView_NoSourceRow")]
    public async Task Write_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, sql));
    }

    [Test]
    public async Task Write_ARefusedStatement_ChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        KvTransaction tx = await database.Transactions.BeginAsync();
        Assert.ThrowsAsync<CamusDBException>(() => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "UPDATE r SET v = 1 / 0 WHERE TRUE", null)));
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(Accepted(1), await Outcome(database, executor, dbname, "SELECT id FROM r WHERE v = 0"));
    }

    // ── A condition that reads a row removes nothing ─────────────────────────────────────────────

    [TestCase("SELECT CASE WHEN v > 0 THEN 1 / 0 ELSE 1 END FROM em", TestName = "LiveBranch_CaseThen")]
    [TestCase("SELECT CASE WHEN v > 0 THEN 1 ELSE 1 / 0 END FROM em", TestName = "LiveBranch_CaseElse")]
    [TestCase("SELECT CASE v WHEN 1 THEN 1 / 0 END FROM em", TestName = "LiveBranch_SimpleCase")]
    [TestCase("SELECT CASE WHEN v > 0 THEN 1 / 0 ELSE 1 END FROM r", TestName = "LiveBranch_NoRowSelectsIt")]
    [TestCase("SELECT CASE WHEN FALSE THEN 1 WHEN v > 0 THEN 1 / 0 END FROM em", TestName = "LiveBranch_AfterADeadBranch")]
    [TestCase("SELECT IF(v > 0, 1 / 0, 1) FROM em", TestName = "LiveBranch_IfTrueResult")]
    [TestCase("SELECT IF(v > 0, 1, 1 / 0) FROM em", TestName = "LiveBranch_IfFalseResult")]
    [TestCase("SELECT COALESCE(v, 1 / 0) FROM em", TestName = "LiveBranch_CoalesceAfterAColumn")]
    [TestCase("SELECT COALESCE(NULL, 1 / 0) FROM em", TestName = "LiveBranch_CoalesceAfterNull")]
    [TestCase("SELECT id FROM em WHERE v = 1 AND 1 / 0 = 1", TestName = "LiveBranch_AndAfterAColumn")]
    [TestCase("SELECT id FROM em WHERE v = 1 OR 1 / 0 = 1", TestName = "LiveBranch_OrAfterAColumn")]
    [TestCase("SELECT id FROM em WHERE NULL AND 1 / 0 = 1", TestName = "LiveBranch_NullDoesNotDecideAnd")]
    [TestCase("SELECT id FROM em WHERE 1 / 0 = 1 AND FALSE", TestName = "LiveBranch_DivisionBeforeFalse")]
    [TestCase("SELECT id FROM em WHERE 1 / 0 = 1 OR TRUE", TestName = "LiveBranch_DivisionBeforeTrue")]
    public async Task LiveBranch_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, sql));
    }

    [Test]
    public async Task ACaseOverAParameter_IsNotAConstantCondition()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.Bool, false) };

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT CASE WHEN @p THEN 1 / 0 ELSE 1 END FROM em", parameters));
        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT CASE WHEN @p THEN 1 / 0 ELSE 1 END", parameters));
        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "INSERT INTO em (id, v) VALUES (1, CASE WHEN @p THEN 1 / 0 ELSE 1 END)", parameters));
    }

    // ── A constant condition removes a branch ────────────────────────────────────────────────────

    [TestCase("SELECT CASE WHEN FALSE THEN 1 / 0 ELSE 1 END FROM em", 0, TestName = "DeadBranch_CaseFalse")]
    [TestCase("SELECT CASE WHEN NULL THEN 1 / 0 ELSE 1 END FROM em", 0, TestName = "DeadBranch_CaseNull")]
    [TestCase("SELECT CASE WHEN 1 = 1 THEN 1 ELSE 1 / 0 END FROM em", 0, TestName = "DeadBranch_ElseAfterConstantTrue")]
    [TestCase("SELECT CASE WHEN FALSE THEN 1 / 0 ELSE v END FROM em", 0, TestName = "DeadBranch_FalseBesideAColumn")]
    [TestCase("SELECT CASE WHEN TRUE THEN v ELSE 1 / 0 END FROM em", 0, TestName = "DeadBranch_ElseBesideAColumn")]
    [TestCase("SELECT CASE WHEN TRUE THEN v WHEN v > 0 THEN 1 / 0 END FROM em", 0, TestName = "DeadBranch_AfterConstantTrue")]
    [TestCase("SELECT CASE WHEN v > 0 THEN 1 WHEN TRUE THEN 2 ELSE 1 / 0 END FROM em", 0, TestName = "DeadBranch_ElseAfterALaterTrue")]
    [TestCase("SELECT CASE WHEN (v = 1 AND FALSE) THEN 1 / 0 ELSE 1 END FROM em", 0, TestName = "DeadBranch_ConditionThatFolds")]
    [TestCase("SELECT CASE 1 WHEN 2 THEN 1 / 0 ELSE v END FROM em", 0, TestName = "DeadBranch_SimpleCaseNoMatch")]
    [TestCase("SELECT CASE 1 WHEN 1 THEN v ELSE 1 / 0 END FROM em", 0, TestName = "DeadBranch_SimpleCaseMatch")]
    [TestCase("SELECT CASE WHEN 1 = 2 THEN (1 / 0) ELSE 7 END", 1, TestName = "DeadBranch_FromlessCase")]
    [TestCase("SELECT CASE WHEN 1 = 1 THEN 'first' WHEN (1 / 0) = 1 THEN 'second' END", 1, TestName = "DeadBranch_DeadCondition")]
    [TestCase("SELECT IF(true, 1, 1 / 0)", 1, TestName = "DeadBranch_IfTrue")]
    [TestCase("SELECT IF(false, 1 / 0, 7)", 1, TestName = "DeadBranch_IfFalse")]
    [TestCase("SELECT IF(FALSE, 1 / 0, v) FROM em", 0, TestName = "DeadBranch_IfBesideAColumn")]
    [TestCase("SELECT id FROM em WHERE FALSE AND 1 / 0 = 1", 0, TestName = "DeadBranch_FalseAnd")]
    [TestCase("SELECT id FROM em WHERE TRUE OR 1 / 0 = 1", 0, TestName = "DeadBranch_TrueOr")]
    [TestCase("SELECT id FROM em WHERE FALSE AND v = 1 / 0", 0, TestName = "DeadBranch_FalseAndAColumn")]
    [TestCase("SELECT id FROM em WHERE v = 1 AND FALSE AND 1 / 0 = 1", 0, TestName = "DeadBranch_ColumnAndFalseAnd")]
    [TestCase("SELECT id FROM em WHERE v = 1 OR TRUE OR 1 / 0 = 1", 0, TestName = "DeadBranch_ColumnOrTrueOr")]
    [TestCase("SELECT id FROM em WHERE NOT (FALSE AND 1 / 0 = 1)", 0, TestName = "DeadBranch_UnderNot")]
    [TestCase("SELECT COALESCE(1, 1 / 0) FROM em", 0, TestName = "DeadBranch_CoalesceAfterAConstant")]
    [TestCase("SELECT COALESCE(NULL, 1, 1 / 0) FROM em", 0, TestName = "DeadBranch_CoalesceAfterNullAndAConstant")]
    [TestCase("SELECT COALESCE(v, 1, 1 / 0) FROM em", 0, TestName = "DeadBranch_CoalesceAfterAColumnAndAConstant")]
    [TestCase("SELECT 1 / COALESCE(1, 1 / 0) FROM em", 0, TestName = "DeadBranch_CoalesceAsADivisor")]
    public async Task DeadBranch_IsNotRefused(string sql, int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(rows), await Outcome(database, executor, dbname, sql));
    }

    /// <summary>
    /// A subquery binds before the statement around it, so a dead branch does not keep it from its own
    /// check. PostgreSQL does not plan this subquery, and returns no rows.
    /// </summary>
    [Test]
    public async Task ASubqueryInADeadBranch_StillBinds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT CASE WHEN FALSE THEN (SELECT 1 / 0 FROM em) ELSE 1 END FROM em"));
    }

    // ── EXISTS discards its select list ──────────────────────────────────────────────────────────

    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 / 0 FROM em)", TestName = "Exists_SelectList")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT 1 / 0 FROM em LIMIT 1)", TestName = "Exists_SelectListWithLimit")]
    [TestCase("SELECT id FROM em WHERE EXISTS (SELECT id FROM em ORDER BY 1 / 0)", TestName = "Exists_OrderBy")]
    [TestCase("SELECT id FROM em e1 WHERE EXISTS (SELECT 1 / 0 FROM em e2 WHERE e2.id = e1.id)", TestName = "Exists_SelectList_Correlated")]
    public async Task Exists_DiscardedOutput_IsNotRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, sql));
    }

    // ── Not a constant: a row-time error ─────────────────────────────────────────────────────────

    [TestCase("SELECT v / 0 FROM em", TestName = "Column_DividendOverZero")]
    [TestCase("SELECT id FROM em WHERE v / 0 = 1", TestName = "Column_DividendOverZero_InWhere")]
    [TestCase("SELECT 1 / v FROM em", TestName = "Column_Divisor")]
    [TestCase("SELECT 1 / NULL FROM em", TestName = "Null_Divisor")]
    [TestCase("SELECT NULL / 0 FROM em", TestName = "Null_Dividend")]
    [TestCase("SELECT 1 / (SELECT 0 FROM r) FROM em", TestName = "Subquery_Divisor")]
    public async Task NotAConstantDivision_NeedsARow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, sql));
    }

    [TestCase("UPDATE em SET v = 1 / (SELECT 0 FROM r) WHERE FALSE", TestName = "Subquery_DivisorInAnUpdateSet")]
    [TestCase("UPDATE em SET v = 1 WHERE id = 1 / (SELECT 0 FROM r)", TestName = "Subquery_DivisorInAnUpdateWhere")]
    [TestCase("DELETE FROM em WHERE id = 1 / (SELECT 0 FROM r)", TestName = "Subquery_DivisorInADeleteWhere")]
    public async Task ASubqueryDivisorOfAWrite_IsNotAConstant(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, sql));
    }

    [TestCase("SELECT v / 0 FROM r", TestName = "RowTime_ColumnOverZero")]
    [TestCase("SELECT 1 / v FROM r", TestName = "RowTime_ZeroInAColumn")]
    [TestCase("SELECT 1 / (SELECT 0 FROM r) FROM r", TestName = "RowTime_SubqueryDivisor")]
    public async Task ARowStillReachesTheError(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, sql));
    }

    [Test]
    public async Task AParameter_IsNotAConstant()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> zero = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 0) };

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, "SELECT 1 / @p FROM em", zero));
        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT 1 / @p FROM r", zero));
    }

    [Test]
    public async Task AGuardOverAColumn_StillRuns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, "SELECT CASE WHEN v <> 0 THEN 1 / v ELSE 42 END AS x FROM r", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(42L, rows[0].Row["x"].LongValue);
    }

    [TestCase("SELECT 1 / 1 FROM r", 1, TestName = "Correct_ANonZeroDivisor")]
    [TestCase("SELECT 1 / (2 - 1) FROM r", 1, TestName = "Correct_ANestedNonZeroDivisor")]
    [TestCase("SELECT 1.5 / 0.5 FROM r", 1, TestName = "Correct_FloatDivisor")]
    [TestCase("SELECT id FROM r WHERE v = 0 / 1", 1, TestName = "Correct_ZeroDividend")]
    public async Task ACorrectDivision_IsNotRefused(string sql, int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(rows), await Outcome(database, executor, dbname, sql));
    }

    /// <summary>Another value error in a constant stays an error of the row that reaches it.</summary>
    [Test]
    public async Task AnotherValueError_KeepsItsOwnRule()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, "SELECT (9223372036854775807 + 1) / 0 FROM em"));
        Assert.That(await Outcome(database, executor, dbname, "SELECT (9223372036854775807 + 1) / 0 FROM r"), Does.StartWith(CamusDBErrorCodes.IntegerValueOutOfRange));
    }

    // ── A stored definition fails when a statement uses it ───────────────────────────────────────

    [Test]
    public async Task View_IsAccepted_AndAReadOfItIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW vw AS SELECT 1 / 0 AS x FROM em");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT x FROM vw"));
    }

    [Test]
    public async Task View_WithADivisionInASubquery_IsAccepted_AndAReadOfItIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW vn AS SELECT id FROM em WHERE id IN (SELECT 1 / 0 FROM em)");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT id FROM vn"));
    }

    [Test]
    public async Task View_OverAViewWithADivision_IsAccepted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW v1 AS SELECT 1 / 0 AS x FROM em");
        await Commit(database, executor, dbname, "CREATE VIEW v2 AS SELECT x FROM v1");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT x FROM v2"));
    }

    /// <summary>
    /// CREATE VIEW turns the check off while it binds the body. The next statement in the same
    /// asynchronous flow must have it on, also for the same text as the body.
    /// </summary>
    [Test]
    public async Task View_DoesNotTurnTheCheckOffForTheNextStatement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW vw AS SELECT 1 / 0 AS x FROM em");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT 1 / 0 AS x FROM em"));
    }

    /// <summary>
    /// The scope is tested directly because a statement cannot show this: CREATE VIEW opens it in an
    /// async method, and the runtime restores an AsyncLocal for the caller when that method returns.
    /// </summary>
    [Test]
    public void TheSuspendScope_EndsWithItsBlock_AndKeepsAnOuterScope()
    {
        NodeAst division = SQLParserProcessor.Parse("SELECT 1 / 0").leftAst!;

        using (ConstantDivisionCheck.SuspendForStoredDefinition())
        {
            using (ConstantDivisionCheck.SuspendForStoredDefinition())
                ConstantDivisionCheck.Check(division);

            ConstantDivisionCheck.Check(division);
        }

        CamusDBException ex = Assert.Throws<CamusDBException>(() => ConstantDivisionCheck.Check(division))!;
        Assert.AreEqual(Refused, ex.Code + ": " + ex.Message);
    }

    [Test]
    public async Task CheckConstraint_IsAccepted_AndAWriteIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE ck (id INT64 PRIMARY KEY, v INT64, CHECK (v = 1 / 0))");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "INSERT INTO ck (id, v) VALUES (1, 1)"));
    }

    [Test]
    public async Task AddConstraint_IsAccepted_AndAWriteIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "ALTER TABLE em ADD CONSTRAINT c1 CHECK (v = 1 / 0)");

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "INSERT INTO em (id, v) VALUES (1, 1)"));
    }

    // ── The refusal does not depend on what ran before ───────────────────────────────────────────

    [Test]
    public async Task TheSameStatementTwice_IsRefusedTwice()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT 1 / 0 FROM em"));
        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, "SELECT 1 / 0 FROM em"));
    }

    [Test]
    public async Task ACachedCorrectStatement_StaysCorrect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(1), await Outcome(database, executor, dbname, "SELECT 1 / 1 FROM r"));
        Assert.AreEqual(Accepted(1), await Outcome(database, executor, dbname, "SELECT 1 / 1 FROM r"));
    }

    // ── A long statement ─────────────────────────────────────────────────────────────────────────

    private static string Chain(string first, string op, string term, string last, int terms) =>
        first + string.Concat(Enumerable.Repeat($" {op} {term}", terms)) + last;

    [TestCase(800, TestName = "LongChain_Constant")]
    public async Task ALongConstantChain_IsRefused(int terms)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, $"SELECT {Chain("1", "+", "1", " + 1 / 0", terms)} FROM em"));
    }

    [TestCase(800, TestName = "LongChain_OverAColumn")]
    public async Task ALongChainOverAColumn_IsRefused(int terms)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, $"SELECT {Chain("v", "+", "1", " + 1 / 0", terms)} FROM em"));
    }

    [TestCase(800, TestName = "LongChain_ConstantAnd")]
    public async Task ALongConstantAnd_IsRefused(int terms)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Refused, await Outcome(database, executor, dbname, $"SELECT id FROM em WHERE {Chain("TRUE", "AND", "TRUE", " AND 1 / 0 = 1", terms)}"));
    }

    [TestCase(800, TestName = "LongChain_NoDivisionByZero")]
    public async Task ALongChainWithACorrectDivision_IsAccepted(int terms)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(Accepted(0), await Outcome(database, executor, dbname, $"SELECT {Chain("v", "+", "1", " + 1 / 1", terms)} FROM em"));
    }
}
