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
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A statement error that the statement alone shows must not depend on the rows. These tests run each
/// mistake over a table with rows and an empty table, under LIMIT 0 and WHERE FALSE, in a dead branch,
/// and at each expression position. The positive cases guard the other side: a check that refuses a
/// correct statement is worse than no check, so each rule also has a statement it must accept.
/// </summary>
[NonParallelizable]
public sealed class TestStaticStatementChecks : SharedNodeBaseTest
{
    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>Runs one statement of any kind and returns the row count, in a transaction that is rolled back.</summary>
    private static async Task<int> Run(
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
                return (await cursor.ToListAsync()).Count;
            }

            if (sql.StartsWith("CREATE", StringComparison.Ordinal) || sql.StartsWith("ALTER", StringComparison.Ordinal))
            {
                await executor.ExecuteDDLSQL(ticket);
                return 0;
            }

            return (int)(await executor.ExecuteNonSQLQuery(ticket)).ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    private static async Task<string> Refusal(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        try
        {
            int rows = await Run(database, executor, dbname, sql, parameters);
            return "accepted: " + rows + " rows";
        }
        catch (CamusDBException ex)
        {
            return ex.Code;
        }
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string table in new[] { "t", "e", "u" })
        {
            await Ddl(database, executor, dbname,
                $"CREATE TABLE {table} (id INT64 PRIMARY KEY, a INT64, s STRING, d DATE, f BOOL, n FLOAT64, tags array(string))");
        }

        await Write(database, executor, dbname,
            "INSERT INTO t (id, a, s, d, f, n, tags) VALUES " +
            "(1, 10, 'one', '2026-01-01', TRUE, 1.5, ARRAY['red']), (2, 20, 'two', '2026-01-02', FALSE, 2.5, ARRAY['blue'])");

        await Write(database, executor, dbname,
            "INSERT INTO u (id, a, s, d, f, n, tags) VALUES (1, 10, 'one', '2026-01-01', TRUE, 1.5, ARRAY['red'])");

        return (dbname, database, executor);
    }

    // ── The statements the defect report names ───────────────────────────────────────────────────

    [TestCase("SELECT nofunc(a) FROM t", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_TableWithRows")]
    [TestCase("SELECT nofunc(a) FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_EmptyTable")]
    [TestCase("SELECT nofunc(a) FROM t LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_LimitZero")]
    [TestCase("SELECT nofunc(a) FROM t WHERE FALSE", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_WhereFalse")]
    [TestCase("SELECT id FROM t WHERE nofunc(a) = 1 LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_InWhere_LimitZero")]
    [TestCase("SELECT id FROM t ORDER BY nofunc(a) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "UnknownFunction_InOrderBy_LimitZero")]
    [TestCase("SELECT id FROM t WHERE a = 'x'", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_TableWithRows")]
    [TestCase("SELECT id FROM e WHERE s = 1", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_EmptyTable")]
    [TestCase("SELECT id FROM t WHERE a = 'x' LIMIT 0", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_LimitZero")]
    [TestCase("SELECT id FROM t WHERE a = 'x' AND FALSE", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_DeadAndBranch")]
    [TestCase("SELECT id FROM t WHERE TRUE OR a = 'x'", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_DeadOrBranch")]
    [TestCase("SELECT a + 'x' FROM t LIMIT 0", CamusDBErrorCodes.InvalidInput, TestName = "TypeError_Arithmetic_LimitZero")]
    [TestCase("SELECT id FROM t WHERE 1", CamusDBErrorCodes.InvalidInput, TestName = "NonBooleanWhere_Integer")]
    [TestCase("SELECT id FROM t WHERE 'x'", CamusDBErrorCodes.InvalidInput, TestName = "NonBooleanWhere_String")]
    public async Task StatementFromTheDefectReport_IsRefused(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(code, await Refusal(database, executor, dbname, sql));
    }

    /// <summary>
    /// Every mistake in every statement shape, over a table with rows and over an empty one. A shape that
    /// reads no row and a shape that reads all of them must give one error.
    /// </summary>
    [Test]
    public async Task EachMistake_FailsInEachShape_WhateverTheRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string Expression, string Code)[] mistakes =
        [
            ("nofunc(a)", CamusDBErrorCodes.InvalidAstStmt),
            ("abs(a, a)", CamusDBErrorCodes.InvalidInput),
            ("a = 'x'", CamusDBErrorCodes.InvalidInput),
            ("s = 1", CamusDBErrorCodes.InvalidInput),
            ("f = 1", CamusDBErrorCodes.InvalidInput),
            ("a < 'x'", CamusDBErrorCodes.InvalidInput),
            ("a BETWEEN 'x' AND 'y'", CamusDBErrorCodes.InvalidInput),
            ("a IN (1, 'x')", CamusDBErrorCodes.InvalidInput),
            ("a NOT IN ('x')", CamusDBErrorCodes.InvalidInput),
            ("d = 'not a date'", CamusDBErrorCodes.InvalidInput),
            ("NULLIF(a, 'x')", CamusDBErrorCodes.InvalidInput),
            ("a + 'x'", CamusDBErrorCodes.InvalidInput),
            ("s * 2", CamusDBErrorCodes.InvalidInput),
            ("-s", CamusDBErrorCodes.InvalidInput),
            ("a || 1", CamusDBErrorCodes.InvalidInput),
            ("a & 'x'", CamusDBErrorCodes.InvalidInput),
            ("~s", CamusDBErrorCodes.InvalidInput),
            ("NOT a", CamusDBErrorCodes.InvalidInput),
            ("a AND TRUE", CamusDBErrorCodes.InvalidInput),
            ("TRUE OR s", CamusDBErrorCodes.InvalidInput),
            ("a IS TRUE", CamusDBErrorCodes.InvalidInput),
            ("a LIKE 'x'", CamusDBErrorCodes.InvalidAstStmt),
            ("s ~ 1", CamusDBErrorCodes.InvalidAstStmt),
            ("a[1]", CamusDBErrorCodes.InvalidInput),
            ("tags['x']", CamusDBErrorCodes.InvalidInput),
            ("tags = ARRAY[1]", CamusDBErrorCodes.InvalidInput),
            ("1 = ANY (tags)", CamusDBErrorCodes.InvalidInput),
            ("a < ANY (tags)", CamusDBErrorCodes.InvalidInput),
            ("a < ANY (a)", CamusDBErrorCodes.InvalidInput),
            ("CASE WHEN a THEN 1 END", CamusDBErrorCodes.InvalidInput),
            ("CASE a WHEN 'x' THEN 1 END", CamusDBErrorCodes.InvalidInput),
            ("IF(a, 1, 2)", CamusDBErrorCodes.InvalidInput),
            ("(a + 1) = 'x'", CamusDBErrorCodes.InvalidInput),
            ("(s || 'y') = 1", CamusDBErrorCodes.InvalidInput),
            ("CAST(a AS STRING) = 1", CamusDBErrorCodes.InvalidInput),
        ];

        string[] shapes =
        [
            "SELECT {e} FROM {t}",
            "SELECT {e} AS x FROM {t} LIMIT 0",
            "SELECT {e} FROM {t} WHERE FALSE",
            "SELECT {e} FROM {t} WHERE NULL",
            "SELECT id FROM {t} ORDER BY {e} LIMIT 0",
            "SELECT id FROM {t} WHERE id < 0 ORDER BY {e}",
            "SELECT {e} FROM {t} GROUP BY {e} LIMIT 0",
            "SELECT id, CASE WHEN FALSE THEN {e} END FROM {t} LIMIT 0",
            "SELECT id FROM (SELECT id, {e} AS x FROM {t}) d LIMIT 0",
            "SELECT id FROM {t} UNION ALL SELECT id FROM {t} WHERE id = 0 UNION ALL SELECT {e} FROM {t} LIMIT 0",
            "EXPLAIN SELECT {e} FROM {t}",
        ];

        List<string> wrong = [];

        foreach ((string expression, string code) in mistakes)
        {
            foreach (string shape in shapes)
            {
                foreach (string table in new[] { "t", "e" })
                {
                    string sql = shape.Replace("{e}", expression).Replace("{t}", table);
                    string outcome = await Refusal(database, executor, dbname, sql);

                    if (outcome != code)
                        wrong.Add($"{sql} => {outcome}, expected {code}");
                }
            }
        }

        Assert.IsEmpty(wrong, string.Join(Environment.NewLine, wrong));
    }

    /// <summary>A condition that is not boolean in a WHERE: the mistakes that are valid expressions on their own.</summary>
    [Test]
    public async Task EachMistake_FailsInEachConditionShape_WhateverTheRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        (string Expression, string Code)[] mistakes =
        [
            ("nofunc(a) = 1", CamusDBErrorCodes.InvalidAstStmt),
            ("a = 'x'", CamusDBErrorCodes.InvalidInput),
            ("s = 1", CamusDBErrorCodes.InvalidInput),
            ("NOT a", CamusDBErrorCodes.InvalidInput),
            ("(a + 'x') = 1", CamusDBErrorCodes.InvalidInput),
            ("a", CamusDBErrorCodes.InvalidInput),
            ("s", CamusDBErrorCodes.InvalidInput),
            ("n", CamusDBErrorCodes.InvalidInput),
            ("1", CamusDBErrorCodes.InvalidInput),
            ("1.5", CamusDBErrorCodes.InvalidInput),
            ("'x'", CamusDBErrorCodes.InvalidInput),
            ("a + 1", CamusDBErrorCodes.InvalidInput),
            ("CAST(1 AS INT64)", CamusDBErrorCodes.InvalidInput),
        ];

        string[] shapes =
        [
            "SELECT id FROM {t} WHERE {e}",
            "SELECT id FROM {t} WHERE {e} LIMIT 0",
            "SELECT COUNT(*) FROM {t} WHERE {e}",
            "SELECT id FROM {t} WHERE id = 1 AND ({e} OR TRUE)",
            "SELECT a, COUNT(*) FROM {t} GROUP BY a HAVING {e} LIMIT 0",
            "SELECT x.id FROM {t} x JOIN u ON {e} LIMIT 0",
            "SELECT x.id FROM {t} x LEFT JOIN u ON {e} LIMIT 0",
            "SELECT id FROM u WHERE id IN (SELECT id FROM {t} WHERE {e})",
            "SELECT id FROM u WHERE EXISTS (SELECT 1 FROM {t} WHERE {e})",
            "SELECT id FROM (SELECT id, a, s, n FROM {t} WHERE {e}) d LIMIT 0",
            "UPDATE {t} SET a = 0 WHERE {e}",
            "DELETE FROM {t} WHERE {e}",
        ];

        List<string> wrong = [];

        foreach ((string expression, string code) in mistakes)
        {
            foreach (string shape in shapes)
            {
                foreach (string table in new[] { "t", "e" })
                {
                    // In a join ON both tables have the column, so an unqualified name is ambiguous.
                    string written = shape.Contains(" ON ", StringComparison.Ordinal)
                        ? expression.Replace("a", "x.a").Replace("s", "x.s").Replace("n", "x.n").Replace("x.x.", "x.").Replace("x.nofunc", "nofunc")
                        : expression;

                    if (shape.Contains(" ON ", StringComparison.Ordinal) && expression != written && !written.Contains("x.", StringComparison.Ordinal))
                        continue;

                    string sql = shape.Replace("{e}", written).Replace("{t}", table);
                    string outcome = await Refusal(database, executor, dbname, sql);

                    if (outcome != code)
                        wrong.Add($"{sql} => {outcome}, expected {code}");
                }
            }
        }

        Assert.IsEmpty(wrong, string.Join(Environment.NewLine, wrong));
    }

    // ── A non-boolean condition and the writes it could reach ────────────────────────────────────

    [TestCase("UPDATE t SET a = 0 WHERE 1", TestName = "Update_WhereOne_WritesNothing")]
    [TestCase("UPDATE t SET a = 0 WHERE a", TestName = "Update_WhereIntegerColumn_WritesNothing")]
    [TestCase("DELETE FROM t WHERE 1", TestName = "Delete_WhereOne_DeletesNothing")]
    [TestCase("DELETE FROM t WHERE s", TestName = "Delete_WhereStringColumn_DeletesNothing")]
    public async Task NonBooleanWrite_IsRefused_AndChangesNothing(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        KvTransaction tx = await database.Transactions.BeginAsync();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null)));

        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(2, await Run(database, executor, dbname, "SELECT id FROM t WHERE a IN (10, 20)"));
    }

    [Test]
    public async Task Update_WhereTrue_StillWritesEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Write(database, executor, dbname, "UPDATE t SET a = 0 WHERE TRUE");

        Assert.AreEqual(2, await Run(database, executor, dbname, "SELECT id FROM t WHERE a = 0"));
    }

    // ── Types that only a value shows are checked per row ────────────────────────────────────────

    [Test]
    public async Task Parameter_NonBooleanCondition_IsRefusedWhenARowReachesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        foreach (ColumnValue value in new[] { new ColumnValue(ColumnType.Integer64, 1L), new ColumnValue(ColumnType.Integer64, 0L), new ColumnValue(ColumnType.String, "x"), new ColumnValue(ColumnType.Float64, 1.5) })
        {
            Dictionary<string, ColumnValue> parameters = new() { ["@p"] = value };

            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, "SELECT id FROM t WHERE @p", parameters), value.Type.ToString());
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, "DELETE FROM t WHERE @p", parameters), value.Type.ToString());
        }

        Assert.AreEqual(2, await Run(database, executor, dbname, "SELECT id FROM t WHERE @p", new() { ["@p"] = ColumnValue.FromBool(true) }));
        Assert.AreEqual(0, await Run(database, executor, dbname, "SELECT id FROM t WHERE @p", new() { ["@p"] = ColumnValue.FromBool(false) }));
        Assert.AreEqual(0, await Run(database, executor, dbname, "SELECT id FROM t WHERE @p", new() { ["@p"] = ColumnValue.Null }));
    }

    [TestCase("SELECT id FROM t WHERE length(s)", TestName = "FunctionResult_InWhere_IsRefusedWhenARowReachesIt")]
    [TestCase("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*)", TestName = "AggregateResult_InHaving_IsRefusedWhenAGroupReachesIt")]
    [TestCase("SELECT x.id FROM t x JOIN u ON length(x.s)", TestName = "FunctionResult_InJoinOn_IsRefusedWhenARowReachesIt")]
    [TestCase("SELECT id FROM u WHERE EXISTS (SELECT 1 FROM t WHERE length(t.s) AND t.id = u.id)", TestName = "FunctionResult_InCorrelatedExists_IsRefused")]
    public async Task FunctionResult_NonBooleanCondition_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, sql));
    }

    [Test]
    public async Task ParameterOperand_TypeMismatch_IsRefusedWhenARowReachesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Dictionary<string, ColumnValue> text = new() { ["@p"] = new ColumnValue(ColumnType.String, "x") };

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, "SELECT id FROM t WHERE a = @p", text));

        // The same statement over no rows has no value to compare, and a NULL parameter has no type.
        Assert.AreEqual(0, await Run(database, executor, dbname, "SELECT id FROM e WHERE a = @p", text));
        Assert.AreEqual(0, await Run(database, executor, dbname, "SELECT id FROM t WHERE a = @p", new() { ["@p"] = ColumnValue.Null }));
        Assert.AreEqual(1, await Run(database, executor, dbname, "SELECT id FROM t WHERE a = @p", new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 10L) }));
    }

    // ── Every expression position, including the ones outside a plain SELECT ─────────────────────

    [TestCase("UPDATE e SET a = nofunc(1) WHERE TRUE", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_UpdateSet")]
    [TestCase("UPDATE e SET a = 1 WHERE nofunc(a) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_UpdateWhere")]
    [TestCase("UPDATE e SET a = 1 WHERE s = 1", CamusDBErrorCodes.InvalidInput, TestName = "Position_UpdateWhere_TypeError")]
    [TestCase("UPDATE e SET a = a + 'x' WHERE TRUE", CamusDBErrorCodes.InvalidInput, TestName = "Position_UpdateSet_TypeError")]
    [TestCase("UPDATE e SET a = COUNT(*) WHERE TRUE", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_UpdateSet_Aggregate")]
    [TestCase("DELETE FROM e WHERE nofunc(a) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_DeleteWhere")]
    [TestCase("DELETE FROM e WHERE COUNT(*) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_DeleteWhere_Aggregate")]
    [TestCase("UPDATE e SET a = 1 WHERE TRUE RETURNING nofunc(a)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_Returning")]
    [TestCase("UPDATE e SET a = 1 WHERE TRUE RETURNING a + 'x'", CamusDBErrorCodes.InvalidInput, TestName = "Position_Returning_TypeError")]
    [TestCase("INSERT INTO e (id, a) VALUES (1, CASE WHEN FALSE THEN nofunc(1) ELSE 1 END)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_InsertValues_DeadBranch")]
    [TestCase("INSERT INTO e (id, a) SELECT id, nofunc(a) FROM u WHERE FALSE", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_InsertSelect")]
    [TestCase("INSERT INTO t (id, a) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET a = nofunc(1)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_OnConflictSet")]
    [TestCase("INSERT INTO e (id, a) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET a = 1 WHERE nofunc(1) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_OnConflictWhere")]
    [TestCase("INSERT INTO e (id, a) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET a = 1 WHERE 1", CamusDBErrorCodes.InvalidInput, TestName = "Position_OnConflictWhere_NonBoolean")]
    [TestCase("SELECT id FROM generate_series(nofunc(1), 3) AS g(id) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_TableFunctionArgument")]
    [TestCase("SELECT nofunc(1) WHERE FALSE", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_FromlessWhere")]
    [TestCase("SELECT 5 WHERE nofunc(1) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_FromlessWhere_Condition")]
    [TestCase("SELECT 1 + 'x' WHERE FALSE", CamusDBErrorCodes.InvalidInput, TestName = "Position_FromlessWhere_TypeError")]
    [TestCase("SELECT nofunc(1) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_Fromless_LimitZero")]
    [TestCase("SELECT SUM(nofunc(a)) FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_AggregateArgument")]
    [TestCase("SELECT COUNT(*) FROM e HAVING nofunc(1) = 1", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_Having")]
    [TestCase("SELECT COUNT(*) FROM e GROUP BY nofunc(a)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_GroupBy")]
    [TestCase("SELECT x.id FROM e x JOIN u ON nofunc(x.a) = 1 LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_JoinOn")]
    [TestCase("SELECT id FROM u WHERE a IN (SELECT nofunc(a) FROM e) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_InSubquery")]
    [TestCase("SELECT id FROM u WHERE id IN (SELECT id FROM e WHERE COUNT(*) > 0)", CamusDBErrorCodes.InvalidInput, TestName = "Position_SemiJoinFilter_Aggregate")]
    [TestCase("SELECT id FROM u WHERE a = (SELECT nofunc(a) FROM e) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_ScalarSubquery")]
    [TestCase("SELECT id FROM u WHERE EXISTS (SELECT 1 FROM e WHERE nofunc(e.a) = u.a) LIMIT 0", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_CorrelatedExists")]
    [TestCase("SELECT id FROM u WHERE EXISTS (SELECT 1 FROM e WHERE e.s = 1 AND e.a = u.a) LIMIT 0", CamusDBErrorCodes.InvalidInput, TestName = "Position_CorrelatedExists_TypeError")]
    [TestCase("SELECT SUM(nofunc(a)) OVER () FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_WindowArgument")]
    [TestCase("SELECT row_number() OVER (PARTITION BY nofunc(a)) FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_WindowPartition")]
    [TestCase("SELECT row_number() OVER (ORDER BY nofunc(a)) FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_WindowOrder")]
    [TestCase("UPDATE e SET a = row_number() OVER () WHERE TRUE", CamusDBErrorCodes.InvalidInput, TestName = "Position_WindowCall_InUpdateSet")]
    [TestCase("DELETE FROM e WHERE row_number() OVER () = 1", CamusDBErrorCodes.InvalidInput, TestName = "Position_WindowCall_InDeleteWhere")]
    [TestCase("CREATE VIEW v1 AS SELECT nofunc(a) AS x FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_ViewBody")]
    [TestCase("CREATE VIEW v2 AS SELECT id FROM e WHERE s = 1", CamusDBErrorCodes.InvalidInput, TestName = "Position_ViewBody_TypeError")]
    [TestCase("CREATE TABLE ctas1 AS SELECT nofunc(a) AS x FROM e", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_CreateTableAsSelect")]
    [TestCase("CREATE TABLE c1 (id INT64 PRIMARY KEY, a INT64, CHECK (nofunc(a) > 0))", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_CheckConstraint")]
    [TestCase("CREATE TABLE c2 (id INT64 PRIMARY KEY, a INT64 CHECK (a = 'x'))", CamusDBErrorCodes.InvalidInput, TestName = "Position_CheckConstraint_TypeError")]
    [TestCase("CREATE TABLE c3 (id INT64 PRIMARY KEY, a INT64, CHECK (a))", CamusDBErrorCodes.InvalidInput, TestName = "Position_CheckConstraint_NonBooleanColumn")]
    [TestCase("CREATE TABLE c4 (id INT64 PRIMARY KEY, a INT64, CHECK (1))", CamusDBErrorCodes.InvalidInput, TestName = "Position_CheckConstraint_NonBooleanLiteral")]
    [TestCase("ALTER TABLE e ADD CONSTRAINT c5 CHECK (nofunc(a) > 0)", CamusDBErrorCodes.InvalidAstStmt, TestName = "Position_AddCheckConstraint")]
    [TestCase("ALTER TABLE e ADD CONSTRAINT c6 CHECK (s = 1)", CamusDBErrorCodes.InvalidInput, TestName = "Position_AddCheckConstraint_TypeError")]
    [TestCase("ALTER TABLE e ADD CONSTRAINT c7 CHECK (a)", CamusDBErrorCodes.InvalidInput, TestName = "Position_AddCheckConstraint_NonBoolean")]
    [TestCase("SELECT generate_series(1, 3) FROM e", CamusDBErrorCodes.FeatureNotSupported, TestName = "SetReturningFunction_InSelectList_EmptyTable")]
    [TestCase("SELECT abs() FROM e", CamusDBErrorCodes.InvalidInput, TestName = "WrongArgumentCount_EmptyTable")]
    public async Task EachExpressionPosition_IsChecked(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(code, await Refusal(database, executor, dbname, sql));
    }

    // ── The type of a subquery's column, also when it returns no value ───────────────────────────

    [TestCase("SELECT id FROM t WHERE a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_WrongType")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT s FROM t WHERE id = 1)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_WithValue_WrongType")]
    [TestCase("SELECT id FROM e WHERE a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_OverEmptyTable")]
    [TestCase("SELECT id FROM t WHERE a + (SELECT s FROM e) = 1", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InArithmetic")]
    [TestCase("SELECT id FROM t WHERE (SELECT a FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_AsCondition")]
    [TestCase("SELECT (SELECT s FROM e) + 1 FROM t", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InProjection")]
    [TestCase("SELECT a = (SELECT s FROM e) FROM t LIMIT 0", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InProjection_LimitZero")]
    [TestCase("SELECT 1 = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_FromlessSelect")]
    [TestCase("SELECT x.id FROM t x JOIN u ON x.a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InJoinOn")]
    [TestCase("SELECT a, COUNT(*) FROM t GROUP BY a HAVING a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InHaving")]
    [TestCase("UPDATE t SET a = 1 WHERE a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InUpdateWhere")]
    [TestCase("DELETE FROM t WHERE a = (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "ScalarSubquery_NoRow_InDeleteWhere")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_NoRow_WrongType")]
    [TestCase("SELECT id FROM t WHERE a NOT IN (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "NotInSubquery_NoRow_WrongType")]
    [TestCase("SELECT id FROM t WHERE id IN (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_NoRow_OverPrimaryKey")]
    [TestCase("SELECT id FROM e WHERE a IN (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_NoRow_OverEmptyTable")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT s FROM t WHERE id = 0)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_FilterRemovesEveryRow")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT s AS x FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_NoRow_AliasedColumn")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT y.s FROM e y)", CamusDBErrorCodes.InvalidInput, TestName = "InSubquery_NoRow_QualifiedColumn")]
    [TestCase("SELECT id FROM t WHERE a < ANY (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "QuantifiedSubquery_NoRow_WrongType")]
    [TestCase("SELECT id FROM t WHERE a > ALL (SELECT s FROM e)", CamusDBErrorCodes.InvalidInput, TestName = "QuantifiedAllSubquery_NoRow_WrongType")]
    [TestCase("UPDATE t SET a = (SELECT s FROM e) WHERE TRUE", CamusDBErrorCodes.UnknownType, TestName = "ScalarSubquery_NoRow_AssignedToAColumn")]
    public async Task SubqueryColumnType_IsChecked_WithOrWithoutAValue(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(code, await Refusal(database, executor, dbname, sql));
    }

    [TestCase("SELECT id FROM t WHERE a IN (SELECT d.s FROM (SELECT id, CAST(id AS STRING) AS s FROM e) d)", 0, TestName = "InSubquery_DerivedStringColumn_IsNotKnown")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT a FROM e)", 0, TestName = "ScalarSubquery_NoRow_SameType_IsNull")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT id + 9 FROM u)", 1, TestName = "ScalarSubquery_ExpressionColumn")]
    [TestCase("SELECT id FROM t WHERE n > (SELECT a FROM e)", 0, TestName = "ScalarSubquery_NoRow_NumericPair")]
    [TestCase("SELECT id FROM t WHERE d = (SELECT s FROM e)", 0, TestName = "ScalarSubquery_NoRow_DateWithStringColumn")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT MAX(a) FROM e)", 0, TestName = "ScalarSubquery_Aggregate_TypeIsNotKnown")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT length(s) FROM e)", 0, TestName = "ScalarSubquery_Function_TypeIsNotKnown")]
    [TestCase("SELECT id FROM t WHERE a = (SELECT NULL FROM e)", 0, TestName = "ScalarSubquery_NullLiteral")]
    [TestCase("SELECT id, (SELECT s FROM e) AS x FROM t", 2, TestName = "ScalarSubquery_NoRow_Projected_IsNull")]
    [TestCase("SELECT id FROM t WHERE (SELECT s FROM e) IS NULL", 2, TestName = "ScalarSubquery_NoRow_IsNullTest")]
    [TestCase("SELECT id FROM t WHERE COALESCE((SELECT s FROM e), s) = 'one'", 1, TestName = "ScalarSubquery_NoRow_InCoalesce")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT a FROM e)", 0, TestName = "InSubquery_NoRow_SameType")]
    [TestCase("SELECT id FROM t WHERE a NOT IN (SELECT a FROM e)", 2, TestName = "NotInSubquery_NoRow_SameType")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT n FROM e)", 0, TestName = "InSubquery_NoRow_NumericPair")]
    [TestCase("SELECT id FROM t WHERE s IN (SELECT s FROM e WHERE s = 'x')", 0, TestName = "InSubquery_NoRow_WithFilter")]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT a FROM e UNION ALL SELECT a FROM u)", 1, TestName = "InSubquery_SetOperation")]
    [TestCase("SELECT id FROM t WHERE s IN (SELECT s FROM e UNION ALL SELECT s FROM u)", 1, TestName = "InSubquery_SetOperation_StringColumn")]
    [TestCase("SELECT id FROM t WHERE a < ANY (SELECT a FROM e)", 0, TestName = "QuantifiedSubquery_NoRow_SameType")]
    [TestCase("SELECT id FROM t WHERE a < ALL (SELECT a FROM e)", 2, TestName = "QuantifiedAllSubquery_NoRow_SameType")]
    [TestCase("UPDATE t SET s = (SELECT s FROM e) WHERE TRUE", 2, TestName = "ScalarSubquery_NoRow_AssignedNull")]
    [TestCase("UPDATE t SET n = (SELECT a FROM e) WHERE TRUE", 2, TestName = "ScalarSubquery_NoRow_AssignedWithConversion")]
    public async Task SubqueryColumnType_CorrectStatement_IsAccepted(string sql, int expectedRows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expectedRows, await Run(database, executor, dbname, sql));
    }

    // ── A value of a type the column cannot take ─────────────────────────────────────────────────

    [TestCase("UPDATE e SET a = 'x' WHERE TRUE", TestName = "Assign_StringToInteger_EmptyTable")]
    [TestCase("UPDATE t SET a = 'x' WHERE FALSE", TestName = "Assign_StringToInteger_NoRowMatches")]
    [TestCase("UPDATE t SET a = 'x' WHERE TRUE", TestName = "Assign_StringToInteger_TableWithRows")]
    [TestCase("UPDATE e SET s = 1 WHERE TRUE", TestName = "Assign_IntegerToString_EmptyTable")]
    [TestCase("UPDATE e SET a = 1.5 WHERE TRUE", TestName = "Assign_FloatToInteger_EmptyTable")]
    [TestCase("UPDATE e SET f = 1 WHERE TRUE", TestName = "Assign_IntegerToBool_EmptyTable")]
    [TestCase("UPDATE e SET a = s WHERE TRUE", TestName = "Assign_StringColumnToInteger_EmptyTable")]
    [TestCase("UPDATE e SET a = s || 'x' WHERE TRUE", TestName = "Assign_StringExpressionToInteger_EmptyTable")]
    [TestCase("UPDATE e SET a = CAST(1 AS STRING) WHERE TRUE", TestName = "Assign_CastToInteger_EmptyTable")]
    [TestCase("UPDATE e SET a = CASE WHEN f THEN 'x' ELSE 'y' END WHERE TRUE", TestName = "Assign_CaseToInteger_EmptyTable")]
    [TestCase("UPDATE e SET d = 1 WHERE TRUE", TestName = "Assign_IntegerToDate_EmptyTable")]
    [TestCase("UPDATE e SET a = tags WHERE TRUE", TestName = "Assign_ArrayToInteger_EmptyTable")]
    [TestCase("UPDATE e SET tags = 'x' WHERE TRUE", TestName = "Assign_StringToArray_EmptyTable")]
    [TestCase("UPDATE e SET a = (SELECT s FROM u) WHERE TRUE", TestName = "Assign_SubqueryValue_EmptyTable")]
    [TestCase("UPDATE e SET a = (SELECT s FROM e) WHERE TRUE", TestName = "Assign_SubqueryNoRow_EmptyTable")]
    [TestCase("INSERT INTO e (id, a) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET a = 'x'", TestName = "Assign_OnConflictSet_NoConflict")]
    [TestCase("INSERT INTO e (id, a) SELECT id, 'x' FROM e", TestName = "InsertSelect_LiteralToInteger_NoSourceRow")]
    [TestCase("INSERT INTO e (id, a) SELECT id, 'x' FROM t WHERE FALSE", TestName = "InsertSelect_LiteralToInteger_FilterRemovesEveryRow")]
    [TestCase("INSERT INTO e (id, a) SELECT id, s FROM t LIMIT 0", TestName = "InsertSelect_ColumnToInteger_LimitZero")]
    [TestCase("INSERT INTO e (id, a) SELECT id, s FROM t", TestName = "InsertSelect_ColumnToInteger_WithRows")]
    [TestCase("INSERT INTO e (id, a) SELECT 1, 'x' WHERE FALSE", TestName = "InsertSelect_FromlessWhereFalse")]
    [TestCase("INSERT INTO e SELECT id, s, s, d, f, n, tags FROM t WHERE FALSE", TestName = "InsertSelect_ImplicitTargetColumns")]
    public async Task WrongAssignment_IsRefused_WhateverTheRows(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(CamusDBErrorCodes.UnknownType, await Refusal(database, executor, dbname, sql));
    }

    // These statements return no rows on an empty table, which is a recorded difference from PostgreSQL.
    // A row must still reach the error: the difference is when the mistake shows, never whether it does.
    [TestCase("SELECT d.id FROM (SELECT id, s FROM t) AS d WHERE d.id = d.s", CamusDBErrorCodes.InvalidInput, TestName = "RowReachesIt_DerivedStringColumn")]
    [TestCase("SELECT 9223372036854775807 + 1 FROM t", CamusDBErrorCodes.IntegerValueOutOfRange, TestName = "RowReachesIt_IntegerOverflow")]
    public async Task ADifferenceOnNoRows_StillFails_WhenARowReachesIt(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(code, await Refusal(database, executor, dbname, sql));
    }

    [TestCase("UPDATE e SET d = 'not a date' WHERE TRUE", CamusDBErrorCodes.InvalidInput, TestName = "Assign_BadDateLiteral_EmptyTable")]
    [TestCase("UPDATE e SET n = 'not a number' WHERE TRUE", CamusDBErrorCodes.InvalidInput, TestName = "Assign_BadFloatLiteral_EmptyTable")]
    [TestCase("INSERT INTO e (id, d) SELECT id, 'not a date' FROM e", CamusDBErrorCodes.InvalidInput, TestName = "InsertSelect_BadDateLiteral_NoSourceRow")]
    public async Task BadLiteral_InAssignment_IsRefused(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(code, await Refusal(database, executor, dbname, sql));
    }

    [TestCase("INSERT INTO e (id, a) SELECT id, a FROM (SELECT id, s AS a FROM t WHERE FALSE) x", 0, TestName = "InsertSelect_DerivedStringColumn_IsNotKnown")]
    [TestCase("UPDATE t SET a = NULL WHERE TRUE", 2)]
    [TestCase("UPDATE t SET n = 1 WHERE TRUE", 2, TestName = "Assign_IntegerWidensToFloat")]
    [TestCase("UPDATE t SET n = a WHERE TRUE", 2, TestName = "Assign_IntegerColumnWidensToFloat")]
    [TestCase("UPDATE t SET d = '2026-02-02' WHERE TRUE", 2, TestName = "Assign_StringLiteralToDate")]
    [TestCase("UPDATE t SET n = 'Infinity' WHERE TRUE", 2, TestName = "Assign_FloatTextToFloat")]
    [TestCase("UPDATE t SET a = CAST(s AS INT64) WHERE id < 0", 0, TestName = "Assign_CastConvertsTheColumn")]
    [TestCase("UPDATE t SET s = s || 'x' WHERE TRUE", 2, TestName = "Assign_ConcatToString")]
    [TestCase("UPDATE t SET a = length(s) WHERE TRUE", 2, TestName = "Assign_FunctionResult")]
    [TestCase("UPDATE t SET tags = ARRAY['a', 'b'] WHERE TRUE", 2, TestName = "Assign_ArrayLiteral")]
    [TestCase("UPDATE t SET tags = ARRAY[] WHERE TRUE", 2, TestName = "Assign_EmptyArrayLiteral")]
    [TestCase("UPDATE t SET tags = NULL WHERE TRUE", 2, TestName = "Assign_NullToArray")]
    [TestCase("UPDATE t SET a = CASE WHEN f THEN 1 ELSE NULL END WHERE TRUE", 2, TestName = "Assign_CaseWithNull")]
    [TestCase("INSERT INTO e (id, a) SELECT id, a FROM t", 2, TestName = "InsertSelect_SameType")]
    [TestCase("INSERT INTO e (id, n) SELECT id, a FROM t", 2, TestName = "InsertSelect_IntegerWidensToFloat")]
    [TestCase("INSERT INTO e (id, d) SELECT id, '2026-02-02' FROM t", 2, TestName = "InsertSelect_StringLiteralToDate")]
    [TestCase("INSERT INTO e (id, d) SELECT id, d FROM t", 2, TestName = "InsertSelect_DateToDate")]
    [TestCase("INSERT INTO e SELECT * FROM t", 2, TestName = "InsertSelect_Star")]
    [TestCase("INSERT INTO e (id, a) SELECT id, NULL FROM t", 2, TestName = "InsertSelect_NullLiteral")]
    [TestCase("INSERT INTO e (id, a) SELECT id, length(s) FROM t", 2, TestName = "InsertSelect_FunctionResult")]
    [TestCase("INSERT INTO e (id, a) SELECT id, a FROM t UNION ALL SELECT id + 9, a FROM u", 3, TestName = "InsertSelect_SetOperation")]
    [TestCase("INSERT INTO e (id, s) SELECT x.id, y.s FROM t x JOIN u y ON x.id = y.id", 1, TestName = "InsertSelect_Join")]
    public async Task CorrectAssignment_IsAccepted(string sql, int expectedRows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expectedRows, await Run(database, executor, dbname, sql));
    }

    /// <summary>
    /// The static rule and the write path must agree on every pair of types. Each type is assigned to each
    /// column over a matching row and over no row. A refusal with no row must also be the refusal with a
    /// row. A type refusal with a row must also fail with no row. A value error, such as text that does not
    /// parse, is free to appear with a row only.
    /// </summary>
    [Test]
    public async Task AssignmentRule_AgreesWithTheWritePath_ForEveryPairOfTypes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        (string Column, string Declared, string Value)[] types =
        [
            ("c_id", "object_id", "CAST('507f1f77bcf86cd799439011' AS object_id)"),
            ("c_int", "int64", "CAST(1 AS int64)"),
            ("c_str", "string", "CAST(1 AS string)"),
            ("c_bool", "bool", "CAST('true' AS bool)"),
            ("c_f64", "float64", "CAST(1 AS float64)"),
            ("c_f32", "float32", "CAST(1 AS float32)"),
            ("c_bytes", "bytes", "CAST('a' AS bytes)"),
            ("c_date", "date", "CAST('2026-01-01' AS date)"),
            ("c_dt", "datetime", "CAST('2026-01-01 00:00:00' AS datetime)"),
            ("c_uuid", "uuid", "CAST('b6c7a3c0-5a3f-4d4f-9f64-0a1b2c3d4e5f' AS uuid)"),
            ("c_num", "numeric", "CAST('1.5' AS numeric)"),
            ("c_json", "jsonb", "CAST('1' AS jsonb)"),
            ("c_arr", "array(string)", "ARRAY['x']"),
            ("c_arrn", "array(float64)", "ARRAY[1]"),
        ];

        await Ddl(database, executor, dbname,
            $"CREATE TABLE m (id int64 PRIMARY KEY, {string.Join(", ", types.Select(t => t.Column + " " + t.Declared))})");
        await Write(database, executor, dbname, "INSERT INTO m (id) VALUES (1)");

        List<string> wrong = [];

        foreach ((string column, _, _) in types)
        {
            foreach ((_, string declared, string value) in types)
            {
                string noRow = await Refusal(database, executor, dbname, $"UPDATE m SET {column} = {value} WHERE FALSE");
                string withRow = await Refusal(database, executor, dbname, $"UPDATE m SET {column} = {value} WHERE id = 1");

                if (noRow == CamusDBErrorCodes.UnknownType && withRow != noRow)
                    wrong.Add($"{column} = {declared}: refused with no row, but {withRow} with a row");

                if (withRow == CamusDBErrorCodes.UnknownType && noRow != withRow)
                    wrong.Add($"{column} = {declared}: {noRow} with no row, but refused with a row");
            }
        }

        Assert.IsEmpty(wrong, string.Join(Environment.NewLine, wrong));
    }

    /// <summary>
    /// <see cref="CastScalarFunctions.HasImplicitConversion"/> must list exactly the pairs that
    /// <see cref="CastScalarFunctions.CoerceToColumnType(ColumnValue, ColumnType)"/> converts.
    /// </summary>
    [Test]
    public void ImplicitConversionList_MatchesTheCoercion()
    {
        ColumnValue[] samples =
        [
            new ColumnValue(ColumnType.Id, "507f1f77bcf86cd799439011"),
            new ColumnValue(ColumnType.Integer64, 1L),
            new ColumnValue(ColumnType.String, "1"),
            ColumnValue.FromBool(true),
            new ColumnValue(ColumnType.Float64, 1.0),
            new ColumnValue(ColumnType.Float32, 1.0),
            new ColumnValue(new byte[] { 1 }),
            new ColumnValue(ColumnType.Date, 0L),
            new ColumnValue(ColumnType.DateTime, 0L),
            ColumnValue.FromNumeric(1),
        ];

        List<string> wrong = [];

        foreach (ColumnType target in Enum.GetValues<ColumnType>())
        {
            foreach (ColumnValue sample in samples)
            {
                if (target == sample.Type || target == ColumnType.Null)
                    continue;

                bool converts;
                try
                {
                    converts = !ReferenceEquals(CastScalarFunctions.CoerceToColumnType(sample, target), sample);
                }
                catch (CamusDBException)
                {
                    // A conversion that runs and refuses this sample's text is still a conversion.
                    converts = true;
                }

                if (converts != CastScalarFunctions.HasImplicitConversion(sample.Type, target))
                    wrong.Add($"{sample.Type} to {target}: coercion {converts}");
            }
        }

        Assert.IsEmpty(wrong, string.Join(Environment.NewLine, wrong));
    }

    // ── The other side: a correct statement must stay correct ───────────────────────────────────

    [TestCase("SELECT id FROM t WHERE TRUE", 2)]
    [TestCase("SELECT id FROM t WHERE FALSE", 0)]
    [TestCase("SELECT id FROM t WHERE NULL", 0)]
    [TestCase("SELECT id FROM t WHERE f", 1)]
    [TestCase("SELECT id FROM t WHERE NOT f", 1)]
    [TestCase("SELECT id FROM t WHERE f IS TRUE", 1)]
    [TestCase("SELECT id FROM t WHERE NULL AND TRUE", 0)]
    [TestCase("SELECT id FROM t WHERE a = NULL", 0)]
    [TestCase("SELECT id FROM t WHERE NULL = 'x'", 0)]
    [TestCase("SELECT id FROM t WHERE a = 10.0", 1)]
    [TestCase("SELECT id FROM t WHERE n = 1", 0)]
    [TestCase("SELECT id FROM t WHERE n > a", 0)]
    [TestCase("SELECT id FROM t WHERE d = '2026-01-01'", 1)]
    [TestCase("SELECT id FROM t WHERE d BETWEEN '2026-01-01' AND '2026-01-31'", 2)]
    [TestCase("SELECT id FROM t WHERE d IN ('2026-01-01', '2026-01-02')", 2)]
    [TestCase("SELECT id FROM e WHERE s = d", 0, TestName = "StringColumnWithDate_HasAComparisonRule")]
    [TestCase("SELECT id FROM t WHERE s LIKE 'o%'", 1)]
    [TestCase("SELECT id FROM t WHERE s ~ '^o'", 1)]
    [TestCase("SELECT id FROM t WHERE upper(s) = 'ONE'", 1)]
    [TestCase("SELECT id FROM t WHERE COALESCE(a, 0) = 10", 1)]
    [TestCase("SELECT id FROM t WHERE COALESCE(s, 'x') = 'one'", 1)]
    [TestCase("SELECT id FROM t WHERE NULLIF(a, 10) IS NULL", 1)]
    [TestCase("SELECT id FROM t WHERE NULLIF(a, NULL) = 10", 1)]
    [TestCase("SELECT id FROM t WHERE (a + 1) * 2 > 1", 2)]
    [TestCase("SELECT id FROM t WHERE (s || 'x') = 'onex'", 1)]
    [TestCase("SELECT id FROM t WHERE (s || 1) = 'one1'", 1)]
    [TestCase("SELECT id FROM t WHERE (a & 2) = 2", 1)]
    [TestCase("SELECT id FROM t WHERE tags[1] = 'red'", 1)]
    [TestCase("SELECT id FROM t WHERE tags = ARRAY['red']", 1)]
    [TestCase("SELECT id FROM t WHERE tags = ARRAY[]", 0)]
    [TestCase("SELECT id FROM t WHERE 'red' = ANY (tags)", 1)]
    [TestCase("SELECT id FROM t WHERE a < ANY (ARRAY[15])", 1)]
    [TestCase("SELECT id FROM t WHERE CASE WHEN f THEN TRUE ELSE FALSE END", 1)]
    [TestCase("SELECT id FROM t WHERE CASE a WHEN 10 THEN TRUE ELSE NULL END", 1)]
    [TestCase("SELECT id FROM t WHERE IF(f, TRUE, FALSE)", 1)]
    [TestCase("SELECT id FROM t WHERE COALESCE(f, FALSE)", 1)]
    [TestCase("SELECT id FROM t WHERE CAST(a AS STRING) = '10'", 1)]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT a FROM u)", 1)]
    [TestCase("SELECT id FROM t WHERE a = (SELECT a FROM u)", 1)]
    [TestCase("SELECT id FROM t WHERE a IN (SELECT a FROM e)", 0)]
    [TestCase("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)", 1)]
    [TestCase("SELECT id FROM t WHERE (SELECT f FROM u)", 2)]
    [TestCase("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u)", 2)]
    [TestCase("SELECT id, a + 1 AS total FROM t ORDER BY total", 2)]
    [TestCase("SELECT id, s AS a FROM t ORDER BY a", 2, TestName = "OrderByAlias_OutranksAColumnOfAnotherType")]
    [TestCase("SELECT a, COUNT(*) AS c FROM t GROUP BY a HAVING c > 0", 2)]
    [TestCase("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 0", 2)]
    [TestCase("SELECT a, f AS big FROM t GROUP BY a, f HAVING big", 1, TestName = "HavingAlias_OfABooleanColumn")]
    [TestCase("SELECT x.id FROM t x JOIN u ON x.a = u.a", 1)]
    [TestCase("SELECT x.id FROM t x JOIN u ON TRUE", 2)]
    [TestCase("SELECT x.id FROM t x LEFT JOIN u ON x.f", 2)]
    [TestCase("SELECT d.a FROM (SELECT a FROM t) d WHERE d.a = 10", 1)]
    [TestCase("SELECT d.a FROM (SELECT s AS a FROM t) d WHERE d.a = 'one'", 1, TestName = "DerivedColumn_OfStringType_ComparesWithString")]
    [TestCase("SELECT x.a FROM (SELECT a FROM t) x JOIN (SELECT s AS a FROM t) y ON y.a = 'one' WHERE x.a = 10", 1, TestName = "TwoDerivedTables_SameColumnName_ReadsTheNamedTable")]
    [TestCase("SELECT v FROM (SELECT id AS v FROM t UNION ALL SELECT a FROM t) d WHERE v > 0", 4)]
    [TestCase("SELECT r FROM (SELECT row_number() OVER (ORDER BY id) AS r FROM t) d WHERE r = 1", 1)]
    [TestCase("SELECT value FROM generate_series(1, 3) AS g(value) WHERE value = 2", 1)]
    [TestCase("SELECT abs(a), length(s), round(n) FROM t", 2)]
    [TestCase("SELECT ABS(a), Upper(s) FROM t", 2, TestName = "FunctionName_IsCaseInsensitive")]
    [TestCase("SELECT EXTRACT(YEAR FROM d) FROM t", 2)]
    [TestCase("SELECT COUNT(*), MAX(a), json_agg(a) FROM t", 1)]
    [TestCase("CREATE TABLE ok1 (id INT64 PRIMARY KEY, a INT64 CHECK (a > 0), s STRING, f BOOL, CHECK (f), CHECK (length(s) > 1), CHECK (s LIKE 'x%'), CHECK (a IS NOT NULL))", 0, TestName = "CheckConstraint_Correct_IsAccepted")]
    [TestCase("ALTER TABLE e ADD CONSTRAINT ok2 CHECK (a > 0 AND s <> 'x')", 0, TestName = "AddCheckConstraint_Correct_IsAccepted")]
    [TestCase("SELECT 5 WHERE TRUE", 1)]
    [TestCase("SELECT 5 WHERE NULL", 0)]
    [TestCase("SELECT 5 WHERE NOT FALSE", 1)]
    [TestCase("UPDATE t SET a = a + 1 WHERE f", 1)]
    [TestCase("UPDATE t SET a = CAST(s AS INT64) WHERE id < 0", 0)]
    [TestCase("DELETE FROM t WHERE f", 1)]
    [TestCase("DELETE FROM t WHERE COALESCE(f, FALSE)", 1)]
    public async Task CorrectStatement_IsAccepted(string sql, int expectedRows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expectedRows, await Run(database, executor, dbname, sql));
    }

    /// <summary>
    /// The parse cache shares one tree for one SQL text, so a check must read the types of the schema it
    /// runs against. The same text is correct on one database and wrong on the other, in both orders.
    /// </summary>
    [Test]
    public async Task SameText_OnTwoSchemas_IsCheckedAgainstEach()
    {
        (string dbA, DatabaseDescriptor databaseA, CommandExecutor executor) = await CreateDatabase();
        (string dbB, DatabaseDescriptor databaseB, CommandExecutor executorB) = await CreateDatabase();

        await Ddl(databaseA, executor, dbA, "CREATE TABLE z (id INT64 PRIMARY KEY, c INT64)");
        await Ddl(databaseB, executorB, dbB, "CREATE TABLE z (id INT64 PRIMARY KEY, c STRING)");

        const string numeric = "SELECT id FROM z WHERE c = 1";
        const string text = "SELECT id FROM z WHERE c = 'x'";

        for (int i = 0; i < 2; i++)
        {
            Assert.AreEqual(0, await Run(databaseA, executor, dbA, numeric));
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(databaseB, executorB, dbB, numeric));
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(databaseA, executor, dbA, text));
            Assert.AreEqual(0, await Run(databaseB, executorB, dbB, text));
        }
    }

    /// <summary>A refusal runs on each execution: a cached parse or binding of the same text does not skip it.</summary>
    [Test]
    public async Task RepeatedStatement_IsRefusedEachTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        for (int i = 0; i < 3; i++)
        {
            Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await Refusal(database, executor, dbname, "SELECT nofunc(a) FROM e"));
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, "SELECT id FROM e WHERE 1"));
        }
    }

    /// <summary>
    /// The type check walks a chain of operators once, so a statement near the depth limit binds without
    /// a quadratic cost and without a stack overflow, which would end the process.
    /// </summary>
    [Test]
    public async Task LongOperatorChain_BindsAndRuns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string chain = "a" + string.Concat(Enumerable.Repeat(" + 1", 900));

        Assert.AreEqual(2, await Run(database, executor, dbname, $"SELECT id FROM t WHERE {chain} > 0"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, await Refusal(database, executor, dbname, $"SELECT id FROM e WHERE {chain} = 'x'"));
    }
}
