/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A comparison between two types with no comparison rule is an error, never a silent FALSE.
///
/// <para>Before, <c>5 = '5'</c> and <c>0.0 = '0.0'</c> were FALSE, and <c>DELETE FROM t WHERE s = a</c>
/// (a STRING column against an INT64 column) reported 0 rows changed. A type mistake in a predicate
/// then looks like a valid answer. The rule is GoogleSQL's: every comparison operator, <c>IN</c>, the
/// simple CASE, NULLIF, <c>= ANY</c> and <c>array_contains</c> raise
/// <see cref="CamusDBErrorCodes.InvalidInput"/>, and the user writes a CAST. Pairs that do have a rule
/// keep it: mixed numerics, a string against a date, datetime, uuid, object id or jsonb, and a NULL
/// operand (UNKNOWN, no error).</para>
///
/// <para>The planner must not answer such a predicate from an index: a seek key built from a constant
/// of the wrong type finds nothing, and the seek would return no rows where a scan raises the error.
/// The seek tests check the plan and the error on a table large enough for the index to win.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIncomparableTypeComparison : BaseTest
{
    private static readonly string[] Operators = ["=", "<>", "!=", "<", ">", "<=", ">="];

    // Pairs of typed literals with no comparison rule, written both ways round by the tests.
    private static readonly (string Left, string Right)[] IncomparablePairs =
    [
        ("5", "'5'"),
        ("0.0", "'0.0'"),
        ("NUMERIC '1.5'", "'1.5'"),
        ("true", "1"),
        ("true", "'true'"),
        ("CAST('0x61' AS BYTES)", "'a'"),
        ("CAST('0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b' AS UUID)", "5"),
        ("CAST('2026-01-01' AS DATE)", "5"),
    ];

    // ─── Scalar operators ─────────────────────────────────────────────────────

    [Test]
    public async Task EveryComparisonOperator_OnAnIncomparablePair_IsInvalidInput_InBothOrders()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        foreach ((string left, string right) in IncomparablePairs)
        {
            foreach (string op in Operators)
            {
                AssertNoRule(() => Query(executor, db, $"SELECT {left} {op} {right}", null), $"{left} {op} {right}");
                AssertNoRule(() => Query(executor, db, $"SELECT {right} {op} {left}", null), $"{right} {op} {left}");
            }

            AssertNoRule(() => Query(executor, db, $"SELECT {left} BETWEEN {right} AND {right}", null), $"{left} BETWEEN");
        }
    }

    [Test]
    public async Task TheErrorNamesTheOperatorAndBothTypes()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT 5 = '5'", null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.AreEqual("No matching signature for operator = for argument types: Integer64, String", ex.Message);
    }

    [TestCase("SELECT 1 = 1.0", true)]
    [TestCase("SELECT 2 <> NUMERIC '2.00'", false)]
    [TestCase("SELECT NUMERIC '1.5' = 1.5", true)]
    [TestCase("SELECT CAST(1.5 AS FLOAT32) = NUMERIC '1.5'", true)]
    [TestCase("SELECT CAST('2026-01-01' AS DATE) = '2026-01-01'", true)]
    [TestCase("SELECT CAST('2026-01-01' AS DATE) = CAST('2026-01-01T00:00:00Z' AS DATETIME)", true)]
    [TestCase("SELECT CAST('0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b' AS UUID) = '0190A1B2-C3D4-7E5F-8A9B-0C1D2E3F4A5B'", true)]
    [TestCase("SELECT CAST('0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b' AS UUID) = 'not a uuid'", false)]
    [TestCase("SELECT CAST('7' AS INT64) = 7", true)]
    [TestCase("SELECT 5 = CAST('5' AS INT64)", true)]
    [TestCase("SELECT CAST(5 AS STRING) = '5'", true)]
    public async Task PairsWithAComparisonRule_KeepIt(string sql, bool expected)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await Scalar(executor, db, sql);
        Assert.AreEqual(ColumnType.Bool, value.Type, sql);
        Assert.AreEqual(expected, value.BoolValue, sql);
    }

    [TestCase("SELECT NULL = '5'")]
    [TestCase("SELECT 5 = NULL")]
    [TestCase("SELECT NULL <> true")]
    [TestCase("SELECT NULL IN (1, 'a')")]
    [TestCase("SELECT CASE NULL WHEN 'a' THEN 1 END")]
    public async Task ANullOperand_IsUnknown_NotAnError(string sql)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await Scalar(executor, db, sql)).Type, sql);
    }

    // ─── DML ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task DeleteAndUpdate_ComparingAStringColumnWithAnIntegerColumn_FailAndChangeNothing()
    {
        (string db, CommandExecutor executor) = await SeedMixed(10, indexed: false);

        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE s = a", null), "DELETE s = a");
        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE a = s", null), "DELETE a = s");
        AssertNoRule(() => NonQuery(executor, db, "UPDATE mixed SET b = true WHERE s = a", null), "UPDATE s = a");
        AssertNoRule(() => NonQuery(executor, db, "UPDATE mixed SET b = true WHERE a <> s", null), "UPDATE a <> s");
        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE b = 1", null), "DELETE bool = int");

        Assert.AreEqual(10, await Count(executor, db, "mixed", ""), "a refused statement deletes no row");
        Assert.AreEqual(0, await Count(executor, db, "mixed", "WHERE b = true"), "a refused statement updates no row");

        // The comparison the user meant, written with a CAST, works.
        Assert.AreEqual(10, await Count(executor, db, "mixed", "WHERE CAST(s AS INT64) = a"));
        await NonQuery(executor, db, "DELETE FROM mixed WHERE CAST(s AS INT64) = a", null);
        Assert.AreEqual(0, await Count(executor, db, "mixed", ""));
    }

    [Test]
    public async Task APredicateOverAnEmptyTable_IsChecked_WhenTheStatementShowsBothTypes()
    {
        (string db, CommandExecutor executor) = await SeedMixed(0, indexed: false);

        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE s = a", null), "DELETE");
        AssertNoRule(() => Count(executor, db, "mixed", "WHERE s = a"), "SELECT");

        // A parameter has a type that only its value shows, and a predicate over no rows reads no value.
        Dictionary<string, ColumnValue> stringParameter = new() { ["@p"] = new ColumnValue(ColumnType.String, "3") };
        Assert.AreEqual(0, await Count(executor, db, "mixed", "WHERE a = @p", stringParameter));
    }

    [Test]
    public async Task ATypedParameterOfTheWrongType_IsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(10, indexed: false);

        Dictionary<string, ColumnValue> stringParameter = new() { ["@p"] = new ColumnValue(ColumnType.String, "3") };
        AssertNoRule(() => Count(executor, db, "mixed", "WHERE a = @p", stringParameter), "a = @p");
        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE a = @p", stringParameter), "DELETE a = @p");

        Dictionary<string, ColumnValue> integerParameter = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 3) };
        Assert.AreEqual(1, await Count(executor, db, "mixed", "WHERE a = @p", integerParameter));
        Assert.AreEqual(10, await Count(executor, db, "mixed", ""));
    }

    // ─── IN, CASE, NULLIF, ANY, array_contains ────────────────────────────────

    [TestCase("SELECT 5 IN (1, 'a')")]
    [TestCase("SELECT 1 IN (1, 'a')")]
    [TestCase("SELECT 1 IN ('a', 1)")]
    [TestCase("SELECT 1 NOT IN (2, 'a')")]
    [TestCase("SELECT 1 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 'a')")]
    [TestCase("SELECT 'a' IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)")]
    [TestCase("SELECT true IN (1, 2)")]
    [TestCase("SELECT CASE 5 WHEN 'a' THEN 1 ELSE 0 END")]
    [TestCase("SELECT CASE 5 WHEN 6 THEN 1 WHEN 'a' THEN 2 ELSE 0 END")]
    [TestCase("SELECT NULLIF(5, '5')")]
    [TestCase("SELECT 5 = ANY (ARRAY['a', 'b'])")]
    [TestCase("SELECT array_contains(ARRAY[1, 2], 'a')")]
    public async Task MembershipAndCaseForms_OnAnIncomparablePair_AreInvalidInput(string sql)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        AssertNoRule(() => Query(executor, db, sql, null), sql);
    }

    [Test]
    public async Task SimpleCase_StopsAtTheFirstMatch_AsBefore()
    {
        // The first matching WHEN wins and later WHENs are not evaluated, so a later WHEN whose type
        // only its value shows cannot fail a row that an earlier WHEN matched. A later WHEN whose type
        // the statement shows fails before any row is read, as in PostgreSQL.
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        Dictionary<string, ColumnValue> stringParameter = new() { ["@p"] = new ColumnValue(ColumnType.String, "a") };
        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT CASE 5 WHEN 5 THEN 1 WHEN @p THEN 2 END AS c", stringParameter);
        Assert.AreEqual(1L, rows[0]["c"].LongValue);

        AssertNoRule(() => Query(executor, db, "SELECT CASE 5 WHEN 5 THEN 1 WHEN 'a' THEN 2 END", null), "literal WHEN");
    }

    [Test]
    public async Task InOverAColumn_OnTheScanAndThePreparedSetPaths_IsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(30, indexed: false);

        string shortList = "(1, 'x')";
        string longList = "(" + string.Join(", ", Enumerable.Range(1, 12)) + ", 'x')";

        foreach (string list in new[] { shortList, longList })
        {
            AssertNoRule(() => Count(executor, db, "mixed", $"WHERE a IN {list}"), $"IN {list}");
            AssertNoRule(() => Count(executor, db, "mixed", $"WHERE a NOT IN {list}"), $"NOT IN {list}");
            // Not at the top of the WHERE clause: the list is evaluated through the memo, not the planner.
            AssertNoRule(() => Count(executor, db, "mixed", $"WHERE b OR a IN {list}"), $"OR IN {list}");
            AssertNoRule(() => NonQuery(executor, db, $"DELETE FROM mixed WHERE a IN {list}", null), $"DELETE IN {list}");
        }

        AssertNoRule(() => Count(executor, db, "mixed", "WHERE a IN (SELECT s FROM mixed)"), "IN subquery");
        Assert.AreEqual(30, await Count(executor, db, "mixed", ""));
    }

    // ─── Joins and HAVING ─────────────────────────────────────────────────────

    [Test]
    public async Task JoinOnAnIncomparablePair_IsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(10, indexed: true);

        AssertNoRule(() => Query(executor, db, "SELECT x.id FROM mixed x JOIN mixed y ON x.a = y.s", null), "JOIN ON a = s");
        AssertNoRule(() => Query(executor, db, "SELECT x.id FROM mixed x LEFT JOIN mixed y ON y.s = x.a", null), "LEFT JOIN ON s = a");
        AssertNoRule(() => Query(executor, db, "SELECT x.id FROM mixed x, mixed y WHERE x.a = y.s", null), "comma join");

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT x.id FROM mixed x JOIN mixed y ON x.a = CAST(y.s AS INT64)", null);
        Assert.AreEqual(10, rows.Count);
    }

    [Test]
    public async Task HavingOnAnIncomparablePair_IsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(10, indexed: false);

        AssertNoRule(() => Query(executor, db, "SELECT s, COUNT(*) AS c FROM mixed GROUP BY s HAVING COUNT(*) = 'x'", null), "HAVING");
    }

    // ─── Index seek against table scan ────────────────────────────────────────

    [Test]
    public async Task AnIndexedColumn_ComparedWithAConstantOfTheWrongType_IsNotSeek_AndIsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(200, indexed: true);

        // The index is usable, and the planner takes it for a constant of the column's type.
        Assert.IsTrue(await UsesIndex(executor, db, "SELECT id FROM mixed WHERE a = 7"), "a = 7 seeks the index");
        Assert.IsTrue(await UsesIndex(executor, db, "SELECT id FROM mixed WHERE a IN (7, 8)"), "a IN (7, 8) seeks the index");
        Assert.AreEqual(1, await Count(executor, db, "mixed", "WHERE a = 7"));

        foreach (string where in new[]
        {
            "WHERE a = 'x'", "WHERE a = '7'", "WHERE 'x' = a", "WHERE a <> 'x'", "WHERE a > 'x'",
            "WHERE a IN ('x')", "WHERE a IN (7, 'x')", "WHERE a IN (7, 8, 9, 10, 11, 12, 13, 14, 15, 'x')",
            "WHERE a = true", "WHERE s = 7", "WHERE s IN (7, 8)",
        })
        {
            // EXPLAIN binds the statement, so it is refused before a plan exists.
            AssertNoRule(() => UsesIndex(executor, db, $"SELECT id FROM mixed {where}"), $"EXPLAIN {where}");
            AssertNoRule(() => Count(executor, db, "mixed", where), where);
            AssertNoRule(() => NonQuery(executor, db, $"DELETE FROM mixed {where}", null), $"DELETE {where}");
        }

        // A parameter reaches the planner with its value, so the planner must not seek on it.
        Dictionary<string, ColumnValue> stringParameter = new() { ["@p"] = new ColumnValue(ColumnType.String, "7") };
        AssertNoRule(() => Count(executor, db, "mixed", "WHERE a = @p", stringParameter), "a = @p");
        AssertNoRule(() => Count(executor, db, "mixed", "WHERE a IN (7, @p)", stringParameter), "a IN (7, @p)");

        Assert.AreEqual(200, await Count(executor, db, "mixed", ""));
    }

    [Test]
    public async Task APrimaryKeyLookup_WithAConstantOfTheWrongType_IsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SeedMixed(50, indexed: false);

        AssertNoRule(() => Count(executor, db, "mixed", "WHERE id = 'x'"), "id = 'x'");
        AssertNoRule(() => NonQuery(executor, db, "UPDATE mixed SET b = true WHERE id = '3'", null), "UPDATE id = '3'");
        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM mixed WHERE id IN ('3', 4)", null), "DELETE id IN");

        Assert.AreEqual(50, await Count(executor, db, "mixed", ""));
        Assert.AreEqual(0, await Count(executor, db, "mixed", "WHERE b = true"));
    }

    // ─── Arrays ───────────────────────────────────────────────────────────────

    [Test]
    public async Task EveryComparisonOperator_OnArraysWithDifferentElementTypes_IsInvalidInput_InBothOrders()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        foreach ((string left, string right) in new[] { ("ARRAY[1, 2]", "ARRAY['a']"), ("ARRAY['a']", "ARRAY[1, 2]"), ("ARRAY[true]", "ARRAY[1.5]"), ("ARRAY[1]", "ARRAY[1.0]") })
        {
            foreach (string op in Operators)
                AssertNoRule(() => Query(executor, db, $"SELECT {left} {op} {right}", null), $"{left} {op} {right}");
        }
    }

    [Test]
    public async Task TheArrayErrorNamesBothElementTypes()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT ARRAY[1, 2] = ARRAY['a']", null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.AreEqual("No matching signature for operator = for argument types: ARRAY<Integer64>, ARRAY<String>", ex.Message);
    }

    [TestCase("SELECT ARRAY[1, 2] = ARRAY[1, 2]", true)]
    [TestCase("SELECT ARRAY[1, 2] < ARRAY[1, 3]", true)]
    [TestCase("SELECT ARRAY['a'] <> ARRAY['b']", true)]
    [TestCase("SELECT ARRAY[] = ARRAY[1]", false)]
    [TestCase("SELECT ARRAY['a'] > ARRAY[]", true)]
    public async Task ArraysWithOneElementType_OrAnUnknownOne_KeepTheirRule(string sql, bool expected)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await Scalar(executor, db, sql);
        Assert.AreEqual(ColumnType.Bool, value.Type, sql);
        Assert.AreEqual(expected, value.BoolValue, sql);
    }

    [TestCase("SELECT ARRAY[1] IN (ARRAY['a'])")]
    [TestCase("SELECT ARRAY[1] IN (ARRAY[1], ARRAY['a'])")]
    [TestCase("SELECT ARRAY[1] NOT IN (ARRAY[2], ARRAY['a'])")]
    [TestCase("SELECT ARRAY[1] IN (ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[4], ARRAY[5], ARRAY[6], ARRAY[7], ARRAY[8], ARRAY[9], ARRAY['a'])")]
    [TestCase("SELECT ARRAY['a'] IN (ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[4], ARRAY[5], ARRAY[6], ARRAY[7], ARRAY[8], ARRAY[9])")]
    [TestCase("SELECT CASE ARRAY[1] WHEN ARRAY['a'] THEN 1 ELSE 0 END")]
    [TestCase("SELECT NULLIF(ARRAY[1], ARRAY['a'])")]
    [TestCase("SELECT array_contains(ARRAY[1], ARRAY[1])")]
    public async Task MembershipAndCaseForms_OnArraysWithDifferentElementTypes_AreInvalidInput(string sql)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        AssertNoRule(() => Query(executor, db, sql, null), sql);
    }

    [Test]
    public async Task ArrayMembership_WithOneElementType_StillMatches_OnBothInPaths()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        string longList = string.Join(", ", Enumerable.Range(1, 12).Select(i => $"ARRAY[{i}]"));

        Assert.IsTrue((await Scalar(executor, db, "SELECT ARRAY[2] IN (ARRAY[1], ARRAY[2])")).BoolValue);
        Assert.IsTrue((await Scalar(executor, db, $"SELECT ARRAY[7] IN ({longList})")).BoolValue);
        Assert.IsFalse((await Scalar(executor, db, $"SELECT ARRAY[70] IN ({longList})")).BoolValue);
        Assert.IsFalse((await Scalar(executor, db, $"SELECT ARRAY[] IN ({longList})")).BoolValue);
    }

    [Test]
    public async Task ArrayColumns_WithDifferentElementTypes_FailInWhereDeleteAndJoins_AndChangeNothing()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, db, "CREATE TABLE arrs (id int64 PRIMARY KEY NOT NULL, ints array(int64) NULL, strs array(string) NULL)");
        await NonQuery(executor, db,
            "INSERT INTO arrs (id, ints, strs) VALUES (1, ARRAY[1], ARRAY['1']), (2, ARRAY[2], ARRAY['2']), (3, ARRAY[3], ARRAY['3'])", null);

        AssertNoRule(() => Count(executor, db, "arrs", "WHERE ints = strs"), "WHERE ints = strs");
        AssertNoRule(() => Count(executor, db, "arrs", "WHERE ints IN (ARRAY['1'])"), "WHERE ints IN (ARRAY['1'])");
        AssertNoRule(() => NonQuery(executor, db, "DELETE FROM arrs WHERE ints = strs", null), "DELETE");
        AssertNoRule(() => NonQuery(executor, db, "UPDATE arrs SET ints = ARRAY[0] WHERE strs <> ints", null), "UPDATE");
        AssertNoRule(() => Query(executor, db, "SELECT x.id FROM arrs x JOIN arrs y ON x.ints = y.strs", null), "JOIN");

        // Derived columns: their declared type is Array on both sides, so only the element types
        // differ. The join must not become a hash key that calls every pair unequal.
        AssertNoRule(() => Query(executor, db,
            "SELECT x.id FROM (SELECT id, ints FROM arrs) x JOIN (SELECT id, strs FROM arrs) y ON x.ints = y.strs", null),
            "JOIN over derived tables");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db,
            "SELECT x.id FROM (SELECT id, ints FROM arrs) x JOIN (SELECT id, ints FROM arrs) y ON x.ints = y.ints", null);
        Assert.AreEqual(3, rows.Count, "a derived join on arrays of one element type still matches");

        Assert.AreEqual(3, await Count(executor, db, "arrs", "WHERE ints = ARRAY[0] OR ints <> ARRAY[0]"));
    }

    [TestCase("SELECT ARRAY[1] AS v UNION SELECT ARRAY['a'] AS v")]
    [TestCase("SELECT ARRAY['a'] AS v UNION ALL SELECT ARRAY[1] AS v")]
    [TestCase("SELECT ARRAY[1] AS v INTERSECT SELECT ARRAY['a'] AS v")]
    [TestCase("SELECT ARRAY[1] AS v EXCEPT SELECT ARRAY[true] AS v")]
    [TestCase("SELECT v FROM (SELECT ARRAY[1] AS v UNION ALL SELECT ARRAY['a'] AS v) d ORDER BY v")]
    [TestCase("SELECT MIN(v) FROM (SELECT ARRAY[1] AS v UNION ALL SELECT ARRAY['a'] AS v) d")]
    public async Task SetOperations_OverArraysWithDifferentElementTypes_AreInvalidInput(string sql)
    {
        // Before, the UNION accepted the pair, an INTERSECT returned no rows, and an ORDER BY or MIN
        // over the result failed inside the sort with a .NET exception.
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, sql, null), sql)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{sql}: {ex.Message}");
        StringAssert.Contains("cannot be combined", ex.Message, sql);
    }

    [Test]
    public async Task SetOperations_OverArraysWithOneElementType_OrAnEmptyArray_StillRun()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(2, (await Query(executor, db, "SELECT v FROM (SELECT ARRAY[2] AS v UNION SELECT ARRAY[1] AS v) d ORDER BY v", null)).Count);
        Assert.AreEqual(1, (await Query(executor, db, "SELECT ARRAY[1] AS v INTERSECT SELECT ARRAY[1] AS v", null)).Count);
        Assert.AreEqual(2, (await Query(executor, db, "SELECT ARRAY[1] AS v UNION ALL SELECT ARRAY[] AS v", null)).Count);
    }

    // ─── Transports ───────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_DeleteWithAnIncomparablePredicate_IsInvalidInput_AndDeletesNothing()
    {
        (CommandExecutor executor, HttpTransactionCoordinator coordinator) = ServiceExecutor();
        PreparedStatementRegistry registry = new(Options);

        try
        {
            ExecuteSQLController Sql(object body) =>
                new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(body) };

            string db = "db" + Guid.NewGuid().ToString("n");
            await Sql(new { sql = $"CREATE DATABASE {db}" }).ExecuteSQLDDL();
            await Sql(new { databaseName = db, sql = "CREATE TABLE mixed (id int64 PRIMARY KEY NOT NULL, s string NULL, a int64 NULL)" }).ExecuteSQLDDL();
            await Sql(new { databaseName = db, sql = "INSERT INTO mixed (id, s, a) VALUES (1, '1', 1), (2, '2', 2)" }).ExecuteNonSQLQuery();

            JsonResult delete = await Sql(new { databaseName = db, sql = "DELETE FROM mixed WHERE s = a" }).ExecuteNonSQLQuery();
            ExecuteNonSQLQueryResponse deleteResponse = (ExecuteNonSQLQueryResponse)delete.Value!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, deleteResponse.Code, deleteResponse.Message);
            StringAssert.Contains("No matching signature for operator =", deleteResponse.Message);

            JsonResult select = await Sql(new { databaseName = db, sql = "SELECT 5 = '5'" }).ExecuteSQLQuery();
            ExecuteSQLQueryResponse selectResponse = (ExecuteSQLQueryResponse)select.Value!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, selectResponse.Code, selectResponse.Message);

            Assert.AreEqual(2, await Count(executor, db, "mixed", ""), "the refused DELETE removed no row");
        }
        finally
        {
            await executor.DisposeAsync();
        }
    }

    [Test]
    public async Task Grpc_DeleteWithAnIncomparablePredicate_IsInvalidInput_AndDeletesNothing()
    {
        (CommandExecutor executor, HttpTransactionCoordinator coordinator) = ServiceExecutor();

        try
        {
            CamusSqlService service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

            string db = "db" + Guid.NewGuid().ToString("n");
            await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
            await service.ExecuteDdl(new SqlRequest { Database = db, Sql = "CREATE TABLE mixed (id int64 PRIMARY KEY NOT NULL, s string NULL, a int64 NULL)" }, new TestServerCallContext());

            ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
            ObservingStreamWriter<BatchExecuteResponse> writer = new();
            Task server = service.BatchExecute(reader, writer, new TestServerCallContext());

            reader.Push(new BatchExecuteRequest
            {
                RequestId = 1,
                Kind = BatchStatementKind.NonQuery,
                Request = new SqlRequest { Database = db, Sql = "INSERT INTO mixed (id, s, a) VALUES (1, '1', 1), (2, '2', 2)" },
            });
            Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, (await Terminal(writer, 1)).PayloadCase);

            reader.Push(new BatchExecuteRequest
            {
                RequestId = 2,
                Kind = BatchStatementKind.NonQuery,
                Request = new SqlRequest { Database = db, Sql = "DELETE FROM mixed WHERE s = a" },
            });
            BatchExecuteResponse deleteReply = await Terminal(writer, 2);
            Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, deleteReply.PayloadCase);
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, deleteReply.Error.Code, deleteReply.Error.Message);
            StringAssert.Contains("No matching signature for operator =", deleteReply.Error.Message);

            reader.Complete();
            await server;

            Assert.AreEqual(2, await Count(executor, db, "mixed", ""), "the refused DELETE removed no row");
        }
        finally
        {
            await executor.DisposeAsync();
        }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static void AssertNoRule(Func<Task> statement, string label)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await statement(), label)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{label}: {ex.Message}");
        StringAssert.Contains("No matching signature for operator", ex.Message, label);
    }

    /// <summary>
    /// A table with an INT64 primary key, a STRING column <c>s</c> that spells the key, an INT64
    /// column <c>a</c> equal to the key, and a BOOL column <c>b</c> (false). With
    /// <paramref name="indexed"/>, <c>a</c> and <c>s</c> have non-unique indexes and the table is
    /// analyzed, so the cost-based planner can seek them.
    /// </summary>
    private async Task<(string Db, CommandExecutor Executor)> SeedMixed(int rows, bool indexed)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, db, "CREATE TABLE mixed (id int64 PRIMARY KEY NOT NULL, s string NULL, a int64 NULL, b bool NULL)");

        if (indexed)
        {
            await Ddl(executor, db, "CREATE INDEX mixed_a ON mixed (a)");
            await Ddl(executor, db, "CREATE INDEX mixed_s ON mixed (s)");
        }

        if (rows > 0)
        {
            IEnumerable<string> values = Enumerable.Range(1, rows).Select(i => $"({i}, '{i}', {i}, false)");
            await NonQuery(executor, db, "INSERT INTO mixed (id, s, a, b) VALUES " + string.Join(", ", values), null);
        }

        if (indexed)
            await Query(executor, db, "ANALYZE mixed", null);

        return (db, executor);
    }

    private static async Task<bool> UsesIndex(CommandExecutor executor, string db, string sql)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> plan = await Query(executor, db, "EXPLAIN " + sql, null);
        return plan.Any(r => r["node"].StrValue is { } node && node.StartsWith("index-", StringComparison.Ordinal));
    }

    private (CommandExecutor, HttpTransactionCoordinator) ServiceExecutor()
    {
        CommandExecutor executor = new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        return (executor, new HttpTransactionCoordinator(executor));
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private static Task<BatchExecuteResponse> Terminal(ObservingStreamWriter<BatchExecuteResponse> writer, int requestId) =>
        writer.WaitFor(m => m.RequestId == requestId && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or
            BatchExecuteResponse.PayloadOneofCase.NonQuery or
            BatchExecuteResponse.PayloadOneofCase.Error);

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task NonQuery(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<long> Count(CommandExecutor executor, string db, string table, string tail, Dictionary<string, ColumnValue>? parameters = null)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, $"SELECT COUNT(*) AS c FROM {table} {tail}", parameters);
        Assert.AreEqual(1, rows.Count);
        return rows[0]["c"].LongValue;
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, string db, string sql)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db, sql, null);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Values.First();
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));

            List<IReadOnlyDictionary<string, ColumnValue>> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row.Row);

            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
