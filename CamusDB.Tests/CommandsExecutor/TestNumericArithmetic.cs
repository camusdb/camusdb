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
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// NUMERIC in expressions: <c>+ - * /</c>, unary minus and <c>%</c> with the Spanner result types
/// (NUMERIC with INT64 or NUMERIC stays NUMERIC, with a float it is FLOAT64), the aggregates
/// (SUM, AVG, MIN, MAX; global and grouped), the math functions, COALESCE, and the static types a
/// CTAS column takes from such expressions.
/// </summary>
internal sealed class TestNumericArithmetic : SharedNodeBaseTest
{
    private const string MaxText = "99999999999999999999999999999.999999999";

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> Setup()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE t (id int64 PRIMARY KEY, p NUMERIC, n int64, f float64)", ddl: true);
        await Exec(executor, db, "INSERT INTO t (id, p, n, f) VALUES (1, 2.5, 3, 0.5), (2, -1.25, 4, 1.5), (3, NULL, 5, 2.5)");
        return (db, executor);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql, bool ddl = false)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(tx, db.Name, sql, null);
        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Evaluates one expression over the row with id 1 and returns its value.</summary>
    private static async Task<ColumnValue> Eval(CommandExecutor executor, DatabaseDescriptor db, string expression) =>
        (await Select(executor, db, $"SELECT {expression} AS x FROM t WHERE id = 1"))[0].Row["x"];

    private static void AssertNumeric(string expected, ColumnValue actual, string because = "")
    {
        Assert.AreEqual(ColumnType.Numeric, actual.Type, because);
        Assert.AreEqual(expected, actual.NumericValue, because);
    }

    // ── Operators ───────────────────────────────────────────────────────────

    [Test, NonParallelizable]
    public async Task Operators_WithNumericOrInteger_StayNumericAndExact()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        AssertNumeric("0.3", await Eval(executor, db, "NUMERIC '0.1' + NUMERIC '0.2'"));
        AssertNumeric("3.5", await Eval(executor, db, "p + 1"));
        AssertNumeric("3.5", await Eval(executor, db, "1 + p"));
        AssertNumeric("-0.5", await Eval(executor, db, "p - n"));
        AssertNumeric("7.5", await Eval(executor, db, "p * n"));
        AssertNumeric("0.833333333", await Eval(executor, db, "p / 3"));
        AssertNumeric("0.833333333", await Eval(executor, db, "p / NUMERIC '3'"));
        AssertNumeric("0.000000001", await Eval(executor, db, "NUMERIC '0.000000001' * NUMERIC '0.5'"), "half away from zero");
        AssertNumeric("-2.5", await Eval(executor, db, "-p"));
        AssertNumeric("0.5", await Eval(executor, db, "p % 2"));
        AssertNumeric("-0.5", await Eval(executor, db, "-p % 2"), "the sign of the dividend");

        ColumnValue equal = (await Select(executor, db,
            "SELECT id FROM t WHERE NUMERIC '0.1' + NUMERIC '0.2' = NUMERIC '0.3' AND id = 1"))[0].Row["id"];
        Assert.AreEqual(1L, equal.LongValue);
    }

    [Test, NonParallelizable]
    public async Task Operators_WithAFloat_GiveFloat64()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        ColumnValue withLiteral = await Eval(executor, db, "p + 0.1");
        Assert.AreEqual(ColumnType.Float64, withLiteral.Type, "a bare decimal literal is FLOAT64, as in Spanner");
        Assert.AreEqual(2.6, withLiteral.FloatValue, 1e-12);

        ColumnValue withColumn = await Eval(executor, db, "p * f");
        Assert.AreEqual(ColumnType.Float64, withColumn.Type);
        Assert.AreEqual(1.25, withColumn.FloatValue);
    }

    [Test, NonParallelizable]
    public async Task Operators_NullDivisionAndOverflow()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        Assert.AreEqual(ColumnType.Null, (await Select(executor, db, "SELECT p + 1 AS x FROM t WHERE id = 3"))[0].Row["x"].Type);

        CamusDBException byZero = Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, "p / 0"))!;
        StringAssert.Contains("Division by zero", byZero.Message);
        Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, "p % NUMERIC '0'"));

        CamusDBException overflow = Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, $"NUMERIC '{MaxText}' + p"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, overflow.Code);

        CamusDBException product = Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, $"NUMERIC '{MaxText}' * 2"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, product.Code);
    }

    // ── Aggregates ──────────────────────────────────────────────────────────

    [Test, NonParallelizable]
    public async Task Aggregates_Global()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        QueryResultRow row = (await Select(executor, db,
            "SELECT SUM(p) AS s, AVG(p) AS a, MIN(p) AS lo, MAX(p) AS hi, COUNT(p) AS c FROM t"))[0];
        AssertNumeric("1.25", row.Row["s"]);
        AssertNumeric("0.625", row.Row["a"]);
        AssertNumeric("-1.25", row.Row["lo"]);
        AssertNumeric("2.5", row.Row["hi"]);
        Assert.AreEqual(2L, row.Row["c"].LongValue);

        // An integer average stays Float64.
        Assert.AreEqual(ColumnType.Float64, (await Select(executor, db, "SELECT AVG(n) AS a FROM t"))[0].Row["a"].Type);
        // NULL is skipped: (2.5 - 1.25 + 1 + 1) / 4.
        await Exec(executor, db, "INSERT INTO t (id, p) VALUES (4, 1), (5, 1)");
        AssertNumeric("0.8125", (await Select(executor, db, "SELECT AVG(p) AS a FROM t"))[0].Row["a"]);
    }

    [Test, NonParallelizable]
    public async Task Aggregates_SumIsExact_AndOverflowIsAnError()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE m (id int64 PRIMARY KEY, p NUMERIC)", ddl: true);

        string values = string.Join(", ", Enumerable.Range(1, 1000).Select(i => $"({i}, 0.01)"));
        await Exec(executor, db, $"INSERT INTO m (id, p) VALUES {values}");
        AssertNumeric("10", (await Select(executor, db, "SELECT SUM(p) AS s FROM m"))[0].Row["s"]);

        await Exec(executor, db, $"INSERT INTO m (id, p) VALUES (2001, {MaxText}), (2002, {MaxText})");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Select(executor, db, "SELECT SUM(p) AS s FROM m WHERE id > 2000"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, ex.Code);

        // The total passes the range but the average does not.
        AssertNumeric(MaxText, (await Select(executor, db, "SELECT AVG(p) AS a FROM m WHERE id > 2000"))[0].Row["a"]);
    }

    [Test, NonParallelizable]
    public async Task Aggregates_Grouped()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE g (id int64 PRIMARY KEY, k int64, p NUMERIC)", ddl: true);
        await Exec(executor, db, "INSERT INTO g (id, k, p) VALUES (1, 1, 0.1), (2, 1, 0.2), (3, 2, 1), (4, 2, 2), (5, 2, NULL)");

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT k, SUM(p) AS s, AVG(p) AS a, MAX(p) AS hi FROM g GROUP BY k ORDER BY k");

        AssertNumeric("0.3", rows[0].Row["s"]);
        AssertNumeric("0.15", rows[0].Row["a"]);
        AssertNumeric("0.2", rows[0].Row["hi"]);
        AssertNumeric("3", rows[1].Row["s"]);
        AssertNumeric("1.5", rows[1].Row["a"]);
    }

    [Test, NonParallelizable]
    public async Task Aggregates_MixedNumericAndInteger_SumAsNumeric()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        QueryResultRow row = (await Select(executor, db,
            "SELECT SUM(CASE WHEN id = 1 THEN p ELSE n END) AS s, AVG(CASE WHEN id = 1 THEN p ELSE n END) AS a FROM t"))[0];
        AssertNumeric("11.5", row.Row["s"], "2.5 + 4 + 5");
        AssertNumeric("3.833333333", row.Row["a"]);
    }

    /// <summary>
    /// The coordinator's AVG finalizer for a partial (distributed) aggregate divides the merged SUM by
    /// the merged COUNT. A NUMERIC sum must give a NUMERIC average, not the zero its unused double
    /// field would give.
    /// </summary>
    [Test]
    public void PartialAverageFinalizer_KeepsNumeric()
    {
        PartialAggregatePlan plan = new()
        {
            ShipProjections = [],
            MergeProjections = [],
            AvgFinalizers = [new PartialAvgFinalizer("a", "a_sum", "a_count"), new PartialAvgFinalizer("b", "b_sum", "b_count")],
        };

        QueryResultRow merged = new(ObjectIdValue.Empty, new Dictionary<string, ColumnValue>
        {
            ["a_sum"] = ColumnValue.FromNumericString("10"),
            ["a_count"] = new(ColumnType.Integer64, 3),
            ["b_sum"] = new(ColumnType.Integer64, 10),
            ["b_count"] = new(ColumnType.Integer64, 4),
        });

        QueryResultRow finalized = QueryExecutor.FinalizeAverages(merged, plan);

        AssertNumeric("3.333333333", finalized.Row["a"]);
        Assert.AreEqual(ColumnType.Float64, finalized.Row["b"].Type);
        Assert.AreEqual(2.5, finalized.Row["b"].FloatValue);
        Assert.IsFalse(finalized.Row.ContainsKey("a_sum"));
    }

    // ── Functions ───────────────────────────────────────────────────────────

    [Test, NonParallelizable]
    public async Task Functions_KeepNumericAndRoundHalfAwayFromZero()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        AssertNumeric("1.25", await Eval(executor, db, "abs(NUMERIC '-1.25')"));
        AssertNumeric("3", await Eval(executor, db, "ceil(p)"));
        AssertNumeric("2", await Eval(executor, db, "floor(p)"));
        AssertNumeric("-1", await Eval(executor, db, "ceil(NUMERIC '-1.25')"));
        AssertNumeric("-2", await Eval(executor, db, "floor(NUMERIC '-1.25')"));
        AssertNumeric("3", await Eval(executor, db, "round(p)"));
        AssertNumeric("-3", await Eval(executor, db, "round(-p)"));
        AssertNumeric("-1.3", await Eval(executor, db, "round(NUMERIC '-1.25', 1)"));
        AssertNumeric("1300", await Eval(executor, db, "round(NUMERIC '1250', -2)"), "half away from zero, left of the point");
        AssertNumeric("2", await Eval(executor, db, "trunc(p)"));
        AssertNumeric("-1.2", await Eval(executor, db, "trunc(NUMERIC '-1.25', 1)"));
        AssertNumeric("1200", await Eval(executor, db, "trunc(NUMERIC '1299.99', -2)"));
        AssertNumeric("0.5", await Eval(executor, db, "mod(p, 2)"));
        AssertNumeric("1", await Eval(executor, db, "sign(p)"), "Spanner: SIGN of a NUMERIC is a NUMERIC");
        AssertNumeric("-1", await Eval(executor, db, "sign(-p)"));
        AssertNumeric("0", await Eval(executor, db, "sign(NUMERIC '0')"));
        AssertNumeric("0.5", await Eval(executor, db, "sign(p) / 2"), "a NUMERIC sign divides exactly, not as integers");

        ColumnValue sqrt = await Eval(executor, db, "sqrt(NUMERIC '6.25')");
        Assert.AreEqual(ColumnType.Float64, sqrt.Type);
        Assert.AreEqual(2.5, sqrt.FloatValue);

        CamusDBException ceilMax = Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, $"ceil(NUMERIC '{MaxText}')"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, ceilMax.Code);
    }

    /// <summary>
    /// A precision far left of the point gives zero, also at <c>long.MinValue</c>, where the digit
    /// count to drop does not fit in a long. -29 is the last precision whose unit (10²⁹) can still
    /// round the maximum up, past the range; from -30 on every value rounds to zero.
    /// </summary>
    [Test, NonParallelizable]
    public async Task RoundAndTrunc_FarLeftPrecision_GiveZero_UpToLongMinValue()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        foreach (string function in new[] { "round", "trunc" })
        {
            AssertNumeric("0", await Eval(executor, db, $"{function}(NUMERIC '1.25', -9223372036854775808)"), function);
            AssertNumeric("0", await Eval(executor, db, $"{function}(NUMERIC '-1.25', -9223372036854775807)"), function);
            AssertNumeric("0", await Eval(executor, db, $"{function}(NUMERIC '{MaxText}', -30)"), function);
        }

        AssertNumeric("0", await Eval(executor, db, $"trunc(NUMERIC '{MaxText}', -29)"));
        CamusDBException roundMax = Assert.ThrowsAsync<CamusDBException>(() => Eval(executor, db, $"round(NUMERIC '{MaxText}', -29)"))!;
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, roundMax.Code, "10^29 is past the range");
    }

    [Test, NonParallelizable]
    public async Task Trunc_OnIntegerAndFloat()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        Assert.AreEqual(1200L, (await Eval(executor, db, "trunc(1299, -2)")).LongValue);
        Assert.AreEqual(1299L, (await Eval(executor, db, "trunc(1299)")).LongValue);
        Assert.AreEqual(-2.0, (await Eval(executor, db, "trunc(-2.7)")).FloatValue);
        Assert.AreEqual(2.5, (await Eval(executor, db, "trunc(2.56, 1)")).FloatValue, 1e-12);
    }

    [Test, NonParallelizable]
    public async Task Coalesce_WidensIntegerToNumeric()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        AssertNumeric("2.5", await Eval(executor, db, "COALESCE(p, 0)"));

        // A NULL value carries no column type, and COALESCE types its result from the values it sees,
        // so with p NULL the result is the integer 0 (the same holds for a NULL float column). The
        // declared type is NUMERIC, and a CTAS column coerces the integer into it.
        ColumnValue fromNull = (await Select(executor, db, "SELECT COALESCE(p, 0) AS x FROM t WHERE id = 3"))[0].Row["x"];
        Assert.AreEqual(0, MixedNumericComparison.ToDouble(fromNull));
    }

    // ── Static types: CTAS ──────────────────────────────────────────────────

    [Test, NonParallelizable]
    public async Task Ctas_TakesTheExpressionTypes()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        await Exec(executor, db,
            "CREATE TABLE c AS SELECT p * 2 AS doubled, n + 1 AS next, p + f AS mixed, abs(p) AS magnitude, round(p, 1) AS rounded FROM t",
            ddl: true);

        Dictionary<string, string> types = (await Select(executor, db, "SHOW COLUMNS FROM c"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);

        Assert.AreEqual("NUMERIC", types["doubled"]);
        Assert.AreEqual("INT64", types["next"], "integer arithmetic was typed STRING and the copy failed");
        Assert.AreEqual("FLOAT64", types["mixed"]);
        Assert.AreEqual("NUMERIC", types["magnitude"]);
        Assert.AreEqual("NUMERIC", types["rounded"]);

        List<QueryResultRow> rows = await Select(executor, db, "SELECT doubled, next FROM c WHERE next = 4");
        AssertNumeric("5", rows[0].Row["doubled"]);

        await Exec(executor, db, "CREATE TABLE g AS SELECT AVG(n) AS a, AVG(p) AS ap, SUM(p) AS sp FROM t", ddl: true);
        Dictionary<string, string> aggregateTypes = (await Select(executor, db, "SHOW COLUMNS FROM g"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);
        Assert.AreEqual("FLOAT64", aggregateTypes["a"], "an integer average is a float");
        Assert.AreEqual("NUMERIC", aggregateTypes["ap"]);
        Assert.AreEqual("NUMERIC", aggregateTypes["sp"]);
    }

    /// <summary>
    /// SIGN of a NUMERIC is NUMERIC. A CASE is typed as the widest numeric type of all its branches,
    /// not of the ELSE alone, so a CTAS column holds every branch's value: an INT64 column could not
    /// hold the NUMERIC 2.5 that the THEN branch returns.
    /// </summary>
    [Test, NonParallelizable]
    public async Task Ctas_TypesSignAndCaseFromEveryBranch()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        await Exec(executor, db,
            "CREATE TABLE s AS SELECT id, SIGN(p) AS sg, CASE WHEN id = 1 THEN p ELSE 0 END AS cs, " +
            "CASE WHEN id = 1 THEN p ELSE f END AS cf, CASE WHEN id = 1 THEN NULL ELSE n END AS cn FROM t",
            ddl: true);

        Dictionary<string, string> types = (await Select(executor, db, "SHOW COLUMNS FROM s"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);

        Assert.AreEqual("NUMERIC", types["sg"]);
        Assert.AreEqual("NUMERIC", types["cs"], "NUMERIC beside INT64");
        Assert.AreEqual("FLOAT64", types["cf"], "NUMERIC beside FLOAT64");
        Assert.AreEqual("INT64", types["cn"], "a NULL branch does not count");

        Dictionary<long, QueryResultRow> rows = (await Select(executor, db, "SELECT id, sg, cs FROM s"))
            .ToDictionary(r => r.Row["id"].LongValue);
        AssertNumeric("2.5", rows[1].Row["cs"]);
        AssertNumeric("0", rows[2].Row["cs"]);
        AssertNumeric("-1", rows[2].Row["sg"]);
    }
}
