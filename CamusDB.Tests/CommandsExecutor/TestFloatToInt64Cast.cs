/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A float converts to Integer64 by rounding half away from zero, the GoogleSQL rule, and the same
/// rule as NUMERIC, jsonb and <c>round()</c>. Before, the cast truncated (<c>CAST(2.5 AS INT64)</c>
/// gave 2 and <c>CAST(-1.5 AS INT64)</c> gave -1), and a Float32 could not be cast at all. A value
/// with no integer in range is <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>. A context that
/// takes an integer but does not name a cast refuses a float, so no second rule can appear there.
/// Everything is driven through SQL tickets.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestFloatToInt64Cast : BaseTest
{
    /// <summary>The largest double below one half. Adding 0.5 and then taking the floor gives 1 for it.</summary>
    private const string JustBelowHalf = "0.49999999999999994";

    /// <summary>The source text of a Float64 and the integer the conversion must give.</summary>
    private static readonly (string Literal, long Expected)[] Float64Boundaries =
    [
        ("0.0", 0L),
        ("0.4", 0L),
        ("-0.4", 0L),
        (JustBelowHalf, 0L),
        ("-" + JustBelowHalf, 0L),
        ("0.5", 1L),
        ("-0.5", -1L),
        ("0.6", 1L),
        ("-0.6", -1L),
        ("1.5", 2L),
        ("-1.5", -2L),
        ("2.4", 2L),
        ("-2.4", -2L),
        ("2.5", 3L),
        ("-2.5", -3L),
        ("3.5", 4L),
        ("-3.5", -4L),
        // The last doubles that hold a fraction of one half.
        ("4503599627370495.5", 4503599627370496L),
        ("-4503599627370495.5", -4503599627370496L),
        // Around 2^53, where a double stops holding every integer. 2^53 + 1 is not a double: the
        // literal is already 2^53 before the cast sees it.
        ("9007199254740991.0", 9007199254740991L),
        ("-9007199254740991.0", -9007199254740991L),
        ("9007199254740992.0", 9007199254740992L),
        ("9007199254740993.0", 9007199254740992L),
        // The largest double below 2^63, and -2^63 itself, which is long.MinValue.
        ("9223372036854774784.0", 9223372036854774784L),
        ("-9223372036854775808.0", long.MinValue),
    ];

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string db,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertInt64(long expected, ColumnValue actual, string what)
    {
        Assert.AreEqual(ColumnType.Integer64, actual.Type, what);
        Assert.AreEqual(expected, actual.LongValue, what);
    }

    private static void AssertRefused(string expectedCode, string what, AsyncTestDelegate call)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(call, what)!;
        Assert.AreEqual(expectedCode, ex.Code, $"{what}: {ex.Message}");
    }

    // ── The boundary table ──────────────────────────────────────────────────

    [Test]
    public async Task Float64_RoundsHalfAwayFromZero_InEverySpellingOfTheCast()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string literal, long expected) in Float64Boundaries)
        {
            List<QueryResultRow> rows = await Query(executor, database, db,
                $"SELECT CAST({literal} AS int64) AS a, ({literal})::int64 AS b, to_int64({literal}) AS c");

            AssertInt64(expected, rows.Single().Row["a"], $"CAST({literal} AS int64)");
            AssertInt64(expected, rows.Single().Row["b"], $"({literal})::int64");
            AssertInt64(expected, rows.Single().Row["c"], $"to_int64({literal})");

            // A parameter has no source text, so this arm reads only the binary value.
            Dictionary<string, ColumnValue> parameters = new()
            {
                ["@p"] = new ColumnValue(ColumnType.Float64, double.Parse(literal, CultureInfo.InvariantCulture)),
            };
            List<QueryResultRow> bound = await Query(executor, database, db, "SELECT CAST(@p AS int64) AS a", parameters);
            AssertInt64(expected, bound.Single().Row["a"], $"CAST(@p AS int64) with {literal}");
        }
    }

    [Test]
    public async Task Float64Column_RoundsHalfAwayFromZero()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, f float64)");

        for (int i = 0; i < Float64Boundaries.Length; i++)
            await Dml(executor, database, db, $"INSERT INTO t (id, f) VALUES ({i}, {Float64Boundaries[i].Literal})");

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, CAST(f AS int64) AS i FROM t ORDER BY id");

        Assert.AreEqual(Float64Boundaries.Length, rows.Count);
        for (int i = 0; i < rows.Count; i++)
            AssertInt64(Float64Boundaries[i].Expected, rows[i].Row["i"], $"column value {Float64Boundaries[i].Literal}");
    }

    [Test]
    public async Task Float32Column_RoundsHalfAwayFromZero_InEverySpellingOfTheCast()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, r float32, want int64)");

        (string Literal, long Expected)[] cases =
        [
            ("0.5", 1L),
            ("-0.5", -1L),
            ("1.5", 2L),
            ("-1.5", -2L),
            ("2.5", 3L),
            ("-2.5", -3L),
            // The largest float32 below one half.
            ("0.49999997", 0L),
            ("-0.49999997", 0L),
            // The last float32 values that hold a fraction of one half.
            ("8388607.5", 8388608L),
            ("-8388607.5", -8388608L),
            // 2^24 + 1 is not a float32: the stored value is 2^24.
            ("16777217.0", 16777216L),
        ];

        for (int i = 0; i < cases.Length; i++)
            await Dml(executor, database, db, $"INSERT INTO t (id, r, want) VALUES ({i}, {cases[i].Literal}, {cases[i].Expected})");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT want, CAST(r AS int64) AS a, r::int64 AS b, to_int64(r) AS c FROM t ORDER BY id");

        Assert.AreEqual(cases.Length, rows.Count);
        foreach (QueryResultRow row in rows)
        {
            long want = row.Row["want"].LongValue;
            AssertInt64(want, row.Row["a"], $"CAST(r AS int64), want {want}");
            AssertInt64(want, row.Row["b"], $"r::int64, want {want}");
            AssertInt64(want, row.Row["c"], $"to_int64(r), want {want}");
        }
    }

    /// <summary>
    /// The one documented rule: every numeric type gives the same integer for the same tie, and
    /// <c>round</c> agrees with the casts.
    /// </summary>
    [TestCase("0.5", 1L)]
    [TestCase("-0.5", -1L)]
    [TestCase("1.5", 2L)]
    [TestCase("-1.5", -2L)]
    [TestCase("2.5", 3L)]
    [TestCase("-2.5", -3L)]
    [TestCase("3.5", 4L)]
    [TestCase("-3.5", -4L)]
    public async Task EveryNumericType_GivesTheSameIntegerForATie(string text, long expected)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await Query(executor, database, db,
            $"SELECT CAST({text} AS int64) AS f64, CAST(CAST({text} AS float32) AS int64) AS f32, " +
            $"CAST(NUMERIC '{text}' AS int64) AS num, CAST(jsonb '{text}' AS int64) AS doc, " +
            $"CAST(round({text}) AS int64) AS rounded");

        foreach (string column in new[] { "f64", "f32", "num", "doc", "rounded" })
            AssertInt64(expected, rows.Single().Row[column], column);
    }

    // ── No integer in range ─────────────────────────────────────────────────

    [Test]
    public async Task NonFiniteAndPastTheRange_AreOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string cast in new[]
        {
            "CAST(CAST('NaN' AS float64) AS int64)",
            "CAST(CAST('Infinity' AS float64) AS int64)",
            "CAST(CAST('-Infinity' AS float64) AS int64)",
            "CAST(CAST('NaN' AS float32) AS int64)",
            "CAST(CAST('Infinity' AS float32) AS int64)",
            "CAST(CAST('-Infinity' AS float32) AS int64)",
            // 2^63, and the first double below -2^63.
            "CAST(9223372036854775808.0 AS int64)",
            "CAST(-9223372036854777856.0 AS int64)",
            "CAST(CAST(1e30 AS float32) AS int64)",
            "CAST(CAST(-1e30 AS float32) AS int64)",
            "to_int64(CAST('NaN' AS float64))",
            "(CAST('Infinity' AS float64))::int64",
        })
        {
            AssertRefused(CamusDBErrorCodes.IntegerValueOutOfRange, cast,
                async () => await Query(executor, database, db, $"SELECT {cast} AS x"));
        }
    }

    [Test]
    public async Task AStatementThatCastsNaN_StoresNothing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, f float64, r float32, v int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, f, r, v) VALUES (1, 'NaN', 'NaN', 7)");

        AssertRefused(CamusDBErrorCodes.IntegerValueOutOfRange, "from a float64 column",
            async () => await Dml(executor, database, db, "UPDATE t SET v = CAST(f AS int64) WHERE id = 1"));
        AssertRefused(CamusDBErrorCodes.IntegerValueOutOfRange, "from a float32 column",
            async () => await Dml(executor, database, db, "UPDATE t SET v = CAST(r AS int64) WHERE id = 1"));

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT v FROM t WHERE id = 1");
        AssertInt64(7L, rows.Single().Row["v"], "the stored value");
    }

    // ── The rounded value in a statement ────────────────────────────────────

    [Test]
    public async Task RoundedValue_ReachesWritesPredicatesGroupsAndChecks()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, f float64, v int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, f, v) VALUES (1, 2.5, 0), (2, 3.4, 0), (3, 3.5, 0), (4, -1.5, 0)");

        // UPDATE stores the rounded value.
        Assert.AreEqual(4, await Dml(executor, database, db, "UPDATE t SET v = CAST(f AS int64) WHERE id >= 1"));
        long[] stored = (await Query(executor, database, db, "SELECT v FROM t ORDER BY id"))
            .Select(r => r.Row["v"].LongValue).ToArray();
        CollectionAssert.AreEqual(new[] { 3L, 3L, 4L, -2L }, stored);

        // INSERT ... SELECT stores the rounded value.
        await Ddl(executor, database, db, "CREATE TABLE copied (id int64 PRIMARY KEY, n int64)");
        await Dml(executor, database, db, "INSERT INTO copied (id, n) SELECT id, CAST(f AS int64) FROM t");
        long[] copied = (await Query(executor, database, db, "SELECT n FROM copied ORDER BY id"))
            .Select(r => r.Row["n"].LongValue).ToArray();
        CollectionAssert.AreEqual(new[] { 3L, 3L, 4L, -2L }, copied);

        // A filter over the cast keeps the rows that round to 3, not the rows that truncate to 3.
        long[] filtered = (await Query(executor, database, db, "SELECT id FROM t WHERE CAST(f AS int64) = 3 ORDER BY id"))
            .Select(r => r.Row["id"].LongValue).ToArray();
        CollectionAssert.AreEqual(new[] { 1L, 2L }, filtered);

        // GROUP BY over the cast.
        List<QueryResultRow> groups = await Query(executor, database, db,
            "SELECT CAST(f AS int64) AS k, COUNT(*) AS n FROM t GROUP BY CAST(f AS int64) ORDER BY k");
        CollectionAssert.AreEqual(new[] { -2L, 3L, 4L }, groups.Select(r => r.Row["k"].LongValue).ToArray());
        CollectionAssert.AreEqual(new[] { 1L, 2L, 1L }, groups.Select(r => r.Row["n"].LongValue).ToArray());

        // A constant cast reaches a primary-key seek as the rounded value.
        long[] sought = (await Query(executor, database, db, "SELECT id FROM t WHERE id = CAST(2.5 AS int64)"))
            .Select(r => r.Row["id"].LongValue).ToArray();
        CollectionAssert.AreEqual(new[] { 3L }, sought);

        // A CHECK evaluates the same rule: 2.5 rounds to 3 and passes, 2.4 rounds to 2 and does not.
        await Ddl(executor, database, db, "CREATE TABLE checked (id int64 PRIMARY KEY, f float64 CHECK (CAST(f AS int64) >= 3))");
        await Dml(executor, database, db, "INSERT INTO checked (id, f) VALUES (1, 2.5)");
        Assert.ThrowsAsync<CamusDBException>(async () => await Dml(executor, database, db, "INSERT INTO checked (id, f) VALUES (2, 2.4)"));
    }

    // ── Places that take an integer without a cast ──────────────────────────

    [Test]
    public async Task AssigningAFloatToAnInt64Column_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 7)");

        foreach (string sql in new[]
        {
            "INSERT INTO t (id, v) VALUES (2, 2.5)",
            "INSERT INTO t (id, v) VALUES (2.5, 1)",
            "UPDATE t SET v = 2.5 WHERE id = 1",
            "UPDATE t SET v = 2.0 WHERE id = 1",
            "INSERT INTO t (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = 2.5",
        })
        {
            Assert.ThrowsAsync<CamusDBException>(async () => await Dml(executor, database, db, sql), sql);
        }

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, v FROM t");
        AssertInt64(1L, rows.Single().Row["id"], "the only row");
        AssertInt64(7L, rows.Single().Row["v"], "the stored value");
    }

    [Test]
    public async Task AnIntegerArgument_RefusesAFloat()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in new[]
        {
            "SELECT substring('abc', 1.5) AS x",
            "SELECT substring('abc', 1, 1.5) AS x",
            "SELECT round(1.25, 1.0) AS x",
            "SELECT make_date(2024.0, 1, 1) AS x",
            "SELECT x FROM generate_series(1, 2.5) AS g(x)",
            "SELECT (ARRAY[1, 2])[1.5] AS x",
            "SELECT jsonb '[1, 2]' -> 1.5 AS x",
            "SELECT jsonb '[1, 2]' - 1.5 AS x",
            "SELECT 1.5 & 1 AS x",
            "SELECT ~1.5 AS x",
        })
        {
            AssertRefused(CamusDBErrorCodes.InvalidInput, sql, async () => await Query(executor, database, db, sql));
        }
    }

    [Test]
    public async Task TextWithAFraction_IsNotAnInteger()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Both references refuse this: a string to integer cast reads an integer, it does not round.
        foreach (string sql in new[]
        {
            "SELECT CAST('2.5' AS int64) AS x",
            "SELECT '2.5'::int64 AS x",
            "SELECT CAST(jsonb '{\"a\": 2.5}' ->> 'a' AS int64) AS x",
        })
        {
            AssertRefused(CamusDBErrorCodes.InvalidInput, sql, async () => await Query(executor, database, db, sql));
        }

        // A jsonb number is a number, so it rounds.
        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT CAST(jsonb '{\"a\": 2.5}' -> 'a' AS int64) AS x");
        AssertInt64(3L, rows.Single().Row["x"], "a jsonb number");
    }

    [Test]
    public async Task LimitAndOffset_RefuseANonInteger_ForEveryPlanShape()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, k int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, k) VALUES (1, 1), (2, 2), (3, 3)");

        Dictionary<string, ColumnValue>[] bindings =
        [
            new() { ["@p"] = new ColumnValue(ColumnType.Float64, 1.5) },
            new() { ["@p"] = new ColumnValue(ColumnType.Float64, 2.0) },
            new() { ["@p"] = ColumnValue.FromNumeric(NumericMath.FromInt64(2)) },
        ];

        foreach (string sql in new[]
        {
            "SELECT id FROM t LIMIT @p",
            "SELECT id FROM t OFFSET @p",
            "SELECT id FROM t LIMIT 1 OFFSET @p",
            "SELECT id FROM t ORDER BY id LIMIT @p",
            "SELECT id FROM t ORDER BY id LIMIT 1 OFFSET @p",
            "SELECT id FROM t ORDER BY k LIMIT @p",
            "SELECT id FROM t WHERE id >= 1 LIMIT @p",
            "SELECT a.id FROM t AS a INNER JOIN t AS b ON a.id = b.id LIMIT @p",
        })
        {
            foreach (Dictionary<string, ColumnValue> parameters in bindings)
            {
                AssertRefused(CamusDBErrorCodes.InvalidInput, $"{sql} with {parameters["@p"].Type}",
                    async () => await Query(executor, database, db, sql, parameters));
            }
        }

        // An integer still works.
        Dictionary<string, ColumnValue> two = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, 2L) };
        Assert.AreEqual(2, (await Query(executor, database, db, "SELECT id FROM t ORDER BY id LIMIT @p", two)).Count);
    }
}
