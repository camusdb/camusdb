/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Float64 and Float32 follow the PostgreSQL rule. Before, the engine was not consistent:
/// <c>CAST('Infinity' AS float64)</c> was refused, but <c>1e308 * 10</c> stored infinity, the
/// literal <c>1E-400</c> became 0, and a <c>float32</c> column narrowed <c>1e300</c> to infinity.
/// Now:
/// <list type="bullet">
///   <item>An overflow or an underflow from finite values is
///   <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>, on every path: literal, arithmetic,
///   function, CAST, narrowing and aggregate.</item>
///   <item><c>'Infinity'</c>, <c>'-Infinity'</c> and <c>'NaN'</c> are values. A row that holds one
///   is readable, comparable, sortable, groupable, indexable, updatable and deletable, and it
///   survives a reopen.</item>
///   <item>The order is -infinity &lt; numbers &lt; +infinity &lt; NaN, NaN = NaN, and -0 = 0. A
///   table scan and an index seek return the same rows, and a UNIQUE index refuses -0 next to 0.</item>
/// </list>
/// Everything is driven through SQL tickets. The standalone and the cluster-mode engine run the
/// same tests.
/// </summary>
public abstract class FloatSpecialValueTests : BaseTest
{
    /// <summary>A NaN with the sign bit clear. <c>double.NaN</c> has it set, so the two have different bits.</summary>
    private static readonly double PositiveNaN = BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000);

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

    private static async Task<int> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, DatabaseDescriptor database, string db, string expression)
    {
        List<QueryResultRow> rows = await Query(executor, database, db, $"SELECT {expression} AS x");
        Assert.AreEqual(1, rows.Count, expression);
        return rows[0].Row["x"];
    }

    private static async Task<long[]> Ids(CommandExecutor executor, DatabaseDescriptor database, string db, string sql) =>
        (await Query(executor, database, db, sql)).Select(r => r.Row["id"].LongValue).ToArray();

    private static CamusDBException AssertFails(string code, string what, AsyncTestDelegate call)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(call, what)!;
        Assert.AreEqual(code, ex.Code, $"{what}: {ex.Message}");
        return ex;
    }

    /// <summary>Equal as SQL values, and with the same sign, so -0 is not taken for 0 and NaN matches NaN.</summary>
    private static void AssertSameFloat(double expected, double actual, string what)
    {
        if (double.IsNaN(expected))
        {
            Assert.IsTrue(double.IsNaN(actual), $"{what}: expected NaN, got {actual}");
            return;
        }

        Assert.AreEqual(expected, actual, $"{what}");
        Assert.AreEqual(double.IsNegative(expected), double.IsNegative(actual), $"{what}: the sign of {actual}");
    }

    // ── Overflow and underflow are errors ──────────────────────────────────

    [Test]
    public async Task OverflowAndUnderflow_FromFiniteValues_AreOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string expression in new[]
        {
            // Literals past the range: they never become ±infinity or zero.
            "1e400", "-4.56E500", "1E-400", "-1e-400",
            // Arithmetic.
            "1e308 * 10", "-1e308 * 10", "1e308 + 1e308", "-1e308 - 1e308", "1e300 / 1e-300",
            "1e-300 * 1e-300", "1e-300 / 1e300", "5e-324 / 2", "1e308 * 10 - 1e308",
            // Functions.
            "pow(10, 400)", "pow(10, -400)", "pow(2.0, 1024)", "pow(-10, 401)", "round(1.7e308, -308)",
            // Narrowing to float32, and float32 arithmetic past the float32 range.
            "CAST(1e300 AS float32)", "CAST(-1e300 AS float32)", "CAST(1e-300 AS float32)",
            "CAST(3e38 AS float32) + CAST(3e38 AS float32)", "CAST(3e38 AS float32) * 2",
            // Text past the range.
            "CAST('1e400' AS float64)", "CAST('-1e400' AS float64)", "CAST('1e-400' AS float64)",
            "CAST('1e39' AS float32)", "CAST('1e-50' AS float32)",
            "CAST(CAST('1e400' AS jsonb) AS float64)",
        })
        {
            CamusDBException ex = AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, expression,
                () => Scalar(executor, database, db, expression));
            StringAssert.Contains("flow", ex.Message, expression);
        }
    }

    [Test]
    public async Task ValuesAtTheBoundary_AreExact()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string expression, double expected) in new[]
        {
            ("1.7976931348623157e308 * 1", double.MaxValue),
            ("-1.7976931348623157e308 * 1", -double.MaxValue),
            ("1e308 * 1.5", 1.5e308),
            // A subnormal is a value, not an underflow.
            ("4e-320", 4e-320),
            ("2.2250738585072014e-308 / 2", 1.1125369292536007e-308),
            ("pow(2.0, -1074)", double.Epsilon),
            ("pow(2.0, 1023)", Math.ScaleB(1.0, 1023)),
            // A zero mantissa is zero, whatever the exponent.
            ("0e-400", 0.0),
            ("0.0 * 1e-300", 0.0),
            ("1e-300 * 0", 0.0),
            ("-0.0", -0.0),
            // The scale of round and trunc never turns a finite value into NaN or infinity.
            ("round(1.5, 400)", 1.5),
            ("round(1e300, 10)", 1e300),
            ("trunc(5.5, -400)", 0.0),
            ("round(123.456, 2)", 123.46),
            ("trunc(123.456, 1)", 123.4),
            ("round(5.5, -1)", 10.0),
        })
        {
            ColumnValue value = await Scalar(executor, database, db, expression);
            Assert.AreEqual(ColumnType.Float64, value.Type, expression);
            AssertSameFloat(expected, value.FloatValue, expression);
        }

        ColumnValue single = await Scalar(executor, database, db, "CAST(3.4028234663852886e38 AS float32)");
        Assert.AreEqual(ColumnType.Float32, single.Type);
        Assert.AreEqual(float.MaxValue, (float)single.FloatValue);

        single = await Scalar(executor, database, db, "CAST(1e-45 AS float32)");
        Assert.AreEqual(float.Epsilon, (float)single.FloatValue, "the smallest float32 subnormal is a value");
    }

    // ── Non-finite values are legal ────────────────────────────────────────

    [Test]
    public async Task NamedValues_AreLegalAndFollowIeee()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string expression, double expected) in new[]
        {
            ("CAST('Infinity' AS float64)", double.PositiveInfinity),
            ("CAST('-Infinity' AS float64)", double.NegativeInfinity),
            ("CAST('+infinity' AS float64)", double.PositiveInfinity),
            ("CAST('inf' AS float64)", double.PositiveInfinity),
            ("CAST('-INF' AS float64)", double.NegativeInfinity),
            ("CAST('  NaN ' AS float64)", double.NaN),
            ("CAST('nan' AS float64)", double.NaN),
            ("CAST('-0' AS float64)", -0.0),
            ("CAST('.5' AS float64)", 0.5),
            ("CAST(' 1.5e3 ' AS float64)", 1500.0),
            // Arithmetic on a non-finite operand gives the IEEE result: it is not an overflow.
            ("CAST('Infinity' AS float64) + 1", double.PositiveInfinity),
            ("CAST('Infinity' AS float64) * 2", double.PositiveInfinity),
            ("CAST('Infinity' AS float64) - CAST('Infinity' AS float64)", double.NaN),
            ("CAST('NaN' AS float64) * 0", double.NaN),
            ("1 / CAST('Infinity' AS float64)", 0.0),
            ("-(CAST('Infinity' AS float64))", double.NegativeInfinity),
            ("abs(CAST('-Infinity' AS float64))", double.PositiveInfinity),
            ("sqrt(CAST('Infinity' AS float64))", double.PositiveInfinity),
            ("pow(CAST('NaN' AS float64), 0)", 1.0),
            ("pow(2, CAST('Infinity' AS float64))", double.PositiveInfinity),
            ("pow(2, CAST('-Infinity' AS float64))", 0.0),
            ("round(CAST('NaN' AS float64), 2)", double.NaN),
            ("CAST(CAST('Infinity' AS float32) AS float64)", double.PositiveInfinity),
        })
        {
            ColumnValue value = await Scalar(executor, database, db, expression);
            Assert.AreEqual(ColumnType.Float64, value.Type, expression);
            AssertSameFloat(expected, value.FloatValue, expression);
        }

        ColumnValue single = await Scalar(executor, database, db, "CAST('INFINITY' AS float32)");
        Assert.AreEqual(ColumnType.Float32, single.Type);
        Assert.IsTrue(float.IsPositiveInfinity((float)single.FloatValue));
        Assert.IsTrue(double.IsNaN((await Scalar(executor, database, db, "CAST(CAST('NaN' AS float64) AS float32)")).FloatValue));

        foreach ((string expression, string expected) in new[]
        {
            ("CAST(CAST('Infinity' AS float64) AS string)", "Infinity"),
            ("CAST(CAST('-Infinity' AS float64) AS string)", "-Infinity"),
            ("CAST(CAST('NaN' AS float64) AS string)", "NaN"),
            ("CAST(-0.0 AS string)", "-0"),
            // JSON has no number for these values: to_jsonb gives a JSON string, as PostgreSQL does.
            ("CAST(to_jsonb(CAST('Infinity' AS float64)) AS string)", "\"Infinity\""),
            ("CAST(to_jsonb(CAST('NaN' AS float64)) AS string)", "\"NaN\""),
        })
            Assert.AreEqual(expected, (await Scalar(executor, database, db, expression)).StrValue, expression);
    }

    [Test]
    public async Task ConversionsWithNoResult_AreClassifiedErrors()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string expression, string code) in new[]
        {
            // Not the float text form: .NET alone would accept the symbol.
            ("CAST('abc' AS float64)", CamusDBErrorCodes.InvalidInput),
            ("CAST('∞' AS float64)", CamusDBErrorCodes.InvalidInput),
            ("CAST('1.5.5' AS float64)", CamusDBErrorCodes.InvalidInput),
            ("CAST('1e' AS float64)", CamusDBErrorCodes.InvalidInput),
            ("CAST('' AS float32)", CamusDBErrorCodes.InvalidInput),
            // No real result.
            ("pow(0, -1)", CamusDBErrorCodes.InvalidInput),
            ("pow(-8, 0.5)", CamusDBErrorCodes.InvalidInput),
            ("sqrt(-1.0)", CamusDBErrorCodes.InvalidInput),
            ("sign(CAST('NaN' AS float64))", CamusDBErrorCodes.InvalidInput),
            // Division by zero is an error, never an infinity, for any dividend.
            ("1.0 / 0", CamusDBErrorCodes.InvalidInput),
            ("CAST('Infinity' AS float64) / 0", CamusDBErrorCodes.InvalidInput),
            // An integer and a NUMERIC have no infinity and no NaN.
            ("CAST(CAST('NaN' AS float64) AS int64)", CamusDBErrorCodes.IntegerValueOutOfRange),
            ("CAST(CAST('Infinity' AS float64) AS int64)", CamusDBErrorCodes.IntegerValueOutOfRange),
            ("CAST(CAST('-Infinity' AS float64) AS int64)", CamusDBErrorCodes.IntegerValueOutOfRange),
            ("CAST(CAST('NaN' AS float64) AS numeric)", CamusDBErrorCodes.InvalidInput),
        })
            AssertFails(code, expression, () => Scalar(executor, database, db, expression));
    }

    // ── The life cycle of rows that hold the special values ────────────────

    /// <summary>
    /// The rows of <see cref="SpecialTables"/>. <c>f</c> is float64, <c>r</c> float32, <c>i</c> int64
    /// (<c>id % 3</c>). Row 3 holds -0 and row 4 holds 0; rows 7 and 9 hold NaNs with different bits.
    /// </summary>
    private static readonly (long id, double f)[] SpecialRows =
    [
        (1, double.NegativeInfinity), (2, -1.5), (3, -0.0), (4, 0.0), (5, 1.5),
        (6, double.PositiveInfinity), (7, double.NaN), (9, PositiveNaN),
    ];

    /// <summary>
    /// Two tables with the same rows: <c>t</c> has no index on the float columns, and <c>ti</c> has
    /// one on each, so every predicate runs once as a table scan and once as an index seek. The rows
    /// arrive in a mixed order — NaN first, NULL second, finite values in a batch, a NaN parameter
    /// last — so no result depends on the insertion order.
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SpecialTables()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string table in new[] { "t", "ti" })
        {
            await Ddl(executor, database, db, $"CREATE TABLE {table} (id int64 PRIMARY KEY, f float64, r float32, i int64)");

            if (table == "ti")
            {
                await Ddl(executor, database, db, "CREATE INDEX ti_f ON ti (f)");
                await Ddl(executor, database, db, "CREATE INDEX ti_r ON ti (r)");
                await Ddl(executor, database, db, "CREATE INDEX ti_i ON ti (i)");
            }

            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (7, 'NaN', 'NaN', 1)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (8, NULL, NULL, 2)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (3, -0.0, -0.0, 0)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (1, CAST('-Infinity' AS float64), CAST('-Infinity' AS float32), 1)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (6, 'Infinity', 'Infinity', 0)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (5, 1.5, 1.5, 2), (2, -1.5, -1.5, 2), (4, 0.0, 0, 1)");
            await Dml(executor, database, db, $"INSERT INTO {table} (id, f, r, i) VALUES (9, @f, @r, 0)", new()
            {
                { "@f", new ColumnValue(ColumnType.Float64, PositiveNaN) },
                { "@r", new ColumnValue(ColumnType.Float32, (double)BitConverter.Int32BitsToSingle(0x7FC0_0000)) },
            });
        }

        return (db, database, executor);
    }

    [Test]
    public async Task Rows_ReadBackExactly_AndSurviveAReopen()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        async Task Check(DatabaseDescriptor current, string stage)
        {
            foreach (string table in new[] { "t", "ti" })
            {
                Dictionary<long, QueryResultRow> rows = (await Query(executor, current, db, $"SELECT id, f, r FROM {table}"))
                    .ToDictionary(r => r.Row["id"].LongValue);

                Assert.AreEqual(9, rows.Count, $"{stage} {table}");
                Assert.AreEqual(ColumnType.Null, rows[8].Row["f"].Type, $"{stage} {table}: NULL stays NULL");

                foreach ((long id, double f) in SpecialRows)
                {
                    Assert.AreEqual(ColumnType.Float64, rows[id].Row["f"].Type, $"{stage} {table} row {id}");
                    AssertSameFloat(f, rows[id].Row["f"].FloatValue, $"{stage} {table} row {id} f");
                    Assert.AreEqual(ColumnType.Float32, rows[id].Row["r"].Type, $"{stage} {table} row {id}");
                    AssertSameFloat((float)f, (float)rows[id].Row["r"].FloatValue, $"{stage} {table} row {id} r");
                }
            }
        }

        await Check(database, "before the reopen");

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        await Check(await executor.OpenDatabase(db), "after the reopen");
    }

    [Test]
    public async Task Predicates_GiveTheSameRowsByScanAndByIndex()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        foreach ((string predicate, long[] expected) in new[]
        {
            ("f = CAST('NaN' AS float64)", new long[] { 7, 9 }),
            ("f = 0", new long[] { 3, 4 }),
            ("f = 0.0", new long[] { 3, 4 }),
            ("f = -0.0", new long[] { 3, 4 }),
            ("f >= 0", new long[] { 3, 4, 5, 6, 7, 9 }),
            ("f > 1", new long[] { 5, 6, 7, 9 }),
            ("f < 0", new long[] { 1, 2 }),
            ("f <= -0.0", new long[] { 1, 2, 3, 4 }),
            ("f = CAST('Infinity' AS float64)", new long[] { 6 }),
            ("f = CAST('-Infinity' AS float64)", new long[] { 1 }),
            ("f >= CAST('Infinity' AS float64)", new long[] { 6, 7, 9 }),
            ("f < CAST('NaN' AS float64)", new long[] { 1, 2, 3, 4, 5, 6 }),
            ("f > CAST('NaN' AS float64)", Array.Empty<long>()),
            ("f <> 0", new long[] { 1, 2, 5, 6, 7, 9 }),
            ("f BETWEEN -2 AND 2", new long[] { 2, 3, 4, 5 }),
            ("f IN (0, 1.5)", new long[] { 3, 4, 5 }),
            ("f IN (-0.0)", new long[] { 3, 4 }),
            ("f IS NULL", new long[] { 8 }),
            ("r = 0", new long[] { 3, 4 }),
            ("r > 1", new long[] { 5, 6, 7, 9 }),
            ("r = CAST('NaN' AS float32)", new long[] { 7, 9 }),
            ("r = CAST('NaN' AS float64)", new long[] { 7, 9 }),
            ("r < CAST('Infinity' AS float32)", new long[] { 1, 2, 3, 4, 5 }),
            // An Integer64 column against float literals, and the reverse above (f = 0).
            ("i = 0.0", new long[] { 3, 6, 9 }),
            ("i = -0.0", new long[] { 3, 6, 9 }),
            ("i < CAST('Infinity' AS float64)", new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }),
            ("i < CAST('NaN' AS float64)", new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }),
            ("i = CAST('NaN' AS float64)", Array.Empty<long>()),
        })
        {
            foreach (string table in new[] { "t", "ti" })
            {
                long[] ids = await Ids(executor, database, db, $"SELECT id FROM {table} WHERE {predicate} ORDER BY id");
                CollectionAssert.AreEqual(expected, ids, $"{table}: WHERE {predicate}");
            }
        }
    }

    [Test]
    public async Task OrderGroupDistinctAndAggregates_FollowTheSqlOrder()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        foreach (string table in new[] { "t", "ti" })
        {
            // NULL first, then -infinity < numbers < +infinity < NaN; -0 and 0 are peers.
            CollectionAssert.AreEqual(new long[] { 8, 1, 2, 3, 4, 5, 6, 7, 9 },
                await Ids(executor, database, db, $"SELECT id FROM {table} ORDER BY f, id"), $"{table}: ORDER BY f");
            CollectionAssert.AreEqual(new long[] { 7, 9, 6, 5, 3, 4, 2, 1, 8 },
                await Ids(executor, database, db, $"SELECT id FROM {table} ORDER BY f DESC, id"), $"{table}: ORDER BY f DESC");
            CollectionAssert.AreEqual(new long[] { 8, 1, 2, 3, 4, 5, 6, 7, 9 },
                await Ids(executor, database, db, $"SELECT id FROM {table} ORDER BY r, id"), $"{table}: ORDER BY r");

            // One group for -0 and 0, and one for the two NaNs.
            List<QueryResultRow> groups = await Query(executor, database, db, $"SELECT f, COUNT(*) AS c FROM {table} GROUP BY f");
            Assert.AreEqual(7, groups.Count, $"{table}: GROUP BY f");
            Assert.AreEqual(2, groups.Single(g => g.Row["f"].Type == ColumnType.Float64 && g.Row["f"].FloatValue == 0).Row["c"].LongValue, $"{table}: the zero group");
            Assert.AreEqual(2, groups.Single(g => g.Row["f"].Type == ColumnType.Float64 && double.IsNaN(g.Row["f"].FloatValue)).Row["c"].LongValue, $"{table}: the NaN group");

            Assert.AreEqual(7, (await Query(executor, database, db, $"SELECT DISTINCT f FROM {table}")).Count, $"{table}: DISTINCT f");
            Assert.AreEqual(7, (await Query(executor, database, db, $"SELECT DISTINCT r FROM {table}")).Count, $"{table}: DISTINCT r");

            QueryResultRow aggregates = (await Query(executor, database, db,
                $"SELECT MIN(f) AS lo, MAX(f) AS hi, COUNT(f) AS n, SUM(f) AS s, AVG(f) AS a FROM {table}")).Single();
            AssertSameFloat(double.NegativeInfinity, aggregates.Row["lo"].FloatValue, $"{table}: MIN");
            AssertSameFloat(double.NaN, aggregates.Row["hi"].FloatValue, $"{table}: MAX is NaN, the greatest value");
            Assert.AreEqual(8, aggregates.Row["n"].LongValue, $"{table}: COUNT");
            AssertSameFloat(double.NaN, aggregates.Row["s"].FloatValue, $"{table}: SUM over NaN is NaN");
            AssertSameFloat(double.NaN, aggregates.Row["a"].FloatValue, $"{table}: AVG over NaN is NaN");

            QueryResultRow finite = (await Query(executor, database, db,
                $"SELECT SUM(f) AS s, MAX(f) AS hi FROM {table} WHERE f > -1 AND f < CAST('Infinity' AS float64)")).Single();
            AssertSameFloat(1.5, finite.Row["s"].FloatValue, $"{table}: SUM of -0, 0 and 1.5");
            AssertSameFloat(1.5, finite.Row["hi"].FloatValue, $"{table}: MAX");

            QueryResultRow infinite = (await Query(executor, database, db,
                $"SELECT SUM(f) AS s FROM {table} WHERE f > 1 AND f < CAST('NaN' AS float64)")).Single();
            AssertSameFloat(double.PositiveInfinity, infinite.Row["s"].FloatValue, $"{table}: a SUM with +infinity is +infinity");
        }
    }

    [Test]
    public async Task Rows_AreUpdatableAndDeletable_ByTheirValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        foreach (string table in new[] { "t", "ti" })
        {
            Assert.AreEqual(2, await Dml(executor, database, db, $"UPDATE {table} SET f = 2.5 WHERE f = CAST('NaN' AS float64)"), $"{table}: UPDATE of the NaN rows");
            CollectionAssert.AreEqual(new long[] { 7, 9 }, await Ids(executor, database, db, $"SELECT id FROM {table} WHERE f = 2.5 ORDER BY id"), table);
            CollectionAssert.IsEmpty(await Ids(executor, database, db, $"SELECT id FROM {table} WHERE f = CAST('NaN' AS float64)"), $"{table}: no NaN key is left");

            Assert.AreEqual(2, await Dml(executor, database, db, $"UPDATE {table} SET f = 'NaN' WHERE f = 0"), $"{table}: UPDATE of the zero rows");
            CollectionAssert.AreEqual(new long[] { 3, 4 }, await Ids(executor, database, db, $"SELECT id FROM {table} WHERE f = CAST('NaN' AS float64) ORDER BY id"), table);

            Assert.AreEqual(1, await Dml(executor, database, db, $"DELETE FROM {table} WHERE f = CAST('Infinity' AS float64)"), $"{table}: DELETE of +infinity");
            Assert.AreEqual(1, await Dml(executor, database, db, $"DELETE FROM {table} WHERE f = CAST('-Infinity' AS float64)"), $"{table}: DELETE of -infinity");
            Assert.AreEqual(2, await Dml(executor, database, db, $"DELETE FROM {table} WHERE f = CAST('NaN' AS float64)"), $"{table}: DELETE of NaN");
            Assert.AreEqual(2, await Dml(executor, database, db, $"DELETE FROM {table} WHERE r = CAST('NaN' AS float32)"), $"{table}: DELETE by the float32 NaN");

            CollectionAssert.AreEqual(new long[] { 2, 5, 8 }, await Ids(executor, database, db, $"SELECT id FROM {table} ORDER BY id"), $"{table}: what is left");
            CollectionAssert.AreEqual(new long[] { 2, 5 }, await Ids(executor, database, db, $"SELECT id FROM {table} WHERE f > -100 ORDER BY id"), $"{table}: what the index holds");
        }
    }

    [Test]
    public async Task UniqueIndex_RefusesASecondKeyForOneValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE u (id int64 PRIMARY KEY, f float64, r float32)");
        await Ddl(executor, database, db, "CREATE UNIQUE INDEX u_f ON u (f)");
        await Ddl(executor, database, db, "CREATE UNIQUE INDEX u_r ON u (r)");

        await Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (1, 0.0, 0)");
        AssertFails(CamusDBErrorCodes.DuplicateUniqueKeyValue, "-0 next to 0",
            () => Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (2, -0.0, 2)"));

        await Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (3, 'NaN', 'NaN')");
        AssertFails(CamusDBErrorCodes.DuplicateUniqueKeyValue, "a NaN with other bits next to NaN",
            () => Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (4, @f, 4)",
                new() { { "@f", new ColumnValue(ColumnType.Float64, PositiveNaN) } }));
        AssertFails(CamusDBErrorCodes.DuplicateUniqueKeyValue, "a float32 -0 next to a float32 0",
            () => Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (5, 5, -0.0)"));

        await Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (6, 'Infinity', 'Infinity')");
        await Dml(executor, database, db, "INSERT INTO u (id, f, r) VALUES (7, '-Infinity', '-Infinity')");

        CollectionAssert.AreEqual(new long[] { 1, 3, 6, 7 }, await Ids(executor, database, db, "SELECT id FROM u ORDER BY id"));
    }

    [Test]
    public async Task Writes_ThatOverflow_StoreNothing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE s (id int64 PRIMARY KEY, f float64, r float32)");
        await Dml(executor, database, db, "INSERT INTO s (id, f, r) VALUES (1, 1e308, 1), (2, 1e308, 2)");

        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "UPDATE",
            () => Dml(executor, database, db, "UPDATE s SET f = f * 10 WHERE id = 1"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "INSERT of an expression",
            () => Dml(executor, database, db, "INSERT INTO s (id, f, r) VALUES (3, 1e308 * 10, 3)"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "INSERT of a float64 into float32",
            () => Dml(executor, database, db, "INSERT INTO s (id, f, r) VALUES (4, 1, 1e300)"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "UPDATE of a float32 column",
            () => Dml(executor, database, db, "UPDATE s SET r = 1e-300 WHERE id = 2"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "INSERT of the text form past the range",
            () => Dml(executor, database, db, "INSERT INTO s (id, f, r) VALUES (5, '1e400', 5)"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "a typed float32 value past the float32 range", async () =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                await executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "s",
                    values: new() { new() {
                        { "id", new ColumnValue(ColumnType.Integer64, 6) },
                        { "r", new ColumnValue(ColumnType.Float32, 1e300) },
                    }}));
                await database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        });

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, f, r FROM s ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2 }, rows.Select(r => r.Row["id"].LongValue).ToArray(), "no row was added");
        Assert.AreEqual(1e308, rows[0].Row["f"].FloatValue, "row 1 is unchanged");
        Assert.AreEqual(2.0, rows[1].Row["r"].FloatValue, "row 2 is unchanged");

        // An overflow in a predicate is the error, not a comparison with infinity.
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "WHERE",
            () => Ids(executor, database, db, "SELECT id FROM s WHERE f * 10 > 0"));
        AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, "DELETE WHERE",
            () => Dml(executor, database, db, "DELETE FROM s WHERE f * 10 > 0"));
        Assert.AreEqual(2, (await Query(executor, database, db, "SELECT id FROM s")).Count, "the DELETE removed nothing");
    }

    [Test]
    public async Task Sums_PastTheRange_AreOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE s (id int64 PRIMARY KEY, f float64, g int64)");
        await Dml(executor, database, db, "INSERT INTO s (id, f, g) VALUES (1, 1e308, 1), (2, 1e308, 1), (3, -1e308, 2)");

        foreach (string sql in new[]
        {
            "SELECT SUM(f) AS x FROM s WHERE g = 1",
            "SELECT AVG(f) AS x FROM s WHERE g = 1",
            "SELECT g, SUM(f) AS x FROM s GROUP BY g",
            "SELECT id, SUM(f) OVER (PARTITION BY g) AS x FROM s",
            "SELECT id, SUM(f) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x FROM s",
        })
            AssertFails(CamusDBErrorCodes.FloatValueOutOfRange, sql, () => Query(executor, database, db, sql));

        // A window frame adds exactly and checks only its total, so 1e308 + 1e308 - 1e308 is 1e308.
        foreach (QueryResultRow row in await Query(executor, database, db, "SELECT id, SUM(f) OVER () AS x FROM s"))
            Assert.AreEqual(1e308, row.Row["x"].FloatValue, $"row {row.Row["id"].LongValue}");
    }

    [Test]
    public async Task Analyze_AndADefault_HoldSpecialValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        // ANALYZE persists the bounds and the common values of f, -infinity and NaN among them.
        await Query(executor, database, db, "ANALYZE t");
        await Query(executor, database, db, "ANALYZE ti");
        CollectionAssert.AreEqual(new long[] { 5, 6, 7, 9 }, await Ids(executor, database, db, "SELECT id FROM ti WHERE f > 1 ORDER BY id"),
            "estimates from those statistics still give the right rows");

        await Ddl(executor, database, db, "CREATE TABLE d (id int64 PRIMARY KEY, f float64 DEFAULT('Infinity'), n float64 DEFAULT('NaN'))");
        await Dml(executor, database, db, "INSERT INTO d (id) VALUES (1)");

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        DatabaseDescriptor reopened = await executor.OpenDatabase(db);

        await Dml(executor, reopened, db, "INSERT INTO d (id) VALUES (2)");
        foreach (QueryResultRow row in await Query(executor, reopened, db, "SELECT id, f, n FROM d"))
        {
            AssertSameFloat(double.PositiveInfinity, row.Row["f"].FloatValue, $"row {row.Row["id"].LongValue} f");
            AssertSameFloat(double.NaN, row.Row["n"].FloatValue, $"row {row.Row["id"].LongValue} n");
        }

        CollectionAssert.AreEqual(new long[] { 5, 6, 7, 9 }, await Ids(executor, reopened, db, "SELECT id FROM ti WHERE f > 1 ORDER BY id"),
            "the statistics read back after the reopen");
    }

    [Test]
    public async Task Subqueries_CarrySpecialValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SpecialTables();

        // A scalar or IN subquery result is substituted into the outer query as a literal; ±infinity,
        // NaN and float32 have no literal token.
        CollectionAssert.AreEqual(new long[] { 7, 9 }, await Ids(executor, database, db,
            "SELECT id FROM t WHERE f = (SELECT MAX(f) FROM ti) ORDER BY id"), "MAX is NaN");
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, database, db,
            "SELECT id FROM t WHERE f = (SELECT MIN(f) FROM ti) ORDER BY id"), "MIN is -infinity");
        CollectionAssert.AreEqual(new long[] { 1, 6, 7, 9 }, await Ids(executor, database, db,
            "SELECT id FROM t WHERE f IN (SELECT f FROM ti WHERE id IN (1, 6, 7)) ORDER BY id"), "IN over special values");
        CollectionAssert.AreEqual(new long[] { 6 }, await Ids(executor, database, db,
            "SELECT id FROM t WHERE r = (SELECT r FROM ti WHERE id = 6) ORDER BY id"), "a float32 scalar subquery");
        CollectionAssert.AreEqual(new long[] { 3, 4 }, await Ids(executor, database, db,
            "SELECT id FROM t WHERE f = (SELECT f FROM ti WHERE id = 3) ORDER BY id"), "-0 from a subquery");
    }
}

/// <summary>The float rule on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestFloatSpecialValues : FloatSpecialValueTests;

/// <summary>
/// The float rule on a cluster-mode engine, where the schema is a replicated log (a DEFAULT of
/// infinity travels in a schema-change payload) and a write runs through the cluster commit path.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestFloatSpecialValuesCluster : FloatSpecialValueTests
{
    private EmbeddedKahuna? node;
    private string storageRoot = "";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-float-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"test-float-{Guid.NewGuid():N}",
            Storage = "sqlite",
            StoragePath = storageRoot,
            WalStorage = "sqlite",
            WalPath = storageRoot,
            InitialPartitions = 4
        }.WithTestNodeDefaults());
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);
        await node.FlushAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (node is not null)
            await node.DisposeAsync();
        try { if (storageRoot.Length > 0) System.IO.Directory.Delete(storageRoot, recursive: true); }
        catch { /* best-effort */ }
    }

    /// <summary>A cluster-mode engine over this fixture's own node.</summary>
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options, sharedNode: node!, isClusterMode: true);
    }
}

/// <summary>
/// The helpers of the float rule, against values computed independently: the SQL order is a total
/// order, the canonical key value folds -0 and every NaN, the parser takes the PostgreSQL text form
/// only, and each checked operation agrees with IEEE arithmetic except where it must refuse.
/// </summary>
[TestFixture]
public sealed class TestFloatMath
{
    private static readonly double[] Ordered =
    [
        double.NegativeInfinity, -double.MaxValue, -1.0, -double.Epsilon, 0.0, double.Epsilon, 1.0,
        double.MaxValue, double.PositiveInfinity, double.NaN,
    ];

    [Test]
    public void Compare_IsATotalOrder_WithNaNOnTop()
    {
        for (int i = 0; i < Ordered.Length; i++)
        {
            Assert.AreEqual(0, FloatMath.Compare(Ordered[i], Ordered[i]), $"{Ordered[i]} = itself");

            for (int j = i + 1; j < Ordered.Length; j++)
            {
                Assert.AreEqual(-1, FloatMath.Compare(Ordered[i], Ordered[j]), $"{Ordered[i]} < {Ordered[j]}");
                Assert.AreEqual(1, FloatMath.Compare(Ordered[j], Ordered[i]), $"{Ordered[j]} > {Ordered[i]}");
                // ±double.MaxValue narrows to ±infinity, so the float32 order may tie, never invert.
                Assert.LessOrEqual(FloatMath.Compare((float)Ordered[i], (float)Ordered[j]), 0, $"float32 {Ordered[i]} <= {Ordered[j]}");
            }
        }

        double negativeNaN = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8_0000_0000_0001));
        Assert.AreEqual(0, FloatMath.Compare(-0.0, 0.0));
        Assert.AreEqual(0, FloatMath.Compare(negativeNaN, double.NaN));
        Assert.AreEqual(1, FloatMath.Compare(negativeNaN, double.PositiveInfinity), "a sign-bit NaN is still above +infinity");
        Assert.IsTrue(FloatMath.SqlEquals(double.NaN, negativeNaN));
        Assert.IsTrue(FloatMath.SqlEquals(-0.0, 0.0));
        Assert.IsFalse(FloatMath.SqlEquals(double.NaN, 1.0));
    }

    [Test]
    public void Canonical_FoldsNegativeZeroAndEveryNaN()
    {
        Assert.AreEqual(0L, BitConverter.DoubleToInt64Bits(FloatMath.Canonical(-0.0)));
        Assert.AreEqual(0x7FF8_0000_0000_0000L, BitConverter.DoubleToInt64Bits(FloatMath.Canonical(double.NaN)));
        Assert.AreEqual(0x7FF8_0000_0000_0000L, BitConverter.DoubleToInt64Bits(FloatMath.Canonical(BitConverter.Int64BitsToDouble(0x7FF0_0000_0000_0001))));
        Assert.AreEqual(0, BitConverter.SingleToInt32Bits(FloatMath.Canonical(-0.0f)));
        Assert.AreEqual(0x7FC0_0000, BitConverter.SingleToInt32Bits(FloatMath.Canonical(float.NaN)));

        foreach (double value in new[] { 1.5, -1.5, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity })
            Assert.AreEqual(BitConverter.DoubleToInt64Bits(value), BitConverter.DoubleToInt64Bits(FloatMath.Canonical(value)), $"{value} is kept");
    }

    [TestCase("1.5", "Ok", 1.5)]
    [TestCase("  -2.5e-3\t", "Ok", -0.0025)]
    [TestCase("+7", "Ok", 7.0)]
    [TestCase("5.", "Ok", 5.0)]
    [TestCase(".5E+2", "Ok", 50.0)]
    [TestCase("4e-320", "Ok", 4e-320)]
    [TestCase("0e-999999", "Ok", 0.0)]
    [TestCase("1e309", "Overflow", 0.0)]
    [TestCase("-1e99999999999", "Overflow", 0.0)]
    [TestCase("1e-400", "Underflow", 0.0)]
    [TestCase("-0.0001e-320", "Underflow", 0.0)]
    [TestCase("", "Invalid", 0.0)]
    [TestCase(".", "Invalid", 0.0)]
    [TestCase("1e", "Invalid", 0.0)]
    [TestCase("e5", "Invalid", 0.0)]
    [TestCase("1,5", "Invalid", 0.0)]
    [TestCase("0x10", "Invalid", 0.0)]
    [TestCase("∞", "Invalid", 0.0)]
    [TestCase("infinit", "Invalid", 0.0)]
    [TestCase("1.5 2", "Invalid", 0.0)]
    public void TryParseFloat64_AcceptsTheTextFormOnly(string text, string statusName, double expected)
    {
        // The status travels by name: the enum is internal, and a public test method cannot take it.
        FloatMath.ParseStatus status = Enum.Parse<FloatMath.ParseStatus>(statusName);
        Assert.AreEqual(status, FloatMath.TryParseFloat64(text, out double value), text);
        if (status == FloatMath.ParseStatus.Ok)
            Assert.AreEqual(expected, value, text);
    }

    [TestCase("NaN")]
    [TestCase("nan")]
    [TestCase("Infinity")]
    [TestCase("+Infinity")]
    [TestCase("-infinity")]
    [TestCase("inf")]
    [TestCase("+INF")]
    [TestCase("-inf")]
    public void TryParse_AcceptsTheNamedValues(string text)
    {
        Assert.AreEqual(FloatMath.ParseStatus.Ok, FloatMath.TryParseFloat64(text, out double value), text);
        Assert.AreEqual(FloatMath.ParseStatus.Ok, FloatMath.TryParseFloat32(text, out float single), text);
        Assert.IsFalse(double.IsFinite(value), text);
        Assert.IsFalse(float.IsFinite(single), text);
        Assert.AreEqual(text.StartsWith('-'), value < 0, text);
    }

    [Test]
    public void TryParseFloat32_UsesTheSinglePrecisionRange()
    {
        Assert.AreEqual(FloatMath.ParseStatus.Overflow, FloatMath.TryParseFloat32("3.5e38", out _));
        Assert.AreEqual(FloatMath.ParseStatus.Underflow, FloatMath.TryParseFloat32("1e-46", out _));
        Assert.AreEqual(FloatMath.ParseStatus.Ok, FloatMath.TryParseFloat32("3.4028234e38", out float max));
        Assert.AreEqual(float.MaxValue, max);
        Assert.AreEqual(FloatMath.ParseStatus.Overflow, FloatMath.TryParseFloat32("1e300", out _), "1e300 is past the float32 range");
    }

    [Test]
    public void CheckedOperations_AgreeWithIeee_OrRefuse()
    {
        double[] operands =
        [
            0.0, -0.0, double.Epsilon, -double.Epsilon, 1e-300, -1e-300, 1e-160, 0.5, 1.0, -1.0, 3.0, 1e160, 1e300,
            -1e300, double.MaxValue, -double.MaxValue, double.PositiveInfinity, double.NegativeInfinity, double.NaN,
        ];

        foreach (double l in operands)
        {
            foreach (double r in operands)
            {
                Check("+", l, r, l + r, () => FloatMath.Add(l, r));
                Check("-", l, r, l - r, () => FloatMath.Subtract(l, r));
                Check("*", l, r, l * r, () => FloatMath.Multiply(l, r));
                if (r != 0)
                    Check("/", l, r, l / r, () => FloatMath.Divide(l, r));
            }
        }

        static void Check(string symbol, double l, double r, double ieee, Func<double> checkedResult)
        {
            bool finiteInputs = double.IsFinite(l) && double.IsFinite(r);
            bool overflow = finiteInputs && double.IsInfinity(ieee);
            bool underflow = finiteInputs && ieee == 0 && l != 0 && (symbol is "*" ? r != 0 : symbol is "/");

            if (overflow || underflow)
            {
                CamusDBException ex = Assert.Throws<CamusDBException>(() => checkedResult(), $"{l:R} {symbol} {r:R}")!;
                Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, ex.Code);
                return;
            }

            double result = checkedResult();
            Assert.AreEqual(BitConverter.DoubleToInt64Bits(ieee), BitConverter.DoubleToInt64Bits(result), $"{l:R} {symbol} {r:R}");
        }
    }

    [Test]
    public void NarrowToSingle_RefusesOnlyALostFiniteValue()
    {
        Assert.AreEqual(float.MaxValue, FloatMath.NarrowToSingle(float.MaxValue));
        Assert.AreEqual(float.Epsilon, FloatMath.NarrowToSingle(float.Epsilon));
        Assert.AreEqual(0, BitConverter.SingleToInt32Bits(FloatMath.NarrowToSingle(0.0)));
        Assert.IsTrue(float.IsNegative(FloatMath.NarrowToSingle(-0.0)), "-0 narrows to -0");
        Assert.IsTrue(float.IsPositiveInfinity(FloatMath.NarrowToSingle(double.PositiveInfinity)));
        Assert.IsTrue(float.IsNaN(FloatMath.NarrowToSingle(double.NaN)));

        foreach (double lost in new[] { 1e39, -1e39, double.MaxValue, 1e-46, -1e-46, double.Epsilon })
        {
            CamusDBException ex = Assert.Throws<CamusDBException>(() => FloatMath.NarrowToSingle(lost), $"{lost:R}")!;
            Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, ex.Code);
        }
    }

    [Test]
    public void WireCodec_RoundTripsEverySpecialValue()
    {
        foreach (ColumnType type in new[] { ColumnType.Float64, ColumnType.Float32 })
        {
            foreach (double value in new[] { double.PositiveInfinity, double.NegativeInfinity, double.NaN, -0.0, 0.0, double.Epsilon, double.MaxValue })
            {
                double expected = type == ColumnType.Float32 ? (float)value : value;
                using System.IO.MemoryStream stream = new();
                using (System.Text.Json.Utf8JsonWriter writer = new(stream))
                    ColumnValueWireCodec.Write(writer, new ColumnValue(type, expected));

                using System.Text.Json.JsonDocument document = System.Text.Json.JsonDocument.Parse(stream.ToArray());
                ColumnValue read = ColumnValueWireCodec.Read(document.RootElement);

                Assert.AreEqual(type, read.Type, $"{type} {value}");
                if (double.IsNaN(expected))
                    Assert.IsTrue(double.IsNaN(read.FloatValue), $"{type} NaN");
                else
                    Assert.AreEqual(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(read.FloatValue), $"{type} {value:R}");
            }
        }
    }

    [Test]
    public void RowJson_WritesNamesForNonFiniteValues_AndNumbersOtherwise()
    {
        foreach ((ColumnValue value, string expected) in new[]
        {
            (new ColumnValue(ColumnType.Float64, double.PositiveInfinity), "\"Infinity\""),
            (new ColumnValue(ColumnType.Float64, double.NegativeInfinity), "\"-Infinity\""),
            (new ColumnValue(ColumnType.Float64, double.NaN), "\"NaN\""),
            (new ColumnValue(ColumnType.Float32, double.PositiveInfinity), "\"Infinity\""),
            (new ColumnValue(ColumnType.Float32, double.NaN), "\"NaN\""),
            (new ColumnValue(ColumnType.Float64, 1.5), "1.5"),
            (new ColumnValue(ColumnType.Float64, -0.0), "-0"),
        })
        {
            using System.IO.MemoryStream stream = new();
            using (System.Text.Json.Utf8JsonWriter writer = new(stream))
                CamusDB.Core.CommandsExecutor.Controllers.Queries.CompactRowJsonWriter.WriteValue(writer, value);
            Assert.AreEqual(expected, System.Text.Encoding.UTF8.GetString(stream.ToArray()), value.ToString());

            // The object form of the same encoder must serialize to the same token.
            string viaObject = System.Text.Json.JsonSerializer.Serialize(
                CamusDB.Core.CommandsExecutor.Controllers.Queries.CompactRowEncoder.EncodeValue(value));
            Assert.AreEqual(expected, viaObject, $"object form of {value}");
        }
    }
}
