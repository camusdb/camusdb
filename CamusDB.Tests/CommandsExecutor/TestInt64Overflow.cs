/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
/// Integer64 arithmetic never wraps. Before, <c>9223372036854775807 + 1</c> returned
/// <c>-9223372036854775808</c>, <c>SUM</c> wrapped the same way, an UPDATE stored the wrapped value
/// and reported the rows as modified, and <c>-9223372036854775808 / -1</c> threw a raw
/// <see cref="OverflowException"/> that reached the client as an unclassified error. Now every
/// Integer64 result outside the 64-bit range is <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>,
/// and a statement that overflows stores nothing. Everything is driven through SQL tickets. The
/// standalone and the cluster-mode engine run the same tests.
/// </summary>
public abstract class Int64OverflowTests : BaseTest
{
    private const string Max = "9223372036854775807";
    private const string Min = "-9223372036854775808";

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

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        List<QueryResultRow> rows = await Query(executor, database, db, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Row["x"];
    }

    /// <summary>Asserts that <paramref name="call"/> fails with the Integer64 out-of-range code.</summary>
    private static CamusDBException AssertOutOfRange(string what, AsyncTestDelegate call)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(call, what)!;
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, ex.Code, $"{what}: {ex.Message}");
        return ex;
    }

    /// <summary>The (id, v) pairs of <c>t</c> in id order.</summary>
    private static async Task<List<(long id, long v)>> Pairs(CommandExecutor executor, DatabaseDescriptor database, string db) =>
        (await Query(executor, database, db, "SELECT id, v FROM t ORDER BY id"))
            .Select(r => (r.Row["id"].LongValue, r.Row["v"].LongValue))
            .ToList();

    /// <summary>A table <c>t (id, v, g)</c> with one row per value, ids from 1, every row in group 1.</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> TableWith(params string[] values)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v int64, g int64)");

        if (values.Length > 0)
        {
            string rows = string.Join(", ", values.Select((value, i) => $"({i + 1}, {value}, 1)"));
            await Dml(executor, database, db, $"INSERT INTO t (id, v, g) VALUES {rows}");
        }

        return (db, database, executor);
    }

    // ── Operators ───────────────────────────────────────────────────────────

    [Test]
    public async Task EachOperator_PastTheRange_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string expression in new[]
        {
            $"{Max} + 1", $"1 + {Max}", $"{Min} + -1", $"{Max} + {Max}", $"{Min} + {Min}",
            $"{Min} - 1", $"{Max} - -1", $"0 - {Min}", $"-1 - {Max} - 1",
            $"{Max} * 2", $"{Min} * -1", $"-1 * {Min}", $"{Min} * {Min}", $"4294967296 * 4294967296",
            $"{Min} / -1",
            $"-({Min})", $"-(-9223372036854775807 - 1)",
        })
        {
            CamusDBException ex = AssertOutOfRange(expression, async () => await Query(executor, database, db, $"SELECT {expression} AS x"));
            Assert.That(ex.Message, Does.Contain("overflow").Or.Contain("outside the int64 range"), expression);
        }
    }

    [Test]
    public async Task EachOperator_AtTheBoundary_GivesTheExactValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach ((string expression, long expected) in new[]
        {
            ($"{Max} + 0", long.MaxValue), ($"{Min} + 0", long.MinValue), ($"{Min} + {Max}", -1L),
            ($"{Max} - {Max}", 0L), ($"{Min} - -1", long.MinValue + 1), ($"-1 - {Max}", long.MinValue),
            ($"{Max} * 1", long.MaxValue), ($"{Max} * -1", -long.MaxValue), ($"{Min} * 1", long.MinValue),
            ($"3037000499 * 3037000499", 3037000499L * 3037000499L),
            ($"{Min} / 1", long.MinValue), ($"{Max} / -1", -long.MaxValue), ($"{Min} / 2", long.MinValue / 2),
            ($"-({Max})", -long.MaxValue),
            // % is mod(), where the true result of min % -1 is 0 and fits: PostgreSQL gives 0 too.
            ($"{Min} % -1", 0L),
        })
        {
            ColumnValue value = await Scalar(executor, database, db, $"SELECT {expression} AS x");
            Assert.AreEqual(ColumnType.Integer64, value.Type, expression);
            Assert.AreEqual(expected, value.LongValue, expression);
        }
    }

    [Test]
    public async Task Operators_OverColumnValues_AreOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith(Max, Min, "-1");

        AssertOutOfRange("v + 1 over max", async () => await Query(executor, database, db, "SELECT v + 1 AS x FROM t WHERE id = 1"));
        AssertOutOfRange("v - 1 over min", async () => await Query(executor, database, db, "SELECT v - 1 AS x FROM t WHERE id = 2"));
        AssertOutOfRange("-v over min", async () => await Query(executor, database, db, "SELECT -v AS x FROM t WHERE id = 2"));
        AssertOutOfRange("min / v with v = -1", async () => await Query(executor, database, db,
            "SELECT a.v / b.v AS x FROM t a CROSS JOIN t b WHERE a.id = 2 AND b.id = 3"));
    }

    [Test]
    public async Task AbsOfTheMinimum_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = AssertOutOfRange("abs(min)", async () => await Query(executor, database, db, $"SELECT abs({Min}) AS x"));
        StringAssert.Contains("Function 'abs' integer overflow", ex.Message);
        Assert.AreEqual(long.MaxValue, (await Scalar(executor, database, db, $"SELECT abs(-{Max}) AS x")).LongValue);
    }

    // ── Literals and conversions ────────────────────────────────────────────

    [Test]
    public async Task IntegerLiteral_PastTheRange_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertOutOfRange("max + 1 literal", async () => await Query(executor, database, db, "SELECT 9223372036854775808 AS x"));
        AssertOutOfRange("min - 1 literal", async () => await Query(executor, database, db, "SELECT -9223372036854775809 AS x"));
        AssertOutOfRange("a 30-digit literal", async () => await Query(executor, database, db, "SELECT 123456789012345678901234567890 AS x"));

        Assert.AreEqual(long.MinValue, (await Scalar(executor, database, db, $"SELECT {Min} AS x")).LongValue);
        Assert.AreEqual(long.MaxValue, (await Scalar(executor, database, db, $"SELECT {Max} AS x")).LongValue);
    }

    [Test]
    public async Task CastToInt64_PastTheRange_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string cast in new[]
        {
            "CAST(9.3e18 AS int64)",
            "CAST(-9.3e18 AS int64)",
            "CAST('9223372036854775808' AS int64)",
            "CAST('-9223372036854775809' AS int64)",
            "CAST(NUMERIC '9223372036854775808' AS int64)",
            "CAST(NUMERIC '9223372036854775807.5' AS int64)",
            "CAST(CAST('9223372036854775808' AS jsonb) AS int64)",
            // Past the NUMERIC range too: the target is int64, so the int64 error describes it.
            "CAST(CAST('1e40' AS jsonb) AS int64)",
        })
        {
            AssertOutOfRange(cast, async () => await Query(executor, database, db, $"SELECT {cast} AS x"));
        }

        // Text that is not an integer stays a conversion error, not a range error.
        CamusDBException notANumber = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, database, db, "SELECT CAST('12abc' AS int64) AS x"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, notANumber.Code, notANumber.Message);

        Assert.AreEqual(long.MaxValue, (await Scalar(executor, database, db, "SELECT CAST('9223372036854775807' AS int64) AS x")).LongValue);
        Assert.AreEqual(long.MinValue, (await Scalar(executor, database, db, "SELECT CAST(NUMERIC '-9223372036854775808' AS int64) AS x")).LongValue);
        Assert.AreEqual(42L, (await Scalar(executor, database, db, "SELECT CAST(CAST('42' AS jsonb) AS int64) AS x")).LongValue);
    }

    // ── SUM ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Sum_PastTheRange_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith(Max, "1");

        AssertOutOfRange("global SUM", async () => await Query(executor, database, db, "SELECT SUM(v) AS x FROM t"));
        AssertOutOfRange("grouped SUM", async () => await Query(executor, database, db, "SELECT g, SUM(v) AS x FROM t GROUP BY g"));
        AssertOutOfRange("SUM of a derived table", async () => await Query(executor, database, db,
            $"SELECT SUM(x) AS x FROM (SELECT {Max} AS x UNION ALL SELECT 1 AS x) q"));
        AssertOutOfRange("SUM in HAVING", async () => await Query(executor, database, db,
            "SELECT g FROM t GROUP BY g HAVING SUM(v) > 0"));
    }

    /// <summary>
    /// Only the final total is checked, so the result does not depend on the order of the rows: a
    /// running total that passes the range and comes back gives the exact total, whichever row comes
    /// first. AVG never overflows: it divides the exact total.
    /// </summary>
    [Test]
    public async Task Sum_ThatPassesTheRangeAndComesBack_GivesTheExactTotal()
    {
        // max + max passes the range; - max - 5 brings the total back to max - 5.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith(Max, Max, "-" + Max, "-5");

        ColumnValue sum = await Scalar(executor, database, db, "SELECT SUM(v) AS x FROM t");
        Assert.AreEqual(ColumnType.Integer64, sum.Type);
        Assert.AreEqual(long.MaxValue - 5, sum.LongValue);

        List<QueryResultRow> grouped = await Query(executor, database, db, "SELECT g, SUM(v) AS x FROM t GROUP BY g");
        Assert.AreEqual(1, grouped.Count);
        Assert.AreEqual(long.MaxValue - 5, grouped[0].Row["x"].LongValue);

        // The same rows in the other order: the negative rows first.
        (string dbR, DatabaseDescriptor databaseR, CommandExecutor executorR) = await TableWith("-5", "-" + Max, Max, Max);
        Assert.AreEqual(long.MaxValue - 5, (await Scalar(executorR, databaseR, dbR, "SELECT SUM(v) AS x FROM t")).LongValue);

        // 2·max fits nowhere in int64, but the average of max and max is max.
        (string db2, DatabaseDescriptor database2, CommandExecutor executor2) = await TableWith(Max, Max);
        ColumnValue average = await Scalar(executor2, database2, db2, "SELECT AVG(v) AS x FROM t");
        Assert.AreEqual(ColumnType.Float64, average.Type);
        Assert.AreEqual((double)long.MaxValue, average.FloatValue);
    }

    [Test]
    public async Task WindowSum_PastTheRange_IsOutOfRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith(Max, "1", "-1");

        AssertOutOfRange("running SUM", async () => await Query(executor, database, db,
            "SELECT id, SUM(v) OVER (ORDER BY id) AS x FROM t"));
        AssertOutOfRange("partition SUM", async () => await Query(executor, database, db,
            "SELECT id, SUM(v) OVER (PARTITION BY g) AS x FROM t WHERE id <= 2"));

        // Every frame of one row before and the current row: (max), (max, 1) overflows.
        AssertOutOfRange("sliding SUM", async () => await Query(executor, database, db,
            "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x FROM t"));

        // The whole partition passes the range and comes back: max + 1 - 1.
        List<QueryResultRow> whole = await Query(executor, database, db,
            "SELECT id, SUM(v) OVER (PARTITION BY g) AS x FROM t");
        Assert.AreEqual(3, whole.Count);
        Assert.IsTrue(whole.All(r => r.Row["x"].LongValue == long.MaxValue), "the exact partition total");

        // The frames of the current row only never overflow.
        List<QueryResultRow> single = await Query(executor, database, db,
            "SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS x FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { long.MaxValue, 1L, -1L }, single.Select(r => r.Row["x"].LongValue).ToArray());
    }

    // ── Writes ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Update_WithAnOverflowingSet_FailsAndChangesNoRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith("1", "2", "3", "0");

        // The probe case: every row overflows, and the statement reported MODIFIED 4 before.
        AssertOutOfRange("every row overflows", async () => await Dml(executor, database, db, $"UPDATE t SET v = v + {Max} WHERE id > 0"));

        // Only the last row overflows: the rows before it are not written either.
        AssertOutOfRange("one row overflows", async () => await Dml(executor, database, db, $"UPDATE t SET v = v * {Max} WHERE id > 0"));

        CollectionAssert.AreEqual(new[] { (1L, 1L), (2L, 2L), (3L, 3L), (4L, 0L) }, await Pairs(executor, database, db));
    }

    [Test]
    public async Task Update_InAnExplicitTransaction_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith("1", "2");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "UPDATE t SET v = 100 WHERE id = 1", null));

            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE t SET v = v + {Max} WHERE id = 2", null)))!;
            Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, ex.Code, ex.Message);

            // A failed statement aborts the transaction, so the earlier UPDATE does not commit either.
            Assert.ThrowsAsync<CamusDBException>(async () => await database.Transactions.CommitAsync(tx));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        CollectionAssert.AreEqual(new[] { (1L, 1L), (2L, 2L) }, await Pairs(executor, database, db));
    }

    [Test]
    public async Task Insert_WithAnOverflowingValue_StoresNothing()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith("1");

        AssertOutOfRange("VALUES", async () => await Dml(executor, database, db,
            $"INSERT INTO t (id, v, g) VALUES (2, 2, 1), (3, {Max} + 1, 1)"));
        AssertOutOfRange("INSERT … SELECT", async () => await Dml(executor, database, db,
            $"INSERT INTO t (id, v, g) SELECT id + 10, v * {Max} * 2, g FROM t"));

        CollectionAssert.AreEqual(new[] { (1L, 1L) }, await Pairs(executor, database, db));
    }

    // ── Predicates ──────────────────────────────────────────────────────────

    /// <summary>
    /// A wrapped <c>max + 1</c> is negative, so <c>v + 1 &gt; 0</c> would be false and the row would be
    /// silently left out (or, for <c>&lt; 0</c>, silently included). The comparison raises the error
    /// instead, and so does a DELETE whose predicate overflows.
    /// </summary>
    [Test]
    public async Task OverflowInAPredicate_RaisesTheError()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWith(Max, "1");

        AssertOutOfRange("WHERE v + 1 > 0", async () => await Query(executor, database, db, "SELECT id FROM t WHERE v + 1 > 0"));
        AssertOutOfRange("WHERE v + 1 < 0", async () => await Query(executor, database, db, "SELECT id FROM t WHERE v + 1 < 0"));
        AssertOutOfRange("WHERE id = max + 1", async () => await Query(executor, database, db, $"SELECT id FROM t WHERE id = {Max} + 1"));
        AssertOutOfRange("DELETE WHERE v * 2 < 0", async () => await Dml(executor, database, db, "DELETE FROM t WHERE v * 2 < 0"));

        CollectionAssert.AreEqual(new[] { (1L, long.MaxValue), (2L, 1L) }, await Pairs(executor, database, db));
    }

    // ── Sequences ───────────────────────────────────────────────────────────

    [Test]
    public async Task Sequence_AtTheMaximum_IsExhaustedNotWrapped()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE SEQUENCE s START WITH 9223372036854775806");

        Assert.AreEqual(long.MaxValue - 1, (await Scalar(executor, database, db, "SELECT nextval('s') AS x")).LongValue);
        Assert.AreEqual(long.MaxValue, (await Scalar(executor, database, db, "SELECT nextval('s') AS x")).LongValue);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, database, db, "SELECT nextval('s') AS x"))!;
        Assert.AreEqual(CamusDBErrorCodes.SequenceExhausted, ex.Code, ex.Message);
    }
}

/// <summary>Integer64 overflow on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInt64Overflow : Int64OverflowTests;

/// <summary>
/// Integer64 overflow on a cluster-mode engine, where the schema is a replicated log and a write runs
/// through the cluster commit path. The arithmetic rules are the same; the write tests show that a
/// statement that overflows stores nothing there too.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInt64OverflowCluster : Int64OverflowTests
{
    private EmbeddedKahuna? node;
    private string storageRoot = "";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        storageRoot = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"camus-int64-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(storageRoot);
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            NodeName = $"test-int64-{Guid.NewGuid():N}",
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
/// <see cref="Int64Math"/> detects overflow with sign-bit tests and a 128-bit multiply, not with
/// <c>checked</c> arithmetic, so the checks inline into the evaluator. This compares every result
/// with the exact <see cref="Int128"/> result, over the boundary values and a fixed random sample.
/// </summary>
[TestFixture]
public sealed class TestInt64Math
{
    private static readonly long[] Boundaries =
    [
        long.MinValue, long.MinValue + 1, long.MinValue / 2, -4294967296L, -3037000500L, -3037000499L,
        -2, -1, 0, 1, 2, 3037000499L, 3037000500L, 4294967296L, long.MaxValue / 2, long.MaxValue - 1, long.MaxValue,
    ];

    private static IEnumerable<long> Operands()
    {
        foreach (long value in Boundaries)
            yield return value;

        Random random = new(20261101);
        for (int i = 0; i < 200; i++)
            yield return random.NextInt64(long.MinValue, long.MaxValue);
    }

    private static void AssertMatches(string symbol, long l, long r, Int128 exact, Func<long> checkedResult)
    {
        if (exact >= long.MinValue && exact <= long.MaxValue)
        {
            Assert.AreEqual((long)exact, checkedResult(), $"{l} {symbol} {r}");
            return;
        }

        CamusDBException ex = Assert.Throws<CamusDBException>(() => checkedResult(), $"{l} {symbol} {r}")!;
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, ex.Code);
    }

    [Test]
    public void EveryOperation_MatchesTheExactResult()
    {
        long[] operands = Operands().ToArray();

        foreach (long l in operands)
        {
            foreach (long r in operands)
            {
                AssertMatches("+", l, r, (Int128)l + r, () => Int64Math.Add(l, r));
                AssertMatches("-", l, r, (Int128)l - r, () => Int64Math.Subtract(l, r));
                AssertMatches("*", l, r, (Int128)l * r, () => Int64Math.Multiply(l, r));

                if (r != 0)
                    AssertMatches("/", l, r, (Int128)l / r, () => Int64Math.Divide(l, r));
            }

            AssertMatches("unary -", l, 0, -(Int128)l, () => Int64Math.Negate(l));
        }
    }

    [Test]
    public void DecimalIntegerText_IsRecognized()
    {
        foreach (string text in new[] { "0", "9223372036854775808", "-9223372036854775809", "+12", " 42 " })
            Assert.IsTrue(Int64Math.IsDecimalIntegerText(text), text);

        foreach (string text in new[] { "", "-", "0x1F", "1.5", "1e3", "12abc", "1_000" })
            Assert.IsFalse(Int64Math.IsDecimalIntegerText(text), text);
    }
}
