/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// NUMERIC columns in index keys and NUMERIC constants in index bounds. Each differential case runs
/// a query with no index (a table scan, where the evaluator decides), then creates an index and runs
/// it again; both results must equal the expected rows. The rules under test: a NUMERIC or INT64
/// bound on a NUMERIC column converts exactly and drives the index; a float bound on a NUMERIC column
/// compares as doubles (Spanner) and is left to the evaluator; a NUMERIC bound on an INT64 column
/// converts exactly with Int128 floor and ceiling, never through a double.
/// </summary>
internal sealed class TestNumericIndex : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> Setup()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname,
            "CREATE TABLE probe (id int64 PRIMARY KEY, p NUMERIC, q int64)");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO probe (id, p, q) VALUES " +
            "(1, -2.5, 1), (2, -1, 2), (3, 0, 3), (4, 0.000000001, 9007199254740993), (5, 1, 5), " +
            "(6, 1.000000001, 6), (7, 2.5, 7), (8, 12345678901234567890.123456789, 8), (9, NULL, NULL), " +
            "(10, 9007199254740993, 10)");

        return (dbname, db, executor);
    }

    private static string Ids(params long[] ids) =>
        string.Join(";", ids.Select(id => "id=" + id).OrderBy(s => s, StringComparer.Ordinal));

    private static void AssertPlanUsesIndex(string[] nodes, string query) =>
        Assert.IsTrue(nodes.Any(n => n.StartsWith("index-", StringComparison.Ordinal)),
            $"Expected an index node for `{query}`; got: {string.Join(", ", nodes)}");

    /// <summary>
    /// Runs one differential case. <paramref name="expectIndex"/> null skips the plan check (a
    /// descending index column absorbs no range bound, for any type, so whether it serves the
    /// query depends on the operator, not on NUMERIC).
    /// </summary>
    private async Task RunDifferential(string where, string indexDdl, string expected, bool? expectIndex)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup();
        string query = "SELECT id FROM probe WHERE " + where;

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(executor, db, dbname, query, indexDdl, expected);

        if (expectIndex is null)
            return;

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, db, dbname, query);
        if (expectIndex.Value)
            AssertPlanUsesIndex(nodes, query);
        else
            Assert.IsFalse(nodes.Any(n => n.StartsWith("index-", StringComparison.Ordinal)),
                $"`{query}` has no exact key range, so it must not drive the index; got: {string.Join(", ", nodes)}");
    }

    // Covering (INCLUDE id), because a non-covering one-sided range with the default selectivity
    // falls back to a table scan on any table with a row count, which would leave the index path
    // of these cases unexercised. Index 1 is descending.
    private static readonly string[] NumericIndexes =
    [
        "CREATE INDEX pi ON probe (p) INCLUDE (id)",
        "CREATE INDEX pi ON probe (p DESC) INCLUDE (id)",
        "CREATE UNIQUE INDEX pi ON probe (p) INCLUDE (id)",
    ];

    // ── NUMERIC and INT64 bounds on a NUMERIC column: exact, served by the index ───────────────

    [Test, NonParallelizable, Combinatorial]
    public async Task NumericColumn_ExactBounds_SameRowsThroughTheIndex(
        [Values(0, 1, 2)] int index,
        [Values(
            "p = NUMERIC '1'|5",
            "p = 1|5",
            "p = NUMERIC '1.000000001'|6",
            "p = NUMERIC '1.0000000005'|6",
            "p = NUMERIC '0.5'|",
            "p > 1|6,7,8,10",
            "p >= 1|5,6,7,8,10",
            "p < 0|1,2",
            "p <= NUMERIC '0.000000001'|1,2,3,4",
            "p > NUMERIC '-2.5'|2,3,4,5,6,7,8,10",
            "p BETWEEN -1 AND NUMERIC '2.5'|2,3,4,5,6,7",
            "p IN (1, NUMERIC '2.5', 3)|5,7",
            "p = 9007199254740993|10",
            "p = 9007199254740992|",
            "p >= NUMERIC '12345678901234567890.123456789'|8",
            "p > NUMERIC '12345678901234567890.123456788'|8",
            "p > NUMERIC '12345678901234567890.123456789'|")] string testCase)
    {
        string[] parts = testCase.Split('|');
        long[] ids = parts[1].Length == 0 ? [] : parts[1].Split(',').Select(long.Parse).ToArray();

        if (index == 1 && parts[0].Contains(" IN (", StringComparison.Ordinal))
            Assert.Ignore("An IN-list seek on a descending index returns no rows for every column type: the " +
                          "seek bounds each value with its ascending successor, which is an empty range in " +
                          "descending key order. The defect is outside NUMERIC; this case runs again once the seek is fixed.");

        await RunDifferential(parts[0], NumericIndexes[index], Ids(ids), expectIndex: index == 1 ? null : true);
    }

    // ── Float bounds on a NUMERIC column: compared as doubles, left to the evaluator ───────────

    [Test, NonParallelizable, Combinatorial]
    public async Task NumericColumn_FloatBounds_ComparedAsDoubles_NotByTheIndex(
        [Values(0, 1)] int index,
        [Values(
            "p > 0.5|5,6,7,8,10",
            "p = 0.1|",
            "p = 2.5|7",
            "p IN (2.5, 1.0)|5,7",
            "p < -1.5|1",
            // The double of this literal is 12345678901234567168: the stored value widens to the same
            // double, so the evaluator matches it.
            "p = 12345678901234567890.123456789|8")] string testCase)
    {
        string[] parts = testCase.Split('|');
        long[] ids = parts[1].Length == 0 ? [] : parts[1].Split(',').Select(long.Parse).ToArray();

        await RunDifferential(parts[0], NumericIndexes[index], Ids(ids), expectIndex: index == 1 ? null : false);
    }

    // ── NUMERIC bounds on an INT64 column: exact Int128 floor and ceiling ─────────────────────

    [Test, NonParallelizable]
    [TestCase("q = NUMERIC '9007199254740993'", "4", true)]
    [TestCase("q = NUMERIC '9007199254740992'", "", true)]   // a double would merge the two integers
    [TestCase("q > NUMERIC '6.5'", "4,7,8,10", true)]
    [TestCase("q >= NUMERIC '7'", "4,7,8,10", true)]
    [TestCase("q < NUMERIC '2.5'", "1,2", true)]
    [TestCase("q <= NUMERIC '-0.5'", "", true)]
    [TestCase("q IN (NUMERIC '2', NUMERIC '2.5')", "2", true)]
    // A fractional equality can match no integer: it is never rounded into a key, so the evaluator decides.
    [TestCase("q = NUMERIC '2.5'", "", false)]
    public async Task IntegerColumn_NumericBounds_SameRowsThroughTheIndex(string where, string expected, bool expectIndex)
    {
        long[] ids = expected.Length == 0 ? [] : expected.Split(',').Select(long.Parse).ToArray();
        await RunDifferential(where, "CREATE INDEX qi ON probe (q) INCLUDE (id)", Ids(ids), expectIndex);
    }

    // ── NUMERIC primary key, unique index, composite index ───────────────────────────────────

    [Test, NonParallelizable]
    public async Task NumericPrimaryKey_LookupsAndDuplicates()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE TABLE k (p NUMERIC PRIMARY KEY, name string)");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO k (p, name) VALUES (1.5, 'a'), (-1.5, 'b'), (2, 'c'), (99999999999999999999999999999.999999999, 'max')");

        Assert.AreEqual("name=a", await IndexDifferentialProbe.QueryRendered(executor, db, dbname, "SELECT name FROM k WHERE p = NUMERIC '1.5'"));
        Assert.AreEqual("name=c", await IndexDifferentialProbe.QueryRendered(executor, db, dbname, "SELECT name FROM k WHERE p = 2"));
        Assert.AreEqual("name=max", await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT name FROM k WHERE p = NUMERIC '99999999999999999999999999999.999999999'"));
        Assert.AreEqual("name=a;name=c;name=max", await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT name FROM k WHERE p > 0"));

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, db, dbname, "SELECT name FROM k WHERE p = NUMERIC '1.5'");
        AssertPlanUsesIndex(nodes, "p = NUMERIC '1.5'");

        // 1.50 is the same NUMERIC value as 1.5, so it is the same key.
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO k (p, name) VALUES (1.50, 'dup')", null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test, NonParallelizable]
    public async Task UniqueSecondaryIndex_RefusesTheSameValue()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE TABLE u (id int64 PRIMARY KEY, p NUMERIC)");
        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE UNIQUE INDEX up ON u (p)");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname, "INSERT INTO u (id, p) VALUES (1, -0.000000001)");

        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO u (id, p) VALUES (2, NUMERIC '-1e-9')", null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);

        // A different value with the same low 64 bits is a different key.
        await IndexDifferentialProbe.ExecDML(executor, db, dbname, "INSERT INTO u (id, p) VALUES (3, NUMERIC '18446744073.709551615')");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname, "INSERT INTO u (id, p) VALUES (4, NUMERIC '36893488147.419103231')");
        Assert.AreEqual("id=4", await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM u WHERE p = NUMERIC '36893488147.419103231'"));
    }

    [Test, NonParallelizable]
    public async Task CompositeIndex_NumericThenString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE TABLE c (id int64 PRIMARY KEY, p NUMERIC, name string)");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO c (id, p, name) VALUES (1, 1.5, 'a'), (2, 1.5, 'b'), (3, 2, 'a'), (4, -1, 'z')");

        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(executor, db, dbname,
            "SELECT id FROM c WHERE p = NUMERIC '1.5' AND name = 'b'", "CREATE INDEX pn ON c (p, name)", Ids(2));

        Assert.AreEqual(Ids(1, 2, 3), await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM c WHERE p >= NUMERIC '1.5'"));
        Assert.AreEqual(Ids(4, 1, 2), await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM c WHERE p < 2"));
    }

    // ── The evaluator's mixed comparison, which every scan above relies on ────────────────────

    [Test, NonParallelizable]
    public async Task Evaluator_NumericAgainstInteger_IsExactPast2Pow53()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup();

        Assert.AreEqual(Ids(10), await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM probe WHERE p = 9007199254740993"));
        Assert.AreEqual("", await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM probe WHERE p = 9007199254740992"));
        Assert.AreEqual(Ids(4), await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            "SELECT id FROM probe WHERE q = NUMERIC '9007199254740993'"));
    }
}
