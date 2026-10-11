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
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Single-value seeks into a descending index: IN lists, join-leaf IN lists, IN-subquery
/// semi-joins and anti-joins, a DELETE located by an IN list, and the Serializable range lock an
/// IN-list scan takes. A descending column encodes its keys in reverse, so a seek bounded by the
/// ascending successor <c>[v, v+1)</c> is an empty range there and silently returns no rows. Only
/// a value with a successor is exposed (INT64, FLOAT64, NUMERIC, UUID, BOOL <c>false</c>); Oid and
/// BOOL <c>true</c> have none and are kept as controls. Every case compares the index result with
/// the same query run before the index exists, and checks the plan, so a case cannot pass by
/// falling back to a table scan.
/// </summary>
internal sealed class TestDescendingIndexSeeks : SharedNodeBaseTest
{
    private const int DistinctValues = 20;

    private static string Literal(string type, int i) => type switch
    {
        "int64" => i.ToString(CultureInfo.InvariantCulture),
        "float64" => i.ToString(CultureInfo.InvariantCulture) + ".5",
        "numeric" => $"NUMERIC '{i}.25'",
        "uuid" => $"'00000000-0000-4000-8000-{i:D12}'",
        "oid" => $"'6849f3a1c2e7d50b4f8a{i:x4}'",
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };

    private static string InList(string type, params int[] values) =>
        string.Join(", ", values.Select(v => Literal(type, v)));

    /// <summary>
    /// Creates <c>t(id, v, w)</c>. A non-unique table holds two rows per value (different
    /// <c>w</c>), so a seek must return every row-id suffix of a key; a unique table holds one.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> Setup(string type, bool unique)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname,
            $"CREATE TABLE t (id oid PRIMARY KEY, v {type} NOT NULL, w int64 NOT NULL)");

        List<string> rows = new();
        for (int i = 1; i <= DistinctValues; i++)
        {
            rows.Add($"(gen_id(), {Literal(type, i)}, {i})");
            if (!unique)
                rows.Add($"(gen_id(), {Literal(type, i)}, {i + 100})");
        }

        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO t (id, v, w) VALUES " + string.Join(", ", rows));

        return (dbname, db, executor);
    }

    private static async Task AssertIndexServesSameRows(
        CommandExecutor executor, DatabaseDescriptor db, string dbname,
        string query, string indexDdl, int expectedRows, string expectedNode, Func<Task>? afterIndex = null)
    {
        string before = await IndexDifferentialProbe.QueryRendered(executor, db, dbname, query);
        int beforeCount = before.Length == 0 ? 0 : before.Split(';').Length;
        Assert.AreEqual(expectedRows, beforeCount, $"Table scan returned the wrong row count for: {query}");

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, indexDdl);

        if (afterIndex is not null)
            await afterIndex();

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, db, dbname, query);
        Assert.IsTrue(nodes.Any(n => n.StartsWith(expectedNode, StringComparison.Ordinal)),
            $"Expected a `{expectedNode}` node for `{query}` after `{indexDdl}`; got: {string.Join(", ", nodes)}");

        string after = await IndexDifferentialProbe.QueryRendered(executor, db, dbname, query);
        Assert.AreEqual(before, after, $"After `{indexDdl}` the index returned different rows for: {query}");
    }

    // ── IN list on a single-column index ──────────────────────────────────────────────────

    [Test, NonParallelizable, Combinatorial]
    public async Task InList_SameRowsThroughTheIndex(
        [Values("int64", "float64", "numeric", "uuid", "oid")] string type,
        [Values("DESC", "ASC")] string direction,
        [Values(false, true)] bool unique)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup(type, unique);

        string query = $"SELECT id, v, w FROM t WHERE v IN ({InList(type, 3, 7, 15)})";
        string indexDdl = $"CREATE {(unique ? "UNIQUE " : "")}INDEX v_idx ON t (v {direction})";

        await AssertIndexServesSameRows(executor, db, dbname, query, indexDdl,
            expectedRows: unique ? 3 : 6, expectedNode: "index-in-list");
    }

    /// <summary>
    /// The extreme values of the key range: the largest value encodes first in a descending index
    /// and the smallest last, so a seek that only works in the middle of the keyspace fails here.
    /// </summary>
    [Test, NonParallelizable, Combinatorial]
    public async Task InList_FirstAndLastKeys_SameRowsThroughTheIndex(
        [Values("int64", "uuid")] string type,
        [Values("DESC", "ASC")] string direction)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup(type, unique: false);

        string query = $"SELECT id, v, w FROM t WHERE v IN ({InList(type, 1, DistinctValues)})";

        await AssertIndexServesSameRows(executor, db, dbname, query,
            $"CREATE INDEX v_idx ON t (v {direction})", expectedRows: 4, expectedNode: "index-in-list");
    }

    [Test, NonParallelizable, Combinatorial]
    public async Task InList_Bool_SameRowsThroughTheIndex(
        [Values("DESC", "ASC")] string direction,
        [Values("false", "true", "true, false")] string inList)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname,
            "CREATE TABLE t (id oid PRIMARY KEY, v bool NOT NULL)");

        List<string> rows = new();
        for (int i = 0; i < 6; i++)
            rows.Add($"(gen_id(), {(i < 2 ? "true" : "false")})");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO t (id, v) VALUES " + string.Join(", ", rows));

        int expected = inList switch { "false" => 4, "true" => 2, _ => 6 };

        await AssertIndexServesSameRows(executor, db, dbname, $"SELECT id, v FROM t WHERE v IN ({inList})",
            $"CREATE INDEX v_idx ON t (v {direction})", expected, "index-in-list");
    }

    // ── IN list on the leading column of a composite index (a prefix seek) ─────────────────

    [Test, NonParallelizable]
    [TestCase("v DESC, w")]
    [TestCase("v DESC, w DESC")]
    [TestCase("v, w DESC")]
    public async Task InList_CompositeIndexPrefix_SameRowsThroughTheIndex(string columns)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup("int64", unique: false);

        await AssertIndexServesSameRows(executor, db, dbname, "SELECT id, v, w FROM t WHERE v IN (2, 9, 20)",
            $"CREATE INDEX vw_idx ON t ({columns})", expectedRows: 6, expectedNode: "index-in-list");
    }

    // ── DML located by an IN list ─────────────────────────────────────────────────────────

    [Test, NonParallelizable, Combinatorial]
    public async Task Delete_InListOnDescendingIndex_DeletesEveryMatchingRow(
        [Values("int64", "numeric", "uuid")] string type)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup(type, unique: false);
        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE INDEX v_idx ON t (v DESC)");

        await IndexDifferentialProbe.ExecDML(executor, db, dbname, $"DELETE FROM t WHERE v IN ({InList(type, 4, 11)})");

        string remaining = await IndexDifferentialProbe.QueryRendered(executor, db, dbname,
            $"SELECT id FROM t WHERE v IN ({InList(type, 4, 11)}) OR w < 0");
        Assert.AreEqual("", remaining, "DELETE ... WHERE v IN (...) must remove every row with a listed value");

        string all = await IndexDifferentialProbe.QueryRendered(executor, db, dbname, "SELECT id FROM t");
        Assert.AreEqual(DistinctValues * 2 - 4, all.Split(';').Length, "DELETE must remove only the listed values");
    }

    // ── Join leaf IN list and IN-subquery probes ──────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupJoin()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup("int64", unique: false);

        await IndexDifferentialProbe.ExecDDL(executor, db, dbname,
            "CREATE TABLE u (id oid PRIMARY KEY, k int64 NOT NULL)");

        // k = 5, 10, ..., 40: half of the values exist in t (1..20), half do not.
        List<string> rows = new();
        for (int k = 5; k <= 40; k += 5)
            rows.Add($"(gen_id(), {k})");
        await IndexDifferentialProbe.ExecDML(executor, db, dbname,
            "INSERT INTO u (id, k) VALUES " + string.Join(", ", rows));

        return (dbname, db, executor);
    }

    [Test, NonParallelizable]
    public async Task JoinLeaf_InListOnDescendingIndex_SameRowsThroughTheIndex()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupJoin();

        // The join column t.w has no index but u.k does, so t can be the outer leaf of an index
        // nested-loop join, scanned by the IN list on t.v. On tables this small the planner picks
        // a hash join that reads t as its build input, so the statistics are seeded to the shape
        // that makes the IN-list leaf the cheaper plan: large tables and a selective v. Rows of t
        // with v in (5, 10, 13) have w in (5, 105, 10, 110, 13, 113), and u.k matches 5 and 10.
        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, "CREATE INDEX k_idx ON u (k)");

        async Task SeedStatistics()
        {
            TableDescriptor t = await executor.OpenTable(new OpenTableTicket(dbname, "t"));
            TableDescriptor u = await executor.OpenTable(new OpenTableTicket(dbname, "u"));
            executor.Statistics.SeedRowCountForTesting(db, t, 10_000);
            executor.Statistics.SeedRowCountForTesting(db, u, 10_000);
            await executor.Statistics.SetNdvAsync(db, t, columnNdv: new Dictionary<string, long> { ["v"] = 5_000 }, keyNdv: null);
            await executor.Statistics.SetNdvAsync(db, u, columnNdv: new Dictionary<string, long> { ["k"] = 10_000 }, keyNdv: null);
        }

        await AssertIndexServesSameRows(executor, db, dbname,
            "SELECT t.v, u.k FROM t JOIN u ON t.w = u.k WHERE t.v IN (5, 10, 13)",
            "CREATE INDEX v_idx ON t (v DESC)", expectedRows: 2, expectedNode: "index-in-list", afterIndex: SeedStatistics);
    }

    [Test, NonParallelizable]
    [TestCase("IN", "semi-join", 4)]
    [TestCase("NOT IN", "anti-join", 4)]
    public async Task InSubquery_ProbeIntoDescendingIndex_SameRowsThroughTheIndex(string op, string node, int expected)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupJoin();

        await AssertIndexServesSameRows(executor, db, dbname,
            $"SELECT k FROM u WHERE k {op} (SELECT v FROM t)",
            "CREATE INDEX v_idx ON t (v DESC)", expected, node);
    }

    // ── Serializable range lock of an IN-list scan ────────────────────────────────────────

    /// <summary>
    /// A Serializable read-write IN-list scan locks the index range between its smallest and
    /// largest value. On a descending index the largest value encodes first; if the bounds stay in
    /// value order the lock covers an inverted range and fences nothing. An insert inside the range
    /// must be refused, and one outside it must succeed, which shows the lock is bounded.
    /// </summary>
    [Test, NonParallelizable]
    [TestCase("DESC")]
    [TestCase("ASC")]
    public async Task SerializableInListScan_LocksTheRangeBetweenItsValues(string direction)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await Setup("int64", unique: false);
        await IndexDifferentialProbe.ExecDDL(executor, db, dbname, $"CREATE INDEX v_idx ON t (v {direction})");

        string query = "SELECT id FROM t WHERE v IN (5, 12)";
        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, db, dbname, query);
        Assert.IsTrue(nodes.Any(n => n.StartsWith("index-in-list", StringComparison.Ordinal)),
            $"Expected an index-in-list plan; got: {string.Join(", ", nodes)}");

        KvTransaction scanner = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(scanner, dbname, query, null));
            Assert.AreEqual(4, (await cursor.ToListAsync()).Count, "Pre-check: the scan must see both values");

            KvTransaction inside = await db.Transactions.BeginAsync();
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(inside, dbname, "INSERT INTO t (id, v, w) VALUES (gen_id(), 8, 1000)", null)));
            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
                "An insert between the IN values must be refused while the Serializable scan holds its range lock");
            await db.Transactions.RollbackAsync(inside);

            KvTransaction outside = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(outside, dbname, "INSERT INTO t (id, v, w) VALUES (gen_id(), 17, 1001)", null));
            await db.Transactions.CommitAsync(outside);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(scanner);
        }
    }
}
