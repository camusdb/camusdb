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
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The batched unique probe of an index nested-loop join: a page of outer rows (the index scan fetch
/// batch size, 64 here) probes its distinct keys with one index read and one row read, reported as
/// <c>probe-batches=</c>; the rows come out in outer order, a NULL key or a missing parent pads a
/// left outer join at its position, a key that repeats inside one page probes once, and a LIMIT
/// stops the join after the page that satisfies it.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestIndexNestedLoopProbeBatch : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>The heuristic join order keeps the declared outer side.</summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false };

    [Test]
    public async Task UniqueProbe_DistinctKeys_ProbeOnePageOfOuterRowsPerCall()
    {
        Db db = await CreateDistinctTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(280, rows.Count, "the ten NULL keys and the ten missing parents join nothing");

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(290, probe["kv_lookups"].LongValue, "every non-NULL key is probed once, the ten missing ones included");
        Assert.AreEqual(280, probe["rows_read"].LongValue);
        StringAssert.Contains("probe-batches=5", probe["detail"].StrValue, "300 outer rows in pages of 64");
        StringAssert.DoesNotContain("probe-memo-hits", probe["detail"].StrValue);
    }

    [Test]
    public async Task LeftOuterJoin_NullKeysAndMissingParents_PadAtTheirOuterPosition()
    {
        Db db = await CreateDistinctTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c LEFT JOIN parents p ON p.id = c.parent_id";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(300, rows.Count, "a left join keeps every outer row");

        CollectionAssert.AreEqual(Enumerable.Range(1, 300).Select(i => (long)i).ToList(),
            rows.Select(r => r["child"].LongValue).ToList(), "the rows keep the outer scan order across pages");

        for (int i = 1; i <= 300; i++)
        {
            ColumnValue code = rows[i - 1]["code"];
            if (i <= 280)
                Assert.AreEqual($"p{i}", code.StrValue, $"child {i} meets its parent");
            else
                Assert.AreEqual(ColumnType.Null, code.Type, $"child {i} has no parent and is padded");
        }

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(290, probe["kv_lookups"].LongValue, "a NULL key is never probed");
        StringAssert.Contains("probe-batches=5", probe["detail"].StrValue);
    }

    [Test]
    public async Task UniqueProbe_KeysThatRepeatInsideOnePage_ProbeOnce()
    {
        Db db = await CreateCyclingTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id";

        // With the memo off, every page probes the ten parents its 64 rows cycle through, once each.
        db.Executor.Statistics.IndexProbeMemoCapacityForTesting = 0;

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(300, rows.Count);

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(50, probe["kv_lookups"].LongValue, "ten distinct keys in each of five pages");
        Assert.AreEqual(50, probe["rows_read"].LongValue);
        StringAssert.Contains("probe-batches=5", probe["detail"].StrValue);
        StringAssert.Contains("probe-memo-hits=250", probe["detail"].StrValue, "a repeat inside a page shares the page's probe");
    }

    [Test]
    public async Task UniqueProbe_WithTheMemo_ProbesOnlyTheFirstPage()
    {
        Db db = await CreateCyclingTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id";

        List<string> withMemo = Render(await Query(db, sql));
        db.Executor.Statistics.IndexProbeMemoCapacityForTesting = 0;
        List<string> withoutMemo = Render(await Query(db, sql));
        CollectionAssert.AreEqual(withoutMemo, withMemo);
        db.Executor.Statistics.IndexProbeMemoCapacityForTesting = null;

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(10, probe["kv_lookups"].LongValue, "the first page probes the ten parents; the memo answers the rest");
        StringAssert.Contains("probe-batches=1", probe["detail"].StrValue, "a page with no key to probe issues no call");
        StringAssert.Contains("probe-memo-hits=290", probe["detail"].StrValue);
    }

    [Test]
    public async Task UniqueProbe_ResidualFilterOnTheRightSide_DropsTheRowAfterTheFetch()
    {
        Db db = await CreateDistinctTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id WHERE p.code <> 'p5'";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(279, rows.Count);
        Assert.IsFalse(rows.Any(r => r["child"].LongValue == 5), "child 5 meets the filtered parent and is dropped");

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(290, probe["kv_lookups"].LongValue);
        Assert.AreEqual(280, probe["rows_read"].LongValue, "the filtered row is read, then dropped");
    }

    [Test]
    public async Task UniqueProbe_Limit_StopsAfterThePageThatSatisfiesIt()
    {
        Db db = await CreateDistinctTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id LIMIT 5";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(5, rows.Count);

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(64, probe["kv_lookups"].LongValue, "the first page is probed whole, and no second page is collected");
        StringAssert.Contains("probe-batches=1", probe["detail"].StrValue);
    }

    /// <summary>
    /// 300 parents and 300 children in id order: child i has parent i up to 280, a NULL parent for
    /// 281 to 290, and a parent that does not exist for 291 to 300.
    /// </summary>
    private async Task<Db> CreateDistinctTables()
    {
        Db db = await CreateSchema();

        await Exec(db, "INSERT INTO parents (id, code) VALUES " +
            string.Join(", ", Enumerable.Range(1, 300).Select(i => $"({i}, 'p{i}')")));

        for (int batch = 0; batch < 3; batch++)
        {
            await Exec(db, "INSERT INTO children (id, parent_id) VALUES " +
                string.Join(", ", Enumerable.Range(batch * 100 + 1, 100).Select(i => $"({i}, {ParentOf(i)})")));
        }

        return await Finish(db);

        static string ParentOf(int i) => i <= 280 ? i.ToString() : i <= 290 ? "NULL" : (1000 + i).ToString();
    }

    /// <summary>Ten parents and 300 children in id order, child i with parent ((i - 1) mod 10) + 1, so every page of 64 holds every parent.</summary>
    private async Task<Db> CreateCyclingTables()
    {
        Db db = await CreateSchema();

        await Exec(db, "INSERT INTO parents (id, code) VALUES " +
            string.Join(", ", Enumerable.Range(1, 10).Select(i => $"({i}, 'p{i}')")));

        for (int batch = 0; batch < 3; batch++)
        {
            await Exec(db, "INSERT INTO children (id, parent_id) VALUES " +
                string.Join(", ", Enumerable.Range(batch * 100 + 1, 100).Select(i => $"({i}, {(i - 1) % 10 + 1})")));
        }

        return await Finish(db);
    }

    private async Task<Db> CreateSchema()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE parents (id int64 primary key, code string)");
        await Ddl(db, "CREATE TABLE children (id int64 primary key, parent_id int64)");
        return db;
    }

    private static async Task<Db> Finish(Db db)
    {
        await Analyze(db, "parents");
        await Analyze(db, "children");
        db.Executor.Statistics.ForceIndexNestedLoopForTesting = true;
        return db;
    }

    private static async Task<IReadOnlyDictionary<string, ColumnValue>> ProbeNode(Db db, string sql)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> plan = await Query(db, "EXPLAIN (ANALYZE) " + sql);
        return plan.Single(r => r["node"].StrValue == "index-nested-loop-join");
    }

    private static List<string> Render(List<IReadOnlyDictionary<string, ColumnValue>> rows)
    {
        List<string> rendered = rows.Select(r => string.Join("|", r.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}={(kv.Value.Type == ColumnType.Null ? "null" : kv.Value.Type == ColumnType.String ? kv.Value.StrValue : kv.Value.LongValue.ToString())}"))).ToList();
        rendered.Sort(StringComparer.Ordinal);
        return rendered;
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Analyze(Db db, string table)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: "ANALYZE " + table, parameters: null));
        await foreach (QueryResultRow _ in cursor) { }
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<IReadOnlyDictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row.Row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
