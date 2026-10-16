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
/// The probe memo of an index nested-loop join: an outer stream that repeats a key probes the
/// store once per distinct key, the repeats are answered from the memo and reported as
/// <c>probe-memo-hits=</c>, and the rows are the same with the memo off. Ten parents with thirty
/// children each; the children drive every join here, so each key repeats thirty times.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestIndexNestedLoopProbeMemo : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>The heuristic join order keeps the declared outer side.</summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false };

    [Test]
    public async Task UniqueProbe_RepeatedKeys_ReadTheStoreOncePerDistinctKey()
    {
        Db db = await CreateTables();
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c JOIN parents p ON p.id = c.parent_id";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(300, rows.Count);
        Assert.AreEqual(10, rows.Select(r => r["code"].StrValue).Distinct().Count());

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(10, probe["kv_lookups"].LongValue, "ten distinct parents, one lookup each");
        Assert.AreEqual(10, probe["rows_read"].LongValue);
        StringAssert.Contains("probe-memo-hits=290", probe["detail"].StrValue);
    }

    [Test]
    public async Task NonUniqueProbe_RepeatedKeys_ScanTheIndexOncePerDistinctKey()
    {
        Db db = await CreateTables();
        // A self join on the non-unique parent index: every child meets the 30 children of its parent.
        const string sql = "SELECT c.id AS child, d.id AS sibling FROM children c JOIN children d ON d.parent_id = c.parent_id";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(9_000, rows.Count, "300 children, 30 siblings each");

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(300, probe["kv_scan_entries"].LongValue, "ten distinct keys of thirty entries, not 300 probes");
        Assert.AreEqual(300, probe["rows_read"].LongValue);
        StringAssert.Contains("probe-memo-hits=290", probe["detail"].StrValue);
    }

    [Test]
    public async Task Memo_Off_ReturnsTheSameRows_AndProbesEveryOuterRow()
    {
        Db db = await CreateTables();
        const string sql = "SELECT c.id AS child, d.id AS sibling FROM children c JOIN children d ON d.parent_id = c.parent_id";

        List<string> withMemo = Render(await Query(db, sql));
        db.Executor.Statistics.IndexProbeMemoCapacityForTesting = 0;
        List<string> withoutMemo = Render(await Query(db, sql));
        CollectionAssert.AreEqual(withoutMemo, withMemo);

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(9_000, probe["kv_scan_entries"].LongValue, "every outer row probes with the memo off");
        StringAssert.DoesNotContain("probe-memo-hits", probe["detail"].StrValue);
    }

    [Test]
    public async Task Memo_AtItsCapacity_StopsGrowing_AndStaysCorrect()
    {
        Db db = await CreateTables();
        // Room for one key of thirty rows: the first key is remembered, the other nine are probed each time.
        db.Executor.Statistics.IndexProbeMemoCapacityForTesting = 30;
        const string sql = "SELECT c.id AS child, d.id AS sibling FROM children c JOIN children d ON d.parent_id = c.parent_id";

        Assert.AreEqual(9_000, (await Query(db, sql)).Count);

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        StringAssert.Contains("probe-memo-hits=29", probe["detail"].StrValue);
        Assert.AreEqual(300 + 9 * 29 * 30, probe["kv_scan_entries"].LongValue, "the nine unmemoized keys are probed 29 more times each");
    }

    [Test]
    public async Task LeftOuterJoin_MemoizedMiss_PadsEveryRepeat()
    {
        Db db = await CreateTables();
        // orphan_id is the parent id plus 100, and no parent has such an id: every probe misses, and
        // only the first probe of each of the ten distinct keys reads the store.
        const string sql = "SELECT c.id AS child, p.code AS code FROM children c LEFT JOIN parents p ON p.id = c.orphan_id";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(300, rows.Count);
        Assert.IsTrue(rows.All(r => r["code"].Type == ColumnType.Null), "every child is padded");

        IReadOnlyDictionary<string, ColumnValue> probe = await ProbeNode(db, sql);
        Assert.AreEqual(10, probe["kv_lookups"].LongValue);
        StringAssert.Contains("probe-memo-hits=290", probe["detail"].StrValue);
    }

    // ─── Setup ───────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE parents (id int64 primary key, code string)");
        await Ddl(db, "CREATE TABLE children (id int64 primary key, parent_id int64, orphan_id int64)");
        await Ddl(db, "CREATE INDEX children_parent_idx ON children (parent_id)");

        await Exec(db, "INSERT INTO parents (id, code) VALUES " +
            string.Join(", ", Enumerable.Range(1, 10).Select(i => $"({i}, 'p{i}')")));

        for (int batch = 0; batch < 3; batch++)
        {
            await Exec(db, "INSERT INTO children (id, parent_id, orphan_id) VALUES " +
                string.Join(", ", Enumerable.Range(batch * 100 + 1, 100).Select(i => $"({i}, {(i - 1) % 10 + 1}, {(i - 1) % 10 + 101})")));
        }

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
            .Select(kv => $"{kv.Key}={(kv.Value.Type == ColumnType.Null ? "null" : kv.Value.LongValue.ToString())}"))).ToList();
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
