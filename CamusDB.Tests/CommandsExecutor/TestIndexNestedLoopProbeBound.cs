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
/// A non-unique index nested-loop probe seeks one key with the same bound as every other
/// single-value seek, so it visits the entries of its key and nothing past them. Ten parents with
/// thirty children each: three probes read exactly ninety entries, on an ascending int64 index, a
/// descending int64 index, and a string key that has no successor value.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestIndexNestedLoopProbeBound : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>The heuristic join order keeps the parents on the outer side.</summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false };

    [TestCase(false, false, TestName = "Join_NonUniqueProbe_ReadsOnlyTheEntriesOfTheKey(ascending int64)")]
    [TestCase(true, false, TestName = "Join_NonUniqueProbe_ReadsOnlyTheEntriesOfTheKey(descending int64)")]
    [TestCase(false, true, TestName = "Join_NonUniqueProbe_ReadsOnlyTheEntriesOfTheKey(string, no successor)")]
    public async Task Join_NonUniqueProbe_ReadsOnlyTheEntriesOfTheKey(bool descending, bool stringKey)
    {
        Db db = await CreateTables(descending);
        await Analyze(db, "parents");
        await Analyze(db, "children");
        db.Executor.Statistics.ForceIndexNestedLoopForTesting = true;

        string on = stringKey ? "c.parent_code = p.code" : "c.parent_id = p.id";
        string sql = $"SELECT p.id AS parent, c.id AS child FROM parents p JOIN children c ON {on} WHERE p.id <= 3";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(db, sql);
        Assert.AreEqual(90, rows.Count, "parents 1 to 3, thirty children each");
        Assert.AreEqual(90, rows.Select(r => r["child"].LongValue).Distinct().Count(), "every child once");

        List<IReadOnlyDictionary<string, ColumnValue>> plan = await Query(db, "EXPLAIN (ANALYZE) " + sql);
        IReadOnlyDictionary<string, ColumnValue> probe = plan.Single(r => r["node"].StrValue == "index-nested-loop-join");

        Assert.AreEqual(90, probe["kv_scan_entries"].LongValue, "three probes of thirty entries, and no entry past the key");
        Assert.AreEqual(90, probe["rows_read"].LongValue);
    }

    // ─── Setup ───────────────────────────────────────────────────────────────

    /// <summary>
    /// parents(id, code) with 10 rows and children(id, parent_id, parent_code) with 300 rows,
    /// thirty for each parent, with a non-unique index on each child key column; the int64 index
    /// is descending when <paramref name="descending"/> is set (a string column cannot be).
    /// </summary>
    private async Task<Db> CreateTables(bool descending)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE parents (id int64 primary key, code string)");
        await Ddl(db, "CREATE TABLE children (id int64 primary key, parent_id int64, parent_code string)");
        await Ddl(db, $"CREATE INDEX children_parent_idx ON children (parent_id{(descending ? " DESC" : "")})");
        await Ddl(db, "CREATE INDEX children_code_idx ON children (parent_code)");

        await Exec(db, "INSERT INTO parents (id, code) VALUES " +
            string.Join(", ", Enumerable.Range(1, 10).Select(i => $"({i}, 'p{i}')")));

        for (int batch = 0; batch < 3; batch++)
        {
            await Exec(db, "INSERT INTO children (id, parent_id, parent_code) VALUES " +
                string.Join(", ", Enumerable.Range(batch * 100 + 1, 100).Select(i => $"({i}, {(i - 1) % 10 + 1}, 'p{(i - 1) % 10 + 1}')")));
        }

        return db;
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

    /// <summary>Runs <c>ANALYZE</c> on <paramref name="table"/>, so the planner has row counts and distinct counts.</summary>
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
