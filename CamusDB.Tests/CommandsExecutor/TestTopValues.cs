/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>ANALYZE</c> keeps the most common values of a column next to its histogram, and the
/// estimates read them. The data has 2,000 rows, so a histogram bucket holds 20 rows: twenty
/// popular keys hold 15 rows each, below a bucket, and the other 1,700 rows hold one key each. The
/// whole-bucket rule sees no heavy value here; the list names the twenty.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTopValues : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int PopularKeys = 20;
    private const int RowsPerPopularKey = 15;
    private const int Rows = 2000;

    [Test]
    public async Task Analyze_KeepsTheMostCommonValues_WithTheirRows()
    {
        Db db = await CreateTables(Options);

        List<ColumnTopValue>? keys = (await Histogram(db, "events", "key_id"))?.TopValues;
        Assert.That(keys, Is.Not.Null, "the join key has a list");
        Assert.That(keys!.Count, Is.EqualTo(PopularKeys), "the twenty popular keys; a unique key holds one row and is not common");
        Assert.That(keys.Select(v => v.Value!.LongValue), Is.EqualTo(Enumerable.Range(1, PopularKeys).Select(i => (long)i)), "in ascending value order");
        Assert.That(keys.All(v => v.Rows == RowsPerPopularKey), Is.True, "each with its counted rows");

        List<ColumnTopValue>? statuses = (await Histogram(db, "events", "status"))?.TopValues;
        Assert.That(statuses, Is.Not.Null);
        Assert.That(statuses!.Select(v => (v.Value!.StrValue, v.Rows)), Is.EqualTo(new[] { ("fail", 60L), ("ok", 1800L), ("warn", 140L) }),
            "a column with fewer distinct values than the limit lists every value: the exact distribution");

        Assert.That((await Histogram(db, "events", "id"))?.TopValues, Is.Null, "a unique column lists nothing");
    }

    [Test]
    public async Task Equality_OnACommonValueBelowABucket_ReadsItsRows()
    {
        Db db = await CreateTables(Options);

        Assert.That(await FilterEstimate(db, "SELECT id FROM events WHERE tag = 7"), Is.InRange(12, 18), "15 rows; 1/NDV would say 1");
        Assert.That(await FilterEstimate(db, "SELECT id FROM events WHERE tag = 500"), Is.InRange(1, 2), "a value outside the list: the residual, one row");
        Assert.That(await FilterEstimate(db, "SELECT id FROM events WHERE tag IN (3, 4, 5)"), Is.InRange(40, 50), "the sum of three listed values");
    }

    [Test]
    public async Task Join_OfTwoColumnsSkewedOnTheSameValues_IsSizedFromTheLists()
    {
        Db db = await CreateTables(Options);

        // 20 × 15 × 15 + 1,700 = 6,200 rows; the textbook 2,000 × 2,000 / 1,720 = 2,326.
        Assert.That(await JoinEstimate(db), Is.InRange(5000, 7500));
    }

    [Test]
    public async Task WithTheListOff_TheJoinTakesTheTextbookEstimate()
    {
        Db db = await CreateTables(Options with { StatsTopValues = 0 });

        Assert.That((await Histogram(db, "events", "key_id"))?.TopValues, Is.Null);
        Assert.That(await JoinEstimate(db), Is.InRange(1800, 2900));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { CostBasedJoinOrderEnabled = true, PlanCacheEnabled = false });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE events (id int64 primary key, key_id int64, tag int64, status string)");
        await Ddl(db, "CREATE INDEX events_key ON events (key_id)");
        await Ddl(db, "CREATE TABLE facts (id int64 primary key, key_id int64)");
        await Ddl(db, "CREATE INDEX facts_key ON facts (key_id)");

        List<string> events = new(Rows);
        List<string> facts = new(Rows);
        for (int i = 0; i < Rows; i++)
        {
            // The first 300 rows: keys 1..20, 15 rows each; the rest: one row per key from 21 on.
            long key = i < PopularKeys * RowsPerPopularKey ? 1 + i % PopularKeys : PopularKeys + 1 + (i - PopularKeys * RowsPerPopularKey);
            string status = i % 100 < 90 ? "ok" : i % 100 < 97 ? "warn" : "fail";
            events.Add($"({i}, {key}, {key}, '{status}')");
            facts.Add($"({i}, {key})");
        }

        for (int start = 0; start < Rows; start += 250)
        {
            await Exec(db, "INSERT INTO events (id, key_id, tag, status) VALUES " + string.Join(", ", events.Skip(start).Take(250)));
            await Exec(db, "INSERT INTO facts (id, key_id) VALUES " + string.Join(", ", facts.Skip(start).Take(250)));
        }

        await Query(db, "ANALYZE events");
        await Query(db, "ANALYZE facts");
        return db;
    }

    private static async Task<ColumnHistogram?> Histogram(Db db, string table, string column)
    {
        if (!db.Database.TableDescriptors.TryGetValue(table, out AsyncLazy<TableDescriptor>? lazy))
            throw new InvalidOperationException($"table '{table}' not found");

        return db.Executor.Statistics.GetColumnHistogram(db.Database, await lazy, column);
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

    /// <summary>The largest <c>estimated_rows</c> of the filter nodes of the plan of <paramref name="sql"/>.</summary>
    private static async Task<long> FilterEstimate(Db db, string sql)
    {
        List<QueryResultRow> plan = await Query(db, "EXPLAIN " + sql);
        List<long> filters = plan
            .Where(r => r.Row.TryGetValue("node", out ColumnValue? node) && node.StrValue == "filter")
            .Where(r => r.Row.TryGetValue("estimated_rows", out ColumnValue? rows) && rows.Type == ColumnType.Integer64)
            .Select(r => r.Row["estimated_rows"].LongValue)
            .ToList();

        Assert.IsNotEmpty(filters, "the plan must filter the column without an index:\n" + PlanText(plan));
        return filters.Max();
    }

    /// <summary>The join-order search's row estimate of the join of the two tables on their keys.</summary>
    private static async Task<long> JoinEstimate(Db db)
    {
        List<QueryResultRow> plan = await Query(db, "EXPLAIN (ANALYZE) SELECT e.id FROM events e, facts f WHERE e.key_id = f.key_id");

        foreach (QueryResultRow row in plan)
        {
            string node = row.Row["node"].StrValue ?? "";
            Match match = Regex.Match(row.Row["detail"].StrValue ?? "", @"join-order-est-rows=(\d+)");
            if (node.Contains("join") && match.Success)
                return long.Parse(match.Groups[1].Value);
        }

        Assert.Fail("no join node with a search estimate:\n" + PlanText(plan));
        return 0;
    }

    private static string PlanText(List<QueryResultRow> plan) =>
        string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));

    private static async Task<List<QueryResultRow>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
