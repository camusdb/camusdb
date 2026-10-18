/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An equality or an <c>IN</c> list on a skewed column reads the share of a heavy value from the
/// histogram that <c>ANALYZE</c> builds, and prices a rare value at the residual, instead of
/// <c>1 / NDV</c> for every value. The table has 2,000 rows: <c>status</c> holds four values with
/// shares of 70, 15, 10 and 5 percent, and <c>note</c> holds <c>hot</c> on 60 percent of the rows,
/// <c>warm</c> on 20 percent, and 400 values that each appear once on the rest.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestEqualityHeavyValues : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Rows = 2000;

    [Test]
    public async Task Analyze_EqualityOnAHeavyValue_ReadsItsShare()
    {
        Db db = await CreateOrders();
        await Analyze(db);

        // 1/NDV would say 500 for each of the four values.
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status = 'pending'"), Is.InRange(1200, 1600), "70 percent");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status = 'shipped'"), Is.InRange(250, 350), "15 percent");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status = 'delivered'"), Is.InRange(170, 230), "10 percent");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status = 'cancelled'"), Is.InRange(80, 120), "5 percent");
    }

    [Test]
    public async Task Analyze_EqualityOnARareValue_TakesTheResidual()
    {
        Db db = await CreateOrders();
        await Analyze(db);

        // The residual: 20 percent of the rows over 400 distinct values is one row, where 1/NDV
        // over the 402 distinct values would say 4.
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE note = 'n18'"), Is.InRange(1, 2), "a rare value");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE note = 'hot'"), Is.InRange(1000, 1400), "the heavy value of the same column");
    }

    [Test]
    public async Task Analyze_AnInListOfHeavyValues_SumsTheirShares()
    {
        Db db = await CreateOrders();
        await Analyze(db);

        // 15 + 5 percent; 1/NDV per value would say 1,000.
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status IN ('shipped', 'cancelled')"), Is.InRange(330, 470));
    }

    [Test]
    public async Task Analyze_ARangeAtAHeavyValue_CountsItsWholeMass()
    {
        Db db = await CreateOrders();
        await Analyze(db);

        // The values sort as cancelled, delivered, pending, shipped. An inclusive bound at the
        // heavy value carries its 70 percent; the exclusive forms leave it out.
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status >= 'pending'"), Is.InRange(1600, 1800), "pending and shipped");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status > 'pending'"), Is.InRange(250, 350), "shipped only");
        Assert.That(await FilterEstimate(db, "SELECT id FROM orders WHERE status < 'pending'"), Is.InRange(250, 350), "cancelled and delivered");
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateOrders()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE orders (id int64 primary key, status string, note string)");

        List<string> values = new(Rows);
        for (int i = 0; i < Rows; i++)
        {
            int slot = i % 20;
            string status = slot < 14 ? "pending" : slot < 17 ? "shipped" : slot < 19 ? "delivered" : "cancelled";
            int tenth = i % 10;
            string note = tenth < 6 ? "hot" : tenth < 8 ? "warm" : $"n{i}";
            values.Add($"({i}, '{status}', '{note}')");
        }

        for (int start = 0; start < values.Count; start += 250)
            await Exec(db, "INSERT INTO orders (id, status, note) VALUES " + string.Join(", ", values.Skip(start).Take(250)));

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

    private static async Task Analyze(Db db)
    {
        List<QueryResultRow> rows = await Query(db, "ANALYZE orders");
        Assert.AreEqual(1, rows.Count);
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

        Assert.IsNotEmpty(filters, "the plan must filter the column without an index:\n" + string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue)));
        return filters.Max();
    }

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
