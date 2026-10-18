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
/// A <c>LIKE</c> pattern is priced from the column's histogram instead of the flat fallback: a
/// pattern with a fixed prefix as the range of that prefix, and a pattern with a leading
/// wildcard as the share of the histogram's bucket bounds that match it, each bound weighted by
/// its bucket's rows. The table has 2,000 rows: <c>name</c> is a first and a last name from a
/// short word list, with 5 percent of the rows named <c>Ann ...</c> and no name that holds
/// <c>zq</c>; <c>note</c> holds <c>(voice)</c> on 30 percent of the rows, <c>(writer)</c> on 20
/// percent, <c>(voice) (uncredited)</c> on 10 percent, and a distinct value on the rest.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestPatternSelectivity : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Rows = 2000;

    [Test]
    public async Task Analyze_APrefixPattern_ReadsTheRangeOfThePrefix()
    {
        Db db = await CreatePeople();
        await Analyze(db);

        // 100 of 2,000 rows; the fallback would say 200.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE name LIKE 'Ann%'"), Is.InRange(70, 130));
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE name NOT LIKE 'Ann%'"), Is.InRange(1870, 1930), "the complement");
    }

    [Test]
    public async Task Analyze_AnInfixPattern_ReadsTheShareOfTheMatchingBounds()
    {
        Db db = await CreatePeople();
        await Analyze(db);

        // The heavy values fill whole buckets, so their bounds carry their rows: 800 of 2,000.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE note LIKE '%(voice)%'"), Is.InRange(700, 900));

        // No bound matches: half a bucket, 10 rows, where the fallback would say 200.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE name LIKE '%zq%'"), Is.InRange(1, 25));
    }

    [Test]
    public async Task Analyze_AnOrOfPatternsOnOneColumn_IsTheirUnion()
    {
        Db db = await CreatePeople();
        await Analyze(db);

        // 40 + 20 percent, disjoint values: 1,200 rows.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE note LIKE '%(voice)%' OR note LIKE '%(writer)%'"), Is.InRange(1050, 1350));
    }

    [Test]
    public async Task Analyze_TwoPatternsOnOneColumn_ArePricedTogether()
    {
        Db db = await CreatePeople();
        await Analyze(db);

        // The rows that hold both: 10 percent, 200 rows. The product of the two shares, 40 and 10
        // percent, would say 80.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE note LIKE '%(voice)%' AND note LIKE '%(uncredited)%'"), Is.InRange(150, 250));
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE note LIKE '%(voice)%' AND note NOT LIKE '%(uncredited)%'"), Is.InRange(500, 700), "voice without the uncredited ones: 30 percent");
    }

    [Test]
    public async Task Analyze_APatternNextToAnEquality_MultipliesIntoTheEstimate()
    {
        Db db = await CreatePeople();
        await Analyze(db);

        // Before the pattern estimate, the equality alone priced this filter: 600 rows. The
        // pattern keeps the rows whose name starts with Ann: 5 percent of them.
        Assert.That(await FilterEstimate(db, "SELECT id FROM people WHERE note = '(voice)' AND name LIKE 'Ann%'"), Is.InRange(15, 50));
    }

    [Test]
    public async Task WithoutAHistogram_APatternKeepsTheFallback()
    {
        Db db = await CreatePeople();

        // No ANALYZE: the flat fallback of ten percent, as before.
        Assert.AreEqual(200, await FilterEstimate(db, "SELECT id FROM people WHERE name LIKE 'Ann%'"));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static readonly string[] FirstNames = ["Bob", "Carla", "Dan", "Eve", "Finn", "Gus", "Hana", "Ivan", "Jo", "Kim"];
    private static readonly string[] LastNames = ["Stone", "River", "Night", "City", "Blue", "Red", "Green", "Dark", "Light", "Love"];

    private async Task<Db> CreatePeople()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE people (id int64 primary key, name string, note string)");

        List<string> values = new(Rows);
        for (int i = 0; i < Rows; i++)
        {
            string first = i % 20 == 0 ? "Ann" : FirstNames[i % 10];
            string person = first + " " + LastNames[(i / 10) % 10];
            int tenth = i % 10;
            string note = tenth < 3 ? "(voice)" : tenth < 5 ? "(writer)" : tenth < 6 ? "(voice) (uncredited)" : $"note {i}";
            values.Add($"({i}, '{person}', '{note}')");
        }

        for (int start = 0; start < values.Count; start += 250)
            await Exec(db, "INSERT INTO people (id, name, note) VALUES " + string.Join(", ", values.Skip(start).Take(250)));

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
        List<QueryResultRow> rows = await Query(db, "ANALYZE people");
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

        Assert.IsNotEmpty(filters, "the plan must filter the column:\n" + string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue)));
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
