/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
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
/// A filter on a small dimension table names its rows, and the join search reads them at plan
/// time to price a probe through their keys at the keys' own shares of the fact column. The
/// dimension has 8 kinds; the fact table has 1,000 titles, half of them of kind 1, three tenths
/// of kind 2, and the rest spread over kinds 3 to 8. The average key holds 125 titles; the search
/// estimate of the join (<c>join-order-est-rows</c> under <c>EXPLAIN (ANALYZE)</c>) shows which
/// share it priced.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestPlanTimeLeafLookup : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const string Join = "SELECT t.id FROM kinds k, titles t WHERE k.id = t.kind_id AND ";

    [Test]
    public async Task ANamedKind_PricesTheProbeAtItsOwnShare()
    {
        Db db = await CreateTables(Options with { PlanCacheEnabled = false });

        Assert.That(await JoinEstimate(db, Join + "k.kind = 'kind 1'"), Is.InRange(400, 600), "the popular kind: half the titles");
        Assert.That(await JoinEstimate(db, Join + "k.kind = 'kind 7'"), Is.InRange(15, 60), "a rare kind: its own few titles");
        Assert.That(await JoinEstimate(db, Join + "k.kind IN ('kind 1', 'kind 2')"), Is.InRange(700, 900), "two named kinds: the sum of their shares");
    }

    [Test]
    public async Task WithTheLookupOff_TheProbeIsPricedAtTheAverageKey()
    {
        Db db = await CreateTables(Options with { PlanCacheEnabled = false, StatsPlanTimeLookupMaxRows = 0 });

        Assert.That(await JoinEstimate(db, Join + "k.kind = 'kind 1'"), Is.InRange(100, 150));
        Assert.That(await JoinEstimate(db, Join + "k.kind = 'kind 7'"), Is.InRange(100, 150));
    }

    [Test]
    public async Task ATableAboveTheRowCap_IsNotRead()
    {
        // 8 rows against a cap of 4: the statistics price the probe.
        Db db = await CreateTables(Options with { PlanCacheEnabled = false, StatsPlanTimeLookupMaxRows = 4 });

        Assert.That(await JoinEstimate(db, Join + "k.kind = 'kind 1'"), Is.InRange(100, 150));
    }

    [Test]
    public async Task ARangeFilterAlone_DoesNotNameRows_SoTheStatisticsPriceIt()
    {
        Db db = await CreateTables(Options with { PlanCacheEnabled = false });

        // Two kinds kept by a range: two average keys, 250, where the named keys would give 66.
        Assert.That(await JoinEstimate(db, Join + "k.id > 6"), Is.InRange(200, 300));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options with { CostBasedJoinOrderEnabled = true });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE kinds (id int64 primary key, kind string)");
        await Ddl(db, "CREATE TABLE titles (id int64 primary key, kind_id int64)");
        await Ddl(db, "CREATE INDEX titles_kind ON titles (kind_id)");
        await Exec(db, "INSERT INTO kinds (id, kind) VALUES " + string.Join(", ", Enumerable.Range(1, 8).Select(i => $"({i}, 'kind {i}')")));

        List<string> values = new(1000);
        for (int id = 1; id <= 1000; id++)
        {
            long kind = id <= 500 ? 1 : id <= 800 ? 2 : 3 + (id - 801) % 6;
            values.Add($"({id}, {kind})");
        }

        for (int start = 0; start < values.Count; start += 250)
            await Exec(db, "INSERT INTO titles (id, kind_id) VALUES " + string.Join(", ", values.Skip(start).Take(250)));

        await Query(db, "ANALYZE kinds");
        await Query(db, "ANALYZE titles");
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

    /// <summary>The join-order search's row estimate of the top join of <paramref name="sql"/>.</summary>
    private static async Task<long> JoinEstimate(Db db, string sql)
    {
        List<QueryResultRow> plan = await Query(db, "EXPLAIN (ANALYZE) " + sql);

        foreach (QueryResultRow row in plan)
        {
            string node = row.Row["node"].StrValue ?? "";
            string detail = row.Row["detail"].StrValue ?? "";
            Match match = Regex.Match(detail, @"join-order-est-rows=(\d+)");
            if (node.Contains("join") && match.Success)
                return long.Parse(match.Groups[1].Value);
        }

        Assert.Fail("no join node with a search estimate:\n" + string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue)));
        return 0;
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
