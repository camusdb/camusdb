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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A left row whose key the join's memo already holds costs the lookup join less than a hash
/// probe costs the hash join (<see cref="CostEstimator.MemoHitWeight"/>), so a left input that
/// repeats its keys probes where the two operators were a near tie. The data: 6,000 parts and
/// 6,000 orders over 1,900 of them, so the probe plan pays 1,900 probes and 6,000 memo-priced
/// rows against a hash join that scans the parts and pays one unit for each order.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestProbeMemoPrice : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Parts = 6000;
    private const int Orders = 6000;
    private const int OrderedParts = 1900;

    [Test]
    public void StepCost_AMemoHitUndercutsAHashProbe()
    {
        IndexLookupProfile probe = new(UniqueIndex: true, MatchesPerProbe: 1, LeftKeyDistinct: OrderedParts);
        double lookup = CostEstimator.IndexLookupJoinStepCost(Orders, probe);
        double hash = CostEstimator.HashJoinStepCost(Orders, Parts, Parts);

        // 6,000 × 0.65 + 1,900 × (3 + 1) = 11,500 against 6,000 + 6,000 + 600 = 12,600; at one
        // unit for each left row the lookup step would cost 13,600 and lose.
        Assert.That(lookup, Is.EqualTo(Orders * CostEstimator.MemoHitWeight + OrderedParts * 4.0).Within(1e-9));
        Assert.That(lookup, Is.LessThan(hash));
    }

    [Test]
    public async Task ARepeatedKey_ProbesWhereTheHashJoinWasANearTie()
    {
        Db db = await CreateTables(Options);

        List<QueryResultRow> plan = await Query(db, "EXPLAIN (ANALYZE) SELECT MIN(p.name) FROM orders o, parts p WHERE p.id = o.part_id");
        string text = PlanText(plan);

        Assert.That(text, Does.Contain("index-nested-loop-join"), "the parts join must probe:\n" + text);
        Assert.That(text, Does.Contain("left=o.part_id"), text);
        Assert.That(text, Does.Not.Contain("hash-join"), text);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { CostBasedJoinOrderEnabled = true, PlanCacheEnabled = false });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE parts (id int64 primary key, name string)");
        await Ddl(db, "CREATE TABLE orders (id int64 primary key, part_id int64)");

        List<string> parts = new(Parts);
        for (int i = 1; i <= Parts; i++)
            parts.Add($"({i}, 'part {i}')");

        List<string> orders = new(Orders);
        for (int i = 0; i < Orders; i++)
            orders.Add($"({i}, {i % OrderedParts + 1})");

        for (int start = 0; start < Parts; start += 250)
            await Exec(db, "INSERT INTO parts (id, name) VALUES " + string.Join(", ", parts.Skip(start).Take(250)));

        for (int start = 0; start < Orders; start += 250)
            await Exec(db, "INSERT INTO orders (id, part_id) VALUES " + string.Join(", ", orders.Skip(start).Take(250)));

        await Query(db, "ANALYZE parts");
        await Query(db, "ANALYZE orders");
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
