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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A NULL join key matches nothing and probes nothing. <c>ANALYZE</c> counts the NULL share of
/// each column, the join estimate keeps the share of each side's rows that hold a key, and the
/// probe count of a lookup join scales by it. The data: 1,500 characters, 2,000 roles of which
/// every second one has no character (a NULL key), five roles per movie.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestNullJoinKeys : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Characters = 1500;
    private const int Roles = 2000;

    [Test]
    public async Task Analyze_CountsTheNullShareOfEachColumn()
    {
        Db db = await CreateTables(Options);
        TableDescriptor roles = await Table(db, "roles");

        Assert.That(db.Executor.Statistics.GetColumnNullFraction(db.Database, roles, "character_id"), Is.EqualTo(0.5).Within(0.01));
        Assert.That(db.Executor.Statistics.GetColumnNullFraction(db.Database, roles, "movie_id"), Is.EqualTo(0.0));
        Assert.That(db.Executor.Statistics.GetColumnNullFraction(db.Database, roles, "id"), Is.EqualTo(0.0));
    }

    [Test]
    public async Task Join_OnAHalfNullKey_EstimatesHalfTheRows()
    {
        Db db = await CreateTables(Options);

        // 1,000 roles hold a character; a NULL-blind estimate says 2,000.
        Assert.That(await JoinEstimate(db, "SELECT r.id FROM roles r, characters c WHERE c.id = r.character_id"), Is.InRange(900, 1100));
    }

    [Test]
    public async Task Probe_OnAHalfNullKey_IsPricedForTheKeyedRowsOnly()
    {
        Db db = await CreateTables(Options);

        // 600 roles of 120 movies, 300 of them keyed. A hash join over the 1,500 characters costs
        // 1,650 + 600 units; 600 probes would cost 390 + 600 × 4, 300 probes 390 + 300 × 4, so
        // only the keyed count makes the lookup join the cheaper step.
        List<QueryResultRow> plan = await Query(db, "EXPLAIN (ANALYZE) SELECT r.id FROM roles r, characters c WHERE c.id = r.character_id AND r.movie_id <= 120");
        string text = PlanText(plan);

        Assert.That(text, Does.Contain("index-nested-loop-join"), "the characters join must probe:\n" + text);
        Assert.That(text, Does.Not.Contain("hash-join"), text);
        Assert.That(JoinEstimateOf(plan), Is.InRange(250, 350), "half of the 600 roles hold a character");
    }

    [Test]
    public void StepCost_ScalesTheProbesByTheNonNullShare()
    {
        IndexLookupProfile keyed = new(UniqueIndex: true, MatchesPerProbe: 1, LeftKeyDistinct: 10_000);
        IndexLookupProfile halfNull = keyed with { LeftNonNullFraction = 0.5 };

        double rows = 1000 * CostEstimator.MemoHitWeight;
        Assert.That(CostEstimator.IndexLookupJoinStepCost(1000, keyed), Is.EqualTo(rows + 1000 * 4.0).Within(1e-9));
        Assert.That(CostEstimator.IndexLookupJoinStepCost(1000, halfNull), Is.EqualTo(rows + 500 * 4.0).Within(1e-9));

        // The distinct bound still applies after the share.
        IndexLookupProfile fewKeys = halfNull with { LeftKeyDistinct = 100 };
        Assert.That(CostEstimator.IndexLookupJoinStepCost(1000, fewKeys), Is.EqualTo(rows + 100 * 4.0).Within(1e-9));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { CostBasedJoinOrderEnabled = true, PlanCacheEnabled = false });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE characters (id int64 primary key, name string)");
        await Ddl(db, "CREATE TABLE roles (id int64 primary key, movie_id int64, character_id int64)");
        await Ddl(db, "CREATE INDEX roles_movie ON roles (movie_id)");
        await Ddl(db, "CREATE INDEX roles_character ON roles (character_id)");

        List<string> characters = new(Characters);
        for (int i = 1; i <= Characters; i++)
            characters.Add($"({i}, 'character {i}')");

        List<string> roles = new(Roles);
        for (int i = 0; i < Roles; i++)
        {
            long movie = i % 400 + 1;
            string character = i % 2 == 0 ? "NULL" : (1 + (i / 2) % Characters).ToString();
            roles.Add($"({i}, {movie}, {character})");
        }

        for (int start = 0; start < Characters; start += 250)
            await Exec(db, "INSERT INTO characters (id, name) VALUES " + string.Join(", ", characters.Skip(start).Take(250)));

        for (int start = 0; start < Roles; start += 250)
            await Exec(db, "INSERT INTO roles (id, movie_id, character_id) VALUES " + string.Join(", ", roles.Skip(start).Take(250)));

        await Query(db, "ANALYZE characters");
        await Query(db, "ANALYZE roles");
        return db;
    }

    private static async Task<TableDescriptor> Table(Db db, string table)
    {
        if (!db.Database.TableDescriptors.TryGetValue(table, out AsyncLazy<TableDescriptor>? lazy))
            throw new InvalidOperationException($"table '{table}' not found");

        return await lazy;
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
    private static async Task<long> JoinEstimate(Db db, string sql) =>
        JoinEstimateOf(await Query(db, "EXPLAIN (ANALYZE) " + sql));

    private static long JoinEstimateOf(List<QueryResultRow> plan)
    {
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
