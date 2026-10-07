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

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// What the planner does with a left outer join: the WHERE conjunct on the preserved side is
/// pushed to its scan and the one on the null-extended side is not, every physical join node
/// carries the kind and EXPLAIN shows it, the hash build side is pinned to the right input even
/// when statistics would pick the left, the declared join order is kept, and a plan-cache hit
/// reproduces the miss.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestLeftOuterJoinPlanner : BaseTest
{
    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor, CatalogsManager Catalogs);

    /// <summary>
    /// environments(id, game_id) LEFT JOIN games(id, studio_id): the motivating ORM shape. Two of
    /// three environments reference a game; the third references a game that does not exist.
    /// </summary>
    private async Task<Fixture> SetupAsync(bool indexGamesId = true, CamusDBOptions? options = null)
    {
        CommandExecutor executor = options is null ? CreateCommandExecutor() : CreateCommandExecutor(options);
        string dbname = "lojplan" + Guid.NewGuid().ToString("n")[..12];
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "environments",
            columns: [new("id", ColumnType.Integer64), new("game_id", ColumnType.Integer64), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "games",
            columns: [new("id", ColumnType.Integer64), new("studio_id", ColumnType.Integer64), new("title", ColumnType.String, notNull: true)],
            constraints: indexGamesId
                ? [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]), new(ConstraintType.IndexUnique, "games_id_idx", [new("id", OrderType.Ascending)])]
                : [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.Insert(new InsertTicket(txn, dbname, "games",
            values:
            [
                new() { { "id", new(ColumnType.Integer64, 10L) }, { "studio_id", new(ColumnType.Integer64, 5L) }, { "title", new(ColumnType.String, "Alpha") } },
                new() { { "id", new(ColumnType.Integer64, 20L) }, { "studio_id", new(ColumnType.Integer64, 6L) }, { "title", new(ColumnType.String, "Beta") } },
            ]));

        await executor.Insert(new InsertTicket(txn, dbname, "environments",
            values:
            [
                new() { { "id", new(ColumnType.Integer64, 1L) }, { "game_id", new(ColumnType.Integer64, 10L) }, { "name", new(ColumnType.String, "prod") } },
                new() { { "id", new(ColumnType.Integer64, 2L) }, { "game_id", new(ColumnType.Integer64, 20L) }, { "name", new(ColumnType.String, "stage") } },
                new() { { "id", new(ColumnType.Integer64, 3L) }, { "game_id", new(ColumnType.Integer64, 99L) }, { "name", new(ColumnType.String, "orphan") } },
            ]));

        await database.Transactions.CommitAsync(txn);
        return new Fixture(dbname, database, executor, executor.Catalogs);
    }

    private async Task<(BoundSelectQuery Bound, QueryTicket Ticket)> BindAsync(Fixture f, string sql)
    {
        ExecuteSQLTicket executeTicket = new(
            txnState: await f.Database.Transactions.BeginAsync(), database: f.DbName, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(f.Catalogs, logger)).BindAsync(f.Database, selectQuery);
        return (bound, QueryTicketAdapter.ToQueryTicket(bound, executeTicket));
    }

    private async Task<QueryPlan> PlanAsync(Fixture f, string sql)
    {
        (BoundSelectQuery bound, QueryTicket ticket) = await BindAsync(f, sql);
        return new JoinQueryPlanner(Options, f.Executor.Statistics).GetPlan(f.Database, bound, ticket);
    }

    private static async Task<List<string>> ExplainAsync(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: "EXPLAIN " + sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue ?? ""))).ToList();
    }

    private static async Task<List<QueryResultRow>> RunAsync(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static TableScanNode? FindScan(PhysicalPlanNode? node, string alias)
    {
        switch (node)
        {
            case null:
                return null;
            case TableScanNode { BoundSource: not null } scan when scan.BoundSource.Alias == alias:
                return scan;
            case TableScanNode:
                return null;
            default:
                return FindScan(node.Input, alias);
        }
    }

    /// <summary>The filter pushed into the right source of a join node, whichever operator the planner chose.</summary>
    private static NodeAst? RightFilterOf(PhysicalPlanNode node) => node switch
    {
        HashJoinNode hj => hj.BuildExecutionFilter,
        IndexNestedLoopJoinNode inlj => inlj.RightExecutionFilter,
        MergeJoinNode mj => mj.RightExecutionFilter,
        NestedLoopJoinNode nlj => nlj.RightExecutionFilter,
        _ => throw new AssertionException($"not a join node: {node.GetType().Name}"),
    };

    private static JoinKind KindOf(PhysicalPlanNode node) => node switch
    {
        HashJoinNode hj => hj.Kind,
        IndexNestedLoopJoinNode inlj => inlj.Kind,
        MergeJoinNode mj => mj.Kind,
        NestedLoopJoinNode nlj => nlj.Kind,
        _ => throw new AssertionException($"not a join node: {node.GetType().Name}"),
    };

    private const string Motivating =
        "SELECT e.id, e.game_id, g.studio_id FROM environments e LEFT JOIN games g ON g.id = e.game_id";

    [Test]
    public async Task PreservedSideFilter_IsPushedToItsScan_NullExtendedSideFilter_IsNot()
    {
        Fixture f = await SetupAsync(indexGamesId: false);

        // games.id is the primary key, so the planner probes it: the root is an index nested loop.
        QueryPlan pushed = await PlanAsync(f, Motivating + " WHERE e.id = 3");
        Assert.AreEqual(JoinKind.LeftOuter, KindOf(pushed.Root));
        TableScanNode eScan = FindScan(pushed.Root, "e") ?? throw new AssertionException("no scan for e");
        Assert.IsNotNull(eScan.ExecutionFilter, "the preserved-side conjunct runs on the e scan");
        Assert.IsNull(pushed.ExecutionFilter, "nothing is left for the post-join filter");
        Assert.IsNull(RightFilterOf(pushed.Root));

        QueryPlan kept = await PlanAsync(f, Motivating + " WHERE g.studio_id = 5");
        Assert.IsNull(RightFilterOf(kept.Root), "the null-extended side gets no scan filter");
        Assert.IsNotNull(kept.ExecutionFilter, "the conjunct stays in the post-join filter");

        // The rows prove the rule, not only the plan: the orphan environment survives the join
        // and is then removed by the post-join filter, while an inner-style pushdown would have
        // produced the same count here but a different one for the IS NULL form below.
        List<QueryResultRow> filtered = await RunAsync(f, Motivating + " WHERE g.studio_id = 5");
        Assert.AreEqual(1, filtered.Count);
        Assert.AreEqual(1L, filtered[0].Row["id"].LongValue);

        List<QueryResultRow> unmatched = await RunAsync(f, Motivating + " WHERE g.studio_id IS NULL");
        Assert.AreEqual(1, unmatched.Count, "the padded row is the only one whose studio_id is NULL");
        Assert.AreEqual(3L, unmatched[0].Row["id"].LongValue);
    }

    [Test]
    public async Task MixedChain_OnlyNullExtendedAliasStaysPostJoin()
    {
        Fixture f = await SetupAsync(indexGamesId: false);

        // a JOIN b LEFT JOIN c JOIN d, all over the two tables with distinct aliases.
        const string sql =
            "SELECT a.id FROM environments a JOIN games b ON b.id = a.game_id " +
            "LEFT JOIN environments c ON c.game_id = b.id JOIN games d ON d.id = a.game_id " +
            "WHERE a.id = 1 AND c.name = 'x' AND d.studio_id = 5";

        QueryPlan plan = await PlanAsync(f, sql);

        Assert.IsNotNull(FindScan(plan.Root, "a")!.ExecutionFilter, "a.id = 1 is pushed");
        Assert.IsNotNull(plan.ExecutionFilter, "c.name = 'x' stays post-join");

        // d is the right source of the top join; its filter rides on that node.
        HashJoinNode top = (HashJoinNode)plan.Root;
        Assert.AreEqual("d", top.BuildSource.Alias);
        Assert.IsNotNull(top.BuildExecutionFilter, "d.studio_id = 5 is pushed to the d scan");

        HashJoinNode leftOuter = (HashJoinNode)top.Input!;
        Assert.AreEqual("c", leftOuter.BuildSource.Alias);
        Assert.AreEqual(JoinKind.LeftOuter, leftOuter.Kind);
        Assert.IsNull(leftOuter.BuildExecutionFilter, "c is null-extended: no pushed filter");
    }

    [Test]
    public async Task HashBuildSide_IsPinnedToRight_EvenWhenLeftIsSmaller()
    {
        Fixture f = await SetupAsync(indexGamesId: false);
        f.Executor.Statistics.ForceHashJoinForTesting = true;

        TableDescriptor environments = await f.Database.TableDescriptors["environments"];
        TableDescriptor games = await f.Database.TableDescriptors["games"];
        f.Executor.Statistics.SeedRowCountForTesting(f.Database, environments, 10);
        f.Executor.Statistics.SeedRowCountForTesting(f.Database, games, 10_000);

        // The inner form of the same query picks the left build with these statistics.
        QueryPlan inner = await PlanAsync(f, Motivating.Replace("LEFT JOIN", "JOIN"));
        Assert.AreEqual(HashJoinBuildSide.Left, ((HashJoinNode)inner.Root).BuildSide, "the inner join builds the smaller left side");

        QueryPlan outer = await PlanAsync(f, Motivating);
        HashJoinNode join = (HashJoinNode)outer.Root;
        Assert.AreEqual(JoinKind.LeftOuter, join.Kind);
        Assert.AreEqual(HashJoinBuildSide.Right, join.BuildSide, "a left outer join always probes with its preserved side");
    }

    [Test]
    public async Task Explain_ShowsTheKind_OnEveryOperator_AndNotOnInner()
    {
        Fixture f = await SetupAsync(indexGamesId: true);

        List<string> inner = await ExplainAsync(f, Motivating.Replace("LEFT JOIN", "JOIN"));
        Assert.IsFalse(inner.Any(l => l.Contains("kind=")), "an inner join renders no kind: " + string.Join(" | ", inner));

        f.Executor.Statistics.ForceNestedLoopForTesting = true;
        List<string> nested = await ExplainAsync(f, Motivating);
        Assert.IsTrue(nested.Any(l => l.Contains("nested-loop-join") && l.Contains("kind=left-outer")), string.Join(" | ", nested));
        f.Executor.Statistics.ForceNestedLoopForTesting = false;

        f.Executor.Statistics.ForceIndexNestedLoopForTesting = true;
        List<string> indexed = await ExplainAsync(f, Motivating);
        Assert.IsTrue(indexed.Any(l => l.Contains("index-nested-loop-join") && l.Contains("kind=left-outer")), string.Join(" | ", indexed));
        f.Executor.Statistics.ForceIndexNestedLoopForTesting = false;

        f.Executor.Statistics.ForceHashJoinForTesting = true;
        List<string> hashed = await ExplainAsync(f, Motivating);
        Assert.IsTrue(hashed.Any(l => l.Contains("hash-join") && l.Contains("build=g") && l.Contains("kind=left-outer")), string.Join(" | ", hashed));
        f.Executor.Statistics.ForceHashJoinForTesting = false;

        f.Executor.Statistics.ForceMergeJoinForTesting = true;
        List<string> merged = await ExplainAsync(f, Motivating);
        Assert.IsTrue(merged.Any(l => l.Contains("merge-join") && l.Contains("kind=left-outer")), string.Join(" | ", merged));
        f.Executor.Statistics.ForceMergeJoinForTesting = false;
    }

    [Test]
    public async Task DeclaredJoinOrder_IsKept_WithAndWithoutCostBasedOrdering()
    {
        // Both arms get their own engine: the flag is fixed when the engine is built.
        foreach (bool costBased in new[] { true, false })
        {
            Fixture f = await SetupAsync(indexGamesId: false, options: Options with { CostBasedJoinOrderEnabled = costBased });

            TableDescriptor environments = await f.Database.TableDescriptors["environments"];
            TableDescriptor games = await f.Database.TableDescriptors["games"];
            f.Executor.Statistics.SeedRowCountForTesting(f.Database, environments, 100_000);
            f.Executor.Statistics.SeedRowCountForTesting(f.Database, games, 3);

            // games is far smaller, but it is the null-extended side and must stay on the right.
            QueryPlan plan = await PlanAsync(f, Motivating + " WHERE e.id = 1");
            HashJoinNode join = (HashJoinNode)plan.Root;
            Assert.AreEqual("g", join.BuildSource.Alias, $"costBased={costBased}: the right source stays games");
            Assert.AreEqual("e", FindScan(join.Input, "e")!.BoundSource!.Alias);
        }
    }

    [Test]
    public async Task AutomaticMergeSelection_DoesNotTakeAnotherAliasOrderingAsFree()
    {
        // a LEFT JOIN b ON a.x = b.x LEFT JOIN c ON b.x = c.x: the first join is a merge join over
        // two free index orderings and advertises its output as ordered by a.x. That ordering must
        // not count as free ordering for the second join's key b.x: it is another alias, and on a
        // padded row b.x is NULL while a.x is not. With a bare-name comparison the planner selected
        // a second merge join and then had to sort the whole intermediate result; the same query
        // on b.y, where the bare names differ, chose a hash join and no sort.
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = "lojfree" + Guid.NewGuid().ToString("n")[..12];
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        async Task Table(string name, string[] indexed, params string[] columns)
        {
            List<ConstraintInfo> constraints = [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])];
            foreach (string col in indexed)
                constraints.Add(new(ConstraintType.IndexMulti, $"{name}_{col}", [new(col, OrderType.Ascending)]));

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: name,
                columns: columns.Select(c => new ColumnInfo(c, ColumnType.Integer64)).ToArray(),
                constraints: constraints.ToArray(),
                ifNotExists: false));

            executor.Statistics.SeedRowCountForTesting(database, await database.TableDescriptors[name], 1000);
        }

        await Table("a", ["x"], "id", "x");
        await Table("b", ["x", "y"], "id", "x", "y");
        await Table("c", ["x"], "id", "x");

        Fixture f = new(dbname, database, executor, executor.Catalogs);

        foreach (string key in new[] { "x", "y" })
        {
            List<string> plan = await ExplainAsync(f, $"SELECT a.id FROM a LEFT JOIN b ON a.x = b.{key} LEFT JOIN c ON b.{key} = c.x");
            string rendered = string.Join("\n", plan);

            Assert.IsFalse(plan.Any(l => l.Contains("sort(")), $"key b.{key}: no sort of the intermediate result; plan:\n{rendered}");
            Assert.IsTrue(plan.Any(l => l.Contains("merge-join")), $"key b.{key}: the first join still merges two free orderings; plan:\n{rendered}");
            Assert.AreEqual(1, plan.Count(l => l.Contains("merge-join")), $"key b.{key}: only the first join has free ordering on both sides; plan:\n{rendered}");
        }
    }

    [Test]
    public async Task PlanCacheHit_ReproducesThePlanAndTheRows()
    {
        Fixture f = await SetupAsync(indexGamesId: true, options: Options with { PlanCacheEnabled = true });

        string sql = Motivating + " WHERE e.id = 3 ORDER BY e.id";

        List<string> firstPlan = await ExplainAsync(f, sql);
        List<QueryResultRow> firstRows = await RunAsync(f, sql);

        long hitsBefore = f.Executor.PlanCache.Hits;
        List<QueryResultRow> secondRows = await RunAsync(f, sql);
        List<string> secondPlan = await ExplainAsync(f, sql);

        Assert.Greater(f.Executor.PlanCache.Hits, hitsBefore, "the second execution must be served from the plan cache");
        Assert.AreEqual(firstPlan, secondPlan, "a plan-cache hit renders the same plan as the miss");
        Assert.IsTrue(secondPlan.Any(l => l.Contains("kind=left-outer")));

        Assert.AreEqual(1, firstRows.Count);
        Assert.AreEqual(1, secondRows.Count);
        Assert.AreEqual(3L, secondRows[0].Row["id"].LongValue);
        Assert.AreEqual(ColumnType.Null, secondRows[0].Row["studio_id"].Type, "the orphan environment is padded on both runs");
    }
}
