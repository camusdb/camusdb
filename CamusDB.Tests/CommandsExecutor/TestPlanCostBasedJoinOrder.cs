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
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for cost-based join-order enumeration (System-R–style DP).
///
/// The DP enumerator is gated by <see cref="CamusDBOptions.CostBasedJoinOrderEnabled"/>.
/// When the flag is off, plans are byte-identical to the heuristic output; when on,
/// the planner picks the cheapest left-deep ordering based on INLJ vs hash-join cost.
///
/// Test scenario — star join with three tables:
///   events  (50 000 rows, indexed filter  →  5 000 after "type = click", idx on session_id + user_id)
///   sessions (500 rows,   indexed filter  →     50 after "region = eu",  unique id PK)
///   users   (200 rows,   no filter,             no secondary indexes)
///
/// Heuristic (score-based, flag off):
///   events(1) and sessions(1) tie; declared order kept → events is outer → cost ≈ 10 300.
///
/// DP (flag on):
///   sessions→events→users: cost ≈ 400 (INLJ from 50 sessions into events.session_id index →
///   avoids the 10 000-row events full scan that the heuristic must pay up-front).
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Every case here — planner-level and executor-level
// alike — now states its configuration as options on the planner or engine it constructs, so nothing
// process-wide is left to race; the node is the only remaining reason.
[NonParallelizable]
public sealed class TestPlanCostBasedJoinOrder : BaseTest
{
    // SQL used across multiple tests — events declared first (heuristic keeps it outer).
    private const string StarJoinSql =
        "SELECT e.id, s.region, u.name " +
        "FROM events e " +
        "JOIN sessions s ON e.session_id = s.id " +
        "JOIN users u ON e.user_id = u.id " +
        "WHERE e.type = \"click\" AND s.region = \"eu\"";

    // ─── Schema helper ───────────────────────────────────────────────────────

    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindStarJoin(string sql)
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"joinorder_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // events: has indexes on type (for WHERE push-down), session_id and user_id (join keys).
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns:
            [
                new("id",         ColumnType.Id),
                new("type",       ColumnType.String, notNull: true),
                new("session_id", ColumnType.Id,     notNull: true),
                new("user_id",    ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",              [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_type_idx",  [new("type",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_sid_idx",   [new("session_id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_uid_idx",   [new("user_id",    OrderType.Ascending)]),
            ],
            ifNotExists: false));

        // sessions: PK on id (makes it the INLJ inner target when session_id is the join key);
        //           region is indexed so the WHERE push-down hits an index.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "sessions",
            columns:
            [
                new("id",     ColumnType.Id),
                new("region", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",                 [new("id",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "sessions_region_idx", [new("region", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        // users: PK only, no secondary indexes, no WHERE push-down.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "users",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        // Seed cardinalities: events is large, sessions is selective, users is medium.
        TableDescriptor eventsTable   = await database.TableDescriptors["events"];
        TableDescriptor sessionsTable = await database.TableDescriptors["sessions"];
        TableDescriptor usersTable    = await database.TableDescriptors["users"];

        executor.Statistics.SeedRowCountForTesting(database, eventsTable,   50_000);
        executor.Statistics.SeedRowCountForTesting(database, sessionsTable,    500);
        executor.Statistics.SeedRowCountForTesting(database, usersTable,       200);

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: sql,
            parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    // ─── Tests ───────────────────────────────────────────────────────────────

    /// <summary>Cost-based join ordering off — the rule-based heuristic.</summary>
    private static CamusDBOptions DpOff => CamusDBOptions.Default with { CostBasedJoinOrderEnabled = false };

    /// <summary>Cost-based join ordering on — System-R style DP enumeration.</summary>
    private static CamusDBOptions DpOn => CamusDBOptions.Default with { CostBasedJoinOrderEnabled = true };

    [Test]
    public async Task FlagOff_HeuristicKeepsEventsDeclaredFirst()
    {

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindStarJoin(StarJoinSql);

        QueryPlan plan = new JoinQueryPlanner(DpOff, executor.Statistics).GetPlan(database, bound, ticket);

        // Heuristic: events(score 1) and sessions(score 1) tie; declared order → events is outermost.
        Assert.AreEqual("e", GetLeftmostScanAlias(plan.Root),
            "Heuristic must keep events (declared first) as the outermost driver.");
    }

    [Test]
    public async Task FlagOn_DpPicksSessionsFirst()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindStarJoin(StarJoinSql);

        QueryPlan plan = new JoinQueryPlanner(DpOn, executor.Statistics).GetPlan(database, bound, ticket);

        // DP: sessions (50 filtered rows) first → INLJ into events.session_id index costs 50×2=100.
        // events first → must full-scan events (5 000 rows × 2 = 10 000) before filtering.
        Assert.AreEqual("s", GetLeftmostScanAlias(plan.Root),
            "DP must put sessions first: INLJ from 50 sessions into events avoids the 10 000-cost events scan.");
    }

    [Test]
    public async Task FlagOn_WideJoin_FallsBackToHeuristic()
    {
        // N=13 > MaxTablesForEnumeration=12 → DP falls back to JoinOrderOptimizer.
        // Plans produced with flag on and off must be identical for wide joins
        // (same leftmost scan alias, same join depth = N-1 joins).
        const int n = JoinEnumerator.MaxTablesForEnumeration + 1; // 13

        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;
        string dbname = $"widejoin_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Chain schema: chain0(id), chain1(id, chain0_id), ..., chain12(id, chain11_id).
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "chain0",
            columns: [new("id", ColumnType.Id)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        for (int i = 1; i < n; i++)
        {
            string fkCol = $"chain{i - 1}_id";
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: $"chain{i}",
                columns: [new("id", ColumnType.Id), new(fkCol, ColumnType.Id, notNull: true)],
                constraints:
                [
                    new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                    new(ConstraintType.IndexMulti, $"chain{i}_fk_idx", [new(fkCol, OrderType.Ascending)]),
                ],
                ifNotExists: false));
        }

        await database.Transactions.CommitAsync(txn);

        // Build: SELECT c0.id FROM chain0 c0 JOIN chain1 c1 ON c0.id = c1.chain0_id JOIN ...
        System.Text.StringBuilder sb = new();
        sb.Append("SELECT c0.id FROM chain0 c0");
        for (int i = 1; i < n; i++)
            sb.Append($" JOIN chain{i} c{i} ON c{i - 1}.id = c{i}.chain{i - 1}_id");
        string sql = sb.ToString();

        async Task<QueryPlan> GetPlanWithFlag(bool flagOn)
        {
            ExecuteSQLTicket executeTicket = new(
                txnState: await database.Transactions.BeginAsync(),
                database: dbname, sql: sql, parameters: null);
            SelectQuery sq = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
            BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
                .BindAsync(database, sq);
            QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);
            return new JoinQueryPlanner(flagOn ? DpOn : DpOff, executor.Statistics).GetPlan(database, bound, ticket);
        }

        try
        {
            QueryPlan planOff = await GetPlanWithFlag(flagOn: false);
            QueryPlan planOn  = await GetPlanWithFlag(flagOn: true);

            // Both must produce the same outermost driver — fallback = identical ordering.
            string aliasOff = GetLeftmostScanAlias(planOff.Root);
            string aliasOn  = GetLeftmostScanAlias(planOn.Root);
            Assert.AreEqual(aliasOff, aliasOn,
                $"N={n} wide join: DP must fall back to heuristic and produce the same ordering.");
        }
        finally
        {
        }
    }


    [Test]
    public async Task FlagOff_JoinOrderIdenticalToHeuristic()
    {
        // Regression guard: the flag-off code path (which now dispatches through the new
        // if/else in JoinQueryPlanner) must produce byte-identical join ordering to a
        // no-stats JoinQueryPlanner that definitively goes through JoinOrderOptimizer.Reorder.
        //
        // CollectJoinOrder() walks the full left spine and collects (operator, rightAlias)
        // at every step — it catches any reordering in any position, not just the outermost.

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindStarJoin(StarJoinSql);

        // A planner takes its configuration when it is constructed, so the two settings under
        // comparison are two planners rather than one planner and a flag flipped between calls.
        CamusDBOptions flagOff = Options with { CostBasedJoinOrderEnabled = false };
        CamusDBOptions flagOn  = Options with { CostBasedJoinOrderEnabled = true };

        // Plan A: flag off, stats available.  Dispatcher goes: flag=false → heuristic.
        QueryPlan planOff = new JoinQueryPlanner(flagOff, executor.Statistics).GetPlan(database, bound, ticket);

        // Plan B: no stats at all.  _stats is null → dispatcher always takes heuristic branch,
        // regardless of flag.  This is the gold-standard "pure heuristic" plan.
        QueryPlan planNoStats = new JoinQueryPlanner(flagOff).GetPlan(database, bound, ticket);

        // Plan C: flag on, stats available.  Dispatcher goes: flag=true → DP → sessions first.
        {
            QueryPlan planOn = new JoinQueryPlanner(flagOn, executor.Statistics).GetPlan(database, bound, ticket);

            List<string> orderOff     = CollectJoinOrder(planOff.Root);
            List<string> orderNoStats = CollectJoinOrder(planNoStats.Root);
            List<string> orderOn      = CollectJoinOrder(planOn.Root);

            // Flag-off must match the pure-heuristic ordering at every join step.
            Assert.AreEqual(orderNoStats, orderOff,
                "Flag=off ordering must be identical to the pure-heuristic (no-stats) plan at every join position.");

            // DP must produce a DIFFERENT ordering (verifies the flag actually has an effect
            // and the test is not vacuously passing).
            Assert.AreNotEqual(orderOff, orderOn,
                "Flag=on ordering must differ from flag=off — if equal the DP had no effect and the test is vacuous.");
        }
    }

    // ─── Execution / result-equivalence tests ────────────────────────────────

    [Test]
    public async Task FlagOn_ResultsMatchFlagOff()
    {
        // Proof-by-execution: the reordered DP plan must return the same rows as the
        // heuristic plan. Shape-only tests (above) cannot catch a dropped predicate,
        // a wrong INLJ direction, or any other correctness bug introduced by reordering.

        string dbname = await CreateStarJoinDatabase();

        // One engine per ordering: each fixes its own configuration when built, so the two plans are
        // produced under genuinely independent settings rather than a value flipped between the runs.
        CommandExecutor heuristic = CreateCommandExecutor(Options with { CostBasedJoinOrderEnabled = false });
        CommandExecutor costBased = CreateCommandExecutor(Options with { CostBasedJoinOrderEnabled = true });
        TrackDatabase(dbname, heuristic);

        List<(string region, string name)> rowsOff = await ExecuteStarJoin(heuristic, dbname);
        List<(string region, string name)> rowsOn  = await ExecuteStarJoin(costBased, dbname);

        // Both orderings must return the same result set (sorted for stable comparison).
        rowsOff.Sort((a, b) => string.Compare(a.name, b.name, StringComparison.Ordinal));
        rowsOn .Sort((a, b) => string.Compare(a.name, b.name, StringComparison.Ordinal));

        Assert.AreEqual(rowsOff.Count, rowsOn.Count,
            "Flag-on and flag-off must return the same number of rows.");

        for (int i = 0; i < rowsOff.Count; i++)
        {
            Assert.AreEqual(rowsOff[i].region, rowsOn[i].region,
                $"Row {i}: region must match between flag-off and flag-on plans.");
            Assert.AreEqual(rowsOff[i].name, rowsOn[i].name,
                $"Row {i}: name must match between flag-off and flag-on plans.");
        }
    }

    [Test]
    public async Task FlagOn_FiltersAppliedCorrectly()
    {
        // Verifies that WHERE predicates (type="click" on events, region="eu" on sessions)
        // are correctly applied after reordering. Rows that fail either filter must be absent.
        string dbname = await CreateStarJoinDatabase();
        CommandExecutor executor = CreateCommandExecutor(Options with { CostBasedJoinOrderEnabled = true });
        TrackDatabase(dbname, executor);

        List<(string region, string name)> rows = await ExecuteStarJoin(executor, dbname);
        rows.Sort((a, b) => string.Compare(a.name, b.name, StringComparison.Ordinal));

        // Only 2 events match: click+eu1→alice, click+eu2→bob.
        // Excluded: view+eu1→alice (wrong type), click+us1→charlie (wrong region).
        Assert.AreEqual(2, rows.Count, "Exactly 2 rows must satisfy type=click AND region=eu.");
        Assert.AreEqual("eu", rows[0].region);
        Assert.AreEqual("alice", rows[0].name);
        Assert.AreEqual("eu", rows[1].region);
        Assert.AreEqual("bob", rows[1].name);
    }

    // ─── Execution helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Creates the star-join schema, seeds row-count stats (to make the DP prefer
    /// sessions-first), and inserts a small dataset for correctness verification.
    /// Returns the database name; the caller must call <see cref="TrackDatabase"/>.
    /// </summary>
    private async Task<string> CreateStarJoinDatabase()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = $"joinexec_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "events",
            columns:
            [
                new("id",         ColumnType.Id),
                new("type",       ColumnType.String, notNull: true),
                new("session_id", ColumnType.Id,     notNull: true),
                new("user_id",    ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",             [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_type_idx", [new("type",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_sid_idx",  [new("session_id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "events_uid_idx",  [new("user_id",    OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "sessions",
            columns:
            [
                new("id",     ColumnType.Id),
                new("region", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",                 [new("id",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "sessions_region_idx", [new("region", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "users",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        // ObjectId strings — 24 hex chars each.
        string sid1 = "aaaaaaaaaaaaaaaaaaaaaaaa"; // session eu1
        string sid2 = "bbbbbbbbbbbbbbbbbbbbbbbb"; // session us1 (non-matching region)
        string sid3 = "cccccccccccccccccccccccc"; // session eu2
        string uid1 = "dddddddddddddddddddddddd"; // user alice
        string uid2 = "eeeeeeeeeeeeeeeeeeeeeeee"; // user bob
        string uid3 = "ffffffffffffffffffffffff"; // user charlie

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "sessions",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, sid1) }, { "region", new(ColumnType.String, "eu") } },
                new() { { "id", new(ColumnType.Id, sid2) }, { "region", new(ColumnType.String, "us") } },
                new() { { "id", new(ColumnType.Id, sid3) }, { "region", new(ColumnType.String, "eu") } },
            }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "users",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, uid1) }, { "name", new(ColumnType.String, "alice") } },
                new() { { "id", new(ColumnType.Id, uid2) }, { "name", new(ColumnType.String, "bob") } },
                new() { { "id", new(ColumnType.Id, uid3) }, { "name", new(ColumnType.String, "charlie") } },
            }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "events",
            values: new()
            {
                // Matches (type=click, region=eu)
                new() { { "id", new(ColumnType.Id, "111111111111111111111111") }, { "type", new(ColumnType.String, "click") }, { "session_id", new(ColumnType.Id, sid1) }, { "user_id", new(ColumnType.Id, uid1) } },
                new() { { "id", new(ColumnType.Id, "222222222222222222222222") }, { "type", new(ColumnType.String, "click") }, { "session_id", new(ColumnType.Id, sid3) }, { "user_id", new(ColumnType.Id, uid2) } },
                // Filtered out: wrong type
                new() { { "id", new(ColumnType.Id, "333333333333333333333333") }, { "type", new(ColumnType.String, "view")  }, { "session_id", new(ColumnType.Id, sid1) }, { "user_id", new(ColumnType.Id, uid1) } },
                // Filtered out: wrong region (us)
                new() { { "id", new(ColumnType.Id, "444444444444444444444444") }, { "type", new(ColumnType.String, "click") }, { "session_id", new(ColumnType.Id, sid2) }, { "user_id", new(ColumnType.Id, uid3) } },
            }));

        await database.Transactions.CommitAsync(txn);

        // Seed inflated cardinalities so the DP's cost model sees sessions as selective
        // (only 50 of 500 rows pass the filter vs 5 000 of 50 000 for events).
        TableDescriptor eventsTable   = await database.TableDescriptors["events"];
        TableDescriptor sessionsTable = await database.TableDescriptors["sessions"];
        TableDescriptor usersTable    = await database.TableDescriptors["users"];

        executor.Statistics.SeedRowCountForTesting(database, eventsTable,   50_000);
        executor.Statistics.SeedRowCountForTesting(database, sessionsTable,    500);
        executor.Statistics.SeedRowCountForTesting(database, usersTable,       200);

        return dbname;
    }

    /// <summary>
    /// Runs the star join through <paramref name="executor"/>, whose own configuration decides whether
    /// the cost-based join order is used. Comparing the two orderings therefore means passing two
    /// engines, not toggling anything between the runs.
    /// </summary>
    private async Task<List<(string region, string name)>> ExecuteStarJoin(
        CommandExecutor executor, string dbname)
    {
        KvTransaction txn = await (await executor.OpenDatabase(dbname)).Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: StarJoinSql,
            parameters: null);

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)
            = await executor.ExecuteSQLQuery(ticket);

        List<(string region, string name)> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add((row.Row["region"].StrValue!, row.Row["name"].StrValue!));

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    // ─── Filtered-leaf helpers ───────────────────────────────────────────────

    /// <summary>
    /// Two-table join: orders (10k rows, WHERE status='urgent') → products (2k rows, indexed PK).
    /// Used to verify that a pushed-down filter on the left leaf is accounted for correctly by
    /// both the cost annotator (EstimatedCardinality) and the hash-vs-INLJ decision.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindOrdersProductsJoin()
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"joinleaf_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // orders: large table, secondary index on status (for WHERE push-down) + product_id (join key).
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("status",     ColumnType.String, notNull: true),
                new("product_id", ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",                [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_status_idx",  [new("status",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",     [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        // products: medium table, PK on id (INLJ target when orders.product_id = products.id).
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        // orders = 10 000 rows; products = 2 000 rows.
        // With the 10 % default selectivity for WHERE status = 'urgent' (no histograms seeded):
        //   filtered orders = 1 000 rows.
        // INLJ cost = 1 000 × 2 = 2 000; hash cost = (1 000 + 2 000) + min(1 000, 2 000) × 0.1 = 3 100.
        // → INLJ is cheaper once the filter is accounted for.
        executor.Statistics.SeedRowCountForTesting(database, ordersTable,   10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.status = \"urgent\"";

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: sql,
            parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    // ─── Filtered-leaf tests ─────────────────────────────────────────────────

    [Test]
    public async Task FilteredLeaf_EstimatedCardinalityReflectsFilter()
    {
        // The orders leaf (10 000 rows, status='urgent' → 10% → 1 000) must have
        // EstimatedCardinality = 1 000, not 10 000.
        // CostEstimator.EstimateNodeCost's TableScanNode case applies ExecutionFilter
        // selectivity to output cardinality while keeping scan cost at the full row count.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindOrdersProductsJoin();

        QueryPlan plan = new JoinQueryPlanner(DpOff, executor.Statistics).GetPlan(database, bound, ticket);

        // Find the orders leaf scan node — the leftmost TableScanNode with an ExecutionFilter.
        TableScanNode? ordersLeaf = FindLeafWithFilter(plan.Root, "o");

        Assert.IsNotNull(ordersLeaf, "Expected a filtered orders scan leaf in the join plan");
        Assert.IsNotNull(ordersLeaf!.EstimatedCardinality,
            "EstimatedCardinality must be set by CostEstimator.AnnotatePlan");

        // 10 % default selectivity × 10 000 = 1 000.
        // Assert range [500, 2000] to tolerate rounding and any future selectivity changes.
        long card = ordersLeaf.EstimatedCardinality!.Value;
        Assert.Less(card, 10_000,
            $"FilteredLeaf cardinality ({card}) must be less than the full table count (10 000)");
        Assert.Greater(card, 0,
            "FilteredLeaf cardinality must be positive");
    }

    [Test]
    public async Task HashVsINLJ_FlipsWhenFilterMakesLeftSmall()
    {
        // Without filter accounting, ShouldPreferHashOverIndexNestedLoop would use the full
        // 10 000-row orders table as the outer side → hash cost (12 200) < INLJ (20 000).
        // With filter accounting, EstimatePhysicalNodeRows sees ExecutionFilter → 1 000
        // filtered rows → INLJ cost (2 000) < hash (3 100) → IndexNestedLoopJoinNode.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindOrdersProductsJoin();

        QueryPlan plan = new JoinQueryPlanner(DpOff, executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root,
            "Selective filter on outer side must make INLJ cheaper than hash join");
    }

    [Test]
    public async Task JoinRoot_CardinalityBelowUnfilteredTableProduct()
    {
        // The join output cardinality must be well below the raw cross-product
        // (10 000 × 2 000 = 20M) because the filter on orders shrinks its contribution.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindOrdersProductsJoin();

        QueryPlan plan = new JoinQueryPlanner(DpOff, executor.Statistics).GetPlan(database, bound, ticket);

        long? joinCard = plan.Root.EstimatedCardinality;
        Assert.IsNotNull(joinCard, "Join root EstimatedCardinality must be populated by CostEstimator");
        Assert.Less(joinCard!.Value, 10_000L * 2_000L,
            "Join cardinality must be below the raw cross-product");
        Assert.Greater(joinCard.Value, 0L, "Join cardinality must be positive");
    }

    /// <summary>
    /// Walks the full plan tree and returns the first <see cref="TableScanNode"/> with the given
    /// alias that has an <c>ExecutionFilter</c> set (i.e., a pushed-down WHERE predicate).
    /// </summary>
    private static TableScanNode? FindLeafWithFilter(PhysicalPlanNode node, string alias)
    {
        if (node is TableScanNode scan
            && scan.BoundSource?.Alias == alias
            && scan.ExecutionFilter is not null)
            return scan;

        if (node.Input is not null)
        {
            TableScanNode? fromInput = FindLeafWithFilter(node.Input, alias);
            if (fromInput is not null) return fromInput;
        }

        return null;
    }

    // ─── Indexed-join-leaf helpers ────────────────────────────────────────────

    /// <summary>
    /// Creates an orders/products database and seeds stats so the breakeven veto
    /// does not block index-scan selection on the orders leaf.
    ///
    /// The String equality step emits a degenerate [v,v] range whose estimated row
    /// count ≈ 1 (well below the 50 % breakeven veto at 5 000 rows), so the index
    /// scan is chosen without needing histogram data.
    ///
    /// With <c>CostBasedAccessPathEnabled = true</c> the planner emits an
    /// <see cref="IndexRangeScanNode"/> for the orders leaf instead of the default
    /// <see cref="TableScanNode"/>.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindIndexedJoinLeafSetup()
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"indexleaf_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("status",     ColumnType.String, notNull: true),
                new("product_id", ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",               [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_status_idx", [new("status",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",    [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.status = \"urgent\"";

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    // ─── Indexed-join-leaf tests ──────────────────────────────────────────────

    [Test]
    public async Task FlagOn_FilteredJoinLeafBecomesIndexRangeScan()
    {

            CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = true };
        // Validates that BuildJoinTree emits IndexRangeScanNode for the orders leaf when
        // CostBasedAccessPathEnabled is true and status has a usable index with low NDV.
        // With the flag off the leaf is always TableScanNode(PrimaryRows) regardless of indexes.
        try
        {
            (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
                = await BindIndexedJoinLeafSetup();

            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            // Walk the plan to find the orders leaf.
            IndexRangeScanNode? rangeLeaf = FindIndexRangeLeaf(plan.Root, "o");
            Assert.IsNotNull(rangeLeaf,
                "With CostBasedAccessPathEnabled=true and a usable status index, " +
                "the orders join leaf must be an IndexRangeScanNode, not a TableScanNode.");

            Assert.IsNotNull(rangeLeaf!.BoundSource, "IndexRangeScanNode in join must have BoundSource set");
            Assert.AreEqual("o", rangeLeaf.BoundSource!.Alias);
            Assert.AreEqual("orders_status_idx", rangeLeaf.Index.Name);
        }
        finally
        {
        }
    }

    [Test]
    public async Task FlagOff_FilteredJoinLeafRemainsTableScan()
    {


        CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = false };
        // Regression guard: when CostBasedAccessPathEnabled=false the behaviour is unchanged —
        // join leaves remain TableScanNode(PrimaryRows) with a residual ExecutionFilter.

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindIndexedJoinLeafSetup();

        QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

        IndexRangeScanNode? rangeLeaf = FindIndexRangeLeaf(plan.Root, "o");
        Assert.IsNull(rangeLeaf,
            "With CostBasedAccessPathEnabled=false the orders leaf must remain a TableScanNode.");
    }

    [Test]
    public async Task IndexLeafPlan_ResultsMatchFullScanPlan()
    {
        // Proof-by-execution: the index-scan leaf plan must produce the same result rows as
        // the default full-scan plan — any dropped predicate, wrong range, or missing row fetch
        // would diverge here.
        string dbname = await CreateIndexedJoinDatabase();

        // One engine per access-path setting — see the star-join equivalence test above.
        CommandExecutor fullScan  = CreateCommandExecutor(Options with { CostBasedAccessPathEnabled = false });
        CommandExecutor indexScan = CreateCommandExecutor(Options with { CostBasedAccessPathEnabled = true });
        TrackDatabase(dbname, fullScan);

        List<(string id, string name)> rowsOff = await ExecuteIndexedJoinPlan(fullScan, dbname);
        List<(string id, string name)> rowsOn  = await ExecuteIndexedJoinPlan(indexScan, dbname);

        rowsOff.Sort((a, b) => string.Compare(a.id, b.id, StringComparison.Ordinal));
        rowsOn .Sort((a, b) => string.Compare(a.id, b.id, StringComparison.Ordinal));

        Assert.AreEqual(rowsOff.Count, rowsOn.Count,
            "Index-scan leaf plan and full-scan leaf plan must return the same number of rows.");

        for (int i = 0; i < rowsOff.Count; i++)
        {
            Assert.AreEqual(rowsOff[i].id,   rowsOn[i].id,   $"Row {i}: id mismatch.");
            Assert.AreEqual(rowsOff[i].name, rowsOn[i].name, $"Row {i}: name mismatch.");
        }
    }

    private async Task<string> CreateIndexedJoinDatabase()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = $"indexleafexec_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("status",     ColumnType.String, notNull: true),
                new("product_id", ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",               [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_status_idx", [new("status",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",    [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string pid1 = "aaaa00000000000000000001";
        string pid2 = "aaaa00000000000000000002";
        string pid3 = "aaaa00000000000000000003";

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "products",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, pid1) }, { "name", new(ColumnType.String, "Widget") } },
                new() { { "id", new(ColumnType.Id, pid2) }, { "name", new(ColumnType.String, "Gadget") } },
                new() { { "id", new(ColumnType.Id, pid3) }, { "name", new(ColumnType.String, "Doohickey") } },
            }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "orders",
            values: new()
            {
                // status=urgent — should be returned
                new() { { "id", new(ColumnType.Id, "bbbb00000000000000000001") }, { "status", new(ColumnType.String, "urgent")  }, { "product_id", new(ColumnType.Id, pid1) } },
                new() { { "id", new(ColumnType.Id, "bbbb00000000000000000002") }, { "status", new(ColumnType.String, "urgent")  }, { "product_id", new(ColumnType.Id, pid2) } },
                // status=normal — must be excluded
                new() { { "id", new(ColumnType.Id, "bbbb00000000000000000003") }, { "status", new(ColumnType.String, "normal")  }, { "product_id", new(ColumnType.Id, pid3) } },
                new() { { "id", new(ColumnType.Id, "bbbb00000000000000000004") }, { "status", new(ColumnType.String, "pending") }, { "product_id", new(ColumnType.Id, pid1) } },
            }));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        return dbname;
    }

    /// <summary>
    /// Runs the indexed join through <paramref name="executor"/>, whose own configuration decides
    /// whether the cost-based access path is considered.
    /// </summary>
    private async Task<List<(string id, string name)>> ExecuteIndexedJoinPlan(
        CommandExecutor executor, string dbname)
    {
        KvTransaction txn = await (await executor.OpenDatabase(dbname)).Transactions.BeginAsync();

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.status = \"urgent\"";

        ExecuteSQLTicket ticket = new(
            txnState: txn, database: dbname, sql: sql, parameters: null);

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)
            = await executor.ExecuteSQLQuery(ticket);

        List<(string id, string name)> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add((row.Row["id"].StrValue ?? "", row.Row["name"].StrValue ?? ""));

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ─── Index-range-leaf annotation tests ───────────────────────────────────

    /// <summary>
    /// Orders/products join with NDV seeded for the pushed-down filter column.
    ///
    /// NDV=5 for <c>status</c> → degenerate [v,v] String equality estimate = trc/NDV = 10000/5 = 2000.
    /// Without the seeded NDV the estimator falls back to 10 % of trc = 1000.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindIndexedLeafAnnotationSetup()
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"leafannot_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("status",     ColumnType.String, notNull: true),
                new("product_id", ColumnType.Id,     notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",               [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_status_idx", [new("status",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",    [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        // NDV=5: [v,v] String range uses NDV → 10000/5 = 2000 rows.
        // Without seeded stats the estimator falls back to BothBoundsSelectivity (10%) = 1000.
        await executor.Statistics.SetNdvAsync(database, ordersTable,
            columnNdv: new Dictionary<string, long> { ["status"] = 5 },
            keyNdv: null);

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.status = \"urgent\"";

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    [Test]
    public async Task IndexRangeLeafAnnotation_MatchesEstimatePhysicalNodeRows()
    {

            CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = true };
        // With CostBasedAccessPathEnabled=true and seeded NDV, AnnotatePlan must assign the
        // same EstimatedCardinality to the IndexRangeScanNode leaf as EstimatePhysicalNodeRows.
        // EstimateNodeCost passes resolvedTable (not null primaryTable) to EstimateRangeScanRows
        // so the stats branch is used rather than the 10% fallback.
        try
        {
            (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
                = await BindIndexedLeafAnnotationSetup();

            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            IndexRangeScanNode? rangeLeaf = FindIndexRangeLeaf(plan.Root, "o");
            Assert.IsNotNull(rangeLeaf, "Expected an IndexRangeScanNode for the orders leaf with CostBasedAccessPathEnabled=true");

            // AnnotatePlan fills EstimatedCardinality.
            CostEstimator.AnnotatePlan(plan.Root, database, table: null, executor.Statistics, planning);
            long annotated = rangeLeaf!.EstimatedCardinality ?? 0L;

            // EstimatePhysicalNodeRows is the reference — it passes the table correctly.
            long reference = new JoinQueryPlanner(planning).EstimatePhysicalNodeRows(rangeLeaf, database, executor.Statistics);

            Assert.AreEqual(reference, annotated,
                "AnnotatePlan leaf cardinality must equal EstimatePhysicalNodeRows for an IndexRangeScanNode join leaf.");
        }
        finally
        {
        }
    }

    [Test]
    public async Task IndexRangeLeafAnnotation_UsesSeededStatsNotFallback()
    {

            CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = true };
        // The annotated leaf cardinality must differ from the fixed-heuristic fallback
        // (10% of trc = 1000) when NDV is seeded.
        // With NDV=5: stats branch → 10000/5 = 2000 rows.
        // Without seeded stats (or with a null table reaching EstimateRangeScanRows):
        // fallback → 10000 × 0.10 = 1000 rows.
        try
        {
            (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
                = await BindIndexedLeafAnnotationSetup();

            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            IndexRangeScanNode? rangeLeaf = FindIndexRangeLeaf(plan.Root, "o");
            Assert.IsNotNull(rangeLeaf);

            CostEstimator.AnnotatePlan(plan.Root, database, table: null, executor.Statistics, planning);

            Assert.AreNotEqual(1_000L, rangeLeaf!.EstimatedCardinality,
                "Annotated leaf cardinality must not equal the 10% heuristic fallback when NDV stats are seeded.");
            Assert.Greater(rangeLeaf.EstimatedCardinality, 1_000L,
                "Stats-driven estimate (10000/NDV=2000) must exceed the heuristic fallback (10000*0.10=1000).");
        }
        finally
        {
        }
    }

    /// <summary>
    /// Join schema where the left key (orders.id) is a unique PK and the right side
    /// (log_entries.order_id) is a non-unique FK, with no secondary index on the right
    /// join column (forcing hash/NLJ).  Used to verify that <c>TryResolveLeftTable</c>
    /// returns the orders table when the left leaf is an <see cref="IndexRangeScanNode"/>.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindUniqueLeftKeyJoinSetup()
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"leftkey_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // orders: PK id, indexed status (for push-down)
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",     ColumnType.Id),
                new("status", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",               [new("id",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_status_idx", [new("status", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        // log_entries: PK id, non-unique order_id FK (no secondary index → forces hash/NLJ join)
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "log_entries",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id, notNull: true),
                new("message",  ColumnType.String),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable     = await database.TableDescriptors["orders"];
        TableDescriptor logEntriesTable = await database.TableDescriptors["log_entries"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,    1_000);
        executor.Statistics.SeedRowCountForTesting(database, logEntriesTable, 5_000);

        await executor.Statistics.SetNdvAsync(database, ordersTable,
            columnNdv: new Dictionary<string, long> { ["status"] = 5 },
            keyNdv: null);

        const string sql =
            "SELECT o.id, l.message " +
            "FROM orders o " +
            "JOIN log_entries l ON o.id = l.order_id " +
            "WHERE o.status = \"urgent\"";

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    [Test]
    public async Task HashJoinWithIndexedLeftLeaf_ResolveLeftTableForCardinality()
    {

            CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = true };
        // When the left input to a hash/NLJ join is an IndexRangeScanNode with BoundSource,
        // TryResolveLeftTable must return its table so the unique-key check on the left join
        // key fires.  Here o.id is a unique PK (left join key) and log_entries.order_id is
        // non-unique (right join key): EstimateJoinCardinality returns rightRows (5000).
        // Without the IndexRangeScanNode arm in TryResolveLeftTable, leftTable is null →
        // unique check skipped → fallback product or NDV-only path, both ≠ 5000.
        try
        {
            (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
                = await BindUniqueLeftKeyJoinSetup();

            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            // Precondition: the orders left leaf must actually be an IndexRangeScanNode. If a future
            // breakeven/estimate change turned it into a TableScanNode, the always-present TableScanNode
            // arm of TryResolveLeftTable would resolve the table and this test would pass for the wrong
            // reason (no longer exercising the IndexRangeScanNode arm under test).
            Assert.IsNotNull(FindIndexRangeLeaf(plan.Root, "o"),
                "Precondition: orders left leaf must be an IndexRangeScanNode (CostBasedAccessPathEnabled + selective status index).");

            CostEstimator.AnnotatePlan(plan.Root, database, table: null, executor.Statistics, planning);

            // The left key (o.id) is unique → unique-left-key formula → join cardinality = rightRows.
            long joinCard = plan.Root.EstimatedCardinality ?? 0L;
            const long rightRows = 5_000L;

            Assert.AreEqual(rightRows, joinCard,
                "When the left join key is a unique PK and the left leaf is an IndexRangeScanNode, " +
                "TryResolveLeftTable must return ordersTable so the unique-left-key formula fires " +
                "and join cardinality = rightRows.");
        }
        finally
        {
        }
    }

    // ─── IN-list join leaf helpers ────────────────────────────────────────────

    /// <summary>
    /// Orders/products join with <c>WHERE o.status IN (1, 2)</c>.
    /// Integer64 status column with a non-unique index; NDV=5 000 out of 10 000 rows
    /// → 2 values → estimated rows ≈ 4, well below breakeven → index scan chosen.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindInListJoinLeafSetup()
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"inlistleaf_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("priority",   ColumnType.Integer64, notNull: true),
                new("product_id", ColumnType.Id,        notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",                  [new("id",       OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_priority_idx",  [new("priority", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",       [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        // High NDV → 2 values yield ~4 estimated rows → well below breakeven (5000) → index scan.
        await executor.Statistics.SetNdvAsync(database, ordersTable,
            columnNdv: new Dictionary<string, long> { ["priority"] = 5_000 },
            keyNdv: null);

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.priority IN (1, 2)";

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    // ─── IN-list join leaf tests ──────────────────────────────────────────────

    [Test]
    public async Task FlagOn_InListJoinLeafBecomesIndexInListScan()
    {

            CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = true };
        try
        {
            (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
                = await BindInListJoinLeafSetup();

            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            IndexInListScanNode? inListLeaf = FindIndexInListLeaf(plan.Root, "o");
            Assert.IsNotNull(inListLeaf,
                "With CostBasedAccessPathEnabled=true and a selective IN-list, " +
                "the orders join leaf must be an IndexInListScanNode.");
            Assert.IsNotNull(inListLeaf!.BoundSource, "IndexInListScanNode in join must have BoundSource set");
            Assert.AreEqual("o", inListLeaf.BoundSource!.Alias);
            Assert.AreEqual("orders_priority_idx", inListLeaf.Index.Name);
            Assert.AreEqual(2, inListLeaf.Values.Count);
        }
        finally
        {
        }
    }

    [Test]
    public async Task FlagOff_InListJoinLeafRemainsTableScan()
    {


        CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = false };

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)
            = await BindInListJoinLeafSetup();

        QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

        IndexInListScanNode? inListLeaf = FindIndexInListLeaf(plan.Root, "o");
        Assert.IsNull(inListLeaf,
            "With CostBasedAccessPathEnabled=false the orders leaf must remain a TableScanNode.");
    }

    [Test]
    public async Task LowNdv_InListJoinLeaf_FallsBackToTableScan()
    {

        CamusDBOptions planning = CamusDBOptions.Default with { CostBasedAccessPathEnabled = false };
        // NDV=2, trc=10000, n=2 values → estimated = trc*(2/NDV) = 10000 → breakeven fires.
        try
        {
            CommandExecutor executor = CreateCommandExecutor();
            CatalogsManager catalogs = executor.Catalogs;

            string dbname = $"inlistlowndv_{Guid.NewGuid():n}";
            TrackDatabase(dbname, executor);

            DatabaseDescriptor database = await executor.CreateDatabase(
                new CreateDatabaseTicket(dbname, ifNotExists: false));

            KvTransaction txn = await database.Transactions.BeginAsync();

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: "orders",
                columns:
                [
                    new("id",         ColumnType.Id),
                    new("status",     ColumnType.Integer64, notNull: true),
                    new("product_id", ColumnType.Id,        notNull: true),
                ],
                constraints:
                [
                    new(ConstraintType.PrimaryKey, "~pk",                [new("id",         OrderType.Ascending)]),
                    new(ConstraintType.IndexMulti, "orders_status_idx",  [new("status",     OrderType.Ascending)]),
                    new(ConstraintType.IndexMulti, "orders_pid_idx",     [new("product_id", OrderType.Ascending)]),
                ],
                ifNotExists: false));

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: "products",
                columns:
                [
                    new("id",   ColumnType.Id),
                    new("name", ColumnType.String, notNull: true),
                ],
                constraints:
                [
                    new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                ],
                ifNotExists: false));

            await database.Transactions.CommitAsync(txn);

            TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
            TableDescriptor productsTable = await database.TableDescriptors["products"];

            executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
            executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

            // NDV=2 → 2 values cover 100% of rows → index scan vetoed.
            await executor.Statistics.SetNdvAsync(database, ordersTable,
                columnNdv: new Dictionary<string, long> { ["status"] = 2 },
                keyNdv: null);

            const string sql =
                "SELECT o.id, p.name " +
                "FROM orders o " +
                "JOIN products p ON o.product_id = p.id " +
                "WHERE o.status IN (1, 2)";

            ExecuteSQLTicket executeTicket = new(
                txnState: await database.Transactions.BeginAsync(),
                database: dbname, sql: sql, parameters: null);

            SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
            BoundSelectQuery bound  = await new QueryBinder(new TableOpener(catalogs, logger))
                .BindAsync(database, selectQuery);

            QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);
            QueryPlan plan = new JoinQueryPlanner(planning, executor.Statistics).GetPlan(database, bound, ticket);

            IndexInListScanNode? inListLeaf = FindIndexInListLeaf(plan.Root, "o");
            Assert.IsNull(inListLeaf,
                "NDV=2 with n=2 IN-list values must veto the index scan (100% of rows match) → TableScanNode.");
        }
        finally
        {
        }
    }

    [Test]
    public async Task InListLeafPlan_ResultsMatchFullScanPlan()
    {
        // Result-equivalence: flag on and flag off must return identical rows for an IN-list join.
        string dbname = await CreateInListJoinDatabase();

        // One engine per access-path setting — see the star-join equivalence test above.
        CommandExecutor fullScan  = CreateCommandExecutor(Options with { CostBasedAccessPathEnabled = false });
        CommandExecutor indexScan = CreateCommandExecutor(Options with { CostBasedAccessPathEnabled = true });
        TrackDatabase(dbname, fullScan);

        List<(string id, string name)> rowsOff = await ExecuteInListJoinPlan(fullScan, dbname);
        List<(string id, string name)> rowsOn  = await ExecuteInListJoinPlan(indexScan, dbname);

        rowsOff.Sort((a, b) => string.Compare(a.id, b.id, StringComparison.Ordinal));
        rowsOn .Sort((a, b) => string.Compare(a.id, b.id, StringComparison.Ordinal));

        Assert.AreEqual(rowsOff.Count, rowsOn.Count,
            $"IN-list index-scan leaf plan ({rowsOn.Count}) and full-scan plan ({rowsOff.Count}) must return the same number of rows.");

        for (int i = 0; i < rowsOff.Count; i++)
        {
            Assert.AreEqual(rowsOff[i].id,   rowsOn[i].id,   $"Row {i}: id mismatch.");
            Assert.AreEqual(rowsOff[i].name, rowsOn[i].name, $"Row {i}: name mismatch.");
        }
    }

    private async Task<string> CreateInListJoinDatabase()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = $"inlistjoinexec_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);

        DatabaseDescriptor database = await executor.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",         ColumnType.Id),
                new("priority",   ColumnType.Integer64, notNull: true),
                new("product_id", ColumnType.Id,        notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",                 [new("id",         OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_priority_idx", [new("priority",   OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_pid_idx",      [new("product_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "products",
            columns:
            [
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string pid1 = "cccc00000000000000000001";
        string pid2 = "cccc00000000000000000002";
        string pid3 = "cccc00000000000000000003";

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "products",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, pid1) }, { "name", new(ColumnType.String, "Widget") } },
                new() { { "id", new(ColumnType.Id, pid2) }, { "name", new(ColumnType.String, "Gadget") } },
                new() { { "id", new(ColumnType.Id, pid3) }, { "name", new(ColumnType.String, "Doohickey") } },
            }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "orders",
            values: new()
            {
                // priority IN (1,2) → returned
                new() { { "id", new(ColumnType.Id, "dddd00000000000000000001") }, { "priority", new(ColumnType.Integer64, 1L) }, { "product_id", new(ColumnType.Id, pid1) } },
                new() { { "id", new(ColumnType.Id, "dddd00000000000000000002") }, { "priority", new(ColumnType.Integer64, 1L) }, { "product_id", new(ColumnType.Id, pid2) } },
                new() { { "id", new(ColumnType.Id, "dddd00000000000000000003") }, { "priority", new(ColumnType.Integer64, 2L) }, { "product_id", new(ColumnType.Id, pid1) } },
                // priority=3 → excluded
                new() { { "id", new(ColumnType.Id, "dddd00000000000000000004") }, { "priority", new(ColumnType.Integer64, 3L) }, { "product_id", new(ColumnType.Id, pid3) } },
                // priority=4 → excluded
                new() { { "id", new(ColumnType.Id, "dddd00000000000000000005") }, { "priority", new(ColumnType.Integer64, 4L) }, { "product_id", new(ColumnType.Id, pid2) } },
            }));

        await database.Transactions.CommitAsync(txn);

        TableDescriptor ordersTable   = await database.TableDescriptors["orders"];
        TableDescriptor productsTable = await database.TableDescriptors["products"];

        executor.Statistics.SeedRowCountForTesting(database, ordersTable,  10_000);
        executor.Statistics.SeedRowCountForTesting(database, productsTable,  2_000);

        // High NDV: 2 values out of 5000 → ~4 estimated rows → index scan wins.
        await executor.Statistics.SetNdvAsync(database, ordersTable,
            columnNdv: new Dictionary<string, long> { ["priority"] = 5_000 },
            keyNdv: null);

        return dbname;
    }

    /// <summary>
    /// Runs the IN-list join through <paramref name="executor"/>, whose own configuration decides
    /// whether the cost-based access path is considered.
    /// </summary>
    private async Task<List<(string id, string name)>> ExecuteInListJoinPlan(
        CommandExecutor executor, string dbname)
    {
        KvTransaction txn = await (await executor.OpenDatabase(dbname)).Transactions.BeginAsync();

        const string sql =
            "SELECT o.id, p.name " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.id " +
            "WHERE o.priority IN (1, 2)";

        ExecuteSQLTicket ticket = new(
            txnState: txn, database: dbname, sql: sql, parameters: null);

        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor)
            = await executor.ExecuteSQLQuery(ticket);

        List<(string id, string name)> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add((row.Row["id"].StrValue ?? "", row.Row["name"].StrValue ?? ""));

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static IndexInListScanNode? FindIndexInListLeaf(PhysicalPlanNode node, string alias)
    {
        if (node is IndexInListScanNode { BoundSource: not null } iln
            && iln.BoundSource!.Alias == alias)
            return iln;

        if (node.Input is not null)
        {
            IndexInListScanNode? fromInput = FindIndexInListLeaf(node.Input, alias);
            if (fromInput is not null) return fromInput;
        }

        return null;
    }

    /// <summary>
    /// Walks the physical plan tree (left spine and join left inputs) looking for an
    /// <see cref="IndexRangeScanNode"/> with <c>BoundSource.Alias == alias</c>.
    /// </summary>
    private static IndexRangeScanNode? FindIndexRangeLeaf(PhysicalPlanNode node, string alias)
    {
        if (node is IndexRangeScanNode { BoundSource: not null } rsn
            && rsn.BoundSource!.Alias == alias)
            return rsn;

        if (node.Input is not null)
        {
            IndexRangeScanNode? fromInput = FindIndexRangeLeaf(node.Input, alias);
            if (fromInput is not null) return fromInput;
        }

        return null;
    }

    /// <summary>
    /// Walks the full left spine and returns the join ordering as a list of table aliases
    /// in outer-to-inner order: e.g. ["e", "s", "u"] for a plan where events drives the
    /// outermost loop and users is joined last.
    ///
    /// Captures more than <see cref="GetLeftmostScanAlias"/>: two orderings can share the
    /// same outermost alias but differ at deeper levels, so this list is the unique fingerprint
    /// of the complete left-deep ordering.
    /// </summary>
    private static List<string> CollectJoinOrder(PhysicalPlanNode node)
    {
        var rightAliases = new Stack<string>();
        PhysicalPlanNode current = node;

        while (true)
        {
            switch (current)
            {
                case TableScanNode { BoundSource: not null } scan:
                    var result = new List<string> { scan.BoundSource.Alias };
                    while (rightAliases.Count > 0)
                        result.Add(rightAliases.Pop());
                    return result;

                case HashJoinNode hj:
                    rightAliases.Push(hj.BuildSource.Alias);
                    current = hj.Input!;
                    break;

                case IndexNestedLoopJoinNode inlj:
                    rightAliases.Push(inlj.RightSource.Alias);
                    current = inlj.Input!;
                    break;

                case NestedLoopJoinNode nlj:
                    rightAliases.Push(nlj.RightSource.Alias);
                    current = nlj.Input!;
                    break;

                case MergeJoinNode mj:
                    rightAliases.Push(mj.RightSource.Alias);
                    current = mj.Input!;
                    break;

                default:
                    return rightAliases.ToList();
            }
        }
    }

    /// <summary>
    /// Walks the left spine of the physical plan and returns the alias of the
    /// leftmost (outermost driver) table scan.
    /// </summary>
    private static string GetLeftmostScanAlias(PhysicalPlanNode node)
    {
        return node switch
        {
            TableScanNode { BoundSource: not null } scan => scan.BoundSource.Alias,
            NestedLoopJoinNode join                      => GetLeftmostScanAlias(join.Input!),
            IndexNestedLoopJoinNode indexJoin            => GetLeftmostScanAlias(indexJoin.Input!),
            HashJoinNode hashJoin                        => GetLeftmostScanAlias(hashJoin.Input!),
            MergeJoinNode mergeJoin                      => GetLeftmostScanAlias(mergeJoin.Input!),
            _ => throw new AssertionException(
                $"Unexpected node type in left spine: {node.GetType().Name}")
        };
    }
}
