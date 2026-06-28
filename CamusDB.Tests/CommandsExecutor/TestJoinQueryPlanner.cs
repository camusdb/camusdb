
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
public sealed class TestJoinQueryPlanner : BaseTest
{

    [Test]
    public async Task Plan_PushesSingleTableWherePredicateToUsersScan()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Unindexed equi-join → HashJoinNode; the WHERE predicate is pushed to the users scan.
        HashJoinNode joinNode = FindHashJoinNode(plan.Root);
        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");

        Assert.IsNotNull(usersScan.ExecutionFilter);
        Assert.IsNull(joinNode.BuildExecutionFilter);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public async Task Plan_PushesSingleTablePredicateToRightScan()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE p.published = true");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Unindexed equi-join → HashJoinNode; the WHERE predicate is pushed to the build side.
        HashJoinNode joinNode = FindHashJoinNode(plan.Root);
        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");

        Assert.IsNull(usersScan.ExecutionFilter);
        Assert.IsNotNull(joinNode.BuildExecutionFilter);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public async Task Plan_KeepsCrossTablePredicateAsPostJoinFilter()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id WHERE p.user_id = u.id AND u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Unindexed equi-join → HashJoinNode.
        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");
        HashJoinNode joinNode = FindHashJoinNode(plan.Root);

        Assert.IsNotNull(usersScan.ExecutionFilter);
        Assert.IsNotNull(plan.ExecutionFilter);
        Assert.IsNull(joinNode.BuildExecutionFilter);
    }

    [Test]
    public async Task Plan_UsesHashJoinWhenRightJoinKeyIsNotIndexed()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
            indexPostsUserId: false);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Equi-join with no index on the right key → HashJoinNode (not NestedLoopJoinNode).
        Assert.IsInstanceOf<HashJoinNode>(plan.Root);
        Assert.That(plan.Root, Is.Not.TypeOf<IndexNestedLoopJoinNode>());
        Assert.That(plan.Root, Is.Not.TypeOf<NestedLoopJoinNode>());
    }

    [Test]
    public async Task Plan_UsesIndexNestedLoopJoinWhenRightJoinKeyIsIndexed()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
            indexPostsUserId: true);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root);

        IndexNestedLoopJoinNode joinNode = (IndexNestedLoopJoinNode)plan.Root;
        Assert.AreEqual("posts_user_id_idx", joinNode.Index.Name);
        Assert.AreEqual("u.id", joinNode.LeftLookupColumn);
        Assert.AreEqual("user_id", joinNode.RightIndexColumn);
    }

    [Test]
    public async Task Plan_UsesIndexNestedLoopJoinForCommaJoinEquiPredicate()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u, posts p WHERE p.user_id = u.id",
            indexPostsUserId: true);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root);
    }

    [Test]
    public async Task Plan_PushesSingleSourceFilterForCommaJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u, posts p WHERE p.user_id = u.id AND u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");
        Assert.IsNotNull(usersScan.ExecutionFilter);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public async Task Plan_PushesRequiredColumnsPerAlias()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");

        CollectionAssert.AreEquivalent(new[] { "email", "role", "id" }, plan.RequiredColumnsByAlias!["u"]);
        CollectionAssert.AreEquivalent(new[] { "title", "user_id" }, plan.RequiredColumnsByAlias!["p"]);
        CollectionAssert.AreEquivalent(new[] { "email", "role", "id" }, usersScan.RequiredColumns!);
    }

    [Test]
    public async Task Plan_DerivedTableJoinAnalyzesRequiredColumnsWithoutDerivedAliasEntry()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, d.post_count FROM (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) d "
            + "JOIN app_users u ON u.id = d.user_id");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        Assert.IsFalse(plan.RequiredColumnsByAlias!.ContainsKey("d"));
        CollectionAssert.AreEquivalent(new[] { "email", "id" }, plan.RequiredColumnsByAlias["u"]);
    }

    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)> BindJoinQuery(
        string sql,
        bool indexPostsUserId = false)
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"joinplanner_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "app_users",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
                new("role", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("user_id", ColumnType.Id, notNull: true),
                new("title", ColumnType.String, notNull: true),
                new("published", ColumnType.Bool),
            },
            constraints: indexPostsUserId
                ?
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                    new(ConstraintType.IndexMulti, "posts_user_id_idx", new ColumnIndexInfo[] { new("user_id", OrderType.Ascending) }),
                ]
                :
                [
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                ],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: sql,
            parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Join-Order Heuristics
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R7_PointLookupSourceGoesFirst_WhenDeclaredSecond()
    {
        // posts declared first, app_users second — but users has a unique PK WHERE predicate.
        // After reordering, users (score 0) must be on the outer (left) side.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT p.title, u.email FROM posts p JOIN app_users u ON p.user_id = u.id WHERE u.id = '507f1f77bcf86cd799439011'",
            indexPostsUserId: true);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Root is a join node; its Input (left) must be the users scan.
        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root);
        TableScanNode outerScan = FindScanForAlias(plan.Root, "u");
        Assert.AreEqual("u", outerScan.BoundSource!.Alias);
    }

    [Test]
    public async Task R7_DeclaredOrderPreservedWhenBothSourcesHaveNoPredicates()
    {
        // Neither source has a WHERE predicate — both score 2.
        // Stable sort must keep declared order (p first, then u).
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT p.title, u.email FROM posts p JOIN app_users u ON p.user_id = u.id");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // The outer scan should still be 'p' (declared first, equal priority).
        TableScanNode outerScan = FindScanForAlias(plan.Root, "p");
        Assert.AreEqual("p", outerScan.BoundSource!.Alias);
    }

    [Test]
    public async Task R7_PointLookupAlreadyFirst_NoUnnecessaryRebuild()
    {
        // users declared first already AND has a unique PK predicate → no reorder needed.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.id = '507f1f77bcf86cd799439011'",
            indexPostsUserId: true);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Root must still be an INLJ with users on the outer (left) side.
        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root);
        TableScanNode outerScan = FindScanForAlias(plan.Root, "u");
        Assert.AreEqual("u", outerScan.BoundSource!.Alias);
    }

    [Test]
    public async Task R7_ThreeTableJoin_UniquePredicateSourceGoesFirst()
    {
        // 3-table join: orders → app_users → comments (synthetic). users has a unique WHERE predicate.
        // Expected: users goes first (outer), then declared order for the rest.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindThreeTableJoinQuery(
            "SELECT u.email, p.title, c.body " +
            "FROM posts p JOIN app_users u ON u.id = p.user_id JOIN comments c ON c.post_id = p.id " +
            "WHERE u.id = '507f1f77bcf86cd799439011'",
            indexPostsUserId: true,
            indexCommentsPostId: false);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Outermost left scan should be 'u' (moved to front).
        TableScanNode outerScan = FindScanForAlias(plan.Root, "u");
        Assert.AreEqual("u", outerScan.BoundSource!.Alias);
    }

    [Test]
    public async Task R7_ThreeTableJoin_ChainPredicatesFallBackToDeclaredOrder()
    {
        // 3-table chain join A→B→C with no direct A→C predicate.
        // If the optimizer tried to put C first (score 0), feasibility would fail because
        // no C→A predicate exists.  The optimizer must fall back to declared order.
        // In this test no source has a WHERE predicate, so declared order must be preserved.
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindThreeTableJoinQuery(
            "SELECT u.email, p.title, c.body " +
            "FROM app_users u JOIN posts p ON p.user_id = u.id JOIN comments c ON c.post_id = p.id",
            indexPostsUserId: false,
            indexCommentsPostId: false);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        // Declared order: u → p → c.  The outermost-left scan must be 'u'.
        TableScanNode outerScan = FindScanForAlias(plan.Root, "u");
        Assert.AreEqual("u", outerScan.BoundSource!.Alias);
    }

    [Test]
    public async Task R7_Exec_ThreeTableJoin_ReturnsCorrectRows()
    {
        // End-to-end: insert data into all three tables, then run the join query and verify rows.
        CommandExecutor executor = CreateCommandExecutor();

        string dbname = $"r7exec_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "app_users",
            columns:
            [
                new("id", ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
                new("role", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns:
            [
                new("id", ColumnType.Id),
                new("user_id", ColumnType.Id, notNull: true),
                new("title", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "posts_user_id_idx", [new("user_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "comments",
            columns:
            [
                new("id", ColumnType.Id),
                new("post_id", ColumnType.Id, notNull: true),
                new("body", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "comments_post_id_idx", [new("post_id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        string userId = "507f1f77bcf86cd799439011";
        string postId  = "507f1f77bcf86cd799439012";
        string cmtId   = "507f1f77bcf86cd799439013";

        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: "app_users",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, userId) },
                    { "email", new(ColumnType.String, "alice@example.com") },
                    { "role",  new(ColumnType.String, "admin") },
                }
            }));

        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: "posts",
            values: new()
            {
                new()
                {
                    { "id",      new(ColumnType.Id, postId) },
                    { "user_id", new(ColumnType.Id, userId) },
                    { "title",   new(ColumnType.String, "Hello World") },
                }
            }));

        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: "comments",
            values: new()
            {
                new()
                {
                    { "id",      new(ColumnType.Id, cmtId) },
                    { "post_id", new(ColumnType.Id, postId) },
                    { "body",    new(ColumnType.String, "Great post!") },
                }
            }));

        await database.Transactions.CommitAsync(txn);

        // Run a 3-table join filtered by a string column (avoids the Id-vs-String literal type issue).
        // The join exercises the full reordered execution path.
        KvTransaction readTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket queryTicket = new(
            txnState: readTxn,
            database: dbname,
            sql: "SELECT u.email, p.title, c.body " +
                 "FROM posts p JOIN app_users u ON u.id = p.user_id JOIN comments c ON c.post_id = p.id " +
                 "WHERE u.role = \"admin\"",
            parameters: null);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(queryTicket);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await database.Transactions.CommitAsync(readTxn);

        // The projector strips the alias prefix: "u.email" → key "email", etc.
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("alice@example.com", rows[0].Row["email"].StrValue);
        Assert.AreEqual("Hello World",       rows[0].Row["title"].StrValue);
        Assert.AreEqual("Great post!",       rows[0].Row["body"].StrValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)> BindThreeTableJoinQuery(
        string sql,
        bool indexPostsUserId = false,
        bool indexCommentsPostId = false)
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"joinplanner3_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "app_users",
            columns:
            [
                new("id", ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
                new("role", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        ConstraintInfo[] postsConstraints = indexPostsUserId
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "posts_user_id_idx", [new("user_id", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns:
            [
                new("id", ColumnType.Id),
                new("user_id", ColumnType.Id, notNull: true),
                new("title", ColumnType.String, notNull: true),
            ],
            constraints: postsConstraints,
            ifNotExists: false));

        ConstraintInfo[] commentsConstraints = indexCommentsPostId
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "comments_post_id_idx", [new("post_id", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "comments",
            columns:
            [
                new("id", ColumnType.Id),
                new("post_id", ColumnType.Id, notNull: true),
                new("body", ColumnType.String, notNull: true),
            ],
            constraints: commentsConstraints,
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: sql,
            parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket);
    }

    private static TableScanNode FindScanForAlias(PhysicalPlanNode node, string alias)
    {
        switch (node)
        {
            case TableScanNode { BoundSource: not null } scan when scan.BoundSource.Alias == alias:
                return scan;

            case NestedLoopJoinNode join:
                return FindScanForAlias(join.Input!, alias);

            case IndexNestedLoopJoinNode indexJoin:
                return FindScanForAlias(indexJoin.Input!, alias);

            case HashJoinNode hashJoin:
                return FindScanForAlias(hashJoin.Input!, alias);

            default:
                throw new AssertionException($"Scan for alias '{alias}' not found in join plan");
        }
    }

    private static NestedLoopJoinNode FindJoinNode(PhysicalPlanNode node)
    {
        return node switch
        {
            NestedLoopJoinNode join => join,
            _ => throw new AssertionException("Join node not found in join plan"),
        };
    }

    private static HashJoinNode FindHashJoinNode(PhysicalPlanNode node)
    {
        return node switch
        {
            HashJoinNode hj => hj,
            _ => throw new AssertionException($"HashJoinNode not found; got {node.GetType().Name}"),
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Build-side selection
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Variant of <see cref="BindJoinQuery"/> that also returns the <see cref="CommandExecutor"/>
    /// so callers can access <see cref="CommandExecutor.Statistics"/> to seed row counts before
    /// constructing the plan.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindJoinQueryWithExecutor(string sql, bool indexPostsUserId = false)
    {
        (DatabaseDescriptor db, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(sql, indexPostsUserId);
        // BindJoinQuery uses CreateCommandExecutor() internally; re-bind returns the same database
        // but we need access to the executor for stats injection. The simplest approach is to re-bind
        // using a second executor that shares the same database name — but that would require
        // a shared KV node.
        //
        // Instead, expose BindJoinQuery via a new overload that captures the executor.
        // This internal helper rebuilds from scratch with executor access.
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;
        string dbname = $"hj12_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "app_users",
            columns: [new("id", ColumnType.Id), new("email", ColumnType.String, notNull: true), new("role", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "posts",
            columns: [new("id", ColumnType.Id), new("user_id", ColumnType.Id, notNull: true), new("title", ColumnType.String, notNull: true), new("published", ColumnType.Bool)],
            constraints: indexPostsUserId
                ? [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]), new(ConstraintType.IndexMulti, "posts_user_id_idx", [new("user_id", OrderType.Ascending)])]
                : [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery boundQ = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);
        QueryTicket ticketQ = QueryTicketAdapter.ToQueryTicket(boundQ, executeTicket);

        return (database, boundQ, ticketQ, executor);
    }

    [Test]
    public async Task LeftSideSmaller_BuildSideIsLeft()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        // Seed: app_users has 10 rows, posts has 10 000 rows → left (app_users) is smaller.
        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 10);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 10_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        HashJoinNode join = FindHashJoinNode(plan.Root);
        Assert.AreEqual(HashJoinBuildSide.Left, join.BuildSide,
            "app_users (10 rows) < posts (10 000 rows) → build on left");
    }

    [Test]
    public async Task RightSideSmaller_BuildSideIsRight()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        // Seed: app_users has 50 000 rows, posts has 200 rows → right (posts) is smaller.
        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 50_000);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 200);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        HashJoinNode join = FindHashJoinNode(plan.Root);
        Assert.AreEqual(HashJoinBuildSide.Right, join.BuildSide,
            "posts (200 rows) < app_users (50 000 rows) → build on right");
    }

    [Test]
    public async Task EqualCardinality_DefaultsToRight()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        // Seed equal row counts → tie → default to right.
        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 1_000);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 1_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        HashJoinNode join = FindHashJoinNode(plan.Root);
        Assert.AreEqual(HashJoinBuildSide.Right, join.BuildSide,
            "equal cardinality → tie-break defaults to right");
    }

    [Test]
    public async Task NoStats_DefaultsToRight()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, _) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        // No stats injected — planner has no StatisticsManager.
        QueryPlan plan = new JoinQueryPlanner(stats: null).GetPlan(database, bound, ticket);

        HashJoinNode join = FindHashJoinNode(plan.Root);
        Assert.AreEqual(HashJoinBuildSide.Right, join.BuildSide,
            "absent stats → safe fallback to right");
    }

    // Direct unit test for the EstimatePhysicalNodeRows infinite-recurse fix.
    //
    // The bug: the IndexRangeScanNode arm did `EstimatePhysicalNodeRows(node.Input ?? node)`, but
    // IndexRangeScanNode.Input is always null, so it re-passed the same node and recursed forever →
    // StackOverflow (uncatchable in .NET). The fix returns DefaultTableRowCount without recursing.
    //
    // Note this case is NOT reachable through GetPlan today — BuildJoinTree only emits TableScanNode
    // (never IndexRangeScanNode/IndexLookupNode) into a join subtree, so ChooseBuildSide never sees
    // one. The arm is defensive, so we test it directly rather than through the planner.
    [Test]
    public void EstimatePhysicalNodeRows_IndexRangeScanNode_ReturnsDefaultWithoutRecursing()
    {
        TableIndexSchema index = new("idx", ["col"], IndexType.Multi);
        IndexRangeScanNode node = new(index, fromBound: null, fromInclusive: true, toBound: null, toInclusive: true);

        // database/stats are unused on this arm; a StackOverflow here (the old bug) would crash the
        // process rather than fail the assert, so reaching the assert at all proves no recursion.
        long rows = JoinQueryPlanner.EstimatePhysicalNodeRows(node, database: null!, stats: null);

        Assert.AreEqual(CostEstimator.DefaultTableRowCount, rows);
    }

    [Test]
    public void EstimatePhysicalNodeRows_IndexLookupNode_ReturnsOne()
    {
        TableIndexSchema index = new("idx", ["col"], IndexType.Unique);
        IndexLookupNode node = new(index, new ColumnValue(ColumnType.Integer64, 1L));

        long rows = JoinQueryPlanner.EstimatePhysicalNodeRows(node, database: null!, stats: null);
        Assert.AreEqual(1, rows);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Hash join cost model
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task HashJoinCost_MuchLessThanNestedLoopForSameInputs()
    {
        // For two unindexed N-row inputs:
        //   NestedLoop cost ≈ N × N   KV range-scan entries = N²
        //   HashJoin cost  ≈ N + N   KV range-scan entries + N × 0.1 in-memory rows
        //   For N = 1 000 → NLJ Total = 1 000 000,  HJ Total ≈ 2 100
        //
        // Both plans are built with the same stats so the comparison is apples-to-apples.
        const long N = 1_000;

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, N);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, N);

        // Hash-join plan (unindexed equi-join → planner picks HashJoinNode).
        QueryPlan hashPlan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);
        HashJoinNode hashJoin = FindHashJoinNode(hashPlan.Root);

        // Manually build an equivalent NestedLoopJoinNode plan and annotate it.
        // We reuse the same left input and right source so the stats context is identical.
        NestedLoopJoinNode nljNode = new(hashJoin.Input!, hashJoin.BuildSource, hashJoin.OnPredicate!);
        CostEstimator.AnnotatePlan(nljNode, database, table: null, executor.Statistics);

        Assert.IsNotNull(hashJoin.Cost, "hash join cost must be annotated");
        Assert.IsNotNull(nljNode.Cost,  "NLJ cost must be annotated");

        double hashCost = hashJoin.Cost!.Value.Total;
        double nljCost  = nljNode.Cost!.Value.Total;

        Assert.Greater(nljCost, hashCost,
            $"NLJ cost ({nljCost}) must exceed hash-join cost ({hashCost}) for {N}×{N} inputs");

        // For N=1000, NLJ ≈ 1_000_000 while hash join ≈ 2_100 — check the ratio is substantial.
        Assert.Greater(nljCost / hashCost, 100,
            $"Hash join should be at least 100× cheaper than NLJ at N={N}");

        // Sanity: hash join cost fields
        Assert.AreEqual(N + N, hashJoin.Cost.Value.KvRangeScanEntries,
            "build scan + probe scan = 2N range entries");
        Assert.AreEqual(N, hashJoin.Cost.Value.InMemoryRows,
            "build side materialised in memory");
        Assert.AreEqual(0, hashJoin.Cost.Value.KvPointLookups);
    }

    [Test]
    public async Task HashJoinCost_Annotated_CardinalityNotNull()
    {
        // EstimatedCardinality must be set on HashJoinNode after AnnotatePlan, like every other node.
        const long N = 500;

        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id");

        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, N);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, N);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);
        HashJoinNode join = FindHashJoinNode(plan.Root);

        Assert.IsNotNull(join.EstimatedCardinality, "EstimatedCardinality must be set by AnnotatePlan");
        Assert.Greater(join.EstimatedCardinality!.Value, 0);
        Assert.IsNotNull(join.Cost, "Cost must be set");
        Assert.Greater(join.Cost!.Value.Total, 0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cost-based algorithm selection
    // ─────────────────────────────────────────────────────────────────────────

    // Indexed right key + small outer → INLJ should still win.
    // INLJ cost ≈ 2 × 10 = 20;  hash cost ≈ (10 + 100 000) + 10 × 0.1 ≈ 100 011.
    [Test]
    public async Task IndexedRightKey_SmallOuter_PlansIndexNestedLoop()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
                indexPostsUserId: true);

        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 10);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 100_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root,
            "Small outer (10) + large indexed right (100 000) → INLJ is cheaper");
    }

    // Indexed right key + large outer → hash join should win.
    // INLJ cost ≈ 2 × 100 000 = 200 000;  hash cost ≈ (100 000 + 10) + 10 × 0.1 ≈ 100 011.
    [Test]
    public async Task IndexedRightKey_LargeOuter_PlansHashJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
                indexPostsUserId: true);

        TableDescriptor usersTable = await database.TableDescriptors["app_users"];
        TableDescriptor postsTable = await database.TableDescriptors["posts"];
        executor.Statistics.SeedRowCountForTesting(database, usersTable, 100_000);
        executor.Statistics.SeedRowCountForTesting(database, postsTable, 10);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<HashJoinNode>(plan.Root,
            "Large outer (100 000) + small indexed right (10) → hash join is cheaper");
    }

    // No stats → always prefer INLJ (same behaviour when stats are absent).
    [Test]
    public async Task IndexedRightKey_NoStats_DefaultsToIndexNestedLoop()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, _) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
                indexPostsUserId: true);

        // Pass null stats — planner must fall back to INLJ.
        QueryPlan plan = new JoinQueryPlanner(stats: null).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<IndexNestedLoopJoinNode>(plan.Root,
            "Absent stats must fall back to INLJ");
    }

    // Non-equi join → nested-loop (no equi-keys extractable → neither hash nor INLJ).
    [Test]
    public async Task NonEquiJoin_PlansNestedLoop()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, _) =
            await BindJoinQueryWithExecutor(
                "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.published = true",
                indexPostsUserId: true);

        QueryPlan plan = new JoinQueryPlanner(stats: null).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<NestedLoopJoinNode>(plan.Root,
            "Non-equi join must use nested-loop regardless of index");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Merge-join sort elision / explicit SortNode wiring
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Merge-join sort-elision test fixture: orders (left) JOIN line_items (right) ON o.ext_key = li.order_ext_key.
    /// <para>
    /// The join key is a non-PK string column on both sides, so no existing index covers it
    /// by default — <paramref name="indexLeftJoinKey"/> and <paramref name="indexRightJoinKey"/>
    /// control whether a secondary index is added on each side. This cleanly separates the
    /// "index exists / does not exist" cases from the always-present <c>~pk</c> primary key.
    /// </para>
    /// <para>
    /// ForceMergeJoinForTesting is set so every inner equi-join routes through
    /// <see cref="MergeJoinNode"/> regardless of cost.
    /// </para>
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindMergeJoinQuery(
            string sql,
            bool indexLeftJoinKey  = false,
            bool indexRightJoinKey = false)
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"hj32_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // orders: PK on id, optional secondary index on ext_key (the join column).
        ConstraintInfo[] ordersConstraints = indexLeftJoinKey
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_ext_key_idx", [new("ext_key", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",      ColumnType.Id),
                new("ext_key", ColumnType.String, notNull: true),
                new("name",    ColumnType.String, notNull: true),
            ],
            constraints: ordersConstraints,
            ifNotExists: false));

        // line_items: PK on id, optional secondary index on order_ext_key (the join column).
        ConstraintInfo[] itemsConstraints = indexRightJoinKey
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_ext_key_idx", [new("order_ext_key", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns:
            [
                new("id",            ColumnType.Id),
                new("order_ext_key", ColumnType.String),
                new("product",       ColumnType.String, notNull: true),
            ],
            constraints: itemsConstraints,
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        // Force merge join for all inner equi-joins in this test.
        executor.Statistics.ForceMergeJoinForTesting = true;

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, sql: sql, parameters: null);

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);
        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    // SQL uses the non-PK join key columns ext_key / order_ext_key.
    private const string MjJoinSql =
        "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_ext_key = o.ext_key";

    /// <summary>
    /// When neither side has an index on the join key, the planner must insert a
    /// <see cref="SortNode"/> on both sides of the merge join.
    /// </summary>
    [Test]
    public async Task MergeJoin_SortElision_NoIndex_BothSidesGetSortNode()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindMergeJoinQuery(MjJoinSql, indexLeftJoinKey: false, indexRightJoinKey: false);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root);
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;

        // Left Input must be a SortNode (wrapping the orders TableScanNode).
        Assert.IsInstanceOf<SortNode>(mergeJoin.Input,
            "No index on orders.ext_key → left side must be wrapped in a SortNode");

        // Right physical node must be a SortNode (wrapping the line_items TableScanNode).
        Assert.IsNotNull(mergeJoin.RightPhysicalNode,
            "RightPhysicalNode must be set for table-source right sides");
        Assert.IsInstanceOf<SortNode>(mergeJoin.RightPhysicalNode,
            "No index on line_items.order_ext_key → right side must be wrapped in a SortNode");

        Assert.IsTrue(mergeJoin.LeftIsOrdered,  "LeftIsOrdered must be true (SortNode handles it)");
        Assert.IsTrue(mergeJoin.RightIsOrdered, "RightIsOrdered must be true (SortNode handles it)");
    }

    /// <summary>
    /// When both sides have an index on the join key columns, the planner must use
    /// ForcedIndex table scans on both sides — no <see cref="SortNode"/> inserted.
    /// </summary>
    [Test]
    public async Task MergeJoin_SortElision_BothIndexed_NoSortNodeInserted()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindMergeJoinQuery(MjJoinSql, indexLeftJoinKey: true, indexRightJoinKey: true);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root);
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;

        // Left: ForcedIndex scan on orders.ext_key index.
        Assert.IsInstanceOf<TableScanNode>(mergeJoin.Input,
            "orders.ext_key indexed → left side must be a ForcedIndex TableScanNode (no SortNode)");
        TableScanNode leftScan = (TableScanNode)mergeJoin.Input!;
        Assert.AreEqual(TableScanSource.ForcedIndex, leftScan.Source,
            "Left scan must use ForcedIndex source");
        Assert.IsNotNull(leftScan.Index, "Left scan must carry the index descriptor");
        Assert.AreEqual("orders_ext_key_idx", leftScan.Index!.Name);

        // Right: ForcedIndex scan on line_items.order_ext_key index.
        Assert.IsNotNull(mergeJoin.RightPhysicalNode);
        Assert.IsInstanceOf<TableScanNode>(mergeJoin.RightPhysicalNode,
            "line_items.order_ext_key indexed → right side must be a ForcedIndex TableScanNode (no SortNode)");
        TableScanNode rightScan = (TableScanNode)mergeJoin.RightPhysicalNode!;
        Assert.AreEqual(TableScanSource.ForcedIndex, rightScan.Source,
            "Right scan must use ForcedIndex source");
        Assert.IsNotNull(rightScan.Index);
        Assert.AreEqual("li_order_ext_key_idx", rightScan.Index!.Name);

        Assert.IsTrue(mergeJoin.LeftIsOrdered);
        Assert.IsTrue(mergeJoin.RightIsOrdered);
    }

    /// <summary>
    /// When only the right side has an index, the left gets a <see cref="SortNode"/>
    /// and the right uses a ForcedIndex scan.
    /// </summary>
    [Test]
    public async Task MergeJoin_SortElision_RightIndexOnly_LeftSortNode_RightForcedIndex()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindMergeJoinQuery(MjJoinSql, indexLeftJoinKey: false, indexRightJoinKey: true);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root);
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;

        Assert.IsInstanceOf<SortNode>(mergeJoin.Input,
            "No index on orders.ext_key → left must get a SortNode");

        Assert.IsNotNull(mergeJoin.RightPhysicalNode);
        Assert.IsInstanceOf<TableScanNode>(mergeJoin.RightPhysicalNode,
            "line_items.order_ext_key indexed → right must use ForcedIndex scan (no SortNode)");
        Assert.AreEqual(TableScanSource.ForcedIndex, ((TableScanNode)mergeJoin.RightPhysicalNode!).Source);
    }

    /// <summary>
    /// When only the left side has an index, the right gets a <see cref="SortNode"/>
    /// and the left uses a ForcedIndex scan.
    /// </summary>
    [Test]
    public async Task MergeJoin_SortElision_LeftIndexOnly_RightSortNode_LeftForcedIndex()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindMergeJoinQuery(MjJoinSql, indexLeftJoinKey: true, indexRightJoinKey: false);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root);
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;

        Assert.IsInstanceOf<TableScanNode>(mergeJoin.Input,
            "orders.ext_key indexed → left must use ForcedIndex scan (no SortNode)");
        Assert.AreEqual(TableScanSource.ForcedIndex, ((TableScanNode)mergeJoin.Input!).Source);

        Assert.IsNotNull(mergeJoin.RightPhysicalNode);
        Assert.IsInstanceOf<SortNode>(mergeJoin.RightPhysicalNode,
            "No index on line_items.order_ext_key → right must get a SortNode");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cost-based merge-join selection (production, no ForceMergeJoin flag)
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Variant of <see cref="BindMergeJoinQuery"/> without <c>ForceMergeJoinForTesting</c>.
    /// Used to verify production cost-based selection.
    /// </summary>
    private async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor)>
        BindCostSelectionQuery(
            string sql,
            bool indexLeftJoinKey   = false,
            bool indexRightJoinKey  = false,
            bool compositeRightIndex = false)
    {
        CommandExecutor executor = CreateCommandExecutor();
        CatalogsManager catalogs = executor.Catalogs;

        string dbname = $"hj41_{Guid.NewGuid():n}";
        TrackDatabase(dbname, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        ConstraintInfo[] ordersConstraints = indexLeftJoinKey
            ?
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "orders_ext_key_idx", [new("ext_key", OrderType.Ascending)]),
            ]
            :
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ];

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",      ColumnType.Id),
                new("ext_key", ColumnType.String, notNull: true),
                new("name",    ColumnType.String, notNull: true),
            ],
            constraints: ordersConstraints,
            ifNotExists: false));

        ConstraintInfo[] itemsConstraints = (indexRightJoinKey, compositeRightIndex) switch
        {
            // Composite (order_ext_key, product): TryMatch misses it (Columns.Length > 1),
            // FindIndexForJoinKey finds it as a leading-prefix match.
            (_, true) =>
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_composite_idx",
                    [new("order_ext_key", OrderType.Ascending), new("product", OrderType.Ascending)]),
            ],
            (true, false) =>
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_ext_key_idx", [new("order_ext_key", OrderType.Ascending)]),
            ],
            _ =>
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
            ],
        };

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns:
            [
                new("id",            ColumnType.Id),
                new("order_ext_key", ColumnType.String),
                new("product",       ColumnType.String, notNull: true),
            ],
            constraints: itemsConstraints,
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        // ForceMergeJoinForTesting is intentionally NOT set — production cost selection.

        ExecuteSQLTicket executeTicket = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname, 
            sql: sql, 
            parameters: null
        );

        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(catalogs, logger))
            .BindAsync(database, selectQuery);
        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return (database, bound, ticket, executor);
    }

    private const string CostSelectionSql =
        "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_ext_key = o.ext_key";

    /// <summary>
    /// When neither side has a secondary index on the join key, the planner must
    /// choose hash join (not merge join) — no free ordering available.
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_NoIndex_ChoosesHashJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql, indexLeftJoinKey: false, indexRightJoinKey: false);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<HashJoinNode>(plan.Root,
            "No index on either side → hash join (no free ordering for merge)");
    }

    /// <summary>
    /// When only the right side has a secondary index (the left is unindexed), the planner
    /// must choose INLJ or hash join — not merge join (left side not free-ordered).
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_RightIndexOnly_NoMergeJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql, indexLeftJoinKey: false, indexRightJoinKey: true);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsNotInstanceOf<MergeJoinNode>(plan.Root,
            "Only right side indexed → left not free-ordered → planner must NOT pick merge join");
    }

    /// <summary>
    /// When both sides have secondary indexes on the join key and the left (outer) side
    /// is estimated as large, the planner must pick merge join in production (no force flag).
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_BothIndexed_LargeInput_ChoosesMergeJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql, indexLeftJoinKey: true, indexRightJoinKey: true);

        // Seed large cardinalities so the merge preference threshold (100 rows) is exceeded.
        TableDescriptor ordersTable    = await database.TableDescriptors["orders"];
        TableDescriptor lineItemsTable = await database.TableDescriptors["line_items"];
        executor.Statistics.SeedRowCountForTesting(database, ordersTable,    10_000);
        executor.Statistics.SeedRowCountForTesting(database, lineItemsTable, 50_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root,
            "Both sides indexed + large input → cost-based selection must pick merge join");
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;
        Assert.IsTrue(mergeJoin.LeftIsOrdered,  "ForcedIndex left scan → LeftIsOrdered=true");
        Assert.IsTrue(mergeJoin.RightIsOrdered, "ForcedIndex right scan → RightIsOrdered=true");
    }

    /// <summary>
    /// When both sides have secondary indexes but the outer side is small (below
    /// <c>MergeJoinPreferenceThreshold</c>), INLJ is preferred over merge join.
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_BothIndexed_SmallOuter_PrefersInlj()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql, indexLeftJoinKey: true, indexRightJoinKey: true);

        // Seed small outer, large inner — INLJ wins (few point lookups).
        TableDescriptor ordersTable    = await database.TableDescriptors["orders"];
        TableDescriptor lineItemsTable = await database.TableDescriptors["line_items"];
        executor.Statistics.SeedRowCountForTesting(database, ordersTable,    5);
        executor.Statistics.SeedRowCountForTesting(database, lineItemsTable, 50_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsNotInstanceOf<MergeJoinNode>(plan.Root,
            "Small outer (5 rows) → INLJ preferred over merge join even with both sides indexed");
    }

    /// <summary>
    /// When the right side has a composite index whose leading column is the join key,
    /// TryMatch (requires single-column index) returns false but FindIndexForJoinKey
    /// (accepts leading-prefix match) returns true. With a small outer side the planner
    /// must fall through to hash join — not merge join — enforcing the same large-input
    /// gate as the indexed branch.
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_CompositeRightIndex_SmallOuter_ChoosesHash()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql,
                indexLeftJoinKey: true,
                compositeRightIndex: true);

        // Small outer: below MergeJoinPreferenceThreshold (100).
        TableDescriptor ordersTable    = await database.TableDescriptors["orders"];
        TableDescriptor lineItemsTable = await database.TableDescriptors["line_items"];
        executor.Statistics.SeedRowCountForTesting(database, ordersTable,    5);
        executor.Statistics.SeedRowCountForTesting(database, lineItemsTable, 50_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsNotInstanceOf<MergeJoinNode>(plan.Root,
            "Composite right index + small outer → threshold not met → must NOT pick merge join");
    }

    /// <summary>
    /// Same composite-index scenario but with a large outer side — the unindexed branch
    /// (TryMatch miss, FindIndexForJoinKey hit) should elect merge join, consistent with
    /// the large-input threshold applied in the indexed branch.
    /// </summary>
    [Test]
    public async Task MergeJoin_CostSelect_CompositeRightIndex_LargeInput_ChoosesMergeJoin()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket, CommandExecutor executor) =
            await BindCostSelectionQuery(CostSelectionSql,
                indexLeftJoinKey: true,
                compositeRightIndex: true);

        // Large outer: exceeds MergeJoinPreferenceThreshold (100).
        TableDescriptor ordersTable    = await database.TableDescriptors["orders"];
        TableDescriptor lineItemsTable = await database.TableDescriptors["line_items"];
        executor.Statistics.SeedRowCountForTesting(database, ordersTable,    10_000);
        executor.Statistics.SeedRowCountForTesting(database, lineItemsTable, 50_000);

        QueryPlan plan = new JoinQueryPlanner(executor.Statistics).GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<MergeJoinNode>(plan.Root,
            "Composite right index + large input → unindexed branch must pick merge join (same threshold as indexed branch)");
        MergeJoinNode mergeJoin = (MergeJoinNode)plan.Root;
        Assert.IsTrue(mergeJoin.LeftIsOrdered,  "Left has single-column index → ForcedIndex scan → LeftIsOrdered=true");
        Assert.IsTrue(mergeJoin.RightIsOrdered, "Right has composite index → FindIndexForJoinKey → ForcedIndex scan → RightIsOrdered=true");
    }
}
