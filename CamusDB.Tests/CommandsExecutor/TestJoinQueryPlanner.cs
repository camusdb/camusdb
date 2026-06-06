
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
[NonParallelizable]
public sealed class TestJoinQueryPlanner
{
    private static readonly ILogger<ICamusDB> logger = new LoggerFactory().CreateLogger<ICamusDB>();

    [Test]
    public async Task Plan_PushesSingleTableWherePredicateToUsersScan()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        NestedLoopJoinNode joinNode = FindJoinNode(plan.Root);
        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");

        Assert.IsNotNull(usersScan.ExecutionFilter);
        Assert.IsNull(joinNode.RightExecutionFilter);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public async Task Plan_PushesSingleTablePredicateToRightScan()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id WHERE p.published = true");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        NestedLoopJoinNode joinNode = FindJoinNode(plan.Root);
        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");

        Assert.IsNull(usersScan.ExecutionFilter);
        Assert.IsNotNull(joinNode.RightExecutionFilter);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public async Task Plan_KeepsCrossTablePredicateAsPostJoinFilter()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email FROM app_users u JOIN posts p ON p.user_id = u.id WHERE p.user_id = u.id AND u.role = \"admin\"");

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        TableScanNode usersScan = FindScanForAlias(plan.Root, "u");
        NestedLoopJoinNode joinNode = FindJoinNode(plan.Root);

        Assert.IsNotNull(usersScan.ExecutionFilter);
        Assert.IsNotNull(plan.ExecutionFilter);
        Assert.IsNull(joinNode.RightExecutionFilter);
    }

    [Test]
    public async Task Plan_UsesNestedLoopJoinWhenRightJoinKeyIsNotIndexed()
    {
        (DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket) = await BindJoinQuery(
            "SELECT u.email, p.title FROM app_users u JOIN posts p ON p.user_id = u.id",
            indexPostsUserId: false);

        QueryPlan plan = new JoinQueryPlanner().GetPlan(database, bound, ticket);

        Assert.IsInstanceOf<NestedLoopJoinNode>(plan.Root);
        Assert.That(plan.Root, Is.Not.TypeOf<IndexNestedLoopJoinNode>());
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

    private static async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)> BindJoinQuery(
        string sql,
        bool indexPostsUserId = false)
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger);

        string dbname = $"joinplanner_{Guid.NewGuid():n}";
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
    // R7 — Join-Order Heuristics
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R7_PointLookupSourceGoesFirst_WhenDeclaredSecond()
    {
        // posts declared first, app_users second — but users has a unique PK WHERE predicate.
        // After R7 reordering, users (score 0) must be on the outer (left) side.
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
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger);

        string dbname = $"r7exec_{Guid.NewGuid():n}";
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

    private static async Task<(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)> BindThreeTableJoinQuery(
        string sql,
        bool indexPostsUserId = false,
        bool indexCommentsPostId = false)
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        CommandExecutor executor = new(validator, catalogs, logger);

        string dbname = $"joinplanner3_{Guid.NewGuid():n}";
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
}
