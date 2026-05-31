
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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
