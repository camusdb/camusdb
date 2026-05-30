
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Documents current single-table planner behavior via <see cref="QueryPlanStepType"/> assertions.
/// These tests call <see cref="QueryPlanner.GetPlan"/> only; they do not execute scans.
/// </summary>
public class TestQueryPlanner : BaseTest
{
    private readonly QueryPlanner queryPlanner = new();

    private async Task<(string dbname, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, CommandExecutor executor, List<string> objectIds)> SetupRobotsTableWithYearIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
                new("enabled", ColumnType.Bool)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        List<string> objectIds = new(25);

        for (int i = 0; i < 25; i++)
        {
            string objectId = ObjectIdGenerator.Generate().ToString();

            InsertTicket insertTicket = new(
                txnState: txnState,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, objectId) },
                        { "name", new(ColumnType.String, "some name " + i) },
                        { "year", new(ColumnType.Integer64, 2024 - i) },
                        { "enabled", new(ColumnType.Bool, (i + 1) % 2 == 0) },
                    }
                }
            );

            await executor.Insert(insertTicket);
            objectIds.Add(objectId);
        }

        AlterIndexTicket alterIndexTicket = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );

        await database.Transactions.CommitAsync(txnState);

        txnState = await database.Transactions.BeginAsync();

        await executor.AlterIndex(alterIndexTicket);
        await database.Transactions.CommitAsync(txnState);

        txnState = await database.Transactions.BeginAsync();

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "robots"));

        return (dbname, database, table, txnState, executor, objectIds);
    }

    private static QueryTicket CreateQueryTicketFromSelectSql(
        KvTransaction txnState,
        string databaseName,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);

        string tableName;
        string? indexName = null;

        if (ast.rightAst!.nodeType == NodeType.Identifier)
        {
            tableName = ast.rightAst.yytext!;
        }
        else if (ast.rightAst.nodeType == NodeType.IdentifierWithOpts)
        {
            tableName = ast.rightAst.leftAst!.yytext!;
            if (ast.rightAst.rightAst!.yytext!.Equals("FORCE_INDEX", StringComparison.InvariantCultureIgnoreCase))
            {
                string forcedIndex = ast.rightAst.extendedOne!.yytext!;
                indexName = forcedIndex == "pk" ? CamusDBConfig.PrimaryKeyInternalName : forcedIndex;
            }
        }
        else
        {
            throw new InvalidOperationException("Unexpected table reference in SELECT AST");
        }

        return new QueryTicket(
            txnState: txnState,
            databaseName: databaseName,
            tableName: tableName,
            index: indexName,
            projection: GetProjection(ast),
            filters: null,
            where: ast.extendedOne,
            orderBy: GetOrderBy(ast),
            limit: ast.extendedThree,
            offset: ast.extendedFour,
            parameters: parameters
        );
    }

    private static List<NodeAst>? GetProjection(NodeAst ast)
    {
        List<NodeAst> projectionList = new();
        GetProjectionFields(ast.leftAst!, projectionList);
        return projectionList;
    }

    private static void GetProjectionFields(NodeAst ast, List<NodeAst> projectionList)
    {
        if (ast.nodeType == NodeType.IdentifierList)
        {
            if (ast.leftAst is not null)
                GetProjectionFields(ast.leftAst, projectionList);

            if (ast.rightAst is not null)
                GetProjectionFields(ast.rightAst, projectionList);

            return;
        }

        projectionList.Add(ast);
    }

    private static List<QueryOrderBy>? GetOrderBy(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;

        List<QueryOrderBy> orderClauses = new();
        List<(string, OrderType)> sortList = new();
        GetSortList(ast.extendedTwo, sortList);

        foreach ((string projectionName, OrderType type) in sortList)
            orderClauses.Add(new QueryOrderBy(projectionName, type));

        return orderClauses;
    }

    private static void GetSortList(NodeAst orderByAst, List<(string, OrderType)> sortList)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
        {
            sortList.Add((orderByAst.yytext ?? "", OrderType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortAsc)
        {
            sortList.Add((orderByAst.leftAst!.yytext ?? "", OrderType.Ascending));
            return;
        }

        if (orderByAst.nodeType == NodeType.SortDesc)
        {
            sortList.Add((orderByAst.leftAst!.yytext ?? "", OrderType.Descending));
            return;
        }

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {
            if (orderByAst.leftAst is not null)
                GetSortList(orderByAst.leftAst, sortList);

            if (orderByAst.rightAst is not null)
                GetSortList(orderByAst.rightAst, sortList);

            return;
        }

        throw new InvalidOperationException("Invalid order by clause");
    }

    private static QueryPlanStepType[] StepTypes(QueryPlan plan) =>
        plan.Steps.Select(step => step.Type).ToArray();

    [Test]
    [NonParallelizable]
    public async Task PlanUsesFullTableScanWhenNoPredicate()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT * FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.FullScanFromTableIndex },
            StepTypes(plan));
    }

    [Test]
    [NonParallelizable]
    public async Task PlanUsesForcedIndexFullScan()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT id FROM robots@{FORCE_INDEX=year_idx}");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.FullScanFromIndex, QueryPlanStepType.ReduceToProjections },
            StepTypes(plan));
    }

    [Test]
    [NonParallelizable]
    public async Task PlanUsesPrimaryKeyLookupForIdEquality()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, List<string> objectIds) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            txn,
            database.Name,
            "SELECT * FROM robots WHERE id = @id",
            new() { { "@id", new ColumnValue(ColumnType.Id, objectIds[0]) } });

        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        Assert.AreEqual(objectIds[0], plan.Steps[0].ColumnValue!.StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task PlanUsesSecondaryIndexEqualityLookup()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT * FROM robots WHERE year = 2000");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2000, plan.Steps[0].ColumnValue!.LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task PlanUsesSecondaryIndexRangeScanForBoundedRange()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            txn,
            database.Name,
            "SELECT * FROM robots WHERE year >= 2001 AND year < 2005");

        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2001, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].FromInclusive);
        Assert.AreEqual(2005, plan.Steps[0].ToBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].ToInclusive);
    }

    [Test]
    [NonParallelizable]
    public async Task PlanUsesSecondaryIndexRangeScanForLowerBoundOnly()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT * FROM robots WHERE year > 2020");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2020, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].FromInclusive);
        Assert.IsNull(plan.Steps[0].ToBound);
    }

    [Test]
    [NonParallelizable]
    public async Task PlanFallsBackToFullScanWhenOrPreventsIndexUse()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            txn,
            database.Name,
            "SELECT * FROM robots WHERE year = 2000 OR year = 2001");

        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task PlanAddsSortAndLimitStepsInCurrentOrder()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            txn,
            database.Name,
            "SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5");

        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.RangeScanFromIndex,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.Limit
            },
            StepTypes(plan));
    }

    [Test]
    [NonParallelizable]
    public async Task PlanAddsAggregateAndProjectionStepsForCountStar()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT COUNT(*) FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }

    [Test]
    [NonParallelizable]
    public async Task PlanAddsAggregateStepForAliasedAggregate()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            txn,
            database.Name,
            "SELECT COUNT(*) AS total FROM robots WHERE year < 2005");

        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.RangeScanFromIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }

    [Test]
    [NonParallelizable]
    public async Task PlanAddsProjectionStepForPartialSelect()
    {
        (_, DatabaseDescriptor database, TableDescriptor table, KvTransaction txn, _, _) = await SetupRobotsTableWithYearIndex();

        QueryTicket ticket = CreateQueryTicketFromSelectSql(txn, database.Name, "SELECT id, name FROM robots WHERE year = 2000");
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.QueryFromIndex,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }
}
