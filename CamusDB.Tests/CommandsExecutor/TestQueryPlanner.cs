
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Documents current single-table planner behavior via <see cref="QueryPlanStepType"/> assertions.
/// These tests call <see cref="QueryPlanner.GetPlan"/> only; they do not execute scans.
/// </summary>
public class TestQueryPlanner
{
    private static QueryPlannerTestContext? context;

    private readonly QueryPlanner queryPlanner = new();

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        context = QueryPlannerTestContext.Create();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (context is not null)
        {
            await context.DisposeAsync().ConfigureAwait(false);
            context = null;
        }
    }

    private static QueryTicket CreateQueryTicketFromSelectSql(
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
            txnState: context!.Txn,
            databaseName: QueryPlannerTestContext.DatabaseName,
            tableName: tableName,
            index: indexName,
            projection: GetProjection(ast),
            filters: null,
            where: ast.extendedOne,
            orderBy: GetOrderBy(ast),
            limit: ast.extendedThree,
            offset: ast.extendedFour,
            parameters: parameters,
            groupBy: GetGroupBy(ast),
            analyzedWhere: PredicateAnalyzer.Analyze(ast.extendedOne, parameters)
        );
    }

    private static IReadOnlyList<NodeAst>? GetGroupBy(NodeAst ast)
    {
        if (ast.extendedFive is null)
            return null;

        return new SelectQueryCreator().CreateSelectQuery(ast).GroupBy;
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

    private static Type[] PhysicalNodeTypes(QueryPlan plan)
    {
        List<Type> types = new();
        CollectPhysicalNodeTypes(plan.Root, types);
        return types.ToArray();
    }

    private static void CollectPhysicalNodeTypes(PhysicalPlanNode node, List<Type> types)
    {
        if (node.Input is not null)
            CollectPhysicalNodeTypes(node.Input, types);

        types.Add(node.GetType());
    }

    private static PhysicalPlanNode ScanRoot(QueryPlan plan)
    {
        PhysicalPlanNode node = plan.Root;

        while (node.Input is not null)
            node = node.Input;

        return node;
    }

    [Test]
    public void PlanUsesFullTableScanWhenNoPredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.FullScanFromTableIndex },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[] { typeof(TableScanNode) },
            PhysicalNodeTypes(plan));
        Assert.AreEqual(TableScanSource.PrimaryRows, ((TableScanNode)plan.Root).Source);
    }

    [Test]
    public void PlanUsesForcedIndexFullScan()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT id FROM robots@{FORCE_INDEX=year_idx}");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.FullScanFromIndex, QueryPlanStepType.ReduceToProjections },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[] { typeof(TableScanNode), typeof(ProjectNode) },
            PhysicalNodeTypes(plan));
        Assert.AreEqual(TableScanSource.ForcedIndex, ((TableScanNode)ScanRoot(plan)).Source);
        Assert.AreEqual("year_idx", ((TableScanNode)ScanRoot(plan)).Index!.Name);
    }

    [Test]
    public void PlanUsesPrimaryKeyLookupForIdEquality()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE id = @id",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        Assert.AreEqual(QueryPlannerTestContext.SampleRowId, plan.Steps[0].ColumnValue!.StrValue);
        Assert.IsInstanceOf<IndexLookupNode>(ScanRoot(plan));
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, ((IndexLookupNode)ScanRoot(plan)).Index.Name);
    }

    [Test]
    public void PlanUsesSecondaryIndexEqualityLookup()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE year = 2000");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2000, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.AreEqual(2001, plan.Steps[0].ToBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].ToInclusive);
        Assert.IsInstanceOf<IndexRangeScanNode>(ScanRoot(plan));
    }

    [Test]
    public void PlanUsesSecondaryIndexRangeScanForBoundedRange()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year >= 2001 AND year < 2005");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2001, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].FromInclusive);
        Assert.AreEqual(2005, plan.Steps[0].ToBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].ToInclusive);
        Assert.IsInstanceOf<IndexRangeScanNode>(ScanRoot(plan));
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanUsesSecondaryIndexRangeScanForLowerBoundOnly()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE year > 2020");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2020, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].FromInclusive);
        Assert.IsNull(plan.Steps[0].ToBound);
    }

    [Test]
    public void PlanFallsBackToFullScanWhenOrPreventsIndexUse()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 OR year = 2001");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        Assert.IsInstanceOf<FilterNode>(plan.Root);
        Assert.IsInstanceOf<TableScanNode>(ScanRoot(plan));
    }

    [Test]
    public void PlanAddsSortAndLimitStepsInCurrentOrder()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.RangeScanFromIndex,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.Limit
            },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[]
            {
                typeof(IndexRangeScanNode),
                typeof(SortNode),
                typeof(LimitNode)
            },
            PhysicalNodeTypes(plan));
    }

    [Test]
    public void PlanAddsAggregateAndProjectionStepsForCountStar()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT COUNT(*) FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[]
            {
                typeof(TableScanNode),
                typeof(AggregateNode),
                typeof(ProjectNode)
            },
            PhysicalNodeTypes(plan));
    }

    [Test]
    public void PlanGroupsBeforeSortForGroupedAggregateQuery()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role ORDER BY role");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }

    [Test]
    public void PlanGroupsForGroupByOnlyQuery()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT role FROM robots GROUP BY role ORDER BY role");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }

    [Test]
    public void PlanAddsAggregateStepForAliasedAggregate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT COUNT(*) AS total FROM robots WHERE year < 2005");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

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
    public void PlanAddsProjectionStepForPartialSelect()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT id, name FROM robots WHERE year = 2000");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.RangeScanFromIndex,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[] { typeof(IndexRangeScanNode), typeof(ProjectNode) },
            PhysicalNodeTypes(plan));
    }

    [Test]
    public void PlanUsesCompositeIndexUniqueLookupForFullKeyEquality()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled = true");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2, plan.Steps[0].LookupKey!.Values.Length);
        Assert.AreEqual(2000, plan.Steps[0].LookupKey!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].LookupKey!.Values[1].BoolValue);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanPrefersPrimaryKeyLookupOverSecondaryRange()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE id = @id AND year > 2000",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        Assert.IsNotNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanUsesCompositeIndexPrefixRangeForLeadingEqualityAndTrailingRange()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled > false");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2, plan.Steps[0].FromBound!.Values.Length);
        Assert.AreEqual(2000, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].FromBound!.Values[1].BoolValue);
        Assert.IsFalse(plan.Steps[0].FromInclusive);
        Assert.AreEqual(2001, plan.Steps[0].ToBound!.Values[0].LongValue);
        Assert.AreEqual(1, plan.Steps[0].ToBound!.Values.Length);
        Assert.IsFalse(plan.Steps[0].ToInclusive);
        Assert.IsNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanUsesFullScanWhenNonUniqueStringEqualityCannotBeBoundedExactly()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'bob'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        Assert.IsNotNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanUsesOrderByIndexScanWhenNoPredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots ORDER BY year");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.IsNull(plan.Steps[0].FromBound);
        Assert.IsNull(plan.Steps[0].ToBound);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.RangeScanFromIndex, QueryPlanStepType.SortBy },
            StepTypes(plan));
    }
}
