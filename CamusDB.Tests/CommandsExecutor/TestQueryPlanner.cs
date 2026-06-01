
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
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

        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);

        ExecuteSQLTicket executeTicket = new(
            txnState: context!.Txn,
            database: QueryPlannerTestContext.DatabaseName,
            sql: sql,
            parameters: parameters);

        return QueryTicketAdapter.ToQueryTicket(query, executeTicket);
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

    private static TableDescriptor CreateYearPrimaryKeyTable()
    {
        TableDescriptor source = context!.Table;
        TableDescriptor table = new(source.Id + "-year-pk", source.Name, source.Schema, source.Store);
        table.Indexes.Add(
            CamusDBConfig.PrimaryKeyInternalName,
            new TableIndexSchema(CamusDBConfig.PrimaryKeyInternalName, ["year"], IndexType.Unique));
        return table;
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
    public void PlanUsesSecondaryIndexRangeScanForBetween()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year BETWEEN 2001 AND 2004");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2001, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].FromInclusive);
        Assert.AreEqual(2004, plan.Steps[0].ToBound!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].ToInclusive);
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
    public void PlanElidesSortWhenRangeScanMatchesOrderBy()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year >= 2020 ORDER BY year LIMIT 5");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.RangeScanFromIndex,
                QueryPlanStepType.Limit
            },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[]
            {
                typeof(IndexRangeScanNode),
                typeof(LimitNode)
            },
            PhysicalNodeTypes(plan));
        Assert.AreEqual(5, plan.ScanRowLimit);
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
    public void PlanAddsHavingFilterAfterAggregateForGroupedQuery()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role HAVING cnt > 0 ORDER BY role");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.HavingFilter,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.ReduceToProjections
            },
            StepTypes(plan));
    }

    [Test]
    public void PlanAddsHavingFilterAfterGlobalAggregate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT COUNT(*) AS x FROM robots HAVING x > 0");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Aggregate,
                QueryPlanStepType.HavingFilter,
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
            new[] { QueryPlanStepType.RangeScanFromIndex },
            StepTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void ScanSatisfiesOrderBy_TableScanDoesNotSatisfyOrderById()
    {
        QueryPlanStep tableScanStep = new(QueryPlanStepType.FullScanFromTableIndex);
        List<QueryOrderBy> orderBy = [new("id", OrderType.Ascending)];

        Assert.IsFalse(IndexScanSelector.ScanSatisfiesOrderBy(context!.Table, tableScanStep, orderBy));
    }

    [Test]
    public void PlanElidesSortWhenOrderByIdMatchesPrimaryKeyIndex()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT id FROM robots ORDER BY id LIMIT 1");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.RangeScanFromIndex, QueryPlanStepType.Limit, QueryPlanStepType.ReduceToProjections },
            StepTypes(plan));
        Assert.AreEqual(1, plan.ScanRowLimit);
    }

    [Test]
    public void PlanKeepsSortWhenOrderByIdWithoutMatchingIndex()
    {
        TableDescriptor table = CreateYearPrimaryKeyTable();
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT id FROM robots ORDER BY id LIMIT 1");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.Limit,
                QueryPlanStepType.ReduceToProjections,
            },
            StepTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanElidesSortWhenCompositeIndexMatchesOrderBy()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots ORDER BY year, enabled");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.RangeScanFromIndex },
            StepTypes(plan));
    }

    [Test]
    public void PlanKeepsSortWhenOnlyIndexPrefixMatchesOrderBy()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots ORDER BY year, name");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_idx", plan.Steps[0].Index!.Name);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.RangeScanFromIndex, QueryPlanStepType.SortBy },
            StepTypes(plan));
    }

    [Test]
    public void PlanKeepsSortWhenOrderByIsDescending()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots ORDER BY year DESC");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type);
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.FullScanFromTableIndex, QueryPlanStepType.SortBy },
            StepTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanPushesLimitForSimpleTableScan()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots LIMIT 3");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(3, plan.ScanRowLimit);
    }

    [Test]
    public void PlanPushesLimitIncludingOffsetForSimpleTableScan()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots LIMIT 2 OFFSET 5");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(7, plan.ScanRowLimit);
    }

    [Test]
    public void PlanDoesNotPushLimitWhenResidualFilterExists()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 OR year = 2001 LIMIT 2");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNotNull(plan.ExecutionFilter);
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanDoesNotPushLimitForAggregateQueries()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT COUNT(*) FROM robots LIMIT 1");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanPushesRequiredColumnsForPartialSelect()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT id FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNotNull(plan.ScanRequiredColumns);
        CollectionAssert.AreEquivalent(new[] { "id" }, plan.ScanRequiredColumns);
        CollectionAssert.AreEquivalent(new[] { "id" }, ScanRoot(plan).RequiredColumns);
    }

    [Test]
    public void PlanPushesRequiredColumnsIncludingFilterAndOrderBy()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT id FROM robots WHERE year = 2000 ORDER BY name");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEquivalent(
            new[] { "id", "year", "name" },
            plan.ScanRequiredColumns!);
    }

    [Test]
    public void PlanPushesAllColumnsForSelectStar()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNull(plan.ScanRequiredColumns);
        Assert.IsNull(ScanRoot(plan).RequiredColumns);
    }

    [Test]
    public void PlanPushesRequiredColumnsForHavingAggregateAlias()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role HAVING cnt > 0 ORDER BY role");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEquivalent(new[] { "role" }, plan.ScanRequiredColumns!);
    }

    [Test]
    public void PlanPushesRequiredColumnsForGlobalAggregateHavingAlias()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT COUNT(*) AS x FROM robots HAVING x > 0");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNotNull(plan.ScanRequiredColumns);
        Assert.AreEqual(0, plan.ScanRequiredColumns.Count);
    }

    [Test]
    public void PlanDistinctQueryUsesProjectDistinctSortLimitOrder()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT DISTINCT enabled FROM robots ORDER BY enabled LIMIT 2");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.ReduceToProjections,
                QueryPlanStepType.Distinct,
                QueryPlanStepType.SortBy,
                QueryPlanStepType.Limit,
            },
            StepTypes(plan));
        CollectionAssert.AreEqual(
            new[]
            {
                typeof(TableScanNode),
                typeof(ProjectNode),
                typeof(DistinctNode),
                typeof(SortNode),
                typeof(LimitNode),
            },
            PhysicalNodeTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanDistinctStarUsesDistinctWithoutProject()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT DISTINCT * FROM robots LIMIT 5");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.FullScanFromTableIndex,
                QueryPlanStepType.Distinct,
                QueryPlanStepType.Limit,
            },
            StepTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanDoesNotPushLimitForDistinctQueries()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT DISTINCT enabled FROM robots LIMIT 3");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanDistinctWithOrderByAlwaysIncludesSortAfterDistinct()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT DISTINCT id FROM robots WHERE id = @id ORDER BY id",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        CollectionAssert.AreEqual(
            new[]
            {
                QueryPlanStepType.QueryFromIndex,
                QueryPlanStepType.ReduceToProjections,
                QueryPlanStepType.Distinct,
                QueryPlanStepType.SortBy,
            },
            StepTypes(plan));
    }
}
