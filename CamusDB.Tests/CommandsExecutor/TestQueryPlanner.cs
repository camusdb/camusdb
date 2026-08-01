
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

    private readonly QueryPlanner queryPlanner = new(CamusDBConfig.Ambient);

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

    /// <summary>
    /// Creates a ticket from <paramref name="sql"/> but overrides the index selection with
    /// <paramref name="indexName"/>, exercising <see cref="QueryPlanner.BuildForcedScanNode"/>
    /// and thereby <see cref="IndexScanSelector.TrySelectScanForForcedIndex"/> for the named index.
    /// Internal fields (ExistsSubqueries, SelectQuery, SemiJoinSpecs) are omitted — they are null
    /// for simple SELECT queries and unused by the planner in basic path tests.
    /// </summary>
    private static QueryTicket CreateQueryTicketWithForcedIndex(string sql, string indexName)
    {
        QueryTicket base_ = CreateQueryTicketFromSelectSql(sql);
        return new QueryTicket(
            txnState:      base_.TxnState,
            databaseName:  base_.DatabaseName,
            tableName:     base_.TableName,
            index:         indexName,
            projection:    base_.Projection,
            filters:       base_.Filters,
            where:         base_.Where,
            orderBy:       base_.OrderBy,
            limit:         base_.Limit,
            offset:        base_.Offset,
            parameters:    base_.Parameters,
            groupBy:       base_.GroupBy,
            having:        base_.Having,
            rowNameResolver: base_.RowNameResolver,
            analyzedWhere: base_.AnalyzedWhere,
            isDistinct:    base_.IsDistinct);
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
            CamusDBConstants.PrimaryKeyInternalName,
            new TableIndexSchema(CamusDBConstants.PrimaryKeyInternalName, ["year"], IndexType.Unique));
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
        Assert.AreEqual(CamusDBConstants.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        Assert.AreEqual(QueryPlannerTestContext.SampleRowId, plan.Steps[0].ColumnValue!.StrValue);
        Assert.IsInstanceOf<IndexLookupNode>(ScanRoot(plan));
        Assert.AreEqual(CamusDBConstants.PrimaryKeyInternalName, ((IndexLookupNode)ScanRoot(plan)).Index.Name);
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

    /// <summary>
    /// A boolean column written as a bare predicate must contribute the same equality bound as the
    /// explicit <c>enabled = true</c> form, otherwise it carries no bound at all and any index it
    /// participates in is left unusable.
    /// </summary>
    [Test]
    public void PlanUsesCompositeIndexWhenBooleanColumnIsWrittenAsBarePredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2, plan.Steps[0].LookupKey!.Values.Length);
        Assert.AreEqual(2000, plan.Steps[0].LookupKey!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].LookupKey!.Values[1].BoolValue);
        Assert.IsNull(plan.ExecutionFilter);
    }

    /// <summary>
    /// The negated bare form must seek the complementary key rather than falling back to a scan.
    /// </summary>
    [Test]
    public void PlanUsesCompositeIndexWhenBooleanColumnIsNegatedBarePredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND NOT enabled");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.AreEqual(2, plan.Steps[0].LookupKey!.Values.Length);
        Assert.AreEqual(2000, plan.Steps[0].LookupKey!.Values[0].LongValue);
        Assert.IsFalse(plan.Steps[0].LookupKey!.Values[1].BoolValue);
        Assert.IsNull(plan.ExecutionFilter);
    }

    /// <summary>
    /// <c>IS TRUE</c> selects the same rows as <c>= true</c> in a WHERE clause, so it must bind the
    /// index the same way.
    /// </summary>
    [Test]
    public void PlanUsesCompositeIndexForIsTruePredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled IS TRUE");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.IsTrue(plan.Steps[0].LookupKey!.Values[1].BoolValue);
    }

    [Test]
    public void PlanUsesCompositeIndexForIsFalsePredicate()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled IS FALSE");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        Assert.IsFalse(plan.Steps[0].LookupKey!.Values[1].BoolValue);
    }

    /// <summary>
    /// <c>IS NOT TRUE</c> matches FALSE <em>and NULL</em>. On a nullable column no single equality
    /// bound can express that, so it must stay a residual filter — promoting it to <c>= false</c>
    /// would silently drop the NULL rows. The fixture's <c>enabled</c> column is nullable.
    /// </summary>
    [Test]
    public void PlanDoesNotPromoteIsNotTrueOnNullableColumn()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled IS NOT TRUE");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreNotEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type,
            "IS NOT TRUE on a nullable column must not become a full-key equality lookup");
        Assert.IsNotNull(plan.ExecutionFilter,
            "The NULL-accepting predicate must survive as a residual filter");
    }

    [Test]
    public void PlanDoesNotPromoteIsNotFalseOnNullableColumn()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled IS NOT FALSE");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreNotEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.IsNotNull(plan.ExecutionFilter);
    }

    /// <summary>
    /// A bare non-boolean column keeps numeric truthiness semantics (non-zero is true), which an
    /// equality against true would not preserve, so it must stay a residual filter and must not be
    /// promoted into an index bound.
    /// </summary>
    [Test]
    public void PlanDoesNotPromoteBareNonBooleanColumnToAnIndexBound()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE year");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsInstanceOf<TableScanNode>(ScanRoot(plan));
        Assert.IsNotNull(plan.ExecutionFilter);
    }

    [Test]
    public void PlanPrefersPrimaryKeyLookupOverSecondaryRange()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE id = @id AND year > 2000",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type);
        Assert.AreEqual(CamusDBConstants.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
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
    public void PlanUsesIndexRangeScanForNonUniqueStringEquality()
    {
        // Previously fell back to a full table scan because SupportsExactEqualityPrefixUpperBound
        // returned false for String columns. The planner uses an inclusive [v, v] range scan —
        // the same strategy the IN-list path uses — so the index is exploited.
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'bob'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("name_idx", plan.Steps[0].Index!.Name);
        Assert.IsNull(plan.ExecutionFilter, "Predicate absorbed by inclusive [v,v] range scan");
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
        Assert.AreEqual(CamusDBConstants.PrimaryKeyInternalName, plan.Steps[0].Index!.Name);
        // Limit sits above the projection: it restricts output rows, and stacking it last
        // keeps it above aggregates when the projection contains one.
        CollectionAssert.AreEqual(
            new[] { QueryPlanStepType.RangeScanFromIndex, QueryPlanStepType.ReduceToProjections, QueryPlanStepType.Limit },
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
                QueryPlanStepType.ReduceToProjections,
                QueryPlanStepType.Limit,
            },
            StepTypes(plan));
        Assert.IsNull(plan.ScanRowLimit);
    }

    [Test]
    public void PlanKeepsSortForOrderByOnNullableUniqueCompositeIndex()
    {
        // year_enabled_idx is a UNIQUE index whose columns (year, enabled) are nullable. Rows with a
        // NULL in either column carry no unique index entry (NULLs are distinct), so an unbounded
        // ordered scan over that index would silently drop them. The planner must therefore keep the
        // explicit sort over a full table scan instead of eliding it.
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots ORDER BY year, enabled");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        // The nullable unique composite index is no longer used to fully elide the sort. The planner
        // falls to year_idx (a multi index, which contains NULL rows) for the leading-column ordering
        // and keeps an explicit SortBy — so no NULL rows can be dropped.
        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreNotEqual("year_enabled_idx", plan.Steps[0].Index!.Name);
        CollectionAssert.Contains(StepTypes(plan), QueryPlanStepType.SortBy);
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
        Assert.AreEqual(0, plan.ScanRequiredColumns!.Count);
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
    public void PlanDistinctWithOrderByOverIndexedColumn_ElidesSortNode()
    {
        // Streaming distinct on the PK index covers `id`; ORDER BY id ASC matches the
        // streaming ordering → SortNode is elided. No SortBy in the plan.
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
            },
            StepTypes(plan));

        // The DistinctNode must be streaming.
        DistinctNode distinctNode = plan.StepNodes.OfType<DistinctNode>().Single();
        Assert.IsTrue(distinctNode.IsStreaming, "distinct over indexed id column must be streaming");
    }

    [Test]
    public void PlanDistinctWithOrderByOverNonStreamableColumn_IncludesSortAfterDistinct()
    {
        // `enabled` has no single-column index → hash distinct → SortNode is still needed.
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

        DistinctNode distinctNode = plan.StepNodes.OfType<DistinctNode>().Single();
        Assert.IsFalse(distinctNode.IsStreaming, "distinct over non-indexed enabled must be hash");
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Locate-column plumbing tests
    // ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A QueryTicket carrying an explicit LocateColumns set must produce a plan whose
    /// ScanRequiredColumns equals that set — bypassing RequiredColumnAnalyzer.ComputeSingleTable.
    /// This locks the optimisation plumbing: a future change that stops threading
    /// LocateColumns would break this test even before the correctness tests fire.
    /// </summary>
    [Test]
    public void LocateColumnsOverridesScanRequiredColumns()
    {
        IReadOnlySet<string> locate = new HashSet<string>(StringComparer.Ordinal) { "year" };

        QueryTicket ticket = new(
            txnState: context!.Txn,
            databaseName: QueryPlannerTestContext.DatabaseName,
            tableName: context.Table.Schema.Name!,
            index: null,
            projection: null,          // projection:null would normally → ScanRequiredColumns null
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null,
            locateColumns: locate);

        QueryPlan plan = queryPlanner.GetPlan(context.Database, context.Table, ticket);

        Assert.IsNotNull(plan.ScanRequiredColumns,
            "LocateColumns should override the null-projection default (full decode)");
        CollectionAssert.AreEquivalent(new[] { "year" }, plan.ScanRequiredColumns);
        CollectionAssert.AreEquivalent(new[] { "year" }, ScanRoot(plan).RequiredColumns);
    }

    // Helper: build a bare Identifier node.
    private static NodeAst Id(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    // Helper: build an Integer literal node.
    private static NodeAst IntLit(string v) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, v);

    // Helper: build a binary expression node (e.g. ExprEquals, ExprAdd).
    private static NodeAst Binary(NodeType op, NodeAst left, NodeAst right) =>
        new(op, left, right, null, null, null, null, null, null);

    /// <summary>
    /// ComputeForLocate with a simple equality WHERE returns the referenced column only.
    /// </summary>
    [Test]
    public void ComputeForLocate_SimpleWhere_ReturnsWhereColumns()
    {
        // WHERE year = 2000
        NodeAst where = Binary(NodeType.ExprEquals, Id("year"), IntLit("2000"));
        IReadOnlySet<string>? cols = RequiredColumnAnalyzer.ComputeForLocate(where, filters: null, exprValues: null);

        Assert.IsNotNull(cols);
        CollectionAssert.AreEquivalent(new[] { "year" }, cols);
    }

    /// <summary>
    /// ComputeForLocate with an ExprValues expression includes the RHS column references.
    /// For SET score = score + year, both "score" and "year" must be fetched in the locate phase.
    /// </summary>
    [Test]
    public void ComputeForLocate_ExprValues_IncludesRhsColumns()
    {
        // WHERE enabled = 1 (literal)
        NodeAst where     = Binary(NodeType.ExprEquals, Id("enabled"), IntLit("1"));
        // SET score = score + year  (RHS references score and year)
        NodeAst scoreExpr = Binary(NodeType.ExprAdd, Id("score"), Id("year"));
        Dictionary<string, NodeAst> exprValues = new(StringComparer.Ordinal) { { "score", scoreExpr } };

        IReadOnlySet<string>? cols = RequiredColumnAnalyzer.ComputeForLocate(where, filters: null, exprValues);

        Assert.IsNotNull(cols);
        // "enabled" from WHERE, "score" and "year" from the SET expression RHS.
        CollectionAssert.IsSubsetOf(new[] { "enabled", "score", "year" }, cols);
    }

    /// <summary>
    /// ComputeForLocate with no WHERE and no filters returns an empty (non-null) set.
    /// The scan needs only the row id — ScanRequiredColumns == {} means decode 0 columns.
    /// </summary>
    [Test]
    public void ComputeForLocate_NoWhereNoFilters_ReturnsEmptySet()
    {
        IReadOnlySet<string>? cols = RequiredColumnAnalyzer.ComputeForLocate(
            where: null, filters: null, exprValues: null);

        Assert.IsNotNull(cols);
        Assert.AreEqual(0, cols!.Count);
    }

    /// <summary>
    /// ComputeForLocate returns null (full-decode fallback) when the WHERE contains
    /// a subquery node, because CollectColumnReferences does not descend into subquery
    /// bodies and outer correlation columns would be silently omitted.
    /// </summary>
    [Test]
    public void ComputeForLocate_SubqueryInWhere_ReturnsNullFallback()
    {
        // Build a minimal ExprExistsSubquery node inline — the parser would produce
        // one for "WHERE EXISTS (SELECT 1 FROM …)" but we exercise the guard directly.
        NodeAst existsNode = new NodeAst(
            NodeType.ExprExistsSubquery,
            leftAst: new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, "id"),
            rightAst: null, extendedOne: null, extendedTwo: null,
            extendedThree: null, extendedFour: null, extendedFive: null, yytext: null);

        IReadOnlySet<string>? cols = RequiredColumnAnalyzer.ComputeForLocate(
            where: existsNode, filters: null, exprValues: null);

        Assert.IsNull(cols, "EXISTS subquery WHERE should trigger full-decode fallback");
    }

    // ── Non-unique String/Id equality index scans ─────────────────────────────

    [Test]
    public void StringEqualityOnNonUniqueIndexUsesRangeScan()
    {
        // name_idx is a Multi (non-unique) index on the String column `name`.
        // SupportsExactEqualityPrefixUpperBound returns true for String, so the planner
        // emits RangeScanFromIndex with inclusive [v, v] bounds rather than FullScanFromTableIndex.
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'alice'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type,
            "String equality on a non-unique index must use index-range-scan");
        Assert.AreEqual("name_idx", plan.Steps[0].Index!.Name);
        Assert.IsInstanceOf<IndexRangeScanNode>(ScanRoot(plan));
    }

    [Test]
    public void StringEqualityRangeScanHasInclusiveEqualityBounds()
    {
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'alice'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        QueryPlanStep step = plan.Steps[0];

        // Both from and to must be the same value with inclusive bounds (the [v, v] pattern).
        Assert.IsNotNull(step.FromBound, "FromBound must be set");
        Assert.IsNotNull(step.ToBound, "ToBound must be set");
        Assert.IsTrue(step.FromInclusive, "From must be inclusive");
        Assert.IsTrue(step.ToInclusive, "To must be inclusive");
        Assert.AreEqual("alice", step.FromBound!.Values[0].StrValue);
        Assert.AreEqual("alice", step.ToBound!.Values[0].StrValue);
    }

    [Test]
    public void StringEqualityPredicateAbsorbedByRangeScan_NoExecutionFilter()
    {
        // The [v, v] inclusive range satisfies IsPointComparisonAbsorbed for String/Id,
        // so the equality predicate must be absorbed and the execution filter must be null.
        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'alice'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsNull(plan.ExecutionFilter,
            "Absorbed String equality predicate must not appear in the execution filter");
    }

    [Test]
    public void IdEqualityOnUniqueIndexStillUsesPointLookup()
    {
        // Unique Id lookups must remain QueryFromIndex; the range-scan path is for non-unique only.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE id = @id",
            new() { { "@id", new ColumnValue(ColumnType.Id, QueryPlannerTestContext.SampleRowId) } });
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.QueryFromIndex, plan.Steps[0].Type,
            "Unique Id index must still use a point lookup, not a range scan");
        Assert.IsInstanceOf<IndexLookupNode>(ScanRoot(plan));
    }

    // ── Case 2: String equality prefix + half-open trailing range ─────────────

    [Test]
    public void StringPrefixPlusOpenUpperRange_BoundsToPrefix()
    {
        // name_year_idx covers (name String, year Int64). WHERE name='alice' AND year > 2000
        // has a half-open upper side (no upper year bound). Before the fix: toBound was null
        // → the scan ran to the end of the entire index. After fix: toBound is capped at
        // [alice] inclusive (ScanIndex appends U+FFFF sentinel), so the scan stays within alice.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE name = 'alice' AND year > 2000");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);

        QueryPlanStep step = plan.Steps[0];
        Assert.IsNotNull(step.ToBound,
            "Half-open upper range with String prefix must cap toBound at prefix sentinel");
        Assert.IsTrue(step.ToInclusive, "Prefix sentinel bound must be inclusive");
        Assert.AreEqual("alice", step.ToBound!.Values[0].StrValue);
    }

    [Test]
    public void StringPrefixPlusOpenLowerRange_BoundsToPrefix()
    {
        // WHERE name='alice' AND year < 2020 → half-open lower side (no lower year bound).
        // Before fix: fromBound was null → scan from start of index. After fix: fromBound
        // is floored at [alice] inclusive, so the scan starts at the alice prefix.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE name = 'alice' AND year < 2020");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);

        QueryPlanStep step = plan.Steps[0];
        Assert.IsNotNull(step.FromBound,
            "Half-open lower range with String prefix must floor fromBound at prefix sentinel");
        Assert.IsTrue(step.FromInclusive, "Prefix sentinel bound must be inclusive");
        Assert.AreEqual("alice", step.FromBound!.Values[0].StrValue);
    }

    [Test]
    public void StringPrefixPlusBothSidedRange_BothBoundsPresent()
    {
        // WHERE name='alice' AND year > 2000 AND year < 2020 → both sides bounded.
        // The prefix-sentinel fix must NOT override an already-present range bound.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE name = 'alice' AND year > 2000 AND year < 2020");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);

        QueryPlanStep step = plan.Steps[0];
        Assert.IsNotNull(step.FromBound);
        Assert.IsNotNull(step.ToBound);
        Assert.AreEqual("alice", step.FromBound!.Values[0].StrValue);
        Assert.AreEqual(2000L, step.FromBound!.Values[1].LongValue);
        Assert.AreEqual("alice", step.ToBound!.Values[0].StrValue);
        Assert.AreEqual(2020L, step.ToBound!.Values[1].LongValue);
    }

    // ── Path 3 — String equality prefix on composite index (no range col) ──────

    [Test]
    public void StringEqualityPrefix_CompositeIndex_EmitsInclusiveRangeScanStep()
    {
        // name_year_idx is (name String, year Int64). A WHERE with only name='alice'
        // (no year predicate) hits Path 3: partial equality prefix, no range column.
        // The single-col name_idx wins GetPlan on the normal table (score 5010 > 5001),
        // so we build a variant TableDescriptor with ONLY the composite index, forcing
        // GetPlan to exercise Path 3 rather than Path 1.
        TableDescriptor compositeOnly = new(
            context!.Table.Id,
            context.Table.Name,
            context.Table.Schema,
            context.Table.Store);
        compositeOnly.Indexes.Add(
            CamusDBConstants.PrimaryKeyInternalName,
            context.Table.Indexes[CamusDBConstants.PrimaryKeyInternalName]);
        compositeOnly.Indexes.Add(
            "name_year_idx",
            context.Table.Indexes["name_year_idx"]);

        QueryTicket ticket = CreateQueryTicketFromSelectSql("SELECT * FROM robots WHERE name = 'alice'");
        QueryPlan plan = queryPlanner.GetPlan(context.Database, compositeOnly, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type,
            "Path 3: partial String equality prefix must emit a RangeScanFromIndex");
        Assert.AreEqual("name_year_idx", plan.Steps[0].Index!.Name);

        QueryPlanStep step = plan.Steps[0];
        Assert.IsNotNull(step.FromBound, "FromBound must be set for [alice, alice] range");
        Assert.IsNotNull(step.ToBound,   "ToBound must be set for [alice, alice] range");
        Assert.IsTrue(step.FromInclusive, "From must be inclusive");
        Assert.IsTrue(step.ToInclusive,   "To must be inclusive");
        Assert.AreEqual("alice", step.FromBound!.Values[0].StrValue,
            "FromBound prefix column must equal the equality value");
        Assert.AreEqual("alice", step.ToBound!.Values[0].StrValue,
            "ToBound prefix column must equal the equality value (inclusive sentinel)");
        Assert.AreEqual(1, step.FromBound!.Values.Length,
            "Path 3 bounds have only the prefix column (no trailing range value)");
    }

    // ---- IndexOnly (covering-index detection) tests --------------------------

    [Test]
    public void IndexOnly_IsTrue_WhenProjectionCoversIndexKey()
    {
        // SELECT year FROM robots WHERE year = 2024
        // year_idx covers column "year"; required = {year} ⊆ {year} → IndexOnly.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT year FROM robots WHERE year = 2024");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsTrue(plan.IndexOnly, "plan must be index-only when projection is covered by the index");
        Assert.Contains("year", (System.Collections.IList)plan.IndexOnlyColumns,
            "IndexOnlyColumns must include the projected column");
    }

    [Test]
    public void IndexOnly_IsFalse_WhenPkColumnRequired()
    {
        // SELECT id FROM robots WHERE year = 2024
        // The id column is a user-supplied logical primary key stored in the row; it is NOT the
        // internal KV row id used as the index-entry suffix. year_idx does not carry the id
        // column value, so the primary row must be fetched — never a covering scan.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT id FROM robots WHERE year = 2024");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsFalse(plan.IndexOnly, "plan must NOT be index-only: the id column is not in the index key");
        Assert.IsEmpty(plan.IndexOnlyColumns);
    }

    [Test]
    public void IndexOnly_IsTrue_WhenProjectionCoversCompositeIndex()
    {
        // SELECT name, year FROM robots WHERE name = 'alice' AND year = 2024
        // Both predicate columns match name_year_idx; the composite index covers the required
        // {name, year} set.  Using both predicate columns ensures name_year_idx scores higher
        // than the single-column name_idx (2×10 > 1 prefix score).
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT name, year FROM robots WHERE name = 'alice' AND year = 2024");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsTrue(plan.IndexOnly,
            "plan must be index-only when all projected columns are in the composite index");
        Assert.That(plan.IndexOnlyColumns, Does.Contain("name"));
        Assert.That(plan.IndexOnlyColumns, Does.Contain("year"));
    }

    [Test]
    public void IndexOnly_IsFalse_WhenNonIndexedColumnIsRequired()
    {
        // SELECT name, enabled FROM robots WHERE year = 2024
        // year_idx only covers "year"; "enabled" is not in the index → not covered.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT name, enabled FROM robots WHERE year = 2024");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsFalse(plan.IndexOnly,
            "plan must NOT be index-only when a required column is absent from the index");
        Assert.IsEmpty(plan.IndexOnlyColumns);
    }

    [Test]
    public void IndexOnly_IsFalse_ForSelectStar()
    {
        // SELECT * requires all columns, so ScanRequiredColumns is null → not covered.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2024");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsFalse(plan.IndexOnly, "SELECT * must never be index-only");
        Assert.IsEmpty(plan.IndexOnlyColumns);
    }

    [Test]
    public void IndexOnly_IsFalse_ForFullTableScan()
    {
        // No index on "enabled" forces a full scan; full scans are never index-only.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT year FROM robots WHERE enabled = true");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        // The plan may use a full scan or an index scan on year; if no index chosen, not covered.
        if (plan.Steps is not [{ Index: not null }, ..])
            Assert.IsFalse(plan.IndexOnly, "full table scan must never be index-only");
        // If an index was coincidentally picked, IndexOnly may be true; skip that case.
    }

    [Test]
    public void IndexOnly_IsFalse_WhenResidualWhereColumnNotInIndex()
    {
        // SELECT year FROM robots WHERE year = 2024 AND enabled = true AND name = 'alice'
        //
        // year_enabled_idx (unique, [year, enabled]) wins the access-path selection because it
        // matches two equality columns (unique-lookup score >> range-scan score). That index
        // covers {year, enabled, id}. But "name" is also referenced in the WHERE clause and
        // becomes a residual execution filter — it is NOT in year_enabled_idx. Therefore the
        // required set is {year, enabled, name}, which is not a subset of the index, and the
        // plan must NOT be marked index-only.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT year FROM robots WHERE year = 2024 AND enabled = true AND name = 'alice'");
        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.IsFalse(plan.IndexOnly,
            "residual WHERE column absent from the chosen index must prevent index-only, even when the projected column is covered");
    }

    [Test]
    public void PlanAddsEqualityPrefixLowerBoundForUpperOnlyTrailingRange()
    {
        // "year = 2000 AND enabled < true": the range column contributes only an upper bound,
        // so without a synthesized lower bound the scan would start at the beginning of the
        // index and read (then residual-filter away) every row below the equality prefix —
        // and under Serializable the range lock would cover that whole low keyspace.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT * FROM robots WHERE year = 2000 AND enabled < true");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.RangeScanFromIndex, plan.Steps[0].Type);
        Assert.AreEqual("year_enabled_idx", plan.Steps[0].Index!.Name);

        Assert.IsNotNull(plan.Steps[0].FromBound,
            "an upper-only trailing range with an equality prefix must still get a lower bound at the prefix");
        Assert.AreEqual(1, plan.Steps[0].FromBound!.Values.Length);
        Assert.AreEqual(2000, plan.Steps[0].FromBound!.Values[0].LongValue);
        Assert.IsTrue(plan.Steps[0].FromInclusive, "the equality-prefix lower bound is inclusive");
    }

    [Test]
    public void PlanForcedUniqueIndexOverNullableColumnsFallsBackToPrimaryScan()
    {
        // year_enabled_idx is UNIQUE over nullable columns: unique inserts skip NULL-keyed rows,
        // so an unbounded full scan over it silently drops those rows. The automatic ORDER BY
        // path refuses it; the user-forced path must apply the same correctness guard and fall
        // back to a primary-row scan.
        QueryTicket ticket = CreateQueryTicketFromSelectSql(
            "SELECT id FROM robots@{FORCE_INDEX=year_enabled_idx}");

        QueryPlan plan = queryPlanner.GetPlan(context!.Database, context.Table, ticket);

        Assert.AreEqual(QueryPlanStepType.FullScanFromTableIndex, plan.Steps[0].Type,
            "forced unique index over nullable columns must not row-drop; expect primary full scan");
        Assert.AreEqual(TableScanSource.PrimaryRows, ((TableScanNode)ScanRoot(plan)).Source);
    }

    [Test]
    public void HalfOpenRangeWithoutColumnStatsPrefersFullScan()
    {
        // Without column stats a half-open range falls back to OneBoundSelectivity, which is
        // deliberately equal to BreakevenFraction (with ceiling rounding) so the flip to a full
        // scan happens on EVERY table size — including odd row counts, where truncating
        // arithmetic used to land one row short of the breakeven and keep the ~2×-cost index.
        TableIndexSchema idx = context!.Table.Indexes["year_idx"];

        IndexRangeScanNode halfOpen = new(
            idx,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2000L) }),
            fromInclusive: false,
            toBound: null,
            toInclusive: true);

        Assert.IsTrue(CostEstimator.ShouldPreferFullScan(halfOpen, tableRowCount: 5),
            "odd row count: half-open range without stats must flip to full scan");
        Assert.IsTrue(CostEstimator.ShouldPreferFullScan(halfOpen, tableRowCount: 10),
            "even row count: half-open range without stats must flip to full scan");

        IndexRangeScanNode bothBounds = new(
            idx,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2000L) }),
            fromInclusive: true,
            toBound: new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2005L) }),
            toInclusive: true);

        Assert.IsFalse(CostEstimator.ShouldPreferFullScan(bothBounds, tableRowCount: 10),
            "a two-sided range (10% fallback) must keep the index");
    }
}
