
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical join plan tree for bound multi-source SELECT queries (QP4.3+).
///
/// R4 LIMITATION — distributed-ready properties are single-table only:
/// The R4 distributed-ready properties (<see cref="PhysicalPlanNode.OutputOrdering"/>,
/// <see cref="PhysicalPlanNode.CanDecomposeToLocalPlusMerge"/>, <see cref="PhysicalPlanNode.EstimatedCardinality"/>,
/// <see cref="PhysicalPlanNode.PartitionLocality"/>) are populated by
/// <see cref="QueryPlanner.GetPlan"/> only. This planner does not set <c>OutputOrdering</c>
/// on child scan nodes (join-side scans are never index-selected for ORDER BY here), so join
/// plans always have <c>null</c> <c>OutputOrdering</c> on their leaves.
/// <para>
/// As a result, any distributed-execution or sort-elision logic that reads
/// <c>OutputOrdering</c> from a join plan will see <c>null</c> and must treat the ordering as
/// undefined — which is the correct conservative assumption for the current single-partition
/// implementation.
/// </para>
/// <para>
/// This is intentional for the current single-partition deployment. The R7 join-order
/// heuristics pass (<see cref="JoinOrderOptimizer"/>) reorders sources but does not yet
/// populate these distributed properties on join-side scan nodes. Any distributed sharding
/// work must extend this planner to set them when join-side scans are index-selected.
/// </para>
/// </summary>
internal sealed class JoinQueryPlanner
{
    private readonly StatisticsManager? _stats;

    public JoinQueryPlanner(StatisticsManager? stats = null)
    {
        _stats = stats;
    }

    public QueryPlan GetPlan(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)
    {
        if (!bound.IsMultiSource)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Join planning requires a multi-source query");
        }

        if (bound.Sources.Count == 0 && bound.DerivedSources.Count == 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Join planning requires at least one bound source");
        }

        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        // Apply heuristic join-order rewriting before building the physical plan tree.
        QuerySource orderedSource = JoinOrderOptimizer.Reorder(bound.Query.Source, bound, pushdown);

        QueryPlan plan = new(database, ResolvePlanTable(bound), ticket)
        {
            BoundQuery = bound,
            PredicateAnalysis = PredicateAnalyzer.Analyze(ticket.Where, ticket.Parameters),
            ExecutionFilter = pushdown.PostJoinFilter,
            Root = BuildJoinTree(orderedSource, bound, pushdown),
        };

        foreach (BoundTableSource source in bound.Sources)
            plan.TableSchemaVersionByAlias[source.Alias] = source.Table.Schema.Version;

        QueryPlanStepAdapter.PopulateLinearSteps(plan);
        ProjectionPushdownPlanner.Apply(plan);

        // Annotate the join plan tree with cardinality and cost estimates.
        // Join plans use null for the primary table (multi-source); each scan node is
        // independently costed inside CostEstimator.AnnotatePlan.
        CostEstimator.AnnotatePlan(plan.Root, database, table: null, _stats);

        // Record the plan's query-shape ID and schema-version dependencies.
        // CollectSchemaDeps walks the full BoundSelectQuery tree recursively so that tables
        // referenced only inside derived-table subqueries are included — a schema change to
        // any of them must invalidate a future cached plan.
        plan.QueryShapeId = QueryShapeComputer.Compute(bound.Query);
        plan.SchemaDeps = CollectSchemaDeps(bound);

        return plan;
    }

    // Recursively collects (TableName, SchemaVersion) pairs from every table referenced in
    // the bound query, including tables that appear only inside derived-table subqueries.
    // Without the recursion, a schema change to such a table would not invalidate a cached plan.
    private static List<(string TableName, int SchemaVersion)> CollectSchemaDeps(BoundSelectQuery bound)
    {
        var deps = new List<(string, int)>();
        CollectSchemaDepsInto(bound, deps);
        return deps;
    }

    private static void CollectSchemaDepsInto(
        BoundSelectQuery bound,
        List<(string TableName, int SchemaVersion)> deps)
    {
        foreach (BoundTableSource s in bound.Sources)
            deps.Add((s.Table.Name, s.Table.Schema.Version));

        foreach (BoundDerivedTableSource d in bound.DerivedSources)
            CollectSchemaDepsInto(d.InnerBound, deps);
    }

    private static PhysicalPlanNode BuildJoinTree(
        QuerySource source,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown)
    {
        switch (source)
        {
            case TableSource tableSource:
            {
                BoundTableSource boundSource = BoundSourceCatalog.FindTableSource(bound, tableSource);
                pushdown.ScanFiltersByAlias.TryGetValue(boundSource.Alias, out NodeAst? scanFilter);

                return new TableScanNode(TableScanSource.PrimaryRows)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = scanFilter,
                };
            }

            case DerivedTableSource derivedSource:
            {
                BoundDerivedTableSource boundDerived = BoundSourceCatalog.FindDerivedSource(bound, derivedSource);
                pushdown.ScanFiltersByAlias.TryGetValue(boundDerived.Alias, out NodeAst? scanFilter);

                return new DerivedTableScanNode
                {
                    BoundSource = boundDerived,
                    ExecutionFilter = scanFilter,
                };
            }

            case JoinSource joinSource:
            {
                PhysicalPlanNode left = BuildJoinTree(joinSource.Left, bound, pushdown);
                BoundJoinRightSource right = BoundSourceCatalog.FindJoinRightSource(bound, joinSource.Right);
                pushdown.ScanFiltersByAlias.TryGetValue(right.Alias, out NodeAst? rightFilter);

                if (right.Table is not null
                    && JoinEquiJoinAnalyzer.TryMatch(right.Table, joinSource.OnPredicate, bound, out JoinEquiJoinIndexMatch? indexMatch))
                {
                    return new IndexNestedLoopJoinNode(
                        left,
                        right.Table,
                        joinSource.OnPredicate,
                        indexMatch.Index,
                        indexMatch.LeftLookupColumn,
                        indexMatch.RightIndexColumn)
                    {
                        RightExecutionFilter = rightFilter,
                    };
                }

                return new NestedLoopJoinNode(left, right, joinSource.OnPredicate)
                {
                    RightExecutionFilter = rightFilter,
                };
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unsupported join source: {source.GetType().Name}");
        }
    }

    private static TableDescriptor ResolvePlanTable(BoundSelectQuery bound)
    {
        if (bound.Sources.Count > 0)
            return bound.Sources[0].Table;

        foreach (BoundDerivedTableSource derived in bound.DerivedSources)
        {
            if (derived.InnerBound.Sources.Count > 0)
                return derived.InnerBound.Sources[0].Table;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            "Could not resolve plan table metadata for derived-only query");
    }
}
