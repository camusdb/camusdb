
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Builds a physical join plan tree for bound multi-source SELECT queries (QP4.3+).
/// </summary>
internal sealed class JoinQueryPlanner
{
    public QueryPlan GetPlan(DatabaseDescriptor database, BoundSelectQuery bound, QueryTicket ticket)
    {
        if (bound.Sources.Count < 2)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Join planning requires at least two bound sources");
        }

        JoinPredicatePushdown.Result pushdown = JoinPredicatePushdown.Analyze(bound, ticket.Where);

        QueryPlan plan = new(database, bound.Sources[0].Table, ticket)
        {
            BoundQuery = bound,
            PredicateAnalysis = PredicateAnalyzer.Analyze(ticket.Where, ticket.Parameters),
            ExecutionFilter = pushdown.PostJoinFilter,
            Root = BuildJoinTree(bound.Query.Source, bound, pushdown),
        };

        QueryPlanStepAdapter.PopulateLinearSteps(plan);

        return plan;
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
                BoundTableSource boundSource = FindBoundSource(tableSource, bound);
                pushdown.ScanFiltersByAlias.TryGetValue(boundSource.Alias, out NodeAst? scanFilter);

                return new TableScanNode(TableScanSource.PrimaryRows)
                {
                    BoundSource = boundSource,
                    ExecutionFilter = scanFilter,
                };
            }

            case JoinSource joinSource:
            {
                PhysicalPlanNode left = BuildJoinTree(joinSource.Left, bound, pushdown);
                BoundTableSource right = FindBoundSource((TableSource)joinSource.Right, bound);
                pushdown.ScanFiltersByAlias.TryGetValue(right.Alias, out NodeAst? rightFilter);

                if (JoinEquiJoinAnalyzer.TryMatch(right, joinSource.OnPredicate, bound, out JoinEquiJoinIndexMatch? indexMatch))
                {
                    return new IndexNestedLoopJoinNode(
                        left,
                        right,
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

    private static BoundTableSource FindBoundSource(TableSource tableSource, BoundSelectQuery bound)
    {
        string alias = tableSource.Alias ?? tableSource.TableName;

        foreach (BoundTableSource source in bound.Sources)
        {
            if (source.Source.TableName == tableSource.TableName && source.Alias == alias)
                return source;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Bound source not found for table '{tableSource.TableName}' alias '{alias}'");
    }
}
