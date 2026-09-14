/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Entry point and dispatcher for multi-source (join) queries. It owns the join operators, routes
/// each physical plan node to the one that executes it, and applies the post-scan pipeline. The
/// strategies themselves live in their own classes: <see cref="NestedLoopJoinOperator"/>,
/// <see cref="IndexNestedLoopJoinOperator"/>, <see cref="HashJoinOperator"/>,
/// <see cref="GraceHashJoinOperator"/>, and <see cref="MergeJoinOperator"/>, all reading their
/// rows through <see cref="JoinLeafScanner"/>.
///
/// <para><b>Cache bypass:</b> join queries deliberately ignore any <c>{cache=name}</c> hint on
/// the ticket. Caching a multi-table result requires the generation fence to snapshot the row
/// keyspace for <em>every</em> table in the plan, not just one. Until that multi-keyspace fence
/// is implemented (deferred), routing join results through
/// <see cref="QueryExecutor.QueryWithCache"/> would silently under-fence the non-primary tables
/// and risk serving a stale result as if it were fresh. The dispatch in
/// <c>CommandExecutor.ExecuteSQLQuery</c> routes <c>IsMultiSource</c> queries here before
/// any cache probe is performed. The scaffolding that the deferred fence will need is kept alive
/// in <see cref="JoinLeafScanner"/>.</para>
/// </summary>
internal sealed class QueryJoinExecutor : IJoinNodeExecutor
{
    private readonly JoinExecutionServices services;

    private readonly PlanCache? planCache;

    private readonly DerivedTableExecutor derivedTableExecutor;

    private readonly JoinLeafScanner leafScanner;

    private readonly NestedLoopJoinOperator nestedLoopJoin;

    private readonly IndexNestedLoopJoinOperator indexNestedLoopJoin;

    private readonly HashJoinOperator hashJoin;

    private readonly MergeJoinOperator mergeJoin;

    private readonly JoinFragmentProbeExecutor fragmentProbe;

    private readonly QueryAggregator queryAggregator = new();

    private readonly QueryLimiter queryLimiter = new();

    private readonly QueryProjector queryProjector = new();

    private readonly QueryDistincter queryDistincter = new();

    /// <summary>
    /// Publishes a new configuration snapshot to every join operator at once. Reference assignment
    /// is atomic and the record itself stays immutable; readers pin the snapshot at the top of an
    /// operation, so an in-flight operation keeps the snapshot it started with and a change takes
    /// effect at the next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => services.Options = next;

    public QueryJoinExecutor(QueryExecutor queryExecutor, CamusDBOptions options, StatisticsManager? stats = null, PlanCache? planCache = null,
        ILogger<ICamusDB>? logger = null, IQueryFragmentTransport? fragmentTransport = null, DistributedQueryMetrics? distributedMetrics = null)
    {
        services = new JoinExecutionServices(options)
        {
            Filterer = new QueryFilterer(new ExistsSubqueryExecutor()),
            Sorter = new QuerySorter(),
            Statistics = stats,
            Logger = logger,
            FragmentTransport = fragmentTransport,
            DistributedMetrics = distributedMetrics,
        };

        this.planCache = planCache;

        // Every operator is built once, here. None is allocated per query or per row, and none
        // holds a reference to this facade beyond the IJoinNodeExecutor seam it recurses through.
        derivedTableExecutor = new DerivedTableExecutor(queryExecutor, this);
        leafScanner = new JoinLeafScanner(services, derivedTableExecutor);
        nestedLoopJoin = new NestedLoopJoinOperator(services, this, leafScanner);
        indexNestedLoopJoin = new IndexNestedLoopJoinOperator(services, this, leafScanner);
        mergeJoin = new MergeJoinOperator(services, this, leafScanner);
        fragmentProbe = new JoinFragmentProbeExecutor(services);

        GraceHashJoinOperator graceHashJoin = new(services, this, leafScanner);
        BroadcastJoinPlanner broadcastPlanner = new(services);
        BroadcastHashProbeExecutor broadcastProbe = new(services);

        hashJoin = new HashJoinOperator(
            services, this, leafScanner, nestedLoopJoin, graceHashJoin, broadcastPlanner, broadcastProbe);
    }

    /// <summary>
    /// Executes one node of the join plan. This is the seam the operators recurse through; the
    /// dispatch itself is <see cref="ExecuteJoinTree"/>.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteNode(PhysicalPlanNode node, QueryPlan plan) =>
        ExecuteJoinTree(node, plan);

    /// <summary>
    /// Serves a peer coordinator's broadcast-join probe fragment. Forwards to
    /// <see cref="JoinFragmentProbeExecutor"/>, which is reached through this facade because the
    /// SELECT fragment controller already holds the join executor.
    /// </summary>
    internal IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentJoinProbe(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryFragmentJoinSpec join,
        ObjectIdValue? fromRowId,
        ObjectIdValue? untilRowId,
        int schemaVersion,
        IReadOnlySet<string>? requiredColumns,
        KvTransaction snapshotTx,
        bool wantStats = false,
        CancellationToken cancellationToken = default) =>
        fragmentProbe.ExecuteFragmentJoinProbe(
            database, table, join, fromRowId, untilRowId, schemaVersion, requiredColumns, snapshotTx, wantStats, cancellationToken);

    /// <summary>
    /// Entry point for multi-source (join) queries. Any <c>{cache=name}</c> hint on
    /// <paramref name="ticket"/> is intentionally not consulted here — see the class summary
    /// for the reason. Cache bypass is enforced at the call site in
    /// <c>CommandExecutor.ExecuteSQLQuery</c> by routing <c>IsMultiSource</c> queries here
    /// rather than through <c>QueryExecutor.Query</c>.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> ExecuteJoinQuery(
        DatabaseDescriptor database,
        BoundSelectQuery bound,
        QueryTicket ticket)
    {
        JoinQueryPlanner planner = new(services.Statistics, planCache, services.Options);
        QueryPlan plan = planner.GetPlan(database, bound, ticket);

        IAsyncEnumerable<QueryResultRow> cursor = ExecuteJoinTree(plan.Root, plan);

        if (plan.ExecutionFilter is not null)
            cursor = ApplyWhere(cursor, plan.ExecutionFilter, plan);

        cursor = QueryPostScanPipeline.Apply(
            plan.Database,
            ticket,
            cursor,
            services.Filterer,
            services.Sorter,
            queryAggregator,
            queryProjector,
            queryDistincter,
            queryLimiter
    );

        return WithDerivedMaterializationCleanup(cursor, plan);
    }

    /// <summary>
    /// Wraps <paramref name="cursor"/> so that derived-table <see cref="SpillableRowList"/>
    /// instances stored in the plan are disposed after the cursor is fully consumed,
    /// cancelled, or throws. The try/finally guarantees cleanup under all exit paths including
    /// early-cancel — derived-table spill files are never leaked regardless of how the caller
    /// terminates the enumeration.
    /// </summary>
    private static async IAsyncEnumerable<QueryResultRow> WithDerivedMaterializationCleanup(
        IAsyncEnumerable<QueryResultRow> cursor,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
                yield return row;
        }
        finally
        {
            await plan.DisposeMaterializationsAsync().ConfigureAwait(false);
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ExecuteJoinTree(
        PhysicalPlanNode node,
        QueryPlan plan)
    {
        switch (node)
        {
            case IndexNestedLoopJoinNode indexJoinNode:
            {
                await foreach (QueryResultRow row in indexNestedLoopJoin.ExecuteIndexNestedLoopJoin(indexJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case HashJoinNode hashJoinNode:
            {
                await foreach (QueryResultRow row in hashJoin.ExecuteHashJoin(hashJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case MergeJoinNode mergeJoinNode:
            {
                await foreach (QueryResultRow row in mergeJoin.ExecuteMergeJoin(mergeJoinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case NestedLoopJoinNode joinNode:
            {
                await foreach (QueryResultRow row in nestedLoopJoin.ExecuteNestedLoopJoin(joinNode, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case SortNode { Input: not null } sortNode when sortNode.OrderBy is { Count: > 0 }:
            {
                IAsyncEnumerable<QueryResultRow> sorted = services.Sorter.SortByKeys(ExecuteJoinTree(sortNode.Input!, plan), sortNode.OrderBy, QueryExecutionContext.For(plan.Database, plan.Ticket));
                
                await foreach (QueryResultRow row in sorted.ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case IndexRangeScanNode { BoundSource: not null } rangeLeaf:
            {
                await foreach (QueryResultRow row in leafScanner.ScanBoundTableByIndexRange(rangeLeaf, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case IndexInListScanNode { BoundSource: not null } inListLeaf:
            {
                await foreach (QueryResultRow row in leafScanner.ScanBoundTableByInList(inListLeaf, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case TableScanNode { BoundSource: not null, Source: TableScanSource.ForcedIndex, Index: not null } indexScanNode:
            {
                await foreach (QueryResultRow row in leafScanner.ScanBoundTableByIndex(
                    indexScanNode.BoundSource, indexScanNode.Index, indexScanNode.ExecutionFilter, plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case TableScanNode { BoundSource: not null } scanNode:
            {
                await foreach (QueryResultRow row in leafScanner.ScanBoundTable(
                    scanNode.BoundSource,
                    scanNode.ExecutionFilter,
                    plan,
                    scanNode.RequiredColumns).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            case DerivedTableScanNode { BoundSource: not null } derivedScanNode:
            {
                await foreach (QueryResultRow row in leafScanner.ScanDerivedTable(
                    derivedScanNode.BoundSource,
                    derivedScanNode.ExecutionFilter,
                    plan).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unsupported join plan node: {node.GetType().Name}");
        }
    }

    private async IAsyncEnumerable<QueryResultRow> ApplyWhere(
        IAsyncEnumerable<QueryResultRow> cursor,
        NodeAst where,
        QueryPlan plan)
    {
        await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
        {
            if (await services.Filterer.MeetWhereAsync(where, row.Row, plan.Ticket, plan.Database).ConfigureAwait(false))
                yield return row;
        }
    }
}
