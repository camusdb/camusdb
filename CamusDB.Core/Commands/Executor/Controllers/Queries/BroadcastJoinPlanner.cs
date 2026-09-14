/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Decides whether a hash join's probe should run as broadcast fragments, and prepares the
/// shipping plan when it should.
///
/// <para>Declining is never an error: every gate below returns <see langword="null"/>, and the
/// caller then runs the ordinary local probe. The gates are ordered cheapest first.</para>
/// </summary>
internal sealed class BroadcastJoinPlanner
{
    private readonly JoinExecutionServices services;

    public BroadcastJoinPlanner(JoinExecutionServices services)
    {
        this.services = services;
    }

    /// <summary>
    /// Records why the broadcast probe declined, on the plan and in the log (Debug level).
    /// Declining stays a silent fallback for execution; this makes the decision observable,
    /// because a declined broadcast produces correct results and is otherwise
    /// indistinguishable from one that was never eligible.
    /// </summary>
    private BroadcastJoinPlan? Decline(QueryPlan plan, string reason)
    {
        plan.BroadcastJoinSkipReason = reason;

        ILogger<ICamusDB>? logger = services.Logger;
        if (logger is not null && logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Broadcast join declined: {Reason}", reason);

        return null;
    }

    /// <summary>
    /// Decides whether this hash join's probe runs as broadcast fragments, and prepares the
    /// shipping plan when it does. Every condition falls back to the standard local probe —
    /// declining is never an error. Gates: distributed execution on, a transport, a positive
    /// broadcast cap the build's <b>actual</b> row count fits under, a plain primary-row base
    /// table on the probe side (the left leaf when the right side was built; the right table
    /// source when the left subtree was built), a transaction whose reads need no session
    /// folding and takes no exclusive predicate locks, no dependency collector, shippable ON
    /// and probe-filter predicates, and a multi-span key-range placement with at least one
    /// non-local leader (an all-local placement gains nothing from shipping the build).
    /// </summary>
    internal BroadcastJoinPlan? TryPrepareBroadcastJoin(
        HashJoinNode joinNode,
        QueryPlan plan,
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> hashTable,
        HashJoinBuildSide buildSide)
    {
        // Not a "decline": with no transport, or the feature off, broadcast was never
        // considered, so no skip reason is recorded (see QueryPlan.BroadcastJoinSkipReason).
        if (services.FragmentTransport is null || services.Logger is null)
            return null;

        CamusDBOptions liveOptions = plan.Database.Options;

        if (!liveOptions.DistributedQueryExecutionEnabled || liveOptions.BroadcastJoinMaxBuildRows <= 0)
            return null;

        // The probe side must be a plain primary-row base table so its keyspace can be
        // span-fragmented: the left scan leaf when the right side was built, or the right
        // table source when the left subtree was built (a derived right source cannot be
        // span-scanned remotely).
        TableDescriptor probeTable;
        string probeAlias;
        NodeAst? probeFilter;
        IReadOnlySet<string>? probeRequired;

        if (buildSide == HashJoinBuildSide.Right)
        {
            if (joinNode.Input is not TableScanNode { Source: TableScanSource.PrimaryRows, BoundSource: { } probeSource } probeScan)
                return Decline(plan, $"probe (left input) is not a primary-row base table scan: {joinNode.Input?.GetType().Name}");

            probeTable = probeSource.Table;
            probeAlias = probeSource.Alias;
            probeFilter = probeScan.ExecutionFilter;
            probeRequired = probeScan.RequiredColumns ?? JoinAliasMetadata.GetRequiredColumnsForAlias(plan, probeAlias);
        }
        else
        {
            if (joinNode.BuildSource.Table is not { } rightSource)
                return Decline(plan, "probe (right source) is not a base table");

            probeTable = rightSource.Table;
            probeAlias = rightSource.Alias;
            probeFilter = joinNode.BuildExecutionFilter;
            probeRequired = JoinAliasMetadata.GetRequiredColumnsForAlias(plan, probeAlias);
        }

        if (plan.Ticket.TxnState.FoldReads || plan.Ticket.ExclusivePredicateLocks || plan.DepCollector is not null)
            return Decline(plan, $"transaction shape (foldReads={plan.Ticket.TxnState.FoldReads}, exclusiveLocks={plan.Ticket.ExclusivePredicateLocks}, depCollector={plan.DepCollector is not null})");

        if (joinNode.OnPredicate is null || !FragmentFilterShippability.IsShippable(joinNode.OnPredicate))
            return Decline(plan, "ON predicate is not shippable");

        if (probeFilter is not null && !FragmentFilterShippability.IsShippable(probeFilter))
            return Decline(plan, "probe filter is not shippable");

        EmbeddedKahuna kahuna = plan.Database.Kahuna;
        if (!kahuna.IsClusterMode)
            return Decline(plan, "standalone node");

        TablePlacement placement = kahuna.GetPlacement(probeTable.Store.RowKeySpace);
        if (!placement.IsKeyRange || placement.Spans.Count < 2 || placement.AllLeadersLocal)
            return Decline(plan, $"placement of probe '{probeTable.Name}' (isKeyRange={placement.IsKeyRange}, spans={placement.Spans.Count}, allLeadersLocal={placement.AllLeadersLocal})");

        foreach (PlacementSpan span in placement.Spans)
        {
            if (!GatherNode.TryParseRowIdBound(span.StartKey, out _) || !GatherNode.TryParseRowIdBound(span.EndKey, out _))
                return Decline(plan, "placement span boundary is not a row-id key");
        }

        // Flatten the build in bucket enumeration order. Inter-bucket order is irrelevant
        // (buckets only group equal keys); what matters is that equal-key rows keep their
        // bucket order, so the remote's rebuilt buckets match this coordinator's exactly and
        // match indices reproduce the same merge order.
        int buildCount = 0;
        foreach (List<IReadOnlyDictionary<string, ColumnValue>> bucket in hashTable.Values)
            buildCount += bucket.Count;

        if (buildCount == 0 || buildCount > liveOptions.BroadcastJoinMaxBuildRows)
            return Decline(plan, $"build row count {buildCount} outside (0, {liveOptions.BroadcastJoinMaxBuildRows}]");

        List<IReadOnlyDictionary<string, ColumnValue>> flat = new(buildCount);
        string[] encoded = new string[buildCount];

        foreach (List<IReadOnlyDictionary<string, ColumnValue>> bucket in hashTable.Values)
        {
            foreach (IReadOnlyDictionary<string, ColumnValue> row in bucket)
            {
                encoded[flat.Count] = QueryExecutor.EncodeCells(row);
                flat.Add(row);
            }
        }

        return new BroadcastJoinPlan
        {
            ProbeTable = probeTable,
            ProbeAlias = probeAlias,
            ProbeRequiredColumns = probeRequired,
            ProbeSchemaVersion = JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, probeAlias),
            ProbeFilter = probeFilter,
            Placement = placement,
            FlatBuildRows = flat,
            Spec = new QueryFragmentJoinSpec
            {
                BuildIsLeft = buildSide == HashJoinBuildSide.Left,
                ProbeAlias = probeAlias,
                ProbeKeyColumns = joinNode.ProbeKeyColumns.ToArray(),
                BuildAlias = joinNode.BuildSource.Alias,
                BuildKeyColumns = joinNode.BuildKeyColumns.ToArray(),
                BuildRows = encoded,
                OnPredicateJson = NodeAstWireCodec.Serialize(joinNode.OnPredicate),
                ProbeFilterJson = probeFilter is not null ? NodeAstWireCodec.Serialize(probeFilter) : null,
            },
        };
    }
}

/// <summary>
/// Everything the broadcast probe needs beyond the join node itself: the probe leaf's
/// identity and placement, the build rows flattened into an indexable array (in bucket
/// enumeration order — remote match indices point into it), and the reusable fragment
/// join spec with the build rows already wire-encoded once.
/// </summary>
internal sealed class BroadcastJoinPlan
{
    public required TableDescriptor ProbeTable { get; init; }

    public required string ProbeAlias { get; init; }

    public required IReadOnlySet<string>? ProbeRequiredColumns { get; init; }

    public required int ProbeSchemaVersion { get; init; }

    public required NodeAst? ProbeFilter { get; init; }

    public required TablePlacement Placement { get; init; }

    public required IReadOnlyList<IReadOnlyDictionary<string, ColumnValue>> FlatBuildRows { get; init; }

    public required QueryFragmentJoinSpec Spec { get; init; }
}
