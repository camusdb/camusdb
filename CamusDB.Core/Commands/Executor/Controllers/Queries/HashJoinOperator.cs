/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The equi-join workhorse: materialize one side into a hash table keyed on the join columns,
/// then stream the other side against it.
///
/// <para>The build side is whichever side the planner marked, and the two shapes differ in what
/// the stored rows look like. A right-side build stores unqualified rows and the merge qualifies
/// them at probe time; a left-side build stores rows already qualified, so a probe passes them
/// straight through as the merge's left argument. Either way the emitted column naming is
/// identical.</para>
///
/// <para>A build that does not fit its cap routes to a fallback instead of growing without
/// bound — the Grace hash join when spilling is enabled, a nested-loop scan when it is not. Both
/// re-scan the build from scratch, which costs about twice the reads on this rare path, and that
/// is accepted for the simplicity.</para>
///
/// <para>The broadcast decision happens after the build exists, so it gates on the build's
/// <b>actual</b> row count rather than a cardinality estimate — interior-node estimates are
/// constants on a multi-way join, so an estimate would be worthless here.</para>
/// </summary>
internal sealed class HashJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    private readonly JoinLeafScanner scanner;

    private readonly NestedLoopJoinOperator nestedLoopJoin;

    private readonly GraceHashJoinOperator graceHashJoin;

    private readonly BroadcastJoinPlanner broadcastPlanner;

    private readonly BroadcastHashProbeExecutor broadcastProbe;

    public HashJoinOperator(
        JoinExecutionServices services,
        IJoinNodeExecutor tree,
        JoinLeafScanner scanner,
        NestedLoopJoinOperator nestedLoopJoin,
        GraceHashJoinOperator graceHashJoin,
        BroadcastJoinPlanner broadcastPlanner,
        BroadcastHashProbeExecutor broadcastProbe)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
        this.nestedLoopJoin = nestedLoopJoin;
        this.graceHashJoin = graceHashJoin;
        this.broadcastPlanner = broadcastPlanner;
        this.broadcastProbe = broadcastProbe;
    }

    /// <summary>
    /// Materialises the build side into a hash table keyed on the equi-join columns.
    /// Rows whose join key contains any NULL value are excluded.
    ///
    /// Returns null to signal that the caller should route to a fallback:
    /// <list type="bullet">
    ///   <item>When <see cref="CamusDBOptions.SpillEnabled"/> is true, the effective cap is
    ///     <see cref="CamusDBOptions.SpillEffectiveThreshold"/>; overflow routes to
    ///     <see cref="GraceHashJoinOperator.GraceHashJoinAsync"/>.</item>
    ///   <item>When spill is disabled, the cap is <see cref="CamusDBOptions.HashJoinMaxBuildRows"/>;
    ///     overflow routes to a full nested-loop scan.</item>
    /// </list>
    ///
    /// On cap overflow the build scan is abandoned; the fallback re-scans the build side from
    /// scratch (~2× cost on this rare path). This is intentional: correctness over complexity.
    ///
    /// When <paramref name="buildSide"/> is <see cref="HashJoinBuildSide.Right"/> the
    /// right source (<see cref="HashJoinNode.BuildSource"/>) is scanned and keyed by
    /// <see cref="HashJoinNode.BuildKeyColumns"/> (unqualified column names). Stored rows
    /// are unqualified — <c>MergeRows</c> qualifies them at probe time.
    ///
    /// When <paramref name="buildSide"/> is <see cref="HashJoinBuildSide.Left"/> the left
    /// subtree (<see cref="PhysicalPlanNode.Input"/>) is scanned, each row is qualified
    /// immediately, and keyed by <see cref="HashJoinNode.ProbeKeyColumns"/> (qualified
    /// column names such as <c>o.id</c>). Stored rows are already qualified — at probe
    /// time they are passed directly as the left arg to <c>MergeRows</c>.
    /// </summary>
    private async Task<Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>>?> BuildHashTable(
        HashJoinNode joinNode,
        QueryPlan plan,
        HashJoinBuildSide buildSide)
    {
        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>> table =
            new(CompositeColumnValueComparer.Instance);

        // With spill enabled, cap at SpillEffectiveThreshold so the Grace path is triggered
        // at the configured spill boundary instead of the (much larger) legacy NLJ cap.
        // With spill disabled, honour the legacy HashJoinMaxBuildRows cap.
        QueryExecutionContext context = QueryExecutionContext.For(plan.Database, plan.Ticket);

        int buildCap = context.Options.SpillEnabled
            ? context.Options.SpillEffectiveThreshold
            : context.Options.HashJoinMaxBuildRows;

        int rowCount = 0;

        // Keys are looked up as a span over a reused scratch array; an owned key is copied
        // (comparer Create) only when a new bucket is inserted, so a duplicate-heavy build
        // allocates one key per distinct key rather than one per row.
        var lookup = table.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

        if (buildSide == HashJoinBuildSide.Right)
        {
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;
            ColumnValue[] keyScratch = new ColumnValue[buildKeys.Count];

            await foreach (QueryResultRow row in scanner.ScanJoinRightSource(
                joinNode.BuildSource, joinNode.BuildExecutionFilter, plan).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return null;

                if (!JoinKeyExtractor.TryExtractKeyInto(row.Row, buildKeys, keyScratch)) continue;

                if (!lookup.TryGetValue(keyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                { bucket = []; lookup[keyScratch.AsSpan()] = bucket; }

                bucket.Add(row.Row);
                rowCount++;
            }
        }
        else
        {
            // BuildSide.Left: materialise the left subtree; rows are qualified immediately so
            // they can be passed directly as the "left" arg to MergeRowsAsQueryRow during probe.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
            ColumnValue[] keyScratch = new ColumnValue[probeKeys.Count];
            RowLayout? qualifiedLeftLayout = null;

            await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return null;

                IReadOnlyDictionary<string, ColumnValue> qualified;
                string leftAlias = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftRow);
                if (leftRow.Row is QueryRow leftQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                    qualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
                }
                else
                {
                    qualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
                }

                if (!JoinKeyExtractor.TryExtractKeyInto(qualified, probeKeys, keyScratch)) continue;

                if (!lookup.TryGetValue(keyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                { bucket = []; lookup[keyScratch.AsSpan()] = bucket; }

                bucket.Add(qualified);
                rowCount++;
            }
        }

        return table;
    }

    internal async IAsyncEnumerable<QueryResultRow> ExecuteHashJoin(
        HashJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        HashJoinBuildSide buildSide = joinNode.BuildSide;

        Dictionary<CompositeColumnValue, List<IReadOnlyDictionary<string, ColumnValue>>>? hashTable =
            await BuildHashTable(joinNode, plan, buildSide).ConfigureAwait(false);

        if (hashTable is null)
        {
            // Build cap exceeded. When spilling is enabled, route to Grace/hybrid hash join
            // so large builds are partitioned to disk instead of falling back to O(n·m) NLJ.
            // Both paths re-scan the build side from scratch (~2× cost on this rare path).
            if (plan.Database.Options.SpillEnabled)
            {
                await foreach (QueryResultRow row in graceHashJoin.GraceHashJoinAsync(joinNode, plan, ct).ConfigureAwait(false))
                    yield return row;
                yield break;
            }

            // Spill disabled — fall back to nested-loop for correctness.
            NestedLoopJoinNode fallback = new(joinNode.Input!, joinNode.BuildSource, joinNode.OnPredicate!)
            {
                RightExecutionFilter = joinNode.BuildExecutionFilter,
            };

            await foreach (QueryResultRow row in nestedLoopJoin.ExecuteNestedLoopJoin(fallback, plan).ConfigureAwait(false))
                yield return row;

            yield break;
        }

        string rightAlias = joinNode.BuildSource.Alias;
        QueryTicket ticket = plan.Ticket;
        RowLayout? joinLayout = null;
        RowLayout? qualifiedLeftLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;

        if (buildSide == HashJoinBuildSide.Right)
        {
            // Broadcast probe: decided here, after the build exists, so it gates on the
            // build's ACTUAL row count — never a cardinality estimate (interior-node
            // estimates are constants on multi-way joins). When eligible, the small build
            // side is shipped to each remote probe span's leader and only matched probe
            // rows come back; otherwise the standard sequential probe below runs unchanged.
            BroadcastJoinPlan? broadcast = broadcastPlanner.TryPrepareBroadcastJoin(joinNode, plan, hashTable, HashJoinBuildSide.Right);

            if (broadcast is not null)
            {
                await foreach (QueryResultRow row in broadcastProbe.ExecuteBroadcastHashProbe(joinNode, plan, hashTable, broadcast).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            // Standard path: probe = left subtree, build = right source.
            // Hash table rows are unqualified; MergeRowsAsQueryRow qualifies them with rightAlias.
            // Probe keys go through the span alternate lookup: no key array or composite key
            // is allocated per probe row — misses and hits alike reuse the scratch array.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
            ColumnValue[] probeKeyScratch = new ColumnValue[probeKeys.Count];
            var probeLookup = hashTable.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

            await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
            {
                IReadOnlyDictionary<string, ColumnValue> leftQualified;
                string leftAlias = JoinAliasMetadata.ResolveLeftAlias(joinNode.Input!, leftRow);
                if (leftRow.Row is QueryRow leftQr)
                {
                    qualifiedLeftLayout ??= QueryRowMerger.BuildQualifiedLayout(leftQr.Layout, leftAlias);
                    leftQualified = QueryRowMerger.QualifyRowAsQueryRow(leftQr, qualifiedLeftLayout);
                }
                else
                {
                    leftQualified = QueryRowMerger.QualifyRow(leftRow.Row, leftAlias);
                }

                if (!JoinKeyExtractor.TryExtractKeyInto(leftQualified, probeKeys, probeKeyScratch)) continue;

                if (!probeLookup.TryGetValue(probeKeyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, buildRow, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftQualified, buildRow, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
        else
        {
            // Same broadcast opportunity with the sides flipped: the built (left) side is the
            // small one, so it ships and the RIGHT table's spans probe remotely. This is the
            // shape real statistics usually produce (small side planned left / built).
            BroadcastJoinPlan? broadcast = broadcastPlanner.TryPrepareBroadcastJoin(joinNode, plan, hashTable, HashJoinBuildSide.Left);

            if (broadcast is not null)
            {
                await foreach (QueryResultRow row in broadcastProbe.ExecuteBroadcastHashProbe(joinNode, plan, hashTable, broadcast).ConfigureAwait(false))
                    yield return row;

                yield break;
            }

            // Build-left path: build = left subtree (stored qualified in hash table),
            // probe = right source (scanned unqualified).
            // MergeRowsAsQueryRow(leftQualifiedBuildRow, rightUnqualifiedProbeRow, rightAlias) is the
            // same call shape as the standard path — output column naming is identical.
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;
            ColumnValue[] probeKeyScratch = new ColumnValue[buildKeys.Count];
            var probeLookup = hashTable.GetAlternateLookup<ReadOnlySpan<ColumnValue>>();

            await foreach (QueryResultRow rightRow in scanner.ScanJoinRightSource(
                joinNode.BuildSource, joinNode.BuildExecutionFilter, plan).ConfigureAwait(false))
            {
                if (!JoinKeyExtractor.TryExtractKeyInto(rightRow.Row, buildKeys, probeKeyScratch)) continue;

                if (!probeLookup.TryGetValue(probeKeyScratch.AsSpan(), out List<IReadOnlyDictionary<string, ColumnValue>>? bucket))
                    continue;

                foreach (IReadOnlyDictionary<string, ColumnValue> leftBuildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftBuildRow, rightRow.Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                    QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(leftBuildRow, rightRow.Row, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
    }
}
