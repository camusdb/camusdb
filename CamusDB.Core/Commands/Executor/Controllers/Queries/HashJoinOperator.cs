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
using Microsoft.Extensions.Logging;

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
/// is accepted for the simplicity. Each nested-loop fallback adds one to a count that
/// <c>SHOW ENGINE STATS</c> reports and writes a rate-limited warning
/// (<see cref="JoinFallbackMetrics"/>).</para>
///
/// <para>The broadcast decision happens after the build exists, so it gates on the build's
/// <b>actual</b> row count rather than a cardinality estimate — interior-node estimates are
/// constants on a multi-way join, so an estimate would be worthless here.</para>
///
/// <para>A left outer join always builds the right side and probes the left: the preserved side
/// streams, so a probe row with a NULL key, no bucket, or no bucket row the <c>ON</c> predicate
/// accepts is padded at once (<see cref="OuterJoinPadding"/>), in stream order, with no matched
/// flag on the build rows. The planner never pairs the kind with a left build; this operator
/// refuses that pairing rather than emit inner-join rows for it. The broadcast probe is declined
/// by its planner for the kind, and both fallbacks carry the kind through.</para>
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
    ///   <item>When <see cref="CamusDBOptions.SpillEnabled"/> is true, the effective caps are
    ///     <see cref="CamusDBOptions.SpillEffectiveThreshold"/> rows and
    ///     <see cref="CamusDBOptions.SpillEffectiveThresholdBytes"/> estimated bytes
    ///     (<see cref="JoinHashTable.EstimateAddedBytes"/>); overflow of either routes to
    ///     <see cref="GraceHashJoinOperator.GraceHashJoinAsync"/>.</item>
    ///   <item>When spill is disabled, the caps are <see cref="CamusDBOptions.HashJoinMaxBuildRows"/>
    ///     rows and <see cref="CamusDBOptions.HashJoinMaxBuildBytes"/> estimated bytes, with the same
    ///     estimate; overflow of either routes to a full nested-loop scan.</item>
    /// </list>
    ///
    /// The second value names the cap that stopped the build, so the fallback warning can name
    /// the setting to raise. It is <see cref="BuildLimit.None"/> when the table is returned.
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
    private async Task<(JoinHashTable? Table, BuildLimit StoppedBy)> BuildHashTable(
        HashJoinNode joinNode,
        QueryPlan plan,
        HashJoinBuildSide buildSide)
    {
        JoinHashTable table = new();

        // With spill enabled, cap at SpillEffectiveThreshold so the Grace path is triggered
        // at the configured spill boundary instead of the (much larger) legacy NLJ cap.
        // With spill disabled, honour the legacy HashJoinMaxBuildRows cap.
        QueryExecutionContext context = QueryExecutionContext.For(plan.Database, plan.Ticket);

        int buildCap = context.Options.SpillEnabled
            ? context.Options.SpillEffectiveThreshold
            : context.Options.HashJoinMaxBuildRows;

        // The build also stops at a byte cap, so a build of wide rows does not hold the row cap's
        // worth of them: with spill on it routes to Grace, with spill off it falls back to the
        // nested-loop join. A setting of zero or less removes the byte cap.
        long byteSetting = context.Options.SpillEnabled
            ? context.Options.SpillThresholdBytes
            : context.Options.HashJoinMaxBuildBytes;
        bool capBytes = byteSetting > 0;
        long byteCap = capBytes ? byteSetting : long.MaxValue;
        long buildBytes = 0;

        int rowCount = 0;

        // Keys are looked up as a span over a reused scratch array; an owned key is copied
        // (comparer Create) only when a new bucket is inserted, so a duplicate-heavy build
        // allocates one key per distinct key rather than one per row (see JoinHashTable).

        if (buildSide == HashJoinBuildSide.Right)
        {
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;
            ColumnValue[] keyScratch = new ColumnValue[buildKeys.Count];

            await foreach (QueryResultRow row in ReadRightSide(joinNode, plan, tree, scanner).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return (null, BuildLimit.Rows);
                if (buildBytes >= byteCap) return (null, BuildLimit.Bytes);

                if (!JoinKeyExtractor.TryExtractKeyInto(row.Row, buildKeys, keyScratch)) continue;

                bool newBucket = table.Add(keyScratch, row.Row);
                rowCount++;

                if (capBytes)
                    buildBytes += JoinHashTable.EstimateAddedBytes(keyScratch, row.Row, newBucket);
            }
        }
        else
        {
            // BuildSide.Left: materialise the left subtree; rows are qualified immediately so
            // they can be passed directly as the "left" row of the joined pair during probe.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
            ColumnValue[] keyScratch = new ColumnValue[probeKeys.Count];
            RowLayout? qualifiedLeftLayout = null;

            await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
            {
                if (rowCount >= buildCap) return (null, BuildLimit.Rows);
                if (buildBytes >= byteCap) return (null, BuildLimit.Bytes);

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

                bool newBucket = table.Add(keyScratch, qualified);
                rowCount++;

                if (capBytes)
                    buildBytes += JoinHashTable.EstimateAddedBytes(keyScratch, qualified, newBucket);
            }
        }

        return (table, BuildLimit.None);
    }

    /// <summary>The cap that stopped a hash-join build, if one did.</summary>
    private enum BuildLimit
    {
        /// <summary>The build finished under both caps.</summary>
        None,

        /// <summary>The build reached its row cap.</summary>
        Rows,

        /// <summary>The build reached its byte cap, estimated as the rows were added.</summary>
        Bytes,
    }

    /// <summary>
    /// Reads the right table of <paramref name="joinNode"/>: through its own access path when the
    /// planner gave it one (<see cref="HashJoinNode.RightPhysicalNode"/>), else with a full scan and the
    /// pushed filter. Every read of the right side uses this, so the build, the Grace partitioning
    /// and the probe of a left build all read the input the plan shows.
    /// </summary>
    internal static IAsyncEnumerable<QueryResultRow> ReadRightSide(
        HashJoinNode joinNode, QueryPlan plan, IJoinNodeExecutor tree, JoinLeafScanner scanner) =>
        joinNode.RightPhysicalNode is { } rightInput
            ? tree.ExecuteNode(rightInput, plan)
            : scanner.ScanJoinRightSource(joinNode.BuildSource, joinNode.BuildExecutionFilter, plan, joinNode.Stats);

    /// <summary>
    /// Counts a nested-loop fallback for <c>SHOW ENGINE STATS</c> and writes a warning, at most one
    /// each <see cref="JoinFallbackMetrics.LogIntervalMs"/> for this engine. The plan of the
    /// statement still shows a hash join, so the count and the warning are the signs of the fallback
    /// outside <c>EXPLAIN ANALYZE</c>. The warning names the database and the right side of the
    /// join. It holds no SQL text and no value from the statement, because a literal can be private
    /// data.
    /// </summary>
    private void NoteNestedLoopFallback(HashJoinNode joinNode, QueryPlan plan, BuildLimit stoppedBy)
    {
        if (!services.JoinFallbacks.NoteNestedLoopFallback(out long notLogged) || services.Logger is not { } logger)
            return;

        BoundJoinRightSource right = joinNode.BuildSource;
        CamusDBOptions options = plan.Database.Options;

        (string setting, long limit) = stoppedBy == BuildLimit.Bytes
            ? ("hash_join_max_build_bytes", options.HashJoinMaxBuildBytes)
            : ("hash_join_max_build_rows", (long)options.HashJoinMaxBuildRows);

        logger.LogWarning(
            "A hash join in database {Database} ran as a nested-loop join, which compares each row of one side with every row of the other. " +
            "Its build side passed {Setting} ({Limit}) and spill_enabled is off. " +
            "Right side of the join: {RightSide}. Other fallbacks since the previous warning: {NotLogged}. " +
            "Turn on spill_enabled, or raise that setting",
            plan.Database.Name,
            setting,
            limit,
            right.Table?.Table.Name ?? right.Alias,
            notLogged);
    }

    internal async IAsyncEnumerable<QueryResultRow> ExecuteHashJoin(
        HashJoinNode joinNode,
        QueryPlan plan,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        HashJoinBuildSide buildSide = joinNode.BuildSide;

        if (joinNode.Kind == JoinKind.LeftOuter && buildSide != HashJoinBuildSide.Right)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A left outer hash join must build the right side: the preserved left side is the probe");
        }

        // The dispatcher passes no enumerator token, so without this the Grace path below and the
        // probe loop would see a default token and run to the end after the request was abandoned.
        using CancellationTokenSource? linked = JoinCancellation.Link(plan, ct);
        CancellationToken token = JoinCancellation.Effective(plan, linked);

        (JoinHashTable? hashTable, BuildLimit stoppedBy) = await BuildHashTable(joinNode, plan, buildSide).ConfigureAwait(false);

        if (hashTable is null)
        {
            // Build cap exceeded. When spilling is enabled, route to Grace/hybrid hash join
            // so large builds are partitioned to disk instead of falling back to O(n·m) NLJ.
            // Both paths re-scan the build side from scratch (~2× cost on this rare path).
            if (plan.Database.Options.SpillEnabled)
            {
                if (joinNode.Stats is { } graceStats)
                    graceStats.RanAs = "grace-hash";

                await foreach (QueryResultRow row in graceHashJoin.GraceHashJoinAsync(joinNode, plan, token).ConfigureAwait(false))
                    yield return row;
                yield break;
            }

            // Spill disabled — fall back to nested-loop for correctness. The fallback node shares
            // the hash join's counters, so its right-side re-reads are charged to the node the
            // plan shows; its output passes through this method and is counted there.
            if (joinNode.Stats is { } fallbackStats)
                fallbackStats.RanAs = "nested-loop-fallback";

            NoteNestedLoopFallback(joinNode, plan, stoppedBy);

            NestedLoopJoinNode fallback = new(joinNode.Input!, joinNode.BuildSource, joinNode.OnPredicate!)
            {
                RightExecutionFilter = joinNode.BuildExecutionFilter,
                RightPhysicalNode = joinNode.RightPhysicalNode,
                Kind = joinNode.Kind,
                Stats = joinNode.Stats,
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
        JoinRowPairer pairer = new();
        OuterJoinPadding? padding = joinNode.Kind == JoinKind.LeftOuter
            ? await OuterJoinPadding.CreateAsync(joinNode.BuildSource, plan, joinNode.RightPhysicalNode).ConfigureAwait(false)
            : null;

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
            // Hash table rows are unqualified; the joined row reads them under rightAlias.
            // Probe keys go through the span alternate lookup: no key array or composite key
            // is allocated per probe row — misses and hits alike reuse the scratch array.
            IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
            ColumnValue[] probeKeyScratch = new ColumnValue[probeKeys.Count];

            await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
            {
                // A probe row read from a materialized input passes no scan that would observe the token.
                token.ThrowIfCancellationRequested();

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

                if (padding is not null)
                    (joinLayout, rightOrdinalMap) = padding.Bind(leftQualified);

                // A NULL key or an absent bucket matches nothing: dropped for an inner join,
                // padded once for a left outer join.
                if (!JoinKeyExtractor.TryExtractKeyInto(leftQualified, probeKeys, probeKeyScratch)
                    || !hashTable.TryGetBucket(probeKeyScratch, out JoinBucket bucket))
                {
                    if (padding is not null)
                        yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));

                    continue;
                }

                bool matched = false;

                foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, buildRow, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, rightAlias, joinLayout);
                    QueryRow merged  = pairer.Pair(leftQualified, buildRow, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    matched = true;
                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                if (padding is not null && !matched)
                    yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));
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
            // pairer.Pair(leftQualifiedBuildRow, rightUnqualifiedProbeRow, ...) is the
            // same call shape as the standard path — output column naming is identical.
            IReadOnlyList<string> buildKeys = joinNode.BuildKeyColumns;
            ColumnValue[] probeKeyScratch = new ColumnValue[buildKeys.Count];

            await foreach (QueryResultRow rightRow in ReadRightSide(joinNode, plan, tree, scanner).ConfigureAwait(false))
            {
                if (!JoinKeyExtractor.TryExtractKeyInto(rightRow.Row, buildKeys, probeKeyScratch)) continue;

                if (!hashTable.TryGetBucket(probeKeyScratch, out JoinBucket bucket))
                    continue;

                foreach (IReadOnlyDictionary<string, ColumnValue> leftBuildRow in bucket)
                {
                    joinLayout      ??= QueryRowMerger.BuildJoinLayout(leftBuildRow, rightRow.Row, rightAlias);
                    rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow.Row, rightAlias, joinLayout);
                    QueryRow merged  = pairer.Pair(leftBuildRow, rightRow.Row, joinLayout, rightOrdinalMap);

                    if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, plan.Database).ConfigureAwait(false))
                        continue;

                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }
        }
    }
}
