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
/// Runs a hash join's probe side as one worker per placement span: a span whose leader is remote
/// receives the shipped build and returns only matched probe rows (raw bytes plus match indices
/// into the shipped build), while a local span — or a span whose remote attempt failed — streams
/// unfiltered probe rows for this node to probe itself.
///
/// <para><b>Output order.</b> Spans partition the row keyspace and the channels are drained in
/// span order, so the emitted rows are byte-identical to what the sequential probe would emit.
/// The coordinator always merges a pair against its <b>own</b> build dictionaries; a remote
/// contributes indices and never a merged row.</para>
///
/// <para><b>Thread affinity.</b> Filtering, probing, merging, and ON evaluation all run on the
/// consumer thread, because the filterer and the lazily built layouts are not thread-safe. A
/// worker only fetches and decodes. Moving any of the consumer-thread work onto a worker would be
/// a data race, not a refactor.</para>
///
/// <para><b>Failure.</b> A remote failure resumes that span locally, after the last probe row the
/// remote delivered. Frames are per probe row — all of a row's matches ship together — so a
/// resume duplicates nothing and loses nothing mid-bucket.</para>
/// </summary>
internal sealed class BroadcastHashProbeExecutor
{
    private readonly JoinExecutionServices services;

    public BroadcastHashProbeExecutor(JoinExecutionServices services)
    {
        this.services = services;
    }

    /// <summary>One probe row crossing a broadcast span channel; null <see cref="Matches"/> means "probe on the consumer" (local span or fallback), non-null means the remote already filtered, probed, and ON-checked.</summary>
    private readonly record struct BroadcastProbeItem(ObjectIdValue RowId, QueryRow Row, int[]? Matches);

    /// <summary>
    /// The broadcast probe: one worker per placement span — remote spans ship the build and
    /// receive only matched probe rows (raw bytes + match indices), local spans and fallbacks
    /// stream unfiltered probe rows — drained strictly in span order so output is
    /// byte-identical to the sequential probe (spans partition the row keyspace, and the
    /// coordinator merges every pair against its <b>own</b> build dictionaries; the remote
    /// only ever contributes indices). Filtering, probing, merging, and ON evaluation all run
    /// on this consumer thread — the filterer and lazily built layouts are not thread-safe —
    /// while workers do the fetch and decode. A remote failure resumes that span locally
    /// after the last delivered probe row; frames are per probe row, so nothing is duplicated
    /// or lost mid-bucket.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ExecuteBroadcastHashProbe(
        HashJoinNode joinNode,
        QueryPlan plan,
        JoinHashTable hashTable,
        BroadcastJoinPlan broadcast)
    {
        TableDescriptor table = broadcast.ProbeTable;
        DatabaseDescriptor database = plan.Database;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        HLCTimestamp readTs = plan.Ticket.TxnState.ReadTimestamp;
        CamusDBOptions options = database.Options;
        QueryTicket ticket = plan.Ticket;
        string rightAlias = joinNode.BuildSource.Alias;
        IReadOnlyList<string> probeKeys = joinNode.ProbeKeyColumns;
        IReadOnlyList<PlacementSpan> spans = broadcast.Placement.Spans;

        // The session must exist before workers read tx.TransactionId concurrently.
        await plan.Ticket.TxnState.EnsureSessionStartedAsync(CancellationToken.None, table.Store.PlacementGroup).ConfigureAwait(false);

        // Linked to the request: every probe worker reads and writes through this token, so the
        // link stops the local scans and the remote fragments together on a client disconnect.
        using CancellationTokenSource cts =
            CancellationTokenSource.CreateLinkedTokenSource(plan.Ticket.CancellationToken);

        var channels = new Channel<BroadcastProbeItem>[spans.Count];
        var workers = new Task[spans.Count];

        // EXPLAIN (ANALYZE) only. Thread-safety is by ownership, as in the gather: each span record
        // is written by its span's worker, except RowsDelivered, which only this consumer writes;
        // the node counters are written only on this consumer thread. The probe-side reads belong
        // to the node that owns the probe input: the left leaf for a right build (it never runs as
        // a node on this path, so its rows are counted here), the join itself for a left build
        // (the right side has no node of its own). As in the gather, every probe row this node
        // decodes counts as read, local or shipped; the rows a remote span scanned and did not ship
        // appear only in its span record.
        PlanNodeStats? joinStats = joinNode.Stats;
        GatherSpanExecution[]? spanExecutions = null;
        PlanNodeStats? probeStats = null;
        bool countProbeEmitted = false;

        if (joinStats is not null)
        {
            joinStats.RanAs = "broadcast";
            spanExecutions = new GatherSpanExecution[spans.Count];
            for (int i = 0; i < spans.Count; i++)
                spanExecutions[i] = new GatherSpanExecution { PartitionId = spans[i].PartitionId };
            joinStats.BroadcastSpans = spanExecutions;

            countProbeEmitted = !broadcast.Spec.BuildIsLeft;
            probeStats = countProbeEmitted ? joinNode.Input!.Stats : joinStats;
        }

        Diagnostics.StatementProbe? statementProbe = plan.Ticket.Probe;

        for (int i = 0; i < spans.Count; i++)
        {
            channels[i] = Channel.CreateBounded<BroadcastProbeItem>(
                new BoundedChannelOptions(256) { SingleReader = true, SingleWriter = true });

            PlacementSpan span = spans[i];
            GatherSpanExecution? spanExecution = spanExecutions?[i];

            // Bounds were validated during preparation; a failure here means the placement
            // snapshot changed shape underneath us — fail closed via the channel.
            if (!GatherNode.TryParseRowIdBound(span.StartKey, out ObjectIdValue? fromRowId)
                || !GatherNode.TryParseRowIdBound(span.EndKey, out ObjectIdValue? untilRowId))
            {
                channels[i].Writer.TryComplete(new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Placement span boundary is not a row-id key: " + (span.StartKey ?? span.EndKey)));
                workers[i] = Task.CompletedTask;
                continue;
            }

            bool remote = !span.LeaderIsLocal && span.LeaderEndpoint is not null;

            workers[i] = remote
                ? RunRemoteProbeSpanAsync(channels[i].Writer, span, fromRowId, untilRowId, spanExecution)
                : RunLocalProbeSpanAsync(channels[i].Writer, fromRowId, untilRowId, afterRowId: null);
        }

        // Shared merge state, consumer-thread only. The two build sides differ only in which
        // argument of the merge the probe row occupies and where its keys are read from:
        // build-right probes are qualified left rows (keys via ProbeKeyColumns, merged as the
        // left argument); build-left probes are bare right rows (keys via BuildKeyColumns,
        // merged as the right argument, qualified by the merge itself).
        bool buildIsLeft = broadcast.Spec.BuildIsLeft;
        IReadOnlyList<string> localProbeKeys = buildIsLeft ? joinNode.BuildKeyColumns : probeKeys;
        ColumnValue[] probeKeyScratch = new ColumnValue[localProbeKeys.Count];
        RowLayout? qualifiedProbeLayout = null;
        RowLayout? joinLayout = null;
        Dictionary<string, int>? rightOrdinalMap = null;
        JoinRowPairer pairer = new();

        try
        {
            for (int i = 0; i < spans.Count; i++)
            {
                GatherSpanExecution? spanExecution = spanExecutions?[i];

                await foreach (BroadcastProbeItem item in channels[i].Reader.ReadAllAsync(plan.Ticket.CancellationToken).ConfigureAwait(false))
                {
                    if (spanExecution is not null)
                        spanExecution.RowsDelivered++;

                    statementProbe?.AddRowRead();
                    if (probeStats is not null)
                    {
                        probeStats.KvScanEntries++;
                        probeStats.RowsRead++;

                        // A shipped row already passed the probe filter at the data: it left the leaf.
                        if (countProbeEmitted && item.Matches is not null)
                            probeStats.RowsEmitted++;
                    }

                    qualifiedProbeLayout ??= QueryRowMerger.BuildQualifiedLayout(item.Row.Layout, broadcast.ProbeAlias);
                    IReadOnlyDictionary<string, ColumnValue> qualifiedProbe = QueryRowMerger.QualifyRowAsQueryRow(item.Row, qualifiedProbeLayout);

                    // Key extraction reads the same view the standard probe reads: the
                    // qualified row for a left-side probe, the bare row for a right-side one.
                    IReadOnlyDictionary<string, ColumnValue> keySource = buildIsLeft ? item.Row : qualifiedProbe;

                    if (item.Matches is null)
                    {
                        // Local span (or fallback): the standard probe, on this thread.
                        if (broadcast.ProbeFilter is not null
                            && !await services.Filterer.MeetWhereAsync(broadcast.ProbeFilter, qualifiedProbe, ticket, database).ConfigureAwait(false))
                            continue;

                        if (countProbeEmitted && probeStats is not null)
                            probeStats.RowsEmitted++;

                        if (!JoinKeyExtractor.TryExtractKeyInto(keySource, localProbeKeys, probeKeyScratch)) continue;

                        if (!hashTable.TryGetBucket(probeKeyScratch, out JoinBucket bucket))
                            continue;

                        foreach (IReadOnlyDictionary<string, ColumnValue> buildRow in bucket)
                        {
                            QueryRow merged = MergePair(buildRow, item.Row, qualifiedProbe);

                            if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate!, merged, ticket, database).ConfigureAwait(false))
                                continue;

                            yield return new QueryResultRow(default(ObjectIdValue), merged);
                        }
                    }
                    else
                    {
                        // Remote span: filter, probe, and ON check already ran at the data —
                        // re-running would double-evaluate. Merge against our own build rows.
                        foreach (int matchIndex in item.Matches)
                        {
                            if ((uint)matchIndex >= (uint)broadcast.FlatBuildRows.Count)
                                throw new CamusDBException(
                                    CamusDBErrorCodes.InvalidInternalOperation,
                                    $"Broadcast join fragment returned match index {matchIndex} outside the shipped build ({broadcast.FlatBuildRows.Count} rows)");

                            yield return new QueryResultRow(
                                default(ObjectIdValue),
                                MergePair(broadcast.FlatBuildRows[matchIndex], item.Row, qualifiedProbe));
                        }
                    }
                }

                await workers[i].ConfigureAwait(false);
            }
        }
        finally
        {
            cts.Cancel();

            foreach (Channel<BroadcastProbeItem> channel in channels)
            {
                while (channel.Reader.TryRead(out _)) { }
            }

            foreach (Task worker in workers)
            {
                try
                {
                    await worker.ConfigureAwait(false);
                }
                catch
                {
                    // Consumer is exiting; a worker fault was either already thrown from its
                    // channel or belongs to an abandoned span.
                }
            }
        }

        // The merge argument order is the standard probe's for each build side: build-right
        // merges (qualified probe, bare build); build-left merges (qualified build, bare
        // probe). The lazily built layout/ordinal-map pair is shared for the whole gather —
        // consumer-thread only.
        QueryRow MergePair(
            IReadOnlyDictionary<string, ColumnValue> buildRow,
            QueryRow bareProbe,
            IReadOnlyDictionary<string, ColumnValue> qualifiedProbe)
        {
            if (buildIsLeft)
            {
                joinLayout      ??= QueryRowMerger.BuildJoinLayout(buildRow, bareProbe, rightAlias);
                rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(bareProbe, rightAlias, joinLayout);
                return pairer.Pair(buildRow, bareProbe, joinLayout, rightOrdinalMap);
            }

            joinLayout      ??= QueryRowMerger.BuildJoinLayout(qualifiedProbe, buildRow, rightAlias);
            rightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(buildRow, rightAlias, joinLayout);
            return pairer.Pair(qualifiedProbe, buildRow, joinLayout, rightOrdinalMap);
        }

        async Task RunRemoteProbeSpanAsync(
            ChannelWriter<BroadcastProbeItem> writer,
            PlacementSpan span,
            ObjectIdValue? fromRowId,
            ObjectIdValue? untilRowId,
            GatherSpanExecution? spanExecution)
        {
            // Probe rows already delivered from this span; frames are per probe row (all of a
            // row's matches ship together), so resuming locally AFTER this row on failure
            // duplicates and loses nothing.
            ObjectIdValue? lastEmitted = null;
            long shippedThisSpan = 0;

            if (services.DistributedMetrics is not null)
                Interlocked.Increment(ref services.DistributedMetrics.FragmentsDispatched);

            if (spanExecution is not null)
            {
                spanExecution.DispatchedRemotely = true;
                spanExecution.RemoteEndpoint = span.LeaderEndpoint;
            }

            try
            {
                QueryFragmentRequest request = new()
                {
                    FragmentId = Guid.NewGuid().ToString("n"),
                    DatabaseName = database.Name,
                    DatabaseId = database.Id,
                    TableName = table.Name,
                    TableId = table.Id,
                    SchemaVersion = broadcast.ProbeSchemaVersion,
                    FromRowIdHex = fromRowId?.ToString(),
                    UntilRowIdHex = untilRowId?.ToString(),
                    ReadTsNode = readTs.N,
                    ReadTsPhysical = readTs.L,
                    ReadTsCounter = readTs.C,
                    RequiredColumns = broadcast.ProbeRequiredColumns?.ToArray(),
                    Join = broadcast.Spec,
                    WantStats = spanExecution is not null,
                };

                RowEncoder.RowDecodeState decodeState = new();

                await foreach (QueryFragmentRow fragmentRow in services.FragmentTransport!
                    .ExecuteFragmentAsync(span.LeaderEndpoint!, request, cts.Token).ConfigureAwait(false))
                {
                    // Terminal stats frame (sent only when WantStats was requested): actuals for
                    // EXPLAIN (ANALYZE), never row data.
                    if (fragmentRow.Stats is { } fragmentStats)
                    {
                        if (spanExecution is not null)
                            spanExecution.RemoteRowsScanned = fragmentStats.RowsScanned;
                        continue;
                    }

                    if (fragmentRow.MatchIndices is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            "Broadcast join fragment returned a frame without match indices");

                    if (fragmentRow.RowIdHex is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInternalOperation,
                            "Broadcast join fragment returned a row frame without a row id");

                    ObjectIdValue rowId = ObjectId.ToValue(fragmentRow.RowIdHex);

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema,
                        txId,
                        rowId,
                        fragmentRow.Data,
                        options,
                        broadcast.ProbeRequiredColumns,
                        broadcast.ProbeSchemaVersion,
                        decodeState).ConfigureAwait(false);

                    await writer.WriteAsync(new BroadcastProbeItem(rowId, row, fragmentRow.MatchIndices), cts.Token).ConfigureAwait(false);
                    lastEmitted = rowId;
                    shippedThisSpan++;

                    if (spanExecution is not null)
                        spanExecution.RemoteRowsShipped = shippedThisSpan;
                }

                writer.Complete();
            }
            catch (OperationCanceledException)
            {
                writer.TryComplete();
            }
            catch (Exception ex)
            {
                Log.LogRemoteFragmentFellBackToLocal(services.Logger!, span.LeaderEndpoint!, ex.Message);
                database.Kahuna.InvalidatePlacement(table.Store.RowKeySpace);

                if (services.DistributedMetrics is not null)
                    Interlocked.Increment(ref services.DistributedMetrics.FragmentFallbacks);

                if (spanExecution is not null)
                    spanExecution.FellBackToLocal = true;

                await RunLocalProbeSpanAsync(writer, fromRowId, untilRowId, afterRowId: lastEmitted).ConfigureAwait(false);
            }
            finally
            {
                if (services.DistributedMetrics is not null && shippedThisSpan > 0)
                    Interlocked.Add(ref services.DistributedMetrics.RowsShippedIn, shippedThisSpan);
            }
        }

        async Task RunLocalProbeSpanAsync(
            ChannelWriter<BroadcastProbeItem> writer,
            ObjectIdValue? fromRowId,
            ObjectIdValue? untilRowId,
            ObjectIdValue? afterRowId)
        {
            try
            {
                RowEncoder.RowDecodeState decodeState = new();

                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    plan.Ticket.TxnState,
                    afterRowId: afterRowId,
                    cancellationToken: cts.Token,
                    untilRowId: untilRowId,
                    fromRowId: fromRowId).ConfigureAwait(false))
                {
                    if (data.Length == 0)
                        continue;

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema,
                        txId,
                        rowId,
                        data,
                        options,
                        broadcast.ProbeRequiredColumns,
                        broadcast.ProbeSchemaVersion,
                        decodeState).ConfigureAwait(false);

                    await writer.WriteAsync(new BroadcastProbeItem(rowId, row, null), cts.Token).ConfigureAwait(false);
                }

                writer.Complete();
            }
            catch (OperationCanceledException)
            {
                writer.TryComplete();
            }
            catch (Exception ex)
            {
                writer.TryComplete(ex);
            }
        }
    }
}
