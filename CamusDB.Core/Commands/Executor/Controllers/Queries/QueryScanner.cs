
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Channels;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryScanner
{
    private readonly ILogger<ICamusDB> logger;

    public QueryScanner(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Decides whether a scan should decode rows with the borrowed (zero-copy <see cref="RowView"/>)
    /// backing. Benchmarks show borrowed decode is a broad win for scans that reject or project
    /// (−28% to −78% allocations, up to 2.7× faster), driven hardest by the byte-native string-equality
    /// filter path — but it is enabled only when <b>both</b> hold: a residual filter exists (an unfiltered
    /// <c>SELECT *</c> reads every cell and regresses slightly, so it stays eager), and no downstream
    /// operator <b>retains</b> rows across scan iterations. A borrowed row holds its <i>full</i> KV bytes
    /// for its lifetime, so retaining it (ORDER BY / GROUP BY / DISTINCT / semi-join) would hold more live
    /// memory than the projected-only slot path; those shapes keep the existing decode. The global
    /// <see cref="CamusDBOptions.BorrowedDecode"/> override still forces it on everywhere for A/B runs.
    /// </summary>
    internal static bool ShouldUseBorrowedDecode(QueryPlan plan)
    {
        switch (plan.Database.Options.BorrowedDecode)
        {
            case BorrowedDecodePolicy.ForceBorrowed:
                return true;
            case BorrowedDecodePolicy.ForceEager:
                return false;
        }

        // Adaptive: only the strict-win, memory-safe case — a residual filter, and no downstream operator
        // that retains rows across scan iterations (a borrowed row holds its full KV bytes for its life).
        if (plan.ExecutionFilter is null)
            return false;

        foreach (QueryPlanStep step in plan.Steps)
        {
            if (step.Type is QueryPlanStepType.SortBy
                          or QueryPlanStepType.Aggregate
                          or QueryPlanStepType.Distinct
                          or QueryPlanStepType.SemiJoinProbe)
                return false;
        }

        return true;
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanUsingTableIndex(
        QueryPlan plan,
        QueryFilterer queryFilterer
    )
    {
        TableDescriptor table = plan.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        int visibilityVersion = plan.TableSchemaVersion;
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;
        QueryDependencyCollector? deps = plan.DepCollector;

        // Acquire a phantom-protection range lock on the row key space before the scan.
        // Shared for SELECT; Exclusive for UPDATE/DELETE (blocks concurrent readers from
        // seeing the range while the mutation is in flight). Same-tx re-acquisition is
        // idempotent. Read-only transactions skip this.
        await table.Store.AcquireRowRangeLockAsync(plan.Ticket.TxnState,
            exclusive: plan.Ticket.ExclusivePredicateLocks).ConfigureAwait(false);

        // Full table scan: the entire row-bucket range is a dependency (catches phantom inserts).
        deps?.RecordRange(table.Store.RowKeySpace);
        deps?.RecordSchema(table.Id, table.Schema.Version, table.Schema.ContentsGeneration);

        // One RowLayout per stored schema version. Most scans touch only one version so this
        // holds one entry; mixed-version scans hold a handful. The layout is identical for all
        // rows at the same stored version (requiredColumns and visibilityVersion are constant
        // for the life of a scan), so building it once and reusing it is safe.
        // Slot-backed decode is the default (the response sinks serialize straight from ValueSlots, so
        // even an unfiltered SELECT * no longer materializes ColumnValues — see
        // CamusDBOptions.SlotBackedDecode). With the flag off (eager A/B baseline), a scan with a
        // residual filter still opts in adaptively: a rejected row decodes to slots but never
        // materializes its projection cells.
        RowEncoder.RowDecodeState decodeState =
            new()
            {
                SlotBackedDecode = plan.Database.Options.SlotBackedDecode || plan.ExecutionFilter is not null,
                BorrowedDecode = ShouldUseBorrowedDecode(plan),
            };

        // Focused scan instrumentation: a full primary-table scan is the "storage-heavy read" shape.
        // Counters track rows scanned vs returned; the span+duration isolate storage read time from the
        // downstream CPU stages. Recorded on completion of the iterator (including early break) via finally.
        using System.Diagnostics.Activity? storageSpan =
            Diagnostics.ServerDiagnostics.StartSpan(Diagnostics.ServerDiagnostics.Spans.StorageRead);
        storageSpan?.SetTag("scan", Diagnostics.ServerDiagnostics.Tags.Scan.Full);
        long scanStart = System.Diagnostics.Stopwatch.GetTimestamp();
        long rowsScanned = 0, rowsReturned = 0;

        int parallelism = plan.Database.Options.MaxQueryParallelism;

        try
        {
            if (parallelism > 1)
            {
                // Parallel decode pipeline: one producer streams the scan (identical fetch to the
                // sequential path below), fixed-size chunks decode concurrently, and chunks are
                // consumed in dispatch order — so rows arrive here in exactly the sequential
                // scan's order. Filtering and all bookkeeping stay on this single consumer
                // thread: expression evaluation and the dependency collector are not thread-safe.
                await foreach ((ObjectIdValue rowId, QueryRow queryRow) in ScanAndDecodeInParallel(plan, parallelism).ConfigureAwait(false))
                {
                    rowsScanned++;
                    if (scanStats is not null)
                    {
                        scanStats.KvScanEntries++;
                        scanStats.RowsRead++;
                    }

                    // Record the point dep for every fetched row — catches updates to non-indexed columns.
                    deps?.RecordPoint(table.Store.RowPointKey(rowId));

                    if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    {
                        rowsReturned++;
                        yield return new QueryResultRow(rowId, queryRow);
                    }
                }

                yield break;
            }

            // Read-set folding follows the transaction (KvTransaction.FoldReads), not the plan shape:
            // an optimistic transaction folds this scan's rows so its commit validates them, exactly
            // as a point read would — isolation must not depend on which plan answered the predicate.
            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                plan.Ticket.TxnState, maxRows: plan.ScanRowLimit))
            {
                if (data.Length == 0)
                    continue;

                rowsScanned++;
                if (scanStats is not null)
                    scanStats.KvScanEntries++;

                // Record the point dep for every fetched row — catches updates to non-indexed columns.
                deps?.RecordPoint(table.Store.RowPointKey(rowId));

                QueryRow queryRow = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema,
                    txId,
                    rowId,
                    data,
            plan.Database.Options,
                    plan.ScanRequiredColumns,
                    visibilityVersion,
                    decodeState).ConfigureAwait(false);

                if (scanStats is not null)
                    scanStats.RowsRead++;

                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                {
                    rowsReturned++;
                    yield return new QueryResultRow(rowId, queryRow);
                }
            }
        }
        finally
        {
            Diagnostics.ServerDiagnostics.RecordScanDuration(
                Diagnostics.ServerDiagnostics.Tags.Scan.Full,
                System.Diagnostics.Stopwatch.GetElapsedTime(scanStart).TotalMilliseconds);
            Diagnostics.ServerDiagnostics.RecordQueryRows(
                Diagnostics.ServerDiagnostics.Tags.Scan.Full, Diagnostics.ServerDiagnostics.Tags.Stage.Scanned, rowsScanned);
            Diagnostics.ServerDiagnostics.RecordQueryRows(
                Diagnostics.ServerDiagnostics.Tags.Scan.Full, Diagnostics.ServerDiagnostics.Tags.Stage.Returned, rowsReturned);
        }
    }

    /// <summary>
    /// Rows per decode chunk in the parallel scan pipeline. Large enough to amortize task
    /// dispatch and per-chunk decode-plan construction, small enough that read-ahead memory
    /// (at most ~3× parallelism chunks in flight) stays modest.
    /// </summary>
    private const int ParallelDecodeChunkSize = 64;

    /// <summary>
    /// Fetch-and-decode pipeline for a full primary-row scan when
    /// <see cref="CamusDBOptions.MaxQueryParallelism"/> &gt; 1. One producer enumerates
    /// <c>ScanRows</c> exactly like the sequential path (single transaction use, identical
    /// bounds and row limit), groups rows into chunks, and dispatches each chunk to the thread
    /// pool for decoding; the consumer awaits chunk tasks in dispatch order, so the emitted row
    /// order is byte-identical to the sequential scan for every plan shape. Parallelism is
    /// capped by a semaphore; the bounded channel caps read-ahead so a stalled consumer stalls
    /// the producer instead of buffering the table.
    ///
    /// Two deliberate constraints: each chunk decodes with its own
    /// <see cref="RowEncoder.RowDecodeState"/> (the per-scan decode-plan cache is not
    /// thread-safe), and borrowed (KV-byte-pinning) decode is never used because rows outlive
    /// the scan iteration inside chunk buffers — the same reason retaining operators disable it
    /// in <see cref="ShouldUseBorrowedDecode"/>. Residual-filter evaluation stays on the
    /// consumer thread; expression evaluation is not thread-safe.
    /// </summary>
    private static async IAsyncEnumerable<(ObjectIdValue rowId, QueryRow row)> ScanAndDecodeInParallel(
        QueryPlan plan,
        int parallelism)
    {
        TableDescriptor table = plan.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        int visibilityVersion = plan.TableSchemaVersion;
        CamusDBOptions options = plan.Database.Options;
        IReadOnlySet<string>? requiredColumns = plan.ScanRequiredColumns;
        bool slotBackedDecode = options.SlotBackedDecode || plan.ExecutionFilter is not null;

        using CancellationTokenSource cts = new();

        // Deliberately not disposed with `using`: a straggler decode task releases its slot in
        // a finally that can run after this iterator has exited on an early break.
        SemaphoreSlim decodeSlots = new(parallelism, parallelism);

        // In-order queue of chunk decode tasks. Order of insertion is scan order, and the
        // consumer awaits strictly in that order — this is what preserves sequential output
        // order without a reorder buffer.
        Channel<Task<(ObjectIdValue rowId, QueryRow row)[]>> chunks =
            Channel.CreateBounded<Task<(ObjectIdValue rowId, QueryRow row)[]>>(
                new BoundedChannelOptions(parallelism * 2) { SingleReader = true, SingleWriter = true });

        Task producer = ProduceAsync();

        try
        {
            await foreach (Task<(ObjectIdValue rowId, QueryRow row)[]> chunkTask in
                chunks.Reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
            {
                (ObjectIdValue rowId, QueryRow row)[] decoded = await chunkTask.ConfigureAwait(false);

                foreach ((ObjectIdValue rowId, QueryRow row) item in decoded)
                    yield return item;
            }

            // The channel completing with an exception already rethrows above; this await
            // surfaces a producer fault that raced channel completion.
            await producer.ConfigureAwait(false);
        }
        finally
        {
            cts.Cancel();

            // Early exit: observe abandoned chunk tasks so a decode fault after the consumer
            // stopped never surfaces as an unobserved task exception.
            while (chunks.Reader.TryRead(out Task<(ObjectIdValue rowId, QueryRow row)[]>? pending))
                _ = pending.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);

            try
            {
                await producer.ConfigureAwait(false);
            }
            catch
            {
                // The consumer is exiting; the fault (if any) was already thrown from the
                // channel read or is being discarded with the abandoned scan.
            }
        }

        async Task ProduceAsync()
        {
            try
            {
                List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> chunk = new(ParallelDecodeChunkSize);

                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    plan.Ticket.TxnState, maxRows: plan.ScanRowLimit, cancellationToken: cts.Token).ConfigureAwait(false))
                {
                    if (data.Length == 0)
                        continue;

                    chunk.Add((rowId, data));

                    if (chunk.Count >= ParallelDecodeChunkSize)
                    {
                        await DispatchAsync(chunk).ConfigureAwait(false);
                        chunk = new(ParallelDecodeChunkSize);
                    }
                }

                if (chunk.Count > 0)
                    await DispatchAsync(chunk).ConfigureAwait(false);

                chunks.Writer.Complete();
            }
            catch (OperationCanceledException)
            {
                chunks.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                chunks.Writer.TryComplete(ex);
            }
        }

        async Task DispatchAsync(List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> chunk)
        {
            // The slot is released by the decode task itself, so at most `parallelism` chunks
            // decode concurrently while the producer keeps fetching the next page.
            await decodeSlots.WaitAsync(cts.Token).ConfigureAwait(false);

            // No cancellation token on purpose: once dispatched, the task always runs and always
            // releases its slot; cancellation is handled by the producer stopping dispatch.
            Task<(ObjectIdValue rowId, QueryRow row)[]> task = Task.Run(() => DecodeChunkAsync(chunk));

            await chunks.Writer.WriteAsync(task, cts.Token).ConfigureAwait(false);
        }

        async Task<(ObjectIdValue rowId, QueryRow row)[]> DecodeChunkAsync(
            List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> chunk)
        {
            try
            {
                RowEncoder.RowDecodeState decodeState = new()
                {
                    SlotBackedDecode = slotBackedDecode,
                    BorrowedDecode = false,
                };

                var decoded = new (ObjectIdValue rowId, QueryRow row)[chunk.Count];

                for (int i = 0; i < chunk.Count; i++)
                {
                    (ObjectIdValue rowId, ReadOnlyMemory<byte> data) = chunk[i];

                    QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                        table.Schema,
                        txId,
                        rowId,
                        data,
                        options,
                        requiredColumns,
                        visibilityVersion,
                        decodeState).ConfigureAwait(false);

                    decoded[i] = (rowId, row);
                }

                return decoded;
            }
            finally
            {
                decodeSlots.Release();
            }
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanUsingIndex(
        QueryPlan plan,
        QueryFilterer queryFilterer,
        TableIndexSchema? stepIndex = null
    )
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        int visibilityVersion = plan.TableSchemaVersion;

        // Prefer the step's index (used by planner-forced scans for streaming DISTINCT/GROUP BY)
        // over ticket.IndexName (set only by the SQL FORCE_INDEX hint).
        TableIndexSchema? index = stepIndex;
        if (index is null && !table.Indexes.TryGetValue(ticket.IndexName!, out index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                $"Key '{ticket.IndexName!}' doesn't exist in table '{table.Name}'"
            );
        }

        if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                $"Key '{index!.Name}' doesn't exist in table '{table.Name}'"
            );
        }

        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);
        bool unique = index.Type == IndexType.Unique;
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;
        QueryDependencyCollector? deps = plan.DepCollector;

        // Phantom-protection range lock on the index key space (mirrors ScanUsingTableIndex).
        await table.Store.AcquireIndexRangeLockAsync(ticket.TxnState, index.KvId,
            exclusive: plan.Ticket.ExclusivePredicateLocks).ConfigureAwait(false);

        // Full index scan: record the index bucket range and the schema version.
        deps?.RecordRange(table.Store.IndexKeySpace(index.KvId));
        deps?.RecordSchema(table.Id, table.Schema.Version, table.Schema.ContentsGeneration);

        // Focused scan instrumentation for the index-driven scan shape. Recorded once on completion
        // (covering path's yield break and the paged path both flow through finally).
        using System.Diagnostics.Activity? storageSpan =
            Diagnostics.ServerDiagnostics.StartSpan(Diagnostics.ServerDiagnostics.Spans.StorageRead);
        storageSpan?.SetTag("scan", Diagnostics.ServerDiagnostics.Tags.Scan.IndexRange);
        long scanStart = System.Diagnostics.Stopwatch.GetTimestamp();
        long rowsScanned = 0;

        try
        {
        if (plan.IndexOnly)
        {
            // Covering (index-only) path: every needed column is already in the decoded index
            // key or is the primary-key (row-id) column. Synthesize the QueryRow directly from
            // the scan entry — no GetRow call and no RowEncoder.DecodeAsync call.
            // No per-row point dep is recorded because no primary row was read; the range dep
            // above and the schema dep cover snapshot correctness for the index scan.
            IndexOnlyLayout covered = BuildIndexOnlyLayout(table, plan.ScanRequiredColumns, index);

            await foreach ((CompositeColumnValue decodedKey, ObjectIdValue rowId, ReadOnlyMemory<byte> includeTuple) in table.Store.ScanIndex(
                ticket.TxnState,
                index.KvId,
                keyTypes,
                null,
                null,
                unique,
                fromInclusive: true,
                toInclusive: true,
                maxRows: plan.ScanRowLimit))
            {
                rowsScanned++;
                if (scanStats is not null)
                    scanStats.KvScanEntries++;

                ColumnValue[] values = SynthesizeCoveredValues(covered, decodedKey, includeTuple.Span);
                QueryRow queryRow = new(rowId, covered.Layout, values);

                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    yield return new QueryResultRow(rowId, queryRow);
            }
            yield break;
        }

        // Non-covering path: collect row ids in pages and fetch each page in one batch call.
        // Index order is preserved: the page is filled in scan order and batch results are
        // indexed positionally, so rows are decoded and yielded in scan order. Missing rows
        // (null bytes from the batch) preserve the warn-and-skip contract of the per-entry path.
        // Slot-backed decode is the default (the response sinks serialize straight from ValueSlots, so
        // even an unfiltered SELECT * no longer materializes ColumnValues — see
        // CamusDBOptions.SlotBackedDecode). With the flag off (eager A/B baseline), a scan with a
        // residual filter still opts in adaptively: a rejected row decodes to slots but never
        // materializes its projection cells.
        RowEncoder.RowDecodeState decodeState =
            new()
            {
                SlotBackedDecode = plan.Database.Options.SlotBackedDecode || plan.ExecutionFilter is not null,
                BorrowedDecode = ShouldUseBorrowedDecode(plan),
            };
        int batchSize = plan.Database.Options.IndexScanFetchBatchSize;
        List<ObjectIdValue> pageBuf = new(batchSize);

        // Fetch and decode one buffered page; yields rows in the order they appear in the page.
        async IAsyncEnumerable<QueryResultRow> flushPageAsync(List<ObjectIdValue> page)
        {
            ReadOnlyMemory<byte>?[] batchResult = await table.Store.GetRowsBatch(
                ticket.TxnState, page).ConfigureAwait(false);
            for (int i = 0; i < page.Count; i++)
            {
                ObjectIdValue batchRowId = page[i];
                ReadOnlyMemory<byte>? data = batchResult[i];
                if (data is null || data.Value.Length == 0)
                {
                    logger.LogWarning("Row {RowId} found in index {IndexName} but data is missing in table {TableName}", batchRowId, index.Name, table.Name);
                    continue;
                }
                deps?.RecordPoint(table.Store.RowPointKey(batchRowId));
                if (scanStats is not null) scanStats.RowsRead++;
                QueryRow queryRow = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema, txId, batchRowId, data.Value,
            plan.Database.Options,
                    plan.ScanRequiredColumns, visibilityVersion, decodeState).ConfigureAwait(false);
                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    yield return new QueryResultRow(batchRowId, queryRow);
            }
        }

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndex(
            ticket.TxnState,
            index.KvId,
            keyTypes,
            null,
            null,
            unique,
            fromInclusive: true,
            toInclusive: true,
            maxRows: plan.ScanRowLimit))
        {
            rowsScanned++;
            if (scanStats is not null)
                scanStats.KvScanEntries++;

            pageBuf.Add(rowId);

            if (pageBuf.Count < batchSize)
                continue;

            // Page full — flush, then reset for the next page.
            await foreach (QueryResultRow r in flushPageAsync(pageBuf))
                yield return r;
            pageBuf.Clear();
        }

        // Flush the remaining partial page (skipped when the scan yielded a full multiple of batchSize).
        if (pageBuf.Count > 0)
            await foreach (QueryResultRow r in flushPageAsync(pageBuf))
                yield return r;
        }
        finally
        {
            Diagnostics.ServerDiagnostics.RecordScanDuration(
                Diagnostics.ServerDiagnostics.Tags.Scan.IndexRange,
                System.Diagnostics.Stopwatch.GetElapsedTime(scanStart).TotalMilliseconds);
            Diagnostics.ServerDiagnostics.RecordQueryRows(
                Diagnostics.ServerDiagnostics.Tags.Scan.IndexRange,
                Diagnostics.ServerDiagnostics.Tags.Stage.Scanned, rowsScanned);
        }
    }

    /// <summary>
    /// Layout for an index-only (covering) scan, precomputed once per scan.
    /// <list type="bullet">
    ///   <item><see cref="SlotMap"/> — per output column, where its value comes from: a value
    ///     <c>&gt;= 0</c> is a position into the decoded index <b>key</b>; a value <c>&lt; 0</c> is an
    ///     INCLUDE column (its value comes from the entry-value tuple, filled by the include decode
    ///     plan below).</item>
    ///   <item><see cref="IncludeTypes"/> / <see cref="IncludeOutputByPosition"/> — the include decode
    ///     plan, truncated to the last <i>projected</i> include position. For include position <c>p</c>,
    ///     <c>IncludeOutputByPosition[p]</c> is the output index to write, or <c>-1</c> to skip that
    ///     column entirely (it is not projected). This lets a scan decode only the included columns a
    ///     query actually uses instead of the whole tuple.</item>
    ///   <item><see cref="HasIncludeSlots"/> — true iff at least one output column is sourced from the
    ///     tuple; when false the scan never touches the include payload even if the index has INCLUDE
    ///     columns.</item>
    /// </list>
    /// </summary>
    internal readonly record struct IndexOnlyLayout(
        RowLayout Layout,
        int[] SlotMap,
        ColumnType[] IncludeTypes,
        int[] IncludeOutputByPosition,
        bool HasIncludeSlots);

    private static int EncodeIncludeSlot(int includePosition) => -(includePosition + 1);

    private static bool IsIncludeSlot(int slot) => slot < 0;

    private static int DecodeIncludeSlot(int slot) => -slot - 1;

    /// <summary>
    /// Builds the <see cref="IndexOnlyLayout"/> for a covering scan, including the include decode plan.
    /// Column names are ordered by their position in the table schema (matching
    /// <see cref="RowEncoder.DecodeToQueryRowAsync"/> output order) and filtered to those in
    /// <paramref name="required"/>. Each required column resolves to a key slot or an INCLUDE slot; a
    /// covering scan is only planned when every required column is a key or included column
    /// (<c>TryMarkIndexOnly</c>), so an unresolved column is an invariant violation and throws. The KV
    /// row id is never used as a value source — the logical <c>id</c> column is user-supplied and need
    /// not equal the KV row key. Called from <see cref="QueryExecutor"/> for the range-scan and
    /// unique-lookup paths.
    /// </summary>
    internal static IndexOnlyLayout BuildIndexOnlyLayout(
        TableDescriptor table,
        IReadOnlySet<string>? required,
        TableIndexSchema index)
    {
        List<string> names = [];
        List<int> slots = [];

        // Include decode plan over ALL the index's include positions; entries default to -1 (skip).
        int[] includeOutputByPosition = new int[index.IncludeColumns.Length];
        Array.Fill(includeOutputByPosition, -1);
        int maxProjectedIncludePos = -1;

        if (table.Schema.Columns is not null)
        {
            foreach (TableColumnSchema col in table.Schema.Columns)
            {
                if (required is not null && !required.Contains(col.Name))
                    continue;

                int outputIndex = names.Count;
                names.Add(col.Name);

                int keyPos = Array.IndexOf(index.Columns, col.Name);
                if (keyPos >= 0)
                {
                    slots.Add(keyPos);
                    continue;
                }

                int includePos = Array.IndexOf(index.IncludeColumns, col.Name);
                if (includePos >= 0)
                {
                    slots.Add(EncodeIncludeSlot(includePos));
                    includeOutputByPosition[includePos] = outputIndex;
                    if (includePos > maxProjectedIncludePos)
                        maxProjectedIncludePos = includePos;
                    continue;
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Covering scan required column '{col.Name}' is not a key or INCLUDE column of index '{index.Name}'");
            }
        }

        // Truncate the include plan to the last projected position: positions beyond it are never
        // read nor skipped (nothing after them is projected), so the tuple pass stops early. When no
        // include column is projected the plan is empty and the tuple is never decoded at all.
        int planLength = maxProjectedIncludePos + 1;
        ColumnType[] includeTypes;
        int[] planOutputs;
        if (planLength == 0)
        {
            includeTypes = [];
            planOutputs = [];
        }
        else
        {
            includeTypes = ResolveIncludeTypes(table, index, planLength);
            planOutputs = includeOutputByPosition[..planLength];
        }

        return new IndexOnlyLayout(
            RowLayout.ForColumns(names),
            slots.ToArray(),
            includeTypes,
            planOutputs,
            HasIncludeSlots: planLength > 0);
    }

    /// <summary>
    /// Resolves the <see cref="ColumnType"/> of the first <paramref name="count"/> INCLUDE columns of
    /// the index, in include order, so the entry-value tuple can be decoded/skipped position-by-position.
    /// </summary>
    private static ColumnType[] ResolveIncludeTypes(TableDescriptor table, TableIndexSchema index, int count)
    {
        ColumnType[] types = new ColumnType[count];
        for (int i = 0; i < count; i++)
        {
            TableColumnSchema? col = table.Schema.Columns?.Find(c => string.Equals(c.Name, index.IncludeColumns[i], StringComparison.OrdinalIgnoreCase));
            if (col is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"INCLUDE column '{index.IncludeColumns[i]}' of index '{index.Name}' not found in table schema");
            types[i] = col.Type;
        }
        return types;
    }

    /// <summary>
    /// Synthesizes the <see cref="ColumnValue"/> array for one covering-scan row: key-sourced output
    /// columns are read from <paramref name="decodedKey"/>, and — only when the layout has INCLUDE slots
    /// — the projected included columns are decoded straight from <paramref name="includeTuple"/> in a
    /// single pass (see <see cref="IndexIncludeValueCodec.DecodeTupleInto"/>), skipping unprojected
    /// included columns and never materializing an intermediate composite. A short/absent tuple leaves
    /// missing projected columns as <see cref="ColumnValue.Null"/>. The KV row id is never a value source.
    /// </summary>
    internal static ColumnValue[] SynthesizeCoveredValues(
        in IndexOnlyLayout layout,
        CompositeColumnValue decodedKey,
        ReadOnlySpan<byte> includeTuple)
    {
        int[] slotMap = layout.SlotMap;
        ColumnValue[] values = new ColumnValue[slotMap.Length];

        for (int i = 0; i < slotMap.Length; i++)
        {
            int slot = slotMap[i];
            if (!IsIncludeSlot(slot))
                values[i] = decodedKey.Values[slot];
        }

        if (layout.HasIncludeSlots)
            IndexIncludeValueCodec.DecodeTupleInto(layout.IncludeTypes, layout.IncludeOutputByPosition, values, includeTuple);

        return values;
    }

    private static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
        {
            string colName = index.Columns[i];
            TableColumnSchema? col = table.Schema.Columns?.Find(c => string.Equals(c.Name, colName, StringComparison.OrdinalIgnoreCase));
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }

}
