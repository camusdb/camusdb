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
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Joins by probing an index on the right table once per left row, instead of re-scanning it.
/// The planner picks it when the join key is the leading column of a right-side index.
///
/// <para>Both probe shapes run a page of outer rows at a time (<see cref="ProbeBatch"/>): the page's
/// distinct keys are read with one batched call of the index space (a point probe for a unique index,
/// a bounded seek per key for a non-unique one) and the rows found with one batched read of the row
/// space, instead of one or two store calls per outer row. A non-unique key with more entries than one
/// probe page is streamed through the per-key equality scan instead, so a page of outer rows never
/// materializes more than one page of entries per key; that stream fetches its rows through one
/// <see cref="JoinLeafRowPage"/> that lives for the whole join and is flushed before the stream
/// returns, so it never spans two left rows.</para>
///
/// <para>One <see cref="RightDecodeState"/> is created per execution and threaded through every
/// probe, so the join owns exactly one row-decode plan, one alias-qualified layout and one
/// large-value fetch no matter which fetch path a row takes. Everything else a probe needs that is
/// fixed for the join (the key column types, the stop direction) is resolved once
/// before the outer loop: a join runs one probe per outer row, and the per-probe allocation is
/// what sets its cost on a large join.</para>
///
/// <para>A NULL lookup value is never probed: NULL = NULL is unknown, and a non-unique index walk
/// from a NULL key would otherwise pair the row with every NULL-keyed right row. An inner join
/// drops the row; a left outer join pads it, as it pads any left row whose probe returned nothing
/// the <c>ON</c> predicate accepted.</para>
/// </summary>
internal sealed class IndexNestedLoopJoinOperator
{
    private readonly JoinExecutionServices services;

    private readonly IJoinNodeExecutor tree;

    public IndexNestedLoopJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree)
    {
        this.services = services;
        this.tree = tree;
    }

    /// <summary>
    /// What the pairing of a left row with a right row needs and keeps for the whole join: the pairer,
    /// and the join layout and right ordinal map built from the first pair (or bound per left row by
    /// the outer-join padding). Held in one object so the unique and the non-unique probe share one
    /// pairing routine without copying these back into the outer loop.
    /// </summary>
    private sealed class PairState
    {
        internal readonly JoinRowPairer Pairer = new();
        internal RowLayout? JoinLayout;
        internal Dictionary<string, int>? RightOrdinalMap;
    }

    /// <summary>
    /// The rows a join's probe memo may hold in all, an empty result counting as one. An outer
    /// stream often repeats a key (every cast row of one movie probes the same movie id), and a
    /// repeated probe would read the same entries and rows again at the full cost of a probe. The
    /// memo keeps the right rows of each key for the rest of the join; the cap bounds the memory
    /// of a join whose keys do not repeat, which gains nothing from it.
    /// </summary>
    internal const int ProbeMemoMaxRows = 65_536;

    /// <summary>
    /// The right rows of each probe key the join has seen, after the right-side filter and before
    /// the <c>ON</c> predicate, which is evaluated for each pair because it can name left columns.
    /// Valid for the whole join: the join reads at one snapshot, and the right rows of a key are the
    /// same for every outer row that carries the key. A probe is memoized only when it ran to its
    /// end; a consumer that stops reading in the middle of a probe leaves no partial entry.
    /// </summary>
    private sealed class ProbeMemo(int capacity)
    {
        private readonly Dictionary<ColumnValue, List<QueryResultRow>> rows = new(SqlColumnValueComparer.Instance);
        private int held;

        public bool TryGet(ColumnValue key, out List<QueryResultRow> result) => rows.TryGetValue(key, out result!);

        public bool HasRoom => held < capacity;

        public void Add(ColumnValue key, List<QueryResultRow> result)
        {
            rows[key] = result;
            held += Math.Max(1, result.Count);
        }
    }

    /// <summary>
    /// A page of outer rows of an index probe, kept in outer order until their keys are probed
    /// together. Each outer row records what it needs: a NULL key (never probed), the rows the memo
    /// answered, or the slot of its key among the batch's distinct keys. A key that repeats inside the
    /// batch takes the slot of its first outer row, so the batch probes each distinct key once; the
    /// repeat counts as a memo hit, because the probe is shared exactly as the memo would share it one
    /// batch later. The page size bounds both the outer rows held and the keys of one store call.
    /// </summary>
    private sealed class ProbeBatch(int capacity)
    {
        /// <summary>
        /// One outer row of the batch: <see cref="Rows"/> when the memo answered it, else the key's
        /// <see cref="Slot"/>, or -1 for a NULL key.
        /// </summary>
        internal readonly record struct Outer(IReadOnlyDictionary<string, ColumnValue> Left, List<QueryResultRow>? Rows, int Slot);

        internal readonly List<Outer> Outers = new(capacity);

        /// <summary>The distinct keys to probe, in the form the store takes and in the form the memo keys them.</summary>
        internal readonly List<CompositeColumnValue> Keys = new(capacity);

        internal readonly List<ColumnValue> KeyValues = new(capacity);

        private readonly Dictionary<ColumnValue, int> slotOf = new(capacity, SqlColumnValueComparer.Instance);

        internal int Count => Outers.Count;

        internal bool IsFull => Outers.Count >= capacity;

        internal void Add(IReadOnlyDictionary<string, ColumnValue> left, ColumnValue lookupValue, ProbeMemo? memo, PlanNodeStats? stats)
        {
            if (lookupValue.Type == ColumnType.Null)
            {
                Outers.Add(new(left, null, -1));
                return;
            }

            if (memo is not null && memo.TryGet(lookupValue, out List<QueryResultRow> remembered))
            {
                if (stats is not null)
                    stats.ProbeMemoHits++;

                Outers.Add(new(left, remembered, -1));
                return;
            }

            if (slotOf.TryGetValue(lookupValue, out int slot))
            {
                if (stats is not null)
                    stats.ProbeMemoHits++;

                Outers.Add(new(left, null, slot));
                return;
            }

            slot = Keys.Count;
            slotOf[lookupValue] = slot;
            Keys.Add(new CompositeColumnValue(new[] { lookupValue }));
            KeyValues.Add(lookupValue);
            Outers.Add(new(left, null, slot));
        }

        internal void Clear()
        {
            Outers.Clear();
            Keys.Clear();
            KeyValues.Clear();
            slotOf.Clear();
        }
    }

    /// <summary>The rows of a key with no row, shared by every such key: a memo entry reads it and never writes it.</summary>
    private static readonly List<QueryResultRow> NoRows = [];

    internal async IAsyncEnumerable<QueryResultRow> ExecuteIndexNestedLoopJoin(
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan)
    {
        string rightAlias = joinNode.RightSource.Alias;
        RowLayout? qualifiedLeftLayout = null;
        PairState pairing = new();
        RightDecodeState rightDecodeState = new();
        OuterJoinPadding? padding = joinNode.Kind == JoinKind.LeftOuter
            ? await OuterJoinPadding.CreateAsync(BoundJoinRightSource.FromTable(joinNode.RightSource), plan).ConfigureAwait(false)
            : null;

        // Both probe shapes fetch their primary rows through one page that lives for the whole join, not
        // one per outer row: the page and its decode plan would otherwise be rebuilt for every probe,
        // which is pure overhead for the common one-match probe. A non-unique probe streams through the
        // page's buffer; a unique probe batch fetches by position. The page shares rightDecodeState so
        // the join owns exactly one decode plan and one qualified layout.
        JoinLeafRowPage probePage = new(services, plan, joinNode.RightSource, joinNode.RightExecutionFilter, rightDecodeState, joinNode.Stats);

        // Fixed for the whole join, so resolved once and not per outer row: the column types of the
        // index key. A probe asks for the store's default page: the store stops a bounded scan at
        // the key, so a page costs the entries of the key only, and the largest page is the fewest
        // round trips for a key with many entries.
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(joinNode.RightSource.Table, joinNode.Index);
        int probePageSize = KvStoreConstants.DefaultPageSize;

        int memoCapacity = services.Statistics?.IndexProbeMemoCapacityForTesting ?? ProbeMemoMaxRows;
        ProbeMemo? memo = memoCapacity > 0 ? new ProbeMemo(memoCapacity) : null;

        // The join collects a page of outer rows and probes their distinct keys with batched reads. The
        // page is the one every paged row fetch uses, so one store call never carries more keys than one
        // row page.
        ProbeBatch batch = new(Math.Max(1, services.Options.IndexScanFetchBatchSize));

        await foreach (QueryResultRow leftRow in tree.ExecuteNode(joinNode.Input!, plan).ConfigureAwait(false))
        {
            // Qualify the left row: reuse the source Values array when it is a QueryRow to avoid
            // a per-row Dictionary allocation — only the layout (key names) changes.
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

            if (!leftQualified.TryGetValue(joinNode.LeftLookupColumn, out ColumnValue? lookupValue) || lookupValue is null)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Join lookup column '{joinNode.LeftLookupColumn}' is missing from left row");
            }

            batch.Add(leftQualified, lookupValue, memo, joinNode.Stats);

            if (batch.IsFull)
            {
                await foreach (QueryResultRow joined in FlushProbeBatch(batch, joinNode, plan, pairing, padding, rightAlias, memo, probePage, keyTypes, probePageSize).ConfigureAwait(false))
                    yield return joined;
            }
        }

        if (batch.Count > 0)
        {
            await foreach (QueryResultRow joined in FlushProbeBatch(batch, joinNode, plan, pairing, padding, rightAlias, memo, probePage, keyTypes, probePageSize).ConfigureAwait(false))
                yield return joined;
        }
    }

    /// <summary>
    /// Probes the distinct keys of a batch and pairs every outer row of the batch, in outer order, with
    /// the rows its key found: one index read and one row read for the batch, then the <c>ON</c>
    /// predicate per pair, and outer-join padding for an outer row no pair satisfies. A non-unique key
    /// with more entries than one probe page has no batched answer; the first outer row that carries it
    /// streams it through the per-key equality scan, and the rows it collected serve the later outer
    /// rows of the same batch and the memo. The batch is emptied for the next page of outer rows.
    /// </summary>
    private async IAsyncEnumerable<QueryResultRow> FlushProbeBatch(
        ProbeBatch batch,
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan,
        PairState pairing,
        OuterJoinPadding? padding,
        string rightAlias,
        ProbeMemo? memo,
        JoinLeafRowPage page,
        ColumnType[] keyTypes,
        int probePageSize)
    {
        List<QueryResultRow>?[]? probed = null;

        if (batch.Keys.Count > 0)
        {
            probed = joinNode.UseUniqueLookup
                ? await ProbeUniqueKeys(batch, joinNode, plan, page, memo).ConfigureAwait(false)
                : await ProbeNonUniqueKeys(batch, joinNode, plan, page, memo, keyTypes, probePageSize).ConfigureAwait(false);
        }

        foreach (ProbeBatch.Outer outer in batch.Outers)
        {
            if (padding is not null)
                (pairing.JoinLayout, pairing.RightOrdinalMap) = padding.Bind(outer.Left);

            List<QueryResultRow>? rows = outer.Rows ?? (outer.Slot >= 0 ? probed![outer.Slot] : null);
            bool matched = false;

            if (rows is null && outer.Slot >= 0)
            {
                // A key wider than one probe page: stream it, as the per-key probe always did. The rows
                // are kept for the memo while it has room, and then also for the later outer rows of
                // this batch that carry the same key.
                List<QueryResultRow>? collected = memo is { HasRoom: true } ? [] : null;

                await foreach (QueryResultRow rightRow in ScanMultiIndexRightRows(
                    joinNode, batch.Keys[outer.Slot], plan, page, keyTypes, probePageSize).ConfigureAwait(false))
                {
                    collected?.Add(rightRow);
                    QueryRow? merged = await PairIfAccepted(pairing, joinNode, plan, outer.Left, rightRow.Row, rightAlias).ConfigureAwait(false);

                    if (merged is null)
                        continue;

                    matched = true;
                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                if (collected is not null)
                {
                    memo!.Add(batch.KeyValues[outer.Slot], collected);
                    probed![outer.Slot] = collected;
                }

                if (padding is not null && !matched)
                    yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(outer.Left));

                continue;
            }

            if (rows is not null)
            {
                foreach (QueryResultRow rightRow in rows)
                {
                    QueryRow? merged = await PairIfAccepted(pairing, joinNode, plan, outer.Left, rightRow.Row, rightAlias).ConfigureAwait(false);

                    if (merged is null)
                        continue;

                    matched = true;
                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }

            if (padding is not null && !matched)
                yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(outer.Left));
        }

        batch.Clear();
    }

    /// <summary>
    /// The two store reads of a non-unique probe batch: the entries of every distinct key with one batched
    /// equality scan of the index space (<see cref="KvTableStore.ScanIndexEqualityManyAsync"/>), then the
    /// rows of every entry found with one batched row fetch through <paramref name="page"/>, which applies
    /// the right-side filter. Answers the rows of each key by slot, in index order. A key with more entries
    /// than one probe page answers null: its first page is dropped, uncounted, and the flush streams the
    /// whole key instead, so a batch never holds more than one page of entries per key. Each answered key
    /// enters the memo while the memo has room. The batch counts one entry per row id in
    /// <see cref="PlanNodeStats.KvScanEntries"/> and one call in <see cref="PlanNodeStats.ProbeBatches"/>.
    /// </summary>
    private static async Task<List<QueryResultRow>?[]> ProbeNonUniqueKeys(
        ProbeBatch batch,
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan,
        JoinLeafRowPage page,
        ProbeMemo? memo,
        ColumnType[] keyTypes,
        int probePageSize)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        QueryDependencyCollector? deps = plan.DepCollector;
        int count = batch.Keys.Count;

        // The index bucket range catches phantom inserts for every probed key; the row point deps are
        // recorded per row id below, before the fetch, as the per-key probe records them.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        KvIndexAccessor.IndexEqualityProbe[] probes = await table.Store.ScanIndexEqualityManyAsync(
            plan.Ticket.TxnState, joinNode.Index.KvId, keyTypes, batch.Keys, probePageSize, plan.Ticket.CancellationToken).ConfigureAwait(false);

        PlanNodeStats? stats = joinNode.Stats;
        if (stats is not null)
            stats.ProbeBatches++;

        List<ObjectIdValue> found = [];
        List<int> foundSlot = [];
        List<QueryResultRow>?[] rowsOf = new List<QueryResultRow>?[count];

        for (int slot = 0; slot < count; slot++)
        {
            KvIndexAccessor.IndexEqualityProbe probe = probes[slot];

            if (probe.HasMore)
                continue;

            rowsOf[slot] = probe.RowIds.Count == 0 ? NoRows : new List<QueryResultRow>(probe.RowIds.Count);

            foreach (ObjectIdValue rowId in probe.RowIds)
            {
                if (stats is not null)
                    stats.KvScanEntries++;

                deps?.RecordPoint(table.Store.RowPointKey(rowId));
                found.Add(rowId);
                foundSlot.Add(slot);
            }
        }

        if (found.Count > 0)
        {
            QueryResultRow?[] fetched = await page.FetchAsync(found, plan.Ticket.CancellationToken).ConfigureAwait(false);

            for (int j = 0; j < found.Count; j++)
            {
                if (fetched[j] is { } row)
                    rowsOf[foundSlot[j]]!.Add(row);
            }
        }

        if (memo is not null)
        {
            for (int slot = 0; slot < count; slot++)
            {
                if (rowsOf[slot] is { } rows && memo.HasRoom)
                    memo.Add(batch.KeyValues[slot], rows);
            }
        }

        return rowsOf;
    }

    /// <summary>
    /// The two store reads of a unique probe batch: the row id of every distinct key with one batched
    /// index read, then the rows found with one batched row fetch through <paramref name="page"/>,
    /// which applies the right-side filter. Answers the rows of each key by slot: one row, or none when
    /// the key has no entry or the filter rejects its row. Each key's answer enters the memo while the
    /// memo has room, as a single probe's answer does. The batch counts one probe per distinct key in
    /// <see cref="PlanNodeStats.KvPointLookups"/> and one call in <see cref="PlanNodeStats.ProbeBatches"/>.
    /// </summary>
    private static async Task<List<QueryResultRow>?[]> ProbeUniqueKeys(
        ProbeBatch batch,
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan,
        JoinLeafRowPage page,
        ProbeMemo? memo)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        QueryDependencyCollector? deps = plan.DepCollector;
        int count = batch.Keys.Count;

        // Unique index probe: record the index bucket range (membership check) and schema.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        if (joinNode.Stats is { } stats)
        {
            stats.KvPointLookups += count;
            stats.ProbeBatches++;
        }

        ObjectIdValue?[] rowIds = await table.Store.LookupUniqueRowIdsAsync(
            plan.Ticket.TxnState, joinNode.Index.KvId, batch.Keys, plan.Ticket.CancellationToken).ConfigureAwait(false);

        List<ObjectIdValue> found = new(count);
        List<int> foundSlot = new(count);

        for (int slot = 0; slot < count; slot++)
        {
            if (rowIds[slot] is not { } rowId)
                continue;

            // Record the row point dep whether or not it passes the execution filter — a later
            // update to a non-indexed column could change what the join returns.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));
            found.Add(rowId);
            foundSlot.Add(slot);
        }

        List<QueryResultRow>?[] rowsOf = new List<QueryResultRow>?[count];

        if (found.Count > 0)
        {
            QueryResultRow?[] fetched = await page.FetchAsync(found, plan.Ticket.CancellationToken).ConfigureAwait(false);

            for (int j = 0; j < found.Count; j++)
            {
                if (fetched[j] is { } row)
                    rowsOf[foundSlot[j]] = [row];
            }
        }

        for (int slot = 0; slot < count; slot++)
        {
            rowsOf[slot] ??= NoRows;

            if (memo is { HasRoom: true })
                memo.Add(batch.KeyValues[slot], rowsOf[slot]!);
        }

        return rowsOf;
    }

    /// <summary>
    /// Pairs one left row with one right row and evaluates the <c>ON</c> predicate on the pair: the
    /// joined row when the predicate accepts it, null when it rejects it. The join layout and the
    /// right ordinal map are built from the first pair and kept in <paramref name="pairing"/>. The
    /// predicate evaluation completes synchronously unless it holds a correlated subquery, and then
    /// this method completes without a task of its own.
    /// </summary>
    private async ValueTask<QueryRow?> PairIfAccepted(
        PairState pairing,
        IndexNestedLoopJoinNode joinNode,
        QueryPlan plan,
        IReadOnlyDictionary<string, ColumnValue> leftQualified,
        IReadOnlyDictionary<string, ColumnValue> rightRow,
        string rightAlias)
    {
        pairing.JoinLayout      ??= QueryRowMerger.BuildJoinLayout(leftQualified, rightRow, rightAlias);
        pairing.RightOrdinalMap ??= QueryRowMerger.BuildRightKeyOrdinalMap(rightRow, rightAlias, pairing.JoinLayout);
        QueryRow merged = pairing.Pairer.Pair(leftQualified, rightRow, pairing.JoinLayout, pairing.RightOrdinalMap);

        if (!await services.Filterer.MeetWhereAsync(joinNode.OnPredicate, merged, plan.Ticket, plan.Database).ConfigureAwait(false))
            return null;

        return merged;
    }

    /// <summary>
    /// Non-unique index probe for one outer row, used for a key with more entries than one probe page
    /// (<see cref="ProbeNonUniqueKeys"/> answers the others in a batch): walks the entries of the lookup
    /// key through the store's equality scan (<see cref="KvTableStore.ScanIndexEquality"/>), which bounds
    /// the scan at the key in either index direction and decodes no entry, and fetches the matching
    /// primary rows one page at a time through <paramref name="probePage"/> instead of one round trip per entry.
    /// <para>
    /// The page is owned by the whole join execution, but it is flushed before this method returns,
    /// so a page never spans two outer rows and the rows of one outer row still stream in index order.
    /// The row point dependency is recorded before the fetch, exactly as the per-entry fetch did.
    /// </para>
    /// <para>
    /// <paramref name="keyTypes"/> and <paramref name="pageSize"/> are properties of the join,
    /// resolved once by the caller: a probe runs per outer row and must not rebuild them.
    /// </para>
    /// <para>
    /// Cancellation is checked on every index entry here and on every row inside the page flush,
    /// because the storage enumerator does not observe the token on its own.
    /// </para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ScanMultiIndexRightRows(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        JoinLeafRowPage probePage,
        ColumnType[] keyTypes,
        int pageSize)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        QueryDependencyCollector? deps = plan.DepCollector;
        CancellationToken cancellationToken = plan.Ticket.CancellationToken;

        // Non-unique index equality probe: the index bucket range catches phantom inserts for
        // this key. Per-row point deps are recorded below as each row id is buffered.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanIndexEquality(
            plan.Ticket.TxnState,
            joinNode.Index.KvId,
            keyTypes,
            lookupKey,
            pageSize,
            cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (joinNode.Stats is { } stats)
                stats.KvScanEntries++;

            // Recorded before the fetch, per row id: a later update to a non-indexed projected column
            // must invalidate this result even if the residual filter later rejects the row.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            probePage.Add(rowId);

            if (!probePage.IsFull)
                continue;

            await foreach (QueryResultRow row in probePage.FlushAsync(cancellationToken).ConfigureAwait(false))
                yield return row;
        }

        // The tail of this outer row's matches. Flushing here, not on the next probe, is what keeps a
        // page from spanning two outer rows.
        if (probePage.Count > 0)
        {
            await foreach (QueryResultRow row in probePage.FlushAsync(cancellationToken).ConfigureAwait(false))
                yield return row;
        }
    }
}
