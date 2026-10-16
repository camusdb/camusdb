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
/// <para>Two probe shapes share one outer loop. A unique index answers with at most one row and
/// needs no buffer. A non-unique index walks an equality range, and its row fetches are paged
/// through one <see cref="JoinLeafRowPage"/> that lives for the whole join: rebuilding the buffer
/// and its decode plan for every outer row would be pure overhead on the common one-match
/// probe. The page is flushed before each probe returns, so it never spans two left rows.</para>
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

    private readonly JoinLeafScanner scanner;

    public IndexNestedLoopJoinOperator(JoinExecutionServices services, IJoinNodeExecutor tree, JoinLeafScanner scanner)
    {
        this.services = services;
        this.tree = tree;
        this.scanner = scanner;
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

        // A non-unique probe pages its primary-row fetches through one buffer that lives for the whole
        // join, not one per outer row: the buffer and its decode plan would otherwise be rebuilt for
        // every probe, which is pure overhead for the common one-match probe. The page shares
        // rightDecodeState so the join owns exactly one decode plan and one qualified layout.
        JoinLeafRowPage? probePage = joinNode.UseUniqueLookup
            ? null
            : new JoinLeafRowPage(services, plan, joinNode.RightSource, joinNode.RightExecutionFilter, rightDecodeState, joinNode.Stats);

        // Fixed for the whole join, so resolved once and not per outer row: the column types of the
        // index key. A probe asks for the store's default page: the store stops a bounded scan at
        // the key, so a page costs the entries of the key only, and the largest page is the fewest
        // round trips for a key with many entries.
        ColumnType[] keyTypes = JoinAliasMetadata.GetIndexColumnTypes(joinNode.RightSource.Table, joinNode.Index);
        int probePageSize = KvStoreConstants.DefaultPageSize;

        int memoCapacity = services.Statistics?.IndexProbeMemoCapacityForTesting ?? ProbeMemoMaxRows;
        ProbeMemo? memo = memoCapacity > 0 ? new ProbeMemo(memoCapacity) : null;

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

            if (padding is not null)
                (pairing.JoinLayout, pairing.RightOrdinalMap) = padding.Bind(leftQualified);

            if (lookupValue.Type == ColumnType.Null)
            {
                if (padding is not null)
                    yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));

                continue;
            }

            CompositeColumnValue lookupKey = new(new[] { lookupValue });
            bool matched = false;

            if (memo is not null && memo.TryGet(lookupValue, out List<QueryResultRow> remembered))
            {
                // A key an earlier outer row probed: its right rows come from the memo, and the store
                // reads nothing. The ON predicate still runs for each pair, as it does below.
                if (joinNode.Stats is { } hitStats)
                    hitStats.ProbeMemoHits++;

                foreach (QueryResultRow rightRow in remembered)
                {
                    QueryRow? merged = await PairIfAccepted(pairing, joinNode, plan, leftQualified, rightRow.Row, rightAlias).ConfigureAwait(false);

                    if (merged is null)
                        continue;

                    matched = true;
                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }

                if (padding is not null && !matched)
                    yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));

                continue;
            }

            // The rows this probe returns, kept for the memo while it has room. Filled only when the
            // probe runs to its end, so an outer enumeration that stops early leaves no partial entry.
            List<QueryResultRow>? collected = memo is { HasRoom: true } ? [] : null;

            if (joinNode.UseUniqueLookup)
            {
                // A unique index answers with at most one row, so the probe is one awaited call and
                // not an enumeration of its own per outer row.
                QueryResultRow? rightRow = await LookupUniqueRightRow(joinNode, lookupKey, plan, rightDecodeState).ConfigureAwait(false);

                if (rightRow is { } found)
                {
                    collected?.Add(found);
                    QueryRow? merged = await PairIfAccepted(pairing, joinNode, plan, leftQualified, found.Row, rightAlias).ConfigureAwait(false);

                    if (merged is not null)
                    {
                        matched = true;
                        yield return new QueryResultRow(default(ObjectIdValue), merged);
                    }
                }
            }
            else
            {
                await foreach (QueryResultRow rightRow in ScanMultiIndexRightRows(
                    joinNode, lookupKey, plan, probePage!, keyTypes, probePageSize).ConfigureAwait(false))
                {
                    collected?.Add(rightRow);
                    QueryRow? merged = await PairIfAccepted(pairing, joinNode, plan, leftQualified, rightRow.Row, rightAlias).ConfigureAwait(false);

                    if (merged is null)
                        continue;

                    matched = true;
                    yield return new QueryResultRow(default(ObjectIdValue), merged);
                }
            }

            if (collected is not null)
                memo!.Add(lookupValue, collected);

            if (padding is not null && !matched)
                yield return new QueryResultRow(default(ObjectIdValue), padding.Pad(leftQualified));
        }
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
    /// Unique index probe for one outer row: one point read of the index entry and, when it exists,
    /// one row read through <see cref="JoinLeafScanner.LoadRightRow"/>. Returns null when the key has
    /// no entry or the row fails the residual filter. The common case allocates only what the store
    /// needs: no enumerator, and no task when Kahuna answers synchronously.
    /// </summary>
    private async ValueTask<QueryResultRow?> LookupUniqueRightRow(
        IndexNestedLoopJoinNode joinNode,
        CompositeColumnValue lookupKey,
        QueryPlan plan,
        RightDecodeState decodeState)
    {
        BoundTableSource source = joinNode.RightSource;
        TableDescriptor table = source.Table;
        QueryDependencyCollector? deps = plan.DepCollector;

        // Unique index probe: record the index bucket range (membership check) and schema.
        deps?.RecordRange(table.Store.IndexKeySpace(joinNode.Index.KvId));
        deps?.RecordSchema(table.Id, JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias), table.Schema.ContentsGeneration);

        ObjectIdValue? rowId = await table.Store.LookupUnique(plan.Ticket.TxnState, joinNode.Index.KvId, lookupKey, plan.Ticket.CancellationToken).ConfigureAwait(false);

        if (joinNode.Stats is { } stats)
            stats.KvPointLookups++;

        if (rowId is null)
            return null;

        // Record the row point dep whether or not it passes the execution filter — a later
        // update to a non-indexed column could change what the join returns.
        deps?.RecordPoint(table.Store.RowPointKey(rowId.Value));

        return await scanner.LoadRightRow(source, rowId.Value, joinNode.RightExecutionFilter, plan, decodeState, joinNode.Stats).ConfigureAwait(false);
    }

    /// <summary>
    /// Non-unique index probe for one outer row: walks the entries of the lookup key through the
    /// store's equality scan (<see cref="KvTableStore.ScanIndexEquality"/>), which bounds the scan at
    /// the key in either index direction and decodes no entry, and fetches the matching primary rows
    /// one page at a time through <paramref name="probePage"/> instead of one round trip per entry.
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
