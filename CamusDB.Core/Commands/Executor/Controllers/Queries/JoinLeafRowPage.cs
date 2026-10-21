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
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Buffers the row ids an index walk produces and resolves each full page with one batch fetch,
/// instead of one round trip per index entry. Used by the three join leaf scans and by the
/// non-unique probe of an index nested-loop join (<see cref="IndexNestedLoopJoinOperator.ScanMultiIndexRightRows"/>);
/// the join's unique probe fetches the rows of a probe batch through <see cref="FetchAsync"/>, which
/// answers by position and does not use the buffer.
/// <para>
/// Index order survives the paging: the page is filled in scan order and the batch result is read
/// positionally, so rows decode and yield in scan order, and a row the batch reports as absent is
/// skipped exactly as the per-entry fetch skipped it. Duplicate row ids in one page are fine —
/// each position resolves independently.
/// </para>
/// <para>
/// The fetch goes through <see cref="KvTableStore.GetRowsBatchLockedForMutation"/> and not the
/// plain batch read. A join leaf takes no range lock, so under Serializable read-write the shared
/// point lock the per-entry read acquired on every row key is the only protection the join has,
/// and that lock set must stay exactly what it was. Under every other isolation level and mode
/// that helper acquires nothing, so a page costs one round trip.
/// </para>
/// <para>
/// Lock timing under Serializable read-write: the per-entry read locked a row the moment the
/// consumer asked for it; a page locks every row it holds when it is flushed, and a probe batch
/// (<see cref="FetchAsync"/>) locks every row of its page of outer keys before the first of them
/// is returned. After a fully consumed scan the lock set is identical. When the consumer stops
/// early (LIMIT, cancellation) the page can hold up to page-size minus one shared point locks on
/// rows the consumer never saw, and a probe batch up to one probe page of rows per outer key of
/// its page. This is accepted rather than gated: the extra locks sit only on rows the index
/// matched for this join, never on unrelated keys; a shared lock blocks no reader and only forces
/// a concurrent writer of those rows to retry; and every other isolation level and mode locks
/// nothing, so the exposure is bounded by <see cref="CamusDBOptions.IndexScanFetchBatchSize"/>
/// outer keys times one probe page and limited to the one mode that already holds every row it
/// reads until commit.
/// </para>
/// <para>
/// The buffer is emptied on every exit from <see cref="FlushAsync"/>, including an abandoned or
/// faulted enumeration, so a page reused across probes can never carry a stale id into the next
/// outer row.
/// </para>
/// </summary>
internal sealed class JoinLeafRowPage
{
    private readonly JoinExecutionServices services;
    private readonly QueryPlan plan;
    private readonly BoundTableSource source;
    private readonly NodeAst? executionFilter;
    private readonly IReadOnlySet<string>? required;
    private readonly int schemaVersion;
    private readonly HLCTimestamp txId;
    private readonly CamusDBOptions options;
    private readonly RightDecodeState decode;
    private readonly List<ObjectIdValue> rowIds;
    private readonly int pageSize;
    private readonly PlanNodeStats? stats;

    // The index the ids came from and, positionally with rowIds, the entry key each id was read
    // under, so a fetched row is checked against its entry (see IndexEntryCheck). Null for a page
    // that is not fed by an index, or for an id added without its key.
    private readonly TableIndexSchema? index;
    private readonly List<CompositeColumnValue?> entryKeys;

    /// <param name="decode">
    /// The decode plan and alias-qualified layout to use. A caller that also decodes right rows
    /// elsewhere passes its own state so the join builds one plan and one layout, not two; a
    /// leaf scan passes nothing and the page owns a fresh one.
    /// </param>
    /// <param name="stats">
    /// The counters each fetched row is charged to under EXPLAIN (ANALYZE): the leaf's own node for a
    /// leaf scan, the join node for an index nested-loop probe. Null otherwise. The caller counts
    /// the index entries it walks; the page counts the rows the fetch returned.
    /// </param>
    internal JoinLeafRowPage(
        JoinExecutionServices services,
        QueryPlan plan,
        BoundTableSource source,
        NodeAst? executionFilter,
        RightDecodeState? decode = null,
        PlanNodeStats? stats = null,
        TableIndexSchema? index = null)
    {
        this.services = services;
        this.plan = plan;
        this.source = source;
        this.executionFilter = executionFilter;
        this.decode = decode ?? new RightDecodeState();
        this.stats = stats;
        this.index = index;

        // Pinned once for the whole scan, like every other reader of the published options record.
        options = services.Options;
        required = JoinAliasMetadata.GetRequiredColumnsForAlias(plan, source.Alias);
        if (index is not null)
            required = IndexEntryCheck.RequiredColumns(required, index);
        schemaVersion = JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias);
        txId = plan.Ticket.TxnState.TransactionId;

        // A non-positive configured size degrades to one row per fetch instead of an unbounded buffer.
        pageSize = Math.Max(1, options.IndexScanFetchBatchSize);
        rowIds = new List<ObjectIdValue>(pageSize);
        entryKeys = new List<CompositeColumnValue?>(pageSize);
    }

    internal int Count => rowIds.Count;

    internal bool IsFull => rowIds.Count >= pageSize;

    /// <summary>
    /// Buffers a row id. <paramref name="entryKey"/> is the key of the index entry the id was read
    /// under, when the page was built for an index; the fetched row is checked against it.
    /// </summary>
    internal void Add(ObjectIdValue rowId, CompositeColumnValue? entryKey = null)
    {
        rowIds.Add(rowId);
        entryKeys.Add(entryKey);
    }

    /// <summary>
    /// Fetches the buffered page in one call, then yields the rows that survive the residual
    /// filter, in page order. The buffer is emptied when the enumeration ends for any reason:
    /// full consumption, early disposal by the consumer, or a fault in the fetch or decode.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> FlushAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (rowIds.Count == 0)
            yield break;

        try
        {
            ReadOnlyMemory<byte>?[] batch = await FetchBytesAsync(rowIds, cancellationToken).ConfigureAwait(false);

            for (int i = 0; i < rowIds.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                QueryResultRow? row = await DecodeAcceptedAsync(rowIds[i], batch[i], entryKeys[i]).ConfigureAwait(false);

                if (row is { } accepted)
                    yield return accepted;
            }
        }
        finally
        {
            rowIds.Clear();
            entryKeys.Clear();
        }
    }

    /// <summary>
    /// Fetches <paramref name="ids"/> in one call and answers one slot per id, in input order: the
    /// decoded row when the store holds it and the residual filter accepts it, null otherwise. The
    /// batched unique probe of an index nested-loop join uses it: the ids of a probe batch belong to
    /// distinct index keys, and each row must go back to its key, which a stream of the surviving rows
    /// cannot tell. The page's own buffer is not touched, so a caller may use both in one join.
    /// </summary>
    internal async Task<QueryResultRow?[]> FetchAsync(IReadOnlyList<ObjectIdValue> ids, CancellationToken cancellationToken)
    {
        QueryResultRow?[] rows = new QueryResultRow?[ids.Count];

        if (ids.Count == 0)
            return rows;

        ReadOnlyMemory<byte>?[] batch = await FetchBytesAsync(ids, cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < ids.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            rows[i] = await DecodeAcceptedAsync(ids[i], batch[i]).ConfigureAwait(false);
        }

        return rows;
    }

    private Task<ReadOnlyMemory<byte>?[]> FetchBytesAsync(IReadOnlyList<ObjectIdValue> ids, CancellationToken cancellationToken)
    {
        TableDescriptor table = source.Table;

        return table.Store.GetRowsBatchLockedForMutation(
            plan.Ticket.TxnState, ids, cancellationToken, decode.FetchFor(table.Schema, required));
    }

    /// <summary>
    /// Decodes one fetched row and applies the residual filter: the row when the store answered bytes
    /// and the filter accepts it, null otherwise. Every row the store answered counts as read, whether or
    /// not the filter keeps it.
    /// </summary>
    private async ValueTask<QueryResultRow?> DecodeAcceptedAsync(ObjectIdValue rowId, ReadOnlyMemory<byte>? dataOpt, CompositeColumnValue? entryKey = null)
    {
        if (dataOpt is null || dataOpt.Value.Length == 0)
            return null;

        TableDescriptor table = source.Table;

        QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
            table.Schema,
            txId,
            rowId,
            dataOpt.Value,
            options,
            required,
            schemaVersion,
            decode.DecodeState).ConfigureAwait(false);

        plan.Ticket.Probe?.AddRowRead();
        if (stats is not null)
            stats.RowsRead++;

        // The entry answered the seek; the row must still hold its key (see IndexEntryCheck).
        if (index is not null && entryKey is not null
            && !IndexEntryCheck.Confirms(table, index, entryKey, row, services.Logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<ICamusDB>.Instance))
            return null;

        if (executionFilter is not null)
        {
            decode.QualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
            IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, decode.QualifiedLayout);

            if (!await services.Filterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                return null;
        }

        return new QueryResultRow(rowId, row);
    }
}

/// <summary>
/// Carries the two pieces of mutable state that the index-nested-loop right-side probe
/// needs to reuse across iterations of the outer left-row loop:
/// <list type="bullet">
///   <item><see cref="DecodeState"/> — the per-probe <see cref="RowEncoder.RowDecodeState"/>,
///   keyed by stored schema version; holds the precomputed row-decode plan built by
///   <see cref="RowEncoder.DecodeToQueryRowAsync"/> for each version encountered, so the plan
///   (layout plus per-column read/skip steps) is built at most once per version instead of on
///   every right-row decode.</item>
///   <item><see cref="QualifiedLayout"/> — the alias-prefixed layout built from the first
///   right <see cref="QueryRow"/>; reused on every subsequent right-row qualify so
///   <see cref="QueryRowMerger.BuildQualifiedLayout"/> is called at most once per join node
///   rather than once per row.</item>
/// </list>
/// One instance is created per <see cref="IndexNestedLoopJoinOperator.ExecuteIndexNestedLoopJoin"/>
/// call and passed into the join's <see cref="JoinLeafRowPage"/>. The one instance serves every
/// right row of the join node, whether the row comes from a unique probe batch
/// (<see cref="JoinLeafRowPage.FetchAsync"/>) or from a non-unique probe's page
/// (<see cref="JoinLeafRowPage.FlushAsync"/>), so the decode plan and the qualified layout are built
/// once per join node and not once per fetch path.
/// </summary>
internal sealed class RightDecodeState
{
    internal readonly RowEncoder.RowDecodeState DecodeState = new();
    internal RowLayout? QualifiedLayout;

    private LargeValueFetch? fetch;
    private bool fetchBuilt;

    /// <summary>
    /// The large-value fetch of the right rows, built on the first call and reused by every later one.
    /// One state serves one join node, so the schema and the required set never change. A fetch per
    /// row was the largest allocation of a unique probe, paid by rows that hold no large value.
    /// </summary>
    internal LargeValueFetch? FetchFor(TableSchema schema, IReadOnlySet<string>? required)
    {
        if (!fetchBuilt)
        {
            fetch = LargeValueFetch.Columns(schema, required);
            fetchBuilt = true;
        }

        return fetch;
    }
}
