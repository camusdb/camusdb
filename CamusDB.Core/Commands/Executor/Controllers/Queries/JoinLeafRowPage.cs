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
/// non-unique probe of an index nested-loop join (<see cref="IndexNestedLoopJoinOperator.ScanMultiIndexRightRows"/>).
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
/// consumer asked for it; a page locks every row it holds when it is flushed. After a fully
/// consumed scan the lock set is identical. When the consumer stops early (LIMIT, cancellation)
/// the page can hold up to page-size minus one shared point locks on rows the consumer never
/// saw. This is accepted rather than gated: the extra locks sit only on rows the index matched
/// for this scan, never on unrelated keys; a shared lock blocks no reader and only forces a
/// concurrent writer of those rows to retry; and every other isolation level and mode locks
/// nothing, so the exposure is bounded by <see cref="CamusDBOptions.IndexScanFetchBatchSize"/>
/// and limited to the one mode that already holds every row it reads until commit.
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

    /// <param name="decode">
    /// The decode plan and alias-qualified layout to use. A caller that also decodes right rows
    /// elsewhere passes its own state so the join builds one plan and one layout, not two; a
    /// leaf scan passes nothing and the page owns a fresh one.
    /// </param>
    internal JoinLeafRowPage(
        JoinExecutionServices services,
        QueryPlan plan,
        BoundTableSource source,
        NodeAst? executionFilter,
        RightDecodeState? decode = null)
    {
        this.services = services;
        this.plan = plan;
        this.source = source;
        this.executionFilter = executionFilter;
        this.decode = decode ?? new RightDecodeState();

        // Pinned once for the whole scan, like every other reader of the published options record.
        options = services.Options;
        required = JoinAliasMetadata.GetRequiredColumnsForAlias(plan, source.Alias);
        schemaVersion = JoinAliasMetadata.GetTableSchemaVersionForAlias(plan, source.Alias);
        txId = plan.Ticket.TxnState.TransactionId;

        // A non-positive configured size degrades to one row per fetch instead of an unbounded buffer.
        pageSize = Math.Max(1, options.IndexScanFetchBatchSize);
        rowIds = new List<ObjectIdValue>(pageSize);
    }

    internal int Count => rowIds.Count;

    internal bool IsFull => rowIds.Count >= pageSize;

    internal void Add(ObjectIdValue rowId) => rowIds.Add(rowId);

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
            TableDescriptor table = source.Table;

            ReadOnlyMemory<byte>?[] batch = await table.Store.GetRowsBatchLockedForMutation(
                plan.Ticket.TxnState, rowIds, cancellationToken, LargeValueFetch.Columns(table.Schema, required)).ConfigureAwait(false);

            for (int i = 0; i < rowIds.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ObjectIdValue rowId = rowIds[i];
                ReadOnlyMemory<byte>? dataOpt = batch[i];

                if (dataOpt is null || dataOpt.Value.Length == 0)
                    continue;

                QueryRow row = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema,
                    txId,
                    rowId,
                    dataOpt.Value,
                    options,
                    required,
                    schemaVersion,
                    decode.DecodeState).ConfigureAwait(false);

                if (executionFilter is not null)
                {
                    decode.QualifiedLayout ??= QueryRowMerger.BuildQualifiedLayout(row.Layout, source.Alias);
                    IReadOnlyDictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRowAsQueryRow(row, decode.QualifiedLayout);

                    if (!await services.Filterer.MeetWhereAsync(executionFilter, qualified, plan.Ticket, plan.Database).ConfigureAwait(false))
                        continue;
                }

                yield return new QueryResultRow(rowId, row);
            }
        }
        finally
        {
            rowIds.Clear();
        }
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
/// call and passed through <see cref="IndexNestedLoopJoinOperator.ProbeRightIndex"/> into
/// <see cref="JoinLeafScanner.LoadRightRow"/>. The one instance serves every right row of the join
/// node, whether the row is loaded one at a time through
/// <see cref="JoinLeafScanner.LoadRightRow"/> or a page at a time through
/// <see cref="JoinLeafRowPage"/>, so the decode plan and the qualified layout are built once per
/// join node and not once per fetch path.
/// </summary>
internal sealed class RightDecodeState
{
    internal readonly RowEncoder.RowDecodeState DecodeState = new();
    internal RowLayout? QualifiedLayout;
}
