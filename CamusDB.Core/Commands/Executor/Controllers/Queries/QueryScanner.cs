
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
        deps?.RecordSchema(table.Id, table.Schema.Version);

        // One RowLayout per stored schema version. Most scans touch only one version so this
        // holds one entry; mixed-version scans hold a handful. The layout is identical for all
        // rows at the same stored version (requiredColumns and visibilityVersion are constant
        // for the life of a scan), so building it once and reusing it is safe.
        Dictionary<int, RowLayout> layoutCache = new();

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(plan.Ticket.TxnState, maxRows: plan.ScanRowLimit))
        {
            if (data.Length == 0)
                continue;

            if (scanStats is not null)
                scanStats.KvScanEntries++;

            // Record the point dep for every fetched row — catches updates to non-indexed columns.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            QueryRow queryRow = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                txId,
                rowId,
                data,
                plan.ScanRequiredColumns,
                visibilityVersion,
                layoutCache).ConfigureAwait(false);

            if (scanStats is not null)
                scanStats.RowsRead++;

            if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                yield return new QueryResultRow(rowId, queryRow);
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
        deps?.RecordSchema(table.Id, table.Schema.Version);

        if (plan.IndexOnly)
        {
            // Covering (index-only) path: every needed column is already in the decoded index
            // key or is the primary-key (row-id) column. Synthesize the QueryRow directly from
            // the scan entry — no GetRow call and no RowEncoder.DecodeAsync call.
            // No per-row point dep is recorded because no primary row was read; the range dep
            // above and the schema dep cover snapshot correctness for the index scan.
            (RowLayout coveredLayout, int[] slotMap) = BuildIndexOnlyLayout(table, plan.ScanRequiredColumns, index);

            await foreach ((CompositeColumnValue decodedKey, ObjectIdValue rowId) in table.Store.ScanIndex(
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
                if (scanStats is not null)
                    scanStats.KvScanEntries++;

                ColumnValue[] values = SynthesizeCoveredValues(slotMap, decodedKey);
                QueryRow queryRow = new(rowId, coveredLayout, values);

                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    yield return new QueryResultRow(rowId, queryRow);
            }
            yield break;
        }

        // Non-covering path: collect row ids in pages and fetch each page in one batch call.
        // Index order is preserved: the page is filled in scan order and batch results are
        // indexed positionally, so rows are decoded and yielded in scan order. Missing rows
        // (null bytes from the batch) preserve the warn-and-skip contract of the per-entry path.
        Dictionary<int, RowLayout> layoutCache = new();
        int batchSize = CamusDBConfig.IndexScanFetchBatchSize;
        List<ObjectIdValue> pageBuf = new(batchSize);

        // Fetch and decode one buffered page; yields rows in the order they appear in the page.
        async IAsyncEnumerable<QueryResultRow> flushPageAsync(List<ObjectIdValue> page)
        {
            byte[]?[] batchResult = await table.Store.GetRowsBatch(ticket.TxnState, page).ConfigureAwait(false);
            for (int i = 0; i < page.Count; i++)
            {
                ObjectIdValue batchRowId = page[i];
                byte[]? data = batchResult[i];
                if (data is null || data.Length == 0)
                {
                    logger.LogWarning("Row {RowId} found in index {IndexName} but data is missing in table {TableName}", batchRowId, index.Name, table.Name);
                    continue;
                }
                deps?.RecordPoint(table.Store.RowPointKey(batchRowId));
                if (scanStats is not null) scanStats.RowsRead++;
                QueryRow queryRow = await RowEncoder.DecodeToQueryRowAsync(
                    table.Schema, txId, batchRowId, data,
                    plan.ScanRequiredColumns, visibilityVersion, layoutCache).ConfigureAwait(false);
                if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                    yield return new QueryResultRow(batchRowId, queryRow);
            }
        }

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
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

    /// <summary>
    /// Builds the <see cref="RowLayout"/> and accompanying slot map for an index-only (covering)
    /// scan. Column names are ordered by their position in the table schema (matching
    /// <see cref="RowEncoder.DecodeToQueryRowAsync"/> output order) and filtered to those in
    /// <paramref name="required"/>.
    ///
    /// Each <c>slotMap[i]</c> is the position of that column in <paramref name="index"/>.Columns;
    /// the value is read from the decoded <see cref="CompositeColumnValue"/> at that position. A
    /// covering scan is only planned when every required column is an index-key column
    /// (<c>TryMarkIndexOnly</c> restricts the covered set to the index key), so the lookup always
    /// succeeds — a missing column throws rather than falling back to the KV row id, since the
    /// logical <c>id</c> column is user-supplied and need not equal the KV row key.
    /// Called from <see cref="QueryExecutor"/> for the range-scan and unique-lookup paths.
    /// </summary>
    internal static (RowLayout layout, int[] slotMap) BuildIndexOnlyLayout(
        TableDescriptor table,
        IReadOnlySet<string>? required,
        TableIndexSchema index)
    {
        List<string> names = [];
        List<int> slots = [];

        if (table.Schema.Columns is not null)
        {
            foreach (TableColumnSchema col in table.Schema.Columns)
            {
                if (required is not null && !required.Contains(col.Name))
                    continue;

                names.Add(col.Name);

                // Covering is only marked when every required column is an index-key column
                // (TryMarkIndexOnly restricts `available` to index.Columns), so this lookup
                // always succeeds. A -1 here would mean the planner marked a plan index-only
                // whose required set is not a subset of the index key — an invariant violation.
                int keyPos = Array.IndexOf(index.Columns, col.Name);
                if (keyPos < 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"Covering scan required column '{col.Name}' is not in index '{index.Name}' key");
                slots.Add(keyPos);
            }
        }

        return (RowLayout.ForColumns(names), slots.ToArray());
    }

    /// <summary>
    /// Synthesizes the <see cref="ColumnValue"/> array for one covering-scan row by reading each
    /// column straight from the decoded index key. Every <paramref name="slotMap"/> entry is a
    /// non-negative position into <paramref name="decodedKey"/> because a covering scan is only
    /// planned when every required column is an index-key column (see <see cref="BuildIndexOnlyLayout"/>
    /// and <c>TryMarkIndexOnly</c>). The KV row id is deliberately not used as a value source: the
    /// logical <c>id</c> column is user-supplied and need not equal the internal KV row key.
    /// Called from <see cref="QueryExecutor"/> for the range-scan and unique-lookup paths.
    /// </summary>
    internal static ColumnValue[] SynthesizeCoveredValues(
        int[] slotMap,
        CompositeColumnValue decodedKey)
    {
        ColumnValue[] values = new ColumnValue[slotMap.Length];
        for (int i = 0; i < slotMap.Length; i++)
            values[i] = decodedKey.Values[slotMap[i]];
        return values;
    }

    private static ColumnType[] GetIndexColumnTypes(TableDescriptor table, TableIndexSchema index)
    {
        ColumnType[] types = new ColumnType[index.Columns.Length];

        for (int i = 0; i < index.Columns.Length; i++)
        {
            string colName = index.Columns[i];
            TableColumnSchema? col = table.Schema.Columns?.Find(c => c.Name == colName);
            types[i] = col?.Type ?? ColumnType.String;
        }

        return types;
    }

}
