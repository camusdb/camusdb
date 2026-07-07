
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
        QueryFilterer queryFilterer
    )
    {
        TableDescriptor table = plan.Table;
        QueryTicket ticket = plan.Ticket;
        int visibilityVersion = plan.TableSchemaVersion;

        if (!table.Indexes.TryGetValue(ticket.IndexName!, out TableIndexSchema? index))
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
                $"Key '{ticket.IndexName!}' doesn't exist in table '{table.Name}'"
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

        // Per-scan layout cache: one entry per stored schema version (constant for a scan).
        Dictionary<int, RowLayout> layoutCache = new();

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

            byte[]? data = await table.Store.GetRow(ticket.TxnState, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
            {
                logger.LogWarning("Row {RowId} found in index {IndexName} but data is missing in table {TableName}", rowId, index.Name, table.Name);
                continue;
            }

            // Record the row point dep — catches updates to non-indexed projected columns.
            deps?.RecordPoint(table.Store.RowPointKey(rowId));

            if (scanStats is not null)
                scanStats.RowsRead++;

            QueryRow queryRow = await RowEncoder.DecodeToQueryRowAsync(
                table.Schema,
                txId,
                rowId,
                data,
                plan.ScanRequiredColumns,
                visibilityVersion,
                layoutCache).ConfigureAwait(false);

            if (await queryFilterer.MeetPlanFilterAsync(plan, queryRow).ConfigureAwait(false))
                yield return new QueryResultRow(rowId, queryRow);
        }
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
