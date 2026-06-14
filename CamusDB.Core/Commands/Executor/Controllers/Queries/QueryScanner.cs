
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
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
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
    )
    {
        TableDescriptor table = plan.Table;
        HLCTimestamp txId = plan.Ticket.TxnState.TransactionId;
        int visibilityVersion = plan.TableSchemaVersion;
        PlanNodeStats? scanStats = plan.CollectRuntimeStats && plan.StepNodes.Count > 0 ? plan.StepNodes[0].Stats : null;

        // R1: acquire a phantom-protection range lock on the row key space before the scan.
        // Shared for SELECT; Exclusive for UPDATE/DELETE (blocks concurrent readers from
        // seeing the range while the mutation is in flight). Same-tx re-acquisition is
        // idempotent. Read-only transactions skip this.
        await table.Store.AcquireRowRangeLockAsync(plan.Ticket.TxnState,
            exclusive: plan.Ticket.ExclusivePredicateLocks).ConfigureAwait(false);

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(plan.Ticket.TxnState, maxRows: plan.ScanRowLimit))
        {
            if (data.Length == 0)
                continue;

            if (scanStats is not null)
                scanStats.KvScanEntries++;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                plan.ScanRequiredColumns,
                visibilityVersion).ConfigureAwait(false);

            if (scanStats is not null)
                scanStats.RowsRead++;

            if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                yield return new(rowId, row);
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanUsingIndex(
        QueryPlan plan,
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
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

        // R1: phantom-protection range lock on the index key space (mirrors ScanUsingTableIndex).
        await table.Store.AcquireIndexRangeLockAsync(ticket.TxnState, index.Name,
            exclusive: plan.Ticket.ExclusivePredicateLocks).ConfigureAwait(false);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(
            ticket.TxnState,
            index.Name,
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

            if (scanStats is not null)
                scanStats.RowsRead++;

            Dictionary<string, ColumnValue> row = await RowEncoder.DecodeAsync(
                table.Schema,
                txId,
                rowId,
                data,
                plan.ScanRequiredColumns,
                visibilityVersion).ConfigureAwait(false);

            if (await queryFilterer.MeetPlanFilterAsync(plan, row).ConfigureAwait(false))
                yield return new(rowId, row);
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
