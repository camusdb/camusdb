
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Models;
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

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(txId))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(
                table.Schema,
                rowId,
                data,
                plan.ScanRequiredColumns);

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

        if (!table.Indexes.TryGetValue(ticket.IndexName!, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                $"Key '{ticket.IndexName!}' doesn't exist in table '{table.Name}'"
            );
        }

        HLCTimestamp txId = ticket.TxnState.TransactionId;
        ColumnType[] keyTypes = GetIndexColumnTypes(table, index);
        bool unique = index.Type == IndexType.Unique;

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId) in table.Store.ScanIndex(txId, index.Name, keyTypes, null, null, unique))
        {
            byte[]? data = await table.Store.GetRow(txId, rowId).ConfigureAwait(false);
            if (data is null || data.Length == 0)
            {
                logger.LogWarning("Row {RowId} found in index {IndexName} but data is missing in table {TableName}", rowId, index.Name, table.Name);
                continue;
            }

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(
                table.Schema,
                rowId,
                data,
                plan.ScanRequiredColumns);

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
