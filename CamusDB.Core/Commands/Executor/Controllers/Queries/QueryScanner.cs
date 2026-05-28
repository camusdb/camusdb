
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryScanner
{
    internal async IAsyncEnumerable<QueryResultRow> ScanUsingTableIndex(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
    )
    {
        HLCTimestamp txId = ticket.TxnState.TransactionId;

        await foreach ((ObjectIdValue rowId, byte[] data) in table.Store.ScanRows(txId))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(rowId, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(rowId, row);
                }
                else
                    yield return new(rowId, row);
            }
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ScanUsingIndex(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
    )
    {
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
                continue;

            Dictionary<string, ColumnValue> row = RowEncoder.Decode(table.Schema, rowId, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(rowId, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(rowId, row);
                }
                else
                    yield return new(rowId, row);
            }
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
