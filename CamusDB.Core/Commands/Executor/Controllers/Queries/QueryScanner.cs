
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

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
        BufferPoolManager tablespace = database.BufferPool;

        await ticket.TxnState.TryAdquireTableRowsLock(table);

        await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> entry in table.Rows.EntriesTraverse(ticket.TxnState.TxnId))
        {
            ObjectIdValue dataOffset = entry.GetValue(ticket.TxnType, ticket.TxnState.TxnId);

            if (dataOffset.IsNull())
            {
                //Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(dataOffset).ConfigureAwait(false);
            if (data.Length == 0)
            {
                //Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, entry.Key, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(new(entry.Key, dataOffset), row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(new(entry.Key, dataOffset), row);
                }
                else
                    yield return new(new(entry.Key, dataOffset), row);
            }
        }
    }

    internal IAsyncEnumerable<QueryResultRow> ScanUsingIndex(
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
        
        return ScanUsingIndex(database, table, index.BTree, ticket, queryFilterer, rowDeserializer);        
    }

    private async IAsyncEnumerable<QueryResultRow> ScanUsingIndex(
        DatabaseDescriptor database,
        TableDescriptor table,
        BTree<CompositeColumnValue, BTreeTuple> index,
        QueryTicket ticket,
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
    )
    {
        using IDisposable _ = await index.ReaderLockAsync().ConfigureAwait(false);

        BufferPoolManager tablespace = database.BufferPool;

        await foreach (BTreeEntry<CompositeColumnValue, BTreeTuple> entry in index.EntriesTraverse(ticket.TxnState.TxnId))
        {
            BTreeTuple? txnValue = entry.GetValue(ticket.TxnType, ticket.TxnState.TxnId);

            if (txnValue is null || txnValue.IsNull())
            {
                //Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(txnValue.SlotTwo).ConfigureAwait(false);
            if (data.Length == 0)
            {
                //Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, txnValue.SlotOne, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(txnValue, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row, ticket.Parameters))
                        yield return new(txnValue, row);
                }
                else
                    yield return new(txnValue, row);
            }
        }
    }

    /*private async IAsyncEnumerable<QueryResultRow> ScanUsingMultiIndex(
        DatabaseDescriptor database,
        TableDescriptor table,
        BTreeMulti<ColumnValue> index,
        QueryTicket ticket,
        QueryFilterer queryFilterer,
        RowDeserializer rowDeserializer
    )
    {
        BufferPoolManager tablespace = database.BufferPool;

        foreach (BTreeMultiEntry<ColumnValue> entry in index.EntriesTraverse())
        {
            //Console.WriteLine("MultiTree={0} Key={0} PageOffset={1}", index.Id, entry.Key, entry.Value!.Size());

            await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> subEntry in entry.Value!.EntriesTraverse(ticket.TxnId))
            {
                //Console.WriteLine(" > Index Key={0} PageOffset={1}", subEntry.Key, subEntry.Value);

                ObjectIdValue dataOffset = subEntry.GetValue(ticket.TxnType, ticket.TxnId);

                if (dataOffset.IsNull())
                {
                    //Console.WriteLine("Index RowId={0} has no page offset value", subEntry.Key);
                    continue;
                }

                byte[] data = await tablespace.GetDataFromPage(dataOffset);
                if (data.Length == 0)
                {
                    //Console.WriteLine("Index RowId={0} has an empty page data", subEntry.Key);
                    continue;
                }

                yield return new(new(subEntry.Key, dataOffset), rowDeserializer.Deserialize(table.Schema, data));
            }
        }
    }*/
}