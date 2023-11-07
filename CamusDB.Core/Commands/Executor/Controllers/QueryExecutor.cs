
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using System;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowDeserializer = new();

    public async Task<List<Dictionary<string, ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (string.IsNullOrEmpty(ticket.IndexName))
            return await QueryUsingTableIndex(database, table, ticket);

        return await QueryUsingIndex(database, table, ticket);
    }

    private async Task<List<Dictionary<string, ColumnValue>>> QueryUsingTableIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {        
        BufferPoolHandler tablespace = database.TableSpace!;

        using IDisposable readerLock = await table.Rows.ReaderWriterLock.ReaderLockAsync();

        List<Dictionary<string, ColumnValue>> rows = new();

        await foreach (BTreeEntry<int, int?> entry in table.Rows.EntriesTraverse())
        {
            if (entry.Value is null)
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value.Value);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            rows.Add(rowDeserializer.Deserialize(table.Schema!, data));
        }

        return rows;
    }

    private async Task<List<Dictionary<string, ColumnValue>>> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, BTree<ColumnValue,BTreeTuple?> index)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        using IDisposable readerLock = await index.ReaderWriterLock.ReaderLockAsync();

        List<Dictionary<string, ColumnValue>> rows = new();

        await foreach (BTreeEntry<ColumnValue, BTreeTuple?> entry in index.EntriesTraverse())
        {
            if (entry.Value is null)
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value.SlotOne);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            rows.Add(rowDeserializer.Deserialize(table.Schema!, data));
        }

        return rows;
    }

    private async Task<List<Dictionary<string, ColumnValue>>> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, BTreeMulti<ColumnValue> index)
    {
        BufferPoolHandler tablespace = database.TableSpace;        

        using IDisposable readerLock = await index.ReaderWriterLock.ReaderLockAsync();

        List<Dictionary<string, ColumnValue>> rows = new();

        foreach (BTreeMultiEntry<ColumnValue> entry in index.EntriesTraverse())
        {
            //Console.WriteLine("MultiTree={0} Key={0} PageOffset={1}", index.Id, entry.Key, entry.Value!.Size());

            await foreach (BTreeEntry<int, int?> subEntry in entry.Value!.EntriesTraverse())
            {
                //Console.WriteLine(" > Index Key={0} PageOffset={1}", subEntry.Key, subEntry.Value);

                if (subEntry.Value is null)
                {
                    Console.WriteLine("Index RowId={0} has no page offset value", subEntry.Key);
                    continue;
                }

                byte[] data = await tablespace.GetDataFromPage(subEntry.Value.Value);
                if (data.Length == 0)
                {
                    Console.WriteLine("Index RowId={0} has an empty page data", subEntry.Key);
                    continue;
                }

                rows.Add(rowDeserializer.Deserialize(table.Schema!, data));
            }
        }

        return rows;
    }

    private async Task<List<Dictionary<string, ColumnValue>>> QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (!table.Indexes.TryGetValue(ticket.IndexName!, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                "Key '" + ticket.IndexName! + "' doesn't exist in table '" + table.Name + "'"
            );
        }

        if (index.Type == IndexType.Unique)
            return await QueryUsingUniqueIndex(database, table, index.UniqueRows!);

        return await QueryUsingMultiIndex(database, table, index.MultiRows!);
    }

    public async Task<List<Dictionary<string, ColumnValue>>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<Dictionary<string, ColumnValue>> rows = new();

        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        if (index.UniqueRows is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        using IDisposable readerLock = await index.UniqueRows.ReaderWriterLock.ReaderLockAsync();

        BTreeTuple? pageOffset = await index.UniqueRows.Get(columnId);

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} has an empty page data", ticket.Id);
            return rows;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return rows;
        }

        Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

        rows.Add(rowDeserializer.Deserialize(table.Schema!, data));

        return rows;
    }
}
