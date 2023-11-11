
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
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowDeserializer = new();

    public IAsyncEnumerable<Dictionary<string, ColumnValue>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (string.IsNullOrEmpty(ticket.IndexName))
            return QueryUsingTableIndex(database, table, ticket);

        return QueryUsingIndex(database, table, ticket);
    }

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingTableIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        using IDisposable readerLock = await table.Rows.ReaderWriterLock.ReaderLockAsync();

        await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> entry in table.Rows.EntriesTraverse())
        {
            if (entry.Value.IsNull())
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            yield return rowDeserializer.Deserialize(table.Schema!, data);
        }
    }

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, BTree<ColumnValue, BTreeTuple?> index)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        using IDisposable readerLock = await index.ReaderWriterLock.ReaderLockAsync();

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

            yield return rowDeserializer.Deserialize(table.Schema!, data);
        }
    }

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, BTreeMulti<ColumnValue> index)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        using IDisposable readerLock = await index.ReaderWriterLock.ReaderLockAsync();

        foreach (BTreeMultiEntry<ColumnValue> entry in index.EntriesTraverse())
        {
            //Console.WriteLine("MultiTree={0} Key={0} PageOffset={1}", index.Id, entry.Key, entry.Value!.Size());

            await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> subEntry in entry.Value!.EntriesTraverse())
            {
                //Console.WriteLine(" > Index Key={0} PageOffset={1}", subEntry.Key, subEntry.Value);

                if (subEntry.Value.IsNull())
                {
                    Console.WriteLine("Index RowId={0} has no page offset value", subEntry.Key);
                    continue;
                }

                byte[] data = await tablespace.GetDataFromPage(subEntry.Value);
                if (data.Length == 0)
                {
                    Console.WriteLine("Index RowId={0} has an empty page data", subEntry.Key);
                    continue;
                }

                yield return rowDeserializer.Deserialize(table.Schema!, data);
            }
        }
    }

    private IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (!table.Indexes.TryGetValue(ticket.IndexName!, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                "Key '" + ticket.IndexName! + "' doesn't exist in table '" + table.Name + "'"
            );
        }

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(database, table, index.UniqueRows!);

        return QueryUsingMultiIndex(database, table, index.MultiRows!);
    }

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

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
            yield break;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            yield break;
        }

        Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

        yield return rowDeserializer.Deserialize(table.Schema!, data);
    }
}
