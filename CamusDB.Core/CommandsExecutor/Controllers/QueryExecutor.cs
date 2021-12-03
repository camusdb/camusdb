
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowReader = new();

    public async Task<List<List<ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (string.IsNullOrEmpty(ticket.IndexName))
            return await QueryUsingTableIndex(database, table, ticket);

        return await QueryUsingIndex(database, table, ticket);
    }

    private async Task<List<List<ColumnValue>>> QueryUsingTableIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (BTreeEntry<int> entry in table.Rows.EntriesTraverse())
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

            rows.Add(rowReader.Deserialize(table.Schema!, data));
        }

        return rows;
    }

    private async Task<List<List<ColumnValue>>> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, BTree<ColumnValue> index)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (BTreeEntry<ColumnValue> entry in index.EntriesTraverse())
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

            rows.Add(rowReader.Deserialize(table.Schema!, data));
        }

        return rows;
    }

    private async Task<List<List<ColumnValue>>> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, BTreeMulti<ColumnValue> index)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (BTreeMultiEntry<ColumnValue> entry in index.EntriesTraverse())
        {
            //Console.WriteLine("MultiTree={0} Key={0} PageOffset={1}", index.Id, entry.Key, entry.Value!.Size());

            foreach (BTreeEntry<int> subEntry in entry.Value!.EntriesTraverse())
            {
                //Console.WriteLine(" > Index Key={0} PageOffset={1}", subEntry.Key, subEntry.Value);

                if (subEntry.Value is null)
                {
                    Console.WriteLine("Index RowId={0} has no page offset value", subEntry.Key);
                    continue;
                }

                byte[] data = await tablespace.GetDataFromPage(subEntry.Key);
                if (data.Length == 0)
                {
                    Console.WriteLine("Index RowId={0} has an empty page data", subEntry.Key);
                    continue;
                }

                rows.Add(rowReader.Deserialize(table.Schema!, data));
            }
        }

        return rows;
    }

    private async Task<List<List<ColumnValue>>> QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
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

    public async Task<List<List<ColumnValue>>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        int? pageOffset = table.Indexes[CamusDBConfig.PrimaryKeyInternalName].UniqueRows!.Get(new ColumnValue(ColumnType.Id, ticket.Id.ToString()));

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} has an empty page data", ticket.Id);
            return rows;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.Value);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return rows;
        }

        rows.Add(rowReader.Deserialize(table.Schema!, data));

        return rows;
    }
}
