
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeleter
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowDeserializer rowDeserializer = new();

    private static ColumnValue? GetColumnValue(TableDescriptor table, Dictionary<string, ColumnValue> columnValues, string name)
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (column.Name == name)
            {
                /*foreach (ColumnValue columnValue in columnValues)
                {
                    if (columnValue.
                }
                if (ticket.Values.TryGetValue(column.Name, out ColumnValue? value))
                    return value;
                break;*/
            }
        }

        return null;
    }

    private async Task DeleteUniqueIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Value.UniqueRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            BTree<ColumnValue, BTreeTuple?> uniqueIndex = index.Value.UniqueRows;

            //await indexSaver.Remove();
        }
    }

    public async Task DeleteById(DatabaseDescriptor database, TableDescriptor table, DeleteByIdTicket ticket)
    {
        Stopwatch timer = new();
        timer.Start();

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

        ColumnValue columnId = new(ColumnType.Id, ticket.Id.ToString());

        BTreeTuple? pageOffset = index.UniqueRows.Get(columnId);

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} has an empty page data", ticket.Id);
            return;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return;
        }

        Dictionary<string, ColumnValue> columnValues = rowDeserializer.Deserialize(table.Schema!, data);

        Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, pageOffset.SlotTwo);

        await DeleteUniqueIndexes(database, table, columnValues);

        index.UniqueRows.Remove(columnId);

        table.Rows.Remove(pageOffset.SlotOne);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine("Row pk {0} with id {0} deleted from page {1}, Time taken: {2}", ticket.Id, pageOffset.SlotOne, pageOffset.SlotTwo, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
