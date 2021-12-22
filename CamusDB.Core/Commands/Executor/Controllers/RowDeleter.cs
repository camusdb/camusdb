
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowDeleter
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowDeserializer rowDeserializer = new();

    private static ColumnValue? GetColumnValue(Dictionary<string, ColumnValue> columnValues, string name)
    {
        if (columnValues.TryGetValue(name, out ColumnValue? columnValue))
            return columnValue;

        return null;
    }

    private async Task DeleteUniqueIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo update in parallel
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Value.UniqueRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            ColumnValue? columnKey = GetColumnValue(columnValues, index.Value.Column);
            if (columnKey is null) // @todo check what to to here
                continue;

            BTree<ColumnValue, BTreeTuple?> uniqueIndex = index.Value.UniqueRows;

            RemoveUniqueIndexTicket ticket = new(
                tablespace: tablespace,
                journal: database.Journal.Writer,
                sequence: 0,
                subSequence: 0,
                failureType: JournalFailureTypes.None,
                index: uniqueIndex,
                key: columnKey
            );

            await indexSaver.Remove(ticket);
        }
    }

    private async Task DeleteMultiIndexes(DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> columnValues)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes) // @todo update in parallel
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            ColumnValue? columnValue = GetColumnValue(columnValues, index.Value.Column);
            if (columnValue is null) // @todo check what to to here
                continue;

            BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            await indexSaver.Remove(tablespace, multiIndex, columnValue);
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

        // @make all of this atomic

        await DeleteUniqueIndexes(database, table, columnValues);

        await DeleteMultiIndexes(database, table, columnValues);

        (bool found, List<BTreeNode<int, int?>> deltas) = table.Rows.Remove(pageOffset.SlotOne);

        if (found)
        {
            // @todo persist index?
        }

        await tablespace.CleanPage(pageOffset.SlotTwo);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine("Row pk {0} with id {1} deleted from page {2}, Time taken: {3}", ticket.Id, pageOffset.SlotOne, pageOffset.SlotTwo, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
