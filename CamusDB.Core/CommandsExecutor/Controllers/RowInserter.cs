
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
using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private readonly RowSerializer rowSerializer = new();

    private static void Validate(TableDescriptor table, InsertTicket ticket) // @todo optimize this
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.Values)
        {
            bool hasColumn = false;

            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];
                if (column.Name == columnValue.Key)
                {
                    hasColumn = true;
                    break;
                }
            }

            if (!hasColumn)
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    "Unknown column '" + columnValue.Key + "' in column list"
                );
        }
    }

    private static ColumnValue? GetColumnValue(TableDescriptor table, InsertTicket ticket, string name)
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (column.Name == name)
            {
                if (ticket.Values.TryGetValue(column.Name, out ColumnValue? value))
                    return value;
                break;
            }
        }

        return null;
    }

    private static ColumnValue CheckUniqueKeyViolations(TableDescriptor table, BTree<ColumnValue, BTreeTuple?> uniqueIndex, InsertTicket ticket, string name)
    {
        ColumnValue? uniqueValue = GetColumnValue(table, ticket, name);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKeyValue,
                "Cannot retrieve unique key for table " + table.Name
            );

        BTreeTuple? rowTuple = uniqueIndex.Get(uniqueValue);

        if (rowTuple is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKeyValue,
                "Duplicate entry for key " + table.Name + " " + uniqueValue
            );

        return uniqueValue;
    }

    private async Task<BTreeTuple> CheckAndUpdateUniqueKeys(DatabaseDescriptor database, TableDescriptor table, uint sequence, InsertTicket ticket)
    {
        BTreeTuple rowTuple = new(-1, -1);

        BufferPoolHandler tablespace = database.TableSpace;

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

            try
            {
                await uniqueIndex.WriteLock.WaitAsync();

                ColumnValue uniqueKeyValue = CheckUniqueKeyViolations(table, uniqueIndex, ticket, index.Value.Column);

                // allocate pages and rowid when needed
                if (rowTuple.SlotOne == -1)
                    rowTuple.SlotOne = await tablespace.GetNextRowId();

                if (rowTuple.SlotTwo == -1)
                    rowTuple.SlotTwo = await tablespace.GetNextFreeOffset();

                // save page + rowid to journal
                JournalInsertSlots schedule = new(sequence, rowTuple);
                await database.JournalWriter.Append(schedule);

                // save index save to journal
                JournalUpdateUniqueIndex indexSchedule = new(sequence, index.Value);
                uint updateIndexSequence = await database.JournalWriter.Append(schedule);

                SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                    tablespace: tablespace,
                    journal: database.JournalWriter,
                    sequence: updateIndexSequence,
                    index: uniqueIndex,
                    key: uniqueKeyValue,
                    value: rowTuple
                );

                await indexSaver.NoLockingSave(saveUniqueIndexTicket);

                // save index save to journal
                //JournalUpdateUniqueIndex indexSchedule = new(sequence, index.Value);
                //uint updateIndexSequence = await database.JournalWriter.Append(schedule);
            }
            finally
            {
                uniqueIndex.WriteLock.Release();
            } 
        }

        return rowTuple;
    }

    private async Task UpdateMultiKeys(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket, BTreeTuple rowTuple)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            ColumnValue? multiKeyValue = GetColumnValue(table, ticket, index.Value.Column);
            if (multiKeyValue is null)
                continue;

            await indexSaver.Save(tablespace, multiIndex, multiKeyValue, rowTuple);
        }
    }

    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Validate(table, ticket);

        Stopwatch timer = new();
        timer.Start();

        JournalInsert schedule = new(ticket);
        uint sequence = await database.JournalWriter.Append(schedule);

        BufferPoolHandler tablespace = database.TableSpace;

        BTreeTuple rowTuple = await CheckAndUpdateUniqueKeys(database, table, sequence, ticket);

        byte[] rowBuffer = rowSerializer.Serialize(table, ticket, rowTuple.SlotOne);

        // Insert data to the page offset
        await tablespace.WriteDataToPage(rowTuple.SlotTwo, rowBuffer);

        // Main table index stores rowid pointing to page offeset
        await indexSaver.Save(tablespace, table.Rows, rowTuple.SlotOne, rowTuple.SlotTwo);

        await UpdateMultiKeys(database, table, ticket, rowTuple);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        /*foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.MultiRows is not null)
            {
                foreach (BTreeMultiEntry entry in index.Value.MultiRows.EntriesTraverse())
                {
                    Console.WriteLine("Index Key={0}/{1} PageOffset={2}", index.Key, entry.Key, entry.Value!.Size());

                    foreach (BTreeEntry entry2 in entry.Value.EntriesTraverse())
                    {
                        Console.WriteLine(" > Index Key={0} PageOffset={1}", entry2.Key, entry2.Value);
                    }
                }
            }
        }*/

        Console.WriteLine("Row {0} inserted at {1}, Time taken: {2}", rowTuple.SlotOne, rowTuple.SlotTwo, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
