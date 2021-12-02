
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

public sealed class RowInserter
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

    private static int? GetRowValue(TableDescriptor table, InsertTicket ticket, string name)
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (column.Name == name)
            {
                if (ticket.Values.TryGetValue(column.Name, out ColumnValue? value))
                    return int.Parse(value.Value);
                break;
            }
        }

        return null;
    }

    private static int CheckUniqueKeyViolations(TableDescriptor table, BTree uniqueIndex, InsertTicket ticket, string name)
    {
        int? uniqueValue = GetRowValue(table, ticket, name);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKeyValue,
                "Cannot retrieve unique key for table " + table.Name
            );

        int? pageOffset = uniqueIndex.Get(uniqueValue.Value);

        if (pageOffset is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKeyValue,
                "Duplicate entry for key " + table.Name + " " + uniqueValue
            );

        return uniqueValue.Value;
    }

    private async Task<InsertRowContext> CheckAndUpdateUniqueKeys(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        InsertRowContext context = new(-1, -1);

        BufferPoolHandler tablespace = database.TableSpace!;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            if (index.Value.UniqueRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            BTree uniqueIndex = index.Value.UniqueRows;

            try
            {
                await uniqueIndex.WriteLock.WaitAsync();

                int uniqueKeyValue = CheckUniqueKeyViolations(table, uniqueIndex, ticket, index.Value.Column);

                // allocate pages and rowid when needed
                if (context.RowId == -1)
                    context.RowId = await tablespace.GetNextRowId();

                if (context.DataPageOffset == -1)
                    context.DataPageOffset = await tablespace.GetNextFreeOffset();

                await indexSaver.NoLockingSave(tablespace, uniqueIndex, uniqueKeyValue, context.DataPageOffset);
            }
            finally
            {
                uniqueIndex.WriteLock.Release();
            }
        }

        return context;
    }

    private async Task UpdateMultiKeys(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket, InsertRowContext context)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            BTreeMulti multiIndex = index.Value.MultiRows;

            int? multiKeyValue = GetRowValue(table, ticket, index.Value.Column);
            if (multiKeyValue is null)
                continue;

            Console.WriteLine(multiKeyValue.Value);

            await indexSaver.Save(tablespace, multiIndex, multiKeyValue.Value, context.DataPageOffset);            
        }
    }

    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        Validate(table, ticket);

        Stopwatch timer = new();
        timer.Start();

        BufferPoolHandler tablespace = database.TableSpace!;

        InsertRowContext context = await CheckAndUpdateUniqueKeys(database, table, ticket);

        // Insert data to a free page and update indexes

        byte[] rowBuffer = rowSerializer.Serialize(table, ticket, context.RowId);

        await tablespace.WriteDataToPage(context.DataPageOffset, rowBuffer);

        await indexSaver.Save(tablespace, table.Rows, context.RowId, context.DataPageOffset);

        await UpdateMultiKeys(database, table, ticket, context);

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

        Console.WriteLine("Row {0} inserted at {1}, Time taken: {2}", context.RowId, context.DataPageOffset, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
