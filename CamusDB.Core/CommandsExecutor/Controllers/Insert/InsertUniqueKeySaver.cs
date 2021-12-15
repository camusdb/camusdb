
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Insert;

internal sealed class InsertUniqueKeySaver : InsertKeyBase
{
    private readonly IndexSaver indexSaver = new();    

    private static ColumnValue CheckUniqueKeyViolations(TableDescriptor table, BTree<ColumnValue, BTreeTuple?> uniqueIndex, InsertTicket ticket, string name)
    {
        ColumnValue? uniqueValue = GetColumnValue(table, ticket, name);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot retrieve unique key for table " + table.Name
            );

        BTreeTuple? rowTuple = uniqueIndex.Get(uniqueValue);

        if (rowTuple is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key " + table.Name + " " + uniqueValue
            );

        return uniqueValue;
    }

    public void CheckUniqueKeys(TableDescriptor table, InsertTicket ticket)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.Value.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            CheckUniqueKeyViolations(table, uniqueIndex, ticket, index.Value.Column);
        }
    }

    public async Task<BTreeTuple> UpdateUniqueKeys(DatabaseDescriptor database, TableDescriptor table, uint sequence, InsertTicket ticket)
    {
        BTreeTuple rowTuple = new(-1, -1);

        BufferPoolHandler tablespace = database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.Value.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            try
            {
                await uniqueIndex.WriteLock.WaitAsync();

                ColumnValue? uniqueKeyValue = GetColumnValue(table, ticket, index.Value.Column);

                if (uniqueKeyValue is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "A null value was found for unique key field " + index.Key
                    );

                // allocate pages and rowid when needed
                if (rowTuple.SlotOne == -1)
                    rowTuple.SlotOne = await tablespace.GetNextRowId();

                if (rowTuple.SlotTwo == -1)
                    rowTuple.SlotTwo = await tablespace.GetNextFreeOffset();

                // save page + rowid to journal
                InsertSlotsLog schedule = new(sequence, rowTuple);
                await database.Journal.Writer.Append(ticket.ForceFailureType, schedule);

                // save index save to journal
                UpdateUniqueIndexLog indexSchedule = new(sequence, index.Value.Column);
                uint updateIndexSequence = await database.Journal.Writer.Append(ticket.ForceFailureType, indexSchedule);

                SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                    tablespace: tablespace,
                    journal: database.Journal.Writer,
                    sequence: updateIndexSequence,
                    failureType: ticket.ForceFailureType,
                    index: uniqueIndex,
                    key: uniqueKeyValue,
                    value: rowTuple
                );

                await indexSaver.NoLockingSave(saveUniqueIndexTicket);

                // save checkpoint of index saved
                UpdateUniqueCheckpointLog checkpoint = new(sequence, index.Value.Column);
                await database.Journal.Writer.Append(ticket.ForceFailureType, checkpoint);
            }
            finally
            {
                uniqueIndex.WriteLock.Release();
            }
        }

        return rowTuple;
    }
}
