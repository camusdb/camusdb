
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.BufferPool;

namespace CamusDB.Core.CommandsExecutor.Controllers.Insert;

internal sealed class InsertUniqueKeySaver : InsertKeyBase
{
    private readonly IndexSaver indexSaver = new();    

    private static async Task<ColumnValue> CheckUniqueKeyViolations(TableDescriptor table, BTree<ColumnValue, BTreeTuple?> uniqueIndex, InsertTicket ticket, string name)
    {
        ColumnValue? uniqueValue = GetColumnValue(table, ticket, name);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot retrieve unique key for table " + table.Name
            );

        BTreeTuple? rowTuple = await uniqueIndex.Get(uniqueValue);

        if (rowTuple is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key " + table.Name + " " + uniqueValue
            );

        return uniqueValue;
    }

    public async Task CheckUniqueKeys(TableDescriptor table, InsertTicket ticket)
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

            await CheckUniqueKeyViolations(table, uniqueIndex, ticket, index.Value.Column);
        }
    }

    public async Task<BTreeTuple> UpdateUniqueKeys(UpdateUniqueIndexTicket ticket)
    {
        InsertTicket insertTicket = ticket.InsertTicket;
        BufferPoolHandler tablespace = ticket.Database.TableSpace;        

        foreach (TableIndexSchema index in ticket.Indexes)
        {
            BTree<ColumnValue, BTreeTuple?>? uniqueIndex = index.UniqueRows;

            if (uniqueIndex is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A unique index tree wasn't found"
                );

            try
            {
                await uniqueIndex.WriteLock.WaitAsync();

                ColumnValue? uniqueKeyValue = GetColumnValue(ticket.Table, insertTicket, index.Column);

                if (uniqueKeyValue is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "A null value was found for unique key field " + index.Column
                    );

                // save index save to journal                

                SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                    tablespace: tablespace,                    
                    sequence: ticket.Sequence,
                    subSequence: 0,                    
                    index: uniqueIndex,
                    key: uniqueKeyValue,
                    value: ticket.RowTuple
                );

                await indexSaver.NoLockingSave(saveUniqueIndexTicket);                
            }
            finally
            {
                uniqueIndex.WriteLock.Release();
            }
        }

        return ticket.RowTuple;
    }
}
