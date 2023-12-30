
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class DMLUniqueKeySaver : DMLKeyBase
{
    private readonly IndexSaver indexSaver = new();

    /// <summary>
    /// Checks if a row with the same primary key is already added to table
    /// </summary>
    /// <param name="table"></param>
    /// <param name="uniqueIndex"></param>
    /// <param name="ticket"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static async Task<ColumnValue> CheckUniqueKeyViolations(TableDescriptor table, BTree<ColumnValue, BTreeTuple?> uniqueIndex, InsertTicket ticket, string name)
    {
        ColumnValue? uniqueValue = GetColumnValue(table, ticket, name);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "The primary key of the table \"" + table.Name + "\" is not present in the list of values."
            );

        BTreeTuple? rowTuple = await uniqueIndex.Get(ticket.TxnId, uniqueValue);

        if (rowTuple is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key \"" + table.Name + "\" " + uniqueValue.Type + " " + uniqueValue.Value
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

            ColumnValue? uniqueKeyValue = GetColumnValue(ticket.Table, insertTicket, index.Column);

            if (uniqueKeyValue is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A null value was found for unique key field " + index.Column
                );

            // save index save to journal                

            SaveUniqueIndexTicket saveUniqueIndexTicket = new(
                tablespace: tablespace,
                index: uniqueIndex,
                txnId: ticket.InsertTicket.TxnId,
                key: uniqueKeyValue,
                value: ticket.RowTuple,
                modifiedPages: ticket.ModifiedPages
            );

            await indexSaver.NoLockingSave(saveUniqueIndexTicket);
        }

        return ticket.RowTuple;
    }
}
