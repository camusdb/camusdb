
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class DMLUniqueKeySaver : DMLKeyBase
{
    /// <summary>
    /// Checks if a row with the same primary key is already added to table
    /// </summary>
    /// <param name="table"></param>
    /// <param name="uniqueIndex"></param>
    /// <param name="ticket"></param>
    /// <param name="name"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    private static async Task<CompositeColumnValue> CheckUniqueKeyViolations(
        TableDescriptor table,
        BTree<CompositeColumnValue, BTreeTuple> uniqueIndex, 
        InsertTicket ticket, 
        string[] columnNames
    )
    {
        CompositeColumnValue? uniqueValue = GetColumnValue(ticket.Values, columnNames);

        if (uniqueValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "The primary key of the table \"" + table.Name + "\" is not present in the list of values."
            );

        BTreeTuple? rowTuple = await uniqueIndex.Get(TransactionType.ReadOnly, ticket.TxnId, uniqueValue);

        if (rowTuple is not null && !rowTuple.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicateUniqueKeyValue,
                "Duplicate entry for key \"" + table.Name + "\" " + uniqueValue
            );

        return uniqueValue;
    }

    public async Task CheckUniqueKeys(TableDescriptor table, InsertTicket ticket)
    {
        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (index.Value.Type != IndexType.Unique)
                continue;

            BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> uniqueIndex = index.Value.BTree;            

            await CheckUniqueKeyViolations(table, uniqueIndex, ticket, index.Value.Columns);
        }
    }
}
