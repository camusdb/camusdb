
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class DMLMultiKeySaver : DMLKeyBase
{
    private readonly IndexSaver indexSaver = new();    

    public async Task UpdateMultiKeys(SaveMultiKeysIndexTicket saveMultiKeysIndexTicket)
    {
        BufferPoolHandler tablespace = saveMultiKeysIndexTicket.Database.TableSpace;

        foreach (KeyValuePair<string, TableIndexSchema> index in saveMultiKeysIndexTicket.Table.Indexes)
        {
            if (index.Value.Type != IndexType.Multi)
                continue;

            if (index.Value.MultiRows is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A multi index tree wasn't found"
                );

            BTreeMulti<ColumnValue> multiIndex = index.Value.MultiRows;

            ColumnValue? multiKeyValue = GetColumnValue(saveMultiKeysIndexTicket.Table, saveMultiKeysIndexTicket.Ticket, index.Value.Column);
            if (multiKeyValue is null)
                continue;

            SaveMultiKeyIndexTicket multiKeyTicket = new(
                tablespace: tablespace,
                multiIndex: multiIndex,
                txnId: saveMultiKeysIndexTicket.Ticket.TxnId,
                multiKeyValue: multiKeyValue,
                rowTuple: saveMultiKeysIndexTicket.RowTuple,
                locks: saveMultiKeysIndexTicket.Locks,
                modifiedPages: saveMultiKeysIndexTicket.ModifiedPages
            );

            await indexSaver.Save(multiKeyTicket);
        }
    }
}
