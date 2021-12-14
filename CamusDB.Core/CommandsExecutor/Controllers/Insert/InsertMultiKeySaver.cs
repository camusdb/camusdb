
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

internal sealed class InsertMultiKeySaver : InsertKeyBase
{
    private readonly IndexSaver indexSaver = new();

    public async Task UpdateMultiKeys(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket, BTreeTuple rowTuple)
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
}
