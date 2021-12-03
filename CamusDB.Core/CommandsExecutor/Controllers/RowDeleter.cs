
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

        Console.WriteLine("Data Pk={0} is at page offset {1}", ticket.Id, pageOffset.SlotTwo);

        index.UniqueRows.Remove(columnId);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;

        Console.WriteLine("Row {0} deleted from {1}, Time taken: {2}", ticket.Id, pageOffset.SlotTwo, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
