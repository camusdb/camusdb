
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class QueryExecutor
{
    public RowReader rowReader = new();

    public async Task<List<List<ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        foreach (BTreeEntry entry in table.Rows.EntriesTraverse())
        {
            if (entry.Value is null)
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value.Value);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            rows.Add(rowReader.Unserialize(table.Schema!, data));
        }

        return rows;
    }

    public async Task<List<List<ColumnValue>>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace!;

        List<List<ColumnValue>> rows = new();

        int? pageOffset = table.Indexes["pk"].Get(ticket.Id);

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} has an empty page data", ticket.Id);
            return rows;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.Value);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            return rows;
        }

        rows.Add(rowReader.Unserialize(table.Schema!, data));

        return rows;
    }

    
}
