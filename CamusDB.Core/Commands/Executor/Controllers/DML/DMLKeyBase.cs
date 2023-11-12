
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal abstract class DMLKeyBase
{
    protected static ColumnValue? GetColumnValue(TableDescriptor table, InsertTicket ticket, string name)
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
}
