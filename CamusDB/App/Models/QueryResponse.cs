
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class QueryResponse
{
    public string Status { get; set; }

    public List<List<ColumnValue>> Rows { get; set; }

    public QueryResponse(string status, List<List<ColumnValue>> rows)
    {
        Status = status;
        Rows = rows;
    }
}
