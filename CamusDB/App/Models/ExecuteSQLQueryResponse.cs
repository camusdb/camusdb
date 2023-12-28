
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class ExecuteSQLQueryResponse
{
    public string Status { get; set; }

    public List<Dictionary<string, ColumnValue>>? Rows { get; set; } 

    public string? Code { get; set; }

    public string? Message { get; set; }

    public ExecuteSQLQueryResponse(string status, List<Dictionary<string, ColumnValue>> rows)
    {
        Status = status;
        Rows = rows;
    }

    public ExecuteSQLQueryResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
