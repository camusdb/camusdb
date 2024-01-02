
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

    public int Total { get; set; }

    public List<Dictionary<string, ColumnValue>> Rows { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public QueryResponse(string status, int total, List<Dictionary<string, ColumnValue>> rows)
    {        
        Status = status;
        Total = total;
        Rows = rows;
    }

    public QueryResponse(string status, string code, string message)
    {
        Status = status;
        Rows = new();
        Code = code;
        Message = message;
    }
}
