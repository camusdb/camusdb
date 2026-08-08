
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.App.Models;

public sealed class ExecuteNonSQLQueryResponse
{
    public string Status { get; set; }

    public int Rows { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    /// <summary>
    /// Set when the statement succeeded but the caller should look twice — today, a time-travel copy
    /// that read no rows, which may mean the requested history had already been reclaimed. Null
    /// otherwise. Carried in the response because a client cannot see the server's log.
    /// </summary>
    public string? Warning { get; set; }

    public HLCTimestamp? CausalToken { get; set; }

    /// <summary>
    /// Total server-side processing time for this request in milliseconds — measured from the
    /// moment the controller began handling it (request-body parse, SQL parse, execution, and
    /// commit) until the response was built. Excludes network transit and client time, so a large
    /// gap between this and the client's observed latency isolates network/connection overhead.
    /// </summary>
    public double? ServerTimeMs { get; set; }

    public ExecuteNonSQLQueryResponse(string status, int rows)
    {
        Status = status;
        Rows = rows;
    }

    public ExecuteNonSQLQueryResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
