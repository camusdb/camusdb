
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class ExecuteDDLSQLResponse
{
    public string Status { get; set; }

    /// <summary>
    /// Rows written by a DDL statement that also loads data — today only
    /// <c>CREATE TABLE … AS SELECT</c>. Zero for every other DDL statement, which writes no rows.
    /// </summary>
    public int Rows { get; set; }

    /// <summary>
    /// Set when the statement succeeded but the caller should look twice — today, a time-travel copy
    /// that read no rows, which may mean the requested history had already been reclaimed. Null
    /// otherwise. Carried in the response because a client cannot see the server's log.
    /// </summary>
    public string? Warning { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public ExecuteDDLSQLResponse(string status)
    {
        Status = status;
    }

    public ExecuteDDLSQLResponse(string status, int rows, string? warning = null)
    {
        Status = status;
        Rows = rows;
        Warning = warning;
    }

    public ExecuteDDLSQLResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
