﻿
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

    public string? Code { get; set; }

    public string? Message { get; set; }

    public ExecuteDDLSQLResponse(string status)
    {
        Status = status;
    }

    public ExecuteDDLSQLResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
