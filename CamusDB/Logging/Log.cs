
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.App;

internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "{Body}")]
    public static partial void LogRequestBody(ILogger logger, string body);

    /// <summary>
    /// Logs the SQL about to execute. Debug level and source-generated, so when Debug is disabled the
    /// call is a no-op — no string formatting, no newline replacement, and no console I/O on the hot
    /// request path. The SQL is passed as a structured property (never mutated with Replace).
    /// </summary>
    [LoggerMessage(Level = LogLevel.Debug, Message = "sql: {Sql}")]
    public static partial void LogExecutingSql(ILogger logger, string sql);
}
