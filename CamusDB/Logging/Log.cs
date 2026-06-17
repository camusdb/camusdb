
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
}
