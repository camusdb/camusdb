
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class ExecuteSQLRequest
{
    public string? DatabaseName { get; set; }

    public string? Sql { get; set; }

    public Dictionary<string, ColumnValue>? Parameters { get; set; }
}
