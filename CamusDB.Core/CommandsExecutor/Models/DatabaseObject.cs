
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class DatabaseObject
{
    public DatabaseObjectType Type { get; set; }

    public string? Name { get; set; }

    public int StartOffset { get; set; }

    public Dictionary<string, int>? Indexes { get; set; }
}
