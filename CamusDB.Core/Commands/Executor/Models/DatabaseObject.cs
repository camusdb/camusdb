
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed record DatabaseObject
{
    public DatabaseObjectType Type { get; set; }

    public string? Id { get; set; }

    public string? Name { get; set; }

    public string? StartOffset { get; set; }

    public Dictionary<string, DatabaseIndexObject>? Indexes { get; set; }
}
