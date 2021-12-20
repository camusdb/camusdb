
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class SystemSchema
{
    public Dictionary<string, DatabaseObject> Objects { get; set; } = new();

    public SemaphoreSlim Semaphore = new(1, 1);
}
