
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public static class DatabaseDescriptors
{
    public static readonly SemaphoreSlim Semaphore = new(1, 1);

    public static readonly Dictionary<string, DatabaseDescriptor> Descriptors = new();
}
