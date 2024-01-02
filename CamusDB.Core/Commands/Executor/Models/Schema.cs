
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class Schema : IDisposable
{
    public Dictionary<string, TableSchema> Tables { get; set; } = new();

    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public void Dispose()
    {
        Semaphore?.Dispose();
    }
}

