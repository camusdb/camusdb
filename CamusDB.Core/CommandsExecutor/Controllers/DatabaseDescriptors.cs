
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal class DatabaseDescriptors : IDisposable
{
    public readonly SemaphoreSlim Semaphore = new(1, 1);

    public readonly Dictionary<string, DatabaseDescriptor> Descriptors = new();

    public DatabaseDescriptors()
    {
        AppDomain.CurrentDomain.ProcessExit += DatabaseDescriptors_Dtor;
    }

    private void DatabaseDescriptors_Dtor(object? sender, EventArgs e)
    {
        foreach (KeyValuePair<string, DatabaseDescriptor> keyValuePair in Descriptors)
            Console.WriteLine(keyValuePair.Key);
    }

    public void Dispose()
    {
        AppDomain.CurrentDomain.ProcessExit -= DatabaseDescriptors_Dtor;
    }
}
