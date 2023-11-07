
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseCloser : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseCloser(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async ValueTask Close(string name)
    {
        if (!databaseDescriptors.Descriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return;

        try
        {
            await databaseDescriptors.Semaphore.WaitAsync();

            if (databaseDescriptor.TableSpace is not null)
                databaseDescriptor.TableSpace.Dispose();

            databaseDescriptor.DbHandler.Dispose();

            databaseDescriptors.Descriptors.Remove(name);

            Console.WriteLine("Database {0} closed", name);
        }
        finally
        {
            databaseDescriptors.Semaphore.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<string, DatabaseDescriptor> keyValuePair in databaseDescriptors.Descriptors)
            await Close(keyValuePair.Key);
    }
}
