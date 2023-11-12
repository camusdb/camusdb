
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Nito.AsyncEx;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseCloser : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseCloser(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async Task Close(string name)
    {
        if (!databaseDescriptors.Descriptors.TryGetValue(name, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
            return;

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;

        databaseDescriptor.TableSpace?.Dispose();

        databaseDescriptor.DbHandler.Dispose();

        databaseDescriptors.Descriptors.TryRemove(name, out _);

        Console.WriteLine("Database {0} closed", name);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<string, AsyncLazy<DatabaseDescriptor>> keyValuePair in databaseDescriptors.Descriptors)
            await Close(keyValuePair.Key);
    }
}
