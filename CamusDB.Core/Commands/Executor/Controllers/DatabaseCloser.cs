
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Closes database descriptors, stopping the associated Kahuna node and freeing memory.
/// </summary>
internal sealed class DatabaseCloser : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseCloser(DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
    }

    public async Task Close(string name)
    {
        if (!databaseDescriptors.Descriptors.TryRemove(name, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
            return;

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;

        await databaseDescriptor.Kahuna.DisposeAsync().ConfigureAwait(false);
        databaseDescriptor.Dispose();

        logger.LogInformation("Database {Name} closed", name);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (string name in databaseDescriptors.Descriptors.Keys.ToList())
            await Close(name).ConfigureAwait(false);
    }
}
