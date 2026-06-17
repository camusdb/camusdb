
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseDropper
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseDropper(DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
    }

    public async Task Drop(string id)
    {
        if (!databaseDescriptors.Descriptors.TryRemove(id, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
            return;

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;

        // Signal all new AddRef calls to fail immediately with DatabaseDoesntExist.
        databaseDescriptor.MarkDropped();

        // Wait for any operations already holding a use-reference to finish.
        // Cap at 30 s to avoid blocking forever on a stuck in-flight operation.
        try
        {
            await databaseDescriptor.WhenDrainedAsync()
                .WaitAsync(TimeSpan.FromSeconds(30))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            logger.LogWarning(
                "DropDatabase for id={Id} timed out waiting for in-flight operations; forcing disposal",
                id);
        }

        if (databaseDescriptor.OwnsKahuna)
            await databaseDescriptor.Kahuna.DisposeAsync().ConfigureAwait(false);

        databaseDescriptor.Dispose();
        Log.LogDatabaseDropped(logger, databaseDescriptor.Name);

        // Remove the on-disk directory (standalone mode only).
        // In cluster mode OwnsKahuna=false — data lives in the shared node.
        if (databaseDescriptor.OwnsKahuna)
        {
            string dataPath = Path.Combine(CamusConfig.DataDirectory, id);
            if (Directory.Exists(dataPath))
            {
                try
                {
                    Directory.Delete(dataPath, recursive: true);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "Failed to delete data directory for dropped database '{Name}' (id={Id})",
                        databaseDescriptor.Name, id);
                }
            }
        }
    }
}
