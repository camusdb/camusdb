
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.Extensions.Logging;

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

    public async Task Drop(string name)
    {
        if (databaseDescriptors.Descriptors.TryRemove(name, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
        {
            DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;
            await databaseDescriptor.Kahuna.DisposeAsync().ConfigureAwait(false);
            databaseDescriptor.Dispose();
        }

        logger.LogInformation("Database {Name} dropped", name);
    }
}
