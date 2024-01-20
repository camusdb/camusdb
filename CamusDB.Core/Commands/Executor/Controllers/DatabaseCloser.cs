
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Closes the database descriptors of a database, allowing the server to free all memory associated with it.
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

    /// <summary>
    /// Close the database descriptor and unloads the storage engine
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public async Task Close(string name)
    {
        if (!databaseDescriptors.Descriptors.TryGetValue(name, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
        {
            string dbPath = Path.Combine(CamusConfig.DataDirectory, name);

            if (!Directory.Exists(dbPath))
                throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

            return;
        }

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;        
        
        databaseDescriptor.Storage.Dispose();
        databaseDescriptor.GC.Dispose();

        databaseDescriptors.Descriptors.TryRemove(name, out _);

        logger.LogInformation("Database {0} closed", name);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<string, AsyncLazy<DatabaseDescriptor>> keyValuePair in databaseDescriptors.Descriptors)
            await Close(keyValuePair.Key);
    }
}
