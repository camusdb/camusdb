
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.CommandsExecutor.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseDropper
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseDropper(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async Task Drop(string name)
    {
        if (!databaseDescriptors.Descriptors.TryGetValue(name, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
        {
            DropInternal(name);
            return;
        }

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;

        databaseDescriptor.Storage.Dispose();

        databaseDescriptors.Descriptors.TryRemove(name, out _);

        DropInternal(name);

        Console.WriteLine("Database {0} dropped", name);
    }

    private static void DropInternal(string name)
    {
        string dbPath = Path.Combine(CamusConfig.DataDirectory, name);

        if (!Directory.Exists(dbPath))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseDoesntExist, "Database doesn't exist");

        // The database is not deleted, but its data directory is renamed.
        // This allows saving data in case it is deleted by mistake.

        string newDbPath = Path.Combine(CamusConfig.DataDirectory, "_" + name + "_" + DateTime.UtcNow.ToString("yyyy-MM-ddTHH-mm-ss-fffffff"));

        Directory.Move(dbPath, newDbPath);
    }
}
