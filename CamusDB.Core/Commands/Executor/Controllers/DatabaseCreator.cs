
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Creates a new database. At this moment, it only creates the data directory where
/// the storage engine will store the objects and records of the database.
/// </summary>
internal sealed class DatabaseCreator
{
    private readonly ILogger<ICamusDB> logger;

    public DatabaseCreator(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    // Called only after the registry has been checked and the id allocated.
    // Creates the physical data directories for a new database (standalone mode only).
    // In cluster mode, data lives in the shared Kahuna node — no directories needed.
    public void Create(string name, string id)
    {
        string dbPath = Path.Combine(CamusConfig.DataDirectory, id);
        Directory.CreateDirectory(Path.Combine(dbPath, "kv"));
        Directory.CreateDirectory(Path.Combine(dbPath, "wal"));

        Log.LogDatabaseCreated(logger, name, dbPath);
    }
}
