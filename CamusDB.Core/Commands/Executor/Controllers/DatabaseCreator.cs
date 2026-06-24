
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Creates a new database. Both standalone and cluster modes store all data in the single
/// shared Kahuna node — no per-database directories are created.
/// </summary>
internal sealed class DatabaseCreator
{
    private readonly ILogger<ICamusDB> logger;

    public DatabaseCreator(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    public Task Create(string name, string id)
    {
        Log.LogDatabaseCreated(logger, name, id);
        return Task.CompletedTask;
    }
}
