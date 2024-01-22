
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

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

    public bool Create(CreateDatabaseTicket ticket)
    {
        string name = ticket.DatabaseName;
                
        string dbPath = Path.Combine(CamusConfig.DataDirectory, name);

        if (Directory.Exists(dbPath))
        {
            if (ticket.IfNotExists)
                return false;

            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");
        }

        if (name == "information_schema")
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Reserved database name");

        logger.LogInformation("Database {Name} successfully created at {DbPath}", name, dbPath);

        return true;
    }
}
