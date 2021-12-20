
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseCreator
{
    public void Create(CreateDatabaseTicket ticket)
    {
        string name = ticket.DatabaseName;
        
        string dbPath = Path.Combine(Config.DataDirectory, name);

        if (Directory.Exists(dbPath))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");

        Directory.CreateDirectory(dbPath);

        //await InitializeDatabaseFiles(name, dbPath);

        Console.WriteLine("Database {0} successfully created at {1}", name, dbPath);
    }
}
