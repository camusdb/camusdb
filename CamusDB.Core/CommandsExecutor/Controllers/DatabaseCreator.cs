
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
    public async Task Create(CreateDatabaseTicket ticket)
    {
        string name = ticket.DatabaseName;
        
        string dbPath = Path.Combine(Config.DataDirectory, name);

        if (Directory.Exists(dbPath))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");

        Directory.CreateDirectory(dbPath);

        await InitializeDatabaseFiles(name, dbPath);
    }

    private static async Task InitializeDatabaseFiles(string name, string dbPath)
    {
        byte[] initialized = new byte[Config.InitialTableSpaceSize];
               
        await Task.WhenAll(new Task[]
        {
            File.WriteAllBytesAsync(Path.Combine(dbPath, "tablespace0"), initialized),
            File.WriteAllBytesAsync(Path.Combine(dbPath, "schema"), initialized),
            File.WriteAllBytesAsync(Path.Combine(dbPath, "system"), initialized),
            File.WriteAllBytesAsync(Path.Combine(dbPath, "journal"), new byte[0])
        });

        // @todo catch IO Exceptions
        // @todo verify tablespaces were created sucessfully

        Console.WriteLine("Database {0} tablespaces created at {1}", name, dbPath);
    }
}
