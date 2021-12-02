
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

        if (Directory.Exists(Config.DataDirectory + "/" + name))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");

        Directory.CreateDirectory(Config.DataDirectory + "/" + name);

        await InitializeDatabaseFiles(name);
    }

    private static async Task InitializeDatabaseFiles(string name)
    {
        byte[] initialized = new byte[Config.InitialTableSpaceSize];

        string absolutePath = Directory.GetCurrentDirectory();

        await Task.WhenAll(new Task[]
        {
            File.WriteAllBytesAsync(absolutePath + "/" + Config.DataDirectory + "/" + name + "/tablespace0", initialized),
            File.WriteAllBytesAsync(absolutePath + "/" + Config.DataDirectory + "/" + name + "/schema", initialized),
            File.WriteAllBytesAsync(absolutePath + "/" + Config.DataDirectory + "/" + name + "/system", initialized)
        });

        // @todo catch IO Exceptions
        // @todo verify tablespaces were created sucessfully

        Console.WriteLine("Database {0} tablespaces created at {1}", name, absolutePath + "/" + Config.DataDirectory + "/" + name);
    }
}
