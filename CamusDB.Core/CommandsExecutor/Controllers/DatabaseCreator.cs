
/**
* This file is part of CamusDB
*
* For the full copyright and license information, please view the LICENSE.txt
* file that was distributed with this source code.
*/

using CamusDB.Core.BufferPool;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class DatabaseCreator
{   
    public async Task Create(string name)
    {
        name = name.ToLowerInvariant(); // @todo validate database name

        if (Directory.Exists(Config.DataDirectory + "/" + name))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");

        Directory.CreateDirectory(Config.DataDirectory + "/" + name);

        await InitializeDatabaseFiles(name);
    }

    private static async Task InitializeDatabaseFiles(string name)
    {
        byte[] initialized = new byte[Config.InitialTableSpaceSize];

        await Task.WhenAll(new Task[]
        {
            File.WriteAllBytesAsync(Config.DataDirectory + "/" + name + "/tablespace0", initialized),
            File.WriteAllBytesAsync(Config.DataDirectory + "/" + name + "/schema", initialized),
            File.WriteAllBytesAsync(Config.DataDirectory + "/" + name + "/system", initialized)
        });

        // @todo catch IO Exceptions
        // @todo verify tablespaces were created sucessfully

        Console.WriteLine("Database tablespaces created");
    }
}
