
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core;

public class CamusStartup
{
    private CommandExecutor executor;

    public CamusStartup(CommandExecutor? executor)
    {
        if (executor is null)
            throw new CamusDBException("?", "failed to initialize");

        this.executor = executor;
    }

    public async Task Initialize()
    {
        await CheckRecovery();
    }

    private async Task CheckRecovery()
    {
        string dbPath = Config.DataDirectory;

        string[] subdirectoryEntries = Directory.GetDirectories(dbPath);
        foreach (string subdirectory in subdirectoryEntries)
            await RecoverDatabase(subdirectory);
    }

    private async Task RecoverDatabase(string path)
    {
        string lockPath = Path.Combine(path, "camus.lock");

        if (!File.Exists(lockPath))
            return;        
        
        string[] parts = path.Split(Path.DirectorySeparatorChar);
        string databaseName = parts[^1];

        Console.WriteLine("Database recovery started for {0}", databaseName);

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName);
        await executor.CloseDatabase(new CloseDatabaseTicket(databaseName));

        Console.WriteLine("Database recovery completed for {0}", databaseName);
    }
}