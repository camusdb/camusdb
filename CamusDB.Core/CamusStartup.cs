
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Config.Models;

namespace CamusDB.Core;

public sealed class CamusStartup
{
    private CommandExecutor executor;

    public CamusStartup(CommandExecutor? executor)
    {
        if (executor is null)
            throw new CamusDBException("?", "failed to initialize");

        this.executor = executor;
    }

    public Task Initialize(string ymlConfig)
    {
        ConfigReader reader = new();

        ConfigDefinition config = reader.Read(ymlConfig);        

        if (config.BufferPoolSize > 0)
            CamusDBConfig.BufferPoolSize = config.BufferPoolSize;

        if (!string.IsNullOrEmpty(config.DataDir))
            CamusDBConfig.DataDirectory = config.DataDir;

        //await CheckRecovery();
        return Task.CompletedTask;
    }
}