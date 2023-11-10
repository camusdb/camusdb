
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;

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

    public Task Initialize()
    {
        //await CheckRecovery();
        return Task.CompletedTask;
    }
}