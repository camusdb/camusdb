
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class TransactionStarter
{
	private readonly ILogger<ICamusDB> logger;

    public TransactionStarter(ILogger<ICamusDB> logger)
	{
		this.logger = logger;
	}

	public Task StartTransaction()
	{
		return Task.CompletedTask;
	}
}
