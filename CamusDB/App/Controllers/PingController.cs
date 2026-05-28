
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class PingController : CommandsController
{
    public PingController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpGet]
    [Route("/ping")]
    public JsonResult PingServer()
    {        
        return new JsonResult(new PingResponse("ok", DateTime.UtcNow));
    }
}
