
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using System.Text.Json;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CloseDatabaseController : CommandsController
{
    private readonly ILogger<ICamusDB> logger;

    public CloseDatabaseController(CommandExecutor executor, ILogger<ICamusDB> logger) : base(executor)
    {
        this.logger = logger;
    }

    [HttpPost]
    [Route("/close-db")]
    public async Task<JsonResult> CloseDatabase()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            CloseDatabaseRequest? request = JsonSerializer.Deserialize<CloseDatabaseRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "CreateDatabase request is not valid");

            CloseDatabaseTicket ticket = new(
                name: request.DatabaseName ?? ""
            );

            await executor.CloseDatabase(ticket);

            return new JsonResult(new CloseDatabaseResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CloseDatabaseResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CloseDatabaseResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
