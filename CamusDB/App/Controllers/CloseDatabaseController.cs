
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
    public CloseDatabaseController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/close-db")]
    public async Task<JsonResult> CloseDatabase()
    {
        try
        {
            using var reader = new StreamReader(Request.Body);
            var body = await reader.ReadToEndAsync();

            CloseDatabaseRequest? request = JsonSerializer.Deserialize<CloseDatabaseRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("CreateDatabase request is not valid");

            CloseDatabaseTicket ticket = new(
                name: request.DatabaseName ?? ""
            );

            await executor.CloseDatabase(ticket);

            return new JsonResult(new CloseDatabaseResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CloseDatabaseResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CloseDatabaseResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
