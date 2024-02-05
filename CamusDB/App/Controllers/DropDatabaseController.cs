
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
using CamusDB.Core.Transactions;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class DropDatabaseController : CommandsController
{
    public DropDatabaseController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/drop-db")]
    public async Task<JsonResult> DropDatabase()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            DropDatabaseRequest? request = JsonSerializer.Deserialize<DropDatabaseRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DropDatabase request is not valid");

            DropDatabaseTicket ticket = new(
                name: request.DatabaseName ?? ""                
            );

            await executor.DropDatabase(ticket);

            return new JsonResult(new DropDatabaseResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DropDatabaseResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DropDatabaseResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
