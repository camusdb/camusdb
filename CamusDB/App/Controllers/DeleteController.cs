
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class DeleteController : CommandsController
{
    public DeleteController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/delete-by-id")]
    public async Task<JsonResult> DeleteById()
    {
        try
        {
            using var reader = new StreamReader(Request.Body);
            var body = await reader.ReadToEndAsync();

            DeleteByIdRequest? request = JsonSerializer.Deserialize<DeleteByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("DeleteById request is not valid");

            DeleteByIdTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                id: request.Id
            );

            await executor.DeleteById(ticket);

            return new JsonResult(new DeleteResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message));
        }
    }
}

