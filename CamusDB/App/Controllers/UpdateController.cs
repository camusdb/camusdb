
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.App.Models;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class UpdateController : CommandsController
{
    public UpdateController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/update-by-id")]
    public async Task<JsonResult> UpdateById()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            UpdateByIdRequest? request = JsonSerializer.Deserialize<UpdateByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("UpdateById request is not valid");

            UpdateByIdTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                id: request.Id ?? "",
                columnValues: request.ColumnValues ?? new()
            );

            await executor.UpdateById(ticket);

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