
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
public sealed class InsertController : CommandsController
{
    public InsertController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/insert")]
    public async Task<JsonResult> Insert()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            Console.WriteLine(body);

            InsertRequest? request = JsonSerializer.Deserialize<InsertRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Insert request is not valid");

            InsertTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                values: request.Values ?? new Dictionary<string, ColumnValue>() //,
                //forceFailureType: JournalFailureTypes.PreUpdateUniqueCheckpoint
            );

            await executor.Insert(ticket);
            return new JsonResult(new InsertResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}

