
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
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class ExecuteSQLController : CommandsController
{
    public ExecuteSQLController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/execute-sql")]
    public async Task<JsonResult> Execute()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("ExecuteSQL request is not valid");

            ExecuteSQLTicket ticket = new(
                database: request.DatabaseName ?? "",
                sql: request.Sql ?? "",
                parameters: request.Parameters
            );

            List<Dictionary<string, ColumnValue>> rows = new();

            await foreach (Dictionary<string, ColumnValue> row in await executor.ExecuteSQLQuery(ticket))
                rows.Add(row);

            return new JsonResult(new ExecuteSQLResponse("ok", rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new ExecuteSQLResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new ExecuteSQLResponse("failed", "CA0000", e.Message));
        }
    }
}
