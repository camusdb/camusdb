
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
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class QueryController : CommandsController
{
    public QueryController(CommandExecutor executor) : base(executor)
    {

    }

    [HttpPost]
    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            QueryRequest? request = JsonSerializer.Deserialize<QueryRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("Query request is not valid");

            QueryTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                index: null,
                filters: request.Filters
            );

            List<Dictionary<string, ColumnValue>> rows = new();

            await foreach (Dictionary<string, ColumnValue> row in await executor.Query(ticket))
                rows.Add(row);

            return new JsonResult(new QueryResponse("ok", rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new QueryResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new QueryResponse("failed", "CA0000", e.Message));
        }
    }

    [HttpPost]
    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            QueryByIdRequest? request = JsonSerializer.Deserialize<QueryByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("QueryById request is not valid");

            QueryByIdTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                id: request.Id ?? ""
            );

            List<Dictionary<string, ColumnValue>> rows = new();

            await foreach (Dictionary<string, ColumnValue> row in await executor.QueryById(ticket))
                rows.Add(row);

            return new JsonResult(new QueryResponse("ok", rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new QueryResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new QueryResponse("failed", "CA0000", e.Message));
        }
    }
}

