
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
public sealed class QueryController : CommandsController
{
    public QueryController(CommandExecutor executor) : base(executor)
    {

    }

    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        try
        {
            QueryTicket ticket = new(
                database: "test",
                name: "my_table"
            );

            List<List<ColumnValue>> rows = await executor.Query(ticket);

            return new JsonResult(new QueryResponse("ok", rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message));
        }
    }

    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        QueryByIdTicket ticket = new(
            database: "test",
            name: "my_table",
            id: 2205016
        );

        List<List<ColumnValue>> rows = await executor.QueryById(ticket);

        return new JsonResult(new QueryResponse("ok", rows));
    }
}

