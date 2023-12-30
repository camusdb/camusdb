
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
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            DeleteByIdRequest? request = JsonSerializer.Deserialize<DeleteByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DeleteById request is not valid");

            DeleteByIdTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                id: request.Id ?? ""
            );

            int deletedRows = await executor.DeleteById(ticket);

            return new JsonResult(new DeleteResponse("ok", deletedRows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/delete")]
    public async Task<JsonResult> Delete()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            DeleteRequest? request = JsonSerializer.Deserialize<DeleteRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Delete request is not valid");

            DeleteTicket ticket = new(
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                where: null,
                filters: request.Filters ?? new()
            );

            int deletedRows = await executor.Delete(ticket);

            return new JsonResult(new DeleteResponse("ok", deletedRows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
