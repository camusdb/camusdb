
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
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class DeleteController : CommandsController
{
    public DeleteController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/delete-by-id")]
    public async Task<JsonResult> DeleteById()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            DeleteByIdRequest? request = JsonSerializer.Deserialize<DeleteByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DeleteById request is not valid");
            
            TransactionState txnState;
            bool newTransaction = false;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
            {
                newTransaction = true;
                txnState = await transactions.Start().ConfigureAwait(false);
            }

            DeleteByIdTicket ticket = new(
                txnState: txnState,
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                id: request.Id ?? ""
            );

            DeleteByIdResult result = await executor.DeleteById(ticket).ConfigureAwait(false);

            if (newTransaction)
                await transactions.Commit(result.Database, txnState);

            return new JsonResult(new DeleteResponse("ok", result.DeletedRows));
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

            TransactionState txnState;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
                txnState = await transactions.Start().ConfigureAwait(false);

            DeleteTicket ticket = new(
                txnState: txnState,
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
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
