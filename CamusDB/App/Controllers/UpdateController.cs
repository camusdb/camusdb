
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class UpdateController : CommandsController
{
    public UpdateController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
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
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "UpdateById request is not valid");

            TransactionState txnState;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
                txnState = await transactions.Start().ConfigureAwait(false);

            UpdateByIdTicket ticket = new(
                txnState: txnState,
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                id: request.Id ?? "",
                values: request.Values ?? new()
            );

            int updatedRows = await executor.UpdateById(ticket);

            return new JsonResult(new UpdateResponse("ok", updatedRows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new UpdateResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new UpdateResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/update")]
    public async Task<JsonResult> Update()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            UpdateRequest? request = JsonSerializer.Deserialize<UpdateRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Update request is not valid");

            TransactionState txnState;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
                txnState = await transactions.Start().ConfigureAwait(false);

            UpdateTicket ticket = new(
                txnState: txnState,
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",                
                plainValues: request.Values ?? new(),
                exprValues: null,
                where: null,
                filters: request.Filters ?? new(),
                parameters: null
            );

            int updatedRows = await executor.Update(ticket);

            return new JsonResult(new UpdateResponse("ok", updatedRows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new UpdateResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new UpdateResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}