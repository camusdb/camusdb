
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using System.Text.Json;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class TransactionsController : CommandsController
{
    public TransactionsController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/start-transaction")]
    public async Task<JsonResult> StartTransaction()
    {
        try
        {
            TransactionState txState = await transactions.Start();

            return new JsonResult(new StartTransactionResponse("ok", txState.TxnId.L, txState.TxnId.C));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new StartTransactionResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new StartTransactionResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/commit-transaction")]
    public async Task<JsonResult> CommitTransaction()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            CommitTransactionRequest? request = JsonSerializer.Deserialize<CommitTransactionRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            TransactionState txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));

            var database = await executor.OpenDatabase(request.DatabaseName ?? "");

            await transactions.Commit(database, txnState);
            
            return new JsonResult(new CommitTransactionResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CommitTransactionResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CommitTransactionResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/rollback-transaction")]
    public async Task<JsonResult> RollbackTransaction()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            CommitTransactionRequest? request = JsonSerializer.Deserialize<CommitTransactionRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            TransactionState txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));

            await transactions.Rollback(txnState);

            return new JsonResult(new CommitTransactionResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CommitTransactionResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CommitTransactionResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}