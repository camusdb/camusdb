
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using System.Text.Json;
using CamusDB.App.Services;
using System;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class TransactionsController : CommandsController
{
    public TransactionsController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/start-transaction")]
    public async Task<JsonResult> StartTransaction()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            StartTransactionRequest? request = string.IsNullOrWhiteSpace(body)
                ? null
                : JsonSerializer.Deserialize<StartTransactionRequest>(body, jsonOptions);

            if (string.IsNullOrEmpty(request?.DatabaseName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required");

            CamusIsolationLevel? isolationLevel = request.IsolationLevel is null ? null
                : Enum.TryParse(request.IsolationLevel, ignoreCase: true, out CamusIsolationLevel parsedLevel) && Enum.IsDefined(parsedLevel)
                    ? parsedLevel
                    : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown isolation level: {request.IsolationLevel}");

            CamusTransactionMode? transactionMode = request.TransactionMode is null ? null
                : Enum.TryParse(request.TransactionMode, ignoreCase: true, out CamusTransactionMode parsedMode) && Enum.IsDefined(parsedMode)
                    ? parsedMode
                    : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown transaction mode: {request.TransactionMode}");

            KvTransaction txState = await transactions.StartAsync(request.DatabaseName, isolationLevel, transactionMode).ConfigureAwait(false);

            return new JsonResult(new StartTransactionResponse("ok", txState.TransactionId.L, txState.TransactionId.C));
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
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            CommitTransactionRequest? request = JsonSerializer.Deserialize<CommitTransactionRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            if (string.IsNullOrEmpty(request.DatabaseName))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "DatabaseName is required");

            KvTransaction txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);

            DatabaseDescriptor database = await executor.OpenDatabase(request.DatabaseName).ConfigureAwait(false);

            await transactions.CommitAsync(database, txnState).ConfigureAwait(false);
            
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
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            CommitTransactionRequest? request = JsonSerializer.Deserialize<CommitTransactionRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            KvTransaction txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);

            await transactions.RollbackAsync(txnState).ConfigureAwait(false);

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
