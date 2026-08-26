
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
using Kahuna.Shared.KeyValue;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class TransactionsController : CommandsController
{
    public TransactionsController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
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

            KeyValueTransactionLocking? locking = request.Locking is null ? null
                : Enum.TryParse(request.Locking, ignoreCase: true, out KeyValueTransactionLocking parsedLocking) && Enum.IsDefined(parsedLocking)
                    ? parsedLocking
                    : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown locking mode: {request.Locking}");

            TransactionPriority? priority = request.Priority is null ? null
                : Enum.TryParse(request.Priority, ignoreCase: true, out TransactionPriority parsedPriority) && Enum.IsDefined(parsedPriority)
                    ? parsedPriority
                    : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown transaction priority: {request.Priority}");

            // Explicit interactive transaction: defer the Kahuna session start so a following
            // SET TRANSACTION LOCKING / SET TRANSACTION PRIORITY can still choose those values
            // (Kahuna pins both at session start). The session opens on the first statement's
            // write/lock/folded-read.
            KvTransaction txState = await transactions.StartAsync(
                request.DatabaseName, isolationLevel, transactionMode, locking, deferStart: true,
                priority: priority, sessionOwned: true).ConfigureAwait(false);

            return new JsonResult(new StartTransactionResponse("ok", txState.ClientId.L, txState.ClientId.C));
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);

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
            LogCommandFailure(e);

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

            // Idempotent by design: a transaction whose statement already failed was rolled back and
            // untracked by the handler that caught the error, and the reaper does the same to an
            // abandoned one. The client sending its own rollback afterwards is correct behavior, and
            // the transaction is in exactly the state it asked for, so report success rather than
            // "Unknown transaction".
            await transactions.RollbackByIdAsync(request.TxnIdPT, request.TxnIdCounter).ConfigureAwait(false);

            return new JsonResult(new CommitTransactionResponse("ok"));
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);

            return new JsonResult(new CommitTransactionResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CommitTransactionResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
