
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.App.Models;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.App.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class UpdateController : CommandsController
{
    public UpdateController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/update")]
    public async Task<JsonResult> Update()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            UpdateRequest? request = JsonSerializer.Deserialize<UpdateRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Update request is not valid");

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                (newTransaction, txnState) = await BeginOrResumeAsync(
                    request.DatabaseName,
                    request.TxnIdPT,
                    request.TxnIdCounter
                ).ConfigureAwait(false);

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

                UpdateResult result = await executor.Update(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.CommitAsync(result.Database, txnState).ConfigureAwait(false);

                return new JsonResult(new UpdateResponse("ok", result.UpdatedRows));
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);

                throw;
            }
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new UpdateResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new UpdateResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
