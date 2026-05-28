
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class DeleteController : CommandsController
{
    public DeleteController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/delete")]
    public async Task<JsonResult> Delete()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            DeleteRequest? request = JsonSerializer.Deserialize<DeleteRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Delete request is not valid");

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                (newTransaction, txnState) = await BeginOrResumeAsync(
                    request.DatabaseName,
                    request.TxnIdPT,
                    request.TxnIdCounter
                ).ConfigureAwait(false);

                DeleteTicket ticket = new(
                    txnState: txnState,
                    databaseName: request.DatabaseName ?? "",
                    tableName: request.TableName ?? "",
                    where: null,
                    filters: request.Filters ?? new()
                );

                DeleteResult result = await executor.Delete(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.CommitAsync(result.Database, txnState).ConfigureAwait(false);

                return new JsonResult(new DeleteResponse("ok", result.DeletedRows));
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

            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
