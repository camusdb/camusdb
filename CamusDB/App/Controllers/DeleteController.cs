
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
            DeleteRequest? request = await JsonSerializer.DeserializeAsync<DeleteRequest>(Request.Body, jsonOptions).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Delete request is not valid");

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    DeleteTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        where: null,
                        filters: request.Filters ?? new()
                    );
                    DeleteResult result = await executor.Delete(ticket).ConfigureAwait(false);
                    return new JsonResult(new DeleteResponse("ok", result.DeletedRows));
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit: retry transparently on transient serialization failures when the
            // resolved level is Serializable; run once for Read Committed.
            int deletedRows = 0;
            Kommander.Time.HLCTimestamp causalToken = default;

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.StartAsync(request.DatabaseName ?? "", null, null, cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    DeleteTicket ticket = new(
                        txnState: tx,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        where: null,
                        filters: request.Filters ?? new()
                    );
                    DeleteResult r = await executor.Delete(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                    deletedRows = r.DeletedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody).ConfigureAwait(false);
            else
                await AutocommitBody(CancellationToken.None).ConfigureAwait(false);

            return new JsonResult(new DeleteResponse("ok", deletedRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
