
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
    public DeleteController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {

    }

    [HttpPost]
    [Route("/delete")]
    public async Task<JsonResult> Delete()
    {
        try
        {
            DeleteRequest? request = await JsonSerializer.DeserializeAsync<DeleteRequest>(Request.Body, jsonOptions, HttpContext.RequestAborted).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Delete request is not valid");

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                using RunningStatement statement = transactions.EnterStatement(request.TxnIdPT, request.TxnIdCounter);
                KvTransaction txnState = statement.Transaction;
                try
                {
                    // A refused request ends the transaction, as a failed SQL statement does.
                    RowWriteFilterRule.Require(request.Filters?.Count ?? 0, RowWriteFilterRule.Delete);
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
                    await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            RowWriteFilterRule.Require(request.Filters?.Count ?? 0, RowWriteFilterRule.Delete);

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
                    causalToken = await transactions.CommitAsync(r.Database, tx, CancellationToken.None).ConfigureAwait(false);
                    deletedRows = r.DeletedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, CancellationToken.None).ConfigureAwait(false);
                    throw;
                }
            }

            // The client's disconnect reaches the begin and the retries, and never the commit or the
            // rollback: a finalize that a disconnect cuts leaves its locks to expire.
            CancellationToken requestAborted = HttpContext.RequestAborted;
            if (options.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody, cancellationToken: requestAborted).ConfigureAwait(false);
            else
                await AutocommitBody(requestAborted).ConfigureAwait(false);

            return new JsonResult(new DeleteResponse("ok", deletedRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);

            return new JsonResult(new DeleteResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (OperationCanceledException) when (ClientClosedRequest)
        {
            return new JsonResult(new DeleteResponse("failed", UnclassifiedErrorCode, LogClientClosedRequest())) { StatusCode = StatusCodes.Status499ClientClosedRequest };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new DeleteResponse("failed", UnclassifiedErrorCode, LogUnclassifiedFailure(e))) { StatusCode = 500 };
        }
    }
}
