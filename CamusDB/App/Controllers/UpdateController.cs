
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
    public UpdateController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {

    }

    [HttpPost]
    [Route("/update")]
    public async Task<JsonResult> Update()
    {
        try
        {
            UpdateRequest? request = await JsonSerializer.DeserializeAsync<UpdateRequest>(Request.Body, jsonOptions).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Update request is not valid");

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
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
                    return new JsonResult(new UpdateResponse("ok", result.UpdatedRows));
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
            int updatedRows = 0;
            Kommander.Time.HLCTimestamp causalToken = default;

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.StartAsync(request.DatabaseName ?? "", null, null, cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    UpdateTicket ticket = new(
                        txnState: tx,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        plainValues: request.Values ?? new(),
                        exprValues: null,
                        where: null,
                        filters: request.Filters ?? new(),
                        parameters: null
                    );
                    UpdateResult r = await executor.Update(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                    updatedRows = r.UpdatedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            if (options.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody).ConfigureAwait(false);
            else
                await AutocommitBody(CancellationToken.None).ConfigureAwait(false);

            return new JsonResult(new UpdateResponse("ok", updatedRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new UpdateResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new UpdateResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
