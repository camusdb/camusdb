
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class InsertController : CommandsController
{
    public InsertController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {

    }

    [HttpPost]
    [Route("/insert")]
    public async Task<JsonResult> Insert()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            Log.LogRequestBody(logger, body);

            InsertRequest? request = JsonSerializer.Deserialize<InsertRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Insert request is not valid");

            if (request.Values is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Insert values are not valid");

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    InsertTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        values: new List<Dictionary<string, ColumnValue>>() { request.Values }
                    );
                    InsertResult result = await executor.Insert(ticket).ConfigureAwait(false);
                    return new JsonResult(new InsertResponse("ok", result.InsertedRows));
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
            int insertedRows = 0;
            Kommander.Time.HLCTimestamp causalToken = default;

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.StartAsync(request.DatabaseName ?? "", null, null, cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    InsertTicket ticket = new(
                        txnState: tx,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        values: new List<Dictionary<string, ColumnValue>>() { request.Values }
                    );
                    InsertResult r = await executor.Insert(ticket).ConfigureAwait(false);
                    causalToken = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                    insertedRows = r.InsertedRows;
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

            return new JsonResult(new InsertResponse("ok", insertedRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new InsertResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
