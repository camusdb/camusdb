
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class QueryController : CommandsController
{
    public QueryController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {

    }

    /// <summary>The column values of each collected row, in order, for the <see cref="QueryResponse"/> shape.</summary>
    private static List<IReadOnlyDictionary<string, ColumnValue>> RowsOf(List<QueryResultRow> collected)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows = new(collected.Count);
        foreach (QueryResultRow row in collected)
            rows.Add(row.Row);
        return rows;
    }

    [HttpPost]
    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        // The client's disconnect signal. It reaches the scan through the ticket only: the
        // transaction lifecycle below keeps its own token, because a cancelled commit or rollback
        // abandons locks that only a lease expiry reclaims.
        CancellationToken requestAborted = HttpContext.RequestAborted;

        try
        {
            QueryRequest? request = await JsonSerializer.DeserializeAsync<QueryRequest>(Request.Body, jsonOptions, requestAborted).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    QueryTicket ticket = new(
                        txnState: txnState,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        index: null,
                        projection: null,
                        where: null,
                        filters: request.Filters,
                        orderBy: request.OrderBy,
                        limit: null,
                        offset: null,
                        parameters: null,
                        cancellationToken: requestAborted
                    );
                    (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket).ConfigureAwait(false);
                    List<IReadOnlyDictionary<string, ColumnValue>> rows = RowsOf(await BufferedResultCollector.CollectAsync(cursor, options).ConfigureAwait(false));
                    return new JsonResult(new QueryResponse("ok", rows.Count, rows));
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
            List<IReadOnlyDictionary<string, ColumnValue>> resultRows = [];
            Kommander.Time.HLCTimestamp causalToken = default;

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.BeginReadOnlyAsync(request.DatabaseName ?? "", request.CausalToken).ConfigureAwait(false);
                try
                {
                    QueryTicket ticket = new(
                        txnState: tx,
                        databaseName: request.DatabaseName ?? "",
                        tableName: request.TableName ?? "",
                        index: null,
                        projection: null,
                        where: null,
                        filters: request.Filters,
                        orderBy: request.OrderBy,
                        limit: null,
                        offset: null,
                        parameters: null,
                        cancellationToken: requestAborted
                    );
                    (DatabaseDescriptor db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket).ConfigureAwait(false);
                    List<IReadOnlyDictionary<string, ColumnValue>> rows = RowsOf(await BufferedResultCollector.CollectAsync(cursor, options).ConfigureAwait(false));
                    causalToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
                    resultRows = rows;
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

            return new JsonResult(new QueryResponse("ok", resultRows.Count, resultRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", UnclassifiedErrorCode, LogUnclassifiedFailure(e))) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        try
        {
            QueryByIdRequest? request = await JsonSerializer.DeserializeAsync<QueryByIdRequest>(Request.Body, jsonOptions, HttpContext.RequestAborted).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "QueryById request is not valid");

            (bool _, KvTransaction txnState) = await BeginOrResumeAsync(
                request.DatabaseName,
                request.TxnIdPT,
                request.TxnIdCounter,
                readOnly: true  // point lookup by id: a pure read, no range lock needed
            ).ConfigureAwait(false);

            QueryByIdTicket ticket = new(
                txnState: txnState,
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                id: request.Id ?? ""
            );

            List<IReadOnlyDictionary<string, ColumnValue>> rows = new();

            await foreach (Dictionary<string, ColumnValue> row in await executor.QueryById(ticket))
                rows.Add(row);

            return new JsonResult(new QueryResponse("ok", rows.Count, rows));
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", UnclassifiedErrorCode, LogUnclassifiedFailure(e))) { StatusCode = 500 };
        }
    }
}
