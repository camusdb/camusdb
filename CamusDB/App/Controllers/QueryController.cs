
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
    public QueryController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            QueryRequest? request = JsonSerializer.Deserialize<QueryRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                (newTransaction, txnState) = await BeginOrResumeAsync(
                    request.DatabaseName,
                    request.TxnIdPT,
                    request.TxnIdCounter,
                    readOnly: true,
                    promoteReadOnly: true  // scan: in key-range mode take a shared range lock (serializable, phantom-free)
                ).ConfigureAwait(false);

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
                    parameters: null
                );

                List<Dictionary<string, ColumnValue>> rows = new();

                (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket).ConfigureAwait(false);

                await foreach (QueryResultRow row in cursor)
                    rows.Add(row.Row);

                if (newTransaction)
                    await transactions.CommitAsync(database, txnState).ConfigureAwait(false);

                return new JsonResult(new QueryResponse("ok", rows.Count, rows));
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

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            QueryByIdRequest? request = JsonSerializer.Deserialize<QueryByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("QueryById request is not valid");

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

            List<Dictionary<string, ColumnValue>> rows = new();

            await foreach (Dictionary<string, ColumnValue> row in await executor.QueryById(ticket))
                rows.Add(row);

            return new JsonResult(new QueryResponse("ok", rows.Count, rows));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", "CA0000", e.Message));
        }
    }
}
