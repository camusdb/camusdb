
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
using CamusDB.Core.Transactions.Models;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class QueryController : CommandsController
{
    public QueryController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            QueryRequest? request = JsonSerializer.Deserialize<QueryRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Query request is not valid");

            TransactionState txnState;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
                txnState = await transactions.Start().ConfigureAwait(false);

            QueryTicket ticket = new(
                txnState: txnState,
                txnType: TransactionType.ReadOnly,
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

            await foreach (QueryResultRow row in await executor.Query(ticket).ConfigureAwait(false))            
                rows.Add(row.Row);

            return new JsonResult(new QueryResponse("ok", rows.Count, rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

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
            string body = await reader.ReadToEndAsync();

            QueryByIdRequest? request = JsonSerializer.Deserialize<QueryByIdRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("QueryById request is not valid");

            TransactionState txnState;

            if (request.TxnIdPT > 0)
                txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
            else
                txnState = await transactions.Start().ConfigureAwait(false);

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
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new QueryResponse("failed", "CA0000", e.Message));
        }
    }
}

