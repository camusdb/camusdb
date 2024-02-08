
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using System.Text.Json;
using System.Diagnostics;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.SQLParser;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class ExecuteSQLController : CommandsController
{
    public ExecuteSQLController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {
        
    }

    [HttpPost]
    [Route("/execute-sql-query")]
    public async Task<JsonResult> ExecuteSQLQuery()
    {
        try
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            logger.LogInformation("{Body}", body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQLQuery request is not valid");

            bool newTransaction = false;
            TransactionState? txnState = null;

            try
            {
                if (request.TxnIdPT > 0)
                    txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
                else
                {
                    newTransaction = true;
                    txnState = await transactions.Start().ConfigureAwait(false);
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState,
                    database: request.DatabaseName ?? "",
                    sql: request.Sql ?? "",
                    parameters: request.Parameters
                );

                List<Dictionary<string, ColumnValue>> rows = new();

                (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);

                await foreach (QueryResultRow row in cursor)
                    rows.Add(row.Row);

                if (newTransaction)
                    await transactions.Commit(database, txnState);

                Console.WriteLine("Elapsed={0}", stopwatch.ElapsedMilliseconds);

                return new JsonResult(new ExecuteSQLQueryResponse("ok", rows.Count, rows));
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotComplete(txnState);

                throw;
            }
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/execute-sql-non-query")]
    public async Task<JsonResult> ExecuteNonSQLQuery()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            logger.LogInformation("{Body}", body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteNonSQLQuery request is not valid");

            bool newTransaction = false;
            TransactionState? txnState = null;

            try
            {
                if (request.TxnIdPT > 0)
                    txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
                else
                {
                    newTransaction = true;
                    txnState = await transactions.Start().ConfigureAwait(false);
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState,
                    database: request.DatabaseName ?? "",
                    sql: request.Sql ?? "",
                    parameters: request.Parameters
                );

                ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.Commit(result.Database, txnState);

                return new JsonResult(new ExecuteNonSQLQueryResponse("ok", result.ModifiedRows));
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotComplete(txnState);

                throw;
            }
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/execute-sql-ddl")]
    public async Task<JsonResult> ExecuteSQLDDL()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            logger.LogInformation("{Body}", body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQL-DDL request is not valid");

            bool newTransaction = false;
            TransactionState? txnState = null;

            try
            {
                if (request.TxnIdPT > 0)
                    txnState = transactions.GetState(new(request.TxnIdPT, request.TxnIdCounter));
                else
                {
                    newTransaction = true;
                    txnState = await transactions.Start().ConfigureAwait(false);
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState,
                    database: request.DatabaseName ?? "",
                    sql: request.Sql ?? "",
                    parameters: request.Parameters
                );

                ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.Commit(result.Database, txnState);
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotComplete(txnState);

                throw;
            }

            return new JsonResult(new ExecuteDDLSQLResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteDDLSQLResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteDDLSQLResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
