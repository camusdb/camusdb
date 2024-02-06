
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
using CamusDB.Core.Transactions.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class InsertController : CommandsController
{
    public InsertController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
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

            logger.LogInformation("{Body}", body);

            InsertRequest? request = JsonSerializer.Deserialize<InsertRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Insert request is not valid");

            if (request.Values is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Insert values are not valid");

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

                InsertTicket ticket = new(
                    txnState: txnState,
                    databaseName: request.DatabaseName ?? "",
                    tableName: request.TableName ?? "",
                    values: new List<Dictionary<string, ColumnValue>>() { request.Values }
                );

                InsertResult result = await executor.Insert(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.Commit(result.Database, txnState);

                return new JsonResult(new InsertResponse("ok", result.InsertedRows));
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

            return new JsonResult(new InsertResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}

