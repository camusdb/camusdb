
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class TransactionsController : CommandsController
{
    public TransactionsController(CommandExecutor executor, TransactionsManager transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {

    }

    [HttpPost]
    [Route("/start-transaction")]
    public async Task<JsonResult> StartTransaction()
    {
        try
        {
            TransactionState txState = await transactions.Start();

            return new JsonResult(new StartTransactionResponse("ok", txState.TxnId.L, txState.TxnId.C));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new StartTransactionResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new StartTransactionResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}