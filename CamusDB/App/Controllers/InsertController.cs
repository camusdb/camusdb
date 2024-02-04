﻿
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

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class InsertController : CommandsController
{
    private readonly ILogger<ICamusDB> logger;

    public InsertController(CommandExecutor executor, ILogger<ICamusDB> logger) : base(executor)
    {
        this.logger = logger;
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

            InsertTicket ticket = new(
                txnId: await executor.NextTxnId().ConfigureAwait(false),
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                values: new List<Dictionary<string, ColumnValue>>() { request.Values }
            );

            await executor.Insert(ticket).ConfigureAwait(false);

            return new JsonResult(new InsertResponse("ok", 1));
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

