
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CommandsController : ControllerBase
{
    private readonly CommandExecutor executor;

    private readonly CommandValidator validator;

    private readonly JsonSerializerOptions jsonOptions;

    public CommandsController(CommandValidator validator, CommandExecutor executor)
    {
        this.executor = executor;
        this.validator = validator;

        this.jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    [Route("/create-db")]
    public async Task<JsonResult> CreateDatabase()
    {
        await executor.CreateDatabase("test");

        return new JsonResult(new CreateDatabaseResponse("ok"));
    }

    [Route("/create-table")]
    public async Task<JsonResult> CreateTable()
    {
        //var system = new Random();

        CreateTableTicket ticket = new(
            database: "test",
            name: "my_table",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("age", ColumnType.Integer),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        try
        {
            await executor.CreateTable(ticket);
            return new JsonResult(new CreateTableResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateTableResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateTableResponse("failed", "CA0000", e.Message));
        }
    }

    [HttpPost]
    [Route("/insert")]
    public async Task<JsonResult> Insert()
    {
        try
        {
            using var reader = new StreamReader(Request.Body);
            var body = await reader.ReadToEndAsync();

            InsertRequest? request = JsonSerializer.Deserialize<InsertRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("Insert request is not valid");

            InsertTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                values: request.Values ?? new Dictionary<string, ColumnValue>()
            );

            validator.Validate(ticket);

            await executor.Insert(ticket);
            return new JsonResult(new InsertResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message));
        }
    }

    [Route("/query")]
    public async Task<JsonResult> Query()
    {
        try
        {
            QueryTicket ticket = new(
                database: "test",
                name: "my_table"
            );

            List<List<ColumnValue>> rows = await executor.Query(ticket);

            return new JsonResult(new QueryResponse("ok", rows));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new InsertResponse("failed", "CA0000", e.Message));
        }
    }

    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        QueryByIdTicket ticket = new(
            database: "test",
            name: "my_table",
            id: 2205016
        );

        List<List<ColumnValue>> rows = await executor.QueryById(ticket);

        return new JsonResult(new QueryResponse("ok", rows));
    }
}
