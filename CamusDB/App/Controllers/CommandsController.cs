
using CamusDB.Core;
using System.Text.Json;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CommandsController : ControllerBase
{
    public readonly CommandExecutor commandExecutor;

    private readonly JsonSerializerOptions jsonOptions;

    public CommandsController(CommandExecutor commandExecutor)
    {
        this.commandExecutor = commandExecutor;

        this.jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    [Route("/create-db")]
    public async Task<JsonResult> CreateDatabase()
    {
        await commandExecutor.CreateDatabase("test");

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
            await commandExecutor.CreateTable(ticket);
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
        using var reader = new StreamReader(Request.Body);
        var body = await reader.ReadToEndAsync();

        InsertRequest? request = JsonSerializer.Deserialize<InsertRequest>(body, jsonOptions);
        if (request == null)
            throw new Exception("Worker Id is required (1)");

        var system = new Random();

        // @todo validate request and create ticket

        InsertTicket ticket = new(
            database: request.DatabaseName ?? "",
            name: request.TableName ?? "",
            columns: request.Columns ?? new string[0],
            values: request.Values ?? new ColumnValue[0]
        );

        Console.WriteLine(ticket.Values.Length);

        try
        {
            await commandExecutor.Insert(ticket);
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
        QueryTicket ticket = new(
            database: "test",
            name: "my_table"
        );

        List<List<ColumnValue>> rows = await commandExecutor.Query(ticket);

        return new JsonResult(new QueryResponse("ok", rows));
    }

    [Route("/query-by-id")]
    public async Task<JsonResult> QueryById()
    {
        QueryByIdTicket ticket = new(
            database: "test",
            name: "my_table",
            id: 2205016
        );

        List<List<ColumnValue>> rows = await commandExecutor.QueryById(ticket);

        return new JsonResult(new QueryResponse("ok", rows));
    }
}
