
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.CommandsExecutor;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CommandsController
{
    public CommandExecutor commandExecutor;

    public CommandsController(CommandExecutor commandExecutor)
    {
        this.commandExecutor = commandExecutor;
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

        if (await commandExecutor.CreateTable(ticket))
            return new JsonResult("{\"status\": \"ok\"}");

        return new JsonResult("{\"status\": \"failed\"}");
    }

    [Route("/insert")]
    public async Task<JsonResult> Insert()
    {
        var system = new Random();

        InsertTicket ticket = new(
            database: "test",
            name: "my_table",
            new string[]
            {
                "id",
                "name",
                "age",
                "enabled"
            },
            new ColumnValue[]
            {
                new ColumnValue(ColumnType.Id, system.Next(1000000, 9999999).ToString()),
                new ColumnValue(ColumnType.String, "some string value"),
                new ColumnValue(ColumnType.Integer, system.Next(1000000, 9999999).ToString()),
                new ColumnValue(ColumnType.Bool, "true")
            }
        );

        if (await commandExecutor.Insert(ticket))
            return new JsonResult(new InsertResponse("ok"));

        return new JsonResult(new InsertResponse("failed"));
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
}
