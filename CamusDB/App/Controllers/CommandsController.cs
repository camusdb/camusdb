
using System;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Library.CommandsExecutor;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.Catalogs.Models;

namespace CamusDB.App.Controllers
{
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

            return new JsonResult("[0]");
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
                    new ColumnValue(ColumnType.Id, "aaa"),
                    new ColumnValue(ColumnType.String, "some string value"),
                    new ColumnValue(ColumnType.Integer, "15"),
                    new ColumnValue(ColumnType.Bool, "true")
                }
            );

            if (await commandExecutor.Insert(ticket))
                return new JsonResult("{\"status\": \"ok\"}");

            return new JsonResult("{\"status\": \"failed\"}");
        }
    }
}

