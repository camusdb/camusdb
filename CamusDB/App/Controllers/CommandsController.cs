
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CommandsController : ControllerBase
{
    private readonly CommandExecutor executor;

    private readonly JsonSerializerOptions jsonOptions;

    public CommandsController(CommandExecutor executor)
    {
        this.executor = executor;

        this.jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    [HttpPost]
    [Route("/create-db")]
    public async Task<JsonResult> CreateDatabase()
    {
        try
        {
            using var reader = new StreamReader(Request.Body);
            var body = await reader.ReadToEndAsync();

            CreateDatabaseRequest? request = JsonSerializer.Deserialize<CreateDatabaseRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("CreateDatabase request is not valid");

            CreateDatabaseTicket ticket = new(
                name: request.DatabaseName ?? ""
            );

            await executor.CreateDatabase(ticket);

            return new JsonResult(new CreateDatabaseResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateDatabaseResponse("failed", e.Code, e.Message));
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateDatabaseResponse("failed", "CA0000", e.Message));
        }
    }

    private ColumnType GetColumnType(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw new Exception("Invalid type");

        if (name == "int")
            return ColumnType.Integer;

        if (name == "string")
            return ColumnType.String;

        if (name == "bool")
            return ColumnType.Bool;

        if (name == "id")
            return ColumnType.Id;

        throw new Exception("Unknown type " + name);
    }

    private IndexType GetIndexType(string? type)
    {
        if (string.IsNullOrEmpty(type))
            return IndexType.None;

        if (type == "unique")
            return IndexType.Unique;

        if (type == "multi")
            return IndexType.Multi;
        
        throw new Exception("Unknown index type " + type);
    }

    private ColumnInfo[] GetColumnInfos(CreateTableColumn[]? columns)
    {
        if (columns is null)
            return Array.Empty<ColumnInfo>();

        ColumnInfo[] columnInfos = new ColumnInfo[columns.Length];

        int i = 0;

        foreach (CreateTableColumn column in columns)
        {
            columnInfos[i++] = new ColumnInfo(
                name: column.Name ?? "",
                type: GetColumnType(column.Type),
                primary: column.Primary,
                notNull: false,
                index: GetIndexType(column.Index),
                defaultValue: null                
            );
        }

        return columnInfos;
    }

    [HttpPost]
    [Route("/create-table")]
    public async Task<JsonResult> CreateTable()
    {        
        try
        {
            using var reader = new StreamReader(Request.Body);
            var body = await reader.ReadToEndAsync();

            CreateTableRequest? request = JsonSerializer.Deserialize<CreateTableRequest>(body, jsonOptions);
            if (request == null)
                throw new Exception("CreateTable request is not valid");

            CreateTableTicket ticket = new(
                database: request.DatabaseName ?? "",
                name: request.TableName ?? "",
                columns: GetColumnInfos(request.Columns)
            );

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
