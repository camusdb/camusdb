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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CreateTableController : CommandsController
{
    public CreateTableController(CommandExecutor executor) : base(executor)
    {

    }

    private static ColumnType GetColumnType(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw new Exception("Invalid type");

        if (name == "int64")
            return ColumnType.Integer64;

        if (name == "string")
            return ColumnType.String;

        if (name == "bool")
            return ColumnType.Bool;

        if (name == "id")
            return ColumnType.Id;

        throw new Exception("Unknown type " + name);
    }

    private static IndexType GetIndexType(string? type)
    {
        if (string.IsNullOrEmpty(type))
            return IndexType.None;

        if (type == "unique")
            return IndexType.Unique;

        if (type == "multi")
            return IndexType.Multi;

        throw new Exception("Unknown index type " + type);
    }

    private static ColumnInfo[] GetColumnInfos(CreateTableColumn[]? columns)
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
                notNull: column.NotNull,
                defaultValue: column.DefaultValue
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
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync();

            CreateTableRequest? request = JsonSerializer.Deserialize<CreateTableRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "CreateTable request is not valid");

            CreateTableTicket ticket = new(
                txnId: await executor.NextTxnId(),
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                columns: GetColumnInfos(request.Columns),
                constraints: Array.Empty<ConstraintInfo>(),
                ifNotExists: request.IfNotExists
            );

            await executor.CreateTable(ticket);

            return new JsonResult(new CreateTableResponse("ok"));
        }
        catch (CamusDBException e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateTableResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            Console.WriteLine("{0}: {1}\n{2}", e.GetType().Name, e.Message, e.StackTrace);
            return new JsonResult(new CreateTableResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}

