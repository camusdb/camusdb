
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
using CamusDB.Core.Transactions;
using CamusDB.App.Services;
using CamusDB.Core.CommandsExecutor.Models.Results;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class CreateTableController : CommandsController
{
    public CreateTableController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {

    }

    private static ColumnType GetColumnType(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw new Exception("Invalid type");

        return name.ToLowerInvariant() switch
        {
            "id"        => ColumnType.Id,
            "int64"     => ColumnType.Integer64,
            "int"       => ColumnType.Integer64,
            "integer"   => ColumnType.Integer64,
            "string"    => ColumnType.String,
            "char"      => ColumnType.String,
            "varchar"   => ColumnType.String,
            "text"      => ColumnType.String,
            "bool"      => ColumnType.Bool,
            "float64"   => ColumnType.Float64,
            "float32"   => ColumnType.Float32,
            "real"      => ColumnType.Float32,
            "date"      => ColumnType.Date,
            "datetime"  => ColumnType.DateTime,
            "timestamp" => ColumnType.DateTime,
            "bytes"     => ColumnType.Bytes,
            "blob"      => ColumnType.Bytes,
            "uuid"      => ColumnType.Uuid,
            "guid"      => ColumnType.Uuid,
            "array"     => ColumnType.Array,
            _           => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown type: " + name),
        };
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
            ColumnType? arrayElementType = null;
            if (!string.IsNullOrEmpty(column.ArrayElementType))
                arrayElementType = GetColumnType(column.ArrayElementType);

            columnInfos[i++] = new ColumnInfo(
                name: column.Name ?? "",
                type: GetColumnType(column.Type),
                notNull: column.NotNull,
                defaultValue: column.DefaultValue,
                maxLength: column.MaxLength,
                arrayElementType: arrayElementType
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
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            CreateTableRequest? request = JsonSerializer.Deserialize<CreateTableRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "CreateTable request is not valid");

            CreateTableTicket ticket = new(
                databaseName: request.DatabaseName ?? "",
                tableName: request.TableName ?? "",
                columns: GetColumnInfos(request.Columns),
                constraints: Array.Empty<ConstraintInfo>(),
                ifNotExists: request.IfNotExists
            );

            await executor.CreateTable(ticket).ConfigureAwait(false);

            return new JsonResult(new CreateTableResponse("ok"));
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CreateTableResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new CreateTableResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}

