
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.App.Services;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal inter-node DDL forwarding endpoint.  Follower nodes POST their DDL
/// tickets here when they are not the schema leader.  The leader executes the
/// ticket directly via <see cref="CommandExecutor"/> and returns a
/// <c>SchemaDdlForwardResponse</c> JSON body.
/// </summary>
[ApiController]
public sealed class SchemaDdlForwardController : CommandsController
{
    public SchemaDdlForwardController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger)
        : base(executor, transactions, logger)
    {
    }

    [HttpPost]
    [Route("/internal/schema-ddl/create-table")]
    public async Task<JsonResult> ForwardCreateTable()
    {
        try
        {
            ForwardCreateTableRequest? req = await ReadJsonBodyAsync<ForwardCreateTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardCreateTable request is null");

            CreateTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                columns: MapColumns(req.Columns),
                constraints: MapConstraints(req.Constraints),
                ifNotExists: req.IfNotExists
            );

            CreateTableResult result = await executor.CreateTable(ticket).ConfigureAwait(false);
            return OkDdl(result.Success);
        }
        catch (CamusDBException e) when (IsNotLeader(e))
        {
            return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/alter-table")]
    public async Task<JsonResult> ForwardAlterTable()
    {
        try
        {
            ForwardAlterTableRequest? req = await ReadJsonBodyAsync<ForwardAlterTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardAlterTable request is null");

            AlterTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                operation: req.Operation,
                column: MapColumn(req.Column)
            );

            bool result = await executor.AlterTable(ticket).ConfigureAwait(false);
            return OkDdl(result);
        }
        catch (CamusDBException e) when (IsNotLeader(e))
        {
            return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/alter-index")]
    public async Task<JsonResult> ForwardAlterIndex()
    {
        try
        {
            ForwardAlterIndexRequest? req = await ReadJsonBodyAsync<ForwardAlterIndexRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardAlterIndex request is null");

            AlterIndexTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                indexName: req.IndexName,
                columns: req.Columns.Select(c => new ColumnIndexInfo(c.Name, c.Order)).ToArray(),
                operation: req.Operation,
                ifNotExists: req.IfNotExists
            );

            bool result = await executor.AlterIndex(ticket).ConfigureAwait(false);
            return OkDdl(result);
        }
        catch (CamusDBException e) when (IsNotLeader(e))
        {
            return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/drop-table")]
    public async Task<JsonResult> ForwardDropTable()
    {
        try
        {
            ForwardDropTableRequest? req = await ReadJsonBodyAsync<ForwardDropTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardDropTable request is null");

            DropTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                ifExists: req.IfExists
            );

            bool result = await executor.DropTable(ticket).ConfigureAwait(false);
            return OkDdl(result);
        }
        catch (CamusDBException e) when (IsNotLeader(e))
        {
            return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    private async Task<T?> ReadJsonBodyAsync<T>()
    {
        using StreamReader reader = new(Request.Body);
        string body = await reader.ReadToEndAsync().ConfigureAwait(false);
        return JsonSerializer.Deserialize<T>(body, jsonOptions);
    }

    private static bool IsNotLeader(CamusDBException e) =>
        e.Message.Contains("DDL must be executed by schema leader");

    private JsonResult OkDdl(bool applied) =>
        new JsonResult(new SchemaDdlForwardResponse { Status = "ok", Applied = applied });

    private JsonResult NotLeaderDdl() =>
        new JsonResult(new SchemaDdlForwardResponse { Status = "not-leader" })
        { StatusCode = (int)HttpStatusCode.ServiceUnavailable };

    private JsonResult BadDdlRequest(string message) =>
        new JsonResult(new SchemaDdlForwardResponse { Status = "failed", Code = CamusDBErrorCodes.InvalidInput, Message = message })
        { StatusCode = 400 };

    private JsonResult FailedDdl(string code, string message) =>
        new JsonResult(new SchemaDdlForwardResponse { Status = "failed", Code = code, Message = message })
        { StatusCode = 500 };

    private static ColumnInfo MapColumn(ColumnInfoRequest r) =>
        new(r.Name, r.Type, r.NotNull, r.Default);

    private static ColumnInfo[] MapColumns(ColumnInfoRequest[] cols) =>
        cols.Select(MapColumn).ToArray();

    private static ConstraintInfo[] MapConstraints(ConstraintInfoRequest[] constraints) =>
        constraints.Select(c => new ConstraintInfo(
            c.Type,
            c.Name,
            c.Columns.Select(col => new ColumnIndexInfo(col.Name, col.Order)).ToArray()
        )).ToArray();
}
