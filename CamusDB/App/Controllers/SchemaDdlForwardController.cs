
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
using CamusDB.Core.Storage.Kv;
using CamusDB.App.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.App.Controllers;

/// <summary>
/// Internal inter-node DDL forwarding endpoint.  Follower nodes POST their DDL
/// tickets here when they are not the schema leader.  The leader executes the
/// ticket directly via <see cref="CommandExecutor"/> and returns a
/// <c>SchemaDdlForwardResponse</c> JSON body.
///
/// Every action begins with an explicit leader check: if this node is not the
/// schema leader for the database it returns 503/not-leader immediately.  This
/// eliminates the steady-state forwarding loop where a follower with a registered
/// <see cref="ISchemaDdlForwarder"/> would otherwise re-forward a request instead
/// of rejecting it.  A leadership change between this check and the executor's
/// internal <c>TryForwardDdlAsync</c> re-check can still cause one additional hop
/// to the new leader, but that chain is finite and bounded by the retry limit.
///
/// After the leader check, each action consults <see cref="DdlOperationIdCache"/>
/// before executing.  If the stable operationId was already processed (the response-
/// lost retry case) the cached response is replayed without re-executing the DDL.
/// Only successful ("ok") responses are cached; errors are never replayed.
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

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();

            DdlOperationIdCache? opCache = GetOperationIdCache();
            if (TryGetCachedResponse(req.OperationId, opCache, out JsonResult? cached))
                return cached;

            CreateTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                columns: MapColumns(req.Columns),
                constraints: MapConstraints(req.Constraints),
                ifNotExists: req.IfNotExists
            );

            CreateTableResult result = await executor.CreateTable(ticket).ConfigureAwait(false);
            return OkDdlCached(result.Success, req.OperationId, opCache);
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

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();

            DdlOperationIdCache? opCache = GetOperationIdCache();
            if (TryGetCachedResponse(req.OperationId, opCache, out JsonResult? cached))
                return cached;

            AlterTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                operation: req.Operation,
                column: MapColumn(req.Column)
            );

            bool result = await executor.AlterTable(ticket).ConfigureAwait(false);
            return OkDdlCached(result, req.OperationId, opCache);
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

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();

            DdlOperationIdCache? opCache = GetOperationIdCache();
            if (TryGetCachedResponse(req.OperationId, opCache, out JsonResult? cached))
                return cached;

            AlterIndexTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                indexName: req.IndexName,
                columns: req.Columns.Select(c => new ColumnIndexInfo(c.Name, c.Order)).ToArray(),
                operation: req.Operation,
                ifNotExists: req.IfNotExists
            );

            bool result = await executor.AlterIndex(ticket).ConfigureAwait(false);
            return OkDdlCached(result, req.OperationId, opCache);
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

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();

            DdlOperationIdCache? opCache = GetOperationIdCache();
            if (TryGetCachedResponse(req.OperationId, opCache, out JsonResult? cached))
                return cached;

            DropTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                ifExists: req.IfExists
            );

            bool result = await executor.DropTable(ticket).ConfigureAwait(false);
            return OkDdlCached(result, req.OperationId, opCache);
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

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns true iff this node is the Raft schema leader for
    /// <paramref name="databaseName"/>.  Resolves <see cref="EmbeddedKahuna"/>
    /// from the DI container so standalone nodes (no cluster node registered)
    /// return false and the request is bounced back as not-leader.
    /// </summary>
    private async Task<bool> IsSchemaLeaderAsync(string databaseName)
    {
        EmbeddedKahuna? clusterNode = HttpContext.RequestServices.GetService<EmbeddedKahuna>();
        if (clusterNode is null)
            return false;

        return await clusterNode.AmISchemaLeaderAsync(databaseName, CancellationToken.None).ConfigureAwait(false);
    }

    private DdlOperationIdCache? GetOperationIdCache() =>
        HttpContext.RequestServices.GetService<DdlOperationIdCache>();

    /// <summary>
    /// Returns true and sets <paramref name="result"/> to the cached response
    /// when <paramref name="operationId"/> was already processed by this leader.
    /// </summary>
    private static bool TryGetCachedResponse(
        string operationId,
        DdlOperationIdCache? cache,
        out JsonResult result)
    {
        if (cache is not null && cache.TryGet(operationId, out SchemaDdlForwardResponse? cached))
        {
            result = new JsonResult(cached);
            return true;
        }
        result = null!;
        return false;
    }

    /// <summary>
    /// Builds an "ok" response, writes it to the cache, and returns it.
    /// Caching is skipped when <paramref name="cache"/> is null (standalone mode).
    /// </summary>
    private static JsonResult OkDdlCached(bool applied, string operationId, DdlOperationIdCache? cache)
    {
        SchemaDdlForwardResponse response = new() { Status = "ok", Applied = applied };
        cache?.Set(operationId, response);
        return new JsonResult(response);
    }

    private async Task<T?> ReadJsonBodyAsync<T>()
    {
        using StreamReader reader = new(Request.Body);
        string body = await reader.ReadToEndAsync().ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(body))
            return default;
        try
        {
            return JsonSerializer.Deserialize<T>(body, jsonOptions);
        }
        catch (System.Text.Json.JsonException)
        {
            return default;
        }
    }

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
