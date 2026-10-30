
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
/// After the leader check, each action calls
/// <see cref="DdlOperationIdCache.TryGetOrReserve"/> before executing.
/// <list type="bullet">
///   <item>Sequential retry (response was lost in transit): the result was already
///     cached — replay it without re-executing the DDL.</item>
///   <item>Concurrent duplicate (same opId arrives while the original is still
///     executing, e.g. because the HTTP client timed out): the duplicate awaits
///     the in-flight TCS and returns the original's result when it completes.
///     If the original faults, the duplicate propagates the same error.</item>
/// </list>
/// </summary>
[ApiController]
public sealed class SchemaDdlForwardController : CommandsController
{
    public SchemaDdlForwardController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger,
        CamusDBOptions options)
        : base(executor, transactions, logger, options)
    {
    }

    [HttpPost]
    [Route("/internal/schema-ddl/create-table")]
    public async Task<JsonResult> ForwardCreateTable()
    {
        ForwardCreateTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardCreateTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardCreateTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            // Cached result or concurrent duplicate awaiting the original.
            // If the original faulted, await throws — caught below.
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            CreateTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                columns: MapColumns(req.Columns),
                constraints: MapConstraints(req.Constraints),
                ifNotExists: req.IfNotExists,
                checkConstraints: MapCheckConstraints(req.CheckConstraints),
                comment: req.Comment,
                foreignKeys: MapForeignKeys(req.ForeignKeys)
            );

            CreateTableResult result = await executor.CreateTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result.Success };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/alter-table")]
    public async Task<JsonResult> ForwardAlterTable()
    {
        ForwardAlterTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardAlterTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardAlterTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            AlterTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                operation: req.Operation,
                column: MapColumn(req.Column),
                newName: req.NewName
            );

            bool result = await executor.AlterTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/alter-index")]
    public async Task<JsonResult> ForwardAlterIndex()
    {
        ForwardAlterIndexRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardAlterIndexRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardAlterIndex request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            AlterIndexTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                indexName: req.IndexName,
                columns: req.Columns.Select(c => new ColumnIndexInfo(c.Name, c.Order)).ToArray(),
                operation: req.Operation,
                ifNotExists: req.IfNotExists,
                newName: req.NewName,
                includeColumns: req.IncludeColumns,
                ifExists: req.IfExists
            );

            bool result = await executor.AlterIndex(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    /// <summary>
    /// Receives a schema-apply acknowledgement from a follower node and records it in the local
    /// <see cref="EmbeddedKahuna"/>'s tracker. This makes the follower's progress visible to the
    /// leader's two-version gate (<c>WaitForAllLiveAsync</c>) in real multi-process deployments.
    ///
    /// <para>The answer says whether this node leads the schema partition of the database the ack
    /// names (the request carries the database id). A sender outside the partition's replica set
    /// has no Raft view of the leader and names a replica from the placement view, which is the
    /// first voter until a gossiped hint forms; an ack recorded there in silence never reaches the
    /// leader's gate, and the gate then skips that node for every later DDL. A 503 not-leader makes
    /// the sender try the next replica. The record is kept here regardless: should this node win
    /// the next election, its gate starts with the follower's progress.</para>
    /// </summary>
    [HttpPost]
    [Route("/internal/schema-ddl/schema-ack")]
    public async Task<JsonResult> ForwardSchemaAck()
    {
        SchemaAckRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<SchemaAckRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("SchemaAck request is null");
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        EmbeddedKahuna? clusterNode = HttpContext.RequestServices.GetService<EmbeddedKahuna>();
        if (clusterNode is null)
            return NotLeaderDdl();

        clusterNode.RecordRemoteSchemaAck(req.Database, req.NodeEndpoint, req.AppliedSchemaVersion);

        try
        {
            if (!await clusterNode.AmISchemaLeaderAsync(req.Database, CancellationToken.None).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        return new JsonResult(new SchemaDdlForwardResponse { Status = "ok" });
    }

    /// <summary>
    /// Answers a DDL proposer's pull probe with this node's own applied schema version of a database,
    /// or that this node does not hold the database open. The proposer asks when it holds no pushed
    /// ack from this node, which happens when the ack was lost or went to a previous leader, when this
    /// node opened the database a moment ago, and under replica placement when this node shares no
    /// partition with the proposer so its Raft activity never shows it. No leader check: the question
    /// is about this node alone, and any node may ask it.
    /// </summary>
    [HttpPost]
    [Route("/internal/schema-ddl/schema-applied")]
    public async Task<JsonResult> ProbeSchemaApplied()
    {
        SchemaAppliedProbeRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<SchemaAppliedProbeRequest>().ConfigureAwait(false);
            if (req is null || string.IsNullOrEmpty(req.Database))
                return new JsonResult(new SchemaAppliedProbeResponse { Status = "failed" }) { StatusCode = (int)HttpStatusCode.BadRequest };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return new JsonResult(new SchemaAppliedProbeResponse { Status = "failed" }) { StatusCode = (int)HttpStatusCode.BadRequest };
        }

        EmbeddedKahuna? clusterNode = HttpContext.RequestServices.GetService<EmbeddedKahuna>();
        if (clusterNode is null)
            return new JsonResult(new SchemaAppliedProbeResponse { Status = "failed" }) { StatusCode = (int)HttpStatusCode.ServiceUnavailable };

        long? applied = clusterNode.GetLocalSchemaApplied(req.Database);

        return new JsonResult(new SchemaAppliedProbeResponse
        {
            Status = "ok",
            Open = applied is not null,
            AppliedSchemaVersion = applied ?? 0,
        });
    }

    /// <summary>
    /// Answers the extension peer probe with the extension builds this node loaded at boot: the
    /// proposer of <c>CREATE EXTENSION</c> or <c>ALTER EXTENSION ... UPDATE</c> refuses unless every
    /// live node loads the target build. No leader check: the question is about this node alone.
    /// </summary>
    [HttpPost]
    [Route("/internal/schema-ddl/loaded-extensions")]
    public JsonResult ProbeLoadedExtensions() =>
        new(new LoadedExtensionsProbeResponse { Status = "ok", Extensions = executor.DescribeLoadedExtensions() });

    [HttpPost]
    [Route("/internal/schema-ddl/rename-table")]
    public async Task<JsonResult> ForwardRenameTable()
    {
        ForwardRenameTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardRenameTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardRenameTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            RenameTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                newName: req.NewName
            );

            bool result = await executor.RenameTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/drop-table")]
    public async Task<JsonResult> ForwardDropTable()
    {
        ForwardDropTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardDropTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardDropTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            DropTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                ifExists: req.IfExists,
                force: req.Force,
                allowMaterializedView: req.AllowMaterializedView
            );

            bool result = await executor.DropTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/truncate-table")]
    public async Task<JsonResult> ForwardTruncateTable()
    {
        ForwardTruncateTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardTruncateTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardTruncateTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            TruncateTableTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                restartIdentity: req.RestartIdentity
            );

            bool result = await executor.TruncateTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/relink-table")]
    public async Task<JsonResult> ForwardRelinkTable()
    {
        ForwardRelinkTableRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardRelinkTableRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardRelinkTable request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            RelinkTableTicket ticket = new(
                databaseName: req.DatabaseName,
                newTableName: req.NewTableName,
                orphanTableId: req.OrphanTableId
            );

            bool result = await executor.RelinkTable(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/alter-constraint")]
    public async Task<JsonResult> ForwardAlterConstraint()
    {
        ForwardAlterConstraintRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardAlterConstraintRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardAlterConstraint request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            AlterConstraintTicket ticket = new(
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                constraintName: req.ConstraintName,
                expression: req.Expression,
                referencedColumns: req.ReferencedColumns,
                operation: req.Operation,
                columnName: req.ColumnName,
                storage: req.Storage,
                foreignKey: req.ForeignKey is { } foreignKey ? MapForeignKeys([foreignKey])[0] : null
            );

            ExecuteDDLSQLResult result = await executor.AlterConstraint(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result.Success };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    [HttpPost]
    [Route("/internal/schema-ddl/comment")]
    public async Task<JsonResult> ForwardComment()
    {
        ForwardCommentRequest? req;
        try
        {
            req = await ReadJsonBodyAsync<ForwardCommentRequest>().ConfigureAwait(false);
            if (req is null)
                return BadDdlRequest("ForwardComment request is null");

            if (!await IsSchemaLeaderAsync(req.DatabaseName).ConfigureAwait(false))
                return NotLeaderDdl();
        }
        catch (CamusDBException e)
        {
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }

        DdlOperationIdCache? opCache = GetOperationIdCache();
        Task<SchemaDdlForwardResponse?>? pending = opCache?.TryGetOrReserve(req.OperationId);
        if (pending is not null)
        {
            try { return new JsonResult(await pending.ConfigureAwait(false)); }
            catch (CamusDBException e) { return FailedDdl(e.Code, e.Message); }
            catch (Exception e) { return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message); }
        }

        try
        {
            // req.Comment is forwarded verbatim: null means IS NULL (remove), "" means IS ''.
            CommentTicket ticket = new(
                target: req.Target,
                databaseName: req.DatabaseName,
                tableName: req.TableName,
                elementName: req.ElementName,
                comment: req.Comment
            );

            ExecuteDDLSQLResult result = await executor.Comment(ticket).ConfigureAwait(false);
            SchemaDdlForwardResponse response = new() { Status = "ok", Applied = result.Success };
            opCache?.SetAndComplete(req.OperationId, response);
            return new JsonResult(response);
        }
        catch (CamusDBException e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            LogCommandFailure(e);
            return FailedDdl(e.Code, e.Message);
        }
        catch (Exception e)
        {
            opCache?.FaultAndRelease(req.OperationId, e);
            logger.LogError("{Name}: {Message}", e.GetType().Name, e.Message);
            return FailedDdl(CamusDBErrorCodes.InvalidInternalOperation, e.Message);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns true iff this node is the Raft schema leader for <paramref name="databaseName"/>.
    ///
    /// <para><b>The check keys by the database id, never by its name.</b> The schema log of a database
    /// lives on the partition of <c>{id}/meta</c>, and every engine-side leadership question asks about
    /// that partition. The name hashes to a different partition, which has a different leader under
    /// leader balancing and may not even be hosted here under replica placement. A name-keyed check
    /// answered "not-leader" on the real leader, so every forward failed until the client happened to
    /// reach the leader directly, and answered "leader" on a node that was not: that node then
    /// re-forwarded the ticket while holding its own DDL semaphore, and the forward it sent to itself
    /// deadlocked until the HTTP client's 100 s timeout. Both faces of that defect were the 43 s
    /// <c>CREATE TABLE</c> on a 5-node, replication-factor-3 cluster.</para>
    ///
    /// <para>Resolves <see cref="EmbeddedKahuna"/> from the DI container so standalone nodes (no
    /// cluster node registered) return false and the request is bounced back as not-leader. Opening
    /// the database is how the id is learned; the executor opens it again for the ticket anyway, and a
    /// database that does not exist surfaces as its own typed error.</para>
    /// </summary>
    private async Task<bool> IsSchemaLeaderAsync(string databaseName)
    {
        EmbeddedKahuna? clusterNode = HttpContext.RequestServices.GetService<EmbeddedKahuna>();
        if (clusterNode is null)
            return false;

        DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);

        return await clusterNode.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
    }

    private DdlOperationIdCache? GetOperationIdCache() =>
        HttpContext.RequestServices.GetService<DdlOperationIdCache>();

    private async Task<T?> ReadJsonBodyAsync<T>()
    {
        using StreamReader reader = new(Request.Body);
        string body = await reader.ReadToEndAsync(HttpContext.RequestAborted).ConfigureAwait(false);
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
        new(r.Name, r.Type, r.NotNull, r.Default, r.MaxLength, r.ArrayElementType,
            r.DefaultFunction, r.NotNullConstraintName, r.Comment, r.Storage,
            r.DefaultSequenceId, r.IdentityAlways, r.Identity, r.DefaultSequenceName);

    private static ColumnInfo[] MapColumns(ColumnInfoRequest[] cols) =>
        cols.Select(MapColumn).ToArray();

    private static ConstraintInfo[] MapConstraints(ConstraintInfoRequest[] constraints) =>
        constraints.Select(c => new ConstraintInfo(
            c.Type,
            c.Name,
            c.Columns.Select(col => new ColumnIndexInfo(col.Name, col.Order)).ToArray(),
            comment: c.Comment
        )).ToArray();

    private static ForeignKeyInfo[] MapForeignKeys(ForeignKeyInfoRequest[]? foreignKeys) =>
        (foreignKeys ?? []).Select(f => new ForeignKeyInfo(
            f.Name,
            f.Columns ?? [],
            f.ReferencedTable,
            f.ReferencedColumns ?? [],
            f.OnDelete,
            f.OnUpdate,
            f.Match,
            f.Deferrable,
            f.InitiallyDeferred)).ToArray();

    private static CheckConstraintInfo[] MapCheckConstraints(CheckConstraintInfoRequest[] checks) =>
        checks.Select(c => new CheckConstraintInfo(c.Name, c.Expression, c.ReferencedColumns)).ToArray();
}
