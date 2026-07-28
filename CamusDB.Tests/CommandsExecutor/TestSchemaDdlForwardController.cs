/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.App.Controllers;
using CamusDB.App.Services;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Exercises <see cref="SchemaDdlForwardController"/> directly (no HTTP pipeline).
/// Covers four C2 gaps: leader gate (503 when not leader), DTO→ticket reconstruction,
/// execute-as-leader, and CamusDBException → failed response mapping.
///
/// One shared in-memory <see cref="EmbeddedKahuna"/> node elects itself leader so
/// every "leader" test exercises the real path: IsSchemaLeaderAsync → executor →
/// response body.
/// </summary>
[TestFixture]
public sealed class TestSchemaDdlForwardController
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private EmbeddedKahuna? node;
    private CommandExecutor? executor;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        node = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = $"ctrl-test-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
        });

        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);

        executor = new CommandExecutor(
            new CommandValidator(),
            new CatalogsManager(NullLogger<ICamusDB>.Instance),
            NullLogger<ICamusDB>.Instance,
            sharedNode: node,
            isClusterMode: true
        );
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (executor is not null) await executor.DisposeAsync();
        if (node is not null) await node.DisposeAsync();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a controller wired to <paramref name="executor"/> with
    /// <paramref name="clusterNode"/> exposed through HttpContext.RequestServices.
    /// Pass <c>null</c> for <paramref name="clusterNode"/> to simulate a
    /// standalone / non-leader node.  Optionally supply a
    /// <paramref name="opCache"/> to enable operationId dedup.
    /// </summary>
    private SchemaDdlForwardController BuildController(
        string jsonBody,
        EmbeddedKahuna? clusterNode = null,
        DdlOperationIdCache? opCache = null)
    {
        HttpTransactionCoordinator txCoord = new(executor!);

        SchemaDdlForwardController controller = new(
            executor!,
            txCoord,
            NullLogger<ICamusDB>.Instance
        );

        byte[] bodyBytes = Encoding.UTF8.GetBytes(jsonBody);
        DefaultHttpContext httpContext = new()
        {
            RequestServices = new SingleServiceProvider(clusterNode, opCache)
        };
        httpContext.Request.Body = new MemoryStream(bodyBytes);
        httpContext.Request.ContentLength = bodyBytes.Length;

        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };
        return controller;
    }

    private static SchemaDdlForwardResponse? ExtractResponse(JsonResult result)
        => result.Value as SchemaDdlForwardResponse;

    private async Task<string> CreateTestDatabaseAsync()
    {
        string dbname = Guid.NewGuid().ToString("n");
        await executor!.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        return dbname;
    }

    private async Task DropTestDatabaseAsync(string dbname)
    {
        try
        {
            await executor!.CloseDatabase(new CloseDatabaseTicket(dbname));
            string dataPath = System.IO.Path.Combine(CamusConfig.DataDirectory, dbname);
            if (System.IO.Directory.Exists(dataPath))
                System.IO.Directory.Delete(dataPath, recursive: true);
        }
        catch { }
    }

    // ── Leader gate ───────────────────────────────────────────────────────────

    [Test]
    public async Task ForwardCreateTable_NotLeader_Returns503()
    {
        string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
        {
            OperationId = "op1",
            DatabaseName = "any",
            TableName = "robots",
            Columns = [],
            Constraints = [],
            IfNotExists = true,
        }, JsonOpts);

        // No EmbeddedKahuna in service provider → IsSchemaLeaderAsync returns false.
        SchemaDdlForwardController ctrl = BuildController(body, clusterNode: null);
        JsonResult result = await ctrl.ForwardCreateTable();

        Assert.AreEqual((int)System.Net.HttpStatusCode.ServiceUnavailable, result.StatusCode);
        SchemaDdlForwardResponse? resp = ExtractResponse(result);
        Assert.IsNotNull(resp);
        Assert.AreEqual("not-leader", resp!.Status);
    }

    [Test]
    public async Task ForwardAlterTable_NotLeader_Returns503()
    {
        string body = JsonSerializer.Serialize(new ForwardAlterTableRequest
        {
            OperationId = "op1",
            DatabaseName = "any",
            TableName = "robots",
            Operation = AlterTableOperation.AddColumn,
            Column = new ColumnInfoRequest { Name = "year", Type = CamusDB.Core.Catalogs.Models.ColumnType.Integer64 },
        }, JsonOpts);

        SchemaDdlForwardController ctrl = BuildController(body, clusterNode: null);
        JsonResult result = await ctrl.ForwardAlterTable();

        Assert.AreEqual((int)System.Net.HttpStatusCode.ServiceUnavailable, result.StatusCode);
        Assert.AreEqual("not-leader", ExtractResponse(result)!.Status);
    }

    [Test]
    public async Task ForwardAlterIndex_NotLeader_Returns503()
    {
        string body = JsonSerializer.Serialize(new ForwardAlterIndexRequest
        {
            OperationId = "op1",
            DatabaseName = "any",
            TableName = "robots",
            IndexName = "idx",
            Columns = [],
            Operation = AlterIndexOperation.DropIndex,
        }, JsonOpts);

        SchemaDdlForwardController ctrl = BuildController(body, clusterNode: null);
        JsonResult result = await ctrl.ForwardAlterIndex();

        Assert.AreEqual((int)System.Net.HttpStatusCode.ServiceUnavailable, result.StatusCode);
        Assert.AreEqual("not-leader", ExtractResponse(result)!.Status);
    }

    [Test]
    public async Task ForwardDropTable_NotLeader_Returns503()
    {
        string body = JsonSerializer.Serialize(new ForwardDropTableRequest
        {
            OperationId = "op1",
            DatabaseName = "any",
            TableName = "robots",
            IfExists = true,
        }, JsonOpts);

        SchemaDdlForwardController ctrl = BuildController(body, clusterNode: null);
        JsonResult result = await ctrl.ForwardDropTable();

        Assert.AreEqual((int)System.Net.HttpStatusCode.ServiceUnavailable, result.StatusCode);
        Assert.AreEqual("not-leader", ExtractResponse(result)!.Status);
    }

    // ── Execute-as-leader + DTO reconstruction ────────────────────────────────

    [Test]
    public async Task ForwardCreateTable_AsLeader_CreatesTable_ReturnsOkApplied()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                Columns =
                [
                    new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id },
                    new ColumnInfoRequest { Name = "name", Type = CamusDB.Core.Catalogs.Models.ColumnType.String, NotNull = true },
                ],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardCreateTable();

            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("ok", resp!.Status);
            Assert.IsTrue(resp.Applied, "table creation must report applied:true");
            Assert.IsNull(result.StatusCode, "ok result must use default 200");

            // Verify the table actually exists on the leader.
            DatabaseDescriptor descriptor = await executor!.OpenDatabase(db);
            Assert.IsTrue(descriptor.Schema.Tables.ContainsKey("robots"), "table must be in schema after create");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_AsLeader_IfNotExistsAndTableExists_ReturnsOkNotApplied()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            // Pre-create the table directly.
            await executor!.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            // Now forward CREATE TABLE IF NOT EXISTS — should return not-applied.
            // Validation runs before the IfNotExists guard, so we must supply valid DDL (with PK).
            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = true,
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardCreateTable();

            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("ok", resp!.Status);
            Assert.IsFalse(resp.Applied, "ifNotExists on existing table must report applied:false");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardAlterTable_AsLeader_AddsColumn_ReturnsOkApplied()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            await executor!.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            string body = JsonSerializer.Serialize(new ForwardAlterTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                Operation = AlterTableOperation.AddColumn,
                Column = new ColumnInfoRequest { Name = "year", Type = CamusDB.Core.Catalogs.Models.ColumnType.Integer64 },
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardAlterTable();

            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("ok", resp!.Status);
            Assert.IsTrue(resp.Applied);

            // Verify the column was added.
            DatabaseDescriptor descriptor = await executor.OpenDatabase(db);
            Assert.IsTrue(descriptor.Schema.Tables["robots"].Columns!.Any(c => c.Name == "year"));
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardAlterIndex_AsLeader_AddsIndex_ReturnsOkApplied()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            await executor!.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns:
                [
                    new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Id),
                    new ColumnInfo("name", CamusDB.Core.Catalogs.Models.ColumnType.String),
                ],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            string body = JsonSerializer.Serialize(new ForwardAlterIndexRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                IndexName = "name_idx",
                Columns = [new ColumnIndexInfoRequest { Name = "name", Order = OrderType.Ascending }],
                Operation = AlterIndexOperation.AddIndex,
                IfNotExists = false,
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardAlterIndex();

            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("ok", resp!.Status);
            Assert.IsTrue(resp.Applied);

            // Verify the index was added to the descriptor.
            CamusDB.Core.CommandsExecutor.Models.TableDescriptor table =
                await executor.OpenTable(new OpenTableTicket(db, "robots"));
            Assert.IsTrue(table.Indexes.ContainsKey("name_idx"));
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardDropTable_AsLeader_DropsTable_ReturnsOkApplied()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            await executor!.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            string body = JsonSerializer.Serialize(new ForwardDropTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                IfExists = false,
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardDropTable();

            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("ok", resp!.Status);
            Assert.IsTrue(resp.Applied);

            DatabaseDescriptor descriptor = await executor.OpenDatabase(db);
            Assert.IsFalse(descriptor.Schema.Tables.ContainsKey("robots"), "table must be absent after drop");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    // ── Error mapping ─────────────────────────────────────────────────────────

    [Test]
    public async Task ForwardDropTable_AsLeader_NonExistentTable_ReturnsFailedWithCode()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            string body = JsonSerializer.Serialize(new ForwardDropTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "does_not_exist",
                IfExists = false, // throws when table is absent
            }, JsonOpts);

            SchemaDdlForwardController ctrl = BuildController(body, clusterNode: node);
            JsonResult result = await ctrl.ForwardDropTable();

            Assert.AreEqual(500, result.StatusCode);
            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("failed", resp!.Status);
            Assert.IsNotNull(resp.Code, "error code must be populated");
            Assert.IsFalse(string.IsNullOrEmpty(resp.Message), "error message must be populated");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_AsLeader_NullBody_ReturnsBadRequest()
    {
        // Empty body deserializes to null — controller returns 400.
        SchemaDdlForwardController ctrl = BuildController("", clusterNode: node);
        JsonResult result = await ctrl.ForwardCreateTable();

        Assert.AreEqual(400, result.StatusCode);
        SchemaDdlForwardResponse? resp = ExtractResponse(result);
        Assert.IsNotNull(resp);
        Assert.AreEqual("failed", resp!.Status);
    }

    // ── OperationId dedup ────────────────────────────────────────────────

    [Test]
    public async Task ForwardCreateTable_DuplicateOperationId_ReturnsCachedResponseWithoutReExecuting()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            DdlOperationIdCache opCache = new();
            string opId = Guid.NewGuid().ToString("N");

            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            // First call — creates the table and seeds the cache.
            SchemaDdlForwardController ctrl1 = BuildController(body, clusterNode: node, opCache: opCache);
            JsonResult result1 = await ctrl1.ForwardCreateTable();
            SchemaDdlForwardResponse? resp1 = ExtractResponse(result1);
            Assert.AreEqual("ok", resp1!.Status);
            Assert.IsTrue(resp1.Applied);

            // Second call — same opId, same body.  Should replay cached response
            // without re-executing (the table already exists; re-running would fail).
            SchemaDdlForwardController ctrl2 = BuildController(body, clusterNode: node, opCache: opCache);
            JsonResult result2 = await ctrl2.ForwardCreateTable();
            SchemaDdlForwardResponse? resp2 = ExtractResponse(result2);

            Assert.AreEqual("ok", resp2!.Status,
                "duplicate operationId must return cached ok, not a TableAlreadyExists error");
            Assert.IsTrue(resp2.Applied,
                "cached response must preserve the original applied:true");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_DifferentOperationIds_ExecutesBothIndependently()
    {
        string db = await CreateTestDatabaseAsync();
        try
        {
            DdlOperationIdCache opCache = new();

            string body1 = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            string body2 = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = Guid.NewGuid().ToString("N"),
                DatabaseName = db,
                TableName = "sensors",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            JsonResult r1 = await BuildController(body1, clusterNode: node, opCache: opCache).ForwardCreateTable();
            JsonResult r2 = await BuildController(body2, clusterNode: node, opCache: opCache).ForwardCreateTable();

            Assert.AreEqual("ok", ExtractResponse(r1)!.Status);
            Assert.AreEqual("ok", ExtractResponse(r2)!.Status);

            DatabaseDescriptor desc = await executor!.OpenDatabase(db);
            Assert.IsTrue(desc.Schema.Tables.ContainsKey("robots"));
            Assert.IsTrue(desc.Schema.Tables.ContainsKey("sensors"), "second DDL with a distinct opId must execute independently");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_ErrorResponseNotCached_RetryWithSameIdExecutesAgain()
    {
        // An error response must NOT be cached. If the same operationId is sent
        // after an error, the DDL must be re-executed (the caller may have fixed
        // whatever caused the first failure, e.g. a name collision).
        string db = await CreateTestDatabaseAsync();
        try
        {
            DdlOperationIdCache opCache = new();
            string opId = Guid.NewGuid().ToString("N");

            // First call: drop a table that does not exist (IfExists = false → error).
            string failBody = JsonSerializer.Serialize(new ForwardDropTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "does_not_exist",
                IfExists = false,
            }, JsonOpts);

            JsonResult failResult = await BuildController(failBody, clusterNode: node, opCache: opCache).ForwardDropTable();
            Assert.AreEqual(500, failResult.StatusCode, "non-existent drop must fail");

            // Now create the table so the retry will succeed.
            await executor!.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "does_not_exist",
                columns: [new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Id)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false
            ));

            // Retry with the same opId — must re-execute (error was not cached).
            string retryBody = JsonSerializer.Serialize(new ForwardDropTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "does_not_exist",
                IfExists = false,
            }, JsonOpts);

            JsonResult retryResult = await BuildController(retryBody, clusterNode: node, opCache: opCache).ForwardDropTable();
            Assert.AreEqual("ok", ExtractResponse(retryResult)!.Status,
                "same opId after an error must re-execute, not replay the cached failure");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    // ── Layer 2: in-flight TCS (concurrent duplicates) ───────────────────────

    [Test]
    public async Task ForwardCreateTable_InFlightDuplicate_AwaitsOwnerResult_DoesNotReExecute()
    {
        // Pre-register an owner directly so we control when it completes, then
        // fire a duplicate through the controller and assert it awaits the TCS
        // rather than executing the DDL a second time.
        string db = await CreateTestDatabaseAsync();
        try
        {
            SyncDdlOperationIdCache opCache = new();
            string opId = Guid.NewGuid().ToString("N");

            // Register ourselves as the in-flight owner — the controller will see
            // this opId as already in-flight and block on the TCS.
            _ = opCache.TryGetOrReserve(opId);

            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            // Fire the duplicate request; it must block on the in-flight TCS.
            Task<JsonResult> duplicateTask = Task.Run(() =>
                BuildController(body, clusterNode: node, opCache: opCache).ForwardCreateTable());

            // Wait until the controller detects it's a duplicate (TryGetOrReserve returned non-null).
            await opCache.WhenDuplicateDetected.WaitAsync(TimeSpan.FromSeconds(5));

            // Verify the table was NOT created — the duplicate must not have executed the DDL.
            DatabaseDescriptor desc = await executor!.OpenDatabase(db);
            Assert.IsFalse(desc.Schema.Tables.ContainsKey("robots"),
                "table must not exist — duplicate must not have executed the DDL");

            // Complete the in-flight operation; the duplicate must unblock with the owner's result.
            opCache.SetAndComplete(opId, new SchemaDdlForwardResponse { Status = "ok", Applied = true });

            JsonResult result = await duplicateTask.WaitAsync(TimeSpan.FromSeconds(5));
            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.AreEqual("ok", resp!.Status, "duplicate must return the owner's cached response");
            Assert.IsTrue(resp.Applied);
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_InFlightDuplicate_PropagatesOwnerFault()
    {
        // When the original faults, the duplicate's await must propagate the same
        // exception, which the controller maps to a "failed" response.
        string db = await CreateTestDatabaseAsync();
        try
        {
            SyncDdlOperationIdCache opCache = new();
            string opId = Guid.NewGuid().ToString("N");

            _ = opCache.TryGetOrReserve(opId); // register as owner

            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            Task<JsonResult> duplicateTask = Task.Run(() =>
                BuildController(body, clusterNode: node, opCache: opCache).ForwardCreateTable());

            await opCache.WhenDuplicateDetected.WaitAsync(TimeSpan.FromSeconds(5));

            // Fault the owner; the duplicate must surface the fault as a failed response.
            opCache.FaultAndRelease(opId, new CamusDBException(CamusDBErrorCodes.TableAlreadyExists, "table already exists"));

            JsonResult result = await duplicateTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.AreEqual(500, result.StatusCode);
            SchemaDdlForwardResponse? resp = ExtractResponse(result);
            Assert.IsNotNull(resp);
            Assert.AreEqual("failed", resp!.Status, "duplicate must surface the owner's fault as a failed response");
            Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, resp.Code);
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    [Test]
    public async Task ForwardCreateTable_ConcurrentSameOpId_ExactlyOneExecutes()
    {
        // Two concurrent requests with the same opId and ifNotExists:false.
        // If dedup works, exactly one executes and the other awaits/gets cached result.
        // Both must return "ok".  Without dedup, one would return "failed"
        // (TableAlreadyExists) because the table was already created by the first.
        string db = await CreateTestDatabaseAsync();
        try
        {
            DdlOperationIdCache opCache = new();
            string opId = Guid.NewGuid().ToString("N");

            string body = JsonSerializer.Serialize(new ForwardCreateTableRequest
            {
                OperationId = opId,
                DatabaseName = db,
                TableName = "robots",
                Columns = [new ColumnInfoRequest { Name = "id", Type = CamusDB.Core.Catalogs.Models.ColumnType.Id }],
                Constraints =
                [
                    new ConstraintInfoRequest
                    {
                        Type = ConstraintType.PrimaryKey,
                        Name = "~pk",
                        Columns = [new ColumnIndexInfoRequest { Name = "id", Order = OrderType.Ascending }],
                    }
                ],
                IfNotExists = false,
            }, JsonOpts);

            Task<JsonResult> t1 = Task.Run(() =>
                BuildController(body, clusterNode: node, opCache: opCache).ForwardCreateTable());
            Task<JsonResult> t2 = Task.Run(() =>
                BuildController(body, clusterNode: node, opCache: opCache).ForwardCreateTable());

            JsonResult[] results = await Task.WhenAll(t1, t2);

            Assert.IsTrue(
                results.All(r => ExtractResponse(r)?.Status == "ok"),
                "both concurrent requests must return ok — without dedup one would have returned failed (TableAlreadyExists)");
        }
        finally
        {
            await DropTestDatabaseAsync(db);
        }
    }

    // ── Minimal IServiceProvider used by BuildController ─────────────────────

    private sealed class SingleServiceProvider(EmbeddedKahuna? kahuna, DdlOperationIdCache? opCache = null) : IServiceProvider
    {
        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(EmbeddedKahuna)) return kahuna;
            if (serviceType == typeof(DdlOperationIdCache)) return opCache;
            return null;
        }
    }

    /// <summary>
    /// Test-only subclass that signals <see cref="WhenDuplicateDetected"/> the
    /// first time <see cref="TryGetOrReserve"/> returns a non-null task (i.e., a
    /// concurrent duplicate has been detected and is now awaiting the in-flight TCS).
    /// </summary>
    private sealed class SyncDdlOperationIdCache : DdlOperationIdCache
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WhenDuplicateDetected => _gate.Task;

        public override Task<SchemaDdlForwardResponse?>? TryGetOrReserve(string operationId)
        {
            Task<SchemaDdlForwardResponse?>? result = base.TryGetOrReserve(operationId);
            if (result is not null)
                _gate.TrySetResult();
            return result;
        }
    }
}
