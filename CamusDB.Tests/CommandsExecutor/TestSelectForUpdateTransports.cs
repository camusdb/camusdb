/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An autocommit <c>SELECT … FOR UPDATE</c> on every row-returning transport. The statement needs a
/// writable transaction to own its locks, so each transport must begin one instead of the read-only
/// snapshot a query runs in, commit it after the rows are buffered, and so release the locks at the end
/// of the statement. A held row under NOWAIT must reach the client as <c>LockNotAvailable</c>: HTTP 409
/// on REST, <c>ABORTED</c> with the code in the trailer on gRPC.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestSelectForUpdateTransports : BaseTest
{
    private const string Lock = "SELECT id, year FROM robots WHERE year >= 2002 ORDER BY year FOR UPDATE";

    private const string LockNoWait = "SELECT id FROM robots WHERE year = 2002 FOR UPDATE NOWAIT";

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    [SetUp]
    public void SetUpTransports()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
        grpc = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(body) };

    private PreparedStatementsController Statements(object body) =>
        new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(body) };

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    private async Task<string> CreateDatabaseWithRobotsAsync()
    {
        string db = "sfu" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE robots (id oid NOT NULL DEFAULT(gen_id()), name string, year int64, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(ddl);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
            "INSERT INTO robots (name, year) VALUES ('alpha', 2001), ('beta', 2002), ('gamma', 2003)", null));
        await database.Transactions.CommitAsync(tx);
        return db;
    }

    /// <summary>
    /// Proves the statement's autocommit transaction released its locks: another transaction can now
    /// lock every row of the table with NOWAIT.
    /// </summary>
    private async Task AssertNoRowIsLockedAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT id FROM robots FOR UPDATE NOWAIT", null));
            Assert.AreEqual(3, (await cursor.ToListAsync()).Count);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Holds the lock of beta (year 2002) in an open transaction the caller must roll back.</summary>
    private async Task<(DatabaseDescriptor database, KvTransaction holder)> HoldBetaAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction holder = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(holder, db, "SELECT id FROM robots WHERE year = 2002 FOR UPDATE", null));
        Assert.AreEqual(1, (await cursor.ToListAsync()).Count);
        return (database, holder);
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task RestQueryLocksInAWritableTransaction_AndReleasesAtTheEnd()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = Lock }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Total);
        Assert.IsNotNull(response.CausalToken, "the statement ran in a writable autocommit transaction and committed it");
        await AssertNoRowIsLockedAsync(db);
    }

    [Test]
    public async Task RestQueryNoWait_OnAHeldRow_Is409LockNotAvailable()
    {
        string db = await CreateDatabaseWithRobotsAsync();
        (DatabaseDescriptor database, KvTransaction holder) = await HoldBetaAsync(db);

        try
        {
            // Read Committed: a Serializable scan would meet the held row at its predicate lock first
            // (see TestSelectForUpdate.Serializable_AFullScanMeetsAHeldRowAtItsPredicateLock).
            JsonResult result = await Sql(new { databaseName = db, sql = LockNoWait, isolationLevel = "ReadCommitted" }).ExecuteSQLQuery();
            ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

            Assert.AreEqual("failed", response.Status);
            Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, response.Code);
            Assert.AreEqual(409, result.StatusCode);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(holder);
        }
    }

    /// <summary>
    /// EXPLAIN (ANALYZE) of a locking read takes the locks, so in autocommit it runs in a writable
    /// transaction (a read-only snapshot would refuse it) and releases the locks when that commits.
    /// Plain EXPLAIN takes no lock and runs as a query does.
    /// </summary>
    [Test]
    public async Task RestExplainAnalyzeLocksInAWritableTransaction_AndPlainExplainRuns()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult analyzed = await Sql(new { databaseName = db, sql = "EXPLAIN (ANALYZE) " + Lock }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse analyzedResponse = (ExecuteSQLQueryResponse)analyzed.Value!;
        Assert.AreEqual("ok", analyzedResponse.Status, analyzedResponse.Message);
        Assert.IsNotNull(analyzedResponse.CausalToken, "EXPLAIN (ANALYZE) ran in a writable autocommit transaction and committed it");
        await AssertNoRowIsLockedAsync(db);

        JsonResult planned = await Sql(new { databaseName = db, sql = "EXPLAIN " + Lock }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse plannedResponse = (ExecuteSQLQueryResponse)planned.Value!;
        Assert.AreEqual("ok", plannedResponse.Status, plannedResponse.Message);
    }

    [Test]
    public async Task RestStreamLocksInAWritableTransaction_AndReleasesAtTheEnd()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        ExecuteSQLController controller = Sql(new { databaseName = db, sql = Lock });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("\"status\":\"ok\"", body);
        StringAssert.Contains("\"total\":2", body);
        await AssertNoRowIsLockedAsync(db);
    }

    [Test]
    public async Task RestPreparedQueryLocksInAWritableTransaction()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult prepared = await Statements(new { databaseName = db, sql = Lock }).PrepareSQLStatement();
        PrepareStatementResponse handle = (PrepareStatementResponse)prepared.Value!;
        Assert.That(handle.StatementId, Is.Not.Null.And.Not.Empty, handle.Message);

        JsonResult result = await Sql(new { statementId = handle.StatementId }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Total);
        await AssertNoRowIsLockedAsync(db);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task GrpcQueryLocksInAWritableTransaction_AndReleasesAtTheEnd()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext context = new(CancellationToken.None);
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = Lock }, writer, context);

        Assert.AreEqual(2, writer.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row));
        Assert.IsNotNull(context.ResponseTrailers.GetValue("camus-causal-token-l"), "the autocommit transaction committed");
        await AssertNoRowIsLockedAsync(db);
    }

    [Test]
    public async Task GrpcQueryNoWait_OnAHeldRow_IsAbortedWithLockNotAvailable()
    {
        string db = await CreateDatabaseWithRobotsAsync();
        (DatabaseDescriptor database, KvTransaction holder) = await HoldBetaAsync(db);

        try
        {
            CapturingStreamWriter<QueryStreamMessage> writer = new();
            RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
                await grpc.ExecuteQuery(
                    new SqlRequest { Database = db, Sql = LockNoWait, IsolationLevel = IsolationLevel.ReadCommitted },
                    writer, new TestServerCallContext(CancellationToken.None)))!;

            Assert.AreEqual(StatusCode.Aborted, error.StatusCode);
            Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, error.Trailers.GetValue("camus-error-code"));
            Assert.AreEqual(0, writer.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(holder);
        }
    }

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.PrepareReply or
        BatchExecuteResponse.PayloadOneofCase.StartReply or
        BatchExecuteResponse.PayloadOneofCase.CommitReply or
        BatchExecuteResponse.PayloadOneofCase.RollbackReply or
        BatchExecuteResponse.PayloadOneofCase.Error;

    /// <summary>Runs batch ops one at a time on one stream and returns every message the server sent.</summary>
    private async Task<List<BatchExecuteResponse>> RunBatchAsync(params (BatchStatementKind Kind, Func<List<BatchExecuteResponse>, SqlRequest> Request)[] ops)
    {
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        for (int i = 0; i < ops.Length; i++)
        {
            int requestId = i + 1;
            reader.Push(new BatchExecuteRequest { RequestId = requestId, Kind = ops[i].Kind, Request = ops[i].Request(writer.Written.ToList()) });
            await writer.WaitFor(m => m.RequestId == requestId && IsTerminal(m));
        }

        reader.Complete();
        await server;
        return writer.Written.ToList();
    }

    private static void AssertBatchQueryLockedTwoRows(List<BatchExecuteResponse> messages, int requestId)
    {
        List<BatchExecuteResponse> op = messages.Where(m => m.RequestId == requestId).ToList();
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, op.Last().PayloadCase, op.Last().Error?.Message);
        Assert.AreEqual(2, op.Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row));
    }

    [Test]
    public async Task BatchQueryOpLocksInAWritableTransaction()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.Query, _ => new SqlRequest { Database = db, Sql = Lock }));

        AssertBatchQueryLockedTwoRows(messages, requestId: 1);
        await AssertNoRowIsLockedAsync(db);
    }

    /// <summary>
    /// A prepared batch query op knows its statement by the stored root type only. The root type of a
    /// FOR UPDATE is its own, so that is enough to begin a writable transaction.
    /// </summary>
    [Test]
    public async Task BatchPreparedQueryOpLocksInAWritableTransaction()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.Prepare, _ => new SqlRequest { Database = db, Sql = Lock }),
            (BatchStatementKind.Query, written => new SqlRequest
            {
                StatementId = written.First(m => m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply)
                    .PrepareReply.StatementId,
            }));

        AssertBatchQueryLockedTwoRows(messages, requestId: 2);
        await AssertNoRowIsLockedAsync(db);
    }
}
