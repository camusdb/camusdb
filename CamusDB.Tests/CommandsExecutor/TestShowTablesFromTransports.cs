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

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Tests.Grpc;

using CamusDB.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW TABLES FROM db</c> through the entry points a client calls: the REST query endpoints, the
/// gRPC unary query, the gRPC batch stream, prepared statements on both, and explicit transactions.
///
/// <para>The statement is a server-level query, so each transport must run it with no transaction.
/// One that sent it down the autocommit path would fail with "DatabaseName is required" when the
/// request names no database, or would try to commit the descriptor the statement never returns when
/// it names one. Both bugs were live for <c>SHOW GRANTS</c>, which is why these tests drive the
/// transports rather than the executor.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowTablesFromTransports : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

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

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
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

    private async Task<string> CreateDatabaseAsync(params string[] tables)
    {
        // A letter first, so the name lexes as an identifier rather than a number.
        string db = "tf" + Guid.NewGuid().ToString("n")[..10];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        foreach (string table in tables)
            await grpc.ExecuteDdl(
                new SqlRequest { Database = db, Sql = $"CREATE TABLE {table} (id int64 PRIMARY KEY NOT NULL)" },
                new TestServerCallContext());

        return db;
    }

    private static string[] Tables(ExecuteSQLQueryResponse response)
        => response.Rows.Rows.Select(r => r.Row["tables"].StrValue!).ToArray();

    private static string[] Tables(IEnumerable<BatchExecuteResponse> messages, int requestId)
        => messages.Where(m => m.RequestId == requestId && m.Row is not null)
            .Select(m => m.Row.Values[0].StringValue).ToArray();

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.Error or
        BatchExecuteResponse.PayloadOneofCase.StartReply or
        BatchExecuteResponse.PayloadOneofCase.CommitReply or
        BatchExecuteResponse.PayloadOneofCase.RollbackReply or
        BatchExecuteResponse.PayloadOneofCase.PrepareReply;

    // ─── REST ─────────────────────────────────────────────────────────────────

    [TestCase(false)]
    [TestCase(true)]
    public async Task RestQueryEndpointListsTheNamedDatabase(bool nameAContextDatabase)
    {
        string target = await CreateDatabaseAsync("alpha", "beta");
        string other = await CreateDatabaseAsync("unrelated");

        object body = nameAContextDatabase
            ? new { databaseName = other, sql = $"SHOW TABLES FROM {target}" }
            : new { sql = $"SHOW TABLES FROM {target}" };

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)(await Sql(body).ExecuteSQLQuery()).Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(new[] { "tables" }, response.Columns.Select(c => c.Name).ToArray(),
            "the column is the one SHOW TABLES declares, so a client reads both forms the same way");
        CollectionAssert.AreEquivalent(new[] { "alpha", "beta" }, Tables(response));
    }

    [Test]
    public async Task RestStreamingEndpointStreamsTheNamedDatabase()
    {
        string target = await CreateDatabaseAsync("alpha");
        string other = await CreateDatabaseAsync("unrelated");

        ExecuteSQLController controller = Sql(new { databaseName = other, sql = $"SHOW TABLES IN {target}" });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("alpha", body);
        StringAssert.DoesNotContain("unrelated", body);
        StringAssert.Contains("\"status\":\"ok\"", body);
    }

    [Test]
    public async Task RestQueryEndpointReportsAnUnknownDatabase()
    {
        string missing = "tfnone" + Guid.NewGuid().ToString("n")[..10];

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)(await Sql(
            new { sql = $"SHOW TABLES FROM {missing}" }).ExecuteSQLQuery()).Value!;

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, response.Code);
    }

    [Test]
    public async Task RestPreparedStatementRunsWithNoDatabase()
    {
        string target = await CreateDatabaseAsync("alpha", "beta");

        PrepareStatementResponse prepared = (PrepareStatementResponse)(await Statements(
            new { databaseName = "", sql = $"SHOW TABLES FROM {target} LIKE 'a%'" }).PrepareSQLStatement()).Value!;
        Assert.AreEqual("ok", prepared.Status, prepared.Message);

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)(await Sql(
            new { statementId = prepared.StatementId }).ExecuteSQLQuery()).Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        CollectionAssert.AreEquivalent(new[] { "alpha" }, Tables(response));
    }

    [Test]
    public async Task RestInsideAnExplicitTransactionOnAnotherDatabaseLeavesItUsable()
    {
        string target = await CreateDatabaseAsync("alpha");
        string other = await CreateDatabaseAsync("kv");

        KvTransaction tx = await coordinator.StartAsync(other);

        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)(await Sql(new
        {
            databaseName = other,
            txnIdPT = tx.ClientId.L,
            txnIdCounter = tx.ClientId.C,
            sql = $"SHOW TABLES FROM {target}",
        }).ExecuteSQLQuery()).Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        CollectionAssert.AreEquivalent(new[] { "alpha" }, Tables(response));

        ExecuteNonSQLQueryResponse insert = (ExecuteNonSQLQueryResponse)(await Sql(new
        {
            databaseName = other,
            txnIdPT = tx.ClientId.L,
            txnIdCounter = tx.ClientId.C,
            sql = "INSERT INTO kv (id) VALUES (1)",
        }).ExecuteNonSQLQuery()).Value!;
        Assert.AreEqual("ok", insert.Status, insert.Message);

        await coordinator.CommitTrackedAsync(tx);

        ExecuteSQLQueryResponse rows = (ExecuteSQLQueryResponse)(await Sql(
            new { databaseName = other, sql = "SELECT id FROM kv" }).ExecuteSQLQuery()).Value!;
        Assert.AreEqual(1, rows.Total, rows.Message);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [TestCase(false)]
    [TestCase(true)]
    public async Task GrpcQueryStreamsTheNamedDatabase(bool nameAContextDatabase)
    {
        string target = await CreateDatabaseAsync("alpha", "beta");
        string other = await CreateDatabaseAsync("unrelated");

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(
            new SqlRequest { Database = nameAContextDatabase ? other : "", Sql = $"SHOW TABLES IN {target}" },
            writer,
            new TestServerCallContext(CancellationToken.None));

        List<QueryStreamMessage> schemas = writer.Written.Where(m => m.Schema is not null).ToList();
        Assert.AreEqual(1, schemas.Count);
        Assert.AreEqual(new[] { "tables" }, schemas[0].Schema.Columns.Select(c => c.Name).ToArray());

        CollectionAssert.AreEquivalent(
            new[] { "alpha", "beta" },
            writer.Written.Where(m => m.Row is not null).Select(m => m.Row.Values[0].StringValue));
    }

    [Test]
    public async Task GrpcQueryReportsAnUnknownDatabaseAsNotFound()
    {
        string missing = "tfnone" + Guid.NewGuid().ToString("n")[..10];

        RpcException error = Assert.ThrowsAsync<RpcException>(async () => await grpc.ExecuteQuery(
            new SqlRequest { Sql = $"SHOW TABLES FROM {missing}" },
            new CapturingStreamWriter<QueryStreamMessage>(),
            new TestServerCallContext(CancellationToken.None)))!;

        Assert.AreEqual(StatusCode.NotFound, error.StatusCode, error.Status.Detail);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, error.Trailers.GetValue("camus-error-code"));
    }

    /// <summary>
    /// The batch stream: an inline query, a prepared one, and both inside an explicit transaction on
    /// another database. The transaction must still commit its own write afterwards.
    /// </summary>
    [Test]
    public async Task GrpcBatchRunsInlinePreparedAndInsideATransaction()
    {
        string target = await CreateDatabaseAsync("alpha", "beta");
        string other = await CreateDatabaseAsync("kv");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        // Inline, no database.
        reader.Push(new BatchExecuteRequest
        {
            RequestId = 1,
            Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Sql = $"SHOW TABLES FROM {target}" },
        });
        BatchExecuteResponse inline = await writer.WaitFor(m => m.RequestId == 1 && IsTerminal(m));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, inline.PayloadCase, inline.Error?.Message);

        // Prepared on the stream, executed by id.
        reader.Push(new BatchExecuteRequest
        {
            RequestId = 2,
            Kind = BatchStatementKind.Prepare,
            Request = new SqlRequest { Database = other, Sql = $"SHOW TABLES FROM {target} LIKE 'b%'" },
        });
        BatchExecuteResponse prepared = await writer.WaitFor(m => m.RequestId == 2 && IsTerminal(m));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.PrepareReply, prepared.PayloadCase, prepared.Error?.Message);

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 3,
            Kind = BatchStatementKind.Query,
            Request = new SqlRequest { StatementId = prepared.PrepareReply.StatementId },
        });
        BatchExecuteResponse preparedRun = await writer.WaitFor(m => m.RequestId == 3 && IsTerminal(m));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, preparedRun.PayloadCase, preparedRun.Error?.Message);

        // Inside an explicit transaction on the other database.
        reader.Push(new BatchExecuteRequest
        {
            RequestId = 4,
            Kind = BatchStatementKind.Start,
            Request = new SqlRequest { Database = other },
        });
        TxnHandle handle = (await writer.WaitFor(m =>
            m.RequestId == 4 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 5,
            Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = other, Sql = $"SHOW TABLES FROM {target}", TxnHandle = handle },
        });
        BatchExecuteResponse inTxn = await writer.WaitFor(m => m.RequestId == 5 && IsTerminal(m));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, inTxn.PayloadCase, inTxn.Error?.Message);

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 6,
            Kind = BatchStatementKind.NonQuery,
            Request = new SqlRequest { Database = other, Sql = "INSERT INTO kv (id) VALUES (1)", TxnHandle = handle },
        });
        BatchExecuteResponse insert = await writer.WaitFor(m => m.RequestId == 6 && IsTerminal(m));
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, insert.PayloadCase, insert.Error?.Message);

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 7,
            Kind = BatchStatementKind.Commit,
            Request = new SqlRequest { Database = other, TxnHandle = handle },
        });
        BatchExecuteResponse commit = await writer.WaitFor(m => m.RequestId == 7 && IsTerminal(m));

        // An unknown database answers in-band with the not-found code.
        reader.Push(new BatchExecuteRequest
        {
            RequestId = 8,
            Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Sql = "SHOW TABLES FROM tfnosuchdatabase" },
        });
        BatchExecuteResponse unknown = await writer.WaitFor(m => m.RequestId == 8 && IsTerminal(m));

        reader.Complete();
        await server;

        IReadOnlyList<BatchExecuteResponse> all = writer.Written;
        CollectionAssert.AreEquivalent(new[] { "alpha", "beta" }, Tables(all, 1));
        CollectionAssert.AreEquivalent(new[] { "beta" }, Tables(all, 3));
        CollectionAssert.AreEquivalent(new[] { "alpha", "beta" }, Tables(all, 5));

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.CommitReply, commit.PayloadCase,
            $"the transaction was not usable after the listing: {commit.Error?.Message}");

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, unknown.PayloadCase);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, unknown.Error.Code);

        ExecuteSQLQueryResponse rows = (ExecuteSQLQueryResponse)(await Sql(
            new { databaseName = other, sql = "SELECT id FROM kv" }).ExecuteSQLQuery()).Value!;
        Assert.AreEqual(1, rows.Total, rows.Message);
    }
}
