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
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
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
/// <c>INSERT … RETURNING</c> over every transport that can carry it: the REST query, stream and
/// non-query endpoints, and gRPC <c>ExecuteQuery</c>, <c>ExecuteNonQuery</c> and both batch op
/// kinds. Each test checks the rows the transport sent and then reads the table back, so a
/// transport that answers with rows but never commits — or commits rows it refused to send — fails.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestInsertReturningTransports : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

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

    // ─── Harness ──────────────────────────────────────────────────────────────

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
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body) };

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    private async Task<string> CreateDatabaseWithRobotsAsync()
    {
        string db = "ir" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db,
            "CREATE TABLE robots (id oid NOT NULL DEFAULT(gen_id()), name string, year int64, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return db;
    }

    private async Task<int> CountRowsAsync(string db, string table = "robots")
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, $"SELECT id FROM {table}", null));
            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;
            return count;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private const string InsertTwo =
        "INSERT INTO robots (name, year) VALUES ('alpha', 2001), ('beta', 2002) RETURNING name, year";

    private static void AssertTwoRobots(IReadOnlyList<string> names, IReadOnlyList<long> years)
    {
        CollectionAssert.AreEqual(new[] { "alpha", "beta" }, names);
        CollectionAssert.AreEqual(new long[] { 2001, 2002 }, years);
    }

    private static void AssertTwoRobots(ResultSchema schema, IReadOnlyList<ResultRow> rows)
    {
        CollectionAssert.AreEqual(new[] { "name", "year" }, schema.Columns.Select(c => c.Name));
        AssertTwoRobots(
            rows.Select(r => r.Values[0].StringValue).ToList(),
            rows.Select(r => r.Values[1].Int64Value).ToList());
    }

    private static void AssertTwoRobots(PositionalRowSet rows)
    {
        AssertTwoRobots(
            rows.Rows.Select(r => r.Row[rows.Schema[0].RowKey].StrValue!).ToList(),
            rows.Rows.Select(r => r.Row[rows.Schema[1].RowKey].LongValue).ToList());
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task RestNonQueryReturnsTheCountAndTheRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = InsertTwo }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Rows);
        CollectionAssert.AreEqual(new[] { "name", "year" }, response.Columns!.Select(c => c.Name));
        AssertTwoRobots(response.ReturningRows!);
        Assert.AreEqual(2, await CountRowsAsync(db));

        // On the wire, positional and next to the count, in the same row encoding a query uses.
        string json = JsonSerializer.Serialize(response, WireJson);
        StringAssert.Contains("\"returningRows\":[[\"alpha\",2001],[\"beta\",2002]]", json);
        StringAssert.Contains("\"columns\":[", json);
    }

    /// <summary>
    /// A client that never uses RETURNING must see the response it always saw: no new keys, not even
    /// null ones.
    /// </summary>
    [Test]
    public async Task RestNonQueryWithoutReturningKeepsTheOldShape()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = "INSERT INTO robots (name) VALUES ('alpha')" }).ExecuteNonSQLQuery();
        string json = JsonSerializer.Serialize(result.Value, WireJson);

        StringAssert.Contains("\"rows\":1", json);
        StringAssert.DoesNotContain("returningRows", json);
        StringAssert.DoesNotContain("\"columns\"", json);
    }

    [Test]
    public async Task RestNonQueryOfNoRowsSendsTheSchemaAndAnEmptyList()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO robots (name, year) SELECT name, year FROM robots WHERE year > 3000 RETURNING name",
        }).ExecuteNonSQLQuery();
        string json = JsonSerializer.Serialize(result.Value, WireJson);

        StringAssert.Contains("\"rows\":0", json);
        StringAssert.Contains("\"returningRows\":[]", json);
        StringAssert.Contains("\"name\":\"name\"", json);
    }

    [Test]
    public async Task RestNonQueryCountOnlyInsertsAndSendsNoRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = InsertTwo, discardReturningRows = true }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Rows);
        Assert.IsNull(response.Columns);
        Assert.IsNull(response.ReturningRows);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    /// <summary>
    /// The query endpoint runs an autocommit SELECT in a read-only snapshot. An INSERT … RETURNING
    /// there must get a writable transaction and must commit, or the rows it reports are lost.
    /// </summary>
    [Test]
    public async Task RestQueryRunsTheInsertAndCommitsIt()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = InsertTwo }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Total);
        CollectionAssert.AreEqual(new[] { "name", "year" }, response.Columns.Select(c => c.Name));
        AssertTwoRobots(response.Rows);
        Assert.IsNotNull(response.CausalToken, "an autocommit write reports its commit");
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task RestQueryRefusesTheCountOnlyFlag()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = InsertTwo, discardReturningRows = true }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, response.Code);
        Assert.AreEqual(0, await CountRowsAsync(db));
    }

    [Test]
    public async Task RestStreamSendsTheRowsAfterTheCommit()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        ExecuteSQLController controller = Sql(new { databaseName = db, sql = InsertTwo });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("\"alpha\"", body);
        StringAssert.Contains("\"beta\"", body);
        StringAssert.Contains("\"status\":\"ok\"", body);
        StringAssert.Contains("\"total\":2", body);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    /// <summary>A failed statement on the stream endpoint sends no row and stores no row.</summary>
    [Test]
    public async Task RestStreamSendsNothingWhenTheStatementFails()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        ExecuteSQLController controller = Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO robots (name) VALUES ('alpha') RETURNING missing",
        });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreNotEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains(CamusDBErrorCodes.UnknownColumn, body);
        StringAssert.DoesNotContain("\"alpha\"", body);
        Assert.AreEqual(0, await CountRowsAsync(db));
    }

    // ─── gRPC unary ───────────────────────────────────────────────────────────

    [Test]
    public async Task GrpcNonQueryReturnsTheCountAndTheRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(new SqlRequest { Database = db, Sql = InsertTwo }, new TestServerCallContext());

        Assert.AreEqual(2, reply.AffectedRows);
        AssertTwoRobots(reply.ReturningSchema, reply.ReturningRows);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task GrpcNonQueryWithoutReturningLeavesTheSchemaUnset()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "INSERT INTO robots (name) VALUES ('alpha')" }, new TestServerCallContext());

        Assert.AreEqual(1, reply.AffectedRows);
        Assert.IsNull(reply.ReturningSchema);
        Assert.AreEqual(0, reply.ReturningRows.Count);
    }

    [Test]
    public async Task GrpcNonQueryCountOnlyLeavesTheSchemaUnset()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = InsertTwo, DiscardReturningRows = true }, new TestServerCallContext());

        Assert.AreEqual(2, reply.AffectedRows);
        Assert.IsNull(reply.ReturningSchema);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task GrpcQueryStreamsTheSchemaThenTheRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext context = new(CancellationToken.None);
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = InsertTwo }, writer, context);

        Assert.AreEqual(QueryStreamMessage.PayloadOneofCase.Schema, writer.Written[0].PayloadCase);
        List<ResultRow> rows = writer.Written.Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Select(m => m.Row).ToList();
        AssertTwoRobots(writer.Written[0].Schema, rows);
        Assert.IsNotNull(context.ResponseTrailers.GetValue("camus-causal-token-l"), "an autocommit write reports its commit");
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task GrpcQueryRefusesTheCountOnlyFlag()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        RpcException error = Assert.ThrowsAsync<RpcException>(async () => await grpc.ExecuteQuery(
            new SqlRequest { Database = db, Sql = InsertTwo, DiscardReturningRows = true }, writer, new TestServerCallContext()))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error.Trailers.GetValue("camus-error-code"));
        Assert.AreEqual(0, writer.Written.Count);
        Assert.AreEqual(0, await CountRowsAsync(db));
    }

    /// <summary>
    /// A unary reply carries every row at once. One a client could not receive is refused before
    /// the commit, so the statement is rolled back and the client is never told a stored write
    /// failed. The count-only flag and the streaming call both still work for the same statement.
    /// </summary>
    [Test]
    public async Task GrpcNonQueryRefusesAnOversizedReplyAndRollsBack()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE blobs (id oid NOT NULL DEFAULT(gen_id()), payload string, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(ddl);

        // Five rows of 1 MiB each: a RETURNING of them is larger than one 4 MiB reply.
        string payload = new('p', 1024 * 1024);
        for (int i = 0; i < 5; i++)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO blobs (payload) VALUES (@p)",
                new() { ["@p"] = new ColumnValue(CamusDB.Core.Catalogs.Models.ColumnType.String, payload) }));
            await database.Transactions.CommitAsync(tx);
        }

        const string copy = "INSERT INTO blobs (payload) SELECT payload FROM blobs RETURNING payload";

        RpcException error = Assert.ThrowsAsync<RpcException>(async () => await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = copy }, new TestServerCallContext()))!;
        Assert.AreEqual(CamusDBErrorCodes.ReturningResultTooLarge, error.Trailers.GetValue("camus-error-code"));
        Assert.AreEqual(StatusCode.ResourceExhausted, error.StatusCode);
        Assert.AreEqual(5, await CountRowsAsync(db, "blobs"), "the refused statement was rolled back");

        NonQueryReply countOnly = await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = copy, DiscardReturningRows = true }, new TestServerCallContext());
        Assert.AreEqual(5, countOnly.AffectedRows);
        Assert.AreEqual(10, await CountRowsAsync(db, "blobs"));

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = copy }, writer, new TestServerCallContext(CancellationToken.None));
        Assert.AreEqual(10, writer.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row));
        Assert.AreEqual(20, await CountRowsAsync(db, "blobs"));
    }

    // ─── gRPC BatchExecute ────────────────────────────────────────────────────

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.Error;

    /// <summary>Runs one batch op and returns every message the server sent for it.</summary>
    private async Task<List<BatchExecuteResponse>> RunBatchOpAsync(BatchStatementKind kind, SqlRequest request)
    {
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = kind, Request = request });

        await writer.WaitFor(m => m.RequestId == 1 && IsTerminal(m));
        reader.Complete();
        await server;

        return writer.Written.Where(m => m.RequestId == 1).ToList();
    }

    [Test]
    public async Task BatchNonQueryOpReturnsTheRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchOpAsync(
            BatchStatementKind.NonQuery, new SqlRequest { Database = db, Sql = InsertTwo });
        BatchExecuteResponse terminal = messages.Last();

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, terminal.PayloadCase, terminal.Error?.Message);
        Assert.AreEqual(2, terminal.NonQuery.AffectedRows);
        AssertTwoRobots(terminal.NonQuery.ReturningSchema, terminal.NonQuery.ReturningRows);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task BatchNonQueryOpCountOnlySendsNoRows()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchOpAsync(
            BatchStatementKind.NonQuery, new SqlRequest { Database = db, Sql = InsertTwo, DiscardReturningRows = true });
        BatchExecuteResponse terminal = messages.Last();

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, terminal.PayloadCase, terminal.Error?.Message);
        Assert.AreEqual(2, terminal.NonQuery.AffectedRows);
        Assert.IsNull(terminal.NonQuery.ReturningSchema);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }

    [Test]
    public async Task BatchQueryOpRunsTheInsertAndCommitsIt()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchOpAsync(
            BatchStatementKind.Query, new SqlRequest { Database = db, Sql = InsertTwo });

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, messages.Last().PayloadCase, messages.Last().Error?.Message);
        ResultSchema schema = messages.First(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Schema).Schema;
        List<ResultRow> rows = messages.Where(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Select(m => m.Row).ToList();
        AssertTwoRobots(schema, rows);
        Assert.AreEqual(2, await CountRowsAsync(db));
    }
}
