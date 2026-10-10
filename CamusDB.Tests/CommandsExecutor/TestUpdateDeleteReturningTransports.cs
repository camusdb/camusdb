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
/// <c>UPDATE … RETURNING</c> and <c>DELETE … RETURNING</c> over every transport that can carry them:
/// the REST query, stream and non-query endpoints, a REST prepared statement, gRPC
/// <c>ExecuteQuery</c> and <c>ExecuteNonQuery</c>, and the batch op kinds, including a prepared batch
/// query. Each test checks the rows the transport sent and then reads the table back, so a transport
/// that answers with rows but never commits — or runs the write in a read-only snapshot — fails.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestUpdateDeleteReturningTransports : BaseTest
{
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

    /// <summary>A database whose <c>robots</c> table holds alpha (2001), beta (2002) and gamma (2003).</summary>
    private async Task<string> CreateDatabaseWithRobotsAsync()
    {
        string db = "udr" + Guid.NewGuid().ToString("n")[..8];
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

    /// <summary>The committed (name, year) pairs of <c>robots</c>, sorted by name.</summary>
    private async Task<List<(string Name, long Year)>> StoredRobotsAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT name, year FROM robots", null));
            List<(string, long)> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add((row.Row["name"].StrValue!, row.Row["year"].LongValue));
            return rows.OrderBy(r => r.Item1, StringComparer.Ordinal).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // Both statements change beta and gamma. The UPDATE returns their new years; the DELETE returns
    // the rows it removed.
    private const string Update = "update";
    private const string Delete = "delete";

    private static string Statement(string kind) => kind == Update
        ? "UPDATE robots SET year = year + 100 WHERE year >= 2002 RETURNING name, year"
        : "DELETE FROM robots WHERE year >= 2002 RETURNING name, year";

    private static (string, long)[] ExpectedReturned(string kind) => kind == Update
        ? [("beta", 2102), ("gamma", 2103)]
        : [("beta", 2002), ("gamma", 2003)];

    private static (string, long)[] ExpectedStored(string kind) => kind == Update
        ? [("alpha", 2001), ("beta", 2102), ("gamma", 2103)]
        : [("alpha", 2001)];

    private static readonly (string, long)[] Untouched = [("alpha", 2001), ("beta", 2002), ("gamma", 2003)];

    private static void AssertReturned(string kind, IEnumerable<(string, long)> returned)
        => CollectionAssert.AreEqual(ExpectedReturned(kind), returned.OrderBy(r => r.Item1, StringComparer.Ordinal).ToArray());

    private static void AssertReturned(string kind, ResultSchema schema, IReadOnlyList<ResultRow> rows)
    {
        CollectionAssert.AreEqual(new[] { "name", "year" }, schema.Columns.Select(c => c.Name));
        AssertReturned(kind, rows.Select(r => (r.Values[0].StringValue, r.Values[1].Int64Value)));
    }

    private static void AssertReturned(string kind, PositionalRowSet rows)
        => AssertReturned(kind, rows.Rows.Select(r => (r.Row[rows.Schema[0].RowKey].StrValue!, r.Row[rows.Schema[1].RowKey].LongValue)));

    private async Task AssertStoredAsync(string db, (string, long)[] expected)
        => CollectionAssert.AreEqual(expected, (await StoredRobotsAsync(db)).Select(r => (r.Name, r.Year)).ToArray());

    // ─── REST ─────────────────────────────────────────────────────────────────

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestNonQueryReturnsTheCountAndTheRows(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = Statement(kind) }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Rows);
        CollectionAssert.AreEqual(new[] { "name", "year" }, response.Columns!.Select(c => c.Name));
        AssertReturned(kind, response.ReturningRows!);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestNonQueryCountOnlyWritesAndSendsNoRows(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = Statement(kind), discardReturningRows = true }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Rows);
        Assert.IsNull(response.Columns);
        Assert.IsNull(response.ReturningRows);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    /// <summary>
    /// The query endpoint runs an autocommit SELECT in a read-only snapshot. An UPDATE or a DELETE with
    /// RETURNING there must get a writable transaction and must commit, or the rows it reports are lost.
    /// </summary>
    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestQueryRunsTheWriteAndCommitsIt(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = Statement(kind) }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(2, response.Total);
        CollectionAssert.AreEqual(new[] { "name", "year" }, response.Columns.Select(c => c.Name));
        AssertReturned(kind, response.Rows);
        Assert.IsNotNull(response.CausalToken, "an autocommit write reports its commit");
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestQueryRefusesTheCountOnlyFlag(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = Statement(kind), discardReturningRows = true }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, response.Code);
        await AssertStoredAsync(db, Untouched);
    }

    /// <summary>Without a RETURNING list the statement is not a query: the query endpoint refuses it and changes nothing.</summary>
    [TestCase("UPDATE robots SET year = 0 WHERE year >= 2002")]
    [TestCase("DELETE FROM robots WHERE year >= 2002")]
    public async Task RestQueryRefusesTheWriteWithoutReturning(string sql)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult result = await Sql(new { databaseName = db, sql }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("failed", response.Status);
        await AssertStoredAsync(db, Untouched);
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestStreamSendsTheRowsAfterTheCommit(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        ExecuteSQLController controller = Sql(new { databaseName = db, sql = Statement(kind) });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("\"beta\"", body);
        StringAssert.Contains("\"gamma\"", body);
        StringAssert.DoesNotContain("\"alpha\"", body);
        StringAssert.Contains("\"status\":\"ok\"", body);
        StringAssert.Contains("\"total\":2", body);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    /// <summary>
    /// A prepared statement reaches the query endpoint with its stored root type only. The endpoint
    /// must still see that the statement writes, open a writable transaction and commit.
    /// </summary>
    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task RestPreparedQueryRunsTheWriteAndCommitsIt(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        JsonResult prepared = await Statements(new { databaseName = db, sql = Statement(kind) }).PrepareSQLStatement();
        PrepareStatementResponse handle = (PrepareStatementResponse)prepared.Value!;
        Assert.That(handle.StatementId, Is.Not.Null.And.Not.Empty, handle.Message);

        JsonResult result = await Sql(new { statementId = handle.StatementId }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        AssertReturned(kind, response.Rows);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    // ─── gRPC unary ───────────────────────────────────────────────────────────

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task GrpcNonQueryReturnsTheCountAndTheRows(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(new SqlRequest { Database = db, Sql = Statement(kind) }, new TestServerCallContext());

        Assert.AreEqual(2, reply.AffectedRows);
        AssertReturned(kind, reply.ReturningSchema, reply.ReturningRows);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task GrpcNonQueryCountOnlyLeavesTheSchemaUnset(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = Statement(kind), DiscardReturningRows = true }, new TestServerCallContext());

        Assert.AreEqual(2, reply.AffectedRows);
        Assert.IsNull(reply.ReturningSchema);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task GrpcQueryStreamsTheSchemaThenTheRows(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext context = new(CancellationToken.None);
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = Statement(kind) }, writer, context);

        Assert.AreEqual(QueryStreamMessage.PayloadOneofCase.Schema, writer.Written[0].PayloadCase);
        List<ResultRow> rows = writer.Written.Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Select(m => m.Row).ToList();
        AssertReturned(kind, writer.Written[0].Schema, rows);
        Assert.IsNotNull(context.ResponseTrailers.GetValue("camus-causal-token-l"), "an autocommit write reports its commit");
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    /// <summary>
    /// A unary reply carries every row at once. One a client could not receive is refused before the
    /// commit, so the UPDATE is rolled back. The count-only flag still works for the same statement.
    /// </summary>
    [Test]
    public async Task GrpcNonQueryRefusesAnOversizedUpdateReplyAndRollsBack()
    {
        string db = await CreateDatabaseWithRobotsAsync();

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE blobs (id oid NOT NULL DEFAULT(gen_id()), n int64, payload string, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(ddl);

        // Five rows of 1 MiB each: a RETURNING of them is larger than one 4 MiB reply.
        string payload = new('p', 1024 * 1024);
        for (int i = 0; i < 5; i++)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO blobs (n, payload) VALUES (0, @p)",
                new() { ["@p"] = new ColumnValue(CamusDB.Core.Catalogs.Models.ColumnType.String, payload) }));
            await database.Transactions.CommitAsync(tx);
        }

        RpcException error = Assert.ThrowsAsync<RpcException>(async () => await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "UPDATE blobs SET n = 1 WHERE n = 0 RETURNING payload" }, new TestServerCallContext()))!;
        Assert.AreEqual(CamusDBErrorCodes.ReturningResultTooLarge, error.Trailers.GetValue("camus-error-code"));
        Assert.AreEqual(StatusCode.ResourceExhausted, error.StatusCode);

        NonQueryReply countOnly = await grpc.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "UPDATE blobs SET n = 2 WHERE n = 0 RETURNING payload", DiscardReturningRows = true },
            new TestServerCallContext());
        Assert.AreEqual(5, countOnly.AffectedRows, "the refused statement was rolled back, so every row still had n = 0");
    }

    /// <summary>A Boolean item in the list reaches the gRPC schema as BOOL, beside BOOL values.</summary>
    [TestCase("UPDATE robots SET year = year + 1 WHERE name = 'beta' RETURNING year > 2000 AS recent")]
    [TestCase("DELETE FROM robots WHERE name = 'beta' RETURNING year > 2000 AS recent")]
    public async Task GrpcSchemaDeclaresBoolForABooleanItem(string sql)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        NonQueryReply reply = await grpc.ExecuteNonQuery(new SqlRequest { Database = db, Sql = sql }, new TestServerCallContext());

        Assert.AreEqual(ColumnType.Bool, reply.ReturningSchema.Columns[0].Type);
        Assert.AreEqual(Value.KindOneofCase.BoolValue, reply.ReturningRows[0].Values[0].KindCase);
        Assert.IsTrue(reply.ReturningRows[0].Values[0].BoolValue);
    }

    // ─── gRPC BatchExecute ────────────────────────────────────────────────────

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

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task BatchNonQueryOpReturnsTheRows(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.NonQuery, _ => new SqlRequest { Database = db, Sql = Statement(kind) }));
        BatchExecuteResponse terminal = messages.Last(m => m.RequestId == 1);

        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, terminal.PayloadCase, terminal.Error?.Message);
        Assert.AreEqual(2, terminal.NonQuery.AffectedRows);
        AssertReturned(kind, terminal.NonQuery.ReturningSchema, terminal.NonQuery.ReturningRows);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task BatchQueryOpRunsTheWriteAndCommitsIt(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.Query, _ => new SqlRequest { Database = db, Sql = Statement(kind) }));

        AssertBatchQueryReturned(kind, messages, requestId: 1);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    /// <summary>
    /// A prepared batch query op knows its statement by the stored root type only. It must still open
    /// a writable transaction for an UPDATE or a DELETE with RETURNING, and commit it.
    /// </summary>
    [TestCase(Update)]
    [TestCase(Delete)]
    public async Task BatchPreparedQueryOpRunsTheWriteAndCommitsIt(string kind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.Prepare, _ => new SqlRequest { Database = db, Sql = Statement(kind) }),
            (BatchStatementKind.Query, written => new SqlRequest
            {
                StatementId = written.First(m => m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply)
                    .PrepareReply.StatementId,
            }));

        AssertBatchQueryReturned(kind, messages, requestId: 2);
        await AssertStoredAsync(db, ExpectedStored(kind));
    }

    /// <summary>
    /// In an explicit batch transaction, a write whose RETURNING expression fails on a row has already
    /// staged its earlier changes. The op reports the error, and the transaction is rolled back with it:
    /// a later COMMIT on the same handle fails, nothing the failed statement changed is stored, and the
    /// client's ROLLBACK still succeeds as a no-op.
    /// </summary>
    [TestCase(Update, BatchStatementKind.NonQuery)]
    [TestCase(Update, BatchStatementKind.Query)]
    [TestCase(Delete, BatchStatementKind.NonQuery)]
    [TestCase(Delete, BatchStatementKind.Query)]
    public async Task BatchExplicitTransactionCannotCommitAWriteWhoseReturningFailed(string kind, BatchStatementKind opKind)
    {
        string db = await CreateDatabaseWithRobotsAsync();

        string failing = kind == Update
            ? "UPDATE robots SET year = year + 10 WHERE year > 0 RETURNING 1 / (year - year)"
            : "DELETE FROM robots WHERE year > 0 RETURNING 1 / (year - year)";

        static TxnHandle Handle(List<BatchExecuteResponse> written)
            => written.First(m => m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply).StartReply;

        List<BatchExecuteResponse> messages = await RunBatchAsync(
            (BatchStatementKind.Start, _ => new SqlRequest { Database = db }),
            (opKind, written => new SqlRequest { Database = db, Sql = failing, TxnHandle = Handle(written) }),
            (BatchStatementKind.Commit, written => new SqlRequest { Database = db, TxnHandle = Handle(written) }),
            (BatchStatementKind.Rollback, written => new SqlRequest { Database = db, TxnHandle = Handle(written) }));

        BatchExecuteResponse statement = messages.Last(m => m.RequestId == 2);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, statement.PayloadCase);
        StringAssert.Contains("zero", statement.Error.Message.ToLowerInvariant());

        BatchExecuteResponse commit = messages.Last(m => m.RequestId == 3);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.Error, commit.PayloadCase,
            "a COMMIT after the failed statement must not succeed");

        BatchExecuteResponse rollback = messages.Last(m => m.RequestId == 4);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.RollbackReply, rollback.PayloadCase, rollback.Error?.Message);

        await AssertStoredAsync(db, Untouched);
    }

    private static void AssertBatchQueryReturned(string kind, List<BatchExecuteResponse> messages, int requestId)
    {
        List<BatchExecuteResponse> op = messages.Where(m => m.RequestId == requestId).ToList();
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.QueryComplete, op.Last().PayloadCase, op.Last().Error?.Message);
        ResultSchema schema = op.First(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Schema).Schema;
        List<ResultRow> rows = op.Where(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row).Select(m => m.Row).ToList();
        AssertReturned(kind, schema, rows);
    }
}
