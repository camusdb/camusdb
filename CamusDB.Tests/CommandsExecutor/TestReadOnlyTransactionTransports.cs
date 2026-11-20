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
using GrpcValue = CamusDB.Grpc.Value;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The read-only refusal as a client sees it, on REST and on gRPC. Each case is a row of the report
/// that found the bug: a write in a read-only transaction, explicit or autocommit, returned "ok" and
/// the write was lost. Now the statement fails with <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/>
/// (HTTP 400; gRPC <c>FAILED_PRECONDITION</c> with the code in the trailer), the commit of an explicit
/// transaction fails, and the table is unchanged.
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestReadOnlyTransactionTransports : BaseTest
{
    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static readonly List<(long, string)> SeedJobs =
        [(1, "queued"), (2, "running"), (3, "done"), (4, "failed")];

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;
    private CamusRowsService rows = null!;

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
        rows = new(executor, coordinator, logger, Options);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

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

    private TransactionsController Transactions(object body) =>
        new(executor, coordinator, logger, Options) { ControllerContext = Context(body) };

    private async Task<string> CreateDatabaseWithJobsAsync()
    {
        string db = "ro" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE jobs (id INT64 PRIMARY KEY, state STRING NOT NULL)", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
            "INSERT INTO jobs (id, state) VALUES (1, 'queued'), (2, 'running'), (3, 'done'), (4, 'failed')", null));
        await database.Transactions.CommitAsync(tx);
        return db;
    }

    private async Task CreateSequenceAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db, "CREATE SEQUENCE job_ids", null));
        await database.Transactions.CommitAsync(ddl);
    }

    private async Task<List<(long, string)>> CommittedJobsAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT id, state FROM jobs", null));
            return (await cursor.ToListAsync())
                .Select(r => (r.Row["id"].LongValue, r.Row["state"].StrValue!))
                .OrderBy(p => p.Item1)
                .ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<StartTransactionResponse> RestStartAsync(object body)
    {
        JsonResult result = await Transactions(body).StartTransaction();
        StartTransactionResponse started = (StartTransactionResponse)result.Value!;
        Assert.That(started.Status, Is.EqualTo("ok"), started.Message);
        return started;
    }

    private async Task<(ExecuteNonSQLQueryResponse Response, int? StatusCode)> RestNonQueryAsync(object body)
    {
        JsonResult result = await Sql(body).ExecuteNonSQLQuery();
        return ((ExecuteNonSQLQueryResponse)result.Value!, result.StatusCode);
    }

    private async Task<CommitTransactionResponse> RestCommitAsync(string db, StartTransactionResponse started)
    {
        JsonResult result = await Transactions(new { databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter })
            .CommitTransaction();
        return (CommitTransactionResponse)result.Value!;
    }

    private static void AssertRefused(ExecuteNonSQLQueryResponse response, int? statusCode, string verb)
    {
        Assert.That(response.Status, Is.EqualTo("failed"), response.Message);
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), response.Message);
        Assert.That(response.Message, Is.EqualTo($"cannot execute {verb} in a read-only transaction"));
        Assert.That(statusCode, Is.EqualTo(400));
    }

    private static void AssertRefused(RpcException error)
    {
        Assert.That(error.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition), error.Status.Detail);
        Assert.That(error.Trailers.GetValue("camus-error-code"), Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction));
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    /// <summary>Report rows 1 and 2: an explicit Serializable or Read Committed read-only transaction.</summary>
    [TestCase("Serializable", "UPDATE jobs SET state = 'ro-write' WHERE id = 3", "UPDATE")]
    [TestCase("Serializable", "INSERT INTO jobs (id, state) VALUES (99, 'ro-insert')", "INSERT")]
    [TestCase("ReadCommitted", "DELETE FROM jobs WHERE id = 1", "DELETE")]
    public async Task RestExplicitReadOnlyTransaction_RefusesTheWrite(string isolationLevel, string sql, string verb)
    {
        string db = await CreateDatabaseWithJobsAsync();

        StartTransactionResponse started = await RestStartAsync(
            new { databaseName = db, transactionMode = "ReadOnly", isolationLevel });

        (ExecuteNonSQLQueryResponse response, int? statusCode) = await RestNonQueryAsync(
            new { databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter, sql });
        AssertRefused(response, statusCode, verb);

        // The transport rolls back an explicit transaction when a statement in it fails, so the commit
        // finds nothing to commit. It must not report success.
        CommitTransactionResponse committed = await RestCommitAsync(db, started);
        Assert.That(committed.Status, Is.EqualTo("failed"), "the refused statement ended the transaction");

        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    /// <summary>Report row 3: an autocommit write that asks for a read-only transaction.</summary>
    [Test]
    public async Task RestAutocommitReadOnlyWrite_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        (ExecuteNonSQLQueryResponse response, int? statusCode) = await RestNonQueryAsync(
            new { databaseName = db, transactionMode = "ReadOnly", sql = "UPDATE jobs SET state = 'auto-ro2' WHERE id = 1" });

        AssertRefused(response, statusCode, "UPDATE");
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    /// <summary>Report row 4: SET TRANSACTION … SERIALIZABLE READ ONLY, then an UPDATE.</summary>
    [Test]
    public async Task RestSetTransactionReadOnly_ThenUpdate_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        StartTransactionResponse started = await RestStartAsync(new { databaseName = db });

        (ExecuteNonSQLQueryResponse set, _) = await RestNonQueryAsync(new
        {
            databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter,
            sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        });
        Assert.That(set.Status, Is.EqualTo("ok"), set.Message);

        (ExecuteNonSQLQueryResponse response, int? statusCode) = await RestNonQueryAsync(new
        {
            databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter,
            sql = "UPDATE jobs SET state = 'x' WHERE id = 4",
        });
        AssertRefused(response, statusCode, "UPDATE");

        CommitTransactionResponse committed = await RestCommitAsync(db, started);
        Assert.That(committed.Status, Is.EqualTo("failed"), "the refused statement ended the transaction");
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    /// <summary>
    /// Report row 5: the write of an abandoned read-only transaction held a row lock, so another writer
    /// got <c>CADB0502</c>. Now the write is refused, and an autocommit write of the same row succeeds
    /// while that transaction is still open.
    /// </summary>
    [Test]
    public async Task RestRefusedWrite_HoldsNoRowLock()
    {
        string db = await CreateDatabaseWithJobsAsync();

        StartTransactionResponse started = await RestStartAsync(
            new { databaseName = db, transactionMode = "ReadOnly", isolationLevel = "Serializable" });

        (ExecuteNonSQLQueryResponse refused, int? statusCode) = await RestNonQueryAsync(new
        {
            databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter,
            sql = "UPDATE jobs SET state = 'ro-write' WHERE id = 4",
        });
        AssertRefused(refused, statusCode, "UPDATE");

        (ExecuteNonSQLQueryResponse other, _) = await RestNonQueryAsync(
            new { databaseName = db, sql = "UPDATE jobs SET state = 'other' WHERE id = 4" });
        Assert.That(other.Status, Is.EqualTo("ok"), other.Message);
        Assert.That(other.Rows, Is.EqualTo(1));

        Assert.That((await CommittedJobsAsync(db)).Single(r => r.Item1 == 4).Item2, Is.EqualTo("other"));
    }

    /// <summary>A write with RETURNING runs through the row-returning endpoint and is refused there too.</summary>
    [Test]
    public async Task RestQueryEndpointWriteReturning_InAReadOnlyTransaction_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        StartTransactionResponse started = await RestStartAsync(
            new { databaseName = db, transactionMode = "ReadOnly", isolationLevel = "Serializable" });

        JsonResult result = await Sql(new
        {
            databaseName = db, txnIdPT = started.TxnIdPT, txnIdCounter = started.TxnIdCounter,
            sql = "DELETE FROM jobs WHERE id = 2 RETURNING id",
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), response.Message);
        Assert.That(result.StatusCode, Is.EqualTo(400));

        await RestCommitAsync(db, started);
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    /// <summary>An autocommit DDL request that asks for ReadOnly is refused, and the table is not created.</summary>
    [Test]
    public async Task RestDdlWithReadOnlyMode_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        JsonResult result = await Sql(new
        {
            databaseName = db, transactionMode = "ReadOnly",
            sql = "CREATE TABLE notes (id INT64 PRIMARY KEY, body STRING)",
        }).ExecuteSQLDDL();
        ExecuteDDLSQLResponse response = (ExecuteDDLSQLResponse)result.Value!;

        Assert.That(response.Status, Is.EqualTo("failed"));
        Assert.That(response.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), response.Message);
        Assert.That(response.Message, Is.EqualTo("cannot execute CREATE TABLE in a read-only transaction"));
        Assert.That(result.StatusCode, Is.EqualTo(400));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.That(database.Schema.Tables.ContainsKey("notes"), Is.False);
    }

    /// <summary>
    /// An autocommit <c>SELECT nextval(...)</c> that asks for no mode runs in the snapshot the server
    /// picks, and draws a value. The same request with <c>"transactionMode": "ReadOnly"</c> declares a
    /// read-only transaction and is refused, on the buffered and the streaming query endpoints.
    /// </summary>
    [Test]
    public async Task RestQueryNextval_RunsWithoutAMode_AndIsRefusedWithReadOnly()
    {
        string db = await CreateDatabaseWithJobsAsync();
        await CreateSequenceAsync(db);

        JsonResult drawn = await Sql(new { databaseName = db, sql = "SELECT nextval('job_ids') AS v" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse drawnResponse = (ExecuteSQLQueryResponse)drawn.Value!;
        Assert.That(drawnResponse.Status, Is.EqualTo("ok"), drawnResponse.Message);
        Assert.That(drawnResponse.Total, Is.EqualTo(1));

        JsonResult refused = await Sql(new { databaseName = db, transactionMode = "ReadOnly", sql = "SELECT nextval('job_ids') AS v" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse refusedResponse = (ExecuteSQLQueryResponse)refused.Value!;
        Assert.That(refusedResponse.Status, Is.EqualTo("failed"));
        Assert.That(refusedResponse.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), refusedResponse.Message);
        Assert.That(refused.StatusCode, Is.EqualTo(400));

        ExecuteSQLController stream = Sql(new { databaseName = db, transactionMode = "ReadOnly", sql = "SELECT nextval('job_ids') AS v" });
        await stream.ExecuteSQLQueryStream();
        Stream body = stream.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        string text = await new StreamReader(body, Encoding.UTF8).ReadToEndAsync();
        StringAssert.Contains(CamusDBErrorCodes.ReadOnlyTransaction, text);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [TestCase(IsolationLevel.Serializable)]
    [TestCase(IsolationLevel.ReadCommitted)]
    public async Task GrpcExplicitReadOnlyTransaction_RefusesTheWrite(IsolationLevel level)
    {
        string db = await CreateDatabaseWithJobsAsync();

        TxnHandle handle = await grpc.StartTransaction(
            new StartTxnRequest { Database = db, IsolationLevel = level, TransactionMode = TransactionMode.ReadOnly },
            new TestServerCallContext());

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteNonQuery(
                new SqlRequest { Database = db, Sql = "UPDATE jobs SET state = 'ro-write' WHERE id = 3", TxnHandle = handle },
                new TestServerCallContext()))!;
        AssertRefused(error);

        // The transport rolls back an explicit transaction when a statement in it fails, so the commit
        // finds nothing to commit. It must not report success.
        Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.CommitTransaction(handle, new TestServerCallContext()));

        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    [Test]
    public async Task GrpcAutocommitReadOnlyWrite_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteNonQuery(
                new SqlRequest { Database = db, Sql = "DELETE FROM jobs WHERE id = 1", TransactionMode = TransactionMode.ReadOnly },
                new TestServerCallContext()))!;

        AssertRefused(error);
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    /// <summary>The row service writes through the ticket path, which refuses a read-only transaction too.</summary>
    [Test]
    public async Task GrpcInsertRow_InAReadOnlyTransaction_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        TxnHandle handle = await grpc.StartTransaction(
            new StartTxnRequest { Database = db, IsolationLevel = IsolationLevel.Serializable, TransactionMode = TransactionMode.ReadOnly },
            new TestServerCallContext());

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await rows.InsertRow(new InsertRowRequest
            {
                Database = db,
                Table = "jobs",
                Values = { new Dictionary<string, GrpcValue> { ["id"] = new() { Int64Value = 50 }, ["state"] = new() { StringValue = "row" } } },
                TxnHandle = handle,
            }, new TestServerCallContext()))!;

        AssertRefused(error);

        try { await grpc.RollbackTransaction(handle, new TestServerCallContext()); } catch (RpcException) { }
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    [Test]
    public async Task GrpcDdlWithReadOnlyMode_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteDdl(
                new SqlRequest { Database = db, Sql = "DROP TABLE jobs", TransactionMode = TransactionMode.ReadOnly },
                new TestServerCallContext()))!;

        AssertRefused(error);
        Assert.That(await CommittedJobsAsync(db), Is.EqualTo(SeedJobs));
    }

    [Test]
    public async Task GrpcQueryNextval_RunsWithoutAMode_AndIsRefusedWithReadOnly()
    {
        string db = await CreateDatabaseWithJobsAsync();
        await CreateSequenceAsync(db);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT nextval('job_ids') AS v" }, writer, new TestServerCallContext());
        Assert.That(writer.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row), Is.EqualTo(1));

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteQuery(
                new SqlRequest { Database = db, Sql = "SELECT nextval('job_ids') AS v", TransactionMode = TransactionMode.ReadOnly },
                new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()))!;
        AssertRefused(error);
    }

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.Error;

    /// <summary>The batch stream's autocommit query declares read-only the same way.</summary>
    [Test]
    public async Task GrpcBatchQueryNextval_WithReadOnly_IsRefused()
    {
        string db = await CreateDatabaseWithJobsAsync();
        await CreateSequenceAsync(db);

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 1,
            Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = db, Sql = "SELECT nextval('job_ids') AS v", TransactionMode = TransactionMode.ReadOnly },
        });
        await writer.WaitFor(m => m.RequestId == 1 && IsTerminal(m));
        reader.Complete();
        await server;

        BatchExecuteResponse last = writer.Written.Last(m => m.RequestId == 1);
        Assert.That(last.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
        Assert.That(last.Error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), last.Error.Message);
    }
}
