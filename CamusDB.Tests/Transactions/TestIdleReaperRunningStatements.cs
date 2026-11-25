
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;
using CamusDB.Tests.Utils;

using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using QueryFilter = CamusDB.Grpc.QueryFilter;
using SqlRequest = CamusDB.Grpc.SqlRequest;
using UpdateByIdRequest = CamusDB.Grpc.UpdateByIdRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The idle reaper must leave a transaction alone while a statement runs on it, on every transport and
/// for autocommit statements too, and must still end a transaction whose client went away.
///
/// <para>Each case drives a real entry point and holds its statement in the middle: at the open of its
/// table (<see cref="TableGate"/>) or at the write of its rows to a client that does not read. While the
/// statement is held, a sweep with a zero timeout runs. Such a sweep takes every transaction that counts
/// as idle, so it stands for a statement that is longer than any timeout.</para>
/// </summary>
[TestFixture]
// Serial: boots embedded Kahuna nodes and holds transactions open across sweeps.
[NonParallelizable]
internal sealed class TestIdleReaperRunningStatements : SharedNodeBaseTest
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

    private const string RowId = "000000000000000000000001";

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private string db = null!;
    private DatabaseDescriptor database = null!;
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry prepared = null!;
    private CamusSqlService sql = null!;
    private CamusRowsService rows = null!;

    private async Task SetUpEngineAsync(CamusDBOptions? options = null)
    {
        CamusDBOptions effective = options ?? Options;
        (db, database, executor) = await CreateDatabase(effective);
        coordinator = new HttpTransactionCoordinator(executor);
        prepared = new PreparedStatementRegistry(effective);
        sql = new CamusSqlService(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), effective);
        rows = new CamusRowsService(executor, coordinator, logger, effective);

        await DdlAsync("CREATE TABLE items (id oid PRIMARY KEY, v INT64)");
        await WriteAsync($"INSERT INTO items (id, v) VALUES ('{RowId}', 10), (gen_id(), 20), (gen_id(), 30)");
    }

    private async Task DdlAsync(string statement)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, statement, null));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
    }

    private async Task WriteAsync(string statement)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, statement, null));
        await database.Transactions.CommitAsync(tx);
    }

    private async Task<List<long>> StoredAsync()
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT v FROM items ORDER BY v", null));
            return (await cursor.ToListAsync()).Select(r => r.Row["v"].LongValue).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>An explicit transaction, as a client's BEGIN leaves it: open, with no statement running.</summary>
    private Task<KvTransaction> BeginExplicitAsync() =>
        coordinator.StartAsync(db, isolationLevel: null, transactionMode: null, sessionOwned: true);

    private static TxnHandle HandleOf(KvTransaction tx) =>
        new() { TxnIdPt = tx.ClientId.L, TxnIdCounter = (uint)tx.ClientId.C };

    private Task<int> SweepAsync() => coordinator.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);

    /// <summary>Test-only: a response body whose writes wait until <see cref="Release"/>, as for a client that does not read.</summary>
    private sealed class ParkedBody : MemoryStream
    {
        private readonly TaskCompletionSource parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Parked => parked.Task.WaitAsync(Deadline);

        public void Release() => released.TrySetResult();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            parked.TrySetResult();
            await released.Task.WaitAsync(cancellationToken);
            await base.WriteAsync(buffer, cancellationToken);
        }
    }

    /// <summary>Test-only: a request body that reports when the server has read all of it.</summary>
    private sealed class SignallingBody(byte[] bytes, Action onEnd) : MemoryStream(bytes)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await base.ReadAsync(buffer, cancellationToken);
            if (read == 0)
                onEnd();

            return read;
        }
    }

    /// <summary>Test-only: records the levels a controller logs at.</summary>
    private sealed class RecordingLogger : ILogger<ICamusDB>
    {
        public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Enqueue((logLevel, formatter(state, exception)));
    }

    // ─── Entry points ─────────────────────────────────────────────────────────

    private static ControllerContext Context(object body, Stream? responseBody = null, CancellationToken aborted = default)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new() { RequestAborted = aborted };
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = responseBody ?? new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body, Stream? responseBody = null, CancellationToken aborted = default) =>
        new(executor, coordinator, prepared, logger, Options) { ControllerContext = Context(body, responseBody, aborted) };

    private static readonly string[] RestWrites =
        ["non-query", "returning", "stream returning", "ddl", "insert", "update", "delete"];

    /// <summary>
    /// Sends one autocommit write to a REST endpoint and returns the HTTP status. With
    /// <paramref name="leaveAfterBody"/> the client of the request leaves when the server has read the
    /// whole body, so before the transaction begins.
    /// </summary>
    private async Task<int> RestWriteAsync(
        string endpoint, RecordingLogger log, CancellationTokenSource client, bool leaveAfterBody = false)
    {
        const string update = "UPDATE items SET v = 11 WHERE v = 10";

        object body = endpoint switch
        {
            "non-query" => new { databaseName = db, sql = update },
            "returning" or "stream returning" => new { databaseName = db, sql = update + " RETURNING v" },
            "ddl" => new { databaseName = db, sql = "ALTER TABLE items ADD COLUMN extra INT64" },
            "insert" => new { databaseName = db, tableName = "items", values = new { id = new { type = (int)ColumnType.Id, strValue = "000000000000000000000009" }, v = Int64(40) } },
            "update" => new { databaseName = db, tableName = "items", values = new { v = Int64(11) }, filters = VEquals(10) },
            "delete" => new { databaseName = db, tableName = "items", filters = VEquals(10) },
            _ => throw new ArgumentOutOfRangeException(nameof(endpoint), endpoint, null),
        };

        ControllerContext context = Context(body, aborted: client.Token);
        if (leaveAfterBody)
            context.HttpContext.Request.Body = new SignallingBody(
                Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson)), client.Cancel);

        ExecuteSQLController sqlController = new(executor, coordinator, prepared, log, Options) { ControllerContext = context };

        if (endpoint == "stream returning")
        {
            await sqlController.ExecuteSQLQueryStream();
            return context.HttpContext.Response.StatusCode;
        }

        JsonResult result = endpoint switch
        {
            "non-query" => await sqlController.ExecuteNonSQLQuery(),
            "returning" => await sqlController.ExecuteSQLQuery(),
            "ddl" => await sqlController.ExecuteSQLDDL(),
            "insert" => await new InsertController(executor, coordinator, log, Options) { ControllerContext = context }.Insert(),
            "update" => await new UpdateController(executor, coordinator, log, Options) { ControllerContext = context }.Update(),
            _ => await new DeleteController(executor, coordinator, log, Options) { ControllerContext = context }.Delete(),
        };
        return result.StatusCode ?? StatusCodes.Status200OK;
    }

    /// <summary>A disconnect is ordinary traffic, so it must not reach the operator's log.</summary>
    private static void AssertQuiet(RecordingLogger log)
    {
        List<string> loud = log.Entries.Where(e => e.Level >= LogLevel.Warning).Select(e => e.Message).ToList();
        Assert.IsEmpty(loud, string.Join("; ", loud));
    }

    private object SqlBody(string statement, KvTransaction? tx) => new
    {
        databaseName = db,
        sql = statement,
        txnIdPT = tx?.ClientId.L ?? 0,
        txnIdCounter = tx?.ClientId.C ?? 0,
    };

    private SqlRequest SqlMessage(string statement, KvTransaction? tx) =>
        new() { Database = db, Sql = statement, TxnHandle = tx is null ? null : HandleOf(tx) };

    private static void AssertOk(JsonResult result)
    {
        (string? status, string? message) = result.Value switch
        {
            ExecuteSQLQueryResponse r => (r.Status, r.Message),
            ExecuteNonSQLQueryResponse r => (r.Status, r.Message),
            ExecuteDDLSQLResponse r => (r.Status, r.Message),
            InsertResponse r => (r.Status, r.Message),
            UpdateResponse r => (r.Status, r.Message),
            DeleteResponse r => (r.Status, r.Message),
            QueryResponse r => (r.Status, r.Message),
            _ => throw new InvalidOperationException($"Unexpected response {result.Value?.GetType().Name}"),
        };
        Assert.AreEqual("ok", status, message);
    }

    private static object Int64(long value) => new { type = (int)ColumnType.Integer64, longValue = value };

    private static object[] VEquals(long value) => [new { columnName = "v", op = "=", value = Int64(value) }];

    private static QueryFilter GrpcVEquals(long value) =>
        new() { ColumnName = "v", Op = "=", Value = new Value { Int64Value = value } };

    /// <summary>
    /// Runs one statement through the named entry point and asserts that it succeeded. A null
    /// <paramref name="tx"/> is autocommit. <paramref name="ct"/> is the token of a unary gRPC call.
    /// </summary>
    private async Task RunAsync(string entryPoint, KvTransaction? tx, CancellationToken ct = default)
    {
        // A row-returning autocommit statement is tracked only when it writes.
        const string update = "UPDATE items SET v = 11 WHERE v = 10";
        string query = tx is null ? update + " RETURNING v" : "SELECT v FROM items";
        TxnHandle? handle = tx is null ? null : HandleOf(tx);
        TestServerCallContext call = new(ct);

        object Row(object body) => JsonSerializer.Deserialize<Dictionary<string, object>>(
            JsonSerializer.Serialize(body, WireJson))!
            .Concat(new Dictionary<string, object>
            {
                ["databaseName"] = db,
                ["tableName"] = "items",
                ["txnIdPT"] = tx?.ClientId.L ?? 0,
                ["txnIdCounter"] = tx?.ClientId.C ?? 0,
            }).ToDictionary(p => p.Key, p => p.Value);

        switch (entryPoint)
        {
            case "rest non-query":
                AssertOk(await Sql(SqlBody(update, tx)).ExecuteNonSQLQuery());
                break;
            case "rest query":
                AssertOk(await Sql(SqlBody(query, tx)).ExecuteSQLQuery());
                break;
            case "rest stream":
            {
                MemoryStream body = new();
                await Sql(SqlBody(query, tx), body).ExecuteSQLQueryStream();
                StringAssert.Contains("\"status\":\"ok\"", Encoding.UTF8.GetString(body.ToArray()));
                break;
            }
            case "rest ddl":
                AssertOk(await Sql(SqlBody("ALTER TABLE items ADD COLUMN extra INT64", tx)).ExecuteSQLDDL());
                break;
            case "rest insert":
                AssertOk(await new InsertController(executor, coordinator, logger, Options)
                {
                    ControllerContext = Context(Row(new { values = new { id = new { type = (int)ColumnType.Id, strValue = "000000000000000000000009" }, v = Int64(40) } })),
                }.Insert());
                break;
            case "rest update":
                AssertOk(await new UpdateController(executor, coordinator, logger, Options)
                {
                    ControllerContext = Context(Row(new { values = new { v = Int64(11) }, filters = VEquals(10) })),
                }.Update());
                break;
            case "rest delete":
                AssertOk(await new DeleteController(executor, coordinator, logger, Options)
                {
                    ControllerContext = Context(Row(new { filters = VEquals(10) })),
                }.Delete());
                break;
            case "rest row query":
                AssertOk(await new QueryController(executor, coordinator, logger, Options)
                {
                    ControllerContext = Context(Row(new { filters = VEquals(10) })),
                }.Query());
                break;
            case "rest query by id":
                AssertOk(await new QueryController(executor, coordinator, logger, Options)
                {
                    ControllerContext = Context(Row(new { id = RowId })),
                }.QueryById());
                break;

            case "grpc non-query":
                await sql.ExecuteNonQuery(SqlMessage(update, tx), call);
                break;
            case "grpc query":
                await sql.ExecuteQuery(SqlMessage(query, tx), new CapturingStreamWriter<QueryStreamMessage>(), call);
                break;
            case "grpc ddl":
                await sql.ExecuteDdl(SqlMessage("ALTER TABLE items ADD COLUMN extra INT64", tx), call);
                break;

            case "grpc insert row":
                await rows.InsertRow(new InsertRowRequest
                {
                    Database = db, Table = "items", TxnHandle = handle,
                    Values = { ["id"] = new Value { IdValue = "000000000000000000000009" }, ["v"] = new Value { Int64Value = 40 } },
                }, call);
                break;
            case "grpc update rows":
                await rows.UpdateRows(new UpdateRowsRequest
                {
                    Database = db, Table = "items", TxnHandle = handle,
                    Values = { ["v"] = new Value { Int64Value = 11 } }, Filters = { GrpcVEquals(10) },
                }, call);
                break;
            case "grpc update by id":
                await rows.UpdateById(new UpdateByIdRequest
                {
                    Database = db, Table = "items", TxnHandle = handle, Id = RowId,
                    Values = { ["v"] = new Value { Int64Value = 11 } },
                }, call);
                break;
            case "grpc delete rows":
                await rows.DeleteRows(new DeleteRowsRequest
                {
                    Database = db, Table = "items", TxnHandle = handle, Filters = { GrpcVEquals(10) },
                }, call);
                break;
            case "grpc delete by id":
                await rows.DeleteById(new RowByIdRequest { Database = db, Table = "items", TxnHandle = handle, Id = RowId }, call);
                break;
            case "grpc row query":
                await rows.Query(new RowQueryRequest { Database = db, Table = "items", TxnHandle = handle },
                    new CapturingStreamWriter<QueryStreamMessage>(), call);
                break;
            case "grpc query by id":
                await rows.QueryById(new RowByIdRequest { Database = db, Table = "items", TxnHandle = handle, Id = RowId },
                    new CapturingStreamWriter<QueryStreamMessage>(), call);
                break;

            case "batch non-query":
                AssertNoBatchError(await BatchAsync(BatchOp(1, BatchStatementKind.NonQuery, update, tx)));
                break;
            case "batch query":
                AssertNoBatchError(await BatchAsync(BatchOp(1, BatchStatementKind.Query, query, tx)));
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(entryPoint), entryPoint, null);
        }
    }

    private BatchExecuteRequest BatchOp(int id, BatchStatementKind kind, string statement, KvTransaction? tx) =>
        new() { RequestId = id, Kind = kind, Request = SqlMessage(statement, tx) };

    private async Task<List<BatchExecuteResponse>> BatchAsync(params BatchExecuteRequest[] ops)
    {
        CapturingStreamWriter<BatchExecuteResponse> writer = new();
        await sql.BatchExecute(new FakeAsyncStreamReader<BatchExecuteRequest>(ops), writer, new TestServerCallContext());
        return writer.Written;
    }

    private static void AssertNoBatchError(List<BatchExecuteResponse> responses)
    {
        BatchExecuteResponse? error = responses.Find(r => r.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Error);
        Assert.IsNull(error, error?.Error.Message);
    }

    // ─── A running statement ──────────────────────────────────────────────────

    private static readonly string[] EveryEntryPoint =
    [
        "rest non-query", "rest query", "rest stream", "rest ddl",
        "rest insert", "rest update", "rest delete", "rest row query", "rest query by id",
        "grpc non-query", "grpc query", "grpc ddl",
        "grpc insert row", "grpc update rows", "grpc update by id", "grpc delete rows", "grpc delete by id",
        "grpc row query", "grpc query by id",
        "batch non-query", "batch query",
    ];

    /// <summary>
    /// The entry points whose autocommit form begins a tracked transaction before it opens the table.
    /// The by-id row writes open the table first, and the row reads use an untracked snapshot.
    /// </summary>
    private static readonly string[] AutocommitEntryPoints =
    [
        "rest non-query", "rest query", "rest stream", "rest ddl",
        "rest insert", "rest update", "rest delete",
        "grpc non-query", "grpc query", "grpc ddl",
        "grpc insert row", "grpc update rows", "grpc delete rows",
        "batch non-query", "batch query",
    ];

    [TestCaseSource(nameof(EveryEntryPoint))]
    public async Task ExplicitTransaction_WithARunningStatement_IsNotReaped(string entryPoint)
    {
        // The gate is on the shared descriptor of the table. With transactional DDL on, a schema statement
        // of a transaction opens the table in its own schema and does not meet the gate.
        await SetUpEngineAsync(Options with { TransactionalDdlEnabled = false });
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task statement = Task.Run(() => RunAsync(entryPoint, tx));
        try
        {
            await gate.Reached;

            Assert.AreEqual(0, await SweepAsync(), "a transaction with a running statement is not idle");
            Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
            Assert.AreEqual(1, coordinator.ActiveCount);
        }
        finally
        {
            gate.Open();
        }

        await statement.WaitAsync(Deadline);

        // The statement ended, so nothing holds the transaction any more and it is idle again.
        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));
        Assert.AreEqual(1, await SweepAsync(), "an idle transaction is still reaped");
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>
    /// With transactional DDL on, a schema statement that waits for the DDL lock counts as running too. The
    /// reap of the transaction after it drops the staged column and frees the lock.
    /// </summary>
    [TestCase("rest ddl")]
    [TestCase("grpc ddl")]
    public async Task ExplicitTransaction_WithAWaitingSchemaStatement_IsNotReaped(string entryPoint)
    {
        await SetUpEngineAsync(Options with { TransactionalDdlEnabled = true });
        KvTransaction tx = await BeginExplicitAsync();

        Task statement;
        TransactionalDdlLock? holder = await TransactionalDdlLock.TryAcquireAsync(database, logger: null);
        Assert.IsNotNull(holder);
        await using (holder)
        {
            statement = Task.Run(() => RunAsync(entryPoint, tx));
            Assert.IsTrue(
                SpinWait.SpinUntil(() => coordinator.RunningStatementsForTest(tx) == 1, Deadline),
                "the statement did not start");

            Assert.AreEqual(0, await SweepAsync(), "a transaction with a waiting statement is not idle");
            Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
            Assert.AreEqual(1, coordinator.ActiveCount);
            Assert.IsFalse(statement.IsCompleted, "the statement must wait for the DDL lock");
        }

        await statement.WaitAsync(Deadline);
        Assert.IsFalse(database.Schema.Tables["items"].Columns!.Exists(c => c.Name == "extra"), "the column is staged only");
        Assert.IsNull(
            await TransactionalDdlLock.TryAcquireAsync(database, logger: null), "the transaction holds the DDL lock");

        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));
        Assert.AreEqual(1, await SweepAsync(), "an idle transaction is still reaped");
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);

        Assert.IsFalse(database.Schema.Tables["items"].Columns!.Exists(c => c.Name == "extra"));
        await using TransactionalDdlLock? free = await TransactionalDdlLock.TryAcquireAsync(database, logger: null);
        Assert.IsNotNull(free, "the reaped transaction must free the DDL lock");
    }

    [TestCaseSource(nameof(AutocommitEntryPoints))]
    public async Task AutocommitStatement_ThatRuns_IsNotReaped(string entryPoint)
    {
        await SetUpEngineAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task statement = Task.Run(() => RunAsync(entryPoint, tx: null));
        try
        {
            await gate.Reached;

            Assert.AreEqual(1, coordinator.ActiveCount, "the autocommit transaction is tracked while it runs");
            Assert.AreEqual(0, await SweepAsync(), "the transaction of a running request is not idle");
            Assert.AreEqual(1, coordinator.ActiveCount);
        }
        finally
        {
            gate.Open();
        }

        await statement.WaitAsync(Deadline);

        Assert.AreEqual(0, coordinator.ActiveCount, "the request finalized its own transaction");
    }

    /// <summary>The statement that a sweep met in the middle commits, and its row is stored.</summary>
    [Test]
    public async Task ExplicitTransaction_SweptDuringItsStatement_Commits()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task statement = Task.Run(() => RunAsync("grpc non-query", tx));
        await gate.Reached;
        Assert.AreEqual(0, await SweepAsync());
        gate.Open();
        await statement.WaitAsync(Deadline);

        await coordinator.CommitTrackedAsync(tx);

        CollectionAssert.AreEqual(new long[] { 11, 20, 30 }, await StoredAsync());
        Assert.AreEqual(0, coordinator.ActiveCount);
    }

    /// <summary>The autocommit statement that a sweep met in the middle commits, and its row is stored.</summary>
    [TestCase("rest non-query")]
    [TestCase("grpc non-query")]
    [TestCase("batch non-query")]
    [TestCase("rest update")]
    [TestCase("grpc update rows")]
    public async Task AutocommitStatement_SweptWhileItRuns_Commits(string entryPoint)
    {
        await SetUpEngineAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task statement = Task.Run(() => RunAsync(entryPoint, tx: null));
        await gate.Reached;
        Assert.AreEqual(0, await SweepAsync());
        gate.Open();
        await statement.WaitAsync(Deadline);

        CollectionAssert.AreEqual(new long[] { 11, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A client that sends two statements of one transaction at once: the transaction is idle only after
    /// the last one ends.
    /// </summary>
    [Test]
    public async Task TwoRunningStatements_TheTransactionIsIdleAfterTheLastOne()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();

        RunningStatement first = coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C);
        RunningStatement second = coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C);
        Assert.AreSame(tx, first.Transaction);

        first.Dispose();
        Assert.AreEqual(0, await SweepAsync());

        second.Dispose();
        Assert.AreEqual(1, await SweepAsync());
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>The idle time counts from the end of the last statement, not from its start.</summary>
    [Test]
    public async Task IdleTime_StartsWhenTheLastStatementEnds()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        TimeSpan timeout = TimeSpan.FromSeconds(1);

        using (coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C))
        {
            await Task.Delay(timeout + TimeSpan.FromMilliseconds(300));
            Assert.AreEqual(0, await coordinator.ReapIdleAsync(timeout), "the statement ran past the timeout");
        }

        Assert.AreEqual(0, await coordinator.ReapIdleAsync(timeout), "the idle time starts at the end of the statement");
        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);

        await Task.Delay(timeout + TimeSpan.FromMilliseconds(300));
        Assert.AreEqual(1, await coordinator.ReapIdleAsync(timeout));
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>
    /// An op that waits for an execution slot of its stream waits for the server, so its transaction is
    /// not idle, although its statement did not start.
    /// </summary>
    [Test]
    public async Task BatchOpThatWaitsForASlot_KeepsItsTransaction()
    {
        await SetUpEngineAsync(Options with { GrpcBatchMaxInFlight = 1 });
        await DdlAsync("CREATE TABLE slow (id INT64 PRIMARY KEY)");
        await WriteAsync("INSERT INTO slow (id) VALUES (1)");

        KvTransaction waiting = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "slow");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task stream = Task.Run(() => sql.BatchExecute(reader, writer, new TestServerCallContext()));
        try
        {
            reader.Push(BatchOp(1, BatchStatementKind.NonQuery, "UPDATE slow SET id = 2 WHERE id = 1", tx: null));
            await gate.Reached;

            reader.Push(BatchOp(2, BatchStatementKind.NonQuery, "UPDATE items SET v = 11 WHERE v = 10", waiting));
            await WaitUntilAsync(() => coordinator.RunningStatementsForTest(waiting) == 1);

            Assert.AreEqual(0, await SweepAsync());
            Assert.AreEqual(KvTransactionStatus.Active, waiting.Status);
        }
        finally
        {
            gate.Open();
        }

        BatchExecuteResponse reply = await writer.WaitFor(m => m.RequestId == 2).WaitAsync(Deadline);
        Assert.AreEqual(BatchExecuteResponse.PayloadOneofCase.NonQuery, reply.PayloadCase);
        reader.Complete();
        await stream.WaitAsync(Deadline);

        await coordinator.CommitTrackedAsync(waiting);
        CollectionAssert.AreEqual(new long[] { 11, 20, 30 }, await StoredAsync());
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using CancellationTokenSource deadline = new(Deadline);
        while (!condition())
            await Task.Delay(10, deadline.Token);
    }

    // ─── A slow client ────────────────────────────────────────────────────────

    /// <summary>A stream whose client does not read keeps its transaction, and its rows arrive later.</summary>
    [TestCase("rest stream")]
    [TestCase("rest stream, last flush")]
    [TestCase("grpc query")]
    [TestCase("grpc row query")]
    [TestCase("grpc query by id")]
    [TestCase("batch query")]
    public async Task RowStreamToASlowClient_IsNotCut(string entryPoint)
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        SlowStream stream = StartSlowStream(entryPoint, tx, CancellationToken.None);

        await stream.Parked;
        Assert.AreEqual(0, await SweepAsync(), "the stream still runs");
        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);

        stream.Release();
        await stream.Call.WaitAsync(Deadline);
        Assert.AreEqual(stream.ExpectedRows, stream.RowsReceived());

        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));
        await coordinator.CommitTrackedAsync(tx);
    }

    /// <summary>
    /// A client that disconnects in the middle of a row stream ends its statement through the request
    /// token. The transaction is rolled back with it, so nothing stays for the reaper.
    /// </summary>
    [TestCase("rest stream")]
    [TestCase("grpc query")]
    [TestCase("grpc row query")]
    [TestCase("grpc query by id")]
    [TestCase("batch query")]
    public async Task ClientThatDisconnectsInARowStream_EndsItsTransaction(string entryPoint)
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        using CancellationTokenSource disconnect = new();
        SlowStream stream = StartSlowStream(entryPoint, tx, disconnect.Token);

        await stream.Parked;
        Assert.AreEqual(0, await SweepAsync());

        disconnect.Cancel();
        try { await stream.Call.WaitAsync(Deadline); }
        catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>
    /// A client that disconnects after its statement produced every row, while the last bytes wait for
    /// it: the statement succeeded, so its transaction stays open, idle, for the reaper.
    /// </summary>
    [Test]
    public async Task ClientThatDisconnectsInTheLastFlush_LeavesATransactionTheReaperEnds()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        using CancellationTokenSource disconnect = new();
        SlowStream stream = StartSlowStream("rest stream, last flush", tx, disconnect.Token);

        await stream.Parked;
        Assert.AreEqual(0, await SweepAsync(), "the last flush belongs to the statement");

        disconnect.Cancel();
        await stream.Call.WaitAsync(Deadline);

        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));
        Assert.AreEqual(1, await SweepAsync());
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>One row stream to a client that does not read until <see cref="Release"/>.</summary>
    private sealed record SlowStream(Task Call, Task Parked, Action Release, int ExpectedRows, Func<int> RowsReceived);

    private SlowStream StartSlowStream(string entryPoint, KvTransaction tx, CancellationToken ct)
    {
        const string select = "SELECT v FROM items";

        if (entryPoint.StartsWith("rest stream"))
        {
            // The endpoint flushes every 128 rows, so the long result is parked in the middle of its
            // cursor and the short one at the flush of its trailer.
            bool lastFlush = entryPoint.EndsWith("last flush");
            string statement = lastFlush ? select : "SELECT 1 AS v FROM generate_series(1, 1000)";
            ParkedBody body = new();
            Task call = Task.Run(() => Sql(SqlBody(statement, tx), body, ct).ExecuteSQLQueryStream(), CancellationToken.None);
            return new SlowStream(call, body.Parked, body.Release, lastFlush ? 3 : 1000, () =>
            {
                string[] lines = Encoding.UTF8.GetString(body.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
                StringAssert.Contains("\"status\":\"ok\"", lines[^1]);
                return lines.Length - 2; // the schema line and the trailer are not rows
            });
        }

        if (entryPoint == "batch query")
        {
            // Rows large enough that a few of them fill the response queue of the stream, so the
            // statement itself waits for the client.
            const int rowCount = 24;
            GatedStreamWriter<BatchExecuteResponse> writer = new();
            writer.ParkNextWrite();

            BatchExecuteRequest op = BatchOp(1, BatchStatementKind.Query, $"SELECT @big AS big FROM generate_series(1, {rowCount})", tx);
            op.Request.Parameters["@big"] = new Value { StringValue = new string('x', 128 * 1024) };

            Task call = Task.Run(() => sql.BatchExecute(
                new FakeAsyncStreamReader<BatchExecuteRequest>([op]), writer, new TestServerCallContext(ct)), CancellationToken.None);
            return new SlowStream(call, writer.WriteIsParked.WaitAsync(Deadline), writer.Release, rowCount,
                () => writer.Written.Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row));
        }

        GatedStreamWriter<QueryStreamMessage> stream = new();
        stream.ParkNextWrite();
        TestServerCallContext context = new(ct);
        TxnHandle handle = HandleOf(tx);

        Task unary = entryPoint switch
        {
            "grpc query" => Task.Run(() => sql.ExecuteQuery(SqlMessage(select, tx), stream, context), CancellationToken.None),
            "grpc row query" => Task.Run(() => rows.Query(
                new RowQueryRequest { Database = db, Table = "items", TxnHandle = handle }, stream, context), CancellationToken.None),
            "grpc query by id" => Task.Run(() => rows.QueryById(
                new RowByIdRequest { Database = db, Table = "items", TxnHandle = handle, Id = RowId }, stream, context), CancellationToken.None),
            _ => throw new ArgumentOutOfRangeException(nameof(entryPoint), entryPoint, null),
        };

        return new SlowStream(unary, stream.WriteIsParked.WaitAsync(Deadline), stream.Release,
            entryPoint == "grpc query by id" ? 1 : 3,
            () => stream.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row));
    }

    // ─── A client that leaves ─────────────────────────────────────────────────

    /// <summary>
    /// A write does not observe the disconnect of its client. It ends on its own, and then the explicit
    /// transaction is idle, so the reaper takes it.
    /// </summary>
    [TestCase("rest")]
    [TestCase("grpc")]
    [TestCase("batch")]
    public async Task ClientThatDisconnectsInAWrite_LeavesATransactionTheReaperEnds(string transport)
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");
        using CancellationTokenSource disconnect = new();

        Task statement = Task.Run(() => DisconnectableWriteAsync(transport, tx, disconnect.Token), CancellationToken.None);
        await gate.Reached;
        disconnect.Cancel();
        Assert.AreEqual(0, await SweepAsync(), "the statement still runs");
        gate.Open();

        try { await statement.WaitAsync(Deadline); }
        catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

        Assert.AreEqual(1, await SweepAsync());
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// The finalize of an autocommit write does not observe the disconnect either: a commit or rollback
    /// that is cut leaves its locks to expire. The write of a client that left commits.
    /// </summary>
    [Test]
    public async Task ClientThatDisconnectsInAnAutocommitWrite_LeavesNothing(
        [Values("rest", "grpc", "batch")] string transport,
        [Values(CamusIsolationLevel.ReadCommitted, CamusIsolationLevel.Serializable)] CamusIsolationLevel level)
    {
        await SetUpEngineAsync(Options with { DefaultIsolationLevel = level });
        TableGate gate = await TableGate.InstallAsync(database, "items");
        using CancellationTokenSource disconnect = new();

        Task statement = Task.Run(() => DisconnectableWriteAsync(transport, tx: null, disconnect.Token), CancellationToken.None);
        await gate.Reached;
        disconnect.Cancel();
        gate.Open();

        try { await statement.WaitAsync(Deadline); }
        catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

        Assert.AreEqual(0, coordinator.ActiveCount);
        CollectionAssert.AreEqual(new long[] { 11, 20, 30 }, await StoredAsync());
    }

    private async Task DisconnectableWriteAsync(string transport, KvTransaction? tx, CancellationToken ct)
    {
        const string update = "UPDATE items SET v = 11 WHERE v = 10";

        switch (transport)
        {
            case "rest":
                await Sql(SqlBody(update, tx), aborted: ct).ExecuteNonSQLQuery();
                break;
            case "grpc":
                await sql.ExecuteNonQuery(SqlMessage(update, tx), new TestServerCallContext(ct));
                break;
            case "batch":
                await sql.BatchExecute(
                    new FakeAsyncStreamReader<BatchExecuteRequest>([BatchOp(1, BatchStatementKind.NonQuery, update, tx)]),
                    new CapturingStreamWriter<BatchExecuteResponse>(), new TestServerCallContext(ct));
                break;
        }
    }

    /// <summary>The typed and unary gRPC writes, which begin their own autocommit transaction.</summary>
    private static readonly string[] UnaryGrpcWrites =
    [
        "grpc non-query", "grpc query", "grpc insert row", "grpc update rows", "grpc update by id",
        "grpc delete rows", "grpc delete by id",
    ];

    /// <summary>
    /// A write whose call was cancelled before its transaction began does not start. Nothing was written,
    /// so this is the one point where a cancel of a write is safe.
    /// </summary>
    [Test]
    public async Task WriteOfACallCancelledBeforeItsBegin_DoesNotStart(
        [ValueSource(nameof(UnaryGrpcWrites))] string entryPoint,
        [Values(CamusIsolationLevel.ReadCommitted, CamusIsolationLevel.Serializable)] CamusIsolationLevel level)
    {
        await SetUpEngineAsync(Options with { DefaultIsolationLevel = level });

        // The service hands a cancel to the gRPC runtime as it is, and the runtime answers CANCELLED.
        Assert.CatchAsync<OperationCanceledException>(
            () => RunAsync(entryPoint, tx: null, new CancellationToken(canceled: true)));

        Assert.AreEqual(0, coordinator.ActiveCount);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A Serializable write that lost a conflict is replayed from its begin. For a client that left in the
    /// meantime there is no replay: the attempt is rolled back and the call ends as cancelled.
    /// </summary>
    [TestCase("grpc non-query")]
    [TestCase("grpc query")]
    [TestCase("grpc update rows")]
    public async Task SerializableWriteThatLostAConflict_IsNotRetriedForAClientThatLeft(string entryPoint)
    {
        await SetUpEngineAsync(Options with { DefaultIsolationLevel = CamusIsolationLevel.Serializable });

        KvTransaction holder = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(holder, db, "UPDATE items SET v = 99 WHERE v = 10", null));

        TableGate gate = await TableGate.InstallAsync(database, "items");
        using CancellationTokenSource disconnect = new();
        Task statement = Task.Run(() => RunAsync(entryPoint, tx: null, disconnect.Token), CancellationToken.None);
        try
        {
            await gate.Reached;
            disconnect.Cancel();
        }
        finally
        {
            gate.Open();
        }

        // A retry loop that goes on ends with the conflict instead, as an RpcException.
        Assert.CatchAsync<OperationCanceledException>(() => statement.WaitAsync(Deadline));
        Assert.AreEqual(0, coordinator.ActiveCount);

        await database.Transactions.RollbackAsync(holder);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>The same for REST: the client leaves after the server read its request.</summary>
    [Test]
    public async Task RestWriteOfAClientThatLeftBeforeTheBegin_DoesNotStart(
        [ValueSource(nameof(RestWrites))] string endpoint,
        [Values(CamusIsolationLevel.ReadCommitted, CamusIsolationLevel.Serializable)] CamusIsolationLevel level)
    {
        await SetUpEngineAsync(Options with { DefaultIsolationLevel = level });
        RecordingLogger log = new();
        using CancellationTokenSource client = new();

        Assert.AreEqual(StatusCodes.Status499ClientClosedRequest, await RestWriteAsync(endpoint, log, client, leaveAfterBody: true));
        Assert.IsTrue(client.IsCancellationRequested, "the client left when its body was read");

        AssertQuiet(log);
        Assert.AreEqual(0, coordinator.ActiveCount);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
        Assert.AreEqual(2, (await database.TableDescriptors["items"]).Schema.Columns!.Count, "the schema change did not start");
    }

    /// <summary>A REST request that was cancelled before the server read it is refused on every endpoint.</summary>
    [TestCaseSource(nameof(RestWrites))]
    public async Task RestWriteCancelledBeforeItsBody_DoesNotStart(string endpoint)
    {
        await SetUpEngineAsync();
        RecordingLogger log = new();
        using CancellationTokenSource client = new();
        client.Cancel();

        Assert.AreEqual(StatusCodes.Status499ClientClosedRequest, await RestWriteAsync(endpoint, log, client));

        AssertQuiet(log);
        Assert.AreEqual(0, coordinator.ActiveCount);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>A REST write that lost a conflict is not replayed for a client that left.</summary>
    [TestCase("non-query")]
    [TestCase("returning")]
    [TestCase("stream returning")]
    [TestCase("insert")]
    [TestCase("update")]
    [TestCase("delete")]
    public async Task SerializableRestWriteThatLostAConflict_IsNotRetriedForAClientThatLeft(string endpoint)
    {
        await SetUpEngineAsync(Options with { DefaultIsolationLevel = CamusIsolationLevel.Serializable });
        KvTransaction holder = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(holder, db, "UPDATE items SET v = 99 WHERE v = 10", null));

        TableGate gate = await TableGate.InstallAsync(database, "items");
        RecordingLogger log = new();
        using CancellationTokenSource client = new();
        Task<int> write = Task.Run(() => RestWriteAsync(endpoint, log, client));
        try
        {
            await gate.Reached;
            client.Cancel();
        }
        finally
        {
            gate.Open();
        }

        // A retry loop that goes on ends with the conflict instead.
        Assert.AreEqual(StatusCodes.Status499ClientClosedRequest, await write.WaitAsync(Deadline));
        AssertQuiet(log);
        Assert.AreEqual(0, coordinator.ActiveCount);

        await database.Transactions.RollbackAsync(holder);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A write with RETURNING observes the disconnect before it writes: it is rolled back, stores nothing
    /// and leaves no lock.
    /// </summary>
    [TestCase("returning")]
    [TestCase("stream returning")]
    public async Task RestReturningWriteOfAClientThatDisconnects_IsRolledBack(string endpoint)
    {
        await SetUpEngineAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");
        RecordingLogger log = new();
        using CancellationTokenSource client = new();
        Task<int> write = Task.Run(() => RestWriteAsync(endpoint, log, client));
        try
        {
            await gate.Reached;
            client.Cancel();
        }
        finally
        {
            gate.Open();
        }

        Assert.AreEqual(StatusCodes.Status499ClientClosedRequest, await write.WaitAsync(Deadline));
        AssertQuiet(log);
        Assert.AreEqual(0, coordinator.ActiveCount);

        await WriteAsync("UPDATE items SET v = 12 WHERE v = 10");
        CollectionAssert.AreEqual(new long[] { 12, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A cancel that the client did not cause is still a failure of the node: it keeps its status and its
    /// error log.
    /// </summary>
    [Test]
    public async Task CancelThatIsNotADisconnect_StaysAnInternalError()
    {
        await SetUpEngineAsync();
        RecordingLogger log = new();
        using CancellationTokenSource client = new();
        ControllerContext context = Context(new { databaseName = db, tableName = "items" }, aborted: client.Token);
        context.HttpContext.Request.Body = new SignallingBody([], () => throw new OperationCanceledException("not the client"));

        JsonResult result = await new InsertController(executor, coordinator, log, Options) { ControllerContext = context }.Insert();

        Assert.AreEqual(500, result.StatusCode);
        Assert.IsTrue(log.Entries.Any(e => e.Level == LogLevel.Error));
    }

    /// <summary>
    /// A transaction that a START opened on a stream ends with the stream, also when the client drops
    /// the stream while a statement of the transaction runs.
    /// </summary>
    [Test]
    public async Task ClientThatDropsItsStream_EndsTheTransactionsItStarted()
    {
        await SetUpEngineAsync();
        using CancellationTokenSource disconnect = new();
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task stream = Task.Run(() => sql.BatchExecute(reader, writer, new TestServerCallContext(disconnect.Token)), CancellationToken.None);

        reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = db } });
        TxnHandle handle = (await writer.WaitFor(m => m.RequestId == 1).WaitAsync(Deadline)).StartReply;
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        TableGate gate = await TableGate.InstallAsync(database, "items");
        reader.Push(BatchOp(2, BatchStatementKind.NonQuery, "UPDATE items SET v = 11 WHERE v = 10", tx));
        await gate.Reached;

        disconnect.Cancel();
        gate.Open();
        try { await stream.WaitAsync(Deadline); }
        catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// An autocommit transaction that its request failed to finalize has no other owner: its client never
    /// received the handle. The reaper takes it, and only then.
    /// </summary>
    [Test]
    public async Task AutocommitTransaction_IsTheReapersAfterItsFinalizeFailed()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await coordinator.StartAsync(db);

        Assert.AreEqual(0, await SweepAsync(), "the request still owns it");
        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);

        // A finalize that fails before it reaches an outcome gives the claim back.
        Assert.IsTrue(coordinator.TryClaimFinalizeForTest(tx));
        Assert.AreEqual(0, await SweepAsync(), "a finalize in flight keeps it");
        coordinator.ReleaseFinalizeForTest(tx);

        Assert.AreEqual(1, await SweepAsync());
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
    }

    // ─── Races ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Races the start of a statement against a sweep that would take its transaction. Exactly one of
    /// them wins: a statement that started keeps its transaction active until it ends, and a sweep that
    /// took the transaction refuses the statement.
    /// </summary>
    [Test]
    public async Task StatementStartRacedWithTheReap_ExactlyOneWins()
    {
        await SetUpEngineAsync();
        const int rounds = 400;
        int statementWon = 0, reaperWon = 0;
        Random random = new(20261124);

        for (int round = 0; round < rounds; round++)
        {
            KvTransaction tx = await BeginExplicitAsync();
            int statementSpin = random.Next(0, 3_000);
            int sweepSpin = random.Next(0, 3_000);
            using Barrier start = new(2);

            Task<bool> statement = Task.Run(async () =>
            {
                start.SignalAndWait();
                Thread.SpinWait(statementSpin);
                try
                {
                    using RunningStatement running = coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C);

                    // Long enough for the sweep to act on a transaction it should have left alone.
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.AreEqual(KvTransactionStatus.Active, tx.Status, "a running statement lost its transaction");
                        await Task.Yield();
                    }
                    return true;
                }
                catch (CamusDBException refused)
                {
                    Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput)
                        .Or.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted));
                    return false;
                }
            });

            Task<int> sweep = Task.Run(async () =>
            {
                start.SignalAndWait();
                Thread.SpinWait(sweepSpin);
                return await SweepAsync();
            });

            bool entered = await statement;
            int reaped = await sweep;

            if (!entered)
            {
                Assert.AreEqual(1, reaped, "only a reap can refuse the statement here");
                Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
                reaperWon++;
            }
            else
            {
                // The sweep may also have come after the statement ended; then it reaped an idle
                // transaction, which is correct.
                statementWon++;
                if (reaped == 0)
                {
                    Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
                    Assert.AreEqual(1, await SweepAsync());
                }
            }

            Assert.AreEqual(0, coordinator.ActiveCount);
        }

        TestContext.Out.WriteLine($"statement won {statementWon}, reaper won {reaperWon}");
    }

    /// <summary>A statement that arrives after the reap claimed its transaction is refused, not run.</summary>
    [Test]
    public async Task StatementAfterAReapClaim_IsRefused()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();

        Assert.IsTrue(coordinator.TryClaimFinalizeForTest(tx));
        CamusDBException refused = Assert.Throws<CamusDBException>(
            () => coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C).Dispose())!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, refused.Code);
        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));

        // The default value of a refused attempt holds nothing.
        coordinator.TryEnterStatement(tx.ClientId.L, tx.ClientId.C).Dispose();
        coordinator.TryEnterStatement(424242, 7).Dispose();
        Assert.AreEqual(0, coordinator.RunningStatementsForTest(tx));

        coordinator.ReleaseFinalizeForTest(tx);
        Assert.AreEqual(1, await SweepAsync());
    }

    /// <summary>
    /// A client can roll back while its statement still runs: the statement keeps the reaper away, and
    /// not the owner of the transaction.
    /// </summary>
    [Test]
    public async Task RunningStatement_DoesNotStopTheClientsOwnRollback()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();

        using (coordinator.EnterStatement(tx.ClientId.L, tx.ClientId.C))
        {
            Assert.IsTrue(await coordinator.RollbackByIdAsync(tx.ClientId.L, tx.ClientId.C));
            Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        }

        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.AreEqual(0, coordinator.OpenExplicitCount);
    }
}
