/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;
using CamusDB.Tests.Utils;

using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW TRANSACTIONS</c> and <c>KILL TRANSACTION</c> on one node, through the real entry points.
///
/// <para>A transaction is begun the way each transport begins one, so the list is checked for what the
/// host records at the begin. A statement is held in the middle at the open of its table
/// (<see cref="TableGate"/>), so a kill can meet a running write or read. The kill's wait is short
/// (<see cref="CamusDBOptions.ClusterActivityPeerTimeoutMs"/> / 2), so a kill that meets a held statement
/// reports that it is pending without slowing the tests.</para>
/// </summary>
[TestFixture]
// Serial: boots embedded Kahuna nodes and holds transactions and statements open across calls.
[NonParallelizable]
internal sealed class TestKillTransaction : SharedNodeBaseTest
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

    private const string RowId = "000000000000000000000001";

    private const string Update = "UPDATE items SET v = 11 WHERE v = 10";

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private string db = null!;
    private DatabaseDescriptor database = null!;
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CapturingLogger audit = null!;
    private PreparedStatementRegistry prepared = null!;
    private CamusSqlService sql = null!;
    private CamusDBOptions options = null!;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        // The kill waits half of this for its rollback.
        ClusterActivityPeerTimeoutMs = 600,
    };

    private async Task SetUpEngineAsync(CamusDBOptions? engineOptions = null)
    {
        options = engineOptions ?? Options;
        (db, database, executor) = await CreateDatabase(options);
        audit = new CapturingLogger();
        coordinator = new HttpTransactionCoordinator(executor, audit);
        prepared = new PreparedStatementRegistry(options);
        sql = new CamusSqlService(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), options);

        await DdlAsync("CREATE TABLE items (id oid PRIMARY KEY, v INT64)");
        await WriteAsync($"INSERT INTO items (id, v) VALUES ('{RowId}', 10), (gen_id(), 20), (gen_id(), 30)");
    }

    [TearDown]
    public void ClearOrigin() => StatementOrigin.Current = null;

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

    private async Task<List<IReadOnlyDictionary<string, ColumnValue>>> ShowAsync(string statement = "SHOW TRANSACTIONS")
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "", statement, null));
        return (await cursor.ToListAsync()).Select(r => r.Row).ToList();
    }

    private Task KillAsync(string transactionId) =>
        executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, "", $"KILL TRANSACTION '{transactionId}'", null));

    private async Task<CamusDBException> KillFailsAsync(string transactionId)
    {
        try
        {
            await KillAsync(transactionId);
        }
        catch (CamusDBException e)
        {
            return e;
        }

        Assert.Fail("The kill was expected to fail");
        return null!;
    }

    private static string Suffix(KvTransaction tx) => $"-t{tx.ClientId.L}.{tx.ClientId.C}";

    private async Task<IReadOnlyDictionary<string, ColumnValue>> RowOfAsync(KvTransaction tx) =>
        (await ShowAsync()).Single(r => r["transaction_id"].StrValue!.EndsWith(Suffix(tx), StringComparison.Ordinal));

    private async Task<string> IdOfAsync(KvTransaction tx) => (await RowOfAsync(tx))["transaction_id"].StrValue!;

    private Task<KvTransaction> BeginExplicitAsync() =>
        coordinator.StartAsync(db, isolationLevel: null, transactionMode: null, deferStart: true, sessionOwned: true);

    private static TxnHandle HandleOf(KvTransaction tx) =>
        new() { TxnIdPt = tx.ClientId.L, TxnIdCounter = (uint)tx.ClientId.C };

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (!condition())
        {
            if (watch.Elapsed > Deadline)
                Assert.Fail("The condition did not hold in time");

            await Task.Delay(10);
        }
    }

    // ─── Transports ───────────────────────────────────────────────────────────

    private static ControllerContext Context(object body, Stream? responseBody = null)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = responseBody ?? new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, prepared, logger, options) { ControllerContext = Context(body) };

    private TransactionsController Transactions(object body) =>
        new(executor, coordinator, logger, options) { ControllerContext = Context(body) };

    private object SqlBody(string statement, KvTransaction tx) => new
    {
        databaseName = db,
        sql = statement,
        txnIdPT = tx.ClientId.L,
        txnIdCounter = tx.ClientId.C,
    };

    private object HandleBody(KvTransaction tx) => new { databaseName = db, txnIdPT = tx.ClientId.L, txnIdCounter = tx.ClientId.C };

    /// <summary>A REST statement in <paramref name="tx"/>, answered with its error code, or null on success.</summary>
    private async Task<string?> RestNonQueryAsync(string statement, KvTransaction tx)
    {
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)(await Sql(SqlBody(statement, tx)).ExecuteNonSQLQuery()).Value!;
        return response.Status == "ok" ? null : response.Code;
    }

    private async Task<string?> GrpcNonQueryAsync(string statement, KvTransaction tx)
    {
        try
        {
            await sql.ExecuteNonQuery(new SqlRequest { Database = db, Sql = statement, TxnHandle = HandleOf(tx) }, new TestServerCallContext());
            return null;
        }
        catch (RpcException e)
        {
            return e.Trailers.GetValue("camus-error-code");
        }
    }

    /// <summary>A batch stream that stays open, so a transaction it begins stays open too.</summary>
    private sealed class OpenBatch(CamusSqlService service, string db) : IAsyncDisposable
    {
        private readonly ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        private readonly ObservingStreamWriter<BatchExecuteResponse> writer = new();
        private Task server = Task.CompletedTask;
        private int nextId;

        public void Open() => server = service.BatchExecute(reader, writer, new TestServerCallContext());

        public async Task<BatchExecuteResponse> SendAsync(BatchStatementKind kind, SqlRequest request)
        {
            int id = Interlocked.Increment(ref nextId);
            reader.Push(new BatchExecuteRequest { RequestId = id, Kind = kind, Request = request });
            return await writer.WaitFor(m => m.RequestId == id && m.PayloadCase is not BatchExecuteResponse.PayloadOneofCase.Schema
                and not BatchExecuteResponse.PayloadOneofCase.Row).WaitAsync(Deadline);
        }

        public async Task<TxnHandle> StartAsync() =>
            (await SendAsync(BatchStatementKind.Start, new SqlRequest { Database = db })).StartReply;

        public async ValueTask DisposeAsync()
        {
            reader.Complete();
            await server.WaitAsync(Deadline);
        }
    }

    /// <summary>
    /// Begins an explicit transaction the way <paramref name="transport"/> does, and runs one write in it
    /// through the same transport. The origin of a request is set as the host's middleware sets it.
    /// </summary>
    private async Task<(KvTransaction Tx, OpenBatch? Batch)> BeginAndWriteAsync(string transport)
    {
        OpenBatch? batch = null;
        KvTransaction tx;

        switch (transport)
        {
            case "rest":
            {
                StatementOrigin.Current = new StatementOrigin("http", null, "203.0.113.7");
                StartTransactionResponse started = (StartTransactionResponse)(await Transactions(new { databaseName = db }).StartTransaction()).Value!;
                tx = coordinator.GetState(started.TxnIdPT, started.TxnIdCounter);
                Assert.IsNull(await RestNonQueryAsync(Update, tx));
                break;
            }
            case "grpc":
            {
                StatementOrigin.Current = new StatementOrigin("grpc", null, "203.0.113.7");
                TxnHandle handle = await sql.StartTransaction(new StartTxnRequest { Database = db }, new TestServerCallContext());
                tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);
                Assert.IsNull(await GrpcNonQueryAsync(Update, tx));
                break;
            }
            case "batch":
            {
                StatementOrigin.Current = new StatementOrigin("grpc-stream", null, "203.0.113.7");
                batch = new OpenBatch(sql, db);
                batch.Open();
                TxnHandle handle = await batch.StartAsync();
                tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);
                BatchExecuteResponse written = await batch.SendAsync(BatchStatementKind.NonQuery,
                    new SqlRequest { Database = db, Sql = Update, TxnHandle = handle });
                Assert.That(written.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), written.Error?.Message);
                break;
            }
            case "embedded":
            {
                tx = await BeginExplicitAsync();
                using RunningStatement statement = coordinator.EnterStatement(tx.ClientId.L, (uint)tx.ClientId.C);
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, Update, null));
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(transport), transport, null);
        }

        return (tx, batch);
    }

    private static string TransportName(string transport) => transport switch
    {
        "rest" => "http",
        "grpc" => "grpc",
        "batch" => "grpc-stream",
        _ => StatementOrigin.EmbeddedTransport,
    };

    // ─── SHOW TRANSACTIONS ────────────────────────────────────────────────────

    [TestCase("rest")]
    [TestCase("grpc")]
    [TestCase("batch")]
    [TestCase("embedded")]
    public async Task IdleTransaction_IsListedWithItsOrigin(string transport)
    {
        await SetUpEngineAsync();
        (KvTransaction tx, OpenBatch? batch) = await BeginAndWriteAsync(transport);
        StatementOrigin.Current = null;

        try
        {
            IReadOnlyDictionary<string, ColumnValue> row = await RowOfAsync(tx);

            StringAssert.StartsWith(executor.QueryActivity.IdPrefix + "-t", row["transaction_id"].StrValue);
            Assert.AreEqual(db, row["database_name"].StrValue);
            Assert.AreEqual(TransportName(transport), row["transport"].StrValue);
            Assert.AreEqual(transport == "embedded" ? null : "203.0.113.7", row["client_address"].StrValue);
            Assert.AreEqual(TransactionActivityStates.Idle, row["state"].StrValue);
            Assert.AreEqual(ColumnType.Null, row["query_id"].Type, "no statement runs");
            Assert.AreEqual(Update, row["last_sql"].StrValue);
            Assert.That(row["modified_keys"].LongValue, Is.GreaterThanOrEqualTo(1), "the update wrote its row");
            Assert.IsFalse(row["holds_ddl_lock"].BoolValue);
            Assert.AreEqual(tx.IsolationLevel.ToString(), row["isolation"].StrValue);
            Assert.AreEqual(tx.Locking.ToString(), row["locking"].StrValue);
            Assert.AreEqual("ReadWrite", row["mode"].StrValue);
            Assert.That(row["age_ms"].FloatValue, Is.GreaterThanOrEqualTo(row["idle_ms"].FloatValue));
            Assert.AreEqual(ColumnType.Null, row["error"].Type);

            // The cluster form lists the same transaction. The shared test node's Raft view also names two
            // quorum witnesses, which answer no activity call and give one error row each.
            List<IReadOnlyDictionary<string, ColumnValue>> cluster = await ShowAsync("SHOW CLUSTER TRANSACTIONS");
            Assert.AreEqual(row["transaction_id"].StrValue, cluster.Single(r => r["error"].Type == ColumnType.Null)["transaction_id"].StrValue);
        }
        finally
        {
            await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
            if (batch is not null)
                await batch.DisposeAsync();
        }

        Assert.IsEmpty(await ShowAsync());
    }

    [TestCase("commit")]
    [TestCase("rollback")]
    [TestCase("reap")]
    public async Task Transaction_LeavesTheListWhenItEnds(string end)
    {
        await SetUpEngineAsync();
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");
        Assert.AreEqual(1, (await ShowAsync()).Count);

        switch (end)
        {
            case "commit":
                await coordinator.CommitTrackedAsync(tx);
                break;
            case "rollback":
                await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
                break;
            case "reap":
                Assert.AreEqual(1, await coordinator.ReapIdleAsync(TimeSpan.Zero));
                break;
        }

        Assert.IsEmpty(await ShowAsync());
    }

    [Test]
    public async Task AutocommitTransactions_AreNotListed()
    {
        await SetUpEngineAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task statement = Task.Run(async () =>
        {
            KvTransaction tx = await coordinator.StartAsync(db);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, Update, null));
            await coordinator.CommitAsync(database, tx);
        });

        try
        {
            await gate.Reached;
            Assert.AreEqual(1, coordinator.ActiveCount, "the autocommit transaction is tracked");
            Assert.IsEmpty(await ShowAsync(), "only explicit transactions are listed");
        }
        finally
        {
            gate.Open();
        }

        await statement.WaitAsync(Deadline);
    }

    [Test]
    public async Task RunningStatement_ShowsStateRunningAndItsQueryId()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task<string?> statement = Task.Run(() => RestNonQueryAsync(Update, tx));
        try
        {
            await gate.Reached;

            IReadOnlyDictionary<string, ColumnValue> row = await RowOfAsync(tx);
            Assert.AreEqual(TransactionActivityStates.Running, row["state"].StrValue);
            Assert.AreEqual(0.0, row["idle_ms"].FloatValue);

            string handle = $"{tx.ClientId.L}.{tx.ClientId.C}";
            string queryId = (await ShowAsync("SHOW QUERIES")).Single(q => q["transaction_id"].StrValue == handle)["query_id"].StrValue!;
            Assert.AreEqual(queryId, row["query_id"].StrValue);
        }
        finally
        {
            gate.Open();
        }

        Assert.IsNull(await statement.WaitAsync(Deadline));
        Assert.AreEqual(TransactionActivityStates.Idle, (await RowOfAsync(tx))["state"].StrValue);
        await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
    }

    [Test]
    public async Task LastSql_IsCutToTheConfiguredLength()
    {
        await SetUpEngineAsync(Options with { QueryActivityMaxSqlLength = 12 });
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");

        Assert.AreEqual(Update[..12], (await RowOfAsync(tx))["last_sql"].StrValue);
        await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
    }

    [Test]
    public async Task EngineWithNoHost_ListsNothingAndKillsNothing()
    {
        (_, _, CommandExecutor bare) = await CreateDatabase();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await bare.ExecuteSQLQuery(new ExecuteSQLTicket(null!, "", "SHOW TRANSACTIONS", null));
        Assert.IsEmpty(await cursor.ToListAsync());

        string id = bare.QueryActivity.IdPrefix + "-t1.1";
        CamusDBException? e = Assert.ThrowsAsync<CamusDBException>(() =>
            bare.ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, "", $"KILL TRANSACTION '{id}'", null)));
        Assert.AreEqual(CamusDBErrorCodes.TransactionNotFound, e!.Code);
    }

    [Test]
    public async Task StatementsNeedNoDatabaseAndTheWordsStayIdentifiers()
    {
        await SetUpEngineAsync();

        Assert.IsEmpty(await ShowAsync());
        Assert.IsEmpty((await ShowAsync("show cluster transactions")).Where(r => r["error"].Type == ColumnType.Null));

        await DdlAsync("CREATE TABLE transactions (id OID PRIMARY KEY, kill STRING NULL, cluster STRING NULL)");
        await WriteAsync("INSERT INTO transactions (id, kill) VALUES (gen_id(), 'x')");

        CamusDBException? wrongVerb = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, "", "STOP TRANSACTION 'x'", null)));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, wrongVerb!.Code);
    }

    // ─── KILL TRANSACTION ─────────────────────────────────────────────────────

    /// <summary>
    /// An idle transaction that holds a row write blocks a writer of that row. The kill rolls it back at
    /// once, and the same writer then succeeds without a wait.
    /// </summary>
    [Test]
    public async Task KillOfAnIdleTransaction_RollsItBack_AndABlockedWriterProceeds()
    {
        await SetUpEngineAsync();
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");
        string id = await IdOfAsync(tx);

        const string other = $"UPDATE items SET v = 99 WHERE id = '{RowId}'";
        Assert.ThrowsAsync<CamusDBException>(() => WriteAsync(other), "the open write blocks the row");

        await KillAsync(id);

        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.IsEmpty(await ShowAsync());

        Stopwatch watch = Stopwatch.StartNew();
        await WriteAsync(other);
        Assert.That(watch.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(400)), "the row is free at once");
        CollectionAssert.AreEqual(new long[] { 20, 30, 99 }, await StoredAsync(), "the killed write never landed");

        // One audit line names the kill.
        Assert.AreEqual(1, audit.Warnings.Count(m => m.Contains("KILL TRANSACTION") && m.Contains(tx.UniqueId)));
    }

    [TestCase("rest")]
    [TestCase("grpc")]
    [TestCase("batch")]
    public async Task KilledClient_GetsTheKillError_AndItsRollbackSucceeds(string transport)
    {
        await SetUpEngineAsync();
        (KvTransaction tx, OpenBatch? batch) = await BeginAndWriteAsync(transport);
        StatementOrigin.Current = null;
        await KillAsync(await IdOfAsync(tx));

        try
        {
            switch (transport)
            {
                case "rest":
                {
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, await RestNonQueryAsync(Update, tx));

                    CommitTransactionResponse commit = (CommitTransactionResponse)(await Transactions(HandleBody(tx)).CommitTransaction()).Value!;
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, commit.Code, commit.Message);

                    CommitTransactionResponse rollback = (CommitTransactionResponse)(await Transactions(HandleBody(tx)).RollbackTransaction()).Value!;
                    Assert.AreEqual("ok", rollback.Status, rollback.Message);
                    break;
                }
                case "grpc":
                {
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, await GrpcNonQueryAsync(Update, tx));

                    RpcException commit = Assert.ThrowsAsync<RpcException>(() =>
                        sql.CommitTransaction(HandleOf(tx), new TestServerCallContext()))!;
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, commit.Trailers.GetValue("camus-error-code"));
                    Assert.AreEqual(StatusCode.FailedPrecondition, commit.StatusCode);

                    await sql.RollbackTransaction(HandleOf(tx), new TestServerCallContext());
                    break;
                }
                case "batch":
                {
                    BatchExecuteResponse statement = await batch!.SendAsync(BatchStatementKind.NonQuery,
                        new SqlRequest { Database = db, Sql = Update, TxnHandle = HandleOf(tx) });
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, statement.Error.Code, statement.Error.Message);

                    BatchExecuteResponse commit = await batch.SendAsync(BatchStatementKind.Commit,
                        new SqlRequest { Database = db, TxnHandle = HandleOf(tx) });
                    Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, commit.Error.Code, commit.Error.Message);

                    BatchExecuteResponse rollback = await batch.SendAsync(BatchStatementKind.Rollback,
                        new SqlRequest { Database = db, TxnHandle = HandleOf(tx) });
                    Assert.AreNotEqual(BatchExecuteResponse.PayloadOneofCase.Error, rollback.PayloadCase, rollback.Error?.Message);
                    break;
                }
            }
        }
        finally
        {
            if (batch is not null)
                await batch.DisposeAsync();
        }

        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A kill that meets a running write fences the transaction at once and reports that it is pending.
    /// The write stops at its next write with the kill's error, and the rollback runs when it ends.
    /// </summary>
    [Test]
    public async Task KillOfARunningWrite_FencesAtOnce_AndRollsBackWhenTheWriteEnds()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task<string?> write = Task.Run(() => RestNonQueryAsync(Update, tx));
        string id;
        try
        {
            await gate.Reached;
            id = await IdOfAsync(tx);

            CamusDBException pending = await KillFailsAsync(id);
            Assert.AreEqual(CamusDBErrorCodes.TransactionKillPending, pending.Code, pending.Message);

            Assert.AreEqual(TransactionActivityStates.Killing, (await RowOfAsync(tx))["state"].StrValue);
            Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, await RestNonQueryAsync("SELECT 1", tx), "a new statement is refused at once");

            CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => coordinator.CommitTrackedAsync(tx))!;
            Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, commit.Code, "the kill owns the finalize");
            Assert.AreEqual(0, await coordinator.ReapIdleAsync(TimeSpan.Zero), "the reaper leaves a killed transaction to its kill");

            Assert.AreEqual(KvTransactionStatus.Active, tx.Status, "no rollback beside the running write");
        }
        finally
        {
            gate.Open();
        }

        Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, await write.WaitAsync(Deadline), "the write stops with the kill's error");

        await WaitUntilAsync(() => coordinator.ActiveCount == 0);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);

        // The same kill now reports the rollback.
        await KillAsync(id);
        CollectionAssert.AreEqual(new long[] { 10, 20, 30 }, await StoredAsync());

        Assert.AreEqual(1, audit.Warnings.Count(m => m.Contains("KILL TRANSACTION") && m.Contains("waits for its running statement")));
    }

    [Test]
    public async Task KillThatWaits_ReportsKilled_WhenTheStatementEndsInTime()
    {
        await SetUpEngineAsync(Options with { ClusterActivityPeerTimeoutMs = 20_000 });
        KvTransaction tx = await BeginExplicitAsync();
        TableGate gate = await TableGate.InstallAsync(database, "items");

        Task<string?> write = Task.Run(() => RestNonQueryAsync(Update, tx));
        await gate.Reached;
        string id = await IdOfAsync(tx);

        Task kill = Task.Run(() => KillAsync(id));
        await WaitUntilAsync(() => tx.KillReason is not null);
        gate.Open();

        await kill.WaitAsync(Deadline);
        Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, await write.WaitAsync(Deadline));
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
    }

    /// <summary>A read the kill meets stops at its next row with the kill's error, not with the cancel error.</summary>
    [Test]
    public async Task KillOfARunningRead_StopsItWithTheKillError()
    {
        await SetUpEngineAsync();
        KvTransaction tx = await BeginExplicitAsync();

        RunningStatement statement = coordinator.EnterStatement(tx.ClientId.L, (uint)tx.ClientId.C);
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db, "SELECT v FROM items", null));
        IAsyncEnumerator<QueryResultRow> cursor = rows.GetAsyncEnumerator();
        Assert.IsTrue(await cursor.MoveNextAsync());

        string id = await IdOfAsync(tx);
        Assert.AreEqual(CamusDBErrorCodes.TransactionKillPending, (await KillFailsAsync(id)).Code);

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync())!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, stopped.Code, stopped.Message);

        await cursor.DisposeAsync();
        statement.Dispose();

        await WaitUntilAsync(() => coordinator.ActiveCount == 0);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    /// <summary>A commit that holds the finalize claim wins; the kill reports it and fences nothing.</summary>
    [Test]
    public async Task KillWhileACommitHoldsTheClaim_IsRefused_AndFencesNothing()
    {
        await SetUpEngineAsync();
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");
        string id = await IdOfAsync(tx);

        Assert.IsTrue(coordinator.TryClaimFinalizeForTest(tx));
        CamusDBException refused = await KillFailsAsync(id);
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, refused.Code, refused.Message);
        Assert.IsNull(tx.KillReason);
        coordinator.ReleaseFinalizeForTest(tx);

        await coordinator.CommitTrackedAsync(tx);
        CollectionAssert.AreEqual(new long[] { 11, 20, 30 }, await StoredAsync());
    }

    /// <summary>
    /// A kill and a commit race for the same transaction, many times. Exactly one wins each round, the
    /// loser reports the winner, and the stored row agrees with the winner.
    /// </summary>
    [Test]
    public async Task KillRacingACommit_ExactlyOneWins()
    {
        await SetUpEngineAsync();
        int commits = 0, kills = 0;

        for (int round = 0; round < 40; round++)
        {
            long value = 1000 + round;
            List<long> before = await StoredAsync();

            KvTransaction tx = await BeginExplicitAsync();
            using (coordinator.EnterStatement(tx.ClientId.L, (uint)tx.ClientId.C))
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"UPDATE items SET v = {value} WHERE id = '{RowId}'", null));

            string id = await IdOfAsync(tx);

            Task<CamusDBException?> commit = Task.Run(async () =>
            {
                try { await coordinator.CommitTrackedAsync(tx); return null; }
                catch (CamusDBException e) { return e; }
            });
            Task<CamusDBException?> kill = Task.Run(async () =>
            {
                try { await KillAsync(id); return null; }
                catch (CamusDBException e) { return e; }
            });

            CamusDBException? commitError = await commit.WaitAsync(Deadline);
            CamusDBException? killError = await kill.WaitAsync(Deadline);
            List<long> stored = await StoredAsync();

            if (commitError is null)
            {
                commits++;
                Assert.IsNotNull(killError, $"round {round}: both won");
                Assert.That(killError!.Code, Is.AnyOf(CamusDBErrorCodes.TransactionAlreadyCompleted, CamusDBErrorCodes.TransactionNotFound), killError.Message);
                if (killError.Code == CamusDBErrorCodes.TransactionAlreadyCompleted)
                    StringAssert.Contains("committed", killError.Message);
                CollectionAssert.Contains(stored, value);
            }
            else
            {
                kills++;
                Assert.IsNull(killError, $"round {round}: neither won ({commitError.Code}: {commitError.Message})");
                Assert.AreEqual(CamusDBErrorCodes.TransactionKilled, commitError.Code, commitError.Message);
                CollectionAssert.AreEqual(before, stored);
            }

            Assert.AreEqual(0, coordinator.ActiveCount, $"round {round}");
        }

        TestContext.Out.WriteLine($"commit won {commits} rounds, kill won {kills}");
    }

    /// <summary>
    /// A transaction whose DDL holds its database's DDL lock (transactional DDL) blocks every other DDL
    /// of the database. The kill discards its change set and frees the lock.
    /// </summary>
    [Test]
    public async Task KillOfATransactionWithPendingDdl_DiscardsItAndFreesTheDdlLock()
    {
        await SetUpEngineAsync(Options with { TransactionalDdlEnabled = true });
        KvTransaction tx = await BeginExplicitAsync();

        ExecuteDDLSQLResponse ddl = (ExecuteDDLSQLResponse)(await Sql(SqlBody("ALTER TABLE items ADD COLUMN extra INT64", tx)).ExecuteSQLDDL()).Value!;
        Assert.AreEqual("ok", ddl.Status, ddl.Message);
        Assert.IsTrue((await RowOfAsync(tx))["holds_ddl_lock"].BoolValue);

        Task other = Task.Run(() => DdlAsync("CREATE TABLE other (id OID PRIMARY KEY)"));
        await Task.Delay(200);
        Assert.IsFalse(other.IsCompleted, "the other DDL waits for the DDL lock");

        await KillAsync(await IdOfAsync(tx));

        await other.WaitAsync(Deadline);
        Assert.AreEqual(0, coordinator.ActiveCount);

        // The change set was discarded: the column does not exist.
        KvTransaction read = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException noColumn = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(read, db, "SELECT extra FROM items", null));
                await cursor.ToListAsync();
            })!;
            StringAssert.Contains("extra", noColumn.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(read);
        }
    }

    /// <summary>
    /// An id of another process (a node restarted since the id was copied) carries another instance tag,
    /// so it can never name a transaction of this process, even when the handle matches.
    /// </summary>
    [Test]
    public async Task IdOfAnotherProcess_OrMalformed_KillsNothing()
    {
        await SetUpEngineAsync();
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");
        string id = await IdOfAsync(tx);

        char[] chars = id.ToCharArray();
        chars[6] = chars[6] == '0' ? '1' : '0';
        string otherProcess = new(chars);

        foreach (string wrong in new[] { otherProcess, id + "x", id.Replace("-t", "-"), "nonsense", "" })
        {
            CamusDBException e = await KillFailsAsync(wrong);
            Assert.AreEqual(CamusDBErrorCodes.TransactionNotFound, e.Code, wrong);
        }

        Assert.AreEqual(KvTransactionStatus.Active, tx.Status);
        Assert.IsNull(tx.KillReason);
        await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
    }

    [Test]
    public async Task SecondKill_OfAKilledTransaction_Succeeds()
    {
        await SetUpEngineAsync();
        (KvTransaction tx, _) = await BeginAndWriteAsync("embedded");
        string id = await IdOfAsync(tx);

        await KillAsync(id);
        await KillAsync(id);

        Assert.AreEqual(1, audit.Warnings.Count(m => m.Contains("KILL TRANSACTION")), "one audit line per kill that ended a transaction");
    }

    // ─── A transaction whose coordinator session is unknown ────────────────────

    /// <summary>
    /// A transaction id minted from the node's clock and never started as a session: the identity of a
    /// transaction whose coordinator session is gone, from the participants' point of view.
    /// </summary>
    private HLCTimestamp MintOrphanTransactionId() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId());

    /// <summary>Plants the session-owned exclusive lock and staged value that the engine's write path leaves.</summary>
    private async Task PlantSessionOwnedHoldingAsync(HLCTimestamp txId, string key)
    {
        (KeyValueResponseType locked, _, _, _) = await SharedKahuna.LocateAndTryAcquireExclusiveLock(
            txId, key, expiresMs: 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Locked, locked);

        (KeyValueResponseType set, _, _) = await SharedKahuna.LocateAndTrySetKeyValue(
            txId, key, value: [7, 7, 7], compareValue: null, compareRevision: -1,
            KeyValueFlags.None, expiresMs: 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, set);
    }

    /// <summary>What a foreign transaction finds when it tries to take the key. A probe that gets it lets it go.</summary>
    private async Task<KeyValueResponseType> ProbeHoldingAsync(string key)
    {
        HLCTimestamp probeId = MintOrphanTransactionId();
        (KeyValueResponseType type, _, _, _) = await SharedKahuna.LocateAndTryAcquireExclusiveLock(
            probeId, key, expiresMs: 5_000, KeyValueDurability.Persistent, CancellationToken.None);

        if (type == KeyValueResponseType.Locked)
            await SharedKahuna.LocateAndTryReleaseExclusiveLock(probeId, key, KeyValueDurability.Persistent, CancellationToken.None);

        return type;
    }

    /// <summary>
    /// Tracks an explicit transaction whose coordinator session does not exist and that holds
    /// <paramref name="key"/>: what the kill meets when the session expired or its node lost it.
    /// </summary>
    private async Task<KvTransaction> TrackOrphanAsync(string key)
    {
        HLCTimestamp orphanId = MintOrphanTransactionId();
        await PlantSessionOwnedHoldingAsync(orphanId, key);

        KvTransaction orphan = new(orphanId, Guid.NewGuid().ToString("n"));
        orphan.TrackModified(key, KeyValueDurability.Persistent);
        orphan.Activity = new TransactionActivity(db, origin: null, principal: null);
        await coordinator.RegisterForTestAsync(database.Transactions, orphan, isExplicit: true);
        return orphan;
    }

    /// <summary>
    /// The kill of a transaction whose coordinator session is unknown, old enough that no session can
    /// still own its holdings: the rollback releases them from the transaction's own key mirror.
    /// </summary>
    [Test]
    public async Task KillOfATransactionWithAnUnknownSession_ReleasesItsMirroredHoldings()
    {
        await SetUpEngineAsync(Options with { AbandonedTransactionReleaseAfterMs = 20 });
        string key = "killorphan/" + Guid.NewGuid().ToString("n");
        KvTransaction orphan = await TrackOrphanAsync(key);
        Assert.AreEqual(KeyValueResponseType.AlreadyLocked, await ProbeHoldingAsync(key), "the holding blocks another transaction");

        await Task.Delay(60);
        await KillAsync(await IdOfAsync(orphan));

        Assert.AreEqual(KvTransactionStatus.RolledBack, orphan.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.AreEqual(KeyValueResponseType.Locked, await ProbeHoldingAsync(key), "the kill released the key");
    }

    /// <summary>
    /// The same kill while a session could still own the holdings: the kill ends the transaction, and the
    /// holdings are released later, by the reaper's sweep, as for a reaped transaction.
    /// </summary>
    [Test]
    public async Task KillOfATransactionWithAnUnknownSession_LeavesYoungHoldingsToTheSweep()
    {
        const int releaseAgeMs = 250;
        await SetUpEngineAsync(Options with { AbandonedTransactionReleaseAfterMs = releaseAgeMs });
        string key = "killorphan/" + Guid.NewGuid().ToString("n");
        KvTransaction orphan = await TrackOrphanAsync(key);

        await KillAsync(await IdOfAsync(orphan));

        Assert.AreEqual(KvTransactionStatus.RolledBack, orphan.Status);
        Assert.AreEqual(0, coordinator.ActiveCount);
        Assert.AreEqual(KeyValueResponseType.AlreadyLocked, await ProbeHoldingAsync(key), "nothing is released while a session could own it");

        await Task.Delay(releaseAgeMs + 60);
        Assert.AreEqual(1, await coordinator.ReleaseDueMirroredHoldingsAsync());
        Assert.AreEqual(KeyValueResponseType.Locked, await ProbeHoldingAsync(key));
    }

    /// <summary>Test-only: a logger that keeps the Warning lines it receives.</summary>
    private sealed class CapturingLogger : ILogger<HttpTransactionCoordinator>
    {
        public ConcurrentQueue<string> Warnings { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Enqueue(formatter(state, exception));
        }
    }
}

/// <summary>
/// The privilege rules of <c>SHOW TRANSACTIONS</c> and <c>KILL TRANSACTION</c> with authentication on:
/// each user sees and kills only their own transactions, a superuser all of them, and an id of another
/// user's transaction answers as an unknown id.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestKillTransactionPrivileges : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> ShowAsync(CommandExecutor executor, Principal principal)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, database: "", sql: "SHOW TRANSACTIONS", parameters: null, principal: principal));
        return (await cursor.ToListAsync()).Select(r => r.Row).ToList();
    }

    private static Task KillAsync(CommandExecutor executor, string id, Principal principal)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            null!, database: "", sql: $"KILL TRANSACTION '{id}'", parameters: null, principal: principal));

    /// <summary>
    /// Creates the test database under a name that starts with a letter, so a GRANT can name it as a bare
    /// identifier, and tracks it for cleanup.
    /// </summary>
    private async Task<(string Db, CommandExecutor Executor)> CreateNamedDatabaseAsync()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "k" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));
        TrackDatabase(db, ex);
        return (db, ex);
    }

    /// <summary>Begins an explicit transaction as a REST request of <paramref name="principal"/> does.</summary>
    private static async Task<KvTransaction> BeginAsAsync(HttpTransactionCoordinator coordinator, string db, Principal? principal)
    {
        AuthorizationContext.Current = new AuthorizationScope(principal, null);
        try
        {
            return await coordinator.StartAsync(db, isolationLevel: null, transactionMode: null, deferStart: true, sessionOwned: true);
        }
        finally
        {
            AuthorizationContext.Current = default;
        }
    }

    [Test]
    public async Task UsersSeeAndKillOnlyTheirOwnTransactions()
    {
        (string db, CommandExecutor ex) = await CreateNamedDatabaseAsync();
        HttpTransactionCoordinator coordinator = new(ex);
        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        foreach (string user in new[] { "alice", "bob" })
        {
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(null!, "", $"CREATE USER {user} IDENTIFIED BY '{user}-password-1'", null, root));
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(null!, "", $"GRANT SELECT ON {db}.* TO {user}", null, root));
        }

        Principal alice = await Login(ex, "alice", "alice-password-1");
        Principal bob = await Login(ex, "bob", "bob-password-1");

        KvTransaction tx = await BeginAsAsync(coordinator, db, alice);

        Assert.That(await ShowAsync(ex, bob), Is.Empty);
        string id = (await ShowAsync(ex, root)).Single()["transaction_id"].StrValue!;
        Assert.That((await ShowAsync(ex, alice)).Single()["user_name"].StrValue, Is.EqualTo("alice"));

        // Bob cannot kill it, and is not told it exists.
        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() => KillAsync(ex, id, bob));
        Assert.That(refused!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionNotFound));
        Assert.That(tx.KillReason, Is.Null);

        // Alice can, and the reason names her.
        await KillAsync(ex, id, alice);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
        Assert.That(tx.KillReason, Does.Contain("user 'alice'"));

        // Bob still gets "not found" for the id of a transaction that was killed.
        refused = Assert.ThrowsAsync<CamusDBException>(() => KillAsync(ex, id, bob));
        Assert.That(refused!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionNotFound));

        // A superuser can kill any user's transaction.
        KvTransaction second = await BeginAsAsync(coordinator, db, alice);
        await KillAsync(ex, (await ShowAsync(ex, root)).Single()["transaction_id"].StrValue!, root);
        Assert.That(second.Status, Is.EqualTo(KvTransactionStatus.RolledBack));
    }

    /// <summary>
    /// A transport that begins a transaction without resolving its caller leaves the owner to the first
    /// statement. Until then only a superuser sees the transaction.
    /// </summary>
    [Test]
    public async Task OwnerUnknownAtTheBegin_IsSetByTheFirstStatement()
    {
        (string db, CommandExecutor ex) = await CreateNamedDatabaseAsync();
        HttpTransactionCoordinator coordinator = new(ex);
        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(null!, "", "CREATE USER alice IDENTIFIED BY 'alice-password-1'", null, root));
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(null!, "", $"GRANT SELECT ON {db}.* TO alice", null, root));
        Principal alice = await Login(ex, "alice", "alice-password-1");

        KvTransaction tx = await BeginAsAsync(coordinator, db, principal: null);

        Assert.That(await ShowAsync(ex, alice), Is.Empty);
        Assert.That((await ShowAsync(ex, root)).Single()["user_name"].Type, Is.EqualTo(ColumnType.Null));

        using (coordinator.EnterStatement(tx.ClientId.L, (uint)tx.ClientId.C))
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SELECT 1", null, alice));
            await cursor.ToListAsync();
        }

        IReadOnlyDictionary<string, ColumnValue> row = (await ShowAsync(ex, alice)).Single();
        Assert.That(row["user_name"].StrValue, Is.EqualTo("alice"));
        Assert.That(row["last_sql"].StrValue, Is.EqualTo("SELECT 1"));

        await coordinator.RollbackByIdAsync(tx.ClientId.L, (uint)tx.ClientId.C);
    }
}
