
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

using Kommander.Time;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// The Kahuna commit timestamp of an explicit transaction, from <see cref="KvTransaction.CommitTimestamp"/>
/// to the unary gRPC commit, the batched COMMIT and COMMIT_IF_OK, and the REST commit. Each reply must
/// carry the value the transaction recorded, with all three HLC components. A transaction that wrote
/// nothing, and a rolled-back one, must report no commit timestamp. The causal token must stay at or
/// above the commit timestamp, so a read at the token sees the commit.
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCommitTimestamp : BaseTest
{
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    private CamusSqlService service = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpService()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        serviceExecutor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(serviceExecutor);
        service = new(serviceExecutor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    private static TestServerCallContext Ctx() => new();

    private async Task<string> CreateDatabaseAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {db}" }, Ctx());
        TrackDatabase(db, serviceExecutor);
        await service.ExecuteDdl(
            new SqlRequest { Database = db, Sql = "CREATE TABLE items (id INT64 PRIMARY KEY NOT NULL, name STRING)" },
            Ctx());
        await service.ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "INSERT INTO items (id, name) VALUES (1, 'seed')" }, Ctx());
        return db;
    }

    private Task<TxnHandle> StartAsync(string db) =>
        service.StartTransaction(new StartTxnRequest { Database = db }, Ctx());

    private Task<NonQueryReply> NonQueryAsync(string db, TxnHandle handle, string sql) =>
        service.ExecuteNonQuery(new SqlRequest { Database = db, Sql = sql, TxnHandle = handle }, Ctx());

    private static HLCTimestamp CommitTimestampOf(CommitReply reply) =>
        new(reply.CommitTimestampN, reply.CommitTimestampL, (uint)reply.CommitTimestampC);

    private static HLCTimestamp CausalTokenOf(CommitReply reply) =>
        new(reply.CausalTokenN, reply.CausalTokenL, (uint)reply.CausalTokenC);

    /// <summary>The reply carries what the transaction recorded, above its id and at or below the causal token.</summary>
    private static void AssertCarriesCommitTimestamp(CommitReply reply, KvTransaction tx)
    {
        HLCTimestamp commitTimestamp = CommitTimestampOf(reply);

        Assert.That(tx.CommitTimestamp.IsNull(), Is.False, "a transaction that wrote records its commit timestamp");
        Assert.That(commitTimestamp, Is.EqualTo(tx.CommitTimestamp), "the reply carries all three components of the recorded value");
        Assert.That(commitTimestamp, Is.GreaterThan(tx.TransactionId), "Kahuna mints the commit timestamp above the transaction id");
        Assert.That(CausalTokenOf(reply), Is.GreaterThanOrEqualTo(commitTimestamp), "the causal token is not below the commit timestamp");
    }

    [Test]
    public async Task UnaryCommit_OfAWrite_CarriesTheRecordedCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();

        TxnHandle handle = await StartAsync(db);
        await NonQueryAsync(db, handle, "INSERT INTO items (id, name) VALUES (2, 'two')");
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        CommitReply reply = await service.CommitTransaction(handle, Ctx());

        AssertCarriesCommitTimestamp(reply, tx);
    }

    [Test]
    public async Task UnaryCommit_OfAReadOnlyTransaction_CarriesNoCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();

        TxnHandle handle = await StartAsync(db);
        CapturingStreamWriter<QueryStreamMessage> rows = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT name FROM items", TxnHandle = handle }, rows, Ctx());
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        CommitReply reply = await service.CommitTransaction(handle, Ctx());

        Assert.That(tx.CommitTimestamp.IsNull(), Is.True, "a transaction that wrote nothing has no commit timestamp");
        Assert.That(CommitTimestampOf(reply).IsNull(), Is.True);
    }

    [Test]
    public async Task UnaryCommit_OfAnEmptyTransaction_CarriesNoCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();

        TxnHandle handle = await StartAsync(db);
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        CommitReply reply = await service.CommitTransaction(handle, Ctx());

        Assert.That(tx.CommitTimestamp.IsNull(), Is.True, "BEGIN then COMMIT never reaches Kahuna");
        Assert.That(CommitTimestampOf(reply).IsNull(), Is.True);
    }

    [Test]
    public async Task Rollback_LeavesNoCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();

        TxnHandle handle = await StartAsync(db);
        await NonQueryAsync(db, handle, "INSERT INTO items (id, name) VALUES (2, 'two')");
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        await service.RollbackTransaction(handle, Ctx());

        Assert.That(tx.CommitTimestamp.IsNull(), Is.True);
    }

    [Test]
    public async Task TwoCommits_OfTheSameRow_HaveIncreasingCommitTimestamps()
    {
        string db = await CreateDatabaseAsync();
        HLCTimestamp previous = HLCTimestamp.Zero;

        for (int i = 0; i < 3; i++)
        {
            TxnHandle handle = await StartAsync(db);
            await NonQueryAsync(db, handle, $"UPDATE items SET name = 'v{i}' WHERE id = 1");
            KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

            CommitReply reply = await service.CommitTransaction(handle, Ctx());

            AssertCarriesCommitTimestamp(reply, tx);
            Assert.That(tx.CommitTimestamp, Is.GreaterThan(previous), "a later write of the same row commits at a later timestamp");
            previous = tx.CommitTimestamp;
        }
    }

    [TestCase(BatchStatementKind.Commit)]
    [TestCase(BatchStatementKind.CommitIfOk)]
    public async Task BatchedCommit_OfAWrite_CarriesTheRecordedCommitTimestamp(BatchStatementKind commitKind)
    {
        string db = await CreateDatabaseAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = db } });
        BatchExecuteResponse started = await writer.WaitFor(m =>
            m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply);
        TxnHandle handle = started.StartReply;

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 2,
            Kind = BatchStatementKind.NonQuery,
            Request = new SqlRequest { Database = db, Sql = "INSERT INTO items (id, name) VALUES (2, 'two')", TxnHandle = handle },
        });
        await writer.WaitFor(m => m.RequestId == 2 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.NonQuery);
        KvTransaction tx = coordinator.GetState(handle.TxnIdPt, handle.TxnIdCounter);

        reader.Push(new BatchExecuteRequest { RequestId = 3, Kind = commitKind, Request = new SqlRequest { Database = db, TxnHandle = handle } });
        BatchExecuteResponse committed = await writer.WaitFor(m => m.RequestId == 3 && IsTerminal(m));
        reader.Complete();
        await server;

        Assert.That(committed.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.CommitReply),
            committed.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Error ? committed.Error.Message : null);
        AssertCarriesCommitTimestamp(committed.CommitReply, tx);
    }

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.CommitReply or
        BatchExecuteResponse.PayloadOneofCase.Error;

    [Test]
    public async Task RestCommit_OfAWrite_AnswersWithTheCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();
        PreparedStatementRegistry statements = new(Options);

        TransactionsController Transactions(string json) =>
            new(serviceExecutor, coordinator, logger, Options) { ControllerContext = Context(json) };

        JsonResult startResult = await Transactions($$"""{"databaseName":"{{db}}"}""").StartTransaction();
        StartTransactionResponse started = (StartTransactionResponse)startResult.Value!;
        Assert.That(started.Status, Is.EqualTo("ok"), started.Message);

        ExecuteSQLController sql = new(serviceExecutor, coordinator, statements, logger, Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(new
            {
                databaseName = db,
                txnIdPT = started.TxnIdPT,
                txnIdCounter = started.TxnIdCounter,
                sql = "INSERT INTO items (id, name) VALUES (2, 'two')",
            })),
        };
        ExecuteNonSQLQueryResponse written = (ExecuteNonSQLQueryResponse)(await sql.ExecuteNonSQLQuery()).Value!;
        Assert.That(written.Status, Is.EqualTo("ok"), written.Message);
        KvTransaction tx = coordinator.GetState(started.TxnIdPT, started.TxnIdCounter);

        JsonResult commitResult = await Transactions(JsonSerializer.Serialize(new
        {
            databaseName = db,
            txnIdPT = started.TxnIdPT,
            txnIdCounter = started.TxnIdCounter,
        }, WebJson)).CommitTransaction();
        CommitTransactionResponse committed = (CommitTransactionResponse)commitResult.Value!;

        Assert.That(committed.Status, Is.EqualTo("ok"), committed.Message);
        Assert.That(committed.CommitTimestamp, Is.Not.Null);
        Assert.That(committed.CommitTimestamp!.Value, Is.EqualTo(tx.CommitTimestamp));
        Assert.That(committed.CausalToken!.Value, Is.GreaterThanOrEqualTo(committed.CommitTimestamp.Value));

        // A client reads the value from JSON, so all three components must be in the body.
        using JsonDocument body = JsonDocument.Parse(JsonSerializer.Serialize(committed, WebJson));
        JsonElement value = body.RootElement.GetProperty("commitTimestamp");
        Assert.That(value.GetProperty("n").GetInt32(), Is.EqualTo(tx.CommitTimestamp.N));
        Assert.That(value.GetProperty("l").GetInt64(), Is.EqualTo(tx.CommitTimestamp.L));
        Assert.That(value.GetProperty("c").GetUInt32(), Is.EqualTo(tx.CommitTimestamp.C));
    }

    [Test]
    public async Task RestCommit_OfAnEmptyTransaction_AnswersWithNoCommitTimestamp()
    {
        string db = await CreateDatabaseAsync();

        TransactionsController Transactions(string json) =>
            new(serviceExecutor, coordinator, logger, Options) { ControllerContext = Context(json) };

        JsonResult startResult = await Transactions($$"""{"databaseName":"{{db}}"}""").StartTransaction();
        StartTransactionResponse started = (StartTransactionResponse)startResult.Value!;

        JsonResult commitResult = await Transactions(JsonSerializer.Serialize(new
        {
            databaseName = db,
            txnIdPT = started.TxnIdPT,
            txnIdCounter = started.TxnIdCounter,
        }, WebJson)).CommitTransaction();
        CommitTransactionResponse committed = (CommitTransactionResponse)commitResult.Value!;

        Assert.That(committed.Status, Is.EqualTo("ok"), committed.Message);
        Assert.That(committed.CommitTimestamp, Is.Null);
    }

    private static ControllerContext Context(string json)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(json);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }
}
