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
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;


using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Routing;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// A standalone node has one destination, so a statement inside a client-owned transaction gets no
/// routing metadata even when the request asks for it — the feature stays invisible there over the
/// batch stream and over REST. The positive half of the contract (a cluster node answering with a
/// <c>prefer</c> for statements inside a transaction) runs on the in-process cluster in the cluster
/// test project, because emission keys on the Kahuna node's own cluster flag.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRoutingAdviceInTransactions : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CamusSqlService service = null!;
    private PreparedStatementRegistry registry = null!;
    private StatementRoutingResolver resolver = null!;

    [SetUp]
    public void SetUpHost()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
        resolver = new(Options);
        service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), Options, routing: resolver);
    }

    [TearDown]
    public async Task TearDownHost()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private static TestServerCallContext Ctx() => new();

    private async Task<string> CreateAccountsAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n")[..12];
        await service.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, Ctx());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE accounts (id int64 PRIMARY KEY, balance int64 NOT NULL)",
        }, Ctx());
        await service.ExecuteNonQuery(new SqlRequest
        {
            Database = db,
            Sql = "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)",
        }, Ctx());
        return db;
    }

    private ExecuteSQLController Sql(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ExecuteSQLController(executor, coordinator, registry, logger, Options, routing: resolver)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    [Test]
    public async Task Batch_StatementsInsideTransaction_StaySilentOnStandaloneNode()
    {
        string db = await CreateAccountsAsync();

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = db } });
        TxnHandle handle = (await writer.WaitFor(m => m.RequestId == 1)).StartReply;

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 2, Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = db, Sql = "SELECT balance FROM accounts WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1 },
        });
        BatchExecuteResponse query = await writer.WaitFor(m => m.RequestId == 2 && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or BatchExecuteResponse.PayloadOneofCase.Error);
        Assert.That(query.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete), query.Error?.Message);
        Assert.That(query.QueryComplete.Routing, Is.Null, "standalone has one destination; no metadata even when asked");

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 3, Kind = BatchStatementKind.NonQuery,
            Request = new SqlRequest { Database = db, Sql = "UPDATE accounts SET balance = 150 WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1 },
        });
        BatchExecuteResponse update = await writer.WaitFor(m => m.RequestId == 3);
        Assert.That(update.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), update.Error?.Message);
        Assert.That(update.NonQuery.Routing, Is.Null);

        reader.Push(new BatchExecuteRequest { RequestId = 4, Kind = BatchStatementKind.Commit, Request = new SqlRequest { Database = db, TxnHandle = handle } });
        await writer.WaitFor(m => m.RequestId == 4);
        reader.Complete();
        await server;
    }

    [Test]
    public async Task Rest_StatementsInsideTransaction_StaySilentOnStandaloneNode()
    {
        string db = await CreateAccountsAsync();
        KvTransaction tx = await coordinator.StartAsync(db, isolationLevel: null, transactionMode: null, deferStart: true, sessionOwned: true);
        TxnHandle handle = new() { TxnIdPt = tx.ClientId.L, TxnIdCounter = (uint)tx.ClientId.C };

        JsonResult queryResult = await Sql(new
        {
            databaseName = db, sql = "SELECT balance FROM accounts WHERE id = 1",
            txnIdPT = handle.TxnIdPt, txnIdCounter = handle.TxnIdCounter, routingAcceptVersion = 1,
        }).ExecuteSQLQuery();
        Assert.That(((ExecuteSQLQueryResponse)queryResult.Value!).Routing, Is.Null);

        JsonResult updateResult = await Sql(new
        {
            databaseName = db, sql = "UPDATE accounts SET balance = 150 WHERE id = 1",
            txnIdPT = handle.TxnIdPt, txnIdCounter = handle.TxnIdCounter, routingAcceptVersion = 1,
        }).ExecuteNonSQLQuery();
        Assert.That(((ExecuteNonSQLQueryResponse)updateResult.Value!).Routing, Is.Null);

        await coordinator.RollbackAsync(tx);
    }
}
