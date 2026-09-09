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
using Microsoft.Extensions.Logging;
using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Routing;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Routing advice on statements that run inside a client-owned transaction, over every transport
/// that carries it, answered by a real cluster node: a negotiated statement inside a transaction
/// gets a <c>prefer</c> naming the table's data leader — the same advice an autocommit statement
/// gets — while an un-negotiated statement keeps its historical shape. The transports are driven
/// directly (batch stream, unary RPCs, REST controller) over one node's executor, with the
/// transaction started on that node.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRoutingAdviceInTransactionsCluster
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private sealed class Host
    {
        public InProcessSchemaCluster Cluster = null!;
        public InProcessSchemaCluster.Node Node = null!;
        public string Db = "";
        public HttpTransactionCoordinator Coordinator = null!;
        public CamusSqlService Service = null!;
        public PreparedStatementRegistry Registry = null!;
        public StatementRoutingResolver Resolver = null!;
        public string DataLeader = "";
    }

    private static TestServerCallContext Ctx() => new();

    private static async Task<Host> SetupAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "accounts",
            columns:
            [
                new ColumnInfo("id", CamusDB.Core.Catalogs.Models.ColumnType.Integer64),
                new ColumnInfo("balance", CamusDB.Core.Catalogs.Models.ColumnType.Integer64),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction setup = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: setup, database: db,
                sql: "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)", parameters: null));
            await leader.Database.Transactions.CommitAsync(setup);
        });

        InProcessSchemaCluster.Node node = cluster.Nodes[0];
        CamusDBOptions options = CamusDBOptions.Default;
        Host h = new()
        {
            Cluster = cluster,
            Node = node,
            Db = db,
            Coordinator = new HttpTransactionCoordinator(node.Executor),
            Registry = new PreparedStatementRegistry(options),
            Resolver = new StatementRoutingResolver(options),
        };
        h.Service = new CamusSqlService(node.Executor, h.Coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), options, routing: h.Resolver);

        // The advice must name whoever leads the table's data, which the serving node may or may not be.
        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));
        for (int attempt = 0; attempt < 50 && h.DataLeader.Length == 0; attempt++)
        {
            TablePlacement placement = node.Kahuna.ReadPlacementUncached(table.Store.RowKeySpace, out bool initialized);
            if (initialized && placement.Spans.Count == 1 && placement.Spans[0].LeaderEndpoint is string leaderEndpoint)
                h.DataLeader = leaderEndpoint;
            else
                await Task.Delay(200);
        }
        Assert.That(h.DataLeader, Is.Not.Empty, "the serving node never learned the data leader");

        return h;
    }

    private static async Task<(KvTransaction tx, TxnHandle handle)> StartAsync(Host h)
    {
        KvTransaction tx = await h.Coordinator.StartAsync(h.Db, isolationLevel: null, transactionMode: null,
            deferStart: true, sessionOwned: true);
        return (tx, new TxnHandle { TxnIdPt = tx.ClientId.L, TxnIdCounter = (uint)tx.ClientId.C });
    }

    [Test]
    public async Task Batch_StatementsInsideTransaction_CarryPreferAdvice()
    {
        Host h = await SetupAsync();
        await using InProcessSchemaCluster _ = h.Cluster;

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = h.Service.BatchExecute(reader, writer, Ctx());

        reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = h.Db } });
        BatchExecuteResponse started = await writer.WaitFor(m => m.RequestId == 1);
        Assert.That(started.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.StartReply), started.Error?.Message);
        TxnHandle handle = started.StartReply;

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 2, Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = h.Db, Sql = "SELECT balance FROM accounts WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1 },
        });
        BatchExecuteResponse query = await writer.WaitFor(m => m.RequestId == 2 && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or BatchExecuteResponse.PayloadOneofCase.Error);
        Assert.That(query.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete), query.Error?.Message);
        Assert.That(query.QueryComplete.Routing, Is.Not.Null, "a negotiated query inside a transaction carries advice");
        Assert.That(query.QueryComplete.Routing.Disposition, Is.EqualTo(RoutingDisposition.Prefer));
        Assert.That(query.QueryComplete.Routing.PreferredNodeId, Is.EqualTo(h.DataLeader));
        Assert.That(query.QueryComplete.Routing.Reason, Is.EqualTo(StatementRoutingAdvice.ReasonSingleTableHash));
        Assert.That(query.QueryComplete.Routing.ReuseScope, Is.EqualTo(RoutingReuseScope.StatementParametersIndependent));

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 3, Kind = BatchStatementKind.NonQuery,
            Request = new SqlRequest { Database = h.Db, Sql = "UPDATE accounts SET balance = 150 WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1 },
        });
        BatchExecuteResponse update = await writer.WaitFor(m => m.RequestId == 3);
        Assert.That(update.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), update.Error?.Message);
        Assert.That(update.NonQuery.Routing, Is.Not.Null, "a negotiated mutation inside a transaction carries advice");
        Assert.That(update.NonQuery.Routing.Disposition, Is.EqualTo(RoutingDisposition.Prefer));
        Assert.That(update.NonQuery.Routing.PreferredNodeId, Is.EqualTo(h.DataLeader));

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 4, Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = h.Db, Sql = "SELECT balance FROM accounts WHERE id = 2", TxnHandle = handle },
        });
        BatchExecuteResponse silent = await writer.WaitFor(m => m.RequestId == 4 && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or BatchExecuteResponse.PayloadOneofCase.Error);
        Assert.That(silent.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete), silent.Error?.Message);
        Assert.That(silent.QueryComplete.Routing, Is.Null, "an un-negotiated statement keeps its historical shape");

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 5, Kind = BatchStatementKind.Query,
            Request = new SqlRequest { Database = h.Db, Sql = "SELECT balance FROM nosuchtable", TxnHandle = handle, RoutingAcceptVersion = 1 },
        });
        BatchExecuteResponse failed = await writer.WaitFor(m => m.RequestId == 5 && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or BatchExecuteResponse.PayloadOneofCase.Error);
        Assert.That(failed.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error), "a failed statement produces no terminator and no advice");

        reader.Push(new BatchExecuteRequest { RequestId = 6, Kind = BatchStatementKind.Rollback, Request = new SqlRequest { Database = h.Db, TxnHandle = handle } });
        await writer.WaitFor(m => m.RequestId == 6);
        reader.Complete();
        await server;
    }

    [Test]
    public async Task Unary_StatementsInsideTransaction_CarryAdvice()
    {
        Host h = await SetupAsync();
        await using InProcessSchemaCluster _ = h.Cluster;
        (KvTransaction tx, TxnHandle handle) = await StartAsync(h);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await h.Service.ExecuteQuery(new SqlRequest
        {
            Database = h.Db, Sql = "SELECT balance FROM accounts WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1,
        }, writer, Ctx());
        QueryStreamMessage last = writer.Written[^1];
        Assert.That(last.PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.RoutingAdvice),
            "the negotiated unary query ends with a trailing routing message, after the rows");
        Assert.That(last.RoutingAdvice.Disposition, Is.EqualTo(RoutingDisposition.Prefer));
        Assert.That(last.RoutingAdvice.PreferredNodeId, Is.EqualTo(h.DataLeader));
        Assert.That(writer.Written[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema), "schema stays first");

        NonQueryReply reply = await h.Service.ExecuteNonQuery(new SqlRequest
        {
            Database = h.Db, Sql = "UPDATE accounts SET balance = 150 WHERE id = 1", TxnHandle = handle, RoutingAcceptVersion = 1,
        }, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(1));
        Assert.That(reply.Routing, Is.Not.Null);
        Assert.That(reply.Routing.Disposition, Is.EqualTo(RoutingDisposition.Prefer));
        Assert.That(reply.Routing.PreferredNodeId, Is.EqualTo(h.DataLeader));

        CapturingStreamWriter<QueryStreamMessage> plain = new();
        await h.Service.ExecuteQuery(new SqlRequest
        {
            Database = h.Db, Sql = "SELECT balance FROM accounts WHERE id = 1", TxnHandle = handle,
        }, plain, Ctx());
        Assert.That(plain.Written.Any(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.RoutingAdvice), Is.False,
            "an un-negotiated unary query never gains a trailing message");

        await h.Coordinator.RollbackAsync(tx);
    }

    private static ExecuteSQLController Sql(Host h, object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ExecuteSQLController(h.Node.Executor, h.Coordinator, h.Registry, logger, CamusDBOptions.Default, routing: h.Resolver)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    [Test]
    public async Task Rest_StatementsInsideTransaction_CarryAdvice_AndStaySilentUnnegotiated()
    {
        Host h = await SetupAsync();
        await using InProcessSchemaCluster _ = h.Cluster;
        (KvTransaction tx, TxnHandle handle) = await StartAsync(h);

        JsonResult queryResult = await Sql(h, new
        {
            databaseName = h.Db, sql = "SELECT balance FROM accounts WHERE id = 1",
            txnIdPT = handle.TxnIdPt, txnIdCounter = handle.TxnIdCounter, routingAcceptVersion = 1,
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse query = (ExecuteSQLQueryResponse)queryResult.Value!;
        Assert.That(query.Status, Is.EqualTo("ok"));
        Assert.That(query.Routing, Is.Not.Null, "a negotiated REST query inside a transaction carries the routing object");
        Assert.That(query.Routing!.Disposition, Is.EqualTo("prefer"));
        Assert.That(query.Routing.PreferredNodeId, Is.EqualTo(h.DataLeader));

        JsonResult updateResult = await Sql(h, new
        {
            databaseName = h.Db, sql = "UPDATE accounts SET balance = 150 WHERE id = 1",
            txnIdPT = handle.TxnIdPt, txnIdCounter = handle.TxnIdCounter, routingAcceptVersion = 1,
        }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse update = (ExecuteNonSQLQueryResponse)updateResult.Value!;
        Assert.That(update.Status, Is.EqualTo("ok"));
        Assert.That(update.Routing, Is.Not.Null);
        Assert.That(update.Routing!.Disposition, Is.EqualTo("prefer"));
        Assert.That(update.Routing.PreferredNodeId, Is.EqualTo(h.DataLeader));

        JsonResult plainResult = await Sql(h, new
        {
            databaseName = h.Db, sql = "SELECT balance FROM accounts WHERE id = 2",
            txnIdPT = handle.TxnIdPt, txnIdCounter = handle.TxnIdCounter,
        }).ExecuteSQLQuery();
        Assert.That(((ExecuteSQLQueryResponse)plainResult.Value!).Routing, Is.Null, "un-negotiated: no routing object");

        await h.Coordinator.RollbackAsync(tx);
    }
}
