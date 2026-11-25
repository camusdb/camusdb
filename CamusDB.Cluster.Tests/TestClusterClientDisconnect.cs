/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;
using CamusDB.Tests.Utils;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using QueryFilter = CamusDB.Grpc.QueryFilter;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// A client that disconnects while its write runs, on every node of a three-node cluster. A table's
/// rows live on one partition, so on two of the nodes the transaction coordinator is remote, and the
/// commit and the rollback go through the leader lookup. That lookup refuses a cancelled token. A finalize
/// on the token of the dropped call would therefore be cut there: the transaction would keep its row lock,
/// and neither the reaper nor a client could end it.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterClientDisconnect
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    // Read Committed, so that the unary and typed calls run the statement once and not in the
    // Serializable retry loop, which has its own token.
    private static readonly CamusDBOptions Options = CamusDBOptions.Default with
    {
        DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted,
    };

    private static async Task<(InProcessSchemaCluster cluster, string db)> StartAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger, options: Options);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => ClusterStatements.AutocommitAsync(
            leader, db, "CREATE TABLE kv (k int64 PRIMARY KEY NOT NULL, n int64)", ddl: true));

        await WriteAsync(cluster.Nodes[0], db, "INSERT INTO kv (k, n) VALUES (1, 0)");

        // Every node opens the table once, so that a gate can be installed on it.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(0, await ReadAsync(node, db));

        return (cluster, db);
    }

    private static async Task WriteAsync(InProcessSchemaCluster.Node on, string db, string sql)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            await on.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await on.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await on.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<long> ReadAsync(InProcessSchemaCluster.Node on, string db)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await on.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT n FROM kv WHERE k = 1", null));
            return (await cursor.ToListAsync()).Single().Row["n"].LongValue;
        }
        finally
        {
            await on.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// The write of a client that left commits through every gateway, and leaves no lock: the next write
    /// of the row, on another node, neither waits nor conflicts.
    /// </summary>
    [TestCase("batch")]
    [TestCase("unary")]
    [TestCase("typed row")]
    [TestCase("rest sql")]
    [TestCase("rest typed row")]
    public async Task AutocommitWriteOfAClientThatLeft_CommitsAndLeavesNoLock(string transport)
    {
        (InProcessSchemaCluster cluster, string db) = await StartAsync();
        await using InProcessSchemaCluster owned = cluster;
        long expected = 0;

        foreach (InProcessSchemaCluster.Node gateway in cluster.Nodes)
        {
            HttpTransactionCoordinator coordinator = new(gateway.Executor);
            using CancellationTokenSource disconnect = new();
            TableGate gate = await TableGate.InstallAsync(gateway.Database!, "kv");

            expected++;
            Task call = Task.Run(() => WriteThroughAsync(gateway, coordinator, db, transport, expected, disconnect.Token), CancellationToken.None);
            await gate.Reached;
            disconnect.Cancel();
            gate.Open();

            try { await call.WaitAsync(Deadline); }
            catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

            Assert.AreEqual(0, coordinator.ActiveCount, $"node {gateway.Index}: the request finalized its transaction");

            InProcessSchemaCluster.Node other = cluster.Nodes[(gateway.Index + 1) % cluster.Nodes.Length];
            await WriteAsync(other, db, "UPDATE kv SET n = n + 100 WHERE k = 1").WaitAsync(Deadline);
            Assert.AreEqual(expected + 100, await ReadAsync(other, db), $"node {gateway.Index}: the write of the client that left is stored");
            await WriteAsync(other, db, "UPDATE kv SET n = n - 100 WHERE k = 1").WaitAsync(Deadline);
        }
    }

    /// <summary>
    /// A batch stream that drops while its COMMIT waits behind a statement: the statement ends, and then
    /// the commit or the rollback of the stream's end finalizes the transaction. Either outcome is
    /// correct; a transaction that stays open with its lock is not.
    /// </summary>
    [Test]
    public async Task ExplicitTransactionOfADroppedStream_IsFinalizedAndLeavesNoLock()
    {
        (InProcessSchemaCluster cluster, string db) = await StartAsync();
        await using InProcessSchemaCluster owned = cluster;

        foreach (InProcessSchemaCluster.Node gateway in cluster.Nodes)
        {
            HttpTransactionCoordinator coordinator = new(gateway.Executor);
            CamusSqlService service = new(gateway.Executor, coordinator, logger,
                TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
            using CancellationTokenSource disconnect = new();
            ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
            ObservingStreamWriter<BatchExecuteResponse> writer = new();
            Task stream = Task.Run(() => service.BatchExecute(reader, writer, new TestServerCallContext(disconnect.Token)), CancellationToken.None);

            reader.Push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = db } });
            TxnHandle handle = (await writer.WaitFor(m => m.RequestId == 1).WaitAsync(Deadline)).StartReply;

            TableGate gate = await TableGate.InstallAsync(gateway.Database!, "kv");
            reader.Push(new BatchExecuteRequest
            {
                RequestId = 2, Kind = BatchStatementKind.NonQuery,
                Request = new SqlRequest { Database = db, Sql = "UPDATE kv SET n = n + 1 WHERE k = 1", TxnHandle = handle },
            });
            reader.Push(new BatchExecuteRequest
            {
                RequestId = 3, Kind = BatchStatementKind.Commit,
                Request = new SqlRequest { Database = db, TxnHandle = handle },
            });

            await gate.Reached;
            disconnect.Cancel();
            gate.Open();

            try { await stream.WaitAsync(Deadline); }
            catch (Exception ex) when (ex is RpcException or OperationCanceledException) { /* the dropped call */ }

            Assert.AreEqual(0, coordinator.ActiveCount, $"node {gateway.Index}: the transaction ended with its stream");

            InProcessSchemaCluster.Node other = cluster.Nodes[(gateway.Index + 1) % cluster.Nodes.Length];
            await WriteAsync(other, db, "UPDATE kv SET n = 0 WHERE k = 1").WaitAsync(Deadline);
            Assert.AreEqual(0, await ReadAsync(other, db));
        }
    }

    private static ControllerContext RestContext(object body, CancellationToken aborted)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(body);
        DefaultHttpContext http = new() { RequestAborted = aborted };
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private static async Task WriteThroughAsync(
        InProcessSchemaCluster.Node gateway,
        HttpTransactionCoordinator coordinator,
        string db,
        string transport,
        long value,
        CancellationToken ct)
    {
        CamusSqlService sql = new(gateway.Executor, coordinator, logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
        SqlRequest update = new() { Database = db, Sql = $"UPDATE kv SET n = {value} WHERE k = 1" };

        switch (transport)
        {
            case "batch":
                await sql.BatchExecute(
                    new FakeAsyncStreamReader<BatchExecuteRequest>([new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.NonQuery, Request = update }]),
                    new CapturingStreamWriter<BatchExecuteResponse>(), new TestServerCallContext(ct));
                break;

            case "unary":
                await sql.ExecuteNonQuery(update, new TestServerCallContext(ct));
                break;

            case "typed row":
                await new CamusRowsService(gateway.Executor, coordinator, logger, Options).UpdateRows(new UpdateRowsRequest
                {
                    Database = db,
                    Table = "kv",
                    Values = { ["n"] = new Value { Int64Value = value } },
                    Filters = { new QueryFilter { ColumnName = "k", Op = "=", Value = new Value { Int64Value = 1 } } },
                }, new TestServerCallContext(ct));
                break;

            case "rest sql":
            {
                JsonResult result = await new ExecuteSQLController(gateway.Executor, coordinator, new PreparedStatementRegistry(Options), logger, Options)
                {
                    ControllerContext = RestContext(new { databaseName = db, sql = update.Sql }, ct),
                }.ExecuteNonSQLQuery();
                Assert.AreEqual("ok", ((ExecuteNonSQLQueryResponse)result.Value!).Status);
                break;
            }

            case "rest typed row":
            {
                JsonResult result = await new UpdateController(gateway.Executor, coordinator, logger, Options)
                {
                    ControllerContext = RestContext(new
                    {
                        databaseName = db,
                        tableName = "kv",
                        values = new { n = new { type = (int)CoreColumnType.Integer64, longValue = value } },
                        filters = new[] { new { columnName = "k", op = "=", value = new { type = (int)CoreColumnType.Integer64, longValue = 1L } } },
                    }, ct),
                }.Update();
                Assert.AreEqual("ok", ((UpdateResponse)result.Value!).Status);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(transport), transport, null);
        }
    }
}
