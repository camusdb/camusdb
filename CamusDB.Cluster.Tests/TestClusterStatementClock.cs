/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.Communication.Rest;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>now()</c> across nodes. The table's keyspace is split into two spans, so a scan from any node
/// runs as a gather with remote fragments. A clock call is never shipped to a fragment: the
/// coordinator evaluates it with its own statement's times. So a query that projects, filters on or
/// aggregates <c>now()</c> reports the coordinator transaction's start on every node, whichever node
/// read each row. A <c>DEFAULT now()</c> and an <c>UPDATE … SET ts = now()</c> write the writer
/// transaction's start into rows of both spans.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterStatementClock
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids, DateTime writerStart)> SetupAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with
                {
                    KeyRangeShardingEnabled = true,
                    DistributedQueryExecutionEnabled = true,
                });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id",  ColumnType.Id),
                new ColumnInfo("num", ColumnType.Integer64, notNull: true),
                new ColumnInfo("ts",  ColumnType.DateTime, defaultFunction: "now"),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        List<string> ids = new(RowCount);
        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        // One row per call, all in one transaction: the default must be the transaction's start for
        // every row, not the time of each call.
        for (int i = 0; i < RowCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",  new(ColumnType.Id,        id) },
                    { "num", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, ids, tx.StartTime);
    }

    /// <summary>
    /// Splits the row keyspace at the median row and waits until every node sees two spans, each with
    /// a known leader, so a scan dispatches a remote fragment for the span its node does not lead.
    /// </summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, List<string> ids)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["readings"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[RowCount / 2]));

        // The range-map leader can still be in election right after setup, so the split is retried.
        bool split = false;
        for (int attempt = 0; attempt < 20 && !split; attempt++)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                if (response.Success)
                {
                    split = true;
                    break;
                }
            }

            if (!split)
                await Task.Delay(250);
        }

        Assert.IsTrue(split, "One node (the meta-partition leader) must commit the split");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 50; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= 2 && placement.Spans.All(s => s.LeaderEndpoint is not null))
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2,
                "Every node's placement must report the split spans");
        }
    }

    private static async Task<List<QueryResultRow>> QueryIn(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: sql, parameters: null));

        return await cursor.ToListAsync();
    }

    private static List<long> Ticks(List<QueryResultRow> rows, string column)
        => rows.Select(r =>
        {
            Assert.AreEqual(ColumnType.DateTime, r.Row[column].Type, column);
            return r.Row[column].LongValue;
        }).ToList();

    [Test]
    public async Task Now_IsTheCoordinatorTransactionStart_OnEveryNode_ThroughAGather()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids, DateTime writerStart) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, ids);

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KvTransaction tx = await node.Database!.Transactions.BeginAsync();
                try
                {
                    // The filter is a pure function of the row, so it ships with the remote fragment;
                    // the clock calls in the projection stay on this node.
                    List<QueryResultRow> projected = await QueryIn(node, tx, db,
                        "SELECT id, ts, now() AS n, statement_timestamp() AS s FROM readings WHERE num >= 50 AND num < 150");

                    Assert.AreEqual(100, projected.Count, $"Node {node.Index}: filtered gather row count");
                    Assert.AreEqual(tx.StartTime.Ticks, Ticks(projected, "n").Distinct().Single(),
                        $"Node {node.Index}: now() is the coordinator transaction's start for rows of both spans");
                    Assert.AreEqual(1, Ticks(projected, "s").Distinct().Count(), $"Node {node.Index}: one statement time");
                    Assert.AreEqual(writerStart.Ticks, Ticks(projected, "ts").Distinct().Single(),
                        $"Node {node.Index}: DEFAULT now() wrote the writer transaction's start into both spans");

                    // A filter on now() is volatile, so it is evaluated here and never shipped.
                    List<QueryResultRow> older = await QueryIn(node, tx, db, "SELECT id FROM readings WHERE ts < now()");
                    Assert.AreEqual(RowCount, older.Count, $"Node {node.Index}: every row is older than this transaction");

                    List<QueryResultRow> aggregated = await QueryIn(node, tx, db, "SELECT count(*) AS c, max(now()) AS m FROM readings");
                    Assert.AreEqual(RowCount, aggregated[0].Row["c"].LongValue);
                    Assert.AreEqual(tx.StartTime.Ticks, aggregated[0].Row["m"].LongValue,
                        $"Node {node.Index}: an aggregate of now() is the transaction's start");

                    await node.Database.Transactions.CommitAsync(tx);
                }
                finally
                {
                    await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
                }
            }

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0,
                "At least one span must have run as a remote fragment, or the test proves nothing about a gather");

            // An UPDATE coordinated on one node writes its transaction's start into rows of both spans.
            InProcessSchemaCluster.Node updater = cluster.Nodes[1];
            KvTransaction updateTx = await updater.Database!.Transactions.BeginAsync();
            try
            {
                await updater.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: updateTx, database: db, sql: "UPDATE readings SET ts = now() WHERE num >= 0", parameters: null));
                await updater.Database.Transactions.CommitAsync(updateTx);
            }
            finally
            {
                await updater.Database.Transactions.RollbackIfNotCompletedAsync(updateTx);
            }

            InProcessSchemaCluster.Node reader = cluster.Nodes[2];
            KvTransaction readTx = await reader.Database!.Transactions.BeginAsync();
            try
            {
                List<QueryResultRow> updated = await QueryIn(reader, readTx, db, "SELECT ts FROM readings");
                Assert.AreEqual(RowCount, updated.Count);
                Assert.AreEqual(updateTx.StartTime.Ticks, Ticks(updated, "ts").Distinct().Single());
            }
            finally
            {
                await reader.Database.Transactions.RollbackIfNotCompletedAsync(readTx);
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
