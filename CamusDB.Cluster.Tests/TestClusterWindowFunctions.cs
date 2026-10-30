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
/// Window functions with distributed query execution on a table split into two spans. The inner
/// query of a windowed SELECT is an ordinary query, so it ships its scan and its partial aggregates
/// to the span owners as any query does; the window runs on the node that runs the statement, over
/// the gathered rows. Every node must return the values computed here from the inserted rows, for a
/// plain window and for a window over an aggregate with GROUP BY.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterWindowFunctions
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private sealed record Row(string Id, long Bucket, long X);

    [Test]
    public async Task Windows_OverASplitTable_MatchTheRowsOnEveryNode()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with
                {
                    KeyRangeShardingEnabled = true,
                    DistributedQueryExecutionEnabled = true,
                });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "t",
                columns:
                [
                    new ColumnInfo("id",     ColumnType.Id),
                    new ColumnInfo("bucket", ColumnType.Integer64, notNull: true),
                    new ColumnInfo("x",      ColumnType.Integer64, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(20)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            // Each id is created just before its row, so the ids and the KV row ids interleave and the
            // first half of the inserts lands below the median split key (see TestClusterNumericGather).
            List<Row> inserted = new(RowCount);
            InProcessSchemaCluster.Node writer = cluster.Nodes[0];
            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

            for (int i = 0; i < RowCount; i++)
            {
                Row row = new(ObjectIdGenerator.Generate().ToString(), i % 4, (i * 37) % 101);
                inserted.Add(row);

                await writer.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "t",
                    values: new() { new() {
                        { "id",     new(ColumnType.Id, row.Id) },
                        { "bucket", new(ColumnType.Integer64, row.Bucket) },
                        { "x",      new(ColumnType.Integer64, row.X) },
                    }}));
            }

            await writer.Database.Transactions.CommitAsync(tx);
            await SplitAtMedianAsync(cluster, inserted.Select(r => r.Id));

            // row_number within each bucket by (x, id); the id is unique, so the order is total.
            Dictionary<string, long> expectedRn = new();
            Dictionary<long, long> bucketTotals = inserted.GroupBy(r => r.Bucket).ToDictionary(g => g.Key, g => g.Sum(r => r.X));

            foreach (IGrouping<long, Row> bucket in inserted.GroupBy(r => r.Bucket))
            {
                long n = 0;
                foreach (Row row in bucket.OrderBy(r => r.X).ThenBy(r => r.Id, StringComparer.Ordinal))
                    expectedRn[row.Id] = ++n;
            }

            List<long> rankedBuckets = bucketTotals.OrderByDescending(kv => kv.Value).Select(kv => kv.Key).ToList();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> rows = await RunSql(node, db,
                    "SELECT id, bucket, row_number() OVER (PARTITION BY bucket ORDER BY x, id) AS rn, "
                    + "sum(x) OVER (PARTITION BY bucket) AS total, count(*) OVER () AS c FROM t");

                Assert.AreEqual(RowCount, rows.Count, $"Node {node.Index}: every row");

                foreach (QueryResultRow row in rows)
                {
                    string id = row.Row["id"].StrValue!;
                    long bucket = row.Row["bucket"].LongValue;

                    Assert.AreEqual(expectedRn[id], row.Row["rn"].LongValue, $"Node {node.Index}: rn of {id}");
                    Assert.AreEqual(bucketTotals[bucket], row.Row["total"].LongValue, $"Node {node.Index}: total of bucket {bucket}");
                    Assert.AreEqual(RowCount, row.Row["c"].LongValue, $"Node {node.Index}: count(*) OVER ()");
                }

                List<QueryResultRow> grouped = await RunSql(node, db,
                    "SELECT bucket, sum(x) AS s, rank() OVER (ORDER BY sum(x) DESC) AS r FROM t GROUP BY bucket ORDER BY r");

                Assert.AreEqual(rankedBuckets, grouped.Select(g => g.Row["bucket"].LongValue).ToList(), $"Node {node.Index}: bucket order");
                Assert.AreEqual(rankedBuckets.Select(b => bucketTotals[b]).ToList(), grouped.Select(g => g.Row["s"].LongValue).ToList());
                Assert.AreEqual(new long[] { 1, 2, 3, 4 }, grouped.Select(g => g.Row["r"].LongValue).ToArray());
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Splits the row keyspace at the median row id and waits until every node sees two spans. Only
    /// the range-map meta-partition leader commits the split; the other nodes refuse it.
    /// </summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, IEnumerable<string> ids)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["t"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[sorted.Count / 2]));

        bool split = false;
        List<string> statuses = [];
        for (int attempt = 0; attempt < 20 && !split; attempt++)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                statuses.Add($"attempt {attempt}, node {node.Index}: {response.Status}");
                if (response.Success)
                {
                    split = true;
                    break;
                }
            }

            if (!split)
                await Task.Delay(250);
        }

        Assert.IsTrue(split, "One node (the meta-partition leader) must commit the split: " + string.Join("; ", statuses.TakeLast(6)));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 50; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= 2)
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2,
                "Every node's placement must report the split spans");
        }
    }

    private static async Task<List<QueryResultRow>> RunSql(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }
}
