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
/// Integer64 SUM and AVG across spans. An aggregate below a gather runs per span and merges the span
/// rows. A span's own Integer64 SUM would be range-checked, so a span total past the int64 range
/// would fail a query whose full total, or average, is in range; and before Integer64 overflow was an
/// error, the span totals wrapped and the AVG of large values was silently wrong. The split ships an
/// Integer64 column's span totals as exact NUMERIC values, and checks only the merged total, which is
/// the single-node rule.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterInt64Gather
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>
    /// Splits the row keyspace at the median row id and waits until every node sees two spans. Only
    /// the range-map meta-partition leader commits the split; the other nodes refuse it.
    /// </summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, IEnumerable<string> ids, string tableName)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors[tableName];
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

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= 2, "Every node's placement must report the split spans");
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

    /// <summary>
    /// The rows below the split key hold <c>long.MaxValue</c> in <c>big</c> and the rows above it hold
    /// <c>-long.MaxValue</c>, so each span total, and each group total within a span, is far past the
    /// int64 range while each final SUM is zero. <c>peak</c> holds <c>long.MaxValue</c> in every row:
    /// its AVG is <c>long.MaxValue</c>, and its SUM is out of range on every node.
    /// </summary>
    [Test]
    public async Task Int64SumAndAvg_AcrossSpans_CheckOnlyTheFinalResult()
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
                tableName: "extremes",
                columns:
                [
                    new ColumnInfo("id",     ColumnType.Id),
                    new ColumnInfo("big",    ColumnType.Integer64, notNull: true),
                    new ColumnInfo("peak",   ColumnType.Integer64, notNull: true),
                    new ColumnInfo("bucket", ColumnType.Integer64, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(20)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            // The split key comes from the median id value, and the KV row id of each row is created
            // just after its id value, so the first half of the inserts lands below the split key.
            List<string> ids = new(RowCount);

            InProcessSchemaCluster.Node writer = cluster.Nodes[0];
            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
            for (int i = 0; i < RowCount; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);
                await writer.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "extremes",
                    values: new() { new() {
                        { "id",     new(ColumnType.Id, id) },
                        { "big",    new(ColumnType.Integer64, i < RowCount / 2 ? long.MaxValue : -long.MaxValue) },
                        { "peak",   new(ColumnType.Integer64, long.MaxValue) },
                        { "bucket", new(ColumnType.Integer64, (long)(i % 4)) },
                    }}));
            }
            await writer.Database.Transactions.CommitAsync(tx);

            await SplitAtMedianAsync(cluster, ids, "extremes");

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> grouped = await RunSql(node, db,
                    "SELECT bucket, SUM(big) AS s, AVG(big) AS a, AVG(peak) AS pk FROM extremes GROUP BY bucket");
                Assert.AreEqual(4, grouped.Count, $"Node {node.Index}: four buckets");

                foreach (QueryResultRow group in grouped)
                {
                    long bucket = group.Row["bucket"].LongValue;
                    Assert.AreEqual(ColumnType.Integer64, group.Row["s"].Type, $"Node {node.Index}: SUM type, bucket {bucket}");
                    Assert.AreEqual(0L, group.Row["s"].LongValue, $"Node {node.Index}: SUM, bucket {bucket}");
                    Assert.AreEqual(ColumnType.Float64, group.Row["a"].Type, $"Node {node.Index}: AVG type, bucket {bucket}");
                    Assert.AreEqual(0.0, group.Row["a"].FloatValue, $"Node {node.Index}: AVG, bucket {bucket}");
                    Assert.AreEqual((double)long.MaxValue, group.Row["pk"].FloatValue, $"Node {node.Index}: AVG of the maximum, bucket {bucket}");
                }

                List<QueryResultRow> total = await RunSql(node, db, "SELECT SUM(big) AS s FROM extremes");
                Assert.AreEqual(ColumnType.Integer64, total[0].Row["s"].Type, $"Node {node.Index}: global SUM type");
                Assert.AreEqual(0L, total[0].Row["s"].LongValue, $"Node {node.Index}: global SUM");

                CamusDBException overflow = Assert.ThrowsAsync<CamusDBException>(() => RunSql(node, db, "SELECT SUM(peak) AS s FROM extremes"))!;
                Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, overflow.Code, $"Node {node.Index}: a final SUM past the range");

                CamusDBException groupedOverflow = Assert.ThrowsAsync<CamusDBException>(() =>
                    RunSql(node, db, "SELECT bucket, SUM(peak) AS s FROM extremes GROUP BY bucket"))!;
                Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, groupedOverflow.Code, $"Node {node.Index}: a grouped SUM past the range");
            }

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "The aggregates must run as span fragments below a gather");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
