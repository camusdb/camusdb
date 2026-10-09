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
/// NUMERIC across nodes. A table with a NUMERIC column is created on the schema leader, written on
/// one node and read on every node, after a split that gives its keyspace two spans. A full scan
/// ships raw row bytes from the remote span; an aggregate ships one partial row per remote span,
/// whose cells cross through the value wire codec in both fragment encodings (the in-process
/// transport round-trips every frame through binary and then NDJSON). The values sit above 2⁶³
/// with 9 fraction digits, so a codec that loses a half or rounds through a double fails.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterNumericGather
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    /// <summary>12345678901234567890.123456789 unscaled: past 2⁶⁴ unscaled, so both halves carry bits.</summary>
    private static readonly Int128 Base = Int128.Parse("12345678901234567890123456789");

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>Row i holds Base + i, negated for odd i, so the sums mix signs and cancel in part.</summary>
    private static Int128 AmountOf(int i) => i % 2 == 0 ? Base + i * NumericMath.ScaleFactor : -(Base + i * NumericMath.ScaleFactor);

    private static async Task<(InProcessSchemaCluster cluster, string db, Dictionary<string, Int128> amounts)> SetupAsync()
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
            tableName: "ledger",
            columns:
            [
                new ColumnInfo("id",     ColumnType.Id),
                new ColumnInfo("amount", ColumnType.Numeric, notNull: true),
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

        Dictionary<string, Int128> amounts = new(RowCount);
        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            amounts[id] = AmountOf(i);
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "ledger",
                values: new() { new() {
                    { "id",     new(ColumnType.Id, id) },
                    { "amount", ColumnValue.FromNumeric(AmountOf(i)) },
                    { "bucket", new(ColumnType.Integer64, (long)(i % 4)) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, amounts);
    }

    /// <summary>
    /// Splits the row keyspace at the median row id and waits until every node sees two spans. Only
    /// the range-map meta-partition leader commits the split; the other nodes refuse it.
    /// </summary>
    private static async Task SplitAtMedianAsync(InProcessSchemaCluster cluster, IEnumerable<string> ids, string tableName = "ledger")
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors[tableName];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[sorted.Count / 2]));

        // The range-map leader can still be in election right after setup, when every node answers
        // NotLeader, so the split is retried for a few seconds. The statuses go in the failure message.
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

    /// <summary>
    /// A SUM or AVG of NUMERIC values is exact up to the final result. The rows below the split key
    /// hold the maximum NUMERIC value and the rows above it hold its negative, so the total of each
    /// span, and of each group within a span, is far past the range while each final SUM and AVG is
    /// zero. A plan that finalizes a range-checked SUM for each span fails with a NUMERIC overflow.
    /// <c>peak</c> holds the maximum in every row: its AVG is the maximum, and its SUM is out of range.
    /// </summary>
    [Test]
    public async Task NumericSumAndAvg_AcrossSpans_CheckOnlyTheFinalResult()
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
                    new ColumnInfo("amount", ColumnType.Numeric, notNull: true),
                    new ColumnInfo("peak",   ColumnType.Numeric, notNull: true),
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
            // just after its id value, so the two sequences interleave and the first half of the
            // inserts lands below the split key. Creating every id first would put every row above it.
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
                        { "amount", ColumnValue.FromNumeric(i < RowCount / 2 ? NumericMath.MaxUnscaled : -NumericMath.MaxUnscaled) },
                        { "peak",   ColumnValue.FromNumeric(NumericMath.MaxUnscaled) },
                        { "bucket", new(ColumnType.Integer64, (long)(i % 4)) },
                    }}));
            }
            await writer.Database.Transactions.CommitAsync(tx);

            await SplitAtMedianAsync(cluster, ids, "extremes");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> grouped = await RunSql(node, db,
                    "SELECT bucket, SUM(amount) AS s, AVG(amount) AS a, AVG(peak) AS pk FROM extremes GROUP BY bucket");
                Assert.AreEqual(4, grouped.Count, $"Node {node.Index}: four buckets");

                foreach (QueryResultRow group in grouped)
                {
                    long bucket = group.Row["bucket"].LongValue;
                    Assert.AreEqual(Int128.Zero, group.Row["s"].NumericUnscaled, $"Node {node.Index}: SUM, bucket {bucket}");
                    Assert.AreEqual(Int128.Zero, group.Row["a"].NumericUnscaled, $"Node {node.Index}: AVG, bucket {bucket}");
                    Assert.AreEqual(NumericMath.MaxUnscaled, group.Row["pk"].NumericUnscaled, $"Node {node.Index}: AVG of the maximum, bucket {bucket}");
                }

                List<QueryResultRow> total = await RunSql(node, db, "SELECT SUM(amount) AS s FROM extremes");
                Assert.AreEqual(Int128.Zero, total[0].Row["s"].NumericUnscaled, $"Node {node.Index}: global SUM");

                CamusDBException overflow = Assert.ThrowsAsync<CamusDBException>(() => RunSql(node, db, "SELECT SUM(peak) AS s FROM extremes"))!;
                Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, overflow.Code, $"Node {node.Index}: a final SUM past the range");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task NumericColumn_CrossesNodesAndFragments_Exactly()
    {
        (InProcessSchemaCluster cluster, string db, Dictionary<string, Int128> amounts) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, amounts.Keys);

            Int128 expectedSum = Int128.Zero;
            foreach (Int128 amount in amounts.Values)
                expectedSum += amount;

            Int128 expectedAvg = NumericMath.Divide(expectedSum, NumericMath.FromInt64(RowCount));
            Int128 expectedMax = amounts.Values.Max();
            Int128 expectedMin = amounts.Values.Min();

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                // Rows: the schema replicated to this node, and the remote span's row bytes decode here.
                List<QueryResultRow> rows = await RunSql(node, db, "SELECT id, amount FROM ledger");
                Assert.AreEqual(RowCount, rows.Count, $"Node {node.Index}: every row once");

                foreach (QueryResultRow row in rows)
                {
                    ColumnValue amount = row.Row["amount"];
                    Assert.AreEqual(ColumnType.Numeric, amount.Type, $"Node {node.Index}: type");
                    Assert.AreEqual(amounts[row.Row["id"].StrValue!], amount.NumericUnscaled, $"Node {node.Index}: value");
                }

                // Partial aggregates: one row of cells per remote span, merged by the coordinator.
                List<QueryResultRow> totals = await RunSql(node, db,
                    "SELECT SUM(amount) AS s, AVG(amount) AS a, MAX(amount) AS hi, MIN(amount) AS lo, COUNT(*) AS c FROM ledger");
                Assert.AreEqual(1, totals.Count);

                IReadOnlyDictionary<string, ColumnValue> t = totals[0].Row;
                Assert.AreEqual(ColumnType.Numeric, t["s"].Type, $"Node {node.Index}: SUM type");
                Assert.AreEqual(expectedSum, t["s"].NumericUnscaled, $"Node {node.Index}: SUM");
                Assert.AreEqual(ColumnType.Numeric, t["a"].Type, $"Node {node.Index}: AVG type");
                Assert.AreEqual(expectedAvg, t["a"].NumericUnscaled, $"Node {node.Index}: AVG");
                Assert.AreEqual(expectedMax, t["hi"].NumericUnscaled, $"Node {node.Index}: MAX");
                Assert.AreEqual(expectedMin, t["lo"].NumericUnscaled, $"Node {node.Index}: MIN");
                Assert.AreEqual((long)RowCount, t["c"].LongValue, $"Node {node.Index}: COUNT");

                List<QueryResultRow> grouped = await RunSql(node, db,
                    "SELECT bucket, SUM(amount) AS s FROM ledger GROUP BY bucket");
                Assert.AreEqual(4, grouped.Count, $"Node {node.Index}: four buckets");

                foreach (QueryResultRow group in grouped)
                {
                    long bucket = group.Row["bucket"].LongValue;
                    Int128 expected = Int128.Zero;
                    for (int i = (int)bucket; i < RowCount; i += 4)
                        expected += AmountOf(i);

                    Assert.AreEqual(expected, group.Row["s"].NumericUnscaled, $"Node {node.Index}: bucket {bucket}");
                }
            }

            // A NUMERIC SUM or AVG runs above the gather, but MIN and MAX still split into one partial
            // row per remote span, so NUMERIC cells cross the value wire codec.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> extremes = await RunSql(node, db,
                    "SELECT bucket, MIN(amount) AS lo, MAX(amount) AS hi FROM ledger GROUP BY bucket");
                Assert.AreEqual(4, extremes.Count, $"Node {node.Index}: four buckets");

                foreach (QueryResultRow group in extremes)
                {
                    long bucket = group.Row["bucket"].LongValue;
                    List<Int128> inBucket = [];
                    for (int i = (int)bucket; i < RowCount; i += 4)
                        inBucket.Add(AmountOf(i));

                    Assert.AreEqual(inBucket.Min(), group.Row["lo"].NumericUnscaled, $"Node {node.Index}: MIN, bucket {bucket}");
                    Assert.AreEqual(inBucket.Max(), group.Row["hi"].NumericUnscaled, $"Node {node.Index}: MAX, bucket {bucket}");
                }
            }

            // With two spans on three nodes, at least one coordinator reads a span another node
            // serves. Without a remote fragment the codec would not run and this test would prove nothing.
            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "a remote fragment must run");

            // Reopen: every node reloads the schema from its checkpoint and still reads the column as
            // NUMERIC with the exact values.
            for (int i = 0; i < cluster.Nodes.Length; i++)
                await cluster.ReopenDatabaseAsync(i, db);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TableColumnSchema column = node.Database!.Schema.Tables["ledger"].Columns!.Single(c => c.Name == "amount");
                Assert.AreEqual(ColumnType.Numeric, column.Type, $"Node {node.Index}: column type after reopen");

                List<QueryResultRow> rows = await RunSql(node, db, "SELECT id, amount FROM ledger");
                Assert.AreEqual(RowCount, rows.Count, $"Node {node.Index}: rows after reopen");
                foreach (QueryResultRow row in rows)
                    Assert.AreEqual(amounts[row.Row["id"].StrValue!], row.Row["amount"].NumericUnscaled, $"Node {node.Index}: value after reopen");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
