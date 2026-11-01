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
/// Float64 rows that hold ±infinity, NaN and -0, read through span fragments below a gather. A
/// fragment ships decoded values as JSON (ColumnValueWireCodec), and JSON has no number for a
/// non-finite value, so before, the fragment failed while it wrote such a cell. The split also
/// merges per-span MIN, MAX, COUNT and SUM, which must follow the SQL order (NaN above +infinity,
/// -0 = 0) on every node, and a NaN filter ships as a CAST of the text 'NaN'.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterFloatGather
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly double[] Cycle =
    [
        double.NegativeInfinity, -1.5, -0.0, 0.0, 1.5, double.PositiveInfinity, double.NaN,
        BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000),
    ];

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

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

    private static void AssertSameFloat(double expected, ColumnValue actual, string what)
    {
        Assert.AreEqual(ColumnType.Float64, actual.Type, what);
        if (double.IsNaN(expected))
        {
            Assert.IsTrue(double.IsNaN(actual.FloatValue), $"{what}: expected NaN, got {actual.FloatValue}");
            return;
        }

        Assert.AreEqual(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(actual.FloatValue), $"{what}: {actual.FloatValue:R}");
    }

    /// <summary>
    /// Row i holds <c>Cycle[i % 8]</c> in <c>f</c> and <c>i % 4</c> in <c>g</c>, so group g holds
    /// <c>Cycle[g]</c> and <c>Cycle[g + 4]</c>, and every value lands in both spans.
    /// </summary>
    [Test]
    public async Task SpecialFloats_AcrossSpans_ReadAndAggregateByTheSqlOrder()
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
                tableName: "specials",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("n",  ColumnType.Integer64, notNull: true),
                    new ColumnInfo("f",  ColumnType.Float64),
                    new ColumnInfo("g",  ColumnType.Integer64, notNull: true),
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
            for (int i = 0; i < RowCount; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);
                await writer.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "specials",
                    values: new() { new() {
                        { "id", new(ColumnType.Id, id) },
                        { "n",  new(ColumnType.Integer64, (long)i) },
                        { "f",  new(ColumnType.Float64, Cycle[i % 8]) },
                        { "g",  new(ColumnType.Integer64, (long)(i % 4)) },
                    }}));
            }
            await writer.Database.Transactions.CommitAsync(tx);

            await SplitAtMedianAsync(cluster, ids, "specials");

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> all = await RunSql(node, db, "SELECT n, f FROM specials");
                Assert.AreEqual(RowCount, all.Count, $"Node {node.Index}: every row");
                foreach (QueryResultRow row in all)
                {
                    long n = row.Row["n"].LongValue;
                    AssertSameFloat(Cycle[n % 8], row.Row["f"], $"Node {node.Index}: row {n}");
                }

                List<QueryResultRow> grouped = await RunSql(node, db,
                    "SELECT g, MIN(f) AS lo, MAX(f) AS hi, COUNT(f) AS c FROM specials GROUP BY g");
                Assert.AreEqual(4, grouped.Count, $"Node {node.Index}: four groups");
                foreach (QueryResultRow group in grouped)
                {
                    long g = group.Row["g"].LongValue;
                    double a = Cycle[g], b = Cycle[g + 4];
                    double lo = FloatOrderMin(a, b), hi = FloatOrderMax(a, b);
                    AssertSameFloat(lo, group.Row["lo"], $"Node {node.Index}: MIN, group {g}");
                    AssertSameFloat(hi, group.Row["hi"], $"Node {node.Index}: MAX, group {g}");
                    Assert.AreEqual(RowCount / 4, group.Row["c"].LongValue, $"Node {node.Index}: COUNT, group {g}");
                }

                QueryResultRow totals = (await RunSql(node, db,
                    "SELECT MIN(f) AS lo, MAX(f) AS hi FROM specials")).Single();
                AssertSameFloat(double.NegativeInfinity, totals.Row["lo"], $"Node {node.Index}: global MIN");
                AssertSameFloat(double.NaN, totals.Row["hi"], $"Node {node.Index}: global MAX is NaN");

                QueryResultRow infinite = (await RunSql(node, db, "SELECT SUM(f) AS s FROM specials WHERE g = 1")).Single();
                AssertSameFloat(double.PositiveInfinity, infinite.Row["s"], $"Node {node.Index}: SUM of -1.5 and +infinity");

                QueryResultRow zeros = (await RunSql(node, db, "SELECT SUM(f) AS s, COUNT(*) AS c FROM specials WHERE f = 0")).Single();
                Assert.AreEqual(RowCount / 4, zeros.Row["c"].LongValue, $"Node {node.Index}: -0 and 0 rows");
                Assert.AreEqual(0.0, zeros.Row["s"].FloatValue, $"Node {node.Index}: SUM of zeros");

                QueryResultRow nans = (await RunSql(node, db,
                    "SELECT COUNT(*) AS c FROM specials WHERE f = CAST('NaN' AS float64)")).Single();
                Assert.AreEqual(RowCount / 4, nans.Row["c"].LongValue, $"Node {node.Index}: both NaNs match NaN");
            }

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "The queries must run as span fragments below a gather");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>The smaller of two values in the SQL order, where NaN is above every number.</summary>
    private static double FloatOrderMin(double a, double b) => double.IsNaN(a) ? b : double.IsNaN(b) ? a : Math.Min(a, b);

    /// <summary>The larger of two values in the SQL order, where NaN is above every number.</summary>
    private static double FloatOrderMax(double a, double b) => double.IsNaN(a) || double.IsNaN(b) ? double.NaN : Math.Max(a, b);
}
