/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
/// Measures what a top-k aggregate (<c>GROUP BY g ORDER BY aggregate LIMIT 10</c>) costs on a
/// three-node cluster whose table is split into several key-range spans: how many partial rows and
/// how many bytes of aggregate cells the span fragments ship to the coordinator, and how long the
/// query takes compared with a scan floor (<c>SELECT COUNT(*)</c>, one partial row per span) and
/// with the same grouping without ORDER BY and LIMIT.
///
/// <para>Three group columns: a skewed one (a few groups hold most rows), a flat one (every group
/// holds the same number of rows), and one with 10 groups only. A top-k pruning scheme can help the
/// first and cannot help the second, so the baseline records both. The 10-group column makes each
/// span scan and group every row but ship only 10 partial rows, so the difference between it and a
/// high-cardinality column is the cost that grows with the group count: a larger hash table on the
/// span, the encoding and transfer of the partial rows, and the merge on the coordinator.</para>
///
/// <para>Explicit: it loads tens of thousands of rows into an in-process cluster and prints numbers;
/// it asserts only that the queries ran through the partial-aggregate gather.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class BenchTopKAggregation
{
    private const int RowCount = 60_000;

    private const int Groups = 10_000;

    private const int BatchSize = 500;

    private const int Spans = 3;

    private const int Runs = 9;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    [Test]
    [Explicit("benchmark — run alone; prints the top-k aggregation baseline")]
    public async Task TopKAggregationBaseline()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: Spans,
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
                tableName: "orders",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("g_skew", ColumnType.Integer64, notNull: true),
                    new ColumnInfo("g_flat", ColumnType.Integer64, notNull: true),
                    new ColumnInfo("g_low", ColumnType.Integer64, notNull: true),
                    new ColumnInfo("x", ColumnType.Integer64, notNull: true),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(20)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            Stopwatch load = Stopwatch.StartNew();
            List<string> ids = await LoadAsync(cluster.Nodes[0], db);
            TestContext.Progress.WriteLine($"loaded {RowCount} rows in {load.Elapsed.TotalSeconds:F1} s");

            int spanCount = await SplitIntoSpansAsync(cluster, ids);
            TestContext.Progress.WriteLine($"placement: {spanCount} spans");

            string[] queries =
            [
                "SELECT COUNT(*) AS c FROM orders",
                "SELECT g_low, COUNT(*) AS c FROM orders GROUP BY g_low ORDER BY c DESC LIMIT 10",
                "SELECT g_skew, COUNT(*) AS c FROM orders GROUP BY g_skew",
                "SELECT g_skew, COUNT(*) AS c FROM orders GROUP BY g_skew ORDER BY c DESC LIMIT 10",
                "SELECT g_flat, COUNT(*) AS c FROM orders GROUP BY g_flat ORDER BY c DESC LIMIT 10",
                "SELECT g_skew, MAX(x) AS m FROM orders GROUP BY g_skew ORDER BY m DESC LIMIT 10",
                "SELECT g_flat, MAX(x) AS m FROM orders GROUP BY g_flat ORDER BY m DESC LIMIT 10",
                "SELECT g_skew, COUNT(*) AS c, MAX(x) AS m FROM orders GROUP BY g_skew ORDER BY c DESC LIMIT 10",
            ];

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TestContext.Progress.WriteLine($"--- coordinator node {node.Index} ---");

                foreach (string sql in queries)
                    await MeasureAsync(cluster, node, db, sql);
            }

            // A/B of the span-local top k on the same data and placement: "off" removes the top-k field
            // from each remote request (as a peer that ignores it), "on" keeps it. The runs alternate, so
            // drift in machine load hits both arms alike. Local spans apply the top k in both arms.
            string[] topKQueries =
            [
                "SELECT g_skew, MAX(x) AS m FROM orders GROUP BY g_skew ORDER BY m DESC LIMIT 10",
                "SELECT g_flat, MAX(x) AS m FROM orders GROUP BY g_flat ORDER BY m DESC LIMIT 10",
            ];

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                foreach (string sql in topKQueries)
                    await CompareTopKAsync(cluster, node, db, sql);
            }

            long gathers = 0;
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> stats = await RunSql(node, db,
                    "SHOW ENGINE STATS LIKE 'distributed.partial_aggregate_gathers'");
                gathers += stats.Sum(r => r.Row["count"].LongValue);
            }

            TestContext.Progress.WriteLine($"partial_aggregate_gathers (fleet): {gathers}");
            Assert.Greater(gathers, 0L, "The aggregates must run through the partial-aggregate gather");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    private static async Task MeasureAsync(InProcessSchemaCluster cluster, InProcessSchemaCluster.Node node, string db, string sql)
    {
        // One warm-up run, then the measured runs.
        await RunSql(node, db, sql);

        InProcessSchemaCluster.RecordingFragmentTransport transport = cluster.FragmentTransport!;
        double[] millis = new double[Runs];
        long fragments = 0, rows = 0, bytes = 0;
        int resultRows = 0;

        for (int i = 0; i < Runs; i++)
        {
            transport.ResetExecuted();

            Stopwatch watch = Stopwatch.StartNew();
            List<QueryResultRow> result = await RunSql(node, db, sql);
            millis[i] = watch.Elapsed.TotalMilliseconds;

            fragments = transport.ExecutedCount;
            rows = transport.RowsReturned;
            bytes = transport.CellBytesReturned;
            resultRows = result.Count;
        }

        Array.Sort(millis);

        TestContext.Progress.WriteLine(
            $"p50 {millis[Runs / 2],8:F1} ms  min {millis[0],8:F1} ms  remote fragments {fragments}  " +
            $"partial rows shipped {rows,6}  cell chars {bytes,8}  result rows {resultRows,5}  | {sql}");
    }

    private static async Task CompareTopKAsync(InProcessSchemaCluster cluster, InProcessSchemaCluster.Node node, string db, string sql)
    {
        InProcessSchemaCluster.RecordingFragmentTransport transport = cluster.FragmentTransport!;
        const int Pairs = 15;
        double[] off = new double[Pairs], on = new double[Pairs];
        long offRows = 0, onRows = 0;

        await RunSql(node, db, sql);

        try
        {
            for (int i = 0; i < Pairs; i++)
            {
                transport.IgnoreTopK = true;
                transport.ResetExecuted();
                Stopwatch watch = Stopwatch.StartNew();
                await RunSql(node, db, sql);
                off[i] = watch.Elapsed.TotalMilliseconds;
                offRows = transport.RowsReturned;

                transport.IgnoreTopK = false;
                transport.ResetExecuted();
                watch.Restart();
                await RunSql(node, db, sql);
                on[i] = watch.Elapsed.TotalMilliseconds;
                onRows = transport.RowsReturned;
            }
        }
        finally
        {
            transport.IgnoreTopK = false;
        }

        Array.Sort(off);
        Array.Sort(on);

        TestContext.Progress.WriteLine(
            $"node {node.Index}  top-k off p50 {off[Pairs / 2],7:F1} ms ({offRows,6} rows)  on p50 {on[Pairs / 2],7:F1} ms ({onRows,4} rows)  | {sql}");
    }

    /// <summary>
    /// Inserts <see cref="RowCount"/> rows in batches. <c>g_skew</c> follows a power law: the value
    /// <c>Groups * u^4</c> for a uniform <c>u</c> puts most rows in the low group numbers. <c>g_flat</c>
    /// cycles through every group, so each group holds the same count. The seed is fixed.
    /// </summary>
    private static async Task<List<string>> LoadAsync(InProcessSchemaCluster.Node writer, string db)
    {
        Random random = new(42);
        List<string> ids = new(RowCount);

        for (int start = 0; start < RowCount; start += BatchSize)
        {
            List<Dictionary<string, ColumnValue>> batch = new(BatchSize);

            for (int i = start; i < Math.Min(start + BatchSize, RowCount); i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);

                long skew = (long)(Groups * Math.Pow(random.NextDouble(), 4));

                batch.Add(new()
                {
                    { "id", new(ColumnType.Id, id) },
                    { "g_skew", new(ColumnType.Integer64, skew) },
                    { "g_flat", new(ColumnType.Integer64, (long)(i % Groups)) },
                    { "g_low", new(ColumnType.Integer64, (long)(i % 10)) },
                    { "x", new(ColumnType.Integer64, (long)random.Next(1_000_000)) },
                });
            }

            KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
            await writer.Executor.Insert(new InsertTicket(txnState: tx, databaseName: db, tableName: "orders", values: batch));
            await writer.Database.Transactions.CommitAsync(tx);
        }

        return ids;
    }

    /// <summary>Splits the row key space at the 1/Spans quantiles of the inserted ids, and waits until every
    /// node's placement reports the spans. Only the meta-partition leader commits a split.</summary>
    private static async Task<int> SplitIntoSpansAsync(InProcessSchemaCluster cluster, List<string> ids)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["orders"];
        string keySpace = table.Store.RowKeySpace;
        List<string> sorted = ids.OrderBy(x => x, StringComparer.Ordinal).ToList();

        for (int s = 1; s < Spans; s++)
        {
            string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[RowCount * s / Spans]));
            bool committed = false;

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                if (response.Success)
                {
                    committed = true;
                    break;
                }
            }

            Assert.IsTrue(committed, $"Split {s} must be committed by the meta-partition leader");
        }

        int spans = 0;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;

            for (int attempt = 0; attempt < 100; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count >= Spans)
                    break;

                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count >= Spans,
                $"Node {node.Index} placement must report {Spans} spans");

            TestContext.Progress.WriteLine($"node {node.Index}: " + string.Join(", ",
                placement.Spans.Select(sp => sp.LeaderIsLocal ? "local" : "remote")));
            spans = placement.Spans.Count;
        }

        return spans;
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
