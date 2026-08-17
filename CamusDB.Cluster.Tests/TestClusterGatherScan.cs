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
/// End-to-end coverage for span-fragmented (Gather) scans: with distribution and key-range
/// sharding on, a full scan over a table whose keyspace has been split into two spans must
/// plan a Gather (visible in EXPLAIN), return exactly the sequential result — every row once,
/// in ascending row-id order — from every node, and respect LIMIT identically. The split is
/// forced explicitly at a real row key so the placement genuinely has two spans; auto-split
/// thresholds are irrelevant to the test.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterGatherScan
{
    private const int Partitions = 2;
    private const int RowCount = 200;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> insertedIds)> SetupAsync()
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
                new ColumnInfo("id",   ColumnType.Id),
                new ColumnInfo("num",  ColumnType.Integer64, notNull: true),
                new ColumnInfo("ratio", ColumnType.Float64, notNull: true),
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

        List<string> ids = new(RowCount);
        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",  new(ColumnType.Id,        id) },
                    { "num", new(ColumnType.Integer64, (long)i) },
                    { "ratio", new(ColumnType.Float64, i * 0.1) },
                    { "bucket", new(ColumnType.Integer64, (long)(i % 4)) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, ids);
    }

    /// <summary>
    /// Splits the table's row keyspace at the median inserted row and waits until every node's
    /// placement reports two spans. The split must be committed by the range-map meta-partition
    /// (partition 0) leader; other nodes refuse without side effects.
    /// </summary>
    private static async Task SplitAtMedianAsync(
        InProcessSchemaCluster cluster, string db, List<string> insertedIds)
    {
        TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["readings"];
        string keySpace = table.Store.RowKeySpace;

        List<string> sorted = insertedIds.OrderBy(x => x, StringComparer.Ordinal).ToList();
        string splitKey = table.Store.RowPointKey(ObjectId.ToValue(sorted[RowCount / 2]));

        KahunaSplitRangeResponse? split = null;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            KahunaSplitRangeResponse response =
                await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

            if (response.Success)
            {
                split = response;
                break;
            }
        }

        Assert.IsNotNull(split, "One node (the meta-partition leader) must commit the split");

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

    private static async Task<List<QueryResultRow>> RunSql(
        InProcessSchemaCluster.Node node, string db, string sql)
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

    [Test]
    public async Task GatherScan_AfterSplit_EveryNodeReturnsAllRowsInOrder_AndExplainShowsGather()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, db, ids);

            HashSet<string> expected = ids.ToHashSet();
            List<string>? baselineSequence = null;

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> rows = await RunSql(node, db, "SELECT id, num FROM readings");

                Assert.AreEqual(RowCount, rows.Count,
                    $"Node {node.Index}: gather scan must return every row exactly once");

                for (int i = 1; i < rows.Count; i++)
                {
                    Assert.Less(
                        string.CompareOrdinal(rows[i - 1].RowId.ToString(), rows[i].RowId.ToString()), 0,
                        $"Node {node.Index}: rows must arrive in ascending row-id order across spans");
                }

                List<string> idColumn = rows.Select(r => r.Row["id"].StrValue!).ToList();
                Assert.AreEqual(RowCount, idColumn.ToHashSet().Count,
                    $"Node {node.Index}: no duplicated logical rows");
                Assert.IsTrue(idColumn.All(expected.Contains),
                    $"Node {node.Index}: gather scan must return exactly the inserted rows");

                // Every node must emit the identical sequence (spans concat deterministically).
                baselineSequence ??= idColumn;
                Assert.AreEqual(baselineSequence, idColumn,
                    $"Node {node.Index}: row sequence must be identical on every node");
            }

            // LIMIT must be a strict prefix of the full-scan sequence (span-order concat).
            List<QueryResultRow> limited = await RunSql(cluster.Nodes[1], db, "SELECT id FROM readings LIMIT 120");
            Assert.AreEqual(120, limited.Count);
            Assert.AreEqual(baselineSequence!.Take(120), limited.Select(r => r.Row["id"].StrValue!),
                "LIMIT must return the first 120 rows of the full-scan sequence, crossing the span boundary");

            // The plan is fragmented and EXPLAIN says so.
            List<QueryResultRow> explain = await RunSql(cluster.Nodes[0], db, "EXPLAIN SELECT id, num FROM readings");
            bool hasGather = explain.Any(r => r.Row.Values.Any(v => v.StrValue?.Contains("gather") == true)
                && r.Row.Values.Any(v => v.StrValue?.Contains("spans: 2") == true));
            Assert.IsTrue(hasGather, "EXPLAIN must render the gather exchange with its span count. Got: "
                + string.Join(" | ", explain.Select(r => string.Join(",", r.Row.Values.Select(v => v.StrValue)))));

            // EXPLAIN ANALYZE runs the real gather (consumer-side stats are single-threaded)
            // and reports the actual scanned row count on the scan leaf.
            List<QueryResultRow> analyze = await RunSql(cluster.Nodes[0], db,
                "EXPLAIN (ANALYZE) SELECT id, num FROM readings");
            Assert.IsTrue(
                analyze.Any(r => r.Row.Values.Any(v => v.StrValue?.Contains("gather") == true)),
                "EXPLAIN ANALYZE must render the gather node");
            Assert.IsTrue(
                analyze.Any(r => r.Row.TryGetValue("actual_rows", out ColumnValue? ar) && ar.Type == ColumnType.Integer64 && ar.LongValue == RowCount),
                "EXPLAIN ANALYZE must report the full scanned row count through the gather. Got: "
                + string.Join(" | ", analyze.Select(r => string.Join(",", r.Row.Select(kv => kv.Key + "=" + (kv.Value.StrValue ?? kv.Value.LongValue.ToString()))))));

            // Residual filter through the gather stays correct.
            List<QueryResultRow> filtered = await RunSql(cluster.Nodes[2], db,
                "SELECT id, num FROM readings WHERE num >= 50 AND num < 150");
            Assert.AreEqual(100, filtered.Count, "Residual filter must keep exactly the matching rows");

            // ── Remote fragment dispatch ────────────────────────────────────────────────
            // Wait until every span has a known leader endpoint from every node's viewpoint,
            // so the remote-vs-local dispatch decision is deterministic; then a filtered scan
            // from a node that does not lead a span must ship that span's filter to the
            // leader (the shippable-filter pushdown path).
            TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["readings"];
            string keySpace = table.Store.RowKeySpace;

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                for (int attempt = 0; attempt < 50; attempt++)
                {
                    node.Kahuna.InvalidatePlacement(keySpace);
                    if (node.Kahuna.GetPlacement(keySpace).Spans.All(s => s.LeaderEndpoint is not null))
                        break;
                    await Task.Delay(200);
                }
            }

            cluster.FragmentTransport!.ResetExecuted();

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> remoteFiltered = await RunSql(node, db,
                    "SELECT id, num FROM readings WHERE num >= 50 AND num < 150");
                Assert.AreEqual(100, remoteFiltered.Count,
                    $"Node {node.Index}: filtered gather must be correct with remote fragments engaged");
            }

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0,
                "At least one span must have executed as a remote fragment on its leader " +
                "(every node cannot lead every span)");

            // ── Survivor-capped LIMIT pushdown ──────────────────────────────────────────
            // A filtered LIMIT stops each remote span after limit survivors: correct prefix
            // results, and the transport ships at most spans × limit rows instead of every
            // match. The reference is the uncapped filtered result from the same node.
            List<QueryResultRow> allMatches = await RunSql(cluster.Nodes[0], db,
                "SELECT id, num FROM readings WHERE num >= 50 AND num < 150");

            cluster.FragmentTransport.ResetExecuted();

            List<QueryResultRow> limitedFiltered = await RunSql(cluster.Nodes[0], db,
                "SELECT id, num FROM readings WHERE num >= 50 AND num < 150 LIMIT 30");

            Assert.AreEqual(30, limitedFiltered.Count);
            Assert.AreEqual(
                allMatches.Take(30).Select(r => r.Row["id"].StrValue!),
                limitedFiltered.Select(r => r.Row["id"].StrValue!),
                "Filtered LIMIT must be a strict prefix of the filtered scan");

            if (cluster.FragmentTransport.ExecutedCount > 0)
            {
                Assert.LessOrEqual(cluster.FragmentTransport.RowsReturned,
                    30L * cluster.FragmentTransport.ExecutedCount,
                    "Each remote fragment must stop after at most limit survivors");
            }

            // ── Partial aggregation below the gather ────────────────────────────────────
            // A decomposable aliased aggregate ships ONE partial row per remote span instead
            // of the span's rows, and the merged result is exact. num spans 0..199, so the
            // expected values are closed-form.
            cluster.FragmentTransport.ResetExecuted();

            List<QueryResultRow> countRows = await RunSql(cluster.Nodes[0], db,
                "SELECT COUNT(*) AS c FROM readings");
            Assert.AreEqual(1, countRows.Count);
            Assert.AreEqual(200L, countRows[0].Row["c"].LongValue, "Partial COUNT must merge exactly");

            List<QueryResultRow> sumRows = await RunSql(cluster.Nodes[1], db,
                "SELECT SUM(num) AS s FROM readings WHERE num >= 50 AND num < 150");
            Assert.AreEqual(1, sumRows.Count);
            Assert.AreEqual(9950L, sumRows[0].Row["s"].LongValue,
                "Partial integer SUM must merge exactly (sum of 50..149)");

            List<QueryResultRow> maxRows = await RunSql(cluster.Nodes[2], db,
                "SELECT MAX(num) AS m FROM readings WHERE num < 77");
            Assert.AreEqual(1, maxRows.Count);
            Assert.AreEqual(76L, maxRows[0].Row["m"].LongValue, "Partial MAX must merge exactly");

            // Float SUM: partial-merge reordering may legally shift the last bits versus the
            // sequential engine (double addition is not associative) — the accepted contract
            // is tolerance, not bit-equality.
            List<QueryResultRow> floatSum = await RunSql(cluster.Nodes[0], db,
                "SELECT SUM(ratio) AS s FROM readings WHERE num >= 50 AND num < 150");
            Assert.AreEqual(1, floatSum.Count);
            Assert.AreEqual(995.0, floatSum[0].Row["s"].FloatValue, 1e-9,
                "Partial float SUM must merge within tolerance (sum of 5.0..14.9)");

            if (cluster.FragmentTransport.ExecutedCount > 0)
            {
                Assert.LessOrEqual(cluster.FragmentTransport.RowsReturned,
                    (long)cluster.FragmentTransport.ExecutedCount,
                    "Each aggregate fragment ships exactly one partial row, never span rows");
            }

            // ── Grouped partial aggregation ─────────────────────────────────────────────
            // bucket = num % 4: 4 groups of 50 rows. Partial rows per fragment ≤ group count,
            // so a grouped aggregate over a remote span ships at most 4 rows, not 100.
            cluster.FragmentTransport.ResetExecuted();

            List<QueryResultRow> grouped = await RunSql(cluster.Nodes[0], db,
                "SELECT bucket, COUNT(*) AS c, SUM(num) AS s FROM readings GROUP BY bucket");
            Assert.AreEqual(4, grouped.Count, "Four buckets expected");

            foreach (QueryResultRow group in grouped)
            {
                long bucket = group.Row["bucket"].LongValue;
                Assert.AreEqual(50L, group.Row["c"].LongValue, $"bucket {bucket}: COUNT");
                Assert.AreEqual(4900L + 50L * bucket, group.Row["s"].LongValue,
                    $"bucket {bucket}: SUM of 4k+{bucket} for k=0..49");
            }

            if (cluster.FragmentTransport.ExecutedCount > 0)
            {
                Assert.LessOrEqual(cluster.FragmentTransport.RowsReturned,
                    4L * cluster.FragmentTransport.ExecutedCount,
                    "Each grouped-aggregate fragment ships at most one partial row per group");
            }

            // Grouped AVG decomposes into a SUM/COUNT pair finalized after the merge; the
            // averages are exact rationals here, but assert with tolerance per the accepted
            // float contract.
            List<QueryResultRow> groupedAvg = await RunSql(cluster.Nodes[1], db,
                "SELECT bucket, AVG(num) AS a FROM readings GROUP BY bucket");
            Assert.AreEqual(4, groupedAvg.Count);

            foreach (QueryResultRow group in groupedAvg)
            {
                long bucket = group.Row["bucket"].LongValue;
                Assert.AreEqual(98.0 + bucket, group.Row["a"].FloatValue, 1e-9,
                    $"bucket {bucket}: AVG must be 98+{bucket}");
            }

            // Aggregates stay exact when every remote partial fails and spans recompute locally.
            cluster.FragmentTransport.FailNextFragments(12);

            List<QueryResultRow> countFallback = await RunSql(cluster.Nodes[0], db,
                "SELECT COUNT(*) AS c FROM readings");
            Assert.AreEqual(200L, countFallback[0].Row["c"].LongValue,
                "Partial COUNT must stay exact under remote fragment failure");

            cluster.FragmentTransport.FailNextFragments(12);

            List<QueryResultRow> groupedFallback = await RunSql(cluster.Nodes[2], db,
                "SELECT bucket, SUM(num) AS s FROM readings GROUP BY bucket");
            Assert.AreEqual(4, groupedFallback.Count);
            Assert.IsTrue(groupedFallback.All(g =>
                    g.Row["s"].LongValue == 4900L + 50L * g.Row["bucket"].LongValue),
                "Grouped partial SUM must stay exact under remote fragment failure");

            // ── Fallback under transport failure ────────────────────────────────────────
            // Every remote attempt fails before the first row; the coordinator must resume
            // each failed span locally and still produce the exact result.
            cluster.FragmentTransport.FailNextFragments(12);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> fallbackFiltered = await RunSql(node, db,
                    "SELECT id, num FROM readings WHERE num >= 50 AND num < 150");
                Assert.AreEqual(100, fallbackFiltered.Count,
                    $"Node {node.Index}: results must be exact when remote fragments fail and spans fall back to local scans");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
