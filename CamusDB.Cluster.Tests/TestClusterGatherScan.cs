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
