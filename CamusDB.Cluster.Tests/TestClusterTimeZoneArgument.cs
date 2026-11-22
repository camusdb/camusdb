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
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.Communication.Rest;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The time zone argument of the date functions across nodes. The table's keyspace is split into two
/// spans, so a scan from any node runs as a gather with remote fragments, and a filter that takes a
/// zone is evaluated on the peer that leads each span. Every node must return the same rows and
/// groups, and they must match the zone rules computed here with the OS tzdata, a source independent
/// of the engine's. A fragment that takes a zone argument carries the coordinator's zone data
/// version, and a peer whose version differs refuses it; the coordinator then scans the span itself and
/// still returns the same answer. A fragment with no zone argument carries no version and still runs
/// on the peer.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterTimeZoneArgument
{
    private const int Partitions = 2;
    private const int RowCount = 200;
    private const string Zone = "America/New_York";

    /// <summary>The first row is 2026-10-31T20:00Z (16:00 EDT), and each next row is 13 minutes later, so
    /// the rows span the fall-back change of 2026-11-01 and its repeated hour.</summary>
    private static readonly DateTime FirstInstant = new(2026, 10, 31, 20, 0, 0, DateTimeKind.Utc);

    private static readonly TimeSpan Step = TimeSpan.FromMinutes(13);

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids)> SetupAsync()
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
                new ColumnInfo("ts",  ColumnType.DateTime, notNull: true),
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
                    { "ts",  new(ColumnType.DateTime,  (FirstInstant + Step * i).Ticks) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);
        return (cluster, db, ids);
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

    private static async Task<List<QueryResultRow>> QueryOn(InProcessSchemaCluster.Node node, string db, string sql)
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
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The row numbers whose local New York hour is 1, from the OS tzdata.</summary>
    private static long[] ExpectedHourOneRows()
    {
        TimeZoneInfo newYork = TimeZoneInfo.FindSystemTimeZoneById(Zone);

        return Enumerable.Range(0, RowCount)
            .Where(i => TimeZoneInfo.ConvertTimeFromUtc(FirstInstant + Step * i, newYork).Hour == 1)
            .Select(i => (long)i)
            .ToArray();
    }

    /// <summary>The count of rows per local New York day, keyed by the UTC instant of local midnight,
    /// from the OS tzdata.</summary>
    private static string[] ExpectedDayGroups()
    {
        TimeZoneInfo newYork = TimeZoneInfo.FindSystemTimeZoneById(Zone);

        return Enumerable.Range(0, RowCount)
            .Select(i => TimeZoneInfo.ConvertTimeFromUtc(FirstInstant + Step * i, newYork).Date)
            .GroupBy(localDay => localDay)
            .OrderBy(g => g.Key)
            .Select(g => TimeZoneInfo.ConvertTimeToUtc(g.Key, newYork).Ticks + ":" + g.Count())
            .ToArray();
    }

    /// <summary>A filter with no zone argument, which ships to the peers on every node.</summary>
    private static async Task AssertIntegerFilterOnEveryNode(InProcessSchemaCluster cluster, string db)
    {
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            List<QueryResultRow> rows = await QueryOn(node, db, "SELECT num FROM readings WHERE num >= 50 AND num < 150");
            Assert.AreEqual(Enumerable.Range(50, 100).Select(n => (long)n).ToArray(),
                rows.Select(r => r.Row["num"].LongValue).OrderBy(n => n).ToArray(),
                $"Node {node.Index}: rows of the integer filter");
        }
    }

    private static async Task AssertEveryNodeAgrees(InProcessSchemaCluster cluster, string db)
    {
        long[] expectedHourOne = ExpectedHourOneRows();
        string[] expectedDays = ExpectedDayGroups();

        // The repeated hour 01:00-02:00 holds 4 or 5 rows of each offset, so the filter must keep both.
        Assert.Greater(expectedHourOne.Length, 8, "the fixture must cover the repeated hour");
        Assert.AreEqual(3, expectedDays.Length, "the fixture must span three local days");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            List<QueryResultRow> hourOne = await QueryOn(node, db,
                $"SELECT num FROM readings WHERE date_part('hour', ts, '{Zone}') = 1");

            Assert.AreEqual(expectedHourOne, hourOne.Select(r => r.Row["num"].LongValue).OrderBy(n => n).ToArray(),
                $"Node {node.Index}: rows at local hour 1");

            List<QueryResultRow> days = await QueryOn(node, db,
                $"SELECT date_trunc('day', ts, '{Zone}') AS day, count(*) AS n FROM readings " +
                $"GROUP BY date_trunc('day', ts, '{Zone}') ORDER BY day");

            Assert.AreEqual(expectedDays, days.Select(r => r.Row["day"].LongValue + ":" + r.Row["n"].LongValue).ToArray(),
                $"Node {node.Index}: rows per local day");
        }
    }

    [Test]
    public async Task ZoneFilter_RunsOnRemoteFragments_AndEveryNodeGivesTheSameAnswer()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, ids);
            cluster.FragmentTransport!.ResetExecuted();

            await AssertEveryNodeAgrees(cluster, db);

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0,
                "At least one span must have run as a remote fragment, or the test proves nothing about a gather");
            Assert.Greater(cluster.FragmentTransport.RowsReturned, 0, "the peers served the zone filter");

            Assert.IsTrue(cluster.FragmentTransport.ExecutedRequests
                    .Where(r => r.FilterJson.Contains(Zone, StringComparison.Ordinal))
                    .Any(),
                "the zone filter must have been shipped to a peer");

            Assert.IsTrue(cluster.FragmentTransport.ExecutedRequests
                    .Where(r => r.FilterJson.Contains(Zone, StringComparison.Ordinal))
                    .All(r => r.TimeZoneDataVersion == TimeZoneRules.DataVersion),
                "a fragment that takes a zone argument carries the coordinator's zone data version");

            // A fragment with no zone argument carries no version.
            cluster.FragmentTransport.ResetExecuted();
            await AssertIntegerFilterOnEveryNode(cluster, db);

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "the integer filter ran on the peers");
            Assert.IsTrue(cluster.FragmentTransport.ExecutedRequests.All(r => r.TimeZoneDataVersion is null),
                "a fragment with no zone argument carries no zone data version");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task PeerWithOtherZoneData_RefusesOnlyZoneFragments_AndTheCoordinatorStillAnswers()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();

        try
        {
            await SplitAtMedianAsync(cluster, ids);
            cluster.FragmentTransport!.ResetExecuted();
            cluster.FragmentTransport.TimeZoneDataVersionOverride = "TZDB: 1999a (mapping: 0)";

            await AssertEveryNodeAgrees(cluster, db);

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "fragments were dispatched");
            Assert.AreEqual(0, cluster.FragmentTransport.RowsReturned,
                "every peer refused the zone fragments before their first row, so the coordinators scanned those spans themselves");

            // The same peers still serve a fragment that takes no zone argument.
            cluster.FragmentTransport.ResetExecuted();
            await AssertIntegerFilterOnEveryNode(cluster, db);

            Assert.Greater(cluster.FragmentTransport.ExecutedCount, 0, "the integer filter was dispatched");
            Assert.Greater(cluster.FragmentTransport.RowsReturned, 0,
                "the peers served the integer filter although their zone data differs");
        }
        finally
        {
            cluster.FragmentTransport!.TimeZoneDataVersionOverride = null;
            await cluster.DisposeAsync();
        }
    }
}
