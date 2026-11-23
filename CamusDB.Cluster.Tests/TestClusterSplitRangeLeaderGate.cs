/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using Kommander.System;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The manual range split of the cluster API on three nodes: a node that does not lead the partition
/// owning the range map answers <c>NotLeader</c> before it scans the key space and names the leader,
/// and the leader commits the split.
///
/// <para>A fault-injection harness places a split inside a window of a few seconds and follows the
/// leader hint. A node that scanned the whole key space for a split key before it answered
/// <c>NotLeader</c> spent seconds of that window on an answer that needs no key.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterSplitRangeLeaderGate
{
    private const int Partitions = 2;
    private const int RowCount = 60;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    [Test]
    public async Task SplitRange_OnANodeThatDoesNotLeadTheRangeMap_AnswersNotLeaderWithoutAScan_AndTheLeaderSplits()
    {
        CamusDBOptions options = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };

        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: Partitions, loggerFactory: sharedLoggerFactory, logger: logger, options: options);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id",  ColumnType.Id),
                new ColumnInfo("num", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < RowCount; i++)
        {
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",  new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "num", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        await writer.Database.Transactions.CommitAsync(tx);

        InProcessSchemaCluster.Node rangeMapLeader = await RangeMapLeaderAsync(cluster);
        InProcessSchemaCluster.Node other = cluster.Nodes.First(node => node != rangeMapLeader);

        // A node applies the range map of a new table, and learns who leads, a moment after the table exists.
        ClusterSplitRangeResponse refused = await EventuallyAsync(
            "the other node to see the row space and name the range map leader",
            () => SplitAsync(other, options, db),
            answer => answer.Status != "NoRange" && answer.LeaderHint is not null);

        Assert.That(refused.Success, Is.False);
        Assert.That(refused.Status, Is.EqualTo("NotLeader"), refused.Reason);
        Assert.That(refused.Determinate, Is.True);
        Assert.That(refused.KeysSeen, Is.EqualTo(0), "a node that cannot run the split did not scan the key space");
        Assert.That(refused.SplitKey, Is.Null);
        Assert.That(refused.LeaderHint, Is.EqualTo(rangeMapLeader.Kahuna.Raft.GetLocalEndpoint()));

        ClusterSplitRangeResponse committed = await EventuallyAsync(
            "the range map leader to commit the split",
            () => SplitAsync(rangeMapLeader, options, db),
            answer => answer.Success);

        Assert.That(committed.KeysSeen, Is.EqualTo(RowCount));
        Assert.That(committed.SplitKey, Is.Not.Null);
        Assert.That(committed.NewPartitionId, Is.GreaterThan(0));
    }

    /// <summary>One call of <c>POST /v1/cluster/split-range</c> for the row space of <c>readings</c> on <paramref name="node"/>.</summary>
    private static async Task<ClusterSplitRangeResponse> SplitAsync(InProcessSchemaCluster.Node node, CamusDBOptions options, string db)
    {
        DefaultHttpContext http = new();
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = IPAddress.Loopback;
        http.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes($"{{\"database\":\"{db}\",\"table\":\"readings\"}}"));

        ClusterController controller = new(node.Executor, new HttpTransactionCoordinator(node.Executor), logger, options, node.Kahuna)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        JsonResult result = await controller.SplitRange();
        return (ClusterSplitRangeResponse)result.Value!;
    }

    /// <summary>The node that leads the partition owning the range map, once the cluster has elected one.</summary>
    private static async Task<InProcessSchemaCluster.Node> RangeMapLeaderAsync(InProcessSchemaCluster cluster)
    {
        long deadline = Environment.TickCount64 + 30_000;

        while (Environment.TickCount64 < deadline)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                if (await node.Kahuna.Raft.AmILeaderQuick(RaftSystemConfig.SystemPartition))
                    return node;
            }

            await Task.Delay(100);
        }

        throw new AssertionException("no node leads the partition that owns the range map");
    }

    /// <summary>Repeats <paramref name="ask"/> until its answer satisfies <paramref name="done"/>, for up to 30 seconds.</summary>
    private static async Task<ClusterSplitRangeResponse> EventuallyAsync(
        string what, Func<Task<ClusterSplitRangeResponse>> ask, Func<ClusterSplitRangeResponse, bool> done)
    {
        long deadline = Environment.TickCount64 + 30_000;
        ClusterSplitRangeResponse? last = null;

        while (Environment.TickCount64 < deadline)
        {
            last = await ask();

            if (done(last))
                return last;

            await Task.Delay(250);
        }

        throw new AssertionException($"Timed out waiting for {what}; last answer: {last?.Status}: {last?.Reason}");
    }
}
