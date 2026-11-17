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
using Kommander;
using Kommander.System;
using CamusDB.App.Services;
using CamusDB.Core;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Coverage for <see cref="LeadershipHandoffService.HandOffAsync"/>: a node about to stop gives every
/// data partition it leads to a peer, so none of them waits out an election once the node is gone.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterLeadershipHandoff
{
    private const int Partitions = 3;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    [Test]
    public async Task HandOff_MovesEveryDataPartitionTheNodeLeadsToAPeer()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger);

        InProcessSchemaCluster.Node stopping = cluster.Nodes[0];
        List<int> led = [];
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            List<int> mine = await DataPartitionsLedAsync(node.Kahuna.Raft);
            if (mine.Count > led.Count)
                (stopping, led) = (node, mine);
        }
        Assert.That(led, Is.Not.Empty, "no node leads a data partition");

        DateTime started = DateTime.UtcNow;
        (int handedOff, int total) = await LeadershipHandoffService.HandOffAsync(
            stopping.Kahuna.Raft, TimeSpan.FromSeconds(15), logger, CancellationToken.None);
        TestContext.Progress.WriteLine(
            $"{stopping.Kahuna.Raft.GetLocalEndpoint()} led [{string.Join(", ", led)}]; handed off {handedOff} of {total} in {(DateTime.UtcNow - started).TotalMilliseconds:F0} ms");

        Assert.That(total, Is.EqualTo(led.Count), "did not try every partition the node leads");
        Assert.That(handedOff, Is.EqualTo(led.Count), "did not hand off every partition the node leads");

        foreach (int partitionId in led)
        {
            Assert.That(await stopping.Kahuna.Raft.AmILeaderQuick(partitionId), Is.False,
                $"the stopping node still leads partition {partitionId}");
            Assert.That(await PeerLeadsAsync(cluster, stopping, partitionId, TimeSpan.FromSeconds(10)), Is.True,
                $"no peer leads partition {partitionId}");
        }

        // A node that leads nothing has nothing to hand off.
        Assert.That(await LeadershipHandoffService.HandOffAsync(
            stopping.Kahuna.Raft, TimeSpan.FromSeconds(15), logger, CancellationToken.None), Is.EqualTo((0, 0)));
    }

    private static async Task<List<int>> DataPartitionsLedAsync(IRaft raft)
    {
        List<int> led = [];
        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            if (range.PartitionId == RaftSystemConfig.SystemPartition || !raft.HostsPartition(range.PartitionId))
                continue;

            if (await raft.AmILeaderQuick(range.PartitionId))
                led.Add(range.PartitionId);
        }
        return led;
    }

    private static async Task<bool> PeerLeadsAsync(
        InProcessSchemaCluster cluster, InProcessSchemaCluster.Node stopping, int partitionId, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                if (!ReferenceEquals(node, stopping) && await node.Kahuna.Raft.AmILeaderQuick(partitionId))
                    return true;
            }

            await Task.Delay(100);
        }
        return false;
    }
}
