/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Cluster-mode coverage for <see cref="EmbeddedKahuna.GetPlacement"/>: the placement snapshot
/// must agree with real routing (hash partition id identical on every node), report exactly one
/// leader per span across the cluster, and flip to descriptor-backed spans once a key space is
/// key-range seeded. Placement is advisory, so these tests allow leadership beliefs a settling
/// window but never accept a wrong partition id — routing math has no tolerance.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterPlacement
{
    private const int Partitions = 2;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db)> SetupAsync(bool sharding)
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = sharding });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id",   ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);
        return (cluster, db);
    }

    private static async Task InsertOneRowAsync(string db, InProcessSchemaCluster.Node node)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        await node.Executor.Insert(new InsertTicket(
            txnState: tx, databaseName: db, tableName: "robots",
            values: new() { new() {
                { "id",   new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                { "name", new(ColumnType.String, "r1") },
            }}));
        await node.Database.Transactions.CommitAsync(tx);
    }

    [Test]
    public async Task HashPlacement_SameSingleSpanOnEveryNode_ExactlyOneLeader()
    {
        (InProcessSchemaCluster cluster, string db) = await SetupAsync(sharding: false);

        try
        {
            await InsertOneRowAsync(db, cluster.Nodes[0]);

            TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["robots"];
            string keySpace = table.Store.RowKeySpace;

            // Every node must compute the same single hash span with the same partition id —
            // this is routing math, no settling window allowed.
            TablePlacement first = cluster.Nodes[0].Kahuna.GetPlacement(keySpace);
            Assert.AreEqual(1, first.Spans.Count, "Hash key space resolves to one span");
            Assert.IsFalse(first.IsKeyRange);
            int partitionId = first.Spans[0].PartitionId;
            Assert.That(partitionId, Is.InRange(1, Partitions),
                "Hash partition must be in the user data pool [1, partitions]");

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TablePlacement placement = node.Kahuna.GetPlacement(keySpace);
                Assert.AreEqual(partitionId, placement.Spans[0].PartitionId,
                    "All nodes must route the bucket to the same partition");
                Assert.IsTrue(placement.Spans[0].HostedLocally,
                    "Legacy full replication: every node hosts every partition");
            }

            // Exactly one node should believe itself leader of the span. Leadership belief is
            // gossip/heartbeat-fed, so allow a settling window before judging.
            await cluster.Nodes[0].Kahuna.Raft.WaitForLeader(partitionId, CancellationToken.None);

            int leaders = 0;
            for (int attempt = 0; attempt < 25; attempt++)
            {
                leaders = 0;
                foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                {
                    node.Kahuna.InvalidatePlacement(keySpace);
                    TablePlacement placement = node.Kahuna.GetPlacement(keySpace);
                    if (placement.Spans[0].LeaderIsLocal)
                    {
                        leaders++;
                        Assert.AreEqual(0.0, placement.RemoteLeaderFraction,
                            "The leader node sees its single span as local");
                    }
                }

                if (leaders == 1)
                    break;

                await Task.Delay(200);
            }

            Assert.AreEqual(1, leaders, "Exactly one node believes it leads the span");
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task KeyRangePlacement_AfterSeed_ReportsDescriptorSpans()
    {
        (InProcessSchemaCluster cluster, string db) = await SetupAsync(sharding: true);

        try
        {
            // First write seeds the range descriptor via the meta-partition leader and
            // propagates routing mode to every node.
            await InsertOneRowAsync(db, cluster.Nodes[0]);

            TableDescriptor table = await cluster.Nodes[0].Database!.TableDescriptors["robots"];
            string keySpace = table.Store.RowKeySpace;

            // Descriptor propagation is replicated but applied per node; allow a settling
            // window until every node's placement reports key-range spans.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                TablePlacement? placement = null;

                for (int attempt = 0; attempt < 50; attempt++)
                {
                    node.Kahuna.InvalidatePlacement(keySpace);
                    placement = node.Kahuna.GetPlacement(keySpace);
                    if (placement.IsKeyRange)
                        break;

                    await Task.Delay(200);
                }

                Assert.IsNotNull(placement);
                Assert.IsTrue(placement!.IsKeyRange,
                    "After seeding, every node's placement must be descriptor-backed");
                Assert.GreaterOrEqual(placement.Spans.Count, 1);
                Assert.IsTrue(placement.Spans.All(s => s.PartitionId >= 1),
                    "Descriptor spans must reference user data partitions");
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
