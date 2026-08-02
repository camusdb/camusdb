
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Multi-node data-path coverage for key-range sharding (the CamusDB side of the
/// auto-seed / propagate-to-all-nodes path).
///
/// <para>
/// Kahuna proves seed forwarding and routing-mode propagation at the unit level
/// (<c>TestRegisterKeyRangeForwarding</c>): a follower that registers a space forwards the
/// initial range descriptor to the meta-partition (partition 0) leader, which commits it and
/// replicates it so every node flips its local hash→range routing mode. These tests prove the
/// same end-to-end <b>through CamusDB</b> across a real 3-node in-process cluster:
/// </para>
///
/// <list type="bullet">
///   <item>First touch of a table on a node that does <b>not</b> lead the range-map meta
///   partition must seed via the leader and <b>not</b> throw "No range descriptor covers key"
///   on the first write.</item>
///   <item>Rows written through one node are visible to a full-table scan issued from any other
///   node — the seed descriptor and routing mode reached every node.</item>
/// </list>
///
/// <para>Sharding is opt-in (<c>CAMUS_KEY_RANGE_SHARDING</c>), so these tests flip
/// <see cref="CamusDBOptions.KeyRangeShardingEnabled"/> on the cluster they start and restore it on
/// teardown. <c>partitions: 2</c> gives a real data-partition pool ([1, 2]) distinct from the
/// reserved meta partition 0.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing, which shows up as spurious leadership churn.
[NonParallelizable]
public sealed class TestKeyRangeShardingCluster
{
    private const int Partitions = 2;

    // Partition 0 is Kahuna's reserved range-map meta partition (RangeMapStore.MetaPartitionId);
    // its leader is the only node that can commit a seed descriptor.
    private const int MetaPartitionId = 0;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();




    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task<(InProcessSchemaCluster cluster, string db)> SetupClusterWithTableAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id",    ColumnType.Id),
                new ColumnInfo("name",  ColumnType.String, notNull: true),
                new ColumnInfo("value", ColumnType.Integer64),
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

    /// <summary>
    /// Returns a node that does not currently lead the range-map meta partition (partition 0),
    /// so that the first table touch on it must forward the seed to the leader.
    /// </summary>
    private static async Task<InProcessSchemaCluster.Node> FindNonMetaLeaderNodeAsync(InProcessSchemaCluster cluster)
    {
        await cluster.Nodes[0].Kahuna.Raft.WaitForLeader(MetaPartitionId, CancellationToken.None);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (!await node.Kahuna.Raft.AmILeader(MetaPartitionId, CancellationToken.None))
                return node;
        }

        throw new AssertionException("Every node claims meta-partition leadership; cannot pick a non-leader");
    }

    private static async Task InsertRowsAsync(string db, InProcessSchemaCluster.Node node, int count)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            await node.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "robots",
                values: new() { new() {
                    { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "name",  new(ColumnType.String,    $"robot-{i}") },
                    { "value", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        await node.Database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ScanCountAsync(string db, InProcessSchemaCluster.Node node)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.Query(new QueryTicket(
                txnState:     tx,
                databaseName: db,
                tableName:    "robots",
                index:        null,
                projection:   null,
                where:        null,
                filters:      null,
                orderBy:      null,
                limit:        null,
                offset:       null,
                parameters:   null
            ));

            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;

            await node.Database.Transactions.CommitAsync(tx);
            return count;
        }
        catch
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    // -----------------------------------------------------------------------
    // 1. First touch on a non-meta-leader node must seed via the leader and not
    //    throw "No range descriptor covers key" on the first write; rows are then
    //    visible from another node.
    // -----------------------------------------------------------------------

    [Test]
    public async Task FirstTouchOnNonMetaLeader_InsertSeedsViaLeader_AndScanFromAnotherNodeSeesRows()
    {
        (InProcessSchemaCluster cluster, string db) = await SetupClusterWithTableAsync();
        await using InProcessSchemaCluster scope = cluster;

        InProcessSchemaCluster.Node nonMetaLeader = await FindNonMetaLeaderNodeAsync(cluster);

        // First touch of "robots" on a non-meta-leader node. With key-range sharding on this
        // triggers RegisterKeyRangeAsync, which must forward the seed to the partition-0 leader
        // and wait for the descriptor to replicate back — otherwise the first write would throw
        // "No range descriptor covers key".
        Assert.DoesNotThrowAsync(() => InsertRowsAsync(db, nonMetaLeader, count: 10),
            "First write from a non-meta-leader node must seed via the leader, not fail unseeded");

        // Scan from a different node — the seed descriptor and routing mode must have propagated.
        InProcessSchemaCluster.Node other = cluster.Nodes.First(n => n.Index != nonMetaLeader.Index);
        int count = await ScanCountAsync(db, other);

        Assert.AreEqual(10, count,
            "All rows written through the non-meta-leader node must be visible from another node");
    }

    // -----------------------------------------------------------------------
    // 2. Rows written through one node are visible to a full-table scan from every
    //    node — the routing mode reached all three nodes.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowsWrittenOnOneNode_AreVisibleFromAllNodes()
    {
        (InProcessSchemaCluster cluster, string db) = await SetupClusterWithTableAsync();
        await using InProcessSchemaCluster scope = cluster;

        await InsertRowsAsync(db, cluster.Nodes[0], count: 8);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            int count = await ScanCountAsync(db, node);
            Assert.AreEqual(8, count,
                $"Node {node.Index} must see all 8 rows under key-range sharding (routing mode propagated)");
        }
    }
}
