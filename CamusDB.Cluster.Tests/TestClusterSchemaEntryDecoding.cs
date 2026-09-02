/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// What a committed schema-log entry costs the nodes it reaches. A partition carries the traffic of
/// every database that hashes to it, and the node that proposed a change is delivered its own entry
/// twice — once through replication and once through the local apply that lets it observe the change
/// before the statement returns. These tests pin the resulting decode counts, which is the only way
/// to see that the frame-header skips are engaged: the schema converges either way.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterSchemaEntryDecoding
{
    [Test]
    public async Task DdlOnTheLeaderIsDecodedOncePerNode()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await cluster.WaitForSchemaLeaderNodeAsync(db);

        long[] before = DecodeCounts(cluster);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // One delta reached every node, so every node paid for exactly one decode. The proposer
        // received the entry a second time and recognized it from the frame; before the frame
        // existed it would show two.
        AssertDecodedOncePerDelta(cluster, before, deltas: 1);
    }

    [Test]
    public async Task DdlIssuedOnAFollowerIsDecodedOncePerNode()
    {
        // The forwarder is what lets a follower accept the statement and hand it to the leader.
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(node => node.Index != leader.Index);

        long[] before = DecodeCounts(cluster);

        // The follower forwards the statement to the leader, which proposes it. The follower is
        // therefore delivered the entry once and the leader twice.
        await follower.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        AssertDecodedOncePerDelta(cluster, before, deltas: 1);
    }

    [Test]
    public async Task ADatabaseSharingAPartitionDecodesNothing()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);

        (string changed, string bystander) = TwoDatabasesOnOneSchemaLogPartition(cluster);

        // The bystander is opened first and its descriptors are captured, because the fixture keeps
        // one current database per node and the changed database has to be the one it points at.
        await cluster.OpenDatabaseOnAllNodesAsync(bystander);
        DatabaseDescriptor[] bystanderDescriptors = [.. cluster.Nodes.Select(node => node.Database!)];

        await cluster.OpenDatabaseOnAllNodesAsync(changed);

        long[] before = [.. bystanderDescriptors.Select(descriptor => descriptor.SchemaEntriesDecoded)];

        await cluster.RunOnSchemaLeaderAsync(changed, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: changed,
            tableName: "robots",
            columns: [new ColumnInfo("id", ColumnType.Id)],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(changed, version: 1);

        // Every entry for the changed database was delivered to the bystander's subscriber too,
        // because both databases ride the same partition. The frame names the owning database, so
        // the bystander dropped each one without reading it.
        for (int i = 0; i < bystanderDescriptors.Length; i++)
            Assert.AreEqual(
                before[i],
                bystanderDescriptors[i].SchemaEntriesDecoded,
                $"node {i} deserialized an entry belonging to another database"
            );
    }

    [Test]
    public async Task ACorruptEntryFailsRatherThanBeingSkipped()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        string dbId = leader.Database!.Id;

        SchemaChangeLogEntry entry = new()
        {
            Database = dbId,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaCreateTablePayload
            {
                TableId = "000000000000000000000101",
                TableName = "robots",
                Columns = [new() { Id = "000000000000000000000102", Name = "id", Type = ColumnType.Id, NotNull = true }]
            })
        };

        // The header stays intact and the body is cut, so the entry still claims this database and a
        // version the node has not reached. It must reach the decode and surface there — a frame the
        // skip could not interpret must never be treated as "nothing to do".
        byte[] framed = SchemaChangeLogEntryCodec.Encode(entry);
        byte[] corrupt = framed.AsSpan(0, framed.Length - (framed.Length / 3)).ToArray();

        Assert.ThrowsAsync<CamusDBException>(() => leader.Kahuna.ReplicateSchemaChangeAsync(dbId, corrupt));

        Assert.AreEqual(0, leader.Database!.Schema.SchemaVersion);
    }

    private static long[] DecodeCounts(InProcessSchemaCluster cluster)
        => [.. cluster.Nodes.Select(node => node.Database!.SchemaEntriesDecoded)];

    private static void AssertDecodedOncePerDelta(InProcessSchemaCluster cluster, long[] before, long deltas)
    {
        for (int i = 0; i < cluster.Nodes.Length; i++)
            Assert.AreEqual(
                deltas,
                cluster.Nodes[i].Database!.SchemaEntriesDecoded - before[i],
                $"node {i} decoded the wrong number of entries"
            );
    }

    /// <summary>
    /// Two database names whose schema logs land on the same partition, so an entry for one is
    /// genuinely delivered to the other's subscriber. Picking names until the partitions collide is
    /// the only way to reach that path: the partition is derived from the name.
    /// </summary>
    private static (string, string) TwoDatabasesOnOneSchemaLogPartition(InProcessSchemaCluster cluster)
    {
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 5_000; i++)
        {
            string db = cluster.NextSchemaLogDatabaseName();
            int partition = cluster.Nodes[0].Kahuna.SchemaLogPartition(db);

            if (byPartition.TryGetValue(partition, out string? existing))
                return (existing, db);

            byPartition[partition] = db;
        }

        throw new AssertionException("Could not find two database names sharing a schema log partition");
    }
}
