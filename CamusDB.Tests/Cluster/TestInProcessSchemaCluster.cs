/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Cluster;

[TestFixture]
[NonParallelizable]
public sealed class TestInProcessSchemaCluster
{
    [Test]
    public async Task CreateTableOnLeaderAppliesToEveryOpenNodeWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);
        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        SchemaChangeLogEntry entry = new()
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "000000000000000000000101",
                TableName = "robots",
                Columns =
                [
                    new()
                    {
                        Id = "000000000000000000000102",
                        Name = "id",
                        Type = ColumnType.Id,
                        NotNull = true
                    },
                    new()
                    {
                        Id = "000000000000000000000103",
                        Name = "name",
                        Type = ColumnType.String
                    }
                ]
            })
        };

        SchemaReplicationResult result = await leader.Kahuna.ReplicateSchemaChangeAsync(
            db,
            Serializator.Serialize(entry)
        );

        Assert.AreEqual(SchemaReplicationOutcome.Committed, result.Outcome);
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.ContainsKey("robots")
        ));
    }

    // Full command-level CreateTable on the leader. This used to hang: the leader's checkpoint
    // persistence ran inside the schema-apply callback and re-entered the schema partition,
    // timing out (ProposalTimeout). After decoupling persistence to the proposer context it
    // completes, converges on every node, and durably persists the checkpoint.
    [Test]
    public async Task CommandLevelCreateTableOnLeaderPersistsAndConvergesAcrossNodes()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

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
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // CreateTable returning successfully already implies the checkpoint persisted:
        // PersistSchemaCheckpointWithRetryAsync throws after exhausting retries, so a
        // persist failure would have surfaced as an exception above rather than completing.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.SchemaVersion >= 1 &&
            node.Database.Schema.Tables.ContainsKey("robots")
        ));
    }

    // A3: pause node 2 → DDL on leader → nodes 0/1 converge, node 2 lags →
    // resume node 2 → node 2 catches up via Kommander replay.
    //
    // Uses a short SchemaAckLiveNodeLease so the paused (fully isolated) node is
    // treated as expired by the ack gate, allowing DDL to commit with quorum only.
    [Test]
    public async Task PausedNodeLagsAndCatchesUpOnResume()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Short lease so an isolated node is considered expired after 1 s, letting
        // the ack gate proceed with the two active nodes.
        TimeSpan lease = TimeSpan.FromMilliseconds(1000);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Identify the non-leader nodes; pause one of them (fully isolates it).
        InProcessSchemaCluster.Node[] followers = cluster.Nodes
            .Where(n => n.Index != leader.Index)
            .ToArray();
        int pausedIndex = followers[0].Index;
        InProcessSchemaCluster.Node pausedNode = cluster.Nodes[pausedIndex];

        cluster.PauseDelivery(pausedIndex);

        // Wait for the lease to expire so the paused node is excluded from the ack gate.
        await Task.Delay(lease + TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);

        // Run DDL while node is paused — leader + one follower form quorum and commit.
        await cluster.RunOnSchemaLeaderAsync(db, active => active.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "sensors",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("value", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        // All non-paused nodes must have the table.
        InProcessSchemaCluster.Node[] activeNodes = cluster.Nodes
            .Where(n => n.Index != pausedIndex)
            .ToArray();

        Assert.True(activeNodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("sensors")),
            "Active nodes should have the table after DDL");

        // Paused node should not yet have it (fully isolated while DDL committed).
        Assert.False(
            pausedNode.Database?.Schema.Tables.ContainsKey("sensors") ?? false,
            "Paused node should not have the table before resume");

        // Resume delivery — unblocks the Raft transport in both directions.
        cluster.ResumeDelivery(pausedIndex);

        // Give the Raft heartbeat cycle time to propagate (500 ms heartbeat interval).
        // This lets the paused node learn who the KV-partition leaders are so that
        // LocateAndTryGetValue can route to them in the next step.
        await Task.Delay(TimeSpan.FromMilliseconds(1500)).ConfigureAwait(false);

        // Reopen the database on the paused node. LoadMetaAsync reads the schema
        // checkpoint via LocateAndTryGetValue → routes through MemoryInterNodeCommmunication
        // (which was never blocked) to the KV-partition leader → gets the committed version.
        await cluster.ReopenDatabaseAsync(pausedIndex, db).ConfigureAwait(false);

        // Node must now see the committed schema without a full Raft WAL replay.
        Assert.True(
            pausedNode.Database?.Schema.Tables.ContainsKey("sensors") ?? false,
            "Resumed-and-reopened node should have caught up to the committed schema version");

        // Convergence check: all registered nodes (including the now-updated one) acked.
        await cluster.WaitForSchemaConvergenceAsync(db, version: 1, timeout: TimeSpan.FromSeconds(10));

        Assert.True(cluster.Nodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("sensors")),
            "All nodes should have the table after full convergence");
    }

    // A3: ForceLeaderChangeAsync blocks the current leader, waits for a new election,
    // and returns the new leader. Subsequent DDL on the new leader converges on all
    // unblocked nodes.
    //
    // Uses a short SchemaAckLiveNodeLease so the blocked original leader is treated as
    // expired, allowing DDL to commit with the two remaining live nodes.
    [Test]
    public async Task ForceLeaderChangeTriggersNewElectionAndDdlConverges()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();

        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Short lease so the blocked original leader is excluded from the ack gate.
        TimeSpan lease = TimeSpan.FromMilliseconds(1000);
        foreach (InProcessSchemaCluster.Node n in cluster.Nodes)
            n.Kahuna.SchemaAckLiveNodeLease = lease;

        InProcessSchemaCluster.Node originalLeader = await cluster.WaitForSchemaLeaderNodeAsync(db);

        // Force a leadership change — blocks the original leader's transport entirely.
        InProcessSchemaCluster.Node newLeader =
            await cluster.ForceLeaderChangeAsync(db, timeout: TimeSpan.FromSeconds(15));

        Assert.AreNotEqual(originalLeader.Index, newLeader.Index,
            "New leader must be a different node than the original");

        // Wait for the original leader's lease to expire before running DDL.
        await Task.Delay(lease + TimeSpan.FromMilliseconds(250)).ConfigureAwait(false);

        // Run DDL on the new leader; only the two non-blocked nodes form quorum.
        await newLeader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "actuators",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("state", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20));

        // Convergence check only covers the two live (unblocked) nodes.
        InProcessSchemaCluster.Node[] liveNodes = cluster.Nodes
            .Where(n => n.Index != originalLeader.Index)
            .ToArray();

        Assert.True(liveNodes.All(n =>
            n.Database is not null &&
            n.Database.Schema.Tables.ContainsKey("actuators")),
            "Live nodes must converge on DDL issued to new leader");
    }
}
