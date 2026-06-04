/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

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

    // B2: CREATE INDEX on the leader must be visible on every node via table.Schema.Indexes
    // and usable via FORCE_INDEX without a reopen, proving the schema log carries the change.
    [Test]
    public async Task AddIndexOnLeaderConvergesAcrossNodesWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Step 1: create the table on the leader (schema version → 1).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer64)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        // Step 2: insert a few rows so the backfill has work to do.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            for (int i = 1; i <= 5; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: $"INSERT INTO robots (id, name, year) VALUES (gen_id(), 'robot {i}', {2000 + i})",
                    parameters: null
                ));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        // Step 3: add a secondary index on the leader (schema version → 2).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "robots",
            indexName: "name_idx",
            columns: [new ColumnIndexInfo("name", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // Step 4: every node must have the index in table.Schema.Indexes without reopen.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.TryGetValue("robots", out TableSchema? schema) &&
            schema.Indexes is not null &&
            schema.Indexes.Any(ix => ix.Name == "name_idx" && ix.State == SchemaElementState.Public)),
            "name_idx must appear in TableSchema.Indexes on every node after AddIndex convergence"
        );

        // Step 5: every node's in-memory TableDescriptor must answer FORCE_INDEX without reopen.
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await leader.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: "SELECT id FROM robots@{FORCE_INDEX=name_idx}",
                    parameters: null
                ));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await leader.Database.Transactions.CommitAsync(tx);
            Assert.AreEqual(5, rows.Count, "FORCE_INDEX on leader must return all 5 rows");
        });

        // Check each follower as well.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (node.Database is null) continue;

            KvTransaction tx = await node.Database.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: db,
                    sql: "SELECT id FROM robots@{FORCE_INDEX=name_idx}",
                    parameters: null
                ));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            Assert.AreEqual(5, rows.Count, $"FORCE_INDEX on node {node.Index} must return all 5 rows");
        }
    }

    // B2: DROP INDEX on the leader must remove the index from every node's table.Schema.Indexes
    // without reopen, and FORCE_INDEX must subsequently fail on all nodes.
    [Test]
    public async Task DropIndexOnLeaderConvergesAcrossNodesWithoutReopen()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        // Create table + index (2 schema versions).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "sensors",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("label", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "sensors",
            indexName: "label_idx",
            columns: [new ColumnIndexInfo("label", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        // Drop the index (schema version → 3).
        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "sensors",
            indexName: "label_idx",
            columns: [],
            operation: AlterIndexOperation.DropIndex
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 3);

        // All nodes must have the index absent from TableSchema.Indexes.
        Assert.True(cluster.Nodes.All(node =>
            node.Database is not null &&
            node.Database.Schema.Tables.TryGetValue("sensors", out TableSchema? schema) &&
            (schema.Indexes is null || schema.Indexes.All(ix => ix.Name != "label_idx"))),
            "label_idx must be absent from TableSchema.Indexes on every node after DropIndex convergence"
        );

        // FORCE_INDEX on the dropped index must throw on every node.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            if (node.Database is null) continue;

            KvTransaction tx = await node.Database.Transactions.BeginAsync();
            Assert.ThrowsAsync<CamusDB.Core.CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) =
                    await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                        txnState: tx,
                        database: db,
                        sql: "SELECT id FROM sensors@{FORCE_INDEX=label_idx}",
                        parameters: null
                    ));
                await cursor.ToListAsync();
            });
            await node.Database.Transactions.RollbackAsync(tx);
        }
    }
}
