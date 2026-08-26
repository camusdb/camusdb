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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>TRUNCATE</c> across a three-node cluster: the contents swap must be one committed generation
/// that every node sees, whether the statement arrived at the schema leader or at a follower.
///
/// <para>A follower cannot run it locally. It allocates no storage id and takes no fence of its own —
/// a fence held on a follower while it waits for the leader would fence nothing — so the whole
/// statement is forwarded and the follower only waits until it can see the result.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterTruncate
{
    private static async Task SeedRobotsAsync(InProcessSchemaCluster cluster, string db, int rows)
    {
        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "robots",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            ));

            for (int i = 0; i < rows; i++)
            {
                KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx, database: db,
                    sql: $"INSERT INTO robots (id, name) VALUES (gen_id(), 'r{i}')", parameters: null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);
    }

    private static async Task<int> CountRowsAsync(InProcessSchemaCluster.Node node, string db)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: "SELECT id FROM robots", parameters: null));

            return (await cursor.ToListAsync()).Count;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertEveryNodeSeesOneEmptyGeneration(InProcessSchemaCluster cluster, string sourceTableId)
    {
        string? agreedStorageId = null;

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableSchema schema = node.Database!.Schema.Tables["robots"];

            Assert.AreEqual(sourceTableId, schema.Id,
                $"node {node.Index}: the relation's identity must survive the truncate");
            Assert.AreEqual(1, schema.ContentsGeneration,
                $"node {node.Index}: exactly one contents generation must have been committed");
            Assert.AreNotEqual(sourceTableId, schema.EffectiveStorageId,
                $"node {node.Index}: the relation must read a new key-space");
            Assert.IsNotNull(schema.ContentsValidFrom, $"node {node.Index}: the cut must be recorded");

            agreedStorageId ??= schema.EffectiveStorageId;
            Assert.AreEqual(agreedStorageId, schema.EffectiveStorageId,
                $"node {node.Index}: every node must adopt the same key-space");
        }
    }

    [Test]
    public async Task TruncateOnTheLeaderConvergesOnEveryNode()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await SeedRobotsAsync(cluster, db, 5);

        string sourceTableId = cluster.Nodes[0].Database!.Schema.Tables["robots"].Id!;

        await cluster.RunOnSchemaLeaderAsync(db, leader =>
            leader.Executor.TruncateTable(new TruncateTableTicket(db, "robots"))
                .WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        AssertEveryNodeSeesOneEmptyGeneration(cluster, sourceTableId);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(0, await CountRowsAsync(node, db),
                $"node {node.Index} must read the table as empty once the truncate is acknowledged");
    }

    [Test]
    public async Task TruncateOnAFollowerIsForwardedAndProducesTheSameOneGeneration()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await SeedRobotsAsync(cluster, db, 4);

        string sourceTableId = cluster.Nodes[0].Database!.Schema.Tables["robots"].Id!;

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(node => node.Index != leader.Index);

        Assert.True(await follower.Executor.TruncateTable(new TruncateTableTicket(db, "robots"))
            .WaitAsync(TimeSpan.FromSeconds(30)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        AssertEveryNodeSeesOneEmptyGeneration(cluster, sourceTableId);

        // The forward must not return before the originating node can see the result: a caller that
        // issues DDL and then reads on the same connection must not observe the pre-truncate rows.
        Assert.AreEqual(0, await CountRowsAsync(follower, db),
            "the node that issued the forwarded truncate must already see the new generation");
    }

    [Test]
    public async Task TruncateThenInsertOnAFollowerLandsInTheNewGeneration()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await SeedRobotsAsync(cluster, db, 3);

        await cluster.RunOnSchemaLeaderAsync(db, leader =>
            leader.Executor.TruncateTable(new TruncateTableTicket(db, "robots"))
                .WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 2);

        InProcessSchemaCluster.Node leaderNode = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node writer = cluster.Nodes.First(node => node.Index != leaderNode.Index);

        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();
        await writer.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db,
            sql: "INSERT INTO robots (id, name) VALUES (gen_id(), 'after')", parameters: null));
        await writer.Database.Transactions.CommitAsync(tx);

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(1, await CountRowsAsync(node, db),
                $"node {node.Index} must see exactly the row written after the truncate");
    }
}
