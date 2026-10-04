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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The write-shape fence across a three-node cluster, where the transaction and the schema change run
/// on different nodes. The schema leader cannot see a follower's transactions, so two things must hold
/// on the follower by itself: its apply of the change refuses the later commit of a write planned
/// before it, and it withholds its acknowledgement while such a commit is in flight, which keeps the
/// leader's backfill from reading the table too early.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterSchemaChangeSpanningTransactions
{
    private const string LateIndex = "CREATE INDEX weather_city_late ON weather (city)";

    private const string ThroughLateIndex = "SELECT id FROM weather@{FORCE_INDEX=weather_city_late} WHERE city = 'atlantis'";

    [Test]
    public async Task CommitOnAFollowerAfterTheIndexBuildIsRefused()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        KvTransaction tx = await follower.Database!.Transactions.BeginAsync();
        await follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        await DdlAsync(leader, db, LateIndex);
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await follower.Database.Transactions.CommitAsync(tx))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, refused.Code, refused.Message);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);

        // The retry plans against the index, and every node finds the row through it.
        await DmlAsync(follower, db, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')");

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(1, await CountAsync(node, db, ThroughLateIndex), $"node {node.Index}");
    }

    [Test]
    public async Task IndexBuildWaitsForACommitInFlightOnAFollower()
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(nodeCount: 3, wireLeaderForwarder: true);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await SeedAsync(cluster, db);

        InProcessSchemaCluster.Node leader = await cluster.WaitForSchemaLeaderNodeAsync(db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        KvTransaction tx = await follower.Database!.Transactions.BeginAsync();
        await follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        // The first half of CommitAsync: the gate is passed, and no commit request has left yet.
        Assert.IsTrue(tx.TryEnterCommit(out _));

        Task build = DdlAsync(leader, db, LateIndex);

        // The follower has applied the first step, but it must not acknowledge it. So the leader's
        // full-convergence gates stay closed and the backfill does not read the table.
        Assert.AreNotSame(build, await Task.WhenAny(build, Task.Delay(TimeSpan.FromSeconds(2))),
            "The index build must not finish while a commit planned before it is in flight on a follower");

        await follower.Database.Transactions.CommitAsync(tx);
        await build;
        await cluster.WaitForSchemaConvergenceAsync(db, leader.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(30));

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            Assert.AreEqual(1, await CountAsync(node, db, ThroughLateIndex), $"node {node.Index}: the row must be in the index");
    }

    private static async Task SeedAsync(InProcessSchemaCluster cluster, string db)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await DdlAsync(leader, db, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)");
            await DmlAsync(leader, db, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'quito'), (3, NULL)");
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(60));

    private static async Task DmlAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> CountAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> rows) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            int count = 0;
            await foreach (QueryResultRow _ in rows)
                count++;

            return count;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
