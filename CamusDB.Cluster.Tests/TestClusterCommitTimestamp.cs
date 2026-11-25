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
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using Kommander.Time;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The Kahuna commit timestamp that <see cref="KvTransaction.CommitTimestamp"/> records, on a
/// three-node cluster. The coordinator mints it on its own clock, which can be on a node other than
/// the one that runs the statements. These tests check that every gateway records the value, that the
/// causal token is not below it, that writes of one row through different gateways commit in
/// increasing order, and that a commit retried after its coordinator node died reports the same value.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterCommitTimestamp
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db)> StartAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);
        await DdlOnLeaderAsync(cluster, db, "CREATE TABLE kv (k int64 PRIMARY KEY NOT NULL, n int64)");
        return (cluster, db);
    }

    /// <summary>Runs one write in its own explicit transaction and commits it on the same node.</summary>
    private static async Task<(KvTransaction tx, HLCTimestamp causalToken)> WriteAsync(
        InProcessSchemaCluster.Node on, string db, string sql)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            await on.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            HLCTimestamp causalToken = await on.Database.Transactions.CommitAsync(tx);
            return (tx, causalToken);
        }
        finally
        {
            await on.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertRecorded(KvTransaction tx, HLCTimestamp causalToken, string context)
    {
        Assert.IsFalse(tx.CommitTimestamp.IsNull(), $"{context}: a write commit records its commit timestamp");
        Assert.That(tx.CommitTimestamp, Is.GreaterThan(tx.TransactionId), $"{context}: minted above the transaction id");
        Assert.That(causalToken, Is.GreaterThanOrEqualTo(tx.CommitTimestamp), $"{context}: the causal token is not below the commit timestamp");
    }

    [Test]
    public async Task EveryGateway_RecordsTheCommitTimestamp_AlsoWhenTheCoordinatorIsElsewhere()
    {
        (InProcessSchemaCluster cluster, string db) = await StartAsync();
        await using InProcessSchemaCluster owned = cluster;

        HashSet<int> nodeIds = cluster.Nodes.Select(n => n.Kahuna.Raft.GetLocalNodeId()).ToHashSet();
        int remoteCoordinated = 0;
        long key = 0;

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            int localId = node.Kahuna.Raft.GetLocalNodeId();

            for (int i = 0; i < 4; i++)
            {
                (KvTransaction tx, HLCTimestamp causalToken) = await WriteAsync(node, db, $"INSERT INTO kv (k, n) VALUES ({++key}, 0)");
                AssertRecorded(tx, causalToken, $"node {node.Index}, write {i}");

                // The coordinator mints the value on its own clock, so N names the coordinator node.
                Assert.That(nodeIds, Does.Contain(tx.CommitTimestamp.N), "the commit timestamp carries a node id of the cluster");
                if (tx.CommitTimestamp.N != localId)
                    remoteCoordinated++;
            }
        }

        Assert.That(remoteCoordinated, Is.GreaterThan(0),
            "a table's rows live on one partition, so the gateways that do not lead it commit through a remote coordinator");
    }

    [Test]
    public async Task OneRow_WrittenThroughEveryGateway_CommitsInIncreasingOrder()
    {
        (InProcessSchemaCluster cluster, string db) = await StartAsync();
        await using InProcessSchemaCluster owned = cluster;

        (KvTransaction seed, _) = await WriteAsync(cluster.Nodes[0], db, "INSERT INTO kv (k, n) VALUES (1, 0)");
        HLCTimestamp previous = seed.CommitTimestamp;

        for (int i = 1; i <= 6; i++)
        {
            InProcessSchemaCluster.Node node = cluster.Nodes[i % cluster.Nodes.Length];
            (KvTransaction tx, HLCTimestamp causalToken) = await WriteAsync(node, db, $"UPDATE kv SET n = {i} WHERE k = 1");

            AssertRecorded(tx, causalToken, $"update {i} on node {node.Index}");
            Assert.That(tx.CommitTimestamp, Is.GreaterThan(previous),
                $"update {i} on node {node.Index}: a later write of the row commits at a later timestamp");
            previous = tx.CommitTimestamp;
        }
    }

    /// <summary>
    /// The commit lands on a remote coordinator, its answer does not reach the gateway, and the
    /// coordinator node dies. The gateway retries COMMIT on the same handle, and a node that never held
    /// the session answers. That answer must carry the timestamp of the commit that landed.
    ///
    /// <para>The lost answer is simulated after the fact: the first commit completes, and the test puts
    /// the transaction back in the state a lost answer leaves (Finalizing, no commit timestamp). Kahuna
    /// sees the same thing in both cases, because the commit is decided and durable either way.</para>
    /// </summary>
    [Test]
    public async Task ACommitRetriedAfterItsCoordinatorDied_ReportsTheSameCommitTimestamp()
    {
        (InProcessSchemaCluster cluster, string db) = await StartAsync();
        await using InProcessSchemaCluster owned = cluster;

        // Find a gateway whose commit is coordinated on another node, so that node can be killed
        // while the gateway lives.
        InProcessSchemaCluster.Node? gateway = null;
        KvTransaction? tx = null;
        long key = 0;

        for (int attempt = 0; attempt < 12 && gateway is null; attempt++)
        {
            InProcessSchemaCluster.Node node = cluster.Nodes[attempt % cluster.Nodes.Length];
            (KvTransaction written, _) = await WriteAsync(node, db, $"INSERT INTO kv (k, n) VALUES ({++key}, 0)");

            if (written.CommitTimestamp.N != node.Kahuna.Raft.GetLocalNodeId())
            {
                gateway = node;
                tx = written;
            }
        }

        Assert.IsNotNull(gateway, "no commit was coordinated on a node other than its gateway");

        HLCTimestamp landed = tx!.CommitTimestamp;
        int coordinatorIndex = cluster.Nodes.First(n => n.Kahuna.Raft.GetLocalNodeId() == landed.N).Index;

        tx.Status = KvTransactionStatus.Finalizing;
        tx.CommitTimestamp = HLCTimestamp.Zero;

        await cluster.KillNodeAsync(coordinatorIndex);

        // The coordinator's partitions elect a new leader first. Until then the commit answers that its
        // outcome is unresolved, and the caller retries the same commit.
        CamusDBException? lastUnresolved = null;
        long deadline = Environment.TickCount64 + 60_000;
        while (tx.Status != KvTransactionStatus.Committed && Environment.TickCount64 < deadline)
        {
            try
            {
                await gateway!.Database!.Transactions.CommitAsync(tx);
            }
            catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
            {
                lastUnresolved = e;
                await Task.Delay(250);
            }
        }

        Assert.AreEqual(KvTransactionStatus.Committed, tx.Status, $"the retried commit did not resolve: {lastUnresolved?.Message}");
        Assert.AreEqual(landed, tx.CommitTimestamp,
            "the answer of a node that never held the session carries the timestamp of the commit that landed");
    }

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async schemaLeader =>
        {
            await schemaLeader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                .WaitAsync(TimeSpan.FromSeconds(60));
            version = schemaLeader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }
}
