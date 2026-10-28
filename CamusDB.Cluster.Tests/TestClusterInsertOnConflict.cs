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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>INSERT … ON CONFLICT</c> and <c>UPSERT</c> on a three-node cluster, run through a node that does
/// not lead the table's data partition. The arbiter fence, the probe, the update and the insert all go
/// to the leader on another node, and a second transaction on the leader node itself must still meet the
/// fence of the first.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing.
[NonParallelizable]
public sealed class TestClusterInsertOnConflict
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    [TestCase(KeyValueTransactionLocking.Pessimistic)]
    [TestCase(KeyValueTransactionLocking.Optimistic)]
    public async Task UpsertThroughANodeThatDoesNotLeadTheArbiterKey(KeyValueTransactionLocking locking)
    {
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await DdlOnLeaderAsync(cluster, db, "CREATE TABLE kv (k int64 PRIMARY KEY NOT NULL, email string, n int64, UNIQUE KEY kv_email (email))");

        InProcessSchemaCluster.Node leader = await DataLeaderAsync(cluster, db, "kv");
        InProcessSchemaCluster.Node remote = cluster.Nodes.First(n => !ReferenceEquals(n, leader));

        await RunAsync(remote, db, "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 10)", locking);

        int changed = await RunAsync(remote, db,
            "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 5), (2, 'b@x', 20) ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n", locking);
        Assert.AreEqual(2, changed);

        Assert.AreEqual(0, await RunAsync(remote, db, "INSERT INTO kv (k, email, n) VALUES (3, 'b@x', 0) ON CONFLICT DO NOTHING", locking));
        Assert.AreEqual(1, await RunAsync(remote, db, "UPSERT INTO kv (k, n) VALUES (2, 200)", locking));

        CollectionAssert.AreEqual(new[] { "1|a@x|15", "2|b@x|200" }, await RowsAsync(leader, db));

        // Two transactions upsert one new key, the first through the remote node and the second on the
        // leader node: the second meets the first one's fence, wherever it runs.
        KvTransaction first = await remote.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: locking);
        KvTransaction second = await leader.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: locking);

        const string Increment = "INSERT INTO kv (k, n) VALUES (9, 1) ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n";

        try
        {
            await remote.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(first, db, Increment, null));

            CamusDBException? refused = Assert.CatchAsync<CamusDBException>(async () =>
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(second, db, Increment, null)));
            Assert.IsNotNull(refused);
            Assert.IsTrue(SerializableRetryHelper.IsRetryable(refused!), $"expected a retryable code, got {refused!.Code}");

            await remote.Database.Transactions.CommitAsync(first);
        }
        finally
        {
            await remote.Database.Transactions.RollbackIfNotCompletedAsync(first);
            await leader.Database.Transactions.RollbackIfNotCompletedAsync(second);
        }

        await RunAsync(leader, db, Increment, locking);

        CollectionAssert.AreEqual(new[] { "1|a@x|15", "2|b@x|200", "9||2" }, await RowsAsync(leader, db));
    }

    private static async Task<int> RunAsync(InProcessSchemaCluster.Node on, string db, string sql, KeyValueTransactionLocking locking)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: locking);
        try
        {
            int modified = (await on.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null))).ModifiedRows;
            await on.Database.Transactions.CommitAsync(tx);
            return modified;
        }
        finally
        {
            await on.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<string>> RowsAsync(InProcessSchemaCluster.Node on, string db)
    {
        KvTransaction tx = await on.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await on.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT k, email, n FROM kv ORDER BY k", null));

            List<string> rows = (await cursor.ToListAsync())
                .Select(r => $"{r.Row["k"].LongValue}|{r.Row["email"].StrValue}|{r.Row["n"].LongValue}")
                .ToList();

            await on.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await on.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The node that leads the partition of the table's rows and of its unique indexes.</summary>
    private static async Task<InProcessSchemaCluster.Node> DataLeaderAsync(InProcessSchemaCluster cluster, string db, string table)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            InProcessSchemaCluster.Node any = cluster.Nodes[0];
            TableDescriptor descriptor = await any.Executor.OpenTable(new OpenTableTicket(db, table));
            TablePlacement placement = any.Kahuna.ReadPlacementUncached(descriptor.Store.RowKeySpace, out bool initialized);
            string? leaderEndpoint = initialized && placement.Spans.Count == 1 ? placement.Spans[0].LeaderEndpoint : null;

            if (leaderEndpoint is not null && cluster.Nodes.FirstOrDefault(n => n.Kahuna.Raft.GetLocalEndpoint() == leaderEndpoint) is { } node)
                return node;

            await Task.Delay(200);
        }

        throw new Exception($"No data leader for table '{table}' became known");
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
