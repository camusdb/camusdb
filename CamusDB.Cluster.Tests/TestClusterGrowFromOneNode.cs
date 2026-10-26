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

using Kahuna.Shared.Communication.Rest;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The growth path for a deployment that starts on one machine: a one-node cluster (cluster mode, no
/// peers) takes two more nodes with a join and keeps every row it held. A standalone node cannot do
/// this, because its embedded Raft group is a fixed single-node quorum that refuses joins, so a single
/// machine that expects to grow starts in cluster mode.
///
/// <para>The first node runs with key-range routing and three data partitions, holds two tables with
/// secondary indexes, and splits one row space before anyone joins, so the joiners must also take a
/// partition that the split created. After the joins, every node must serve every row by point read,
/// scan and index, agree on the table's placement, apply a new DDL and accept writes. With the first
/// node stopped, the two joiners keep quorum and still serve every row.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestClusterGrowFromOneNode
{
    private const int Partitions = 3;
    private const int AccountRows = 120;
    private const int OrderRows = 60;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                .WaitAsync(TimeSpan.FromSeconds(60));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(60));
    }

    private static async Task ExecAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await node.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> QueryAsync(InProcessSchemaCluster.Node node, string db, string sql)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await node.Database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Runs <paramref name="action"/> until it succeeds. A partition whose leader is still being elected
    /// (after a join, or after the first node stops) answers with a retryable error for a few seconds.
    /// </summary>
    private static async Task<T> EventuallyAsync<T>(string what, Func<Task<T>> action, TimeSpan? timeout = null)
    {
        long deadline = Environment.TickCount64 + (long)(timeout ?? TimeSpan.FromSeconds(60)).TotalMilliseconds;
        Exception? last = null;

        while (Environment.TickCount64 < deadline)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (ex is CamusDBException or TimeoutException)
            {
                last = ex;
                await Task.Delay(250);
            }
        }

        throw new AssertionException($"{what} did not succeed in time; last error: {last?.GetType().Name}: {last?.Message}");
    }

    private static string InsertAccounts(int from, int count) =>
        "INSERT INTO accounts (id, owner, balance) VALUES " +
        string.Join(", ", Enumerable.Range(from, count).Select(i => $"({i}, 'owner{i % 7}', {i * 10})"));

    private static string InsertOrders(int from, int count) =>
        "INSERT INTO orders (id, account_id, amount) VALUES " +
        string.Join(", ", Enumerable.Range(from, count).Select(i => $"({i}, {i % AccountRows}, {i})"));

    /// <summary>Splits the accounts row space at its median stored row and returns the space.</summary>
    private static async Task<string> SplitAccountsAsync(InProcessSchemaCluster.Node node)
    {
        TableDescriptor table = await node.Database!.TableDescriptors["accounts"];
        string keySpace = table.Store.RowKeySpace;

        List<ObjectIdValue> rowIds = [];
        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(KvTransaction.CreateReadOnly()))
            rowIds.Add(rowId);

        Assert.That(rowIds, Has.Count.EqualTo(AccountRows));
        rowIds.Sort((a, b) => string.CompareOrdinal(a.ToString(), b.ToString()));
        string splitKey = table.Store.RowPointKey(rowIds[rowIds.Count / 2]);

        KahunaSplitRangeResponse split = await EventuallyAsync("split of the accounts row space", async () =>
        {
            KahunaSplitRangeResponse response =
                await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);
            if (!response.Success)
                throw new TimeoutException($"split refused: {response.Status}");
            return response;
        });

        Assert.That(split.Success, Is.True);
        return keySpace;
    }

    /// <summary>The partition ids of a space's spans as one node routes them, in key order.</summary>
    private static async Task<string> PartitionsOfAsync(InProcessSchemaCluster.Node node, string keySpace, int minSpans)
    {
        return await EventuallyAsync($"placement of {keySpace} on node {node.Index}", () =>
        {
            node.Kahuna.InvalidatePlacement(keySpace);
            TablePlacement placement = node.Kahuna.GetPlacement(keySpace);
            if (!placement.IsKeyRange || placement.Spans.Count < minSpans)
                throw new TimeoutException($"{placement.Spans.Count} span(s), key-range {placement.IsKeyRange}");
            return Task.FromResult(string.Join(",", placement.Spans.Select(s => s.PartitionId)));
        });
    }

    /// <summary>Every account and order row is served by this node by scan, by point read and through each index.</summary>
    private static async Task AssertServesEveryRowAsync(InProcessSchemaCluster.Node node, string db, int accounts)
    {
        string where = $"node {node.Index}";

        List<QueryResultRow> scanned = await EventuallyAsync($"accounts scan on {where}",
            () => QueryAsync(node, db, "SELECT id FROM accounts"));
        Assert.That(scanned.Select(r => r.Row["id"].LongValue).OrderBy(v => v), Is.EqualTo(Enumerable.Range(0, accounts).Select(i => (long)i)),
            $"{where}: the scan returns every account");

        foreach (int id in new[] { 0, 1, AccountRows / 2, AccountRows - 1, accounts - 1 })
        {
            List<QueryResultRow> point = await EventuallyAsync($"point read of account {id} on {where}",
                () => QueryAsync(node, db, $"SELECT balance FROM accounts WHERE id = {id}"));
            Assert.That(point, Has.Count.EqualTo(1), $"{where}: point read of account {id}");
            Assert.That(point[0].Row["balance"].LongValue, Is.EqualTo(id * 10L), $"{where}: balance of account {id}");
        }

        List<QueryResultRow> byOwner = await EventuallyAsync($"owner index on {where}",
            () => QueryAsync(node, db, "SELECT id FROM accounts@{FORCE_INDEX=owner_idx} WHERE owner = 'owner3'"));
        Assert.That(byOwner.Select(r => r.Row["id"].LongValue).OrderBy(v => v),
            Is.EqualTo(Enumerable.Range(0, accounts).Where(i => i % 7 == 3).Select(i => (long)i)),
            $"{where}: the owner index holds every matching account");

        List<QueryResultRow> orders = await EventuallyAsync($"orders scan on {where}",
            () => QueryAsync(node, db, "SELECT id FROM orders"));
        Assert.That(orders, Has.Count.EqualTo(OrderRows), $"{where}: the scan returns every order");

        List<QueryResultRow> byAccount = await EventuallyAsync($"account index on {where}",
            () => QueryAsync(node, db, "SELECT id FROM orders@{FORCE_INDEX=account_idx} WHERE account_id = 5"));
        Assert.That(byAccount.Select(r => r.Row["id"].LongValue).OrderBy(v => v),
            Is.EqualTo(Enumerable.Range(0, OrderRows).Where(i => i % AccountRows == 5).Select(i => (long)i)),
            $"{where}: the account index holds every matching order");
    }

    [Test]
    public async Task OneNodeCluster_TakesTwoJoiners_AndEveryNodeServesEveryRow()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 1, partitions: Partitions, loggerFactory: sharedLoggerFactory, logger: logger,
            options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });

        try
        {
            // One node: create the data, then split a row space so the joiners must take a partition
            // that did not exist at bootstrap.
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);
            InProcessSchemaCluster.Node first = cluster.Nodes[0];

            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE accounts (id INT64 PRIMARY KEY, owner STRING, balance INT64)");
            await DdlOnLeaderAsync(cluster, db, "CREATE INDEX owner_idx ON accounts (owner)");
            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE orders (id INT64 PRIMARY KEY, account_id INT64, amount INT64)");
            await DdlOnLeaderAsync(cluster, db, "CREATE INDEX account_idx ON orders (account_id)");

            await ExecAsync(first, db, InsertAccounts(0, AccountRows));
            await ExecAsync(first, db, InsertOrders(0, OrderRows));

            string accountsSpace = await SplitAccountsAsync(first);
            string partitionsBefore = await PartitionsOfAsync(first, accountsSpace, minSpans: 2);
            TestContext.Out.WriteLine($"accounts row space before the joins: partitions {partitionsBefore}");

            await AssertServesEveryRowAsync(first, db, AccountRows);

            // Two joins, one at a time: Kommander changes membership one server at a time.
            for (int i = 0; i < 2; i++)
            {
                InProcessSchemaCluster.Node joined = await cluster.JoinNodeAsync();
                TestContext.Out.WriteLine($"node {joined.Index} ({joined.Kahuna.Raft.GetLocalEndpoint()}) joined");
                await cluster.OpenDatabaseOnNodeAsync(joined.Index, db);
            }

            Assert.That(cluster.Nodes, Has.Length.EqualTo(3));
            await cluster.WaitForSchemaConvergenceAsync(db, first.Database!.Schema.SchemaVersion, timeout: TimeSpan.FromSeconds(60));

            // Every node serves every row and routes the split space the same way.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                await AssertServesEveryRowAsync(node, db, AccountRows);
                Assert.That(await PartitionsOfAsync(node, accountsSpace, minSpans: 2), Is.EqualTo(partitionsBefore),
                    $"node {node.Index} routes the split accounts space as the first node did");
            }

            // A DDL after the joins reaches every node, and every node accepts writes.
            await DdlOnLeaderAsync(cluster, db, "CREATE TABLE audit (id INT64 PRIMARY KEY, node INT64)");

            int nextAccount = AccountRows;
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                await EventuallyAsync($"writes on node {node.Index}", async () =>
                {
                    await ExecAsync(node, db, $"INSERT INTO audit (id, node) VALUES ({node.Index}, {node.Index})");
                    return true;
                });
                int id = nextAccount++;
                await EventuallyAsync($"account insert on node {node.Index}", async () =>
                {
                    await ExecAsync(node, db, InsertAccounts(id, 1));
                    return true;
                });
            }

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                List<QueryResultRow> audit = await EventuallyAsync($"audit scan on node {node.Index}",
                    () => QueryAsync(node, db, "SELECT node FROM audit"));
                Assert.That(audit.Select(r => r.Row["node"].LongValue).OrderBy(v => v), Is.EqualTo(new long[] { 0, 1, 2 }),
                    $"node {node.Index} sees the write of every node");
                await AssertServesEveryRowAsync(node, db, nextAccount);
            }

            // The first node stops. The joiners keep quorum and still serve every row.
            await cluster.KillNodeAsync(0);

            foreach (InProcessSchemaCluster.Node node in cluster.Nodes.Skip(1))
                await AssertServesEveryRowAsync(node, db, nextAccount);

            InProcessSchemaCluster.Node survivor = cluster.Nodes[1];
            int lastAccount = nextAccount++;
            await EventuallyAsync("account insert after the first node stopped", async () =>
            {
                await ExecAsync(survivor, db, InsertAccounts(lastAccount, 1));
                return true;
            }, TimeSpan.FromSeconds(90));

            await AssertServesEveryRowAsync(cluster.Nodes[2], db, nextAccount);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
