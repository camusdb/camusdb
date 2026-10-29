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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// <c>SELECT … FOR UPDATE</c> across nodes: a transaction that runs on a node which does not lead the
/// table's partition locks rows whose leader is elsewhere, and the locks it takes and meets are the same
/// ones a transaction on the leader takes and meets. Point locks are routed to the key's leader, so a
/// held row is refused, skipped or reported the same way from every node. Under key-range sharding,
/// one page of row locks can reach two leader nodes, and each node's group stops at its own held row.
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing, which shows up as spurious leadership churn.
[NonParallelizable]
public sealed class TestSelectForUpdateCluster
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(
        builder => builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static async Task<(InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader)> SetupJobsAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 2, partitions: 1,
                loggerFactory: sharedLoggerFactory, logger: logger);
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "jobs",
            columns:
            [
                new ColumnInfo("k", ColumnType.String, notNull: true),
                new ColumnInfo("state", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("k", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction setup = await leader.Database!.Transactions.BeginAsync();
            await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: setup, database: db,
                sql: "INSERT INTO jobs (k, state) VALUES ('a', 'ready'), ('b', 'ready'), ('c', 'ready'), ('d', 'ready'), ('e', 'ready')",
                parameters: null));
            await leader.Database.Transactions.CommitAsync(setup);
        });

        InProcessSchemaCluster.Node node = await cluster.WaitForSchemaLeaderNodeAsync(db);
        return (cluster, db, node);
    }

    private static Task<KvTransaction> BeginAsync(InProcessSchemaCluster.Node on) =>
        on.Database!.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);

    private static async Task<string[]> KeysAsync(InProcessSchemaCluster.Node on, string db, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await on.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
        return (await cursor.ToListAsync()).Select(r => r.Row["k"].StrValue!).ToArray();
    }

    private static async Task<string> FailureCodeAsync(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }

        Assert.Fail("the statement was expected to fail");
        return "";
    }

    [Test]
    public async Task Cluster_RowsLockedThroughEitherNode_AreHeldAgainstTheOther()
    {
        (InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node leader) = await SetupJobsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => !ReferenceEquals(n, leader));

        // The leader holds row a.
        KvTransaction holder = await BeginAsync(leader);
        Assert.AreEqual(new[] { "a" }, await KeysAsync(leader, db, holder, "SELECT k FROM jobs WHERE k = 'a' FOR UPDATE"));

        // From the other node: NOWAIT reports it, an UPDATE is refused, and SKIP LOCKED passes over it.
        KvTransaction remote = await BeginAsync(follower);
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, await FailureCodeAsync(
            () => KeysAsync(follower, db, remote, "SELECT k FROM jobs WHERE k = 'a' FOR UPDATE NOWAIT")));
        await follower.Database!.Transactions.RollbackIfNotCompletedAsync(remote);

        KvTransaction writer = await BeginAsync(follower);
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, await FailureCodeAsync(
            () => follower.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: writer, database: db, sql: "UPDATE jobs SET state = 'taken' WHERE k = 'a'", parameters: null))));
        await follower.Database.Transactions.RollbackIfNotCompletedAsync(writer);

        KvTransaction skipper = await BeginAsync(follower);
        Assert.AreEqual(new[] { "b", "c" }, await KeysAsync(follower, db, skipper,
            "SELECT k FROM jobs WHERE state = 'ready' ORDER BY k LIMIT 2 FOR UPDATE SKIP LOCKED"));

        // The rows the other node locked are held against the leader, and the row after the LIMIT is free.
        KvTransaction probe = await BeginAsync(leader);
        Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, await FailureCodeAsync(
            () => KeysAsync(leader, db, probe, "SELECT k FROM jobs WHERE k = 'b' FOR UPDATE NOWAIT")));
        await leader.Database!.Transactions.RollbackIfNotCompletedAsync(probe);

        KvTransaction free = await BeginAsync(leader);
        Assert.AreEqual(new[] { "d" }, await KeysAsync(leader, db, free, "SELECT k FROM jobs WHERE k = 'd' FOR UPDATE NOWAIT"));
        await leader.Database.Transactions.CommitAsync(free);

        await follower.Database.Transactions.CommitAsync(skipper);
        await leader.Database.Transactions.CommitAsync(holder);

        // Both transactions ended, so every row can be locked again from the other node.
        KvTransaction sweep = await BeginAsync(follower);
        Assert.AreEqual(5, (await KeysAsync(follower, db, sweep, "SELECT k FROM jobs FOR UPDATE NOWAIT")).Length);
        await follower.Database.Transactions.CommitAsync(sweep);
    }

    private const int SplitRows = 8;

    /// <summary>
    /// Starts a two-node key-range cluster with table <c>jobs</c>, splits its row space at the fifth
    /// row, and returns the keys <c>k</c> in row-key order. A row key is a generated row id, not a
    /// column value, so the setup reads the row ids back and sets <c>n</c> to each row's position in
    /// row-key order: <c>ORDER BY n</c> then visits the left half, then the right half. When both
    /// halves have the same leader, the setup moves the leader of the right half to the other node.
    /// </summary>
    private static async Task<(InProcessSchemaCluster cluster, string db, string[] keys)> SetupSplitJobsAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 2, partitions: 2,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "jobs",
            columns:
            [
                new ColumnInfo("k", ColumnType.String, notNull: true),
                new ColumnInfo("n", ColumnType.Integer64, notNull: true),
                new ColumnInfo("state", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("k", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];
        KvTransaction seed = await writer.Database!.Transactions.BeginAsync();
        await writer.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: seed, database: db,
            sql: "INSERT INTO jobs (k, n, state) VALUES " +
                 string.Join(", ", Enumerable.Range(0, SplitRows).Select(i => $"('k{i}', -1, 'ready')")),
            parameters: null));
        await writer.Database.Transactions.CommitAsync(seed);

        KvTransaction read = await BeginAsync(writer);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await writer.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: read, database: db, sql: "SELECT k FROM jobs", parameters: null));
        List<(ObjectIdValue RowId, string Key)> rows =
            (await cursor.ToListAsync()).Select(r => (r.RowId, r.Row["k"].StrValue!)).ToList();
        await writer.Database.Transactions.CommitAsync(read);

        Assert.AreEqual(SplitRows, rows.Count);
        rows.Sort((a, b) => string.CompareOrdinal(a.RowId.ToString(), b.RowId.ToString()));
        string[] keys = rows.Select(r => r.Key).ToArray();

        KvTransaction order = await BeginAsync(writer);
        for (int i = 0; i < keys.Length; i++)
        {
            await writer.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: order, database: db, sql: $"UPDATE jobs SET n = {i} WHERE k = '{keys[i]}'", parameters: null));
        }
        await writer.Database.Transactions.CommitAsync(order);

        TableDescriptor table = await writer.Database.TableDescriptors["jobs"];
        string keySpace = table.Store.RowKeySpace;
        await KeyRangeSplitHarness.SplitAtAsync(cluster, keySpace, table.Store.RowPointKey(rows[SplitRows / 2].RowId));

        PlacementSpan[] spans = await SpansWithLeadersAsync(cluster, keySpace);
        if (spans[0].LeaderEndpoint == spans[1].LeaderEndpoint)
        {
            InProcessSchemaCluster.Node from = cluster.Nodes.First(n => n.Kahuna.Raft.GetLocalEndpoint() == spans[1].LeaderEndpoint);
            InProcessSchemaCluster.Node to = cluster.Nodes.First(n => !ReferenceEquals(n, from));
            await from.Kahuna.Raft.TransferLeadershipAsync(spans[1].PartitionId, to.Kahuna.Raft.GetLocalEndpoint());

            for (int attempt = 0; attempt < 75; attempt++)
            {
                spans = await SpansWithLeadersAsync(cluster, keySpace);
                if (spans[0].LeaderEndpoint != spans[1].LeaderEndpoint)
                    break;
                await Task.Delay(200);
            }
        }

        Assert.AreNotEqual(spans[0].LeaderEndpoint, spans[1].LeaderEndpoint,
            "the two halves of the row space must have leaders on two nodes");
        return (cluster, db, keys);
    }

    /// <summary>
    /// The two spans of <paramref name="keySpace"/>, in key order, once every node reports both of
    /// them with a known leader.
    /// </summary>
    private static async Task<PlacementSpan[]> SpansWithLeadersAsync(InProcessSchemaCluster cluster, string keySpace)
    {
        PlacementSpan[]? spans = null;
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TablePlacement? placement = null;
            for (int attempt = 0; attempt < 75; attempt++)
            {
                node.Kahuna.InvalidatePlacement(keySpace);
                placement = node.Kahuna.GetPlacement(keySpace);
                if (placement.IsKeyRange && placement.Spans.Count == 2 && placement.Spans.All(s => s.LeaderEndpoint is not null))
                    break;
                await Task.Delay(200);
            }

            Assert.IsTrue(placement!.IsKeyRange && placement.Spans.Count == 2, $"node {node.Index} must report the two spans");
            Assert.IsTrue(placement.Spans.All(s => s.LeaderEndpoint is not null), $"node {node.Index} must know each span's leader");
            spans ??= placement.Spans.ToArray();
        }

        return spans!;
    }

    private static async Task<string[]> KeysOfAsync(InProcessSchemaCluster.Node on, string db, KvTransaction tx, string sql, string key)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await on.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: sql, parameters: new() { ["@k"] = new(ColumnType.String, key) }));
        return (await cursor.ToListAsync()).Select(r => r.Row["k"].StrValue!).ToArray();
    }

    /// <summary>
    /// One SKIP LOCKED page whose rows have leaders on two nodes, with a held row in each node's group.
    /// Kahuna stops each group at its held row and answers nothing for the rows after it, in both
    /// groups at once; the statement must send those rows again and lock them, skip only the two held
    /// rows, and fill the LIMIT from the rows after the page.
    /// </summary>
    [Test]
    public async Task Cluster_ASkipLockedPageOverTwoLeaders_SkipsTheHeldRowOfEachGroup()
    {
        (InProcessSchemaCluster cluster, string db, string[] keys) = await SetupSplitJobsAsync();
        await using InProcessSchemaCluster clusterScope = cluster;

        InProcessSchemaCluster.Node first = cluster.Nodes[0];
        InProcessSchemaCluster.Node second = cluster.Nodes[1];
        const string LockOne = "SELECT k FROM jobs WHERE k = @k FOR UPDATE";
        const string ProbeOne = "SELECT k FROM jobs WHERE k = @k FOR UPDATE NOWAIT";

        // Hold one row in each half: row 1 in the left half and row 5 in the right half.
        KvTransaction holder = await BeginAsync(first);
        Assert.AreEqual(new[] { keys[1] }, await KeysOfAsync(first, db, holder, LockOne, keys[1]));
        Assert.AreEqual(new[] { keys[5] }, await KeysOfAsync(first, db, holder, LockOne, keys[5]));

        // The first page has six rows, 0 to 5: three in each group, with a held row in each.
        KvTransaction skipper = await BeginAsync(second);
        string[] taken = await KeysAsync(second, db, skipper,
            "SELECT k FROM jobs WHERE state = 'ready' ORDER BY n LIMIT 6 FOR UPDATE SKIP LOCKED");
        Assert.AreEqual(new[] { keys[0], keys[2], keys[3], keys[4], keys[6], keys[7] }, taken);

        // Every row the skipper returned is held against the other node, in both groups. The rows
        // after each group's held row are among them: those are the rows Kahuna did not answer.
        foreach (string key in taken)
        {
            KvTransaction probe = await BeginAsync(first);
            Assert.AreEqual(CamusDBErrorCodes.LockNotAvailable, await FailureCodeAsync(
                () => KeysOfAsync(first, db, probe, ProbeOne, key)), $"row {Array.IndexOf(keys, key)} must be held by the skipper");
            await first.Database!.Transactions.RollbackIfNotCompletedAsync(probe);
        }

        await second.Database!.Transactions.CommitAsync(skipper);
        await first.Database!.Transactions.CommitAsync(holder);

        KvTransaction sweep = await BeginAsync(second);
        Assert.AreEqual(SplitRows, (await KeysAsync(second, db, sweep, "SELECT k FROM jobs FOR UPDATE NOWAIT")).Length);
        await second.Database.Transactions.CommitAsync(sweep);
    }
}
