/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Where a client-owned transaction's Kahuna session lives when its table is key-range routed, on a
/// real three-node cluster. Kahuna hashes an unpinned coordinator key, which names no key-range
/// table's partition, so CamusDB pins the session to the partition of the table's one row range.
/// The session then lives on the row range's leader: a transaction executed there registers
/// everything locally, and a transaction executed elsewhere forwards to that leader, as under hash
/// routing. The pin is read from the key, so a split during the transaction does not lose the
/// session. Kahuna counts registrations by route on a process-wide meter, so each test measures the
/// delta around its own transaction and runs serially.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSessionAnchorKeyRangeCluster
{
    private const int RowCount = 40;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static readonly CamusDBOptions KeyRange = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };

    /// <summary>Sums the registration counter by <c>route</c> while it is alive.</summary>
    private sealed class RegistrationProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly object gate = new();
        private long forwarded;
        private long local;

        public RegistrationProbe()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Kahuna" && instrument.Name == "kahuna.durable_tx.session_registration_forwards")
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key != "route")
                        continue;
                    lock (gate)
                    {
                        if (Equals(tag.Value, "forwarded")) forwarded += value;
                        else if (Equals(tag.Value, "local")) local += value;
                    }
                }
            });
            listener.Start();
        }

        public long Forwarded { get { lock (gate) return forwarded; } }

        public long Local { get { lock (gate) return local; } }

        public void Dispose() => listener.Dispose();
    }

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids)> SetupAsync()
    {
        InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger, options: KeyRange);

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "accounts",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("balance", ColumnType.Integer64),
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(30)));

            await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

            InProcessSchemaCluster.Node seeder = cluster.Nodes[0];
            List<string> ids = [];
            KvTransaction setup = await seeder.Database!.Transactions.BeginAsync();
            for (int i = 0; i < RowCount; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);
                await seeder.Executor.Insert(new InsertTicket(
                    txnState: setup, databaseName: db, tableName: "accounts",
                    values: new() { new() {
                        { "id",      new(ColumnType.Id,        id) },
                        { "balance", new(ColumnType.Integer64, 100L) },
                    }}));
            }
            await seeder.Database.Transactions.CommitAsync(setup);

            // Registration is node-local: open the table everywhere so every node routes it by range.
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
                await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

            return (cluster, db, ids);
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>The one row range of the table and the node that leads it, once every node sees the range.</summary>
    private static async Task<(PlacementSpan span, InProcessSchemaCluster.Node leader)> RowRangeLeaderAsync(
        InProcessSchemaCluster cluster, string db)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            InProcessSchemaCluster.Node any = cluster.Nodes[0];
            TableDescriptor table = await any.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

            bool everyNodeSeesOneRange = cluster.Nodes.All(n =>
            {
                TablePlacement p = n.Kahuna.ReadPlacementUncached(table.Store.RowKeySpace, out bool initialized);
                return initialized && p.IsKeyRange && p.Spans.Count == 1;
            });

            TablePlacement placement = any.Kahuna.ReadPlacementUncached(table.Store.RowKeySpace, out _);
            string? leader = everyNodeSeesOneRange ? placement.Spans[0].LeaderEndpoint : null;
            if (leader is not null)
            {
                InProcessSchemaCluster.Node? node = cluster.Nodes.FirstOrDefault(n => n.Kahuna.Raft.GetLocalEndpoint() == leader);
                if (node is not null)
                    return (placement.Spans[0], node);
            }
            await Task.Delay(200);
        }

        throw new Exception("No leader for the accounts row range became known");
    }

    private static async Task UpdateAsync(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string rowId)
    {
        ExecuteNonSQLResult update = await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: $"UPDATE accounts SET balance = balance + 1 WHERE id = '{rowId}'", parameters: null));
        Assert.That(update.ModifiedRows, Is.EqualTo(1));
    }

    /// <summary>A client-owned transaction: read the row, update it, commit — all on one node.</summary>
    private static async Task<KvTransaction> RunTransferAsync(InProcessSchemaCluster.Node node, string db, string rowId)
    {
        DatabaseDescriptor database = node.Database!;
        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: $"SELECT balance FROM accounts WHERE id = '{rowId}'", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.That(rows, Has.Count.EqualTo(1));

        await UpdateAsync(node, tx, db, rowId);
        await database.Transactions.CommitAsync(tx);
        return tx;
    }

    private static async Task<long> BalanceAsync(InProcessSchemaCluster.Node node, string db, string rowId)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: node.Database!.Transactions.BeginReadOnly(), database: db,
            sql: $"SELECT balance FROM accounts WHERE id = '{rowId}'", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.That(rows, Has.Count.EqualTo(1));
        return rows[0].Row["balance"].LongValue;
    }

    private static int PinnedPartitionOf(KvTransaction tx)
    {
        Assert.That(CoordinatorKeys.TryGetPinnedPartition(tx.CoordinatorKey, out int partition), Is.True,
            $"the session key is pinned: {tx.CoordinatorKey}");
        return partition;
    }

    [Test]
    public async Task TransactionOnTheRowRangeLeader_RegistersEverythingLocally()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        (PlacementSpan span, InProcessSchemaCluster.Node leader) = await RowRangeLeaderAsync(cluster, db);

        using RegistrationProbe probe = new();
        KvTransaction tx = await RunTransferAsync(leader, db, ids[0]);

        Assert.Multiple(() =>
        {
            Assert.That(PinnedPartitionOf(tx), Is.EqualTo(span.PartitionId), "the session is pinned to the row range's partition");
            Assert.That(probe.Local, Is.GreaterThan(0), "the transaction registered operations");
            Assert.That(probe.Forwarded, Is.EqualTo(0),
                "a session pinned to the executing node's own partition forwards no registration");
        });
        Assert.That(await BalanceAsync(leader, db, ids[0]), Is.EqualTo(101L));
    }

    [Test]
    public async Task TransactionOffTheRowRangeLeader_IsPinnedToTheSamePartition()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        (PlacementSpan span, InProcessSchemaCluster.Node leader) = await RowRangeLeaderAsync(cluster, db);
        InProcessSchemaCluster.Node follower = cluster.Nodes.First(n => n.Index != leader.Index);

        using RegistrationProbe probe = new();
        KvTransaction tx = await RunTransferAsync(follower, db, ids[0]);

        Assert.Multiple(() =>
        {
            Assert.That(PinnedPartitionOf(tx), Is.EqualTo(span.PartitionId),
                "every node pins the table's sessions to the same partition");
            Assert.That(probe.Forwarded, Is.GreaterThan(0),
                "executed away from the row range's leader, the registrations go to that leader");
        });
        Assert.That(await BalanceAsync(leader, db, ids[0]), Is.EqualTo(101L));
    }

    [Test]
    public async Task SplitDuringTheTransaction_TheCommitStillReachesThePinnedSession()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        (PlacementSpan span, InProcessSchemaCluster.Node leader) = await RowRangeLeaderAsync(cluster, db);
        InProcessSchemaCluster.Node node = cluster.Nodes.First(n => n.Index != leader.Index);
        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));

        // Open the session before the split, as the first statement of a transaction does. Kahuna
        // refuses a split while a transaction holds a write inside the range, so the write comes after.
        KvTransaction tx = await node.Database!.Transactions.BeginAsync(deferStart: true);
        await tx.EnsureSessionStartedAsync(default, table.Store.PlacementGroup);
        Assert.That(PinnedPartitionOf(tx), Is.EqualTo(span.PartitionId));

        string splitKey = KeyRangeSplitHarness.MedianRowKey(table, await KeyRangeSplitHarness.ScanRowIdsAsync(node, table));
        await KeyRangeSplitHarness.SplitAtAsync(cluster, table.Store.RowKeySpace, splitKey);

        // Every row: the median split key leaves rows on both sides, so the session registers writes
        // that land in both new ranges.
        ExecuteNonSQLResult update = await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: db, sql: "UPDATE accounts SET balance = balance + 1 WHERE balance >= 0", parameters: null));
        Assert.That(update.ModifiedRows, Is.EqualTo(RowCount));
        await node.Database.Transactions.CommitAsync(tx);

        Assert.That(PinnedPartitionOf(tx), Is.EqualTo(span.PartitionId), "the pin does not move with the range");
        foreach (string id in ids)
            Assert.That(await BalanceAsync(node, db, id), Is.EqualTo(101L), $"row {id} committed");
    }

    [Test]
    public async Task DividedRowSpace_StartsUnpinned_AndCommits()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync();
        await using InProcessSchemaCluster _ = cluster;

        InProcessSchemaCluster.Node node = cluster.Nodes[0];
        TableDescriptor table = await node.Executor.OpenTable(new OpenTableTicket(db, "accounts"));
        string splitKey = KeyRangeSplitHarness.MedianRowKey(table, await KeyRangeSplitHarness.ScanRowIdsAsync(node, table));
        await KeyRangeSplitHarness.SplitAtAsync(cluster, table.Store.RowKeySpace, splitKey);

        KvTransaction tx = await RunTransferAsync(node, db, ids[0]);

        Assert.That(tx.CoordinatorKey, Is.EqualTo(tx.UniqueId),
            "the partition of a divided row space depends on the row key, so the session keeps the bare id");
        Assert.That(await BalanceAsync(node, db, ids[0]), Is.EqualTo(101L));
    }
}
