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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// A pessimistic child INSERT keeps Kahuna's one-phase commit when its parent lives on another
/// partition. The child takes a shared rendezvous lock on the parent's unique-index key and reads it,
/// but a lock is not a written key, and a pessimistic transaction has no read set to validate, so the
/// commit still has one partition and enters the one-phase gate.
///
/// <para>This needs a cluster of several nodes: the gate rules for a lock or a read beyond the written
/// keys apply only to a Raft group that a remote replica can join. The gate verdict is recorded on
/// Kahuna's process-wide meter (<c>kahuna.durable_tx.one_phase_gate</c>, tag <c>outcome</c>), from the
/// coordinator's context, so the test measures the change around its own transactions and runs
/// serially. Two controls frame the result: an INSERT into a table with no foreign key enters the gate
/// the same way, and an optimistic child does not enter it. An optimistic child validates its read
/// set at commit, and the rendezvous lock is a range lock over one key, which the gate counts as a
/// predicate (<c>predicate_read</c>) on any partition.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterForeignKeyOnePhaseCommit
{
    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>Sums the gate verdicts by <c>outcome</c> while it is alive.</summary>
    private sealed class GateProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly Dictionary<string, long> outcomes = new(StringComparer.Ordinal);

        public GateProbe()
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Kahuna" && instrument.Name == "kahuna.durable_tx.one_phase_gate")
                    l.EnableMeasurementEvents(instrument);
            };

            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key != "outcome" || tag.Value?.ToString() is not { } outcome)
                        continue;

                    lock (outcomes)
                        outcomes[outcome] = outcomes.GetValueOrDefault(outcome) + value;
                }
            });

            listener.Start();
        }

        public long Count(string outcome)
        {
            lock (outcomes)
                return outcomes.GetValueOrDefault(outcome);
        }

        public string Describe()
        {
            lock (outcomes)
                return string.Join(", ", outcomes.Select(kv => $"{kv.Key}={kv.Value}"));
        }

        public void Dispose() => listener.Dispose();
    }

    [Test]
    public async Task PessimisticChildInsertCommitsInOnePhaseAcrossPartitions()
    {
        // The production options builder turns apply-time validation on (EmbeddedKahunaOptionsBuilder);
        // this harness builds its nodes directly and would inherit Kahuna's default, which is off. With
        // it off, every INSERT that checks its own unique keys leaves the gate on a validated base, with
        // or without a foreign key, and the test would measure nothing.
        await using InProcessSchemaCluster cluster = await InProcessSchemaCluster.StartAsync(
            nodeCount: 3, partitions: 3, loggerFactory: sharedLoggerFactory, logger: logger,
            configureNode: node => node.OnePhaseApplyTimeValidation = true);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await DdlOnLeaderAsync(cluster, db, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await DmlOnLeaderAsync(cluster, db, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        // Pick a child table whose rows hash to another partition than the parent's, so the rendezvous
        // read is an off-partition read. Each table's placement group is its own id.
        int parentPartition = await PartitionOfAsync(cluster, db, "cities");
        string? child = null;

        for (int attempt = 0; attempt < 12 && child is null; attempt++)
        {
            string candidate = "weather" + attempt;
            await DdlOnLeaderAsync(cluster, db, $"CREATE TABLE {candidate} (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");

            if (await PartitionOfAsync(cluster, db, candidate) != parentPartition)
                child = candidate;
        }

        Assert.IsNotNull(child, "No child table landed on another partition than its parent");

        InProcessSchemaCluster.Node dataLeader = await DataLeaderAsync(cluster, db, child!);

        const int Inserts = 3;

        // The baseline: the same INSERT shape into a table with no foreign key enters the gate. A
        // failure below that this baseline shares is not caused by the foreign key.
        await DdlOnLeaderAsync(cluster, db, "CREATE TABLE plain (id int64 PRIMARY KEY NOT NULL, city string)");
        InProcessSchemaCluster.Node plainLeader = await DataLeaderAsync(cluster, db, "plain");

        using (GateProbe gate = new())
        {
            for (int i = 0; i < Inserts; i++)
                await InsertChildAsync(plainLeader, db, "plain", 100 + i, KeyValueTransactionLocking.Pessimistic);

            TestContext.Out.WriteLine($"Baseline inserts without a foreign key: {gate.Describe()}");
            Assert.That(gate.Count("entered"), Is.GreaterThanOrEqualTo(Inserts), "Baseline: " + gate.Describe());
        }

        using (GateProbe gate = new())
        {
            for (int i = 0; i < Inserts; i++)
                await InsertChildAsync(dataLeader, db, child!, 100 + i, KeyValueTransactionLocking.Pessimistic);

            TestContext.Out.WriteLine($"Pessimistic child inserts: {gate.Describe()}");

            Assert.That(gate.Count("entered"), Is.GreaterThanOrEqualTo(Inserts), gate.Describe());
            Assert.AreEqual(0, gate.Count("off_partition_read"), gate.Describe());
            Assert.AreEqual(0, gate.Count("predicate_read"), gate.Describe());
            Assert.AreEqual(0, gate.Count("held_lock"), gate.Describe());
            Assert.AreEqual(0, gate.Count("multi_partition"), gate.Describe());
        }

        using (GateProbe gate = new())
        {
            await InsertChildAsync(dataLeader, db, child!, 200, KeyValueTransactionLocking.Optimistic);

            TestContext.Out.WriteLine($"Optimistic child insert: {gate.Describe()}");

            // The verdict is predicate_read: the rendezvous lock is a range lock over one key, and a
            // validated transaction that holds a range lock cannot be checked at apply time. That holds
            // whatever partition the parent is on.
            Assert.AreEqual(0, gate.Count("entered"), "An optimistic child validates its read set, so the gate stays closed: " + gate.Describe());
            Assert.That(gate.Count("predicate_read"), Is.GreaterThanOrEqualTo(1), gate.Describe());
        }

        // The constraint is enforced on the same path the gate measured.
        CamusDBException orphan = Assert.ThrowsAsync<CamusDBException>(async () =>
            await InsertChildAsync(dataLeader, db, child!, 300, KeyValueTransactionLocking.Pessimistic, city: "atlantis"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, orphan.Code, orphan.Message);
    }

    /// <summary>
    /// An explicit transaction with a deferred start, so its session anchors on the child's own
    /// partition when the INSERT touches it first.
    /// </summary>
    private static async Task InsertChildAsync(
        InProcessSchemaCluster.Node node, string db, string table, int id, KeyValueTransactionLocking locking, string city = "lima")
    {
        DatabaseDescriptor database = node.Database!;
        KvTransaction tx = await database.Transactions.BeginAsync(locking: locking, deferStart: true);
        try
        {
            await node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                tx, db, $"INSERT INTO {table} (id, city) VALUES ({id}, '{city}')", null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> PartitionOfAsync(InProcessSchemaCluster cluster, string db, string table)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            InProcessSchemaCluster.Node any = cluster.Nodes[0];
            TableDescriptor descriptor = await any.Executor.OpenTable(new OpenTableTicket(db, table));
            TablePlacement placement = any.Kahuna.ReadPlacementUncached(descriptor.Store.RowKeySpace, out bool initialized);

            if (initialized && placement.Spans.Count == 1)
                return placement.Spans[0].PartitionId;

            await Task.Delay(200);
        }

        throw new Exception($"No placement for table '{table}' became known");
    }

    /// <summary>The node that leads the partition of the table's rows, so the transaction runs where its anchor is.</summary>
    private static async Task<InProcessSchemaCluster.Node> DataLeaderAsync(InProcessSchemaCluster cluster, string db, string table)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            InProcessSchemaCluster.Node any = cluster.Nodes[0];
            TableDescriptor descriptor = await any.Executor.OpenTable(new OpenTableTicket(db, table));
            TablePlacement placement = any.Kahuna.ReadPlacementUncached(descriptor.Store.RowKeySpace, out bool initialized);
            string? leader = initialized && placement.Spans.Count == 1 ? placement.Spans[0].LeaderEndpoint : null;

            if (leader is not null && cluster.Nodes.FirstOrDefault(n => n.Kahuna.Raft.GetLocalEndpoint() == leader) is { } node)
                return node;

            await Task.Delay(200);
        }

        throw new Exception($"No data leader for table '{table}' became known");
    }

    private static async Task DdlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql)
    {
        long version = 0;

        await cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            await leader.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
                .WaitAsync(TimeSpan.FromSeconds(60));
            version = leader.Database!.Schema.SchemaVersion;
        });

        await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));
    }

    private static Task DmlOnLeaderAsync(InProcessSchemaCluster cluster, string db, string sql) =>
        cluster.RunOnSchemaLeaderAsync(db, async leader =>
        {
            KvTransaction tx = await leader.Database!.Transactions.BeginAsync();
            try
            {
                await leader.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
                await leader.Database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await leader.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        });
}
