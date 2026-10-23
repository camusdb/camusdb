/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// The lock-free serializable read-only snapshot under key-range sharding, across nodes: the
/// snapshot runs on one node and the writer on another, so the range locks and write intents the
/// writer places live on the partition leader of the slot's index range rather than on the reader's
/// node. A writer on the slot the snapshot looked up is neither refused by the snapshot nor refuses
/// it, and the snapshot's reads stay pinned at its timestamp throughout.
///
/// <para>This is the cluster half of the standalone fixture of the same name. The shape is the one
/// a workload of index lookups against moving rows produces, and it is where the snapshot's shared
/// range lock used to surface as a conflict on the lookup and as a lock-wait on the writer.</para>
///
/// <para>Every writer starts after the snapshot. Kahuna lets a snapshot read pass a foreign write
/// intent that cannot commit at or before the snapshot's timestamp, which a younger writer's cannot;
/// an older in-flight writer is waited for by every snapshot read, lock or no lock.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing).
[NonParallelizable]
public sealed class TestSerializableReadOnlyKeyRangeCluster
{
    private const int Partitions = 2;
    private const int Slots = 20;

    /// <summary>A hang guard on a snapshot lookup; a lookup refused by a lock fails with a conflict first.</summary>
    private static readonly TimeSpan ReadBudget = TimeSpan.FromSeconds(5);

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    private static Task DdlAsync(InProcessSchemaCluster.Node node, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: db, sql: sql, parameters: null))
            .WaitAsync(TimeSpan.FromSeconds(60));

    /// <summary>
    /// Starts a three-node key-range cluster with <c>tokens</c>, its Integer64 index on <c>slot</c>
    /// (range-routed), and one row per slot, and waits for the schema on every node.
    /// </summary>
    private static async Task<(InProcessSchemaCluster cluster, string db)> SetupAsync()
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });

        try
        {
            string db = cluster.NextSchemaLogDatabaseName();
            await cluster.OpenDatabaseOnAllNodesAsync(db);

            long version = 0;
            await cluster.RunOnSchemaLeaderAsync(db, async leader =>
            {
                await DdlAsync(leader, db, "CREATE TABLE tokens (id INT64 PRIMARY KEY, slot INT64, token STRING)");
                await DdlAsync(leader, db, "CREATE INDEX tokens_slot ON tokens (slot)");
                version = leader.Database!.Schema.SchemaVersion;
            });

            await cluster.WaitForSchemaConvergenceAsync(db, version, timeout: TimeSpan.FromSeconds(30));

            InProcessSchemaCluster.Node seeder = cluster.Nodes[0];
            KvTransaction tx = await seeder.Database!.Transactions.BeginAsync();
            try
            {
                for (int slot = 1; slot <= Slots; slot++)
                {
                    await seeder.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db,
                        sql: $"INSERT INTO tokens (id, slot, token) VALUES ({slot}, {slot}, 'token-{slot}')", parameters: null));
                }

                await seeder.Database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await seeder.Database.Transactions.RollbackIfNotCompletedAsync(tx);
            }

            return (cluster, db);
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>The tokens the index lookup of <paramref name="slot"/> returns in <paramref name="tx"/> on <paramref name="node"/>.</summary>
    private static async Task<List<string>> LookupSlot(InProcessSchemaCluster.Node node, string db, KvTransaction tx, long slot)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: db, sql: $"SELECT token FROM tokens WHERE slot = {slot}", parameters: null));

        List<string> tokens = [];

        await foreach (QueryResultRow row in cursor)
            tokens.Add(row.Row["token"].StrValue!);

        return tokens;
    }

    /// <summary>A snapshot lookup that must complete inside <see cref="ReadBudget"/>.</summary>
    private static async Task<List<string>> LookupSlotPromptly(InProcessSchemaCluster.Node node, string db, KvTransaction tx, long slot)
    {
        long started = Stopwatch.GetTimestamp();
        List<string> tokens = await LookupSlot(node, db, tx, slot);

        Assert.That(Stopwatch.GetElapsedTime(started), Is.LessThan(ReadBudget),
            "a snapshot lookup must not wait on a writer");

        return tokens;
    }

    /// <summary>
    /// Runs the scenario with the snapshot on one node and the writer on another: the snapshot looks
    /// slot 7 up, the younger writer runs <paramref name="writerSql"/> against slot 7 and stays open,
    /// the snapshot looks slot 7 up again under the writer's locks and intents, the writer commits,
    /// and the snapshot still reads its pinned instant.
    /// </summary>
    private static Task RunScenario(
        CamusIsolationLevel writerIsolation, KeyValueTransactionLocking writerLocking, params string[] writerSql)
        => RunScenario(
            transactions => transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly),
            writerIsolation, writerLocking, writerSql);

    /// <summary>
    /// The scenario with the reader begun by <paramref name="beginReader"/>: an explicit serializable
    /// read-only transaction, or the snapshot every autocommit SELECT begins.
    /// </summary>
    private static async Task RunScenario(
        Func<KvTransactionsManager, Task<KvTransaction>> beginReader,
        CamusIsolationLevel writerIsolation, KeyValueTransactionLocking writerLocking, params string[] writerSql)
    {
        (InProcessSchemaCluster cluster, string db) = await SetupAsync();

        try
        {
            InProcessSchemaCluster.Node readerNode = cluster.Nodes[1];
            InProcessSchemaCluster.Node writerNode = cluster.Nodes[2];

            KvTransaction snapshot = await beginReader(readerNode.Database!.Transactions);
            CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(readerNode, db, snapshot, 7));

            // Two nodes order their HLCs by physical millisecond first. The lookup above already took
            // longer than one, and this pause keeps the writer younger than the snapshot explicitly.
            await Task.Delay(10);

            KvTransaction writer = await writerNode.Database!.Transactions.BeginAsync(writerIsolation, locking: writerLocking);
            try
            {
                Assert.DoesNotThrowAsync(async () =>
                {
                    foreach (string sql in writerSql)
                        await writerNode.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: writer, database: db, sql: sql, parameters: null));
                }, "a writer on the slot the snapshot looked up must not be refused");

                CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(readerNode, db, snapshot, 7),
                    "the snapshot must read its pinned instant while a younger writer on another node holds the slot");

                Assert.DoesNotThrowAsync(async () => await writerNode.Database.Transactions.CommitAsync(writer),
                    "the writer's commit must not be fenced by the snapshot");
            }
            finally
            {
                await writerNode.Database.Transactions.RollbackIfNotCompletedAsync(writer);
            }

            CollectionAssert.AreEqual(new[] { "token-7" }, await LookupSlotPromptly(readerNode, db, snapshot, 7),
                "the snapshot stays pinned after the writer commits");
            Assert.AreEqual(0, snapshot.GetAcquiredRangeLocks().Count, "the snapshot took no range lock throughout");
            await readerNode.Database.Transactions.CommitAsync(snapshot);

            KvTransaction latest = await cluster.Nodes[0].Database!.Transactions.BeginAsync();
            try
            {
                CollectionAssert.AreEqual(new[] { "moved" }, await LookupSlot(cluster.Nodes[0], db, latest, 7),
                    "a latest-state read on a third node sees the writer's committed change");
            }
            finally
            {
                await cluster.Nodes[0].Database.Transactions.RollbackIfNotCompletedAsync(latest);
            }
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// A pessimistic Serializable read-write <c>UPDATE</c> of the slot: its locate scan takes an exclusive
    /// lock over the slot's index range and it stages the row behind the slot.
    /// </summary>
    [Test]
    public Task Cluster_Snapshot_UpdateOfTheLookedUpSlot_NeitherRefusesNorIsRefused()
        => RunScenario(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic,
            "UPDATE tokens SET token = 'moved' WHERE slot = 7");

    /// <summary>
    /// The move shape: an optimistic Read Committed writer deletes the row behind the slot and inserts a
    /// new one, so the slot's index entries carry live intents when the snapshot looks it up.
    /// </summary>
    [Test]
    public Task Cluster_Snapshot_MoveOfTheLookedUpSlot_NeitherRefusesNorIsRefused()
        => RunScenario(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic,
            "DELETE FROM tokens WHERE slot = 7",
            "INSERT INTO tokens (id, slot, token) VALUES (107, 7, 'moved')");

    /// <summary>
    /// The autocommit SELECT under key-range sharding, across nodes: the reader is the zero-identity
    /// snapshot <see cref="KvTransactionsManager.BeginReadOnly"/> returns, the begin of every HTTP and
    /// gRPC autocommit read. A younger pessimistic writer on another node updates the slot it looked up.
    /// The read is not refused, takes no range lock, and does not fence the writer's commit.
    /// </summary>
    [Test]
    public Task Cluster_AutocommitRead_UpdateOfTheLookedUpSlot_NeitherRefusesNorIsRefused()
        => RunScenario(transactions => Task.FromResult(transactions.BeginReadOnly()),
            CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic,
            "UPDATE tokens SET token = 'moved' WHERE slot = 7");
}
