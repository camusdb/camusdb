
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;
using Kahuna.Shared.Communication.Rest;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Proves a hot range divides itself on a real multi-node cluster, with no administrative call.
///
/// <para>Every other cluster split fixture drives <c>SplitRangeAtKeyWithOutcomeAsync</c> by hand, so
/// it proves a split <i>can</i> be committed and nothing about whether one ever happens on its own.
/// The standalone fixture (<c>CamusDB.Tests.Storage.TestKeyRangeLoadSplitStandalone</c>) covers the
/// single-node path; this one covers what only a cluster can show — that the decision survives the
/// split being committed by the meta-partition leader rather than by the node under load.</para>
/// </summary>
[TestFixture]
// Serial: boots a multi-node in-process cluster (port contention / Raft timing) and asserts on the
// process-wide Kahuna meter.
[NonParallelizable]
public sealed class TestKeyRangeAutoSplitCluster
{
    private const int Partitions = 2;

    private const int SeedRows = 40;

    private const int BatchSize = 10;

    private static readonly TimeSpan LoadDuration = TimeSpan.FromSeconds(10);

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger = sharedLoggerFactory.CreateLogger<ICamusDB>();

    /// <summary>
    /// Load-branch settings for the fixture, all far below their production defaults so the debounce
    /// window elapses inside a test rather than in a quarter of a minute.
    /// </summary>
    private static void ConfigureLoadSplit(EmbeddedKahunaOptions options)
    {
        // Any write rate at all counts as hot. The fixture is about the decision path, not about
        // calibrating a realistic threshold.
        options.RangeSplitLoadThreshold = 0.001;

        // A memory WAL drains as fast as it fills, so a queue-depth gate above zero would never pass
        // and the predicate could never hold — the fixture would time out instead of failing honestly.
        options.RangeSplitLoadMinQueueDepth = 0;

        options.RangeSplitLoadWindow = TimeSpan.FromMilliseconds(500);
        options.RangeSplitLoadPollInterval = TimeSpan.FromMilliseconds(200);

        // Kahuna requires the settle window to cover the leader-stability window, so the two move
        // down together.
        options.RangeSplitSettleWindow = TimeSpan.FromSeconds(1);
        options.MinLeaderStability = TimeSpan.FromSeconds(1);

        // Gossip the load reports without switching on the leader balancer. The signal is what the
        // split decision needs; the balancer moves leadership, and its passes add leadership churn
        // that this fixture's Raft timings are already fighting. What is asserted below is that the
        // child landed on a different *partition*, which does not depend on the balancer.
        options.EnableLoadReports = true;
    }

    [Test]
    public async Task SustainedWriteLoad_SplitsTheRowSpace_WithNoAdminCall()
    {
        await using InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(
                nodeCount: 3, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true },
                configureNode: ConfigureLoadSplit);

        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id",     ColumnType.Id),
                new ColumnInfo("amount", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(20)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        InProcessSchemaCluster.Node writer = cluster.Nodes[0];

        // Open through the executor rather than indexing TableDescriptors: that cache holds only
        // the tables this node has already opened, so a node that has merely applied the CREATE
        // has no entry yet and the indexer throws KeyNotFoundException.
        TableDescriptor table = await writer.Executor.OpenTable(new OpenTableTicket(db, "readings"));
        string keySpace = table.Store.RowKeySpace;

        Assert.AreEqual("KeyRange", KeyRangeSplitHarness.RoutingModeOn(writer, keySpace),
            "A hash-routed space has no descriptor for the split checker to act on, so this test " +
            "would pass without exercising the load branch at all");

        Assert.AreEqual(1, KeyRangeSplitHarness.DescriptorsOn(writer, keySpace).Count,
            "A freshly registered space starts as one whole-space range");

        (int committed, int aborted, List<long> amounts) = await DriveWriteLoadAsync(cluster, db, writer);

        Assert.Greater(committed, 0,
            $"Every write batch was aborted ({aborted} of them), so no partition ever went hot and " +
            "nothing below would prove anything");

        int descriptors = await WaitForDescriptorsAsync(writer, keySpace, atLeast: 2, budget: TimeSpan.FromSeconds(30));

        Assert.Greater(descriptors, 1,
            "The row space never divided. No call here splits it, so either the load knobs did not " +
            "reach the nodes, the load-check actor never started, or the meta-partition leader never " +
            "saw the writing partition as hot.");

        List<KahunaRangeDescriptorResponse> after = KeyRangeSplitHarness.DescriptorsOn(writer, keySpace);
        KeyRangeSplitHarness.AssertCoversSpaceContiguously(after, keySpace, writer.Index);
        KeyRangeSplitHarness.AssertSpansMultiplePartitions(after, keySpace);

        // Every acknowledged row must survive a boundary that moved underneath the writes.
        List<long> readBack = await ScanAmountsAsync(writer, db);
        HashSet<long> expected = [.. Enumerable.Range(0, SeedRows).Select(value => (long)value)];
        expected.UnionWith(amounts);

        Assert.AreEqual(expected.Count, readBack.Count,
            "Every acknowledged row must still be readable after the split, and none may be duplicated");

        foreach (long amount in expected)
            Assert.IsTrue(readBack.Contains(amount), $"The row with amount={amount} was acknowledged and is now missing");
    }

    /// <summary>
    /// Seeds the table and then keeps writing for the whole budget, returning the amounts that were
    /// actually acknowledged.
    ///
    /// <para>An aborted batch is counted and skipped, not failed, and its amounts are not returned. A
    /// write across a moving boundary answers <c>CADB0504</c> or <c>CADB0502</c>, and a multi-statement
    /// transaction must restart — the retry contract a real client works under. The counts are
    /// returned so the caller can prove the load was real rather than entirely refused.</para>
    /// </summary>
    private static async Task<(int Committed, int Aborted, List<long> Amounts)> DriveWriteLoadAsync(
        InProcessSchemaCluster cluster, string db, InProcessSchemaCluster.Node writer)
    {
        await InsertBatchAsync(db, writer, startingAt: 0, count: SeedRows);

        Stopwatch elapsed = Stopwatch.StartNew();
        List<long> amounts = [];
        int committed = 0;
        int aborted = 0;

        for (int batch = 0; elapsed.Elapsed < LoadDuration; batch++)
        {
            int startingAt = SeedRows + (batch * BatchSize);

            try
            {
                await InsertBatchAsync(db, writer, startingAt, BatchSize);
                committed++;

                for (int i = 0; i < BatchSize; i++)
                    amounts.Add(startingAt + i);
            }
            catch (CamusDBException)
            {
                aborted++;
            }
        }

        return (committed, aborted, amounts);
    }

    private static async Task InsertBatchAsync(string db, InProcessSchemaCluster.Node writer, int startingAt, int count)
    {
        KvTransaction tx = await writer.Database!.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
            await writer.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "amount", new(ColumnType.Integer64, (long)(startingAt + i)) },
                }}));

        await writer.Database.Transactions.CommitAsync(tx);
    }

    /// <summary>
    /// Polls one node's descriptor set until it reaches <paramref name="atLeast"/> or the budget runs
    /// out. The split checker runs on its own timer on the meta-partition leader, and the resulting
    /// descriptors then replicate, so the map usually changes after the load stops rather than during
    /// it.
    /// </summary>
    private static async Task<int> WaitForDescriptorsAsync(
        InProcessSchemaCluster.Node node, string keySpace, int atLeast, TimeSpan budget)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        int count = KeyRangeSplitHarness.DescriptorsOn(node, keySpace).Count;

        while (count < atLeast && elapsed.Elapsed < budget)
        {
            await Task.Delay(250);
            count = KeyRangeSplitHarness.DescriptorsOn(node, keySpace).Count;
        }

        return count;
    }

    /// <summary>Reads every row's <c>amount</c> through an ordinary autocommit SELECT.</summary>
    private static async Task<List<long>> ScanAmountsAsync(InProcessSchemaCluster.Node node, string db)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db, "SELECT amount FROM readings", null));

        List<long> amounts = [];

        await foreach (QueryResultRow row in cursor)
            amounts.Add(row.Row["amount"].LongValue);

        return amounts;
    }
}
