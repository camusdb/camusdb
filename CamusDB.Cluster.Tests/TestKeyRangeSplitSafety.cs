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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Establishes that a CamusDB table's key space can actually be divided into ranges owned by
/// different Raft partitions, and that CamusDB observes the result — the precondition for every
/// other claim about behavior across a range boundary.
///
/// <para>Before this fixture, no test in either test project split a live table's space: the
/// key-range coverage proved a space could be <i>registered</i> and that the routing mode reached
/// every node, which is the state before any split. Everything downstream — scans that must merge
/// across children, an index backfill whose target divides underneath it, locks that must follow a
/// range to its child — was therefore only reasoned about, never observed.</para>
///
/// <para>Each test asserts on the descriptor set Kahuna reports, not on a query happening to return
/// the right rows. A refused split leaves one range covering everything, under which every query
/// still returns exactly the right answer — so a test that checks only its rows passes identically
/// whether or not the split it claims to exercise happened.</para>
/// </summary>
[TestFixture]
// Serial: boots in-process clusters, which contend for ports and skew each other's Raft election
// timing when run alongside another cluster fixture.
[NonParallelizable]
public sealed class TestKeyRangeSplitSafety
{
    // Two partitions: the reserved range-map meta partition (0) plus a data partition to seed onto.
    // A split allocates further partitions beyond this as it needs them.
    private const int Partitions = 2;

    private const int RowCount = 40;

    /// <summary>Writes each node must have acknowledged before a split is attempted.</summary>
    private const int WritesBeforeSplit = 2;

    /// <summary>Writes that keep flowing after cutover, so the post-split path is covered too.</summary>
    private const int WritesAfterSplit = 10;

    /// <summary>Backstop so a split that never lands cannot turn a writer into an infinite loop.</summary>
    private const int WriterHardCap = 300;

    private static readonly ILoggerFactory sharedLoggerFactory = LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning).AddConsole());

    private static readonly ILogger<ICamusDB> logger =
        sharedLoggerFactory.CreateLogger<ICamusDB>();

    // -----------------------------------------------------------------------
    // Fixture setup
    // -----------------------------------------------------------------------

    /// <summary>
    /// Starts a cluster with key-range sharding on, creates a table with a secondary index over an
    /// Integer64 column, and fills it.
    ///
    /// <para>The index column is deliberately <c>Integer64</c> rather than a string: only indexes
    /// whose key columns all use the non-String ordered encoding are registered for key-range
    /// routing, so a String-keyed index would stay hash-routed and its space would have no descriptor
    /// to split — the index half of this fixture would then be testing nothing.</para>
    ///
    /// <para>Rows are written through one node and every id is returned, because a split key has to
    /// be a key that really exists on both sides of the division; Kahuna refuses a split that would
    /// leave a child range empty.</para>
    /// </summary>
    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids)> SetupAsync(
        int nodeCount)
    {
        InProcessSchemaCluster cluster =
            await InProcessSchemaCluster.StartAsync(nodeCount: nodeCount, partitions: Partitions,
                loggerFactory: sharedLoggerFactory, logger: logger,
                options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });

        // The caller disposes the cluster it is handed, so anything that throws before the hand-off
        // has to tear it down here. A cluster left running keeps its Raft nodes and ports alive and
        // starves the next fixture's elections, which turns one real failure into a run of unrelated
        // proposal timeouts that hide it.
        try
        {
            return await FillAsync(cluster);
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    private static async Task<(InProcessSchemaCluster cluster, string db, List<string> ids)> FillAsync(
        InProcessSchemaCluster cluster)
    {
        string db = cluster.NextSchemaLogDatabaseName();
        await cluster.OpenDatabaseOnAllNodesAsync(db);

        await cluster.RunOnSchemaLeaderAsync(db, leader => leader.Executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id",     ColumnType.Id),
                new ColumnInfo("label",  ColumnType.String, notNull: true),
                new ColumnInfo("amount", ColumnType.Integer64),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)]),
                new ConstraintInfo(ConstraintType.IndexMulti, "amount_idx",
                    [new ColumnIndexInfo("amount", OrderType.Ascending)]),
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(30)));

        await cluster.WaitForSchemaConvergenceAsync(db, version: 1);

        List<string> ids = await InsertRowsAsync(cluster.Nodes[0], db, RowCount);

        // Open the table on every node so each one registers the spaces for key-range routing.
        // Registration is node-local: a node that never opened the table would hash-route the space
        // even while holding its descriptors, and would read the wrong partition after a split.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            await node.Executor.OpenTable(new OpenTableTicket(db, "readings"));

        return (cluster, db, ids);
    }

    private static async Task<List<string>> InsertRowsAsync(
        InProcessSchemaCluster.Node node, string db, int count)
    {
        List<string> ids = [];
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        for (int i = 0; i < count; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);

            await node.Executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        id) },
                    { "label",  new(ColumnType.String,    $"reading-{i}") },
                    { "amount", new(ColumnType.Integer64, (long)i) },
                }}));
        }

        await node.Database.Transactions.CommitAsync(tx);
        return ids;
    }

    private static Task<TableDescriptor> TableAsync(InProcessSchemaCluster.Node node, string db)
        => node.Executor.OpenTable(new OpenTableTicket(db, "readings"));

    /// <summary>
    /// The immutable key id of a named index, which is the segment that appears in its KV keys —
    /// not the index's display name, which a rename can change without moving any data.
    /// </summary>
    private static string IndexKvId(TableDescriptor table, string indexName)
    {
        TableIndexSchema index = table.Indexes[indexName];

        Assert.That(index.Id, Is.Not.Null.And.Not.Empty,
            $"Index '{indexName}' has no immutable id; its key space cannot be addressed");

        return index.Id!;
    }

    // -----------------------------------------------------------------------
    // 1. A table's row space can be split across a real multi-node cluster.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowSpace_SplitsAtChosenKey_AndEveryNodeAppliesTheDescriptors()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string keySpace = table.Store.RowKeySpace;

        List<KahunaRangeDescriptorResponse> before =
            KeyRangeSplitHarness.DescriptorsOn(cluster.Nodes[0], keySpace);

        Assert.That(before, Has.Count.EqualTo(1),
            "A freshly registered space must start as a single whole-space range; " +
            "starting from more than one would mean an earlier split leaked into this test");

        string splitKey = KeyRangeSplitHarness.MedianRowKey(
            table, await KeyRangeSplitHarness.ScanRowIdsAsync(cluster.Nodes[0], table));
        int after = await KeyRangeSplitHarness.SplitAtAsync(cluster, keySpace, splitKey);

        Assert.That(after, Is.EqualTo(2), "The split must produce exactly two child ranges");

        List<KahunaRangeDescriptorResponse> descriptors =
            KeyRangeSplitHarness.DescriptorsOn(cluster.Nodes[0], keySpace);

        // The division must land where it was asked to land. Kahuna reports success for a split it
        // committed, but the boundary is what determines which rows each partition now owns, so it is
        // checked rather than trusted.
        Assert.That(descriptors[0].EndKey, Is.EqualTo(splitKey),
            "The lower child must end exactly at the requested split key");
        Assert.That(descriptors[1].StartKey, Is.EqualTo(splitKey),
            "The upper child must begin exactly at the requested split key");

        KeyRangeSplitHarness.AssertSpansMultiplePartitions(descriptors, keySpace);
    }

    // -----------------------------------------------------------------------
    // 2. A secondary index's space splits too — index keys route independently
    //    of row keys, so proving the row space splits says nothing about them.
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexSpace_SplitsAtChosenKey_AndEveryNodeAppliesTheDescriptors()
    {
        (InProcessSchemaCluster cluster, string db, List<string> _) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string indexId = IndexKvId(table, "amount_idx");
        string keySpace = table.Store.IndexKeySpace(indexId);

        Assert.That(KeyRangeSplitHarness.RoutingModeOn(cluster.Nodes[0], keySpace), Is.EqualTo("KeyRange"),
            "The Integer64-keyed index must be registered for key-range routing; if it is hash-routed " +
            "the eligibility rule changed and this test is no longer exercising an index split");

        // amount runs 0..RowCount-1, so the midpoint has index entries on both sides of it.
        string splitKey = KeyRangeSplitHarness.IndexKeyAt(
            table, indexId, new CompositeColumnValue(new ColumnValue(ColumnType.Integer64, (long)(RowCount / 2))));

        int after = await KeyRangeSplitHarness.SplitAtAsync(cluster, keySpace, splitKey);

        Assert.That(after, Is.EqualTo(2), "The index space must divide into exactly two child ranges");

        KeyRangeSplitHarness.AssertSpansMultiplePartitions(
            KeyRangeSplitHarness.DescriptorsOn(cluster.Nodes[0], keySpace), keySpace);
    }

    // -----------------------------------------------------------------------
    // 3. A space can be divided more than once. Splitting a child exercises a
    //    bounded range rather than the whole space, which is the case where an
    //    off-by-one in boundary handling would show up.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowSpace_SplitsTwice_ProducingThreeContiguousRanges()
    {
        (InProcessSchemaCluster cluster, string db, List<string> ids) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string keySpace = table.Store.RowKeySpace;

        List<ObjectIdValue> sorted = (await KeyRangeSplitHarness.ScanRowIdsAsync(cluster.Nodes[0], table))
            .OrderBy(id => id.ToString(), StringComparer.Ordinal)
            .ToList();

        await KeyRangeSplitHarness.SplitAtAsync(
            cluster, keySpace, table.Store.RowPointKey(sorted[sorted.Count / 2]));

        // Split the lower child. Its bounds are [-inf, median), so a row from the first quarter
        // falls inside it with rows on both sides.
        int after = await KeyRangeSplitHarness.SplitAtAsync(
            cluster, keySpace, table.Store.RowPointKey(sorted[sorted.Count / 4]));

        Assert.That(after, Is.EqualTo(3), "A second split of the lower child must yield three ranges");

        // Contiguity is asserted per node inside the harness; this re-states the shape at the level
        // the test is about, so a failure names three-way coverage rather than a generic gap.
        List<KahunaRangeDescriptorResponse> descriptors =
            KeyRangeSplitHarness.DescriptorsOn(cluster.Nodes[0], keySpace);

        Assert.That(descriptors.Select(d => d.StartKey).ToArray(),
            Is.Ordered.Using<string?>(Comparer<string?>.Create((a, b) =>
                a is null ? (b is null ? 0 : -1) : b is null ? 1 : string.CompareOrdinal(a, b))),
            "Descriptors must be reported in ascending start-key order");
    }

    // -----------------------------------------------------------------------
    // 4. A split key with nothing on one side must be refused, not silently
    //    accepted. This is what makes the other tests' assertions meaningful:
    //    it shows a refusal is observable rather than indistinguishable from
    //    success.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SplitKeyOutsideTheData_IsRefused_AndLeavesTheSpaceIntact()
    {
        (InProcessSchemaCluster cluster, string db, List<string> _) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string keySpace = table.Store.RowKeySpace;

        // Every row id is 24 lowercase hex characters, so a key ending in 'z' sorts above all of them
        // and the upper half of this split would hold nothing.
        string beyondEveryRow = keySpace + "/zzzzzzzzzzzzzzzzzzzzzzzz";

        KahunaSplitRangeResponse response = await cluster.Nodes[0].Kahuna.Kahuna
            .SplitRangeAtKeyWithOutcomeAsync(keySpace, beyondEveryRow, CancellationToken.None);

        Assert.That(response.Success, Is.False,
            "A split that would leave a child range empty must be refused");
        Assert.That(response.Determinate, Is.True,
            "A policy refusal is final — the map did not change and will not change later");

        Assert.That(KeyRangeSplitHarness.DescriptorsOn(cluster.Nodes[0], keySpace), Has.Count.EqualTo(1),
            "A refused split must leave the space as a single range");
    }

    // -----------------------------------------------------------------------
    // 5. After the cutover, a read issued from a node that did not execute the
    //    split must still see every row, in order. Descriptors replicate but
    //    routing mode does not, so this is where a node that applied one and
    //    not the other would read the wrong partition and silently return a
    //    subset.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ScanFromEveryNodeAfterCutover_ReturnsEveryRowInOrder()
    {
        (InProcessSchemaCluster cluster, string db, List<string> _) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string keySpace = table.Store.RowKeySpace;

        List<ObjectIdValue> rowIds = await KeyRangeSplitHarness.ScanRowIdsAsync(cluster.Nodes[0], table);

        List<string> expected = rowIds
            .Select(id => id.ToString())
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToList();

        Assert.That(expected, Has.Count.EqualTo(RowCount));

        await KeyRangeSplitHarness.SplitAtAsync(
            cluster, keySpace, KeyRangeSplitHarness.MedianRowKey(table, rowIds));

        // Every node, not just the one that committed the split: the node that executed the cutover
        // is the least interesting one, because it necessarily has the current map.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            TableDescriptor nodeTable = await TableAsync(node, db);

            List<string> seen = (await KeyRangeSplitHarness.ScanRowIdsAsync(node, nodeTable))
                .Select(id => id.ToString())
                .ToList();

            Assert.That(seen, Has.Count.EqualTo(RowCount),
                $"Node {node.Index} returned {seen.Count} of {RowCount} rows after the cutover; " +
                "a node that resolved only one child range returns a subset rather than an error");

            Assert.That(seen, Is.EqualTo(expected),
                $"Node {node.Index} must return exactly the same rows, in the same order, as before the split");
        }
    }
    // -----------------------------------------------------------------------
    // 6. Writes issued from nodes that are not executing the split. Only one
    //    node runs any given split, so in a cluster every other node is on the
    //    path a split-local guard cannot see — and a write that slips through
    //    commits onto the old owner, is acknowledged, and then disappears when
    //    the range routes to its new one.
    // -----------------------------------------------------------------------

    [Test]
    public async Task WritesFromEveryNodeWhileOneOfThemSplits_LoseNoAcknowledgedRow()
    {
        (InProcessSchemaCluster cluster, string db, List<string> seededIds) = await SetupAsync(nodeCount: 3);
        await using InProcessSchemaCluster scope = cluster;

        TableDescriptor table = await TableAsync(cluster.Nodes[0], db);
        string keySpace = table.Store.RowKeySpace;

        string splitKey = KeyRangeSplitHarness.MedianRowKey(
            table, await KeyRangeSplitHarness.ScanRowIdsAsync(cluster.Nodes[0], table));

        // Every node writes, so whichever one ends up committing the split, the other two were
        // writing from outside it. That is stronger than picking a non-executing node in advance,
        // and it does not depend on predicting which node wins the meta-partition leadership.
        List<string>[] acknowledged = [.. cluster.Nodes.Select(_ => new List<string>())];
        int[] attempted = new int[cluster.Nodes.Length];
        int stopAfter = int.MaxValue;
        object gate = new();

        List<Task> writers = [];

        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            int index = node.Index;

            writers.Add(Task.Run(async () =>
            {
                while (Volatile.Read(ref attempted[index]) < Volatile.Read(ref stopAfter)
                       && Volatile.Read(ref attempted[index]) < WriterHardCap)
                {
                    string id = ObjectIdGenerator.Generate().ToString();

                    if (await TryInsertOneAsync(node, db, id, 1_000 + Volatile.Read(ref attempted[index])))
                        lock (gate) acknowledged[index].Add(id);

                    Interlocked.Increment(ref attempted[index]);
                }
            }));
        }

        // Do not start the split until every node has a committed write behind it, so the split is
        // landing on a range all three nodes are actively writing to.
        DateTime warmupDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);

        while (DateTime.UtcNow < warmupDeadline)
        {
            lock (gate)
            {
                if (acknowledged.All(list => list.Count >= WritesBeforeSplit))
                    break;
            }

            await Task.Delay(20);
        }

        int[] acknowledgedBeforeSplit;
        lock (gate) acknowledgedBeforeSplit = [.. acknowledged.Select(list => list.Count)];

        (int ranges, int committedBy) = await KeyRangeSplitHarness.SplitAtWithinAsync(
            cluster, keySpace, splitKey, TimeSpan.FromSeconds(60));

        // Keep writing past cutover before letting the writers drain.
        Volatile.Write(ref stopAfter, attempted.Max() + WritesAfterSplit);

        await Task.WhenAll(writers);

        Assert.That(ranges, Is.GreaterThan(1), "The space never divided");

        for (int i = 0; i < cluster.Nodes.Length; i++)
            Assert.That(acknowledgedBeforeSplit[i], Is.GreaterThan(0),
                $"Node {i} had acknowledged nothing when the split began, so it was not one of the " +
                "nodes writing into the range as it moved");

        if (committedBy >= 0)
        {
            int fromOtherNodes = acknowledged
                .Where((_, index) => index != committedBy)
                .Sum(list => list.Count);

            Assert.That(fromOtherNodes, Is.GreaterThan(0),
                $"Only node {committedBy} — the one that ran the split — wrote anything, so the " +
                "cross-node case this test exists for went unexercised");
        }

        HashSet<string> expected = seededIds
            .Concat(acknowledged.SelectMany(list => list))
            .ToHashSet(StringComparer.Ordinal);

        // Read back from every node: a row can be missing because it was lost at cutover, or present
        // on one node and unreachable from another whose routing did not catch up.
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            HashSet<string> seen = await ReadAllIdsAsync(node, db);

            Assert.That(seen, Is.SupersetOf(expected),
                $"Node {node.Index} cannot see {expected.Except(seen).Count()} row(s) the client was " +
                "told had committed — the shape of a write that landed on the old owner after its " +
                "contents were copied to the new one");

            Assert.That(seen.Count, Is.EqualTo(expected.Count),
                $"Node {node.Index} returned rows nobody was told had committed");
        }
    }

    /// <summary>
    /// Inserts one row through <paramref name="node"/> in its own transaction, reporting whether the
    /// client was told it committed. A retryable failure is replayed from a fresh transaction, which is
    /// what CamusDB's autocommit path does with that error class; exhausting the retries reports "not
    /// acknowledged" rather than failing, because a write refused under contention is a correct outcome
    /// and the guarantee under test concerns writes that were acknowledged.
    /// </summary>
    private static async Task<bool> TryInsertOneAsync(
        InProcessSchemaCluster.Node node, string db, string id, long amount)
    {
        for (int attempt = 0; attempt < 6; attempt++)
        {
            KvTransaction tx = await node.Database!.Transactions.BeginAsync();

            try
            {
                await node.Executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "readings",
                    values: new() { new() {
                        { "id",     new(ColumnType.Id,        id) },
                        { "label",  new(ColumnType.String,    $"node{node.Index}-{amount}") },
                        { "amount", new(ColumnType.Integer64, amount) },
                    }}));

                await node.Database.Transactions.CommitAsync(tx);

                return true;
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
                await Task.Delay(20 * (attempt + 1));
            }
        }

        return false;
    }

    /// <summary>
    /// Every <c>id</c> the table holds as <paramref name="node"/> sees it, read through SQL — the same
    /// way a client connected to that node would find out whether its row is still there.
    /// </summary>
    private static async Task<HashSet<string>> ReadAllIdsAsync(InProcessSchemaCluster.Node node, string db)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await node.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: "SELECT id FROM readings", parameters: null));

            HashSet<string> ids = new(StringComparer.Ordinal);

            await foreach (QueryResultRow row in cursor)
                ids.Add(row.Row["id"].StrValue!);

            await node.Database.Transactions.CommitAsync(tx);

            return ids;
        }
        catch
        {
            await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

}
