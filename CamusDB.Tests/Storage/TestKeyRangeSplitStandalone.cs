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

using Kahuna.Shared.Communication.Rest;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Proves a standalone engine's table key spaces really can be divided into ranges on separate Raft
/// partitions, and that a refusal is distinguishable from a success. Everything that reasons about
/// behavior "across a split" depends on this being true, and nothing established it before.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node with several partitions and drives Raft-committed range
// splits, whose timing is disturbed by other node-booting fixtures running alongside.
[NonParallelizable]
public sealed class TestKeyRangeSplitStandalone : KeyRangeSplitFixture
{
    // -----------------------------------------------------------------------
    // 1. The row space of a standalone table divides into two ranges.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowSpace_SplitsAtChosenKey_OnAStandaloneNode()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        Assert.That(RoutingMode(keySpace), Is.EqualTo("KeyRange"),
            "The row space must be key-range routed before a split means anything");

        Assert.That(Descriptors(keySpace), Has.Count.EqualTo(1),
            "A freshly registered space starts as one whole-space range");

        string splitKey = MedianRowKey(table, await ScanRowIdsAsync(table, executor, db));
        int after = await SplitAtAsync(keySpace, splitKey);

        Assert.That(after, Is.EqualTo(2), "The split must produce exactly two ranges");

        List<KahunaRangeDescriptorResponse> descriptors = Descriptors(keySpace);

        // Where the boundary landed decides which partition owns which rows, so it is asserted
        // rather than inferred from the split having been accepted.
        Assert.That(descriptors[0].EndKey, Is.EqualTo(splitKey));
        Assert.That(descriptors[1].StartKey, Is.EqualTo(splitKey));

        Assert.That(descriptors.Select(d => d.PartitionId).Distinct().Count(), Is.GreaterThan(1),
            "The upper range must have moved onto a different partition; two ranges on one partition " +
            "would not exercise cross-partition routing");
    }

    // -----------------------------------------------------------------------
    // 2. A secondary index's space splits independently of the row space.
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexSpace_SplitsAtChosenKey_OnAStandaloneNode()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string indexId = IndexKvId(table, "amount_idx");
        string keySpace = table.Store.IndexKeySpace(indexId);

        Assert.That(RoutingMode(keySpace), Is.EqualTo("KeyRange"),
            "An Integer64-keyed index must be registered for key-range routing");

        // amount runs 0..RowCount-1, so the midpoint has entries on both sides.
        string splitKey = table.Store.IndexKeySpace(indexId) + "/" + KeyEncoder.Encode(
            new CompositeColumnValue(new ColumnValue(ColumnType.Integer64, (long)(RowCount / 2))), null);

        int after = await SplitAtAsync(keySpace, splitKey);

        Assert.That(after, Is.EqualTo(2), "The index space must divide into exactly two ranges");

        // The row space must be untouched: index and row keys are separate spaces, and a split of one
        // that moved the other would mean the key spaces are not as isolated as the layout assumes.
        Assert.That(Descriptors(table.Store.RowKeySpace), Has.Count.EqualTo(1),
            "Splitting the index space must not divide the row space");
    }

    // -----------------------------------------------------------------------
    // 3. Splitting a child range, not just the whole space.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowSpace_SplitsTwice_ProducingThreeContiguousRanges()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        List<ObjectIdValue> sorted = (await ScanRowIdsAsync(table, executor, db))
            .OrderBy(id => id.ToString(), StringComparer.Ordinal)
            .ToList();

        await SplitAtAsync(keySpace, table.Store.RowPointKey(sorted[sorted.Count / 2]));

        // The lower child covers [-inf, median); a row from its first half falls inside it with rows
        // on both sides, which is what the split policy requires.
        int after = await SplitAtAsync(keySpace, table.Store.RowPointKey(sorted[sorted.Count / 4]));

        Assert.That(after, Is.EqualTo(3), "Splitting the lower child must yield three ranges");
    }

    // -----------------------------------------------------------------------
    // 4. A refusal must be observable. Without this, the assertions above could
    //    not be trusted to distinguish a real split from a silent no-op.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SplitKeyBeyondEveryRow_IsRefused_AndLeavesTheSpaceIntact()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        // Row ids are 24 lowercase hex characters, so a key of 'z's sorts above every one of them and
        // the upper half of this split would be empty.
        KahunaSplitRangeResponse response = await TestNode!.Kahuna.SplitRangeAtKeyWithOutcomeAsync(
            keySpace, keySpace + "/zzzzzzzzzzzzzzzzzzzzzzzz", CancellationToken.None);

        Assert.That(response.Success, Is.False,
            "A split that would leave a child range empty must be refused");
        Assert.That(response.Determinate, Is.True,
            "A policy refusal is final: the map did not change and will not change later");

        Assert.That(Descriptors(keySpace), Has.Count.EqualTo(1),
            "A refused split must leave the space as a single range");
    }
}

/// <summary>
/// What key-range sharding actually does on a node started with a single Raft partition.
///
/// <para>The flag's documentation makes an operational promise about this, so it is pinned by a test
/// rather than left to inference: an operator who enables the flag on a single-partition node needs
/// to know whether they got the mode or a silent no-op.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestKeyRangeShardingSinglePartition : KeyRangeSplitFixture
{
    protected override int NodeInitialPartitions => 1;

    [Test]
    public async Task SinglePartitionNode_StillRoutesByKeyRange_ButHasNowhereToSplitOnto()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        Assert.That(RoutingMode(keySpace), Is.EqualTo("KeyRange"),
            "Registration is not refused on a single-partition node: the meta map shares partition 0, " +
            "so one user partition is enough to host ranged data");

        Assert.That(Descriptors(keySpace), Has.Count.EqualTo(1),
            "The space starts as a single whole-space range");

        // Reads must be unaffected either way — this is the property that makes enabling the flag on
        // a single-partition node safe rather than merely pointless.
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        int rows = 0;
        await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx))
            rows++;

        await database.Transactions.CommitAsync(tx);

        Assert.That(rows, Is.EqualTo(RowCount),
            "A single-partition key-range-routed table must read back completely");
    }
}
