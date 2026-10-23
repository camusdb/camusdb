/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Data written under one routing mode, read after a restart with the other value of
/// <see cref="CamusDBOptions.KeyRangeShardingEnabled"/>, on one persistent node with three data
/// partitions. These tests pin what a mode change does to data that already exists:
/// <list type="bullet">
///   <item>Hash to key-range: hash routing and the key-range seed pick a key space's partition with
///   different hash functions, so most spaces move to another partition. The rows stay readable only
///   because Kahuna's storage is node-global: the store keys a row by its key, not by its partition,
///   so a read through the new partition on the same node finds it.</item>
///   <item>Key-range to hash: turning the flag off does not change the routing of a space that was
///   registered. Kahuna rebuilds its key-space registry from the replicated range map at startup, so a
///   space with a descriptor stays key-range routed. Only spaces registered later stay hash-routed.</item>
/// </list>
/// The tests use RocksDB with fixed store and WAL revisions. An empty revision makes Kahuna open a new
/// directory on every start, and a restart then reads an empty store.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRoutingModeChangeOnExistingData
{
    private const int Partitions = 3;
    private const int RowsPerTable = 5;
    private static readonly string[] TableIds = ["a1", "b2", "c3", "d4", "e5", "f6", "g7", "h8"];

    private static readonly CamusDBOptions Hash = CamusDBOptions.Default with { KeyRangeShardingEnabled = false };
    private static readonly CamusDBOptions KeyRange = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };

    private string root = "";

    [SetUp]
    public void SetUp()
    {
        root = Path.Combine(Path.GetTempPath(), $"camus-routing-mode-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(root, recursive: true); } catch { }
    }

    private EmbeddedKahuna NewNode() =>
        new(new EmbeddedKahunaOptions
        {
            NodeName = "routing-mode",
            Storage = "rocksdb",
            StoragePath = Path.Combine(root, "store"),
            StorageRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision,
            WalStorage = "rocksdb",
            WalPath = Path.Combine(root, "wal"),
            WalRevision = EmbeddedKahunaOptionsBuilder.CurrentStorageRevision,
            InitialPartitions = Partitions,
        }.WithTestNodeDefaults());

    private static async Task<EmbeddedKahuna> StartAsync(EmbeddedKahuna node)
    {
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("routing-mode/warmup", CancellationToken.None);
        return node;
    }

    /// <summary>Opens each table's store in <paramref name="options"/>' mode and registers its row space as the engine would.</summary>
    private static async Task<Dictionary<string, KvTableStore>> OpenStoresAsync(EmbeddedKahuna node, CamusDBOptions options)
    {
        Dictionary<string, KvTableStore> stores = [];

        foreach (string id in TableIds)
        {
            KvTableStore store = new(node.Kahuna, options, "testdb", id);
            if (options.KeyRangeShardingEnabled)
                await node.Kahuna.RegisterKeyRangeAsync(store.RowKeySpace);
            stores[id] = store;
        }

        return stores;
    }

    private static ObjectIdValue RowId(int table, int row) => new(1000 + table, 0, row);

    private static byte[] Payload(int table, int row) => [(byte)table, (byte)row, 0x5A];

    private static async Task WriteAllAsync(EmbeddedKahuna node, Dictionary<string, KvTableStore> stores)
    {
        for (int t = 0; t < TableIds.Length; t++)
        {
            (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = $"w-{TableIds[t]}", Locking = KeyValueTransactionLocking.Pessimistic },
                CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Set, type);

            KvTransaction tx = new(handle.TransactionId, $"w-{TableIds[t]}");
            for (int r = 0; r < RowsPerTable; r++)
                await stores[TableIds[t]].InsertRow(tx, RowId(t, r), Payload(t, r));

            (KeyValueResponseType committed, _) = await node.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
            Assert.AreEqual(KeyValueResponseType.Committed, committed);
        }
    }

    /// <summary>One line per table: the partition before and after, rows found by point read and by scan.</summary>
    private sealed record TableOutcome(string Id, int PartitionBefore, int PartitionAfter, bool KeyRangeAfter, int PointReads, int Scanned);

    private static async Task<List<TableOutcome>> ReadAllAsync(
        EmbeddedKahuna node, Dictionary<string, KvTableStore> stores, Dictionary<string, int> partitionBefore)
    {
        List<TableOutcome> outcomes = [];

        for (int t = 0; t < TableIds.Length; t++)
        {
            KvTableStore store = stores[TableIds[t]];
            int found = 0;
            for (int r = 0; r < RowsPerTable; r++)
            {
                ReadOnlyMemory<byte>? row = await store.GetRow(KvTransaction.CreateReadOnly(), RowId(t, r));
                if (row is { } bytes && bytes.ToArray().SequenceEqual(Payload(t, r)))
                    found++;
            }

            int scanned = 0;
            await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> _) in store.ScanRows(KvTransaction.CreateReadOnly()))
                scanned++;

            TablePlacement after = node.ReadPlacementUncached(store.RowKeySpace, out _);
            outcomes.Add(new TableOutcome(TableIds[t], partitionBefore[TableIds[t]], after.Spans[0].PartitionId, after.IsKeyRange, found, scanned));
        }

        return outcomes;
    }

    private async Task<List<TableOutcome>> RunAsync(CamusDBOptions writeMode, CamusDBOptions readMode, bool flushBeforeRestart)
    {
        Dictionary<string, int> partitionBefore = [];

        EmbeddedKahuna first = await StartAsync(NewNode());
        try
        {
            Dictionary<string, KvTableStore> stores = await OpenStoresAsync(first, writeMode);
            await WriteAllAsync(first, stores);

            foreach ((string id, KvTableStore store) in stores)
                partitionBefore[id] = first.ReadPlacementUncached(store.RowKeySpace, out _).Spans[0].PartitionId;

            if (flushBeforeRestart)
                await first.FlushAsync();
        }
        finally
        {
            await first.DisposeAsync();
        }

        EmbeddedKahuna second = await StartAsync(NewNode());
        try
        {
            Dictionary<string, KvTableStore> stores = await OpenStoresAsync(second, readMode);
            List<TableOutcome> outcomes = await ReadAllAsync(second, stores, partitionBefore);

            foreach (TableOutcome o in outcomes)
                TestContext.Out.WriteLine(
                    $"{o.Id}: partition {o.PartitionBefore} -> {o.PartitionAfter} (key-range {o.KeyRangeAfter}), " +
                    $"point reads {o.PointReads}/{RowsPerTable}, scanned {o.Scanned}/{RowsPerTable}");

            return outcomes;
        }
        finally
        {
            await second.DisposeAsync();
        }
    }

    private static void AssertAllReadable(List<TableOutcome> outcomes)
    {
        Assert.Multiple(() =>
        {
            foreach (TableOutcome o in outcomes)
            {
                Assert.That(o.PointReads, Is.EqualTo(RowsPerTable), $"{o.Id}: point reads after the mode change");
                Assert.That(o.Scanned, Is.EqualTo(RowsPerTable), $"{o.Id}: scan after the mode change");
            }
        });
    }

    [Test]
    public async Task Control_HashToHash_Flushed() => AssertAllReadable(await RunAsync(Hash, Hash, flushBeforeRestart: true));

    [Test]
    public async Task Control_KeyRangeToKeyRange_Flushed() => AssertAllReadable(await RunAsync(KeyRange, KeyRange, flushBeforeRestart: true));

    /// <summary>Most spaces change partition, every space is key-range routed, and every row is still readable.</summary>
    private static void AssertMovedToKeyRangeAndReadable(List<TableOutcome> outcomes)
    {
        Assert.That(outcomes.Count(o => o.PartitionBefore != o.PartitionAfter), Is.GreaterThan(outcomes.Count / 2),
            "the key-range seed and hash routing must disagree on most spaces, or the case proves nothing");
        Assert.That(outcomes.All(o => o.KeyRangeAfter), Is.True, "every registered space is key-range routed");
        AssertAllReadable(outcomes);
    }

    /// <summary>Every space keeps its key-range partition and routing although the flag is off, and every row is readable.</summary>
    private static void AssertStillKeyRangeAndReadable(List<TableOutcome> outcomes)
    {
        Assert.Multiple(() =>
        {
            foreach (TableOutcome o in outcomes)
            {
                Assert.That(o.KeyRangeAfter, Is.True, $"{o.Id}: a space with a range descriptor stays key-range routed with the flag off");
                Assert.That(o.PartitionAfter, Is.EqualTo(o.PartitionBefore), $"{o.Id}: the space keeps its seeded partition");
            }
        });
        AssertAllReadable(outcomes);
    }

    /// <summary>
    /// The routing-mode record is persistent: written on the first boot, it holds on the next one, where an
    /// unset key follows it and an explicit key that disagrees is refused.
    /// </summary>
    [Test]
    public async Task RoutingModeRecord_SurvivesARestart()
    {
        EmbeddedKahuna first = await StartAsync(NewNode());
        try
        {
            Assert.That(await RoutingModeStartupCheck.ResolveAsync(first, configured: true, explicitlySet: true,
                NullLogger.Instance, CancellationToken.None), Is.True);
            await first.FlushAsync();
        }
        finally
        {
            await first.DisposeAsync();
        }

        EmbeddedKahuna second = await StartAsync(NewNode());
        try
        {
            Assert.That(await RoutingModeStartupCheck.ResolveAsync(second, configured: false, explicitlySet: false,
                NullLogger.Instance, CancellationToken.None), Is.True, "an unset key follows the record after a restart");

            CamusDBException e = Assert.ThrowsAsync<CamusDBException>(() => RoutingModeStartupCheck.ResolveAsync(
                second, configured: false, explicitlySet: true, NullLogger.Instance, CancellationToken.None))!;
            Assert.That(e.Message, Does.Contain("was created with key-range"));
        }
        finally
        {
            await second.DisposeAsync();
        }
    }

    [Test]
    public async Task HashToKeyRange_Flushed() => AssertMovedToKeyRangeAndReadable(await RunAsync(Hash, KeyRange, flushBeforeRestart: true));

    [Test]
    public async Task HashToKeyRange_NotFlushed() => AssertMovedToKeyRangeAndReadable(await RunAsync(Hash, KeyRange, flushBeforeRestart: false));

    [Test]
    public async Task KeyRangeToHash_Flushed() => AssertStillKeyRangeAndReadable(await RunAsync(KeyRange, Hash, flushBeforeRestart: true));

    [Test]
    public async Task KeyRangeToHash_NotFlushed() => AssertStillKeyRangeAndReadable(await RunAsync(KeyRange, Hash, flushBeforeRestart: false));
}
