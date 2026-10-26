/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using NUnit.Framework;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The bound a store index scan hands to Kahuna: a single-value seek carries its end key, so the
/// backend stops at the key instead of filling the page past it, and a whole-index scan carries
/// none. Both use the store's default page.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestKvTableStoreScanBound
{
    /// <summary>Records the prefix, end key and page size of every range scan, then delegates it.</summary>
    private sealed class RecordingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public readonly List<(string Prefix, string? EndKey, int PageSize)> Scans = [];

        public override IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default, KeyValueScanObservationSink? observations = null, KeyValueReadMode readMode = KeyValueReadMode.Default)
        {
            Scans.Add((prefix, endKey, pageSize));
            return base.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct, coordinatorKey, operationId, observations, readMode);
        }
    }

    [Test]
    public async Task ScanIndex_SingleValueSeek_CarriesItsEndKeyToKahuna()
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await using EmbeddedKahuna _ = node;
        await node.WaitForLeaderAsync("sb1/warmup", CancellationToken.None);

        RecordingKahuna kahuna = new(node.Kahuna);
        KvTableStore store = new(kahuna, CamusDBOptions.Default, "testdb", "sb1");
        ColumnType[] keyTypes = [ColumnType.String];

        KvTransaction tx = await BeginTransaction(node.Kahuna, "sb1-put");
        foreach ((string category, int n) in new[] { ("tools", 1), ("books", 2), ("tools", 3) })
            await store.PutIndexEntry(tx, "idx_cat", Key(category), new ObjectIdValue(n, 0, 0), unique: false);
        await CommitTransaction(node.Kahuna, tx);

        int matches = 0;
        await foreach ((CompositeColumnValue _, ObjectIdValue _, System.ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_cat", keyTypes, Key("tools"), Key("tools"), unique: false))
        {
            matches++;
        }

        Assert.AreEqual(2, matches, "both tools entries, and not the books entry");
        (string prefix, string? endKey, int pageSize) = kahuna.Scans.Single();
        Assert.IsNotNull(endKey, "a single-value seek carries its end key");
        Assert.IsTrue(endKey!.StartsWith(prefix, System.StringComparison.Ordinal));
        Assert.AreEqual(KvStoreConstants.DefaultPageSize, pageSize);

        kahuna.Scans.Clear();
        int all = 0;
        await foreach ((CompositeColumnValue _, ObjectIdValue _, System.ReadOnlyMemory<byte> _) in
            store.ScanIndex(KvTransaction.CreateReadOnly(), "idx_cat", keyTypes, null, null, unique: false))
        {
            all++;
        }

        Assert.AreEqual(3, all);
        Assert.IsNull(kahuna.Scans.Single().EndKey, "a whole-index scan has no end key");
        Assert.AreEqual(KvStoreConstants.DefaultPageSize, kahuna.Scans.Single().PageSize);
    }

    private static CompositeColumnValue Key(string value) => new(new[] { new ColumnValue(ColumnType.String, value) });

    private static async Task<KvTransaction> BeginTransaction(IKahuna kahuna, string uniqueId)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = uniqueId, Locking = KeyValueTransactionLocking.Pessimistic },
            CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type);
        return new KvTransaction(handle.TransactionId, uniqueId);
    }

    private static async Task CommitTransaction(IKahuna kahuna, KvTransaction tx)
    {
        (KeyValueResponseType result, _) = await kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Committed, result);
    }
}
