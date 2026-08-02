
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Verifies predicate (range) lock behaviour for Serializable read-write transactions.
///
/// In hash mode (key-range sharding off) the three range-lock methods —
/// AcquireRowRangeLockAsync, AcquireIndexRangeLockAsync, AcquireBoundedIndexRangeLockAsync —
/// were previously no-ops. Under Serializable+ReadWrite they now fire regardless of sharding
/// mode, giving phantom protection: Kahuna's TrySetHandler/TryDeleteHandler call
/// KeyCoveredByForeignRangeLock at write time and return MustRetry when a foreign shared
/// range lock covers the target key.
///
/// Guarantees verified here:
///   - Row range lock blocks a concurrent insert (phantom insert into scanned row space).
///   - Index range lock blocks a concurrent index-entry insert.
///   - Bounded index range lock blocks inserts within bounds, not outside bounds.
///   - Two Serializable+RW transactions with overlapping row range locks coexist (S∩S).
///   - ReadCommitted scan acquires no range lock — concurrent insert commits freely.
///   - After the range-lock holder commits, a blocked writer can succeed.
/// </summary>
[TestFixture]
public sealed class TestSerializableRangeLocks
{
    private static readonly string IndexName = "srl_idx";

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna, CamusDBOptions.Default);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, "testdb", tag);
        return (node, mgr, store);
    }

    // -----------------------------------------------------------------------
    // 1. Row range lock: Serializable+RW scan blocks a phantom insert
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_RowRangeLock_BlocksInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-01");
        await using EmbeddedKahuna __ = node;

        // Scanner acquires the full row range lock.
        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(scanner);

        // Concurrent insert into the row space is blocked immediately at write time.
        KvTransaction inserter = await mgr.BeginAsync();
        ObjectIdValue newRowId = new(1, 0, 0);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.InsertRow(inserter, newRowId, [42]));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "InsertRow into a row space covered by a foreign shared range lock must fail with TransactionMustRetry");

        await mgr.RollbackAsync(inserter);
        await mgr.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 2. Two Serializable+RW row range locks coexist (S∩S)
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_TwoRowRangeLocks_Coexist()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-02");
        await using EmbeddedKahuna __ = node;

        KvTransaction s1 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction s2 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        Assert.DoesNotThrowAsync(() => store.AcquireRowRangeLockAsync(s1),
            "First Serializable+RW row range lock must succeed");
        Assert.DoesNotThrowAsync(() => store.AcquireRowRangeLockAsync(s2),
            "Second Serializable+RW row range lock must succeed (S∩S compatible)");

        await mgr.CommitAsync(s1);
        await mgr.CommitAsync(s2);
    }

    // -----------------------------------------------------------------------
    // 3. ReadCommitted scan acquires no row range lock — insert commits freely
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_RowScan_TakesNoRangeLock()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-03");
        await using EmbeddedKahuna __ = node;

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(scanner);

        // No range lock taken → concurrent insert must succeed.
        KvTransaction inserter = await mgr.BeginAsync();
        ObjectIdValue newRowId = new(1, 0, 0);
        await store.InsertRow(inserter, newRowId, [42]);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "Insert must not be blocked by a ReadCommitted scanner (no range lock)");

        await mgr.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 4. Index range lock: Serializable+RW scan blocks a phantom index insert
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_IndexRangeLock_BlocksInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-04");
        await using EmbeddedKahuna __ = node;

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireIndexRangeLockAsync(scanner, IndexName);

        // Concurrent insert of an index entry into the locked bucket is blocked.
        KvTransaction inserter = await mgr.BeginAsync();
        ObjectIdValue rowId = new(1, 0, 0);
        await store.InsertRow(inserter, rowId, [1]);
        CompositeColumnValue indexKey = new(new ColumnValue(ColumnType.Integer64, 10L));
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntry(inserter, IndexName, indexKey, rowId, unique: true));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "PutIndexEntry into a bucket covered by a foreign shared range lock must fail with TransactionMustRetry");

        await mgr.RollbackAsync(inserter);
        await mgr.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 5. Bounded index range lock: blocks inserts within bounds, not outside
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_BoundedIndexRangeLock_BlocksInBoundsInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-05");
        await using EmbeddedKahuna __ = node;

        // Lock [10, 20] on the index.
        CompositeColumnValue lo = new(new ColumnValue(ColumnType.Integer64, 10L));
        CompositeColumnValue hi = new(new ColumnValue(ColumnType.Integer64, 20L));

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireBoundedIndexRangeLockAsync(scanner, IndexName, lo, true, hi, true, unique: true);

        // Insert at key 15 (within [10, 20]) — must be blocked.
        KvTransaction inserterIn = await mgr.BeginAsync();
        ObjectIdValue rowIdIn = new(1, 0, 0);
        await store.InsertRow(inserterIn, rowIdIn, [1]);
        CompositeColumnValue inKey = new(new ColumnValue(ColumnType.Integer64, 15L));
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntry(inserterIn, IndexName, inKey, rowIdIn, unique: true));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Insert within the bounded range must be blocked");

        await mgr.RollbackAsync(inserterIn);
        await mgr.CommitAsync(scanner);
    }

    [Test]
    public async Task SerializableRW_BoundedIndexRangeLock_AllowsOutsideBoundsInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-06");
        await using EmbeddedKahuna __ = node;

        // Lock [10, 20] on the index.
        CompositeColumnValue lo = new(new ColumnValue(ColumnType.Integer64, 10L));
        CompositeColumnValue hi = new(new ColumnValue(ColumnType.Integer64, 20L));

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireBoundedIndexRangeLockAsync(scanner, IndexName, lo, true, hi, true, unique: true);

        // Insert at key 99 (outside [10, 20]) — must succeed.
        KvTransaction inserterOut = await mgr.BeginAsync();
        ObjectIdValue rowIdOut = new(2, 0, 0);
        await store.InsertRow(inserterOut, rowIdOut, [2]);
        CompositeColumnValue outKey = new(new ColumnValue(ColumnType.Integer64, 99L));
        await store.PutIndexEntry(inserterOut, IndexName, outKey, rowIdOut, unique: true);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserterOut),
            "Insert outside the bounded range must not be blocked");

        await mgr.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 7. Row range lock releases on commit — subsequent insert succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_RowRangeLock_ReleasedOnCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SRL-07");
        await using EmbeddedKahuna __ = node;

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(scanner);
        await mgr.CommitAsync(scanner);

        // Lock released — insert must now succeed.
        KvTransaction inserter = await mgr.BeginAsync();
        ObjectIdValue rowId = new(1, 0, 0);
        await store.InsertRow(inserter, rowId, [1]);
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "Insert must succeed after the row range lock holder has committed");
    }
}
