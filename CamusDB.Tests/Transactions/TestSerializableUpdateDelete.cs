
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
/// Verifies that UPDATE/DELETE locate scans acquire exclusive predicate range locks,
/// blocking concurrent Serializable+RW readers from entering the same row/index range
/// while the modification is in flight.
///
/// At the KvTableStore level, AcquireRowRangeLockAsync(exclusive: true) simulates what
/// RowUpdater/RowDeleter's QueryTicket(exclusivePredicateLocks: true) causes QueryScanner
/// to do during the locate phase.
///
/// Guarantees verified here:
///   - Exclusive row range lock blocks a concurrent Shared acquire (X∩S incompatible).
///   - Exclusive row range lock blocks a concurrent writer (write-path fence, same as Shared).
///   - Two Shared row range locks still coexist (S∩S not broken by adding the exclusive path).
///   - After the exclusive holder commits, a new Shared acquire succeeds.
///   - Exclusive index range lock blocks a concurrent Shared index acquire.
///   - Bounded exclusive index range lock blocks inserts within bounds, not outside.
/// </summary>
[TestFixture]
public sealed class TestSerializableUpdateDelete
{
    private static readonly string IndexName = "sud_idx";

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna);
        KvTableStore store = new(node.Kahuna, tag);
        return (node, mgr, store);
    }

    // -----------------------------------------------------------------------
    // 1. Exclusive row range lock blocks a concurrent Shared acquire (X∩S)
    //
    // Simulates: UPDATE holds exclusive predicate lock during its locate scan;
    // a concurrent Serializable+RW SELECT tries to acquire Shared on the same
    // range and must fail with TransactionConflict.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExclusiveRowRangeLock_BlocksConcurrentSharedAcquire()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-01");
        await using EmbeddedKahuna __ = node;

        // UPDATE tx: acquires exclusive row range lock over the full row space.
        KvTransaction updater = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(updater, exclusive: true);

        // Concurrent Serializable+RW reader tries Shared on the same range — must fail.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireRowRangeLockAsync(reader));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Shared row range acquire must fail with TransactionConflict when another tx holds Exclusive");

        await mgr.RollbackAsync(reader);
        await mgr.CommitAsync(updater);
    }

    // -----------------------------------------------------------------------
    // 2. Exclusive row range lock blocks concurrent writers (write-path fence)
    //
    // An exclusive range lock is a superset of Shared: it also blocks phantoms.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExclusiveRowRangeLock_BlocksConcurrentInsert()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-02");
        await using EmbeddedKahuna __ = node;

        KvTransaction updater = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(updater, exclusive: true);

        // A concurrent insert into the same row space is blocked by the write-path fence.
        KvTransaction inserter = await mgr.BeginAsync();
        ObjectIdValue newRowId = new(1, 0, 0);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.InsertRow(inserter, newRowId, [42]));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Insert into a row space covered by a foreign exclusive range lock must fail with TransactionMustRetry");

        await mgr.RollbackAsync(inserter);
        await mgr.CommitAsync(updater);
    }

    // -----------------------------------------------------------------------
    // 3. Two Shared row range locks still coexist (S∩S regression guard)
    //
    // Adding the exclusive path must not break S∩S for concurrent SELECT scans.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SharedRowRangeLock_TwoReaders_StillCoexist()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-03");
        await using EmbeddedKahuna __ = node;

        KvTransaction s1 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction s2 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        Assert.DoesNotThrowAsync(() => store.AcquireRowRangeLockAsync(s1),
            "First Shared row range lock must succeed");
        Assert.DoesNotThrowAsync(() => store.AcquireRowRangeLockAsync(s2),
            "Second Shared row range lock must succeed (S∩S compatible)");

        await mgr.CommitAsync(s1);
        await mgr.CommitAsync(s2);
    }

    // -----------------------------------------------------------------------
    // 4. After the exclusive holder commits, a new Shared acquire succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task AfterExclusiveHolderCommits_SharedAcquireSucceeds()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-04");
        await using EmbeddedKahuna __ = node;

        KvTransaction updater = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(updater, exclusive: true);
        await mgr.CommitAsync(updater);

        // Exclusive lock is released — a new Shared acquire must now succeed.
        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.DoesNotThrowAsync(() => store.AcquireRowRangeLockAsync(reader),
            "Shared row range acquire must succeed once the exclusive holder has committed");
        await mgr.CommitAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 5. Exclusive index range lock blocks concurrent Shared index acquire
    //
    // Same guarantee as test 1 but for the index bucket.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExclusiveIndexRangeLock_BlocksConcurrentSharedAcquire()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-05");
        await using EmbeddedKahuna __ = node;

        KvTransaction updater = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireIndexRangeLockAsync(updater, IndexName, exclusive: true);

        KvTransaction reader = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireIndexRangeLockAsync(reader, IndexName));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Shared index range acquire must fail with TransactionConflict when another tx holds Exclusive");

        await mgr.RollbackAsync(reader);
        await mgr.CommitAsync(updater);
    }

    // -----------------------------------------------------------------------
    // 6. Exclusive bounded index range lock blocks inserts within bounds,
    //    not outside — mirrors the phantom-only-within-bounds guarantee for
    //    exclusive predicate locks on ranged UPDATE/DELETE.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExclusiveBoundedIndexRangeLock_BlocksInsertWithin_NotOutside()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("SUD-06");
        await using EmbeddedKahuna __ = node;

        // UPDATE tx acquires exclusive bounded lock on [10, 20].
        KvTransaction updater = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CompositeColumnValue from = new(new ColumnValue(ColumnType.Integer64, 10L));
        CompositeColumnValue to   = new(new ColumnValue(ColumnType.Integer64, 20L));
        await store.AcquireBoundedIndexRangeLockAsync(updater, IndexName, from, true, to, true, unique: true, exclusive: true);

        // Insert inside bounds [10, 20] — blocked by the exclusive lock fence.
        KvTransaction inserter1 = await mgr.BeginAsync();
        ObjectIdValue rowId1 = new(1, 0, 0);
        await store.InsertRow(inserter1, rowId1, [1]);
        CompositeColumnValue keyInside = new(new ColumnValue(ColumnType.Integer64, 15L));
        CamusDBException? exInside = Assert.ThrowsAsync<CamusDBException>(
            () => store.PutIndexEntry(inserter1, IndexName, keyInside, rowId1, unique: true));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, exInside?.Code,
            "Insert within the exclusive bounded range must fail with TransactionMustRetry");
        await mgr.RollbackAsync(inserter1);

        // Insert outside bounds (key = 50) — must succeed.
        KvTransaction inserter2 = await mgr.BeginAsync();
        ObjectIdValue rowId2 = new(2, 0, 0);
        await store.InsertRow(inserter2, rowId2, [2]);
        CompositeColumnValue keyOutside = new(new ColumnValue(ColumnType.Integer64, 50L));
        Assert.DoesNotThrowAsync(() => store.PutIndexEntry(inserter2, IndexName, keyOutside, rowId2, unique: true),
            "Insert outside the exclusive bounded range must succeed");
        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter2),
            "Commit outside the exclusive bounded range must succeed");

        await mgr.CommitAsync(updater);
    }
}
