/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Diagnostics;
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
/// The reader-side half of range-lock enforcement: a range lock requested after a foreign write was
/// staged into the range is ordered against that writer, never granted over it.
///
/// <para><b>Ordering is the whole point.</b> Acquiring the lock <em>before</em> the write exercises
/// the write-time check, which refuses the write outright and is covered by
/// <c>TestSerializableRangeLocks</c>. Every case here stages the write first and requests the lock
/// second. Kahuna answers such a request with the writer named as the lock's holder, and
/// <see cref="KvRangeLockManager"/> then applies its deadlock-avoidance ordering to the pair: an
/// older requester waits for the writer to finish, a younger one that already holds something aborts
/// at once and is replayed from BEGIN, and a requester that holds nothing yet waits whatever its age,
/// because nobody can be waiting on it. A lock is therefore never held over a value that is about to
/// change behind it, which is what keeps a reader and a writer that overlap on a key from both
/// succeeding whichever arrived first.</para>
///
/// <para><b>Why it matters beyond isolation.</b> A range split opens its quiesce window by taking a
/// range lock over the half being moved, so nothing commits into it while the catch-up copy runs. A
/// lock granted over a staged write would let that write commit onto the source partition after the
/// copy was taken and become unreachable once the range routes to its new owner — an acknowledged row
/// that no longer exists. The cases here hold in hash mode as well as under key-range routing,
/// because CamusDB takes these locks in both.</para>
///
/// <para>Each case reads the row back through a fresh transaction afterwards rather than trusting an
/// error code or the absence of one: only the read-back tells a write that landed from one that was
/// dropped.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableRangeLockCommitFence
{
    private const string IndexName = "rlf_idx";

    /// <summary>
    /// Long enough that a wait for a writer the test finishes on purpose never runs out on a slow
    /// runner; the deadline cases pass their own, shorter value.
    /// </summary>
    private const int GenerousDeadlineMs = 5_000;

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag, int lockWaitDeadlineMs = GenerousDeadlineMs)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        CamusDBOptions options = CamusDBOptions.Default with { LockWaitDeadlineMs = lockWaitDeadlineMs };

        KvTransactionsManager mgr = new(node.Kahuna, options);
        KvTableStore store = new(node.Kahuna, options, "testdb", tag);

        return (node, mgr, store);
    }

    /// <summary>Reads a row through a throwaway transaction, so the assertion sees committed state.</summary>
    private static async Task<ReadOnlyMemory<byte>?> ReadBackAsync(
        KvTransactionsManager mgr, KvTableStore store, ObjectIdValue rowId)
    {
        KvTransaction reader = await mgr.BeginAsync();
        ReadOnlyMemory<byte>? row = await store.GetRow(reader, rowId);
        await mgr.CommitAsync(reader);

        return row;
    }

    private static Task<KvTransaction> BeginSerializableAsync(KvTransactionsManager mgr)
        => mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

    // -----------------------------------------------------------------------
    // 1. A requester that holds nothing waits for the writer, whatever its age
    // -----------------------------------------------------------------------

    [Test]
    public async Task AYoungerScannerThatHoldsNothing_WaitsForTheStagedWriterAndThenSeesItsRow()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-01");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(1, 0, 0);

        // The write goes in while nothing is locked, so it is staged rather than refused.
        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [7]);

        // The scanner is younger than the writer, which under plain wait-die would abort it at once.
        // It holds no lock and no write of its own, so nobody can be waiting on it and it may wait.
        KvTransaction scanner = await BeginSerializableAsync(mgr);
        Task acquire = store.AcquireRowRangeLockAsync(scanner);

        await Task.Delay(150);
        Assert.False(acquire.IsCompleted,
            "The lock must not be granted while the writer's intent is live: a grant here is the " +
            "interleaving that lets the writer commit behind a reader's lock");

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "A waiting requester holds no lock, so the writer it waits for must still be free to commit");

        Assert.DoesNotThrowAsync(() => acquire,
            "Once the writer settled, the wait must end in a grant rather than a conflict");

        Assert.IsNotNull(await store.GetRow(scanner, rowId),
            "The lock was granted after the write committed, so the scanner must read the committed row; " +
            "a lock over a stale value is the anomaly this ordering exists to prevent");

        await mgr.CommitAsync(scanner);

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId));
    }

    [Test]
    public async Task AYoungerScannerThatHoldsNothing_GivesUpAtTheDeadlineWhileTheWriterStaysOpen()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) =
            await CreateAsync("RLF-02", lockWaitDeadlineMs: 300);
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(2, 0, 0);

        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [2]);

        KvTransaction scanner = await BeginSerializableAsync(mgr);

        long started = Stopwatch.GetTimestamp();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireRowRangeLockAsync(scanner),
            "The writer never finishes, so the wait must end at the deadline rather than hang");

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "A wait that ran out against a named holder is a definite conflict with nothing committed, " +
            "the same answer an older requester gets when its wait runs out; it is replayed from BEGIN");

        Assert.GreaterOrEqual(Stopwatch.GetElapsedTime(started).TotalMilliseconds, 300,
            "The refusal must come after the deadline, not at once: an immediate abort would mean the " +
            "holds-nothing requester was treated as an ordinary younger one");

        await mgr.RollbackAsync(scanner);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "A requester that gave up left nothing behind, so the writer must commit unhindered");

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId));
    }

    // -----------------------------------------------------------------------
    // 2. A younger requester that already holds something aborts at once
    // -----------------------------------------------------------------------

    [Test]
    public async Task AYoungerScannerThatAlreadyHoldsAWrite_IsRefusedAtOnceAndTheWriterCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-03");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(3, 0, 0);
        ObjectIdValue scannersOwnRow = new(3, 0, 1);

        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [3]);

        // The scanner stages a write of its own first. It now holds something another transaction
        // could wait on, so a wait from it could close a cycle: as the younger side it must die.
        KvTransaction scanner = await BeginSerializableAsync(mgr);
        await store.InsertRow(scanner, scannersOwnRow, [1]);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireRowRangeLockAsync(scanner),
            "A younger requester that holds a write must not be granted a lock over a foreign staged write");

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "The refusal is a definite ordering decision with nothing committed, so it must surface as " +
            "the immediate conflict code an autocommit statement replays from BEGIN — waiting out the " +
            "deadline first would report it as the deadline code instead");

        await mgr.RollbackAsync(scanner);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "The refused requester never held the range, so the writer commits as if it had never asked");

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The write that won the ordering must be visible afterwards");
        Assert.IsNull(await ReadBackAsync(mgr, store, scannersOwnRow),
            "The refused transaction rolled back, so its own staged row must leave nothing behind");
    }

    // -----------------------------------------------------------------------
    // 3. An older requester waits for the younger writer even when it holds something
    // -----------------------------------------------------------------------

    [Test]
    public async Task AnOlderScannerThatHoldsAWrite_WaitsForTheYoungerWriterAndThenSeesItsRow()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-04");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(4, 0, 0);
        ObjectIdValue scannersOwnRow = new(4, 0, 1);

        // Begun first, so it is the older of the pair; its own staged write means the holds-nothing
        // exemption does not apply and only its age lets it wait.
        KvTransaction scanner = await BeginSerializableAsync(mgr);
        await store.InsertRow(scanner, scannersOwnRow, [1]);

        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [4]);

        Task acquire = store.AcquireRowRangeLockAsync(scanner);

        await Task.Delay(150);
        Assert.False(acquire.IsCompleted,
            "The older requester waits for the younger writer rather than being granted over it");

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "The younger writer holds the intent and must be free to finish; it is the waiter, not " +
            "the holder, that the ordering constrains");

        Assert.DoesNotThrowAsync(() => acquire);

        Assert.IsNotNull(await store.GetRow(scanner, rowId),
            "The lock arrived after the commit, so the scanner reads the committed row");

        await mgr.CommitAsync(scanner);

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId));
        Assert.IsNotNull(await ReadBackAsync(mgr, store, scannersOwnRow),
            "The waiting transaction's own write must survive its wait");
    }

    // -----------------------------------------------------------------------
    // 4. The holder's own staged write never blocks its own lock
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheRangeLockHoldersOwnWriteInsideItsRange_StillCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-05");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(5, 0, 0);

        // This is the ordinary shape of a Serializable read-write transaction: scan the range it is
        // about to modify, then modify it. A transaction's own intent must never count as a foreign
        // writer, or every such transaction would block itself.
        KvTransaction scanner = await BeginSerializableAsync(mgr);
        await store.AcquireRowRangeLockAsync(scanner);
        await store.InsertRow(scanner, rowId, [9]);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(scanner),
            "A transaction must be able to write inside the range it holds the lock over");

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The holder's own write must be visible after its commit");
    }

    // -----------------------------------------------------------------------
    // 5. The check is by bounds, not by bucket: a staged write outside them does not count
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexWriteStagedOutsideTheLockedBounds_DoesNotBlockTheLockAndStillCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-06");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(6, 0, 0);

        // Staged first, exactly as in the row cases — the only difference is where the key sorts.
        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [3]);
        await store.PutIndexEntry(
            inserter, IndexName, new(new ColumnValue(ColumnType.Integer64, 99L)), rowId, unique: true);

        KvTransaction scanner = await BeginSerializableAsync(mgr);
        Assert.DoesNotThrowAsync(
            () => store.AcquireBoundedIndexRangeLockAsync(
                scanner, IndexName,
                new(new ColumnValue(ColumnType.Integer64, 10L)), true,
                new(new ColumnValue(ColumnType.Integer64, 20L)), true,
                unique: true),
            "Key 99 sorts outside the requested [10, 20] bounds, so the lock must be granted at once; " +
            "a wait or a refusal here is checking the bucket rather than the range");

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "The out-of-bounds write is outside the granted lock too, so it commits");

        await mgr.CommitAsync(scanner);

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The out-of-bounds write must be visible after its commit");
    }

    // -----------------------------------------------------------------------
    // 6. The same ordering on an index key that does fall inside the bounds
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexWriteStagedInsideTheLockedBounds_RefusesAYoungerScannerThatHoldsAWrite()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-07");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(7, 0, 0);
        ObjectIdValue scannersOwnRow = new(7, 0, 1);

        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [4]);
        await store.PutIndexEntry(
            inserter, IndexName, new(new ColumnValue(ColumnType.Integer64, 15L)), rowId, unique: true);

        KvTransaction scanner = await BeginSerializableAsync(mgr);
        await store.InsertRow(scanner, scannersOwnRow, [1]);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireBoundedIndexRangeLockAsync(
                scanner, IndexName,
                new(new ColumnValue(ColumnType.Integer64, 10L)), true,
                new(new ColumnValue(ColumnType.Integer64, 20L)), true,
                unique: true),
            "An index entry at key 15 sits inside the requested [10, 20] bounds, so the younger " +
            "requester that already holds a write must be refused");

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code);

        await mgr.RollbackAsync(scanner);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter));

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The writer that won the ordering keeps both its row and its index entry");
    }
}
