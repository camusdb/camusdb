
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
/// Verifies that a Serializable+RW transaction that outlives
/// <see cref="CamusDBConfig.MaxSerializableTransactionLifetimeMs"/> is invalidated by CamusDB
/// before its range locks can expire at Kahuna, preventing the silent isolation break that
/// would result from Kahuna expiring a lock while the transaction is still considered live.
///
/// Each test overrides MaxSerializableTransactionLifetimeMs to a small value (100 ms) so
/// the suite completes quickly. The original config value is restored in TearDown.
///
/// [NonParallelizable]: tests mutate the global CamusDBConfig.MaxSerializableTransactionLifetimeMs.
/// A concurrent fixture that begins a Serializable+RW transaction could see the 100 ms value
/// and spuriously hit TransactionLifetimeExceeded.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableTransactionDeadline
{
    private int originalDeadlineMs;

    [SetUp]
    public void SaveConfig()
    {
        originalDeadlineMs = CamusDBConfig.MaxSerializableTransactionLifetimeMs;
    }

    [TearDown]
    public void RestoreConfig()
    {
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = originalDeadlineMs;
    }

    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        KvTransactionsManager mgr = new(node.Kahuna);
        KvTableStore store = new(node.Kahuna, "testdb", tag);
        return (node, mgr, store);
    }

    // -----------------------------------------------------------------------
    // 1. Deadline fires on range-lock acquisition
    //
    // A Serializable+RW transaction acquires a range lock, then sleeps past the
    // configured deadline. When it tries to acquire another range lock, the deadline
    // guard throws TransactionLifetimeExceeded — the transaction is invalidated before
    // its first range lock can silently expire at Kahuna.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_ExceedsDeadline_RangeLockAcquisitionThrows()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("TXD-01");
        await using EmbeddedKahuna __ = node;

        CamusDBConfig.MaxSerializableTransactionLifetimeMs = 100; // 100 ms deadline

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // First lock: acquired while the transaction is still within its lifetime.
        await store.AcquireRowRangeLockAsync(tx);

        // Sleep past the deadline.
        await Task.Delay(200);

        // Second lock acquisition must detect the expired deadline.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.AcquireRowRangeLockAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionLifetimeExceeded, ex?.Code,
            "Range-lock acquisition on an over-deadline Serializable+RW transaction must throw TransactionLifetimeExceeded");

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. Deadline fires at commit
    //
    // A Serializable+RW transaction performs its work within the deadline but then
    // sits idle past it. When CommitAsync is called, the deadline guard aborts the
    // transaction (and releases any range locks) rather than letting it commit with
    // potentially expired locks.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_ExceedsDeadline_CommitThrows()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("TXD-02");
        await using EmbeddedKahuna __ = node;

        CamusDBConfig.MaxSerializableTransactionLifetimeMs = 100; // 100 ms deadline

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Acquire the range lock (within deadline).
        await store.AcquireRowRangeLockAsync(tx);

        // Sleep past the deadline.
        await Task.Delay(200);

        // CommitAsync must detect the expired deadline and throw rather than commit.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => mgr.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionLifetimeExceeded, ex?.Code,
            "CommitAsync on an over-deadline Serializable+RW transaction must throw TransactionLifetimeExceeded");

        // Transaction should be in RolledBack state — CommitAsync auto-releases range locks.
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status,
            "After a deadline-triggered commit failure, the transaction must be RolledBack");
    }

    // -----------------------------------------------------------------------
    // 3. After deadline-abort the range lock is released: a new writer can proceed
    //
    // This is the core correctness check from the spec's "Done when" clause:
    // the holder is aborted (not silently expired), so a foreign writer that
    // was blocked by the exclusive range lock can now acquire it.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_AfterDeadlineAbort_ForeignWriterSucceeds()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("TXD-03");
        await using EmbeddedKahuna __ = node;

        CamusDBConfig.MaxSerializableTransactionLifetimeMs = 100; // 100 ms deadline

        // Holder: acquires an exclusive row range lock.
        KvTransaction holder = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(holder, exclusive: true);

        // Sleep past the deadline.
        await Task.Delay(200);

        // Trigger the deadline abort via CommitAsync (auto-releases range locks).
        Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(holder));
        Assert.AreEqual(KvTransactionStatus.RolledBack, holder.Status);

        // Restore deadline to allow the writer to proceed normally.
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = originalDeadlineMs;

        // Foreign writer: must be able to insert into the row space now that the holder's
        // exclusive lock has been released by the abort.
        KvTransaction writer = await mgr.BeginAsync();
        ObjectIdValue newRowId = new(1, 0, 0);
        Assert.DoesNotThrowAsync(
            () => store.InsertRow(writer, newRowId, [42]),
            "After the deadline-aborted holder releases its exclusive range lock, a new insert must succeed");

        await mgr.CommitAsync(writer);
    }

    // -----------------------------------------------------------------------
    // 4. ReadCommitted transactions are not subject to the deadline
    //
    // The deadline check only applies to Serializable+RW transactions (which hold
    // range locks). ReadCommitted transactions do not acquire range locks, so
    // IsExpired must never trigger for them regardless of elapsed time.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_NotSubjectToDeadline()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("TXD-04");
        await using EmbeddedKahuna __ = node;

        CamusDBConfig.MaxSerializableTransactionLifetimeMs = 100; // 100 ms deadline

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        // Sleep past the configured deadline.
        await Task.Delay(200);

        // Insert must succeed: ReadCommitted does not acquire range locks and the
        // deadline guard only fires for Serializable+RW transactions.
        ObjectIdValue newRowId = new(1, 0, 0);
        Assert.DoesNotThrowAsync(
            () => store.InsertRow(tx, newRowId, [42]),
            "ReadCommitted transaction must not be affected by the serializable deadline");

        await mgr.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 5. Disabled deadline (MaxSerializableTransactionLifetimeMs <= 0): no abort
    //
    // Setting the deadline to 0 or negative disables enforcement. A Serializable+RW
    // transaction can then commit after an arbitrary sleep with no error.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_DeadlineDisabled_CommitsSuccessfully()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("TXD-05");
        await using EmbeddedKahuna __ = node;

        CamusDBConfig.MaxSerializableTransactionLifetimeMs = 0; // disabled

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(tx);

        // Sleep briefly — would exceed a 100 ms deadline if it were active.
        await Task.Delay(50);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(tx),
            "With the deadline disabled, a Serializable+RW transaction must commit without error");
    }
}
