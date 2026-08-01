
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
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// RO: assign one read timestamp T per read-only serializable transaction.
///
/// Verifies:
///   - Serializable+ReadOnly → ReadTimestamp is non-Zero (server-minted T).
///   - ReadTimestamp is identical across the same transaction object (stable single T).
///   - Two separate Serializable+ReadOnly transactions get non-decreasing read timestamps.
///   - All other {level}×{mode} combinations keep ReadTimestamp = Zero (RC fast path unchanged).
///   - Serializable+ReadOnly is zero-identity: TransactionId=Zero, commit/rollback are no-ops.
///   - ReadTimestamp is set before any reads so it is truly a pre-transaction snapshot.
/// </summary>
[TestFixture]
public sealed class TestReadTimestampPlumbing
{
    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> CreateAsync(string tag)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);
        return (node, new KvTransactionsManager(node.Kahuna, CamusDBConfig.Ambient));
    }

    // ------------------------------------------------------------------
    // 1. Serializable + ReadOnly → ReadTimestamp is non-Zero
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_HasNonZeroReadTimestamp()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-01");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.AreNotEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Serializable+ReadOnly must get a non-Zero server-minted read timestamp");
    }

    // ------------------------------------------------------------------
    // 2. ReadTimestamp is stable — same value on every property access
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_ReadTimestampIsStable()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-02");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        HLCTimestamp first  = tx.ReadTimestamp;
        HLCTimestamp second = tx.ReadTimestamp;

        Assert.AreEqual(first, second, "ReadTimestamp must be immutable once assigned");
    }

    // ------------------------------------------------------------------
    // 3. Two successive RO serializable transactions get non-decreasing T
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_SuccessiveTxsGetNonDecreasingTimestamps()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-03");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx1 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        KvTransaction tx2 = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // HLC is non-decreasing — tx2 must have a timestamp >= tx1
        bool nondecreasing = tx2.ReadTimestamp.L > tx1.ReadTimestamp.L
            || (tx2.ReadTimestamp.L == tx1.ReadTimestamp.L && tx2.ReadTimestamp.C >= tx1.ReadTimestamp.C);

        Assert.IsTrue(nondecreasing,
            $"ReadTimestamp must be non-decreasing: tx1={tx1.ReadTimestamp.L}/{tx1.ReadTimestamp.C} tx2={tx2.ReadTimestamp.L}/{tx2.ReadTimestamp.C}");
    }

    // ------------------------------------------------------------------
    // 4. All non-Serializable+ReadOnly paths keep ReadTimestamp = Zero
    // ------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_ReadWrite_HasZeroReadTimestamp()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-04a");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        Assert.AreEqual(HLCTimestamp.Zero, tx.ReadTimestamp);
        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ReadCommitted_ReadOnly_HasZeroReadTimestamp()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-04b");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly);
        Assert.AreEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "ReadCommitted+ReadOnly uses the Zero-snapshot fast path — ReadTimestamp stays Zero");
        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task Serializable_ReadWrite_HasZeroReadTimestamp()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-04c");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.AreEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Serializable+ReadWrite takes the locking path — ReadTimestamp stays Zero (reads are not snapshot reads)");
        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 5. Serializable+ReadOnly carries a real TransactionId (= ReadTimestamp = T)
    //
    // The minting Kahuna transaction is kept alive so the HTTP layer can register and
    // resume it by txnId across multiple requests.
    // TransactionId == ReadTimestamp == T (the server-minted HLC timestamp).
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_HasRealTransactionId()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-05");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.AreNotEqual(HLCTimestamp.Zero, tx.TransactionId,
            "Serializable+ReadOnly must carry a real non-Zero TransactionId so the coordinator can register and resume it by txnId");
        Assert.AreEqual(tx.TransactionId, tx.ReadTimestamp,
            "TransactionId and ReadTimestamp must be the same minted HLC timestamp T");
        Assert.IsTrue(tx.IsReadOnly);
        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode);

        await mgr.RollbackAsync(tx);
    }

    // ------------------------------------------------------------------
    // 6. Commit / rollback on Serializable+ReadOnly finalize the empty tx cleanly
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_CommitIsClean()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-06a");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.DoesNotThrowAsync(async () => await mgr.CommitAsync(tx),
            "CommitAsync on a Serializable+RO tx must finalize cleanly (lightweight rollback of empty tx)");
    }

    [Test]
    public async Task SerializableReadOnly_RollbackIsClean()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-06b");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.DoesNotThrowAsync(async () => await mgr.RollbackAsync(tx),
            "RollbackAsync on a Serializable+RO tx must finalize cleanly");
    }

    // ------------------------------------------------------------------
    // 7. ReadTimestamp is set BEFORE any reads (snapshot assigned at begin)
    // ------------------------------------------------------------------

    [Test]
    public async Task SerializableReadOnly_ReadTimestampSetBeforeAnyRead()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-07");
        await using EmbeddedKahuna __ = node;

        // Capture a wall-clock lower bound before begin
        KvTransaction txBaseline = await mgr.BeginAsync();
        HLCTimestamp beforeBegin = txBaseline.TransactionId;
        await mgr.RollbackAsync(txBaseline);

        KvTransaction tx = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // ReadTimestamp must be >= the timestamp minted just before it
        bool afterBaseline = tx.ReadTimestamp.L > beforeBegin.L
            || (tx.ReadTimestamp.L == beforeBegin.L && tx.ReadTimestamp.C >= beforeBegin.C);

        Assert.IsTrue(afterBaseline,
            $"ReadTimestamp ({tx.ReadTimestamp.L}/{tx.ReadTimestamp.C}) must be >= the baseline minted just before begin ({beforeBegin.L}/{beforeBegin.C})");
    }

    // ------------------------------------------------------------------
    // 8. Default begin (no args) still gives Zero ReadTimestamp
    // ------------------------------------------------------------------

    [Test]
    public async Task DefaultBegin_HasZeroReadTimestamp()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("RT-08");
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.AreEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Default BeginAsync must not assign a ReadTimestamp — zero observable change");

        await mgr.RollbackAsync(tx);
    }
}
