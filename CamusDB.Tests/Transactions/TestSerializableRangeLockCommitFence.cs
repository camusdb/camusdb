/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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
/// The commit-time half of range-lock enforcement: a write that was already staged when a foreign
/// range lock arrives must not commit into the range that lock covers.
///
/// <para><b>Ordering is the whole point, and it is easy to get wrong.</b> Acquiring the lock
/// <em>before</em> the write exercises a different mechanism — the write-time check, which refuses the
/// write outright with <see cref="CamusDBErrorCodes.TransactionMustRetry"/> and is covered by
/// <c>TestSerializableRangeLocks</c>. Every case here stages the write first and takes the lock
/// second, which is the interleaving a write-time check cannot see: acquisition deliberately steps
/// over a live foreign write intent rather than conflicting with it, so the only thing standing
/// between that write and the locked range is the check made when it commits.</para>
///
/// <para><b>Why it matters beyond isolation.</b> A range split opens its quiesce window by taking an
/// exclusive range lock over the half being moved, precisely so nothing commits into it while the
/// catch-up copy runs. A staged write that slipped past that lock would commit onto the source
/// partition after the copy was taken and become unreachable once the range routes to its new owner —
/// an acknowledged row that no longer exists. So these cases are the phantom guarantee and the
/// no-lost-write-under-split guarantee at once, and they hold in hash mode as well as under key-range
/// routing, because CamusDB takes these locks in both.</para>
///
/// <para>Each conflict case reads the row back afterwards rather than trusting the commit's error
/// code. A silently dropped write satisfies "the commit reported an abort" just as well as a working
/// fence does; only the read-back tells them apart.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableRangeLockCommitFence
{
    private const string IndexName = "rlf_idx";

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

    /// <summary>Reads a row through a throwaway transaction, so the assertion sees committed state.</summary>
    private static async Task<ReadOnlyMemory<byte>?> ReadBackAsync(
        KvTransactionsManager mgr, KvTableStore store, ObjectIdValue rowId)
    {
        KvTransaction reader = await mgr.BeginAsync();
        ReadOnlyMemory<byte>? row = await store.GetRow(reader, rowId);
        await mgr.CommitAsync(reader);

        return row;
    }

    // -----------------------------------------------------------------------
    // 1. The gap this fence closes: staged write, then the lock, then the commit
    // -----------------------------------------------------------------------

    [Test]
    public async Task RowWriteStagedBeforeAForeignRangeLockLands_IsAbortedAtCommitAndNeverBecomesVisible()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-01");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(1, 0, 0);

        // The write goes in while nothing is locked, so it is staged rather than refused.
        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [7]);

        // Only now does the scanner claim the row space. This acquisition steps over the live write
        // intent instead of conflicting with it, which is what leaves the commit as the last line of
        // defence.
        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(scanner);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => mgr.CommitAsync(inserter),
            "A staged row write must not commit into a row space a concurrent transaction has since locked");

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "The conflict is definite and nothing was committed, so it must surface as the retryable " +
            "conflict code an autocommit statement replays from BEGIN — not as an unknown outcome");

        await mgr.CommitAsync(scanner);

        Assert.IsNull(await ReadBackAsync(mgr, store, rowId),
            "The aborted write must leave nothing behind. An assertion on the commit's error code alone " +
            "would be satisfied by a write that landed anyway and was merely reported as aborted");
    }

    // -----------------------------------------------------------------------
    // 2. The fence must not block the lock's own holder
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheRangeLockHoldersOwnWriteInsideItsRange_StillCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-02");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(2, 0, 0);

        // This is the ordinary shape of a Serializable read-write transaction: scan the range it is
        // about to modify, then modify it. If the fence did not exclude the holder's own transaction,
        // every such transaction would abort itself.
        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireRowRangeLockAsync(scanner);
        await store.InsertRow(scanner, rowId, [9]);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(scanner),
            "A transaction must be able to write inside the range it holds the lock over");

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The holder's own write must be visible after its commit");
    }

    // -----------------------------------------------------------------------
    // 3. The fence must not over-block: a staged write outside the bounds commits
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexWriteStagedOutsideTheLockedBounds_StillCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-03");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(3, 0, 0);

        // Staged first, exactly as in case 1 — the only difference is where the key sorts.
        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [3]);
        await store.PutIndexEntry(
            inserter, IndexName, new(new ColumnValue(ColumnType.Integer64, 99L)), rowId, unique: true);

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireBoundedIndexRangeLockAsync(
            scanner, IndexName,
            new(new ColumnValue(ColumnType.Integer64, 10L)), true,
            new(new ColumnValue(ColumnType.Integer64, 20L)), true,
            unique: true);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(inserter),
            "Key 99 sorts outside the locked [10, 20] bounds, so the fence must let it through; " +
            "a fence that aborts here is checking the bucket rather than the range");

        await mgr.CommitAsync(scanner);

        Assert.IsNotNull(await ReadBackAsync(mgr, store, rowId),
            "The out-of-bounds write must be visible after its commit");
    }

    // -----------------------------------------------------------------------
    // 4. The same interleaving on an index key that does fall inside the bounds
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexWriteStagedInsideTheLockedBounds_IsAbortedAtCommit()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("RLF-04");
        await using EmbeddedKahuna _ = node;

        ObjectIdValue rowId = new(4, 0, 0);

        KvTransaction inserter = await mgr.BeginAsync();
        await store.InsertRow(inserter, rowId, [4]);
        await store.PutIndexEntry(
            inserter, IndexName, new(new ColumnValue(ColumnType.Integer64, 15L)), rowId, unique: true);

        KvTransaction scanner = await mgr.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await store.AcquireBoundedIndexRangeLockAsync(
            scanner, IndexName,
            new(new ColumnValue(ColumnType.Integer64, 10L)), true,
            new(new ColumnValue(ColumnType.Integer64, 20L)), true,
            unique: true);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => mgr.CommitAsync(inserter),
            "An index entry at key 15 lands inside the locked [10, 20] bounds and must not commit");

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code);

        await mgr.CommitAsync(scanner);

        Assert.IsNull(await ReadBackAsync(mgr, store, rowId),
            "The whole transaction aborts, so the row it also wrote must be absent too — a fence that " +
            "dropped only the offending key would leave a row with no index entry");
    }
}
