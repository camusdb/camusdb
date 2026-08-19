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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The write path across a range split: rows written while a range is being divided, rows written
/// after the boundary has moved, and a transaction whose writes straddle the boundary.
///
/// <para><b>What could go wrong, and why row counts alone would not show it.</b> A split copies the
/// moving half to its new owner and then flips routing to it. A write that commits onto the old owner
/// after the copy was taken is acknowledged to the client and then becomes unreachable — no error is
/// raised anywhere. So every case here records the identity of what the client was told had succeeded
/// and reads that exact set back; asserting on commit statuses, or on counts, is what would miss it.</para>
///
/// <para>Every case also asserts the space really did divide. A refused split leaves one range, under
/// which all of this passes trivially and proves nothing.</para>
///
/// <para><b>Two id sequences, and they are not interchangeable.</b> The <c>id</c> column is what the
/// client supplied and what identifies a row to a reader; the KV row id is minted by the inserter and
/// is what determines which range a row lands in. Membership assertions here read the <c>id</c> column
/// back through SQL, while anything about which side of the boundary a row sits on is computed from
/// row ids. Comparing one against the other silently compares unrelated sequences.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node with several partitions and drives Raft-committed range
// splits, whose timing is disturbed by other node-booting fixtures running alongside.
[NonParallelizable]
public sealed class TestKeyRangeWriteAcrossSplit : KeyRangeSplitFixture
{
    /// <summary>
    /// How long the split driver may keep retrying while writes are in flight. Generous on purpose —
    /// an in-flight write makes the splitter's non-empty check unanswerable, so under continuous
    /// traffic a split lands in a gap between transactions rather than on the first attempt.
    /// </summary>
    private static readonly TimeSpan SplitBudget = TimeSpan.FromSeconds(30);

    /// <summary>Writes that must be acknowledged before the split is even attempted.</summary>
    private const int WritesBeforeSplit = 5;

    /// <summary>Writes the writer keeps issuing after cutover, so the post-split path is covered too.</summary>
    private const int WritesAfterSplit = 20;

    /// <summary>Backstop so a split that never lands cannot turn the writer into an infinite loop.</summary>
    private const int WriterHardCap = 400;

    // -----------------------------------------------------------------------
    // Read-back helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Every <c>id</c> the table currently holds, read through SQL — the same way a client would find
    /// out whether the row it wrote is still there.
    /// </summary>
    private static async Task<HashSet<string>> ReadAllIdsAsync(string db, CommandExecutor executor)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: "SELECT id FROM readings", parameters: null));

            HashSet<string> ids = new(StringComparer.Ordinal);

            await foreach (QueryResultRow row in cursor)
                ids.Add(row.Row["id"].StrValue!);

            await database.Transactions.CommitAsync(tx);

            return ids;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    /// <summary>
    /// Inserts one row in its own transaction and reports whether the client was told it committed.
    ///
    /// <para>A conflict abort is retried from a fresh transaction, which is what CamusDB's autocommit
    /// path does with this error class — the transaction is dead, but nothing was written, so replaying
    /// it is safe. Exhausting the retries is reported as "not acknowledged" rather than failing: a
    /// write refused under contention is a correct outcome, and the guarantee under test concerns
    /// writes the client was told had succeeded.</para>
    /// </summary>
    private static async Task<bool> TryInsertOneAsync(
        CommandExecutor executor, string db, string id, long amount)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);

        for (int attempt = 0; attempt < 6; attempt++)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();

            try
            {
                await executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "readings",
                    values: new() { new() {
                        { "id",     new(ColumnType.Id,        id) },
                        { "label",  new(ColumnType.String,    $"concurrent-{amount}") },
                        { "amount", new(ColumnType.Integer64, amount) },
                    }}));

                await database.Transactions.CommitAsync(tx);

                return true;
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
                await Task.Delay(20 * (attempt + 1));
            }
        }

        return false;
    }

    /// <summary>
    /// Asserts the table's rows do not all sit on one side of <paramref name="splitKey"/>. Without
    /// this, a case claiming to write "across the boundary" could have put everything in one child
    /// range and still passed.
    /// </summary>
    private static void AssertRowsExistOnBothSides(
        TableDescriptor table, IReadOnlyList<ObjectIdValue> rowIds, string splitKey)
    {
        int below = rowIds.Count(id => string.CompareOrdinal(table.Store.RowPointKey(id), splitKey) < 0);

        Assert.That(below, Is.GreaterThan(0).And.LessThan(rowIds.Count),
            $"{below} of {rowIds.Count} rows sort below the boundary, so one child range holds nothing " +
            "and this run never exercised a write reaching two partitions");
    }

    // -----------------------------------------------------------------------
    // 1. Writes running while the split runs
    // -----------------------------------------------------------------------

    [Test]
    public async Task ConcurrentWritesAndASplit_LoseNoAcknowledgedRow()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> seededIds) = await SetupTableAsync();

        string splitKey = MedianRowKey(table, await ScanRowIdsAsync(table, executor, db));

        // Writers and the split run against each other rather than in sequence, and the writer is held
        // open until after cutover so that acknowledged writes exist on all three sides of the event:
        // before it, in flight during it, and after the boundary has moved. Row ids are minted per
        // insert and spread across the space, so they land in both the half that moves and the half
        // that stays.
        List<string> acknowledged = [];
        object gate = new();
        int stopAfter = int.MaxValue;
        int attempted = 0;

        Task writer = Task.Run(async () =>
        {
            while (Volatile.Read(ref attempted) < Volatile.Read(ref stopAfter)
                   && Volatile.Read(ref attempted) < WriterHardCap)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                bool ok = await TryInsertOneAsync(executor, db, id, 1_000 + Volatile.Read(ref attempted));

                if (ok)
                    lock (gate) acknowledged.Add(id);

                Interlocked.Increment(ref attempted);
            }
        });

        // Let real writes commit before the split starts, so the split is genuinely landing on a range
        // that is being written to rather than on a quiet one.
        DateTime warmupDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (AcknowledgedCount(acknowledged, gate) < WritesBeforeSplit && DateTime.UtcNow < warmupDeadline)
            await Task.Delay(20);

        int acknowledgedBeforeSplit = AcknowledgedCount(acknowledged, gate);

        int ranges = await SplitAtWithinAsync(table.Store.RowKeySpace, splitKey, SplitBudget);

        // Keep writing past cutover, then let the writer drain.
        Volatile.Write(ref stopAfter, Volatile.Read(ref attempted) + WritesAfterSplit);

        await writer;

        Assert.That(ranges, Is.GreaterThan(1),
            "The space never divided, so nothing here was written across a boundary");

        Assert.That(acknowledgedBeforeSplit, Is.GreaterThan(0),
            "No write was acknowledged before the split began, so this run did not exercise a split " +
            "landing on a range under live traffic");

        Assert.That(acknowledged, Has.Count.GreaterThan(acknowledgedBeforeSplit),
            "No write was acknowledged after the split, so the post-cutover write path went untested");

        HashSet<string> expected = seededIds.Concat(acknowledged).ToHashSet(StringComparer.Ordinal);
        HashSet<string> actual = await ReadAllIdsAsync(db, executor);

        Assert.That(actual, Is.SupersetOf(expected),
            $"{expected.Except(actual).Count()} row(s) the client was told had committed are missing " +
            "after the split — the shape of a write that landed on the old owner after its contents " +
            "were copied to the new one");

        Assert.That(actual.Count, Is.EqualTo(expected.Count),
            "The read returned rows nobody was told had committed");

        AssertRowsExistOnBothSides(table, await ScanRowIdsAsync(table, executor, db), splitKey);
    }

    private static int AcknowledgedCount(List<string> acknowledged, object gate)
    {
        lock (gate) return acknowledged.Count;
    }

    // -----------------------------------------------------------------------
    // 2. Writes after the boundary moved
    // -----------------------------------------------------------------------

    [Test]
    public async Task WritesAfterCutover_LandOnBothSidesOfTheBoundaryAndAreAllReadable()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> seededIds) = await SetupTableAsync();

        string splitKey = await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<string> written = await InsertRowsAsync(db, executor, RowCount, startingAt: RowCount);

        HashSet<string> expected = seededIds.Concat(written).ToHashSet(StringComparer.Ordinal);
        HashSet<string> actual = await ReadAllIdsAsync(db, executor);

        Assert.That(actual, Is.EquivalentTo(expected),
            "A row written after cutover that routed to the old owner would be missing here");

        AssertRowsExistOnBothSides(table, await ScanRowIdsAsync(table, executor, db), splitKey);
    }

    // -----------------------------------------------------------------------
    // 3. A single transaction writing on both sides of the boundary
    // -----------------------------------------------------------------------

    [Test]
    public async Task OneTransactionWritingOnBothSidesOfTheBoundary_CommitsAllOrNothing()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> seededIds) = await SetupTableAsync();

        string splitKey = await SplitRowSpaceAtMedianAsync(table, executor, db);

        DatabaseDescriptor database = await executor.OpenDatabase(db);

        // Committed batch: one transaction, rows spread across the whole space, so its commit has to be
        // agreed by both partitions rather than one.
        KvTransaction committed = await database.Transactions.BeginAsync();
        List<string> committedIds = [];

        for (int i = 0; i < 30; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            committedIds.Add(id);

            await executor.Insert(new InsertTicket(
                txnState: committed, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        id) },
                    { "label",  new(ColumnType.String,    $"straddle-{i}") },
                    { "amount", new(ColumnType.Integer64, 2_000L + i) },
                }}));
        }

        await database.Transactions.CommitAsync(committed);

        HashSet<string> expected = seededIds.Concat(committedIds).ToHashSet(StringComparer.Ordinal);

        Assert.That(await ReadAllIdsAsync(db, executor), Is.EquivalentTo(expected),
            "A transaction spanning two child ranges must commit all of its writes or none of them");

        AssertRowsExistOnBothSides(table, await ScanRowIdsAsync(table, executor, db), splitKey);

        // Rolled-back batch: nothing from it may survive, on either side.
        KvTransaction rolledBack = await database.Transactions.BeginAsync();
        List<string> discardedIds = [];

        for (int i = 0; i < 30; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            discardedIds.Add(id);

            await executor.Insert(new InsertTicket(
                txnState: rolledBack, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        id) },
                    { "label",  new(ColumnType.String,    $"discarded-{i}") },
                    { "amount", new(ColumnType.Integer64, 3_000L + i) },
                }}));
        }

        await database.Transactions.RollbackAsync(rolledBack);

        Assert.That(await ReadAllIdsAsync(db, executor), Is.EquivalentTo(expected),
            "A rolled-back row survived, or a committed one disappeared. Rollback has to reach both " +
            "child ranges, not only the one the transaction's first write happened to route to");
    }

    // -----------------------------------------------------------------------
    // 4. A range lock still fences a phantom once the space it covers has split
    // -----------------------------------------------------------------------

    [Test]
    public async Task ARangeLockTakenAfterTheSplit_StillBlocksAPhantomInTheSplitSpace()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        DatabaseDescriptor database = await executor.OpenDatabase(db);

        // One lock request over a space that is now two ranges. It has to become a clipped sub-lock on
        // each of them; a lock covering only the range the request happened to route to would leave the
        // other child open to exactly the phantom this protects against.
        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await table.Store.AcquireRowRangeLockAsync(scanner);

        KvTransaction inserter = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.Insert(new InsertTicket(
                txnState: inserter, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "label",  new(ColumnType.String,    "phantom") },
                    { "amount", new(ColumnType.Integer64, 4_000L) },
                }})),
            "An insert into a row space held under a foreign range lock must be refused, whether that " +
            "space is one range or several");

        Assert.That(SerializableRetryHelper.IsRetryable(ex!), Is.True,
            $"The refusal must be retryable so a caller can replay it once the lock clears; got {ex!.Code}");

        await database.Transactions.RollbackIfNotCompletedAsync(inserter);
        await database.Transactions.CommitAsync(scanner);
    }
    // -----------------------------------------------------------------------
    // 5. A range lock taken before the split must still hold after it. The
    //    lock is not what blocks the split — the range divides underneath it
    //    and its live locks are clamped onto the children — so the question
    //    is whether the protection survives the move.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ARangeLockTakenBeforeTheSplit_StillBlocksWritesInBothChildRangesAfterCutover()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;
        string splitKey = MedianRowKey(table, await ScanRowIdsAsync(table, executor, db));

        DatabaseDescriptor database = await executor.OpenDatabase(db);

        // The lock is taken first, while the space is still one range, and is never re-acquired.
        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await table.Store.AcquireRowRangeLockAsync(scanner);

        Assert.That(await SplitAtWithinAsync(keySpace, splitKey, SplitBudget), Is.EqualTo(2),
            "A held range lock does not stop the range from dividing — it is carried onto the children " +
            "instead. If the split were refused here, the rest of this case would be vacuous");

        // Probe both children. Row ids are chosen rather than generated because a generated one carries
        // the current time and always sorts above every existing row, which would leave the lower child
        // untested — and the lower child is the half that stayed on the original partition, so a lock
        // that failed to follow the moving half would still block there and look correct.
        ObjectIdValue belowBoundary = new(1, 0, 0);
        ObjectIdValue aboveBoundary = ObjectIdGenerator.Generate();

        Assert.That(string.CompareOrdinal(table.Store.RowPointKey(belowBoundary), splitKey), Is.LessThan(0),
            "The low probe must sort into the lower child range");
        Assert.That(string.CompareOrdinal(table.Store.RowPointKey(aboveBoundary), splitKey), Is.GreaterThan(0),
            "The high probe must sort into the upper child range");

        foreach ((ObjectIdValue rowId, string side) in
                 new[] { (belowBoundary, "lower"), (aboveBoundary, "upper") })
        {
            KvTransaction blocked = await database.Transactions.BeginAsync();

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                () => table.Store.InsertRow(blocked, rowId, [1]),
                $"A write into the {side} child range must still be refused: the lock was taken over the " +
                "whole space before it divided, and a phantom would open at the boundary if either child " +
                "came out from under it");

            Assert.That(SerializableRetryHelper.IsRetryable(ex!), Is.True,
                $"The refusal must be retryable so a caller can replay once the lock clears; got {ex!.Code}");

            await database.Transactions.RollbackIfNotCompletedAsync(blocked);
        }

        // The refusals are a live conflict, not a property of the post-split space: once the holder is
        // gone the same writes go through. Without this half, a lock that had been dropped entirely and
        // a lock still doing its job would be indistinguishable.
        await database.Transactions.CommitAsync(scanner);

        KvTransaction after = await database.Transactions.BeginAsync();
        await table.Store.InsertRow(after, belowBoundary, [1]);
        await table.Store.InsertRow(after, aboveBoundary, [2]);

        Assert.DoesNotThrowAsync(() => database.Transactions.CommitAsync(after),
            "Both writes must succeed once the range lock has been released");
    }

}
