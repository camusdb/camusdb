
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
/// Verifies the per-transaction mutation cap (CADB0506). Each test builds its manager and store with
/// the small cap it wants, so the suite completes quickly and no cap leaks between tests — the limit
/// is fixed when those objects are constructed, which is why it is passed to the factory rather than
/// assigned afterwards.
///
/// [NonParallelizable]: each test starts its own embedded node. Now that nothing here mutates
/// process-wide configuration, that node is the only remaining reason for the attribute.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionMutationLimit
{
    /// <summary>
    /// Builds a node whose transaction manager and store are configured with <paramref name="mutationLimit"/>.
    /// The limit is fixed when they are constructed, so it is passed in rather than assigned to a global
    /// afterwards — an already-built manager would keep the configuration it was created with.
    /// </summary>
    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store)>
        CreateAsync(string tag, int mutationLimit)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        (KvTransactionsManager mgr, KvTableStore store) = WithLimit(node, tag, mutationLimit);
        return (node, mgr, store);
    }

    /// <summary>
    /// A second manager and store over the same node with a different mutation limit, for the cases that
    /// stage data under one limit and then exercise another.
    /// </summary>
    private static (KvTransactionsManager mgr, KvTableStore store) WithLimit(EmbeddedKahuna node, string tag, int mutationLimit)
    {
        CamusDBOptions options = CamusDBOptions.Default with { MaxMutationsPerTransaction = mutationLimit };
        return (new KvTransactionsManager(node.Kahuna, options), new KvTableStore(node.Kahuna, options, "testdb", tag));
    }

    // -----------------------------------------------------------------------
    // 1. Below-limit transaction commits normally
    // -----------------------------------------------------------------------

    [Test]
    public async Task BelowLimit_CommitsSuccessfully()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-01", 10);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        // Write 5 rows (5 mutations) — well under the limit of 10.
        for (int i = 0; i < 5; i++)
            await store.InsertRow(tx, new ObjectIdValue(i, 0, 0), [1, 2, 3]);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(tx),
            "Transaction under the mutation limit must commit without error");

        Assert.AreEqual(5, tx.MutationCount);
    }

    // -----------------------------------------------------------------------
    // 2. Exceeding the limit throws CADB0506; no partial rows visible after rollback
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExceedsLimit_ThrowsCADB0506_AndNoPartialRows()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-02", 3);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        // First 3 rows succeed (exactly at the limit).
        await store.InsertRow(tx, new ObjectIdValue(1, 0, 0), [1]);
        await store.InsertRow(tx, new ObjectIdValue(2, 0, 0), [2]);
        await store.InsertRow(tx, new ObjectIdValue(3, 0, 0), [3]);

        // 4th row crosses the cap.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.InsertRow(tx, new ObjectIdValue(4, 0, 0), [4]));

        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code);
        StringAssert.Contains("split the work into smaller transactions", ex?.Message ?? "");

        // Roll back and verify no rows are visible.
        await mgr.RollbackAsync(tx);

        KvTransaction reader = KvTransaction.CreateReadOnly();
        ReadOnlyMemory<byte>? row1 = await store.GetRow(reader, new ObjectIdValue(1, 0, 0));
        ReadOnlyMemory<byte>? row4 = await store.GetRow(reader, new ObjectIdValue(4, 0, 0));

        Assert.IsNull(row1, "Rolled-back row 1 must not be visible");
        Assert.IsNull(row4, "Never-written row 4 must not be visible");
    }

    // -----------------------------------------------------------------------
    // 3. Index multiplicity: 1 row + K indexes = 1+K mutations
    //    With limit=3 and K=2, the second INSERT trips the cap.
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexMultiplicity_CountsRowPlusIndexEntries()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-03", 3);
        await using EmbeddedKahuna __ = node;

        // 1 row + 2 index entries = 3 mutations per insert. With limit=3 the first insert
        // exhausts the budget; the second must throw.

        KvTransaction tx = await mgr.BeginAsync();

        // First row: 1 row write + 2 index entries = 3 mutations (exactly at the limit).
        ObjectIdValue rowId1 = new(1, 0, 0);
        CompositeColumnValue key1a = new([new ColumnValue(ColumnType.Integer64, 100L)]);
        CompositeColumnValue key1b = new([new ColumnValue(ColumnType.Integer64, 200L)]);
        await store.WriteRowsBatch(tx, [new KvTableStore.RowWrite
        {
            RowId = rowId1,
            RowData = BranchKvCodec.EncodeValue([1]),
            IndexEntries =
            [
                new KvTableStore.IndexWrite("idx_a", key1a, Unique: false),
                new KvTableStore.IndexWrite("idx_b", key1b, Unique: false),
            ]
        }]);

        Assert.AreEqual(3, tx.MutationCount, "1 row + 2 index entries = 3 mutations");

        // Second row: would add 3 more (total 6 > 3) — must throw.
        ObjectIdValue rowId2 = new(2, 0, 0);
        CompositeColumnValue key2a = new([new ColumnValue(ColumnType.Integer64, 101L)]);
        CompositeColumnValue key2b = new([new ColumnValue(ColumnType.Integer64, 201L)]);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            store.WriteRowsBatch(tx, [new KvTableStore.RowWrite
            {
                RowId = rowId2,
                RowData = BranchKvCodec.EncodeValue([2]),
                IndexEntries =
                [
                    new KvTableStore.IndexWrite("idx_a", key2a, Unique: false),
                    new KvTableStore.IndexWrite("idx_b", key2b, Unique: false),
                ]
            }]));

        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code);

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4. Single mass WriteRowsBatch exceeding the cap fails fast and writes nothing
    // -----------------------------------------------------------------------

    [Test]
    public async Task MassBatchInsert_ExceedsLimit_WritesNothing()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-04", 5);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        // Build a batch of 10 rows (10 mutations) — exceeds limit of 5.
        List<KvTableStore.RowWrite> rows = [];
        for (int i = 0; i < 10; i++)
            rows.Add(new KvTableStore.RowWrite { RowId = new ObjectIdValue(i, 0, 0), RowData = BranchKvCodec.EncodeValue([(byte)i]) });

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.WriteRowsBatch(tx, rows));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code);

        await mgr.RollbackAsync(tx);

        // None of the batch rows must be visible.
        KvTransaction reader = KvTransaction.CreateReadOnly();
        for (int i = 0; i < 10; i++)
        {
            ReadOnlyMemory<byte>? row = await store.GetRow(reader, new ObjectIdValue(i, 0, 0));
            Assert.IsNull(row, $"Batch row {i} must not be visible after rollback");
        }
    }

    // -----------------------------------------------------------------------
    // 5. Mass delete batch exceeding the cap fails fast
    // -----------------------------------------------------------------------

    [Test]
    public async Task MassBatchDelete_ExceedsLimit_Throws()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-05", 0);
        await using EmbeddedKahuna __ = node;

        // Insert 10 rows under unlimited mutations, then try to delete them with a cap of 5.
        KvTransaction setup = await mgr.BeginAsync();
        for (int i = 0; i < 10; i++)
            await store.InsertRow(setup, new ObjectIdValue(i, 0, 0), [(byte)i]);
        await mgr.CommitAsync(setup);
        (mgr, store) = WithLimit(node, "ML-05", 5);
        KvTransaction delTx = await mgr.BeginAsync();

        List<KvTableStore.RowDelete> deletes = [];
        for (int i = 0; i < 10; i++)
            deletes.Add(new KvTableStore.RowDelete { RowId = new ObjectIdValue(i, 0, 0) });

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.DeleteRowsBatch(delTx, deletes));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code);

        await mgr.RollbackAsync(delTx);
    }

    // -----------------------------------------------------------------------
    // 6. Update-twice counts twice (monotonic counter, not distinct-key set)
    // -----------------------------------------------------------------------

    [Test]
    public async Task UpdateTwice_CountsTwice()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-06", 3);
        await using EmbeddedKahuna __ = node;

        // Allow 3 mutations. Write the same row 3 times → should trip on the 4th.

        KvTransaction tx = await mgr.BeginAsync();

        ObjectIdValue rowId = new(1, 0, 0);

        await store.InsertRow(tx, rowId, [1]);  // count = 1
        await store.UpdateRow(tx, rowId, [2]);  // count = 2
        await store.UpdateRow(tx, rowId, [3]);  // count = 3

        // 4th write on the same row must throw.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => store.UpdateRow(tx, rowId, [4]));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code);

        Assert.AreEqual(3, tx.MutationCount);

        await mgr.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 7. DDL / unlimited transactions are truly exempt (mutationLimit = 0)
    // -----------------------------------------------------------------------

    [Test]
    public async Task DDLTransaction_Unlimited_DoesNotTrip()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-07", 3);
        await using EmbeddedKahuna __ = node;

        // A DDL/backfill transaction is created with mutationLimitOverride: 0.
        KvTransaction ddlTx = await mgr.BeginAsync(mutationLimitOverride: 0);

        // Write 100 rows — would exceed the user cap of 3 but the DDL tx has no limit.
        for (int i = 0; i < 100; i++)
            await store.InsertRow(ddlTx, new ObjectIdValue(i, 0, 0), [(byte)(i & 0xFF)]);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(ddlTx),
            "DDL transaction with mutationLimitOverride=0 must never trip the mutation cap");

        // MutationCount is 0 because ReserveMutations is a no-op when limit <= 0.
        Assert.AreEqual(0, ddlTx.MutationCount);
    }

    // -----------------------------------------------------------------------
    // 8. Disabled (MaxMutationsPerTransaction = 0) — no limit, parity with today
    // -----------------------------------------------------------------------

    [Test]
    public async Task Disabled_NoLimit_LargeTransactionCommits()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr, KvTableStore store) = await CreateAsync("ML-08", 0);
        await using EmbeddedKahuna __ = node;

        KvTransaction tx = await mgr.BeginAsync();

        for (int i = 0; i < 200; i++)
            await store.InsertRow(tx, new ObjectIdValue(i, 0, 0), [(byte)(i & 0xFF)]);

        Assert.DoesNotThrowAsync(() => mgr.CommitAsync(tx),
            "With the mutation limit disabled, a large transaction must commit without error");

        Assert.AreEqual(0, tx.MutationCount, "MutationCount is always 0 when limit is disabled");
    }

    // -----------------------------------------------------------------------
    // 9. Non-retryable: ReserveMutations exception is not in the retryable set
    // -----------------------------------------------------------------------

    [Test]
    public void CADB0506_IsNotRetryable()
    {
        CamusDBException ex = new(CamusDBErrorCodes.TransactionMutationLimitExceeded, "too many mutations");
        Assert.IsFalse(SerializableRetryHelper.IsRetryable(ex),
            "CADB0506 must not be retryable — the transaction must be split, not retried");
    }
}
