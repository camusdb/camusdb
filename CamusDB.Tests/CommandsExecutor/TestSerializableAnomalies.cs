
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Serializable-isolation acceptance suite — proves serializability at N = 1 (single embedded Kahuna node).
///
/// Each test targets one classic isolation anomaly and asserts it cannot occur under
/// Serializable isolation. Tests are grouped by anomaly:
///
///   RO snapshot (Serializable+ReadOnly):
///     1. Non-repeatable read eliminated — snapshot pins value at T; concurrent write invisible.
///     2. Phantom eliminated — snapshot pins row set at T; concurrent insert invisible.
///     3. RO never blocks concurrent writers — snapshot txn runs alongside active writers.
///
///   RW strict 2PL (Serializable+ReadWrite):
///     4. Non-repeatable read eliminated — shared read lock prevents concurrent write.
///     5. Phantom eliminated — shared predicate range lock blocks phantom insert.
///     6. Write skew prevented — S→X upgrade conflict aborts one of two concurrent RW txns.
///     7. Lost update prevented — same S→X upgrade conflict, only one write succeeds.
///     8. Contention resolves quickly — no TTL stall; range lock conflict is immediate.
///
/// The cluster equivalents (N = 3) are in CamusDB.Cluster.Tests/TestSerializableAnomaliesCluster.cs.
/// Together they satisfy the single-node ≡ cluster invariant.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableAnomalies : SharedNodeBaseTest
{
    // accounts(id Id PK, name String NOT NULL, balance Integer64)
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, string bobId)>
        SetupAccountsAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String,     notNull: true),
                new("balance", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        string aliceId = ObjectIdGenerator.Generate().ToString();
        string bobId   = ObjectIdGenerator.Generate().ToString();

        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        aliceId) },
                { "name",    new(ColumnType.String,    "alice") },
                { "balance", new(ColumnType.Integer64, 100L)   },
            }}));
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        bobId) },
                { "name",    new(ColumnType.String,    "bob") },
                { "balance", new(ColumnType.Integer64, 100L) },
            }}));
        await db.Transactions.CommitAsync(setup);

        return (dbname, db, executor, aliceId, bobId);
    }

    // Point-read balance by PK — triggers LookupUnique + GetRow, acquiring S point locks on both
    // the index entry and the row when tx is Serializable+RW.
    private static async Task<long> ReadBalanceAsync(
        string dbname, CommandExecutor executor, KvTransaction tx, string accountId)
    {
        QueryTicket ticket = new(
            txnState:     tx,
            databaseName: dbname,
            tableName:    "accounts",
            index:        null,
            projection:   null,
            where:        null,
            filters:      new() { new("id", "=", new(ColumnType.Id, accountId)) },
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows[0].Row["balance"].LongValue;
    }

    // Update balance via PK filter — uses QueryFromIndex locate (S→X upgrade on write).
    private static async Task<UpdateResult> UpdateBalanceAsync(
        string dbname, CommandExecutor executor, KvTransaction tx,
        string accountId, long newBalance)
    {
        return await executor.Update(new UpdateTicket(
            txnState:     tx,
            databaseName: dbname,
            tableName:    "accounts",
            plainValues:  new() { { "balance", new(ColumnType.Integer64, newBalance) } },
            exprValues:   null,
            where:        null,
            filters:      new() { new("id", "=", new(ColumnType.Id, accountId)) },
            parameters:   null
        ));
    }

    // Full-table scan — acquires shared range lock on rows when tx is Serializable+RW.
    private static async Task<List<QueryResultRow>> ScanAllAsync(
        string dbname, CommandExecutor executor, KvTransaction tx)
    {
        QueryTicket ticket = new(
            txnState:     tx,
            databaseName: dbname,
            tableName:    "accounts",
            index:        null,
            projection:   null,
            where:        null,
            filters:      null,
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        return await cursor.ToListAsync();
    }

    // -----------------------------------------------------------------------
    // 1. Serializable+RO eliminates non-repeatable read
    //
    // TxA opens a snapshot at T, reads alice's balance (100).
    // A concurrent ReadCommitted writer updates alice to 999 and commits.
    // TxA reads alice again — must still see 100 (snapshot pinned to T).
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_NonRepeatableRead_Eliminated()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, _) =
            await SetupAccountsAsync();

        KvTransaction snapshot = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        long first = await ReadBalanceAsync(dbname, executor, snapshot, aliceId);
        Assert.AreEqual(100L, first);

        // Concurrent writer updates alice outside the snapshot.
        KvTransaction writer = await db.Transactions.BeginAsync();
        await UpdateBalanceAsync(dbname, executor, writer, aliceId, 999L);
        await db.Transactions.CommitAsync(writer);

        long second = await ReadBalanceAsync(dbname, executor, snapshot, aliceId);
        Assert.AreEqual(100L, second,
            "Serializable+RO snapshot must not see writes committed after the snapshot timestamp");

        await db.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 2. Serializable+RO eliminates phantom reads
    //
    // TxA opens a snapshot, scans — sees 2 rows.
    // A concurrent writer inserts a third row and commits.
    // TxA scans again — must still see 2 rows (snapshot pinned to T).
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_Phantom_Eliminated()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _, _) =
            await SetupAccountsAsync();

        KvTransaction snapshot = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        List<QueryResultRow> first = await ScanAllAsync(dbname, executor, snapshot);
        Assert.AreEqual(2, first.Count);

        // Phantom insert after snapshot.
        KvTransaction writer = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: writer, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "name",    new(ColumnType.String,    "carol") },
                { "balance", new(ColumnType.Integer64, 50L) },
            }}));
        await db.Transactions.CommitAsync(writer);

        List<QueryResultRow> second = await ScanAllAsync(dbname, executor, snapshot);
        Assert.AreEqual(2, second.Count,
            "Serializable+RO snapshot must not see rows inserted after the snapshot timestamp");

        await db.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 3. Serializable+RO never blocks concurrent writers
    //
    // A Serializable+ReadOnly txn acquires no locks, so active writers are
    // never blocked by it. The snapshot simply reads as-of T; writers commit
    // freely (with values invisible to the snapshot).
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRO_NeverBlocks_ConcurrentWriters()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, _) =
            await SetupAccountsAsync();

        KvTransaction snapshot = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        // Snapshot reads alice (no lock acquired — pure MVCC).
        long balanceBefore = await ReadBalanceAsync(dbname, executor, snapshot, aliceId);
        Assert.AreEqual(100L, balanceBefore);

        // Concurrent writer runs and commits freely — not blocked by the RO snapshot.
        KvTransaction writer = await db.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(
            async () =>
            {
                await UpdateBalanceAsync(dbname, executor, writer, aliceId, 200L);
                await db.Transactions.CommitAsync(writer);
            },
            "A concurrent writer must not be blocked by a Serializable+ReadOnly snapshot transaction");

        // Snapshot still sees the pre-snapshot value.
        long balanceAfter = await ReadBalanceAsync(dbname, executor, snapshot, aliceId);
        Assert.AreEqual(100L, balanceAfter,
            "Snapshot must remain pinned to its timestamp even after a concurrent commit");

        await db.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // 4. Serializable+RW eliminates non-repeatable read
    //
    // TxA (Serializable+RW) reads alice — acquires a shared point lock on her row.
    // A concurrent writer (ReadCommitted) tries to update alice — the write-path
    // fence detects TxA's shared lock and returns MustRetry repeatedly until the
    // deadline, ultimately throwing TransactionMustRetry.
    // TxA reads alice again — still 100 (the write never committed).
    // TxA commits → the writer can then proceed (when retried by the caller).
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_NonRepeatableRead_Eliminated()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, _) =
            await SetupAccountsAsync();

        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // TxA reads alice — S point lock acquired on alice's index key and row key.
        long first = await ReadBalanceAsync(dbname, executor, txA, aliceId);
        Assert.AreEqual(100L, first);

        // Concurrent writer tries to write alice — blocked by TxA's shared lock.
        KvTransaction writer = await db.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(dbname, executor, writer, aliceId, 999L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Write to a key locked by a Serializable+RW reader must fail with TransactionMustRetry");

        await db.Transactions.RollbackAsync(writer);

        // TxA reads alice again — still 100 (the writer was blocked).
        long second = await ReadBalanceAsync(dbname, executor, txA, aliceId);
        Assert.AreEqual(100L, second,
            "Serializable+RW read lock must prevent any write to the read key");

        await db.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 5. Serializable+RW eliminates phantom reads
    //
    // TxA (Serializable+RW) scans the full table — acquires a shared range lock
    // on the row key space [−∞, +∞].
    // A concurrent writer tries to insert a new row — the write-path phantom
    // fence detects the shared range lock and returns MustRetry (TransactionMustRetry).
    // TxA scans again — still 2 rows (no phantom).
    // TxA commits → the insert can then retry successfully.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_Phantom_Eliminated()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _, _) =
            await SetupAccountsAsync();

        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        List<QueryResultRow> first = await ScanAllAsync(dbname, executor, txA);
        Assert.AreEqual(2, first.Count);

        // Concurrent insert blocked by shared range lock phantom fence.
        KvTransaction inserter = await db.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.Insert(new InsertTicket(
                txnState: inserter, databaseName: dbname, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "name",    new(ColumnType.String,    "carol") },
                    { "balance", new(ColumnType.Integer64, 50L) },
                }})));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Phantom insert into a Serializable+RW scan range must fail with TransactionMustRetry");

        await db.Transactions.RollbackAsync(inserter);

        // TxA scans again — still 2 rows (no phantom).
        List<QueryResultRow> second = await ScanAllAsync(dbname, executor, txA);
        Assert.AreEqual(2, second.Count,
            "Serializable+RW range lock must prevent phantom inserts");

        await db.Transactions.CommitAsync(txA);

        // After TxA commits the range lock is released; a fresh insert succeeds.
        KvTransaction inserter2 = await db.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(async () =>
        {
            await executor.Insert(new InsertTicket(
                txnState: inserter2, databaseName: dbname, tableName: "accounts",
                values: new() { new() {
                    { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "name",    new(ColumnType.String,    "dave") },
                    { "balance", new(ColumnType.Integer64, 75L) },
                }}));
            await db.Transactions.CommitAsync(inserter2);
        }, "Insert must succeed once the Serializable+RW reader has committed");
    }

    // -----------------------------------------------------------------------
    // 6. Serializable+RW prevents write skew
    //
    // Classic write-skew anomaly:
    //   Constraint: total balance of all accounts must stay >= 0.
    //   alice = 100, bob = 100 (total = 200).
    //   TxA reads both accounts (total = 200, valid), plans to set alice = -50.
    //   TxB reads both accounts (total = 200, valid), plans to set bob = -50.
    //   Under Read Committed both would commit → alice=-50, bob=-50, total=-100 (violation).
    //   Under Serializable the S→X upgrade conflict aborts one transaction.
    //
    // Mechanism: TxA reads alice (S lock on alice's row). TxB reads alice (S lock, S∩S fine).
    // TxA's UPDATE alice needs to upgrade S→X on alice's row — TxB holds S → AlreadyLocked →
    // TxA gets TransactionConflict. TxA aborts. TxB can then update bob (its S lock on bob
    // has no competitor once TxA is gone) and commit. Final: alice=100, bob=-50, total=50 >= 0.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_WriteSkew_Prevented()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, string bobId) =
            await SetupAccountsAsync();

        // Both transactions read both accounts — S point locks on all four {index, row} × {alice, bob}.
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        long aliceA = await ReadBalanceAsync(dbname, executor, txA, aliceId);
        long bobA   = await ReadBalanceAsync(dbname, executor, txA, bobId);
        long aliceB = await ReadBalanceAsync(dbname, executor, txB, aliceId);
        long bobB   = await ReadBalanceAsync(dbname, executor, txB, bobId);

        Assert.AreEqual(200L, aliceA + bobA, "TxA sees total = 200 initially");
        Assert.AreEqual(200L, aliceB + bobB, "TxB sees total = 200 initially");

        // TxA tries to update alice: S→X upgrade on alice's row — but TxB holds S on alice.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(dbname, executor, txA, aliceId, -50L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "TxA's write to alice must fail with TransactionConflict because TxB holds a shared lock on alice");

        await db.Transactions.RollbackAsync(txA);

        // TxB can update bob and commit (TxA's S locks are gone after rollback).
        await UpdateBalanceAsync(dbname, executor, txB, bobId, -50L);
        await db.Transactions.CommitAsync(txB);

        // Verify final state: alice=100, bob=-50, total=50 ≥ 0 (constraint satisfied).
        KvTransaction verify = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        long finalAlice = await ReadBalanceAsync(dbname, executor, verify, aliceId);
        long finalBob   = await ReadBalanceAsync(dbname, executor, verify, bobId);
        await db.Transactions.CommitAsync(verify);

        Assert.AreEqual(100L,  finalAlice, "Alice's balance must be untouched (TxA aborted)");
        Assert.AreEqual(-50L,  finalBob,   "Bob's balance must reflect TxB's committed write");
        Assert.GreaterOrEqual(finalAlice + finalBob, 0L, "Constraint: total balance >= 0 must hold");
    }

    // -----------------------------------------------------------------------
    // 7. Serializable+RW prevents lost updates
    //
    // Both TxA and TxB read K=100 (S point locks acquired).
    // TxA tries to write K=999 → S→X upgrade conflict with TxB's S lock → TransactionConflict.
    // TxA aborts. TxB writes K=999 successfully.
    // Under Read Committed both might see 100 and write 999, but the "lost update"
    // scenario with increments (both read 100, both increment by 1 → result should be 102
    // but RC gives 101) is prevented: one writer aborts, the other's effect is preserved.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_LostUpdate_Prevented()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, string aliceId, _) =
            await SetupAccountsAsync();

        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both read alice — S point locks; S∩S compatible.
        long balA = await ReadBalanceAsync(dbname, executor, txA, aliceId);
        long balB = await ReadBalanceAsync(dbname, executor, txB, aliceId);
        Assert.AreEqual(100L, balA);
        Assert.AreEqual(100L, balB);

        // TxA tries to write: S→X upgrade blocked by TxB's S on alice's row.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdateBalanceAsync(dbname, executor, txA, aliceId, 999L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Lost-update write must fail: TxA cannot upgrade its S lock while TxB holds S on the same row");

        await db.Transactions.RollbackAsync(txA);

        // TxB commits its write (TxA's lock is gone).
        await UpdateBalanceAsync(dbname, executor, txB, aliceId, 999L);
        await db.Transactions.CommitAsync(txB);

        // Verify TxB's write was not lost.
        KvTransaction verify = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        long final = await ReadBalanceAsync(dbname, executor, verify, aliceId);
        await db.Transactions.CommitAsync(verify);

        Assert.AreEqual(999L, final, "TxB's committed write must not be lost");
    }

    // -----------------------------------------------------------------------
    // 8. Contention resolves quickly — no TTL stall on range lock conflict
    //
    // When TxA holds the exclusive predicate range lock (via a full-table UPDATE),
    // TxB's concurrent UPDATE must fail with TransactionConflict immediately —
    // no waiting for a 30-second TTL. This exercises the AlreadyLocked → fast-fail
    // path rather than the MustRetry → deadline path.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_ContentionResolvesImmediately_NoTTLStall()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor, _, _) =
            await SetupAccountsAsync();

        // TxA runs a full-table UPDATE — acquires Exclusive predicate range lock [−∞, +∞].
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        await executor.Update(new UpdateTicket(
            txnState:     txA,
            databaseName: dbname,
            tableName:    "accounts",
            plainValues:  new() { { "balance", new(ColumnType.Integer64, 200L) } },
            exprValues:   null,
            where:        null,
            filters:      null,
            parameters:   null
        ));

        // TxB tries a full-table UPDATE while TxA holds the exclusive range lock.
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        long start = System.Diagnostics.Stopwatch.GetTimestamp();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.Update(new UpdateTicket(
                txnState:     txB,
                databaseName: dbname,
                tableName:    "accounts",
                plainValues:  new() { { "balance", new(ColumnType.Integer64, 300L) } },
                exprValues:   null,
                where:        null,
                filters:      null,
                parameters:   null
            )));

        long elapsedMs = (long)((System.Diagnostics.Stopwatch.GetTimestamp() - start)
            / (double)System.Diagnostics.Stopwatch.Frequency * 1000);

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Concurrent full-table UPDATE must fail with TransactionConflict (AlreadyLocked fast path)");
        Assert.Less(elapsedMs, 2000,
            "Conflict must be detected immediately (< 2 s), not after a 30 s TTL stall");

        await db.Transactions.RollbackAsync(txB);
        await db.Transactions.CommitAsync(txA);
    }
}
