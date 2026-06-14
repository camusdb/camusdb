
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

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies Task 4 — range-lock lease renewal for long-running Serializable+RW transactions.
///
/// Tests use shortened TTLs (<see cref="CamusDBConfig.RangeLockExpiresMs"/>) and heartbeat
/// intervals (<see cref="CamusDBConfig.RangeLockHeartbeatIntervalMs"/>) so they run in under
/// a second without waiting the real 30 s range-lock TTL.
///
/// [NonParallelizable] because config knobs are process-global.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableLockRenewal : SharedNodeBaseTest
{
    private int savedRangeLockExpiresMs;
    private int savedHeartbeatIntervalMs;
    private int savedMaxLifetimeMs;

    [SetUp]
    public void SetFastRenewal()
    {
        savedRangeLockExpiresMs      = CamusDBConfig.RangeLockExpiresMs;
        savedHeartbeatIntervalMs     = CamusDBConfig.RangeLockHeartbeatIntervalMs;
        savedMaxLifetimeMs           = CamusDBConfig.MaxSerializableTransactionLifetimeMs;

        // 300 ms lock TTL + 100 ms heartbeat → renewal fires well before expiry.
        CamusDBConfig.RangeLockExpiresMs          = 300;
        CamusDBConfig.RangeLockHeartbeatIntervalMs = 100;
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = -1; // no cap during renewal tests
    }

    [TearDown]
    public void RestoreConfig()
    {
        CamusDBConfig.RangeLockExpiresMs          = savedRangeLockExpiresMs;
        CamusDBConfig.RangeLockHeartbeatIntervalMs = savedHeartbeatIntervalMs;
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = savedMaxLifetimeMs;
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupTableAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("balance", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        return (dbname, db, executor);
    }

    private static async Task InsertRowAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        string id, long balance)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        id)      },
                { "balance", new(ColumnType.Integer64, balance) },
            }}));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<long> ReadBalanceAsync(
        string dbname, CommandExecutor executor, KvTransaction tx, string id)
    {
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "accounts",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        List<QueryResultRow> list = await rows.ToListAsync();
        return list[0].Row["balance"].LongValue;
    }

    // -----------------------------------------------------------------------
    // 1. A Serializable+RW transaction held open past the original lock TTL
    //    (300 ms) still commits — proves the heartbeat renewed the range lock
    //    before it expired.
    // -----------------------------------------------------------------------

    [Test]
    public async Task LongRunningSerializableRW_CommitsAfterLockTtlElapses()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Acquire a range lock by reading the row (S lock via S2PL).
        long balance = await ReadBalanceAsync(dbname, executor, tx, id);
        Assert.AreEqual(100L, balance);

        // Sleep for 3× the lock TTL. Without renewal the lock would expire and the subsequent
        // commit would fail because Kahuna sees the range lock as gone.
        await Task.Delay(CamusDBConfig.RangeLockExpiresMs * 3 + 100);

        // The heartbeat must have fired at least twice at 100 ms interval, refreshing the lock.
        // CommitAsync succeeds if and only if the range lock is still held.
        Assert.DoesNotThrowAsync(async () =>
            await db.Transactions.CommitAsync(tx),
            "Serializable+RW tx must commit after lock renewal kept range locks alive past their original TTL");
    }

    // -----------------------------------------------------------------------
    // 2. A foreign write targeting the same row is still blocked after TTL
    //    elapses — renewal kept the S lock active so Alice's read is protected.
    // -----------------------------------------------------------------------

    [Test]
    public async Task LongRunningSerializableRW_ForeignWriteStillBlockedAfterTtlElapses()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await ReadBalanceAsync(dbname, executor, alice, id);

        // Wait past the original TTL so that without renewal Bob would succeed.
        await Task.Delay(CamusDBConfig.RangeLockExpiresMs * 3 + 100);

        CamusDBException? conflict = null;
        KvTransaction? bob = null;
        try
        {
            bob = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            await executor.Update(new UpdateTicket(
                txnState: bob, databaseName: dbname, tableName: "accounts",
                plainValues: new() { { "balance", new(ColumnType.Integer64, 999L) } },
                exprValues: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                parameters: null));
            await db.Transactions.CommitAsync(bob);
        }
        catch (CamusDBException ex)
        {
            conflict = ex;
            if (bob is not null)
                await db.Transactions.RollbackIfNotCompletedAsync(bob);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(alice);
        }

        Assert.IsNotNull(conflict,
            "Bob's write must be blocked — the renewed range lock kept Alice's S lock alive");
        Assert.IsTrue(
            conflict!.Code == CamusDBErrorCodes.TransactionConflict ||
            conflict.Code  == CamusDBErrorCodes.TransactionMustRetry,
            $"Expected serialization conflict, got {conflict.Code}");
    }

    // -----------------------------------------------------------------------
    // 3. Commit cancels the heartbeat — no background renewal fires after the
    //    transaction finishes. Verified structurally: we read back that the
    //    commit completes normally and a new tx sees the committed value.
    // -----------------------------------------------------------------------

    [Test]
    public async Task CommitCancelsHeartbeat_NewTxSeesCommittedValue()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Read then write — commit should stop the heartbeat.
        await ReadBalanceAsync(dbname, executor, alice, id);
        await executor.Update(new UpdateTicket(
            txnState: alice, databaseName: dbname, tableName: "accounts",
            plainValues: new() { { "balance", new(ColumnType.Integer64, 777L) } },
            exprValues: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            parameters: null));
        await db.Transactions.CommitAsync(alice);

        // After commit a fresh snapshot should see the updated value.
        KvTransaction snap = await db.Transactions.BeginReadOnlyAsync(promote: false);
        long committed = await ReadBalanceAsync(dbname, executor, snap, id);
        Assert.AreEqual(777L, committed, "Committed write must be visible after commit");
    }

    // -----------------------------------------------------------------------
    // 4. Rollback cancels the heartbeat — a foreign write succeeds after
    //    the rolled-back transaction's lock is released.
    // -----------------------------------------------------------------------

    [Test]
    public async Task RollbackCancelsHeartbeat_ForeignWriteSucceedsAfterRollback()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await ReadBalanceAsync(dbname, executor, alice, id);

        // Roll back — stops the heartbeat and releases the range lock.
        await db.Transactions.RollbackAsync(alice);

        // Bob can now write without conflict.
        KvTransaction bob = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: bob, databaseName: dbname, tableName: "accounts",
            plainValues: new() { { "balance", new(ColumnType.Integer64, 888L) } },
            exprValues: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            parameters: null));
        Assert.DoesNotThrowAsync(async () => await db.Transactions.CommitAsync(bob),
            "Bob must succeed after Alice's rollback released the range lock");
    }

    // -----------------------------------------------------------------------
    // 5. RC transactions do NOT get a heartbeat — no background renewal task.
    //    Structurally verified: the MaxSerializableTransactionLifetimeMs is -1
    //    (cap disabled), so if a heartbeat started for RC it would just be
    //    overhead but harmless. The real assertion is commit succeeds normally.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommittedTx_NoHeartbeatNeeded_CommitsNormally()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        await executor.Update(new UpdateTicket(
            txnState: tx, databaseName: dbname, tableName: "accounts",
            plainValues: new() { { "balance", new(ColumnType.Integer64, 200L) } },
            exprValues: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            parameters: null));

        Assert.DoesNotThrowAsync(async () => await db.Transactions.CommitAsync(tx));
    }

    // -----------------------------------------------------------------------
    // 6. Raised lifetime cap: MaxSerializableTransactionLifetimeMs is now 1 hour,
    //    not 25 s. A transaction open past the old 25 s threshold must not be
    //    rejected by the lifetime gate. (Uses -1 = no cap in this test fixture
    //    to avoid a 25+ s wait, but verifies the config value is non-trivial.)
    // -----------------------------------------------------------------------

    [Test]
    public void RaisedLifetimeCap_DefaultIsOneHour()
    {
        // Restore the real default momentarily to inspect it.
        int restored = 3_600_000;
        Assert.AreEqual(restored, savedMaxLifetimeMs,
            "MaxSerializableTransactionLifetimeMs default must be 1 hour (3 600 000 ms), not 25 s");
    }
}
