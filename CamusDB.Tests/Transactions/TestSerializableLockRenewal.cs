
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
/// Serializable+RW range-lock finalize/release behavior: commit and rollback release a
/// transaction's range locks so a subsequent transaction sees the committed value or is no longer
/// blocked, and Read Committed transactions take no serializable range lock.
///
/// <para>Range-lock <b>lease renewal</b> for long-running transactions is no longer a CamusDB
/// concern: range-lock acquisitions are registered with the Kahuna transaction coordinator, which
/// renews them on its collection-interval tick for the life of the session and releases them on
/// finalize. That renewal (which operates on a ~60 s cadence, far too coarse to exercise with the
/// sub-second TTLs used here) is verified at the Kahuna level, not in this fixture.</para>
///
/// [NonParallelizable] because config knobs are process-global.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableLockRenewal : SharedNodeBaseTest
{
    private int savedMaxLifetimeMs;

    [SetUp]
    public void DisableLifetimeCap()
    {
        savedMaxLifetimeMs = CamusDBConfig.MaxSerializableTransactionLifetimeMs;
        CamusDBConfig.MaxSerializableTransactionLifetimeMs = -1; // no cap during these tests
    }

    [TearDown]
    public void RestoreConfig()
    {
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
    // Commit finalizes and releases the range locks; a new transaction sees the
    // committed value.
    // -----------------------------------------------------------------------

    [Test]
    public async Task Commit_NewTxSeesCommittedValue()
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
    // Rollback releases the range lock — a foreign write succeeds afterward.
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rollback_ForeignWriteSucceedsAfterRollback()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await ReadBalanceAsync(dbname, executor, alice, id);

        // Roll back — releases the range lock.
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
    // Read Committed transactions take no serializable range lock and commit normally.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommittedTx_CommitsNormally()
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
