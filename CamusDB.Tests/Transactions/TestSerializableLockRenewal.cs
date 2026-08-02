
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
/// </summary>
[TestFixture]
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestSerializableLockRenewal : SharedNodeBaseTest
{
    /// <summary>
    /// These tests hold locks deliberately long to observe renewal, so the lifetime cap that would
    /// otherwise abort them is disabled for every engine this fixture builds.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { MaxSerializableTransactionLifetimeMs = -1 };

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupTableAsync(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options ?? Options);

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
    // 6. The lifetime cap defaults to an hour, not 25 s, so a long-running transaction is not
    //    rejected by the lifetime gate. The other tests here disable the cap entirely to avoid a
    //    25+ s wait, so this one asserts the shipped default directly rather than through an engine.
    // -----------------------------------------------------------------------

    [Test]
    public void RaisedLifetimeCap_DefaultIsOneHour()
    {
        Assert.AreEqual(3_600_000, CamusDBOptions.Default.MaxSerializableTransactionLifetimeMs,
            "MaxSerializableTransactionLifetimeMs default must be 1 hour (3 600 000 ms), not 25 s");
    }

}
