
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

using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies Task 3 — explicit BEGIN under Serializable default.
///
/// When an engine's <see cref="CamusDBOptions.DefaultIsolationLevel"/> is Serializable, an explicit
/// <c>BEGIN</c> (no level/mode) must open a Serializable+ReadWrite transaction that enforces
/// S2PL. An explicit RC override must still work.
///
/// The Read Committed case builds its own engine rather than changing what the others see.
/// </summary>
[TestFixture]
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestSerializableBeginDefault : SharedNodeBaseTest
{
    /// <summary>
    /// These tests are about what an explicit BEGIN inherits, so every engine here defaults to
    /// Serializable unless a test asks for something else.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultIsolationLevel = CamusIsolationLevel.Serializable };

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
    // 1. Explicit BEGIN (null, null) under Serializable default →
    //    Serializable + ReadWrite transaction.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitBegin_SerializableDefault_NoOptions_IsSerializableReadWrite()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        // Simulates: BEGIN (no options) — HttpTransactionCoordinator.StartAsync(db, null, null)
        KvTransaction tx = await db.Transactions.BeginAsync(isolationLevel: null, transactionMode: null);

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel,
            "BEGIN with no options must inherit DefaultIsolationLevel = Serializable");
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode,
            "BEGIN with no options must default to ReadWrite");
        Assert.AreNotEqual(HLCTimestamp.Zero, tx.TransactionId,
            "Serializable+RW must be a live Kahuna transaction");

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. Explicit BEGIN READ ONLY under Serializable default →
    //    Serializable + ReadOnly (full tx, with ReadTimestamp pinned).
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitBegin_SerializableDefault_ReadOnly_IsSerializableReadOnly()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            isolationLevel: null, transactionMode: CamusTransactionMode.ReadOnly);

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode);
        // Serializable+ReadOnly mints a real snapshot timestamp (Task 13).
        Assert.AreNotEqual(HLCTimestamp.Zero, tx.ReadTimestamp,
            "Serializable+ReadOnly must carry a pinned read snapshot timestamp");

        await db.Transactions.CommitAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 3. Explicit RC override under Serializable default →
    //    ReadCommitted + ReadWrite (the override wins).
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitBegin_SerializableDefault_RcOverride_IsReadCommitted()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, transactionMode: null);

        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel,
            "Explicit RC override must win over the Serializable default");
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode);

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4. S2PL enforcement: Serializable+RW explicit tx acquires shared lock on read,
    //    blocking a concurrent write until the tx commits.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitBegin_SerializableDefault_ReadAcquiresSharedLock_BlocksWrite()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        // Alice: Serializable+RW explicit tx, reads the row (acquires S lock).
        KvTransaction alice = await db.Transactions.BeginAsync(null, null);
        Assert.AreEqual(CamusIsolationLevel.Serializable, alice.IsolationLevel);
        long _ = await ReadBalanceAsync(dbname, executor, alice, id);

        // Bob: concurrent Serializable+RW tx, tries to write the same row.
        // Should be blocked / conflict with Alice's S lock.
        CamusDBException? conflict = null;
        KvTransaction? bob = null;
        try
        {
            bob = await db.Transactions.BeginAsync(null, null);
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

        Assert.IsNotNull(conflict, "Bob's write must be blocked while Alice holds the S lock");
        Assert.IsTrue(
            conflict!.Code == CamusDBErrorCodes.TransactionConflict ||
            conflict.Code  == CamusDBErrorCodes.TransactionMustRetry,
            $"Expected serialization conflict code, got {conflict.Code}");
    }

    // -----------------------------------------------------------------------
    // 5. Read Committed default: BEGIN (no options) → RC+RW, no S lock acquired.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitBegin_ReadCommittedDefault_NoOptions_IsReadCommittedReadWrite()
    {
        (string _, DatabaseDescriptor db, CommandExecutor __) = await SetupTableAsync(
            Options with { DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted });

        KvTransaction tx = await db.Transactions.BeginAsync(null, null);

        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel,
            "BEGIN with no options must inherit the engine's default of ReadCommitted");
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode);

        await db.Transactions.RollbackAsync(tx);
    }
}
