
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
/// Verifies the server-side autocommit auto-retry wiring.
///
/// The HTTP controllers (InsertController, UpdateController, etc.) wrap autocommit DML and
/// SELECT in <see cref="SerializableRetryHelper.ExecuteAutocommitAsync"/> when the resolved
/// isolation level is Serializable. These tests simulate that pattern end-to-end at the
/// executor level using the same begin→execute→commit body the controllers use.
///
/// Tests run non-parallel because they modify the static
/// <see cref="CamusDBConfig.DefaultIsolationLevel"/> knob.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableAutoRetry : SharedNodeBaseTest
{
    private CamusIsolationLevel savedDefaultLevel;

    [SetUp]
    public void SetSerializableDefault()
    {
        savedDefaultLevel = CamusDBConfig.DefaultIsolationLevel;
        CamusDBConfig.DefaultIsolationLevel = CamusIsolationLevel.Serializable;
    }

    [TearDown]
    public void RestoreDefaultLevel()
    {
        CamusDBConfig.DefaultIsolationLevel = savedDefaultLevel;
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupAccountsAsync()
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
        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        id)      },
                { "balance", new(ColumnType.Integer64, balance) },
            }}));
        await db.Transactions.CommitAsync(setup);
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
    // 1. Autocommit DML (Serializable default): conflict retried transparently
    //
    // Alice holds a Serializable+RW shared lock on the row. Bob's autocommit
    // UPDATE (simulating the controller's body) conflicts on the first attempt.
    // After Alice commits, Bob's retry acquires the exclusive lock and succeeds.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AutocommitUpdate_SerializableDefault_RetriesOnConflictAndSucceeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAccountsAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        // Alice reads the row under Serializable+RW, acquiring a shared point lock.
        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        long _ = await ReadBalanceAsync(dbname, executor, alice, id);

        // Alice commits after a short hold, releasing the shared lock. The hold is kept well
        // inside the retry helper's cumulative backoff budget (~225-375 ms over 5 attempts) so a
        // later retry reliably acquires the lock; the first attempt still conflicts while she holds.
        Task aliceTask = Task.Run(async () =>
        {
            await Task.Delay(100);
            await db.Transactions.CommitAsync(alice);
        });

        // Simulate the controller's autocommit body.
        int attempts = 0;
        async Task AutocommitBody(CancellationToken ct)
        {
            attempts++;
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, ct);
            try
            {
                UpdateTicket ticket = new(
                    txnState: tx, databaseName: dbname, tableName: "accounts",
                    plainValues: new() { { "balance", new(ColumnType.Integer64, 50L) } },
                    exprValues: null, where: null,
                    filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                    parameters: null);
                await executor.Update(ticket);
                await db.Transactions.CommitAsync(tx, ct);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(tx, ct);
                throw;
            }
        }

        // Controller path: Serializable default → retry helper engaged.
        Assert.AreEqual(CamusIsolationLevel.Serializable, CamusDBConfig.DefaultIsolationLevel);
        if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
            await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody);
        else
            await AutocommitBody(CancellationToken.None);

        await aliceTask;

        Assert.GreaterOrEqual(attempts, 2,
            "First attempt must have conflicted; retry was required");

        KvTransaction reader = await db.Transactions.BeginAsync();
        long finalBalance = await ReadBalanceAsync(dbname, executor, reader, id);
        await db.Transactions.RollbackAsync(reader);

        Assert.AreEqual(50L, finalBalance, "Bob's autocommit UPDATE must have committed after retry");
    }

    // -----------------------------------------------------------------------
    // 2. Non-retryable error (PK violation) surfaces after one attempt
    // -----------------------------------------------------------------------

    [Test]
    public async Task AutocommitInsert_SerializableDefault_NonRetryableErrorSurfacesImmediately()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAccountsAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        int attempts = 0;
        CamusDBException? caught = null;

        async Task AutocommitBody(CancellationToken ct)
        {
            attempts++;
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, ct);
            try
            {
                // Insert the same id again → DuplicateUniqueKeyValue (not retryable).
                await executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: dbname, tableName: "accounts",
                    values: new() { new() {
                        { "id",      new(ColumnType.Id,        id)   },
                        { "balance", new(ColumnType.Integer64, 999L) },
                    }}));
                await db.Transactions.CommitAsync(tx, ct);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(tx, ct);
                throw;
            }
        }

        try
        {
            if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody);
            else
                await AutocommitBody(CancellationToken.None);
        }
        catch (CamusDBException ex)
        {
            caught = ex;
        }

        Assert.AreEqual(1, attempts,
            "PK violation is not retryable — exactly 1 attempt expected");
        Assert.IsNotNull(caught);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, caught!.Code);
    }

    // -----------------------------------------------------------------------
    // 3. Read Committed autocommit path is unchanged — no retry wrapper engaged
    //
    // Override the default back to RC for this test only. Confirm normal operation.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AutocommitInsert_ReadCommittedDefault_RunsOnceNoRetryWrap()
    {
        CamusDBConfig.DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted;

        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAccountsAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        int attempts = 0;

        async Task AutocommitBody(CancellationToken ct)
        {
            attempts++;
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, ct);
            try
            {
                await executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: dbname, tableName: "accounts",
                    values: new() { new() {
                        { "id",      new(ColumnType.Id,        id)    },
                        { "balance", new(ColumnType.Integer64, 100L)  },
                    }}));
                await db.Transactions.CommitAsync(tx, ct);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(tx, ct);
                throw;
            }
        }

        // Controller path: RC default → body runs once directly, no retry wrapper.
        if (CamusDBConfig.DefaultIsolationLevel == CamusIsolationLevel.Serializable)
            await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody);
        else
            await AutocommitBody(CancellationToken.None);

        Assert.AreEqual(1, attempts, "RC autocommit runs exactly once without the retry wrapper");
    }

    // -----------------------------------------------------------------------
    // 4. Explicit multi-statement transaction surfaces serialization abort to caller
    //
    // The controller's explicit-tx path does NOT wrap in the retry helper. When
    // a Serializable+RW transaction conflicts, the exception propagates out.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ExplicitTransaction_SerializableDefault_ConflictSurfacesToCaller()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAccountsAsync();

        string id = ObjectIdGenerator.Generate().ToString();
        await InsertRowAsync(dbname, db, executor, id, 100L);

        // Alice holds a Serializable+RW shared lock.
        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        long _ = await ReadBalanceAsync(dbname, executor, alice, id);

        // Bob uses an explicit multi-statement transaction (the controller's explicit-tx path).
        // The controller does NOT retry here — the conflict surfaces to the caller.
        CamusDBException? caught = null;
        KvTransaction? bob = null;
        try
        {
            bob = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            UpdateTicket ticket = new(
                txnState: bob, databaseName: dbname, tableName: "accounts",
                plainValues: new() { { "balance", new(ColumnType.Integer64, 50L) } },
                exprValues: null, where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                parameters: null);
            await executor.Update(ticket);
            await db.Transactions.CommitAsync(bob);
        }
        catch (CamusDBException ex)
        {
            caught = ex;
            if (bob is not null)
                await db.Transactions.RollbackIfNotCompletedAsync(bob);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(alice);
        }

        Assert.IsNotNull(caught,
            "Explicit transaction must surface the serialization conflict — no server-side retry");
        Assert.IsTrue(
            caught!.Code == CamusDBErrorCodes.TransactionConflict ||
            caught.Code  == CamusDBErrorCodes.TransactionMustRetry,
            $"Expected a serialization conflict code, got {caught.Code}");
    }
}
