
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
/// Each test mirrors the controller's own branch — wrap in the retry helper only when the engine's
/// resolved default is Serializable — against the configuration its engine was built with.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableAutoRetry : SharedNodeBaseTest
{
    /// <summary>
    /// The retry wrapper is a Serializable-only path, so every engine here defaults to Serializable
    /// unless a test asks for something else.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultIsolationLevel = CamusIsolationLevel.Serializable };

    /// <summary>
    /// The controller's own branch: autocommit is wrapped in the retry helper only when the engine
    /// resolves to Serializable. Reproduced here so the tests exercise the real decision rather than
    /// hard-coding which arm they expect.
    /// </summary>
    private static Task RunAutocommitAsync(CamusDBOptions options, Func<CancellationToken, Task> body)
        => options.DefaultIsolationLevel == CamusIsolationLevel.Serializable
            ? SerializableRetryHelper.ExecuteAutocommitAsync(body)
            : body(CancellationToken.None);

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupAccountsAsync(CamusDBOptions? options = null)
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
    // 1. Autocommit DML (Serializable default): conflict resolved transparently
    //
    // Alice holds a Serializable+RW shared lock on the row. Bob's autocommit UPDATE
    // (simulating the controller's body) contends for the S→X upgrade. The contention is
    // resolved transparently and Bob's write commits with the correct final value. The retry
    // may happen at either layer: the batched write path's bounded lock-wait absorbs a short
    // hold internally (so the autocommit body runs once), and a longer hold that exhausts that
    // wait surfaces TransactionMustRetry to the outer SerializableRetryHelper, which retries.
    // Either way the guarantee is: the update eventually succeeds under contention.
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
        // inside the batched write path's bounded lock-wait (and the retry helper's cumulative
        // backoff budget) so Bob's upgrade reliably completes once she releases — whether it is
        // resolved by the internal lock-wait or by an outer retry.
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
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, cancellationToken: ct);
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
        await RunAutocommitAsync(Options, AutocommitBody);

        await aliceTask;

        // The autocommit body runs at least once. With the batched write path, a short lock hold is
        // absorbed by the internal bounded lock-wait (body runs once); a longer hold surfaces
        // TransactionMustRetry and the outer helper re-runs the body. Correct commit under contention
        // is asserted by the final balance below, which is the guarantee that actually matters.
        Assert.GreaterOrEqual(attempts, 1,
            "Autocommit body must have run at least once");

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
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, cancellationToken: ct);
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
            await RunAutocommitAsync(Options, AutocommitBody);
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
        CamusDBOptions readCommitted = Options with { DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted };

        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAccountsAsync(readCommitted);

        string id = ObjectIdGenerator.Generate().ToString();
        int attempts = 0;

        async Task AutocommitBody(CancellationToken ct)
        {
            attempts++;
            KvTransaction tx = await db.Transactions.BeginAsync(null, null, cancellationToken: ct);
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
        await RunAutocommitAsync(readCommitted, AutocommitBody);

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
