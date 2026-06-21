
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Diagnostics;
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
/// Verifies the timestamp-ordered deadlock-avoidance policy (wait-die) applied when a
/// Serializable+RW transaction is denied a range lock held by another transaction.
///
/// The requesting transaction compares its own start timestamp against the holder's (returned
/// alongside the denial): an OLDER requester waits for the younger holder to release; a YOUNGER
/// requester aborts immediately. Waits therefore only ever point older→younger, so two transactions
/// contending in reverse order can never both abort — the older one wins deterministically.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableWaitDie : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupDbAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "tasks",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("priority", ColumnType.Integer64),
                new("name",     ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey,  "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "priority_idx",
                    new ColumnIndexInfo[] { new("priority", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await db.Transactions.CommitAsync(setup);

        return (dbname, db, executor);
    }

    private async Task InsertAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        string id, long priority, string name)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx, databaseName: dbname, tableName: "tasks",
            values: new() { new() {
                { "id",       new(ColumnType.Id,        id)       },
                { "priority", new(ColumnType.Integer64, priority) },
                { "name",     new(ColumnType.String,    name)     },
            }}));
        await db.Transactions.CommitAsync(tx);
    }

    private static Task UpdatePriorityAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        KvTransaction tx, long priority, string name)
        => executor.Update(new UpdateTicket(
            txnState: tx, databaseName: dbname, tableName: "tasks",
            plainValues: new() { { "name", new(ColumnType.String, name) } },
            exprValues: null, where: null,
            filters: new() { new("priority", "=", new(ColumnType.Integer64, priority)) },
            parameters: null));

    // -----------------------------------------------------------------------
    // 1. Older requester waits for a younger holder and then wins.
    //
    // txYoung (begun second) holds the exclusive lock first. txOld (begun first)
    // arrives second and is denied; because it is older it WAITS rather than dying.
    // Once txYoung commits, txOld acquires the lock and commits successfully.
    // -----------------------------------------------------------------------

    [Test]
    public async Task OlderRequester_WaitsForYoungerHolder_AndWins()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "original");

        // txOld is begun first → older start timestamp.
        KvTransaction txOld = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txYoung = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // The younger transaction grabs the exclusive lock first.
        await UpdatePriorityAsync(dbname, db, executor, txYoung, 1, "young");

        // Release the younger holder shortly, well within the older requester's lock-wait budget.
        Task release = Task.Run(async () =>
        {
            await Task.Delay(100);
            await db.Transactions.CommitAsync(txYoung);
        });

        // The older transaction is denied, waits, and succeeds after the holder releases.
        Assert.DoesNotThrowAsync(async () =>
        {
            await UpdatePriorityAsync(dbname, db, executor, txOld, 1, "old");
            await db.Transactions.CommitAsync(txOld);
        }, "Older requester must wait for the younger holder and then commit");

        await release;
    }

    // -----------------------------------------------------------------------
    // 2. Younger requester dies immediately (does not wait out the deadline).
    //
    // txOld holds the lock; txYoung arrives second and, being younger, aborts at
    // once with a conflict instead of waiting up to the lock-wait deadline.
    // -----------------------------------------------------------------------

    [Test]
    public async Task YoungerRequester_DiesImmediately()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "original");

        KvTransaction txOld = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction txYoung = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // The older transaction holds the exclusive lock.
        await UpdatePriorityAsync(dbname, db, executor, txOld, 1, "old");

        // The younger transaction is denied and must die quickly, not wait out the deadline.
        Stopwatch sw = Stopwatch.StartNew();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => UpdatePriorityAsync(dbname, db, executor, txYoung, 1, "young"));
        sw.Stop();

        Assert.IsTrue(
            ex!.Code == CamusDBErrorCodes.TransactionConflict ||
            ex.Code == CamusDBErrorCodes.TransactionMustRetry,
            $"Younger requester must abort with a retryable conflict; got {ex.Code}");
        Assert.Less(sw.ElapsedMilliseconds, 400,
            "Younger requester must die immediately, not wait out the lock-wait deadline");

        await db.Transactions.RollbackAsync(txYoung);
        await db.Transactions.CommitAsync(txOld);
    }

    // -----------------------------------------------------------------------
    // 3. Deterministic winner on repeated reverse-order contention.
    //
    // Whichever transaction began first (older) commits; the younger always aborts.
    // No run aborts both parties.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReverseOrderContention_OlderAlwaysWins()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "original");

        for (int i = 0; i < 3; i++)
        {
            KvTransaction txOld = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            KvTransaction txYoung = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

            // The older transaction acquires first and holds.
            await UpdatePriorityAsync(dbname, db, executor, txOld, 1, $"old-{i}");

            // The younger transaction contends and must lose.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                () => UpdatePriorityAsync(dbname, db, executor, txYoung, 1, $"young-{i}"));
            Assert.IsNotNull(ex, "Younger transaction must abort");
            await db.Transactions.RollbackAsync(txYoung);

            // The older transaction commits — a guaranteed winner each round.
            Assert.DoesNotThrowAsync(() => db.Transactions.CommitAsync(txOld),
                "Older transaction must commit");
        }
    }
}
