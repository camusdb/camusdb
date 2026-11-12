/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A read-modify-write of one row by two optimistic Read Committed transactions: the second reads the
/// row while the first has an uncommitted update on it.
///
/// <para>With <see cref="CamusDBOptions.OptimisticIntentWaitMs"/> above zero the second read waits for
/// the first commit and returns its value, and both transactions commit. With the wait off the read
/// returns the old value at once, and the second update is refused with a retryable conflict.</para>
/// </summary>
[TestFixture(1000)]
[TestFixture(0)]
[NonParallelizable]
internal sealed class TestOptimisticIntentWait : BaseTest
{
    private readonly int waitMs;

    public TestOptimisticIntentWait(int waitMs) => this.waitMs = waitMs;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with
        {
            DefaultIsolationLevel = CamusIsolationLevel.ReadCommitted,
            DefaultTransactionLocking = KeyValueTransactionLocking.Optimistic,
            OptimisticIntentWaitMs = waitMs,
        };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<long> BalanceAsync(CommandExecutor executor, string db, KvTransaction tx, int id)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, $"SELECT balance FROM accounts WHERE id = {id}", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows.Single().Row["balance"].LongValue;
    }

    private static Task<KvTransaction> BeginAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)", null));

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed, "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 100)");
        await database.Transactions.CommitAsync(seed);

        return (db, database, executor);
    }

    [Test]
    public async Task ReadBehindAnUncommittedUpdate_ThenUpdate()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            Assert.AreEqual(KeyValueTransactionLocking.Optimistic, first.Locking);

            long firstRead = await BalanceAsync(executor, db, first, 1);
            await ExecAsync(executor, db, first, $"UPDATE accounts SET balance = {firstRead + 50} WHERE id = 1");

            Task<long> secondRead = BalanceAsync(executor, db, second, 1);

            if (waitMs > 0)
            {
                // The read is held behind the uncommitted update.
                await Task.Delay(200);
                Assert.IsFalse(secondRead.IsCompleted, "the read must wait for the uncommitted update");

                await database.Transactions.CommitAsync(first);

                Assert.AreEqual(150, await secondRead, "the read returns the value the first transaction committed");

                await ExecAsync(executor, db, second, "UPDATE accounts SET balance = 151 WHERE id = 1");
                await database.Transactions.CommitAsync(second);

                KvTransaction check = await BeginAsync(database);
                Assert.AreEqual(151, await BalanceAsync(executor, db, check, 1));
                await database.Transactions.CommitAsync(check);
            }
            else
            {
                Assert.AreEqual(100, await secondRead, "without the wait the read returns the committed value");

                await database.Transactions.CommitAsync(first);

                CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(
                    async () => await ExecAsync(executor, db, second, "UPDATE accounts SET balance = 101 WHERE id = 1"));
                Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task TwoTransactionsThatReadEachOthersWrittenRow_AreAnsweredAtTheLimit()
    {
        if (waitMs == 0)
            Assert.Ignore("Only with the wait on.");

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            await ExecAsync(executor, db, first, "UPDATE accounts SET balance = 150 WHERE id = 1");
            await ExecAsync(executor, db, second, "UPDATE accounts SET balance = 250 WHERE id = 2");

            // Each reads the row the other wrote: a wait cycle, ended by the limit.
            System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
            Task<long> firstRead = BalanceAsync(executor, db, first, 2);
            Task<long> secondRead = BalanceAsync(executor, db, second, 1);

            Assert.AreEqual(100, await firstRead);
            Assert.AreEqual(100, await secondRead);
            Assert.GreaterOrEqual(watch.ElapsedMilliseconds, waitMs / 2, "the reads waited");
            Assert.Less(watch.ElapsedMilliseconds, waitMs * 5, "the limit ends the cycle");

            // Neither can now update the row it read below the other's write.
            CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(
                async () => await ExecAsync(executor, db, first, "UPDATE accounts SET balance = 101 WHERE id = 2"));
            Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);

            await database.Transactions.RollbackIfNotCompletedAsync(first);

            // With the first one gone, the second commits its own write.
            await database.Transactions.CommitAsync(second);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task UpdateOfARowReadBeforeTheOtherUpdate_IsRefusedAtOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            // Both read before either writes: no read can wait, and the second update cannot succeed
            // unless the first rolls back.
            Assert.AreEqual(100, await BalanceAsync(executor, db, first, 1));
            Assert.AreEqual(100, await BalanceAsync(executor, db, second, 1));

            await ExecAsync(executor, db, first, "UPDATE accounts SET balance = 150 WHERE id = 1");

            System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
            Task<ExecuteNonSQLResult> secondUpdate = ExecAsync(executor, db, second, "UPDATE accounts SET balance = 101 WHERE id = 1");

            if (waitMs > 0)
            {
                // Refused while the first transaction is still open, without waiting for its commit.
                CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await secondUpdate);
                Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);
                Assert.Less(watch.ElapsedMilliseconds, 400, "the doomed update must not poll until the lock-wait deadline");
                await database.Transactions.CommitAsync(first);
            }
            else
            {
                await Task.Delay(100);
                await database.Transactions.CommitAsync(first);
                CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await secondUpdate);
                Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }
}
