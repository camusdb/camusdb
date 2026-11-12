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
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Kahuna.Shared.KeyValue;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The anomalies Serializable must refuse when a transaction reads rows by key, with optimistic locking:
/// write skew, a lost update, a decision taken on a row that another transaction then changed, a decision
/// taken on a missing row that another transaction then inserted, and a phantom in a scanned table.
///
/// <para>Each test interleaves two transactions so that committing both would be the anomaly, and
/// requires that at most one commits. It runs with the single-key read locks on and off
/// (<see cref="CamusDBOptions.SerializableOptimisticPointLocks"/>): with them off the read set is
/// proved at commit instead.</para>
/// </summary>
[TestFixture(true)]
[TestFixture(false)]
[NonParallelizable]
internal sealed class TestSerializableOptimisticPointReads : BaseTest
{
    private readonly bool pointLocks;

    public TestSerializableOptimisticPointReads(bool pointLocks) => this.pointLocks = pointLocks;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with
        {
            DefaultIsolationLevel = CamusIsolationLevel.Serializable,
            DefaultTransactionLocking = KeyValueTransactionLocking.Optimistic,
            SerializableOptimisticPointLocks = pointLocks,
        };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<long> BalanceAsync(CommandExecutor executor, string db, KvTransaction tx, int id)
        => (await QueryAsync(executor, db, tx, $"SELECT balance FROM accounts WHERE id = {id}")).Single().Row["balance"].LongValue;

    private static Task<KvTransaction> BeginAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

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

    /// <summary>Runs <paramref name="steps"/> and the commit; false when a retryable refusal ended the transaction.</summary>
    private static async Task<bool> TryFinishAsync(DatabaseDescriptor database, KvTransaction tx, Func<Task> steps)
    {
        try
        {
            await steps();
            await database.Transactions.CommitAsync(tx);
            return true;
        }
        catch (CamusDBException e) when (e.Code is CamusDBErrorCodes.TransactionConflict or CamusDBErrorCodes.TransactionMustRetry)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            return false;
        }
    }

    [Test]
    public async Task WriteSkew_AtMostOneCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            Assert.AreEqual(KeyValueTransactionLocking.Optimistic, first.Locking);

            // Both see 200 in total and each withdraws 150 from a different row.
            Assert.AreEqual(200, await BalanceAsync(executor, db, first, 1) + await BalanceAsync(executor, db, first, 2));
            Assert.AreEqual(200, await BalanceAsync(executor, db, second, 1) + await BalanceAsync(executor, db, second, 2));

            Task<bool> firstDone = TryFinishAsync(database, first,
                () => ExecAsync(executor, db, first, "UPDATE accounts SET balance = -50 WHERE id = 1"));
            Task<bool> secondDone = TryFinishAsync(database, second,
                () => ExecAsync(executor, db, second, "UPDATE accounts SET balance = -50 WHERE id = 2"));

            bool[] done = await Task.WhenAll(firstDone, secondDone);
            Assert.IsFalse(done[0] && done[1], "both withdrawals committed: write skew");

            KvTransaction check = await BeginAsync(database);
            long total = await BalanceAsync(executor, db, check, 1) + await BalanceAsync(executor, db, check, 2);
            await database.Transactions.CommitAsync(check);
            Assert.GreaterOrEqual(total, 50, "the total went below what one withdrawal leaves");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task LostUpdate_TheStaleWriterIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            long firstRead = await BalanceAsync(executor, db, first, 1);

            bool secondDone = await TryFinishAsync(database, second, async () =>
            {
                long read = await BalanceAsync(executor, db, second, 1);
                await ExecAsync(executor, db, second, $"UPDATE accounts SET balance = {read + 10} WHERE id = 1");
            });

            bool firstDone = await TryFinishAsync(database, first,
                () => ExecAsync(executor, db, first, $"UPDATE accounts SET balance = {firstRead + 1} WHERE id = 1"));

            Assert.IsFalse(firstDone && secondDone, "both read 100 and both wrote: a lost update");

            KvTransaction check = await BeginAsync(database);
            long balance = await BalanceAsync(executor, db, check, 1);
            await database.Transactions.CommitAsync(check);
            Assert.AreEqual(secondDone ? 110 : 101, balance);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task ARowReadAndNotWritten_ThatAnotherTransactionChanged_RefusesTheReader()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            // The first decides on row 1 and writes row 2; the second decides on row 2 and writes row 1.
            long firstSaw = await BalanceAsync(executor, db, first, 1);
            long secondSaw = await BalanceAsync(executor, db, second, 2);

            bool secondDone = await TryFinishAsync(database, second,
                () => ExecAsync(executor, db, second, $"UPDATE accounts SET balance = {secondSaw + 1} WHERE id = 1"));
            bool firstDone = await TryFinishAsync(database, first,
                () => ExecAsync(executor, db, first, $"UPDATE accounts SET balance = {firstSaw + 1} WHERE id = 2"));

            Assert.IsFalse(firstDone && secondDone, "each wrote the row the other decided on, and both committed");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task AMissingRowReadByKey_ThatAnotherTransactionInserted_RefusesTheReader()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            // Each finds the other's row missing and inserts its own: with both committed, each
            // acted on an absence that no serial order allows for both.
            Assert.AreEqual(0, (await QueryAsync(executor, db, first, "SELECT balance FROM accounts WHERE id = 4")).Count);
            Assert.AreEqual(0, (await QueryAsync(executor, db, second, "SELECT balance FROM accounts WHERE id = 3")).Count);

            bool secondDone = await TryFinishAsync(database, second,
                () => ExecAsync(executor, db, second, "INSERT INTO accounts (id, balance) VALUES (4, 1)"));
            bool firstDone = await TryFinishAsync(database, first,
                () => ExecAsync(executor, db, first, "INSERT INTO accounts (id, balance) VALUES (3, 1)"));

            Assert.IsFalse(firstDone && secondDone, "each inserted the row the other found missing, and both committed");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task AScannedTable_ThatAnotherTransactionInsertedInto_RefusesOneOfThem()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            // Both count the rows and each inserts one more: a phantom for the other.
            Assert.AreEqual(2, (await QueryAsync(executor, db, first, "SELECT id FROM accounts")).Count);
            Assert.AreEqual(2, (await QueryAsync(executor, db, second, "SELECT id FROM accounts")).Count);

            Task<bool> firstDone = TryFinishAsync(database, first,
                () => ExecAsync(executor, db, first, "INSERT INTO accounts (id, balance) VALUES (3, 2)"));
            Task<bool> secondDone = TryFinishAsync(database, second,
                () => ExecAsync(executor, db, second, "INSERT INTO accounts (id, balance) VALUES (4, 2)"));

            bool[] done = await Task.WhenAll(firstDone, secondDone);
            Assert.IsFalse(done[0] && done[1], "both inserted into the table the other scanned, and both committed");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }
}
