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

    /// <summary>
    /// Another transaction moves 50 from row 1 to row 2 and commits between a transaction's read of row 1
    /// and its read of row 2. The reader must not be handed 100 and 150, with or without a write of its own.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task ATransferCommittedBetweenTwoReads_WhatTheReaderSeesAndWhetherItCommits(bool readerWrites)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed, "INSERT INTO accounts (id, balance) VALUES (3, 0)");
        await database.Transactions.CommitAsync(seed);

        KvTransaction reader = await BeginAsync(database);
        KvTransaction mover = await BeginAsync(database);

        try
        {
            long first = await BalanceAsync(executor, db, reader, 1);

            bool moverDone = await TryFinishAsync(database, mover, async () =>
            {
                await ExecAsync(executor, db, mover, "UPDATE accounts SET balance = 50 WHERE id = 1");
                await ExecAsync(executor, db, mover, "UPDATE accounts SET balance = 150 WHERE id = 2");
            });

            long second = -1;
            string readOutcome = "ok";
            bool readerDone = await TryFinishAsync(database, reader, async () =>
            {
                try
                {
                    second = await BalanceAsync(executor, db, reader, 2);
                }
                catch (CamusDBException e)
                {
                    readOutcome = e.Code;
                    throw;
                }

                if (readerWrites)
                    await ExecAsync(executor, db, reader, $"UPDATE accounts SET balance = {first + second} WHERE id = 3");
            });

            TestContext.Out.WriteLine(
                $"pointLocks={pointLocks} readerWrites={readerWrites}: moverCommitted={moverDone} first={first} second={second} " +
                $"secondRead={readOutcome} readerCommitted={readerDone}");

            if (moverDone && readerDone)
                Assert.AreEqual(200, first + second, "the reader committed on two rows that never held these values together");

            // The reader is never handed two values that were not committed together: with the locks the
            // mover is refused, without them the reader's second read is.
            Assert.IsTrue(second == -1 || first + second == 200, $"the reader was handed {first} and {second}");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(reader);
            await database.Transactions.RollbackIfNotCompletedAsync(mover);
        }
    }

    /// <summary>Runs <paramref name="first"/> to its commit, then <paramref name="second"/>, and requires that not both commit.</summary>
    private static async Task<(bool First, bool Second)> AtMostOneCommitsAsync(
        DatabaseDescriptor database, KvTransaction first, Func<Task> firstSteps, KvTransaction second, Func<Task> secondSteps, string anomaly)
    {
        bool firstDone = await TryFinishAsync(database, first, firstSteps);
        bool secondDone = await TryFinishAsync(database, second, secondSteps);

        Assert.IsFalse(firstDone && secondDone, anomaly);
        return (firstDone, secondDone);
    }

    private static string LargeText(int seed)
    {
        Random random = new(seed);
        char[] chars = new char[6000];
        for (int i = 0; i < chars.Length; i++)
            chars[i] = (char)('a' + random.Next(26));
        return new string(chars);
    }

    [Test]
    public async Task ARowReadThroughAJoin_ThenWrittenByBoth_AtMostOneCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE refs (k INT64 PRIMARY KEY, target INT64 NOT NULL)", null));

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed, "INSERT INTO refs (k, target) VALUES (1, 1), (2, 2)");
        await database.Transactions.CommitAsync(seed);

        const string read = "SELECT a.balance FROM refs j JOIN accounts a ON a.id = j.target WHERE j.k = 1";

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            long firstRead = (await QueryAsync(executor, db, first, read)).Single().Row["balance"].LongValue;
            long secondRead = (await QueryAsync(executor, db, second, read)).Single().Row["balance"].LongValue;
            Assert.AreEqual(100, firstRead);
            Assert.AreEqual(100, secondRead);

            (bool firstDone, bool secondDone) = await AtMostOneCommitsAsync(database,
                first, () => ExecAsync(executor, db, first, $"UPDATE accounts SET balance = {firstRead + 10} WHERE id = 1"),
                second, () => ExecAsync(executor, db, second, $"UPDATE accounts SET balance = {secondRead + 1} WHERE id = 1"),
                "both read 100 through the join and both wrote: a lost update");

            KvTransaction check = await BeginAsync(database);
            long balance = await BalanceAsync(executor, db, check, 1);
            await database.Transactions.CommitAsync(check);
            Assert.AreEqual(firstDone ? 110 : secondDone ? 101 : 100, balance);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task RowsReadByAnInList_EachWritesADifferentOne_AtMostOneCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string read = "SELECT balance FROM accounts WHERE id IN (1, 2)";

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            Assert.AreEqual(200, (await QueryAsync(executor, db, first, read)).Sum(r => r.Row["balance"].LongValue));
            Assert.AreEqual(200, (await QueryAsync(executor, db, second, read)).Sum(r => r.Row["balance"].LongValue));

            await AtMostOneCommitsAsync(database,
                first, () => ExecAsync(executor, db, first, "UPDATE accounts SET balance = -50 WHERE id = 1"),
                second, () => ExecAsync(executor, db, second, "UPDATE accounts SET balance = -50 WHERE id = 2"),
                "both withdrawals committed after an IN-list read of both rows: write skew");

            KvTransaction check = await BeginAsync(database);
            long total = (await QueryAsync(executor, db, check, read)).Sum(r => r.Row["balance"].LongValue);
            await database.Transactions.CommitAsync(check);
            Assert.GreaterOrEqual(total, 50);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task RowsAnInvertedIndexReadExaminedAndDropped_EachWritesOne_AtMostOneCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE doctors (id int64 PRIMARY KEY, oncall int64, doc jsonb, INVERTED INDEX gin_doc (doc))", null));

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed,
            "INSERT INTO doctors (id, oncall, doc) VALUES (1, 1, '{\"k\":1}'), (2, 0, '{\"k\":2}'), (3, 0, '{\"k\":3}')");
        await database.Transactions.CommitAsync(seed);

        // The index returns all three rows; the filter on oncall drops rows 2 and 3 after they are read.
        const string read = "SELECT id FROM doctors WHERE doc ? 'k' AND oncall = 1";

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            Assert.AreEqual(1, (await QueryAsync(executor, db, first, read)).Count);
            Assert.AreEqual(1, (await QueryAsync(executor, db, second, read)).Count);

            // Each saw one doctor on call and puts a second one on call: a different row each, both of them
            // rows the other's read examined and dropped. With both committed there are three on call, which
            // neither order of the two allows.
            await AtMostOneCommitsAsync(database,
                first, () => ExecAsync(executor, db, first, "UPDATE doctors SET oncall = 1 WHERE id = 2"),
                second, () => ExecAsync(executor, db, second, "UPDATE doctors SET oncall = 1 WHERE id = 3"),
                "each changed a row the other's read examined and dropped, and both committed");

            KvTransaction check = await BeginAsync(database);
            int onCall = (await QueryAsync(executor, db, check, read)).Count;
            await database.Transactions.CommitAsync(check);
            Assert.LessOrEqual(onCall, 2);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }

    [Test]
    public async Task ALargeValueReadByBoth_ThenReplacedByBoth_AtMostOneCommits()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE docs (id INT64 PRIMARY KEY, body STRING)", null));

        string original = LargeText(1), fromFirst = LargeText(2), fromSecond = LargeText(3);

        KvTransaction seed = await BeginAsync(database);
        await ExecAsync(executor, db, seed, $"INSERT INTO docs (id, body) VALUES (1, '{original}')");
        await database.Transactions.CommitAsync(seed);

        const string read = "SELECT body FROM docs WHERE id = 1";

        KvTransaction first = await BeginAsync(database);
        KvTransaction second = await BeginAsync(database);

        try
        {
            Assert.AreEqual(original, (await QueryAsync(executor, db, first, read)).Single().Row["body"].StrValue);
            Assert.AreEqual(original, (await QueryAsync(executor, db, second, read)).Single().Row["body"].StrValue);

            (bool firstDone, bool secondDone) = await AtMostOneCommitsAsync(database,
                first, () => ExecAsync(executor, db, first, $"UPDATE docs SET body = '{fromFirst}' WHERE id = 1"),
                second, () => ExecAsync(executor, db, second, $"UPDATE docs SET body = '{fromSecond}' WHERE id = 1"),
                "both read the value and both replaced it: a lost update");

            KvTransaction check = await BeginAsync(database);
            string stored = (await QueryAsync(executor, db, check, read)).Single().Row["body"].StrValue!;
            await database.Transactions.CommitAsync(check);
            Assert.AreEqual(firstDone ? fromFirst : secondDone ? fromSecond : original, stored);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }
    }
}
