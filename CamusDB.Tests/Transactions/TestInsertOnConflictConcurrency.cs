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
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// Concurrency of <c>INSERT … ON CONFLICT</c>, in both locking modes and at both isolation levels.
/// The statement takes an exclusive lock on each arbiter key before it reads it, so the answer of the
/// read stays true until the statement writes. These tests check what that fence must give:
/// <list type="bullet">
///   <item>two transactions that upsert the same new key never both insert it;</item>
///   <item>a plain INSERT of a fenced key fails with a retryable error and never leaves a duplicate;</item>
///   <item>N workers that increment one counter through ON CONFLICT DO UPDATE lose no increment;</item>
///   <item>a row committed after the transaction began is found by the upsert, not inserted again.</item>
/// </list>
/// The two-transaction cases are sequenced on one thread, so the winner is known; the counter test
/// runs its workers in parallel.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInsertOnConflictConcurrency : SharedNodeBaseTest
{
    private const string Increment = "INSERT INTO c (k, n) VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET n = c.n + excluded.n";

    private static IEnumerable<TestCaseData> Modes()
    {
        foreach (CamusIsolationLevel isolation in new[] { CamusIsolationLevel.ReadCommitted, CamusIsolationLevel.Serializable })
        {
            foreach (KeyValueTransactionLocking locking in new[] { KeyValueTransactionLocking.Pessimistic, KeyValueTransactionLocking.Optimistic })
                yield return new TestCaseData(isolation, locking).SetArgDisplayNames(isolation.ToString(), locking.ToString());
        }
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> Setup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "CREATE TABLE c (k INT64 NOT NULL PRIMARY KEY, n INT64 NOT NULL)", null));
        await database.Transactions.CommitAsync(tx);

        return (dbname, database, executor);
    }

    private static Task Exec(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

    /// <summary>Every committed row of <c>c</c> as "k|n", ordered by key.</summary>
    private static async Task<List<string>> Rows(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "SELECT k, n FROM c ORDER BY k", null));

            List<string> rows = new();
            await foreach (QueryResultRow row in cursor)
                rows.Add($"{row.Row["k"].LongValue}|{row.Row["n"].LongValue}");

            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertRetryable(CamusDBException? ex, string because)
    {
        Assert.IsNotNull(ex, because);
        Assert.IsTrue(SerializableRetryHelper.IsRetryable(ex!), $"{because}: expected a retryable code, got {ex!.Code} ({ex.Message})");
    }

    [TestCaseSource(nameof(Modes))]
    public async Task TwoTransactionsUpsertTheSameNewKey(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        KvTransaction first = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
        KvTransaction second = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);

        try
        {
            await Exec(executor, dbname, first, Increment);

            // The first transaction holds the fence on k = 1, so the second one cannot insert it too.
            CamusDBException? refused = Assert.CatchAsync<CamusDBException>(async () => await Exec(executor, dbname, second, Increment));
            AssertRetryable(refused, "the second upsert of a fenced key");

            await database.Transactions.CommitAsync(first);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(first);
            await database.Transactions.RollbackIfNotCompletedAsync(second);
        }

        // The retry of the second statement finds the committed row and updates it.
        KvTransaction retry = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
        await Exec(executor, dbname, retry, Increment);
        await database.Transactions.CommitAsync(retry);

        CollectionAssert.AreEqual(new[] { "1|2" }, await Rows(executor, database, dbname));
    }

    [TestCaseSource(nameof(Modes))]
    public async Task PlainInsertOfAFencedKeyIsRefused(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        KvTransaction upsert = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
        KvTransaction insert = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);

        try
        {
            await Exec(executor, dbname, upsert, "INSERT INTO c (k, n) VALUES (5, 1) ON CONFLICT (k) DO NOTHING");

            CamusDBException? refused = Assert.CatchAsync<CamusDBException>(
                async () => await Exec(executor, dbname, insert, "INSERT INTO c (k, n) VALUES (5, 100)"));
            AssertRetryable(refused, "a plain INSERT of a key an upsert fenced");

            await database.Transactions.CommitAsync(upsert);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(upsert);
            await database.Transactions.RollbackIfNotCompletedAsync(insert);
        }

        CollectionAssert.AreEqual(new[] { "5|1" }, await Rows(executor, database, dbname));
    }

    [TestCaseSource(nameof(Modes))]
    public async Task UpsertAfterAnUncommittedPlainInsertNeverDuplicates(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        KvTransaction insert = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
        KvTransaction upsert = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);

        List<string> outcomes = new();

        try
        {
            await Exec(executor, dbname, insert, "INSERT INTO c (k, n) VALUES (6, 100)");

            try
            {
                await Exec(executor, dbname, upsert, "INSERT INTO c (k, n) VALUES (6, 1) ON CONFLICT (k) DO UPDATE SET n = c.n + excluded.n");
                outcomes.Add("upsert staged");
            }
            catch (CamusDBException ex)
            {
                AssertRetryable(ex, "an upsert over a key another transaction is inserting");
                outcomes.Add("upsert refused " + ex.Code);
            }

            try
            {
                await database.Transactions.CommitAsync(insert);
                outcomes.Add("insert committed");
            }
            catch (CamusDBException ex)
            {
                AssertRetryable(ex, "the commit of the plain insert");
                outcomes.Add("insert commit refused " + ex.Code);
            }

            if (outcomes[0] == "upsert staged")
            {
                try
                {
                    await database.Transactions.CommitAsync(upsert);
                    outcomes.Add("upsert committed");
                }
                catch (CamusDBException ex)
                {
                    AssertRetryable(ex, "the commit of the upsert");
                    outcomes.Add("upsert commit refused " + ex.Code);
                }
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(insert);
            await database.Transactions.RollbackIfNotCompletedAsync(upsert);
        }

        // Observed in all four modes: the upsert's fence meets the insert's write of the key, and the
        // younger upsert dies by wait-die. Even an optimistic insert is seen, because a confirmed write
        // folds an implicit lock on its key.
        CollectionAssert.AreEqual(
            new[] { "upsert refused " + CamusDBErrorCodes.TransactionConflict, "insert committed" },
            outcomes);

        CollectionAssert.AreEqual(new[] { "6|100" }, await Rows(executor, database, dbname));
    }

    [TestCaseSource(nameof(Modes))]
    public async Task ConcurrentCounterLosesNoIncrement(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        const int Workers = 6;
        const int StatementsPerWorker = 8;
        int committed = 0;
        int abandoned = 0;

        async Task Worker()
        {
            for (int i = 0; i < StatementsPerWorker; i++)
            {
                try
                {
                    await SerializableRetryHelper.ExecuteAutocommitAsync(async _ =>
                    {
                        KvTransaction tx = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
                        try
                        {
                            await Exec(executor, dbname, tx, Increment);
                            await database.Transactions.CommitAsync(tx);
                        }
                        finally
                        {
                            await database.Transactions.RollbackIfNotCompletedAsync(tx);
                        }
                    }, maxAttempts: 50);

                    Interlocked.Increment(ref committed);
                }
                catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
                {
                    // The retry budget ran out: the statement did not commit, so it adds nothing.
                    Interlocked.Increment(ref abandoned);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(_ => Task.Run(Worker)));

        TestContext.WriteLine($"committed={committed} abandoned={abandoned}");

        Assert.Greater(committed, 0);
        CollectionAssert.AreEqual(new[] { $"1|{committed}" }, await Rows(executor, database, dbname));
    }

    /// <summary>
    /// A row another transaction committed after this transaction began. The arbiter probe reads the
    /// latest committed entry, so the upsert finds the row and updates it in every mode; it never
    /// inserts a second row, and the statement does not fail.
    /// </summary>
    [TestCaseSource(nameof(Modes))]
    public async Task RowCommittedAfterTheTransactionBeganIsUpdated(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname, "CREATE TABLE other (id INT64 NOT NULL PRIMARY KEY)", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction late = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);

        try
        {
            // Fix the transaction's start before the other commit, on a table the upsert does not touch.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(late, dbname, "SELECT id FROM other", null));
            await cursor.ToListAsync();

            KvTransaction early = await database.Transactions.BeginAsync();
            await Exec(executor, dbname, early, "INSERT INTO c (k, n) VALUES (7, 10)");
            await database.Transactions.CommitAsync(early);

            await Exec(executor, dbname, late, "INSERT INTO c (k, n) VALUES (7, 1) ON CONFLICT (k) DO UPDATE SET n = c.n + excluded.n");
            await database.Transactions.CommitAsync(late);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(late);
        }

        CollectionAssert.AreEqual(new[] { "7|11" }, await Rows(executor, database, dbname));
    }
}
