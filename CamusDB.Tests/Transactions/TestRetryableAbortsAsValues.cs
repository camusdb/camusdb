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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;
using CamusDB.Tests.Utils;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// An UPDATE that loses an optimistic write-write race hands its retryable abort to the caller as a
/// value when the caller passes a <see cref="RetryableAbortSink"/>, and throws it when it does not.
///
/// <para>The conflict is real, not injected: a transaction reads a row, a second transaction updates
/// that row and commits, and the first then updates it. Kahuna's early conflict check answers the
/// first transaction's re-read or write with <c>Aborted</c>, which is the bank workload's contention
/// shape. The tests count every <see cref="CamusDBException"/> thrown on the statement's own async
/// flow, so a regression that throws and catches internally, or rethrows through the frames the sink
/// exists to skip, fails here rather than only in a profile.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRetryableAbortsAsValues : SharedNodeBaseTest
{
    // accounts(id String PK, balance Integer64), seeded with two rows.
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAccountsAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE accounts (id STRING NOT NULL PRIMARY KEY, balance INT64 NOT NULL)", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction seed = await database.Transactions.BeginAsync();
        await ExecIn(executor, dbname, seed, "INSERT INTO accounts (id, balance) VALUES (\"a\", 100)");
        await ExecIn(executor, dbname, seed, "INSERT INTO accounts (id, balance) VALUES (\"b\", 200)");
        await database.Transactions.CommitAsync(seed);

        return (dbname, database, executor);
    }

    private static Task<KvTransaction> BeginOptimisticAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: KeyValueTransactionLocking.Optimistic);

    private static Task<ExecuteNonSQLResult> ExecIn(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql, RetryableAbortSink? aborts = null)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null, retryableAborts: aborts));

    private static async Task<List<QueryResultRow>> SelectIn(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<long> CommittedBalance(CommandExecutor executor, DatabaseDescriptor database, string dbname, string id)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await SelectIn(executor, dbname, tx, $"SELECT balance FROM accounts WHERE id = \"{id}\"");
        await database.Transactions.CommitAsync(tx);
        return rows.Single().Row["balance"].LongValue;
    }

    /// <summary>
    /// How many times a test repeats the race before it gives up on the early abort. Kahuna does not
    /// always abort the loser at the statement: a transaction that began in a busy HLC millisecond keeps
    /// reading past the competing commit, and its loss is caught at commit instead. So each test repeats
    /// the race until the statement is aborted, and every attempt, aborted or not, must stay free of
    /// engine exceptions.
    /// </summary>
    private const int MaxAttempts = 20;

    /// <summary>Reads row a in a fresh optimistic transaction, then commits a competing update of it.</summary>
    private static async Task<KvTransaction> ReadThenLoseRowAAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, long winnerBalance)
    {
        // A fresh HLC millisecond for the loser makes the early abort likely; see MaxAttempts.
        await Task.Delay(5);
        KvTransaction loser = await BeginOptimisticAsync(database);
        await SelectIn(executor, dbname, loser, "SELECT balance FROM accounts WHERE id = \"a\"");

        KvTransaction winner = await BeginOptimisticAsync(database);
        await ExecIn(executor, dbname, winner, $"UPDATE accounts SET balance = {winnerBalance} WHERE id = \"a\"");
        await database.Transactions.CommitAsync(winner);

        // The early abort compares the key's applied revision with the loser's read; a commit is
        // acknowledged when it is durable, and its apply can land a moment later. A linearizable
        // read waits for local application, so the loser's next statement meets the new revision.
        await CommittedBalance(executor, database, dbname, "a");

        return loser;
    }

    [Test]
    public async Task LostUpdate_WithoutSink_ThrowsTheKahunaAbort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            long winnerBalance = 1000 + attempt;
            KvTransaction loser = await ReadThenLoseRowAAsync(executor, database, dbname, winnerBalance);

            CamusDBException? thrown = null;
            try
            {
                await ExecIn(executor, dbname, loser, "UPDATE accounts SET balance = balance + 1 WHERE id = \"a\"");
            }
            catch (CamusDBException ex)
            {
                thrown = ex;
            }

            await database.Transactions.RollbackIfNotCompletedAsync(loser);
            Assert.That(await CommittedBalance(executor, database, dbname, "a"), Is.EqualTo(winnerBalance));

            if (thrown is null)
                continue;

            Assert.That(thrown.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
            Assert.That(thrown.Message, Does.Contain("aborted by Kahuna"),
                "the conflict must be Kahuna's early abort, the shape the sink exists for");
            return;
        }

        Assert.Fail($"Kahuna never aborted the lost update at the statement in {MaxAttempts} attempts");
    }

    [Test]
    public async Task LostUpdate_WithSink_CarriesTheAbortWithoutAnyThrow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            long winnerBalance = 1000 + attempt;
            KvTransaction loser = await ReadThenLoseRowAAsync(executor, database, dbname, winnerBalance);

            RetryableAbortSink aborts = new();
            ExecuteNonSQLResult result = default;
            int throws = await EngineThrowCounter.CountAsync(async () =>
                result = await ExecIn(executor, dbname, loser, "UPDATE accounts SET balance = balance + 1 WHERE id = \"a\"", aborts));

            Assert.That(throws, Is.Zero, "a statement with a sink must not throw an engine exception, lost or not");

            await database.Transactions.RollbackIfNotCompletedAsync(loser);
            Assert.That(await CommittedBalance(executor, database, dbname, "a"), Is.EqualTo(winnerBalance),
                "the loser must leave nothing behind");

            if (!aborts.HasAbort)
                continue;

            Assert.That(aborts.Abort!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
            Assert.That(aborts.Abort.Message, Does.Contain("aborted by Kahuna"));
            Assert.That(result.ModifiedRows, Is.Zero);
            return;
        }

        Assert.Fail($"Kahuna never aborted the lost update at the statement in {MaxAttempts} attempts");
    }

    [Test]
    public async Task WritePhaseLoss_WithSink_CarriesTheAbortWithoutAnyThrow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            long winnerBalance = 1000 + attempt;

            // The competing commit lands between the locate scan and the write phase, so the loss is
            // detected by the write phase's re-read or by its batched set, not by the locate scan.
            bool hookRan = false;
            updater.TestBeforeWriteHook = async () =>
            {
                updater.TestBeforeWriteHook = null;
                hookRan = true;
                KvTransaction winner = await BeginOptimisticAsync(database);
                await ExecIn(executor, dbname, winner, $"UPDATE accounts SET balance = {winnerBalance} WHERE id = \"a\"");
                await database.Transactions.CommitAsync(winner);
                await CommittedBalance(executor, database, dbname, "a"); // wait for the apply; see ReadThenLoseRowAAsync
            };

            await Task.Delay(5); // a fresh HLC millisecond; see MaxAttempts
            KvTransaction loser = await BeginOptimisticAsync(database);
            RetryableAbortSink aborts = new();
            int throws;
            try
            {
                throws = await EngineThrowCounter.CountAsync(async () =>
                    await ExecIn(executor, dbname, loser, "UPDATE accounts SET balance = balance + 1 WHERE id = \"a\"", aborts));
            }
            finally
            {
                updater.TestBeforeWriteHook = null;
            }

            Assert.That(hookRan, Is.True, "the competing commit never ran inside the write window");
            Assert.That(throws, Is.Zero);

            await database.Transactions.RollbackIfNotCompletedAsync(loser);
            Assert.That(await CommittedBalance(executor, database, dbname, "a"), Is.EqualTo(winnerBalance));

            if (!aborts.HasAbort)
                continue;

            Assert.That(aborts.Abort!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
            return;
        }

        Assert.Fail($"Kahuna never aborted the lost update at the statement in {MaxAttempts} attempts");
    }

    [Test]
    public async Task UncontendedUpdate_WithSink_CommitsAndRecordsNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccountsAsync();

        KvTransaction tx = await BeginOptimisticAsync(database);
        RetryableAbortSink aborts = new();
        ExecuteNonSQLResult result = await ExecIn(executor, dbname, tx, "UPDATE accounts SET balance = balance + 1 WHERE id = \"b\"", aborts);
        await database.Transactions.CommitAsync(tx);

        Assert.That(aborts.HasAbort, Is.False);
        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(await CommittedBalance(executor, database, dbname, "b"), Is.EqualTo(201L));
    }
}
