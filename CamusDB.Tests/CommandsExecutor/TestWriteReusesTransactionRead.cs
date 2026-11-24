
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
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The write phase of an UPDATE or DELETE takes a row from its transaction's own read by key and does
/// not read the row again, for a Serializable optimistic transaction that proves its reads at commit.
/// The tests count the store's reads around one statement, and cover what the reuse must not change:
/// a transaction sees its own writes, a row another transaction committed in between is refused, and
/// every other transaction shape reads the row again.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestWriteReusesTransactionRead : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { PointReadRowIdCacheEnabled = true, SerializableOptimisticPointLocks = false };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table)> SetupAccounts(
        CamusDBOptions? options = null, int rows = 2)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            options is null ? await CreateDatabase() : await CreateDatabase(options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns:
            [
                new("id", ColumnType.Integer64),
                new("balance", ColumnType.Integer64),
                new("note", ColumnType.String, notNull: false),
            ],
            constraints: [new(ConstraintType.PrimaryKey, CamusDBConstants.PrimaryKeyInternalName, [new("id", OrderType.Ascending)])],
            ifNotExists: false
        ));

        KvTransaction load = await database.Transactions.BeginAsync();
        for (int id = 1; id <= rows; id++)
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: load, database: dbname, sql: $"INSERT INTO accounts (id, balance) VALUES ({id}, {id * 100})", parameters: null));
        }
        await database.Transactions.CommitAsync(load);

        // One read outside the measured transactions, so the store remembers the row id of key 1 and
        // an UPDATE by that key needs no locate read.
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(100));

        return (dbname, database, executor, await database.TableDescriptors["accounts"]);
    }

    private static Task<KvTransaction> Begin(
        DatabaseDescriptor database,
        CamusIsolationLevel isolation = CamusIsolationLevel.Serializable,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Optimistic)
        => database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        return await cursor.ToListAsync();
    }

    private static async Task<long?> Balance(CommandExecutor executor, KvTransaction tx, string dbname, long id)
    {
        List<QueryResultRow> rows = await Query(executor, tx, dbname, $"SELECT balance FROM accounts WHERE id = {id}");
        return rows.Count == 0 ? null : rows.Single().Row["balance"].LongValue;
    }

    private static Task<ExecuteNonSQLResult> Write(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));

    private static async Task<long?> CommittedBalance(DatabaseDescriptor database, CommandExecutor executor, string dbname, long id)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        long? balance = await Balance(executor, tx, dbname, id);
        await database.Transactions.CommitAsync(tx);
        return balance;
    }

    /// <summary>The store's row reads since <paramref name="table"/> was sampled into the three counts.</summary>
    private static (long Batch, long Reused) Reads(TableDescriptor table)
        => (table.Store.PrimaryRowBatchReadCalls, table.Store.PrimaryRowReusedMutationReads);

    // -----------------------------------------------------------------------
    // The reuse
    // -----------------------------------------------------------------------

    [Test]
    public async Task AnUpdateAfterAReadOfTheRowDoesNotReadItAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long batch, long reused) = Reads(table);
        ExecuteNonSQLResult result = await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batch, Is.EqualTo(0), "the write phase must not read the row again");
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(105));
    }

    [Test]
    public async Task ADeleteAfterAReadOfTheRowDoesNotReadItAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long batch, long reused) = Reads(table);
        ExecuteNonSQLResult result = await Write(executor, tx, dbname, "DELETE FROM accounts WHERE id = 1");

        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batch, Is.EqualTo(0));
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.Null);
        Assert.That(await CommittedBalance(database, executor, dbname, 2), Is.EqualTo(200));
    }

    [Test]
    public async Task WithTheReuseOffTheUpdateReadsTheRowAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupAccounts(Options with { TransactionReadReuseEnabled = false });

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long batch, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(table.Store.PrimaryRowBatchReadCalls - batch, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(0));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(105));
    }

    // -----------------------------------------------------------------------
    // Transaction shapes that must read again
    // -----------------------------------------------------------------------

    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
    public async Task ATransactionThatDoesNotProveItsReadsAtCommitReadsTheRowAgain(
        CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction tx = await Begin(database, isolation, locking);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long batch, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(0));
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batch, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(105));
    }

    [Test]
    public async Task ATransactionThatLocksItsPointReadsReadsTheRowAgain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupAccounts(Options with { SerializableOptimisticPointLocks = true });

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long batch, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(0));
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batch, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(105));
    }

    // -----------------------------------------------------------------------
    // The transaction's own writes
    // -----------------------------------------------------------------------

    [Test]
    public async Task ASecondUpdateOfTheRowSeesTheFirst()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long _, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        // The first write dropped the remembered read, so the second one read the written row.
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1));
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(110));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(110));
    }

    [Test]
    public async Task AReadOfARowTheTransactionWroteIsNotKept()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction tx = await Begin(database);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(105));

        (long _, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(0));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(110));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task AnUpdateThenADeleteThenAnInsertOfTheKeyEndWithTheInsertedRow(bool reuse)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor _) =
            await SetupAccounts(Options with { TransactionReadReuseEnabled = reuse });

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");
        Assert.That((await Write(executor, tx, dbname, "DELETE FROM accounts WHERE id = 1")).ModifiedRows, Is.EqualTo(1));
        await Write(executor, tx, dbname, "INSERT INTO accounts (id, balance) VALUES (1, 7)");
        Assert.That((await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 1 WHERE id = 1")).ModifiedRows, Is.EqualTo(1));

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(8));
    }

    // -----------------------------------------------------------------------
    // Rows the reuse must leave whole
    // -----------------------------------------------------------------------

    [Test]
    public async Task ARowWithAnOutOfLineValueKeepsItThroughAReusedUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        string note = new('n', Options.LargeValueThresholdBytes * 8);
        KvTransaction seed = await Begin(database);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: seed, database: dbname, sql: "UPDATE accounts SET note = @note WHERE id = 1",
            parameters: new Dictionary<string, ColumnValue> { ["@note"] = new(ColumnType.String, note) }));
        await database.Transactions.CommitAsync(seed);

        KvTransaction tx = await Begin(database);
        Assert.That(await Balance(executor, tx, dbname, 1), Is.EqualTo(100));

        (long _, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1));
        await database.Transactions.CommitAsync(tx);

        KvTransaction check = await database.Transactions.BeginAsync();
        QueryResultRow row = (await Query(executor, check, dbname, "SELECT balance, note FROM accounts WHERE id = 1")).Single();
        await database.Transactions.CommitAsync(check);

        Assert.That(row.Row["balance"].LongValue, Is.EqualTo(105));
        Assert.That(row.Row["note"].StrValue, Is.EqualTo(note));
    }

    [Test]
    public async Task ATransactionKeepsABoundedNumberOfReadsAndReadsTheOthersAgain()
    {
        const int rows = 70;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts(rows: rows);

        KvTransaction tx = await Begin(database);
        for (int id = 1; id <= rows; id++)
            Assert.That(await Balance(executor, tx, dbname, id), Is.EqualTo(id * 100L));

        (long _, long reused) = Reads(table);
        await Write(executor, tx, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1), "an early read is kept");

        await Write(executor, tx, dbname, $"UPDATE accounts SET balance = balance + 5 WHERE id = {rows}");
        Assert.That(table.Store.PrimaryRowReusedMutationReads - reused, Is.EqualTo(1), "a read past the bound is not kept");

        await database.Transactions.CommitAsync(tx);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(105));
        Assert.That(await CommittedBalance(database, executor, dbname, rows), Is.EqualTo(rows * 100L + 5));
    }

    // -----------------------------------------------------------------------
    // A row another transaction changed after the read
    // -----------------------------------------------------------------------

    /// <summary>
    /// The reader computes from the row it read. Another transaction then commits a change of the row.
    /// With the reuse the reader's UPDATE has no second read to learn that from, so its write or its
    /// commit must refuse. Both transactions never commit, and the row holds every committed change.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task ACommitBetweenTheReadAndTheUpdateIsNeverOverwritten(bool reuse)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor _) =
            await SetupAccounts(Options with { TransactionReadReuseEnabled = reuse });

        // The writer begins first: a write gives way to an older transaction that read the key, so a
        // younger writer would be the one refused.
        KvTransaction writer = await Begin(database);
        Assert.That(await Balance(executor, writer, dbname, 2), Is.EqualTo(200));

        KvTransaction reader = await Begin(database);
        Assert.That(await Balance(executor, reader, dbname, 1), Is.EqualTo(100));

        bool writerCommitted = await TryCommit(database, executor, writer, dbname, "UPDATE accounts SET balance = balance + 50 WHERE id = 1");
        bool readerCommitted = await TryCommit(database, executor, reader, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1");

        Assert.That(writerCommitted && readerCommitted, Is.False, "the reader read the row before the writer's commit");

        long expected = 100 + (writerCommitted ? 50 : 0) + (readerCommitted ? 5 : 0);
        Assert.That(await CommittedBalance(database, executor, dbname, 1), Is.EqualTo(expected));
        Assert.That(writerCommitted, Is.True, "the older transaction is the one that goes on");
    }

    /// <summary>Runs one write and the commit; false when either is refused with a retryable error.</summary>
    private static async Task<bool> TryCommit(DatabaseDescriptor database, CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        try
        {
            ExecuteNonSQLResult result = await Write(executor, tx, dbname, sql);
            Assert.That(result.ModifiedRows, Is.EqualTo(1));
            await database.Transactions.CommitAsync(tx);
            return true;
        }
        catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.TransactionMustRetry)
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            return false;
        }
    }
}
