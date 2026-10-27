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
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The read phase of a write statement in a Read Committed transaction reads at one statement
/// timestamp, as a SELECT does: the locate scan of an <c>UPDATE</c> or <c>DELETE</c>, and the source
/// query of an <c>INSERT … SELECT</c>. The write phase still
/// reads the latest value: it locks each located row, reads it again and re-checks the WHERE, and the
/// uniqueness check of an insert sees a key committed after the timestamp.
///
/// <para>Each test commits a competing change from a test hook that runs after the statement started
/// (and minted its timestamp) and before its read phase reads the first row. A locate scan that read
/// the latest value would see that change; one that reads at the statement timestamp does not. Runs
/// with hash routing and with key-range routing.</para>
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[NonParallelizable]
internal sealed class TestReadCommittedWriteStatementSnapshot : BaseTest
{
    private const int Accounts = 20;

    private const long StartBalance = 100;

    private readonly bool keyRange;

    public TestReadCommittedWriteStatementSnapshot(bool keyRange) => this.keyRange = keyRange;

    protected override int NodeInitialPartitions => keyRange ? 2 : 1;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = keyRange };

    private static Task<ExecuteNonSQLResult> ExecAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, KvTransaction tx, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    /// <summary>Runs <paramref name="sql"/> in its own committed Read Committed transaction.</summary>
    private static async Task CommitAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            await ExecAsync(executor, db, tx, sql);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static Task<KvTransaction> BeginReadCommittedAsync(DatabaseDescriptor database)
        => database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

    private static async Task<long[]> IdsWhereAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string where)
    {
        KvTransaction tx = await BeginReadCommittedAsync(database);
        try
        {
            List<QueryResultRow> rows = await QueryAsync(executor, db, tx, "SELECT id FROM accounts WHERE " + where);
            await database.Transactions.CommitAsync(tx);
            return rows.Select(r => r.Row["id"].LongValue).OrderBy(id => id).ToArray();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Creates <c>accounts</c> (ids 0..19, each with <see cref="StartBalance"/>) and an empty
    /// <c>accounts_copy</c> with the same shape.
    /// </summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            "CREATE TABLE accounts_copy (id INT64 PRIMARY KEY, balance INT64)", null));

        string values = string.Join(", ", Enumerable.Range(0, Accounts).Select(i => $"({i}, {StartBalance})"));
        await CommitAsync(executor, database, db, "INSERT INTO accounts (id, balance) VALUES " + values);

        return (db, database, executor);
    }

    /// <summary>Moves 10 from account 0 to the last account, in one committed transaction.</summary>
    private static async Task TransferAsync(CommandExecutor executor, DatabaseDescriptor database, string db)
    {
        KvTransaction transfer = await BeginReadCommittedAsync(database);
        try
        {
            await ExecAsync(executor, db, transfer, "UPDATE accounts SET balance = balance - 10 WHERE id = 0");
            await ExecAsync(executor, db, transfer, $"UPDATE accounts SET balance = balance + 10 WHERE id = {Accounts - 1}");
            await database.Transactions.CommitAsync(transfer);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(transfer);
        }
    }

    [Test]
    public async Task Update_LocateScan_ReadsAtTheStatementTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        KeyValueReadMode? locateMode = null;
        try
        {
            updater.TestBeforeLocateHook = async () =>
            {
                updater.TestBeforeLocateHook = null;
                locateMode = tx.QueryReadView.Mode;
                await TransferAsync(executor, database, db);
            };

            // The transfer commits after the statement's timestamp: at that timestamp no account holds 110.
            ExecuteNonSQLResult updated = await ExecAsync(executor, db, tx, "UPDATE accounts SET balance = 0 WHERE balance = 110");

            Assert.That(locateMode, Is.EqualTo(KeyValueReadMode.StatementSnapshot), "the locate scan reads in the statement mode");
            Assert.That(updated.ModifiedRows, Is.EqualTo(0), "the locate scan reads at the statement timestamp, before the transfer");
            Assert.That(tx.QueryReadView.Mode, Is.EqualTo(KeyValueReadMode.Default), "the snapshot closes when the statement ends");

            // The next statement reads at a new timestamp and sees the transfer.
            List<QueryResultRow> after = await QueryAsync(executor, db, tx, "SELECT id FROM accounts WHERE balance = 110");
            Assert.That(after.Select(r => r.Row["id"].LongValue), Is.EqualTo(new long[] { Accounts - 1 }));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeLocateHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task Update_WritePhase_RechecksTheLatestValue_WithTheSnapshotClosed()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        KeyValueReadMode? writeMode = null;
        try
        {
            updater.TestBeforeLocateHook = async () =>
            {
                updater.TestBeforeLocateHook = null;
                await TransferAsync(executor, database, db);

                // Installed after the transfer, whose own UPDATE statements run through the same updater.
                updater.TestBeforeWriteHook = () =>
                {
                    updater.TestBeforeWriteHook = null;
                    writeMode = tx.QueryReadView.Mode;
                    return Task.CompletedTask;
                };
            };

            // At the statement timestamp every account holds 100, so the locate scan finds all of them.
            // The write phase locks each one and reads it again: the two that the transfer changed no
            // longer match, and are skipped. Neither one is overwritten from its older value.
            ExecuteNonSQLResult updated = await ExecAsync(executor, db, tx, "UPDATE accounts SET balance = balance + 1 WHERE balance = 100");

            Assert.That(writeMode, Is.EqualTo(KeyValueReadMode.Default), "the write phase reads the latest value");
            Assert.That(updated.ModifiedRows, Is.EqualTo(Accounts - 2));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeLocateHook = null;
            updater.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.That(await IdsWhereAsync(executor, database, db, "balance = 90"), Is.EqualTo(new long[] { 0 }));
        Assert.That(await IdsWhereAsync(executor, database, db, "balance = 110"), Is.EqualTo(new long[] { Accounts - 1 }));
        Assert.That(await IdsWhereAsync(executor, database, db, "balance = 101"), Has.Length.EqualTo(Accounts - 2));
    }

    [Test]
    public async Task Delete_LocateScan_ReadsAtTheStatementTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowDeleter deleter = executor.RowDeleterForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        KeyValueReadMode? locateMode = null;
        try
        {
            deleter.TestBeforeLocateHook = async () =>
            {
                deleter.TestBeforeLocateHook = null;
                locateMode = tx.QueryReadView.Mode;
                await TransferAsync(executor, database, db);
            };

            ExecuteNonSQLResult deleted = await ExecAsync(executor, db, tx, "DELETE FROM accounts WHERE balance = 110");

            Assert.That(locateMode, Is.EqualTo(KeyValueReadMode.StatementSnapshot));
            Assert.That(deleted.ModifiedRows, Is.EqualTo(0), "the locate scan reads at the statement timestamp, before the transfer");

            // At the next statement's timestamp every account still holding 100 matches, and the two
            // that the transfer changed do not.
            ExecuteNonSQLResult rest = await ExecAsync(executor, db, tx, "DELETE FROM accounts WHERE balance = 100");
            Assert.That(rest.ModifiedRows, Is.EqualTo(Accounts - 2));

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            deleter.TestBeforeLocateHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.That(await IdsWhereAsync(executor, database, db, "id >= 0"), Is.EqualTo(new long[] { 0, Accounts - 1 }));
    }

    [Test]
    public async Task InsertSelect_SourceQuery_ReadsAtTheStatementTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowInsertSelector selector = executor.RowInsertSelectorForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        KeyValueReadMode? sourceMode = null;
        try
        {
            selector.TestBeforeSourceReadHook = async () =>
            {
                selector.TestBeforeSourceReadHook = null;
                sourceMode = tx.QueryReadView.Mode;
                await TransferAsync(executor, database, db);
            };

            ExecuteNonSQLResult inserted = await ExecAsync(executor, db, tx,
                "INSERT INTO accounts_copy (id, balance) SELECT id, balance FROM accounts");

            Assert.That(sourceMode, Is.EqualTo(KeyValueReadMode.StatementSnapshot));
            Assert.That(inserted.ModifiedRows, Is.EqualTo(Accounts));
            Assert.That(tx.QueryReadView.Mode, Is.EqualTo(KeyValueReadMode.Default));

            List<QueryResultRow> copy = await QueryAsync(executor, db, tx, "SELECT id, balance FROM accounts_copy");
            Assert.That(copy.Select(r => r.Row["balance"].LongValue), Is.All.EqualTo(StartBalance),
                "the copy is the state at the statement timestamp, before the transfer");

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            selector.TestBeforeSourceReadHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task InsertSelect_UniqueCheck_SeesAKeyCommittedAfterTheStatementTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowInsertSelector selector = executor.RowInsertSelectorForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        try
        {
            selector.TestBeforeSourceReadHook = async () =>
            {
                selector.TestBeforeSourceReadHook = null;
                await CommitAsync(executor, database, db, "INSERT INTO accounts_copy (id, balance) VALUES (7, 1)");
            };

            CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(() => ExecAsync(executor, db, tx,
                "INSERT INTO accounts_copy (id, balance) SELECT id, balance FROM accounts"))!;
            Assert.That(duplicate.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue),
                "the uniqueness check reads the latest value, not the statement timestamp");
        }
        finally
        {
            selector.TestBeforeSourceReadHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task WriteStatements_SeeTheTransactionsOwnWrites()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        KeyValueReadMode? locateMode = null;
        try
        {
            await ExecAsync(executor, db, tx, "INSERT INTO accounts (id, balance) VALUES (50, 777)");

            updater.TestBeforeLocateHook = () =>
            {
                updater.TestBeforeLocateHook = null;
                locateMode = tx.QueryReadView.Mode;
                return Task.CompletedTask;
            };

            Assert.That((await ExecAsync(executor, db, tx, "UPDATE accounts SET balance = 778 WHERE balance = 777")).ModifiedRows, Is.EqualTo(1));
            Assert.That(locateMode, Is.EqualTo(KeyValueReadMode.StatementSnapshot));
            Assert.That((await ExecAsync(executor, db, tx,
                "INSERT INTO accounts_copy (id, balance) SELECT id, balance FROM accounts WHERE balance = 778")).ModifiedRows, Is.EqualTo(1));
            Assert.That((await ExecAsync(executor, db, tx, "DELETE FROM accounts WHERE balance = 778")).ModifiedRows, Is.EqualTo(1));
            Assert.That(await QueryAsync(executor, db, tx, "SELECT id FROM accounts WHERE id = 50"), Is.Empty);

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeLocateHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task TicketUpdate_LocateScan_ReadsAtTheStatementTimestamp()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        KvTransaction tx = await BeginReadCommittedAsync(database);
        try
        {
            updater.TestBeforeLocateHook = async () =>
            {
                updater.TestBeforeLocateHook = null;
                await TransferAsync(executor, database, db);
            };

            UpdateResult result = await executor.Update(new UpdateTicket(
                txnState: tx,
                databaseName: db,
                tableName: "accounts",
                plainValues: new() { { "balance", new ColumnValue(ColumnType.Integer64, 0L) } },
                exprValues: null,
                where: null,
                filters: new() { new("balance", "=", new ColumnValue(ColumnType.Integer64, 110L)) },
                parameters: null));

            Assert.That(result.UpdatedRows, Is.EqualTo(0), "the ticket path locates at the statement timestamp too");

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeLocateHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task SerializableAndOptimistic_LocateTheLatestValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        RowUpdater updater = executor.RowUpdaterForTests;

        KvTransaction serializable = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction optimistic = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Optimistic);
        try
        {
            foreach (KvTransaction tx in new[] { serializable, optimistic })
            {
                KeyValueReadMode? locateMode = null;
                updater.TestBeforeLocateHook = () =>
                {
                    updater.TestBeforeLocateHook = null;
                    locateMode = tx.QueryReadView.Mode;
                    return Task.CompletedTask;
                };

                await ExecAsync(executor, db, tx, "UPDATE accounts SET balance = 5 WHERE id = 3");
                Assert.That(locateMode, Is.EqualTo(KeyValueReadMode.Default), $"{tx.IsolationLevel} {tx.Locking}");

                await database.Transactions.RollbackAsync(tx);
            }
        }
        finally
        {
            updater.TestBeforeLocateHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(serializable);
            await database.Transactions.RollbackIfNotCompletedAsync(optimistic);
        }
    }
}
