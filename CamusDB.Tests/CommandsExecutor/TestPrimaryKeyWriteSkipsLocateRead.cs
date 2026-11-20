/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An UPDATE or DELETE by primary key whose row id the store remembers runs no locate query: its
/// write phase locks the remembered row, reads it at the latest value and re-checks the predicate
/// there. The tests count the store's single-row reads (the locate lookup) and batch reads (the write
/// phase) around one statement, and cover the cases where the remembered id cannot be the answer: a
/// row that is gone, a key that lives under a new row id, a predicate that fails, and a transaction
/// shape whose locate read is not the write phase's read.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[NonParallelizable]
public sealed class TestPrimaryKeyWriteSkipsLocateRead : BaseTest
{
    private readonly bool optimisticPointLocks;

    /// <param name="optimisticPointLocks">
    /// Whether a Serializable optimistic transaction locks its single-key reads or proves them at
    /// commit. Every test must hold under both.
    /// </param>
    public TestPrimaryKeyWriteSkipsLocateRead(bool optimisticPointLocks) => this.optimisticPointLocks = optimisticPointLocks;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { PointReadRowIdCacheEnabled = true, SerializableOptimisticPointLocks = optimisticPointLocks };

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table)> SetupAccounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns:
            [
                new("id", ColumnType.Integer64),
                new("balance", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, CamusDBConstants.PrimaryKeyInternalName, [new("id", OrderType.Ascending)])],
            ifNotExists: false
        ));

        await Write(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (1, 100)");
        await Write(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (2, 200)");

        TableDescriptor table = await database.TableDescriptors["accounts"];
        Assert.That(table.Store.RemembersUniqueRowIds, Is.True);

        return (dbname, database, executor, table);
    }

    /// <summary>Runs one statement in its own transaction of the given shape and commits it.</summary>
    private static async Task<ExecuteNonSQLResult> Write(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        CamusIsolationLevel isolation = CamusIsolationLevel.Serializable,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Optimistic,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(isolationLevel: isolation, locking: locking);
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: parameters));
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<long>> Balances(DatabaseDescriptor database, CommandExecutor executor, string dbname, long id)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: $"SELECT balance FROM accounts WHERE id = {id}", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["balance"].LongValue).ToList();
    }

    /// <summary>Looks the key up once, so the store remembers its row id, and returns that id.</summary>
    private static async Task<(string kvKey, ObjectIdValue rowId)> Remember(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, TableDescriptor table, long id)
    {
        HashSet<string> before = [.. table.Store.RememberedUniqueRowIdKeysForTesting()];
        Assert.That(await Balances(database, executor, dbname, id), Has.Count.EqualTo(1));

        string kvKey = table.Store.RememberedUniqueRowIdKeysForTesting().Single(k => !before.Contains(k));
        Assert.That(table.Store.TryGetCachedUniqueRowIdForTesting(kvKey, out ObjectIdValue rowId), Is.True);
        return (kvKey, rowId);
    }

    // -----------------------------------------------------------------------
    // The remembered row is the statement's row
    // -----------------------------------------------------------------------

    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Optimistic)]
    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
    public async Task AnUpdateByARememberedKeyReadsItsRowOnce(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        long pointReads = table.Store.PrimaryRowPointReadCalls;
        long batchReads = table.Store.PrimaryRowBatchReadCalls;

        ExecuteNonSQLResult result = await Write(database, executor, dbname,
            "UPDATE accounts SET balance = balance + 5 WHERE id = 1", isolation, locking);

        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "no locate read");
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batchReads, Is.EqualTo(1), "the write phase's locked read");
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 105L }));
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 200L }));
    }

    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Optimistic)]
    [TestCase(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
    [TestCase(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
    public async Task ADeleteByARememberedKeyReadsItsRowOnce(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        long pointReads = table.Store.PrimaryRowPointReadCalls;
        long batchReads = table.Store.PrimaryRowBatchReadCalls;

        ExecuteNonSQLResult result = await Write(database, executor, dbname,
            "DELETE FROM accounts WHERE id = 1", isolation, locking);

        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "no locate read");
        Assert.That(table.Store.PrimaryRowBatchReadCalls - batchReads, Is.EqualTo(1), "the write phase's locked read");
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(0), "the delete forgot the key");
        Assert.That(await Balances(database, executor, dbname, 1), Is.Empty);
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 200L }));
    }

    /// <summary>
    /// A pessimistic Read Committed statement locates at its statement timestamp and takes the
    /// exclusive row lock before the write phase's read, so its locate read is not the write phase's
    /// read. It keeps the locate query.
    /// </summary>
    [Test]
    public async Task APessimisticReadCommittedStatementKeepsItsLocateRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);
        await Remember(database, executor, dbname, table, 2);

        long pointReads = table.Store.PrimaryRowPointReadCalls;

        ExecuteNonSQLResult updated = await Write(database, executor, dbname,
            "UPDATE accounts SET balance = balance + 5 WHERE id = 1",
            CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);

        Assert.That(updated.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(1), "the update's locate read");

        pointReads = table.Store.PrimaryRowPointReadCalls;

        ExecuteNonSQLResult deleted = await Write(database, executor, dbname,
            "DELETE FROM accounts WHERE id = 2",
            CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic);

        Assert.That(deleted.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(1), "the delete's locate read");
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 105L }));
        Assert.That(await Balances(database, executor, dbname, 2), Is.Empty);
    }

    [Test]
    public async Task AParameterizedUpdateWithReturningTakesTheRememberedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 2);

        long pointReads = table.Store.PrimaryRowPointReadCalls;

        ExecuteNonSQLResult result = await Write(database, executor, dbname,
            "UPDATE accounts SET balance = balance - @amount WHERE id = @id RETURNING balance",
            parameters: new()
            {
                { "@amount", new ColumnValue(ColumnType.Integer64, 30) },
                { "@id", new ColumnValue(ColumnType.Integer64, 2) },
            });

        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "no locate read");
        Assert.That(result.ReturningRows!.Single().Row.Values.Single().LongValue, Is.EqualTo(170L));
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 170L }));
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
    }

    /// <summary>
    /// A composite primary key resolves to one row only when the predicate fixes every key column. The
    /// statement that does takes the remembered row; the one that fixes a prefix scans, and both
    /// change exactly the rows they name.
    /// </summary>
    [Test]
    public async Task ACompositeKeyTakesTheRememberedRowOnlyWhenEveryColumnIsFixed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "ledger",
            columns:
            [
                new("tenant", ColumnType.Integer64),
                new("id", ColumnType.Integer64),
                new("amount", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, CamusDBConstants.PrimaryKeyInternalName, [new("tenant", OrderType.Ascending), new("id", OrderType.Ascending)])],
            ifNotExists: false
        ));

        await Write(database, executor, dbname, "INSERT INTO ledger (tenant, id, amount) VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)");
        TableDescriptor table = await database.TableDescriptors["ledger"];

        Assert.That(await Amounts(database, executor, dbname, "tenant = 1 AND id = 2"), Is.EqualTo(new[] { 20L }));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1));

        long pointReads = table.Store.PrimaryRowPointReadCalls;

        ExecuteNonSQLResult one = await Write(database, executor, dbname, "UPDATE ledger SET amount = amount + 1 WHERE tenant = 1 AND id = 2");

        Assert.That(one.ModifiedRows, Is.EqualTo(1));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "no locate read");

        ExecuteNonSQLResult prefix = await Write(database, executor, dbname, "UPDATE ledger SET amount = amount + 100 WHERE tenant = 1");

        Assert.That(prefix.ModifiedRows, Is.EqualTo(2));
        Assert.That(await Amounts(database, executor, dbname, "tenant = 1 AND id = 1"), Is.EqualTo(new[] { 110L }));
        Assert.That(await Amounts(database, executor, dbname, "tenant = 1 AND id = 2"), Is.EqualTo(new[] { 121L }));
        Assert.That(await Amounts(database, executor, dbname, "tenant = 2 AND id = 1"), Is.EqualTo(new[] { 30L }));
    }

    private static async Task<List<long>> Amounts(DatabaseDescriptor database, CommandExecutor executor, string dbname, string where)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: $"SELECT amount FROM ledger WHERE {where}", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["amount"].LongValue).ToList();
    }

    /// <summary>
    /// The write phase takes the old index entries from its own read of the row. An update through the
    /// remembered row moves the row's entry in a secondary index, and a delete removes it.
    /// </summary>
    [Test]
    public async Task ASecondaryIndexFollowsAWriteThroughTheRememberedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: ddl, database: dbname, sql: "CREATE INDEX balance_idx ON accounts (balance)", parameters: null));
        await database.Transactions.CommitAsync(ddl);

        table = await database.TableDescriptors["accounts"];
        await Remember(database, executor, dbname, table, 1);
        await Remember(database, executor, dbname, table, 2);

        long pointReads = table.Store.PrimaryRowPointReadCalls;

        await Write(database, executor, dbname, "UPDATE accounts SET balance = 150 WHERE id = 1");
        await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 2");

        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "no locate read");
        Assert.That(await IdsByBalance(database, executor, dbname, 100), Is.Empty, "the old entry is gone");
        Assert.That(await IdsByBalance(database, executor, dbname, 150), Is.EqualTo(new[] { 1L }));
        Assert.That(await IdsByBalance(database, executor, dbname, 200), Is.Empty, "the deleted row left no entry");
    }

    private static async Task<List<long>> IdsByBalance(DatabaseDescriptor database, CommandExecutor executor, string dbname, long balance)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: $"SELECT id FROM accounts@{{FORCE_INDEX=balance_idx}} WHERE balance = {balance}", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["id"].LongValue).ToList();
    }

    // -----------------------------------------------------------------------
    // The remembered row is not the answer
    // -----------------------------------------------------------------------

    /// <summary>
    /// The rest of the predicate fails on the remembered row. The row exists, so no other row can hold
    /// its key: the statement changes nothing, runs no locate query, and the store keeps the id.
    /// </summary>
    [Test]
    public async Task ARememberedRowThatFailsTheRestOfThePredicateIsLeftAlone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        long pointReads = table.Store.PrimaryRowPointReadCalls;

        ExecuteNonSQLResult updated = await Write(database, executor, dbname,
            "UPDATE accounts SET balance = 0 WHERE id = 1 AND balance > 1000");
        ExecuteNonSQLResult deleted = await Write(database, executor, dbname,
            "DELETE FROM accounts WHERE id = 1 AND balance > 1000");

        Assert.That(updated.ModifiedRows, Is.EqualTo(0));
        Assert.That(deleted.ModifiedRows, Is.EqualTo(0));
        Assert.That(table.Store.PrimaryRowPointReadCalls - pointReads, Is.EqualTo(0), "the row was there: nothing to look up");
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1), "a failed predicate does not forget the row");
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
    }

    [Test]
    public async Task AKeyWithNoRowChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        ExecuteNonSQLResult updated = await Write(database, executor, dbname, "UPDATE accounts SET balance = 0 WHERE id = 9");
        ExecuteNonSQLResult deleted = await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 9");

        Assert.That(updated.ModifiedRows, Is.EqualTo(0));
        Assert.That(deleted.ModifiedRows, Is.EqualTo(0));
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 200L }));
    }

    /// <summary>
    /// The remembered id names a row that does not exist while the key lives on under another row id,
    /// as after a delete and a re-insert on another node. The write phase finds no row, and the
    /// statement then resolves the key through the index and writes the row that holds it.
    /// </summary>
    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task AnUpdateWhoseRememberedRowIsGoneFindsTheKeyThroughTheIndex(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        (string kvKey, ObjectIdValue realRowId) = await Remember(database, executor, dbname, table, 1);

        ObjectIdValue bogus = new(0x7fff, 0x7fff, 0x7fff);
        table.Store.EvictCachedUniqueRowId(kvKey);
        table.Store.RememberUniqueRowIdForTesting(kvKey, bogus);

        ExecuteNonSQLResult result = await Write(database, executor, dbname,
            "UPDATE accounts SET balance = balance + 5 WHERE id = 1", isolation);

        Assert.That(result.ModifiedRows, Is.EqualTo(1), "the row that holds the key was updated");
        Assert.That(table.Store.TryGetCachedUniqueRowIdForTesting(kvKey, out ObjectIdValue repaired), Is.True);
        Assert.That(repaired, Is.EqualTo(realRowId), "the index read replaced the stale id");
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 105L }));
    }

    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task ADeleteWhoseRememberedRowIsGoneFindsTheKeyThroughTheIndex(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        (string kvKey, _) = await Remember(database, executor, dbname, table, 1);

        ObjectIdValue bogus = new(0x7fff, 0x7fff, 0x7fff);
        table.Store.EvictCachedUniqueRowId(kvKey);
        table.Store.RememberUniqueRowIdForTesting(kvKey, bogus);

        ExecuteNonSQLResult result = await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 1", isolation);

        Assert.That(result.ModifiedRows, Is.EqualTo(1), "the row that holds the key was deleted");
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(0));
        Assert.That(await Balances(database, executor, dbname, 1), Is.Empty);
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 200L }));
    }

    /// <summary>
    /// A stale id for a key that no row holds any more: the write phase finds no row, the index read
    /// finds no entry, and the statement reports no rows and leaves nothing remembered.
    /// </summary>
    [Test]
    public async Task ARememberedRowThatIsGoneWithItsKeyChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        (string kvKey, ObjectIdValue oldRowId) = await Remember(database, executor, dbname, table, 1);

        await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 1");
        table.Store.RememberUniqueRowIdForTesting(kvKey, oldRowId);

        ExecuteNonSQLResult updated = await Write(database, executor, dbname, "UPDATE accounts SET balance = 1 WHERE id = 1");

        Assert.That(updated.ModifiedRows, Is.EqualTo(0));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(0), "the stale id was dropped");

        table.Store.RememberUniqueRowIdForTesting(kvKey, oldRowId);

        ExecuteNonSQLResult deleted = await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 1");

        Assert.That(deleted.ModifiedRows, Is.EqualTo(0));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(0), "the stale id was dropped");
        Assert.That(await Balances(database, executor, dbname, 1), Is.Empty);
    }

    // -----------------------------------------------------------------------
    // Concurrency: a commit between the statement's start and its locked read
    // -----------------------------------------------------------------------

    /// <summary>
    /// Another transaction deletes the row and inserts its key again, under a new row id, after the
    /// statement took the remembered id and before its locked read. The statement must update the new
    /// row: the remembered row is gone, and the index names the row that holds the key now.
    /// </summary>
    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task AKeyReinsertedUnderANewRowIdBeforeTheLockedReadIsUpdated(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        RowUpdater updater = executor.RowUpdaterForTests;
        bool hookRan = false;

        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            hookRan = true;
            await Write(database, executor, dbname, "DELETE FROM accounts WHERE id = 1");
            await Write(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (1, 500)");
        };

        ExecuteNonSQLResult result;
        try
        {
            result = await Write(database, executor, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1", isolation);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
        }

        Assert.That(hookRan, Is.True, "the competing commits never ran inside the write window");
        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 505L }));
    }

    /// <summary>
    /// Another transaction changes the row and commits after the statement took the remembered id and
    /// before its locked read. The statement has read nothing of the row yet, so it computes from the
    /// committed value and neither update is lost.
    /// </summary>
    [TestCase(CamusIsolationLevel.Serializable)]
    [TestCase(CamusIsolationLevel.ReadCommitted)]
    public async Task ACommitBeforeTheLockedReadIsNotOverwritten(CamusIsolationLevel isolation)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        RowUpdater updater = executor.RowUpdaterForTests;
        bool hookRan = false;

        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            hookRan = true;
            await Write(database, executor, dbname, "UPDATE accounts SET balance = balance + 100 WHERE id = 1");
        };

        ExecuteNonSQLResult result;
        try
        {
            result = await Write(database, executor, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 1", isolation);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
        }

        Assert.That(hookRan, Is.True, "the competing commit never ran inside the write window");
        Assert.That(result.ModifiedRows, Is.EqualTo(1));
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 205L }));
    }

    /// <summary>
    /// A Serializable transaction read the row, and another transaction then tries to change it before
    /// the update's locked read. At most one of the two may commit: the reader saw a value the other
    /// would replace. The update's locked read is the only read the statement makes, and the conflict
    /// must hold with it alone. Which transaction is refused is the engine's choice.
    /// </summary>
    [Test]
    public async Task ASerializableTransactionThatReadTheRowAndAConcurrentWriterDoNotBothCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupAccounts();
        await Remember(database, executor, dbname, table, 1);

        RowUpdater updater = executor.RowUpdaterForTests;
        bool hookRan = false;
        bool competitorCommitted = false;
        bool committed = false;

        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.Serializable, locking: KeyValueTransactionLocking.Optimistic);
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: dbname, sql: "SELECT balance FROM accounts WHERE id = 1", parameters: null));
            Assert.That((await cursor.ToListAsync()).Single().Row["balance"].LongValue, Is.EqualTo(100L));

            updater.TestBeforeWriteHook = async () =>
            {
                updater.TestBeforeWriteHook = null;
                hookRan = true;

                try
                {
                    await Write(database, executor, dbname, "UPDATE accounts SET balance = balance + 100 WHERE id = 1");
                    competitorCommitted = true;
                }
                catch (CamusDBException)
                {
                    // Refused in favor of the transaction that read the row first.
                }
            };

            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: dbname, sql: "UPDATE accounts SET balance = balance + 5 WHERE id = 1", parameters: null));
            await database.Transactions.CommitAsync(tx);
            committed = true;
        }
        catch (CamusDBException)
        {
            // The conflict may surface at the statement or at the commit.
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.That(hookRan, Is.True, "the competing write never ran inside the write window");
        Assert.That(committed && competitorCommitted, Is.False, "a transaction that read 100 committed after the row became 200");

        long expected = 100 + (competitorCommitted ? 100 : 0) + (committed ? 5 : 0);
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { expected }));
    }
}
