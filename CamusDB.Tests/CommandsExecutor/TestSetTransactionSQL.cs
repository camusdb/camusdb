
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that SET TRANSACTION ISOLATION LEVEL actually applies the requested level and mode
/// to the current transaction, and that the SQL form is properly gated.
///
/// The primary API for specifying isolation level at the HTTP layer is the
/// <c>isolationLevel</c>/<c>transactionMode</c> request-body fields. The SQL form
/// <c>SET TRANSACTION ISOLATION LEVEL SERIALIZABLE [READ ONLY|WRITE]</c> applies to the
/// already-begun transaction — valid before any locks are acquired.
/// </summary>
[TestFixture]
public sealed class TestSetTransactionSQL : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupDbAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("value", ColumnType.Integer64),
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

    private static async Task ExecuteSetTransaction(
        string dbname, CommandExecutor executor, KvTransaction tx, string sql)
    {
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        await cursor.ToListAsync(); // drain — should be empty
    }

    // -----------------------------------------------------------------------
    // 1. SET TRANSACTION applies Serializable to the current ReadCommitted txn
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_Serializable_UpdatesIsolationLevel()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel, "Initial level must be ReadCommitted");

        await ExecuteSetTransaction(dbname, executor, tx,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel,
            "IsolationLevel must be Serializable after SET TRANSACTION");
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode,
            "Mode must default to ReadWrite when not specified");

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. SET TRANSACTION SERIALIZABLE READ ONLY applies both level and mode
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_Serializable_ReadOnly_AppliesMode()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync();

        await ExecuteSetTransaction(dbname, executor, tx,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadOnly, tx.TransactionMode,
            "Mode must be ReadOnly after SET TRANSACTION … READ ONLY");

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 3. SET TRANSACTION SERIALIZABLE READ WRITE is idempotent with default
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_Serializable_ReadWrite_Explicit()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync();

        await ExecuteSetTransaction(dbname, executor, tx,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE");

        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel);
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode);

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 3b. SET TRANSACTION ISOLATION LEVEL READ COMMITTED opts down from the
    //     Serializable default; mode defaults to ReadWrite.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_ReadCommitted_OptsDown()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.AreEqual(CamusIsolationLevel.Serializable, tx.IsolationLevel, "Initial level must be Serializable");

        await ExecuteSetTransaction(dbname, executor, tx,
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED");

        Assert.AreEqual(CamusIsolationLevel.ReadCommitted, tx.IsolationLevel,
            "IsolationLevel must be ReadCommitted after SET TRANSACTION … READ COMMITTED");
        Assert.AreEqual(CamusTransactionMode.ReadWrite, tx.TransactionMode,
            "Mode must default to ReadWrite when not specified");

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4a. SET TRANSACTION is rejected after any prior statement — locked case
    //
    // A Serializable+RW SELECT acquires shared point locks. The "already executed
    // a statement" flag is set by MarkStatementExecuted before the query runs, so
    // a subsequent SET TRANSACTION throws before it even checks for locks.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_RejectedAfterStatement_LockedCase()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction setup = await db.Transactions.BeginAsync();
        string id = ObjectIdGenerator.Generate().ToString();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "items",
            values: new() { new() {
                { "id",    new(ColumnType.Id,        id)   },
                { "value", new(ColumnType.Integer64, 1L)   },
            }}));
        await db.Transactions.CommitAsync(setup);

        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "items",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        await rows.ToListAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecuteSetTransaction(dbname, executor, tx,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex?.Code);
        StringAssert.Contains("already executed a statement", ex?.Message);

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4b. SET TRANSACTION is rejected after a ReadCommitted read (no locks)
    //
    // A ReadCommitted SELECT acquires no locks, so the old lock-only check would
    // have allowed a subsequent SET TRANSACTION SERIALIZABLE — silently omitting
    // the shared lock for the data already read. The "statement executed" flag
    // catches this case: the flag is set before the RC SELECT runs, so the
    // subsequent SET TRANSACTION throws regardless of lock state.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_RejectedAfterReadCommitted_Read()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        KvTransaction setup = await db.Transactions.BeginAsync();
        string id = ObjectIdGenerator.Generate().ToString();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "items",
            values: new() { new() {
                { "id",    new(ColumnType.Id,        id)   },
                { "value", new(ColumnType.Integer64, 1L)   },
            }}));
        await db.Transactions.CommitAsync(setup);

        // ReadCommitted — no shared locks acquired on the read.
        KvTransaction tx = await db.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "items",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        await rows.ToListAsync(); // no locks, but statementExecuted is now true

        // SET TRANSACTION after any statement — even a lock-free RC read — must throw.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecuteSetTransaction(dbname, executor, tx,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex?.Code,
            "SET TRANSACTION after a ReadCommitted read must throw even though no locks were acquired");
        StringAssert.Contains("already executed a statement", ex?.Message);

        await db.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 5. After SET TRANSACTION the new level governs subsequent DML
    //
    // Begin ReadCommitted, apply SET TRANSACTION SERIALIZABLE, then run a
    // concurrent update that would succeed under RC (no locks held on the row
    // by the first txn) but would conflict under Serializable (the txn now
    // acquires shared point locks on reads). Verify the Serializable semantics
    // are actually active after the SQL statement.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SetTransaction_SQL_NewLevelGovernsDML()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        // Seed a row.
        KvTransaction setup = await db.Transactions.BeginAsync();
        string id = ObjectIdGenerator.Generate().ToString();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "items",
            values: new() { new() {
                { "id",    new(ColumnType.Id,        id) },
                { "value", new(ColumnType.Integer64, 0L) },
            }}));
        await db.Transactions.CommitAsync(setup);

        // Tx begins as ReadCommitted, then is promoted to Serializable before the read.
        KvTransaction tx = await db.Transactions.BeginAsync(); // ReadCommitted
        await ExecuteSetTransaction(dbname, executor, tx,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE");

        // Read the row — now acquires a shared point lock because IsolationLevel == Serializable.
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "items",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(q);
        await cursor.ToListAsync(); // shared lock acquired

        // A concurrent writer tries to update the same row — the shared lock blocks it.
        KvTransaction writer = await db.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.Update(new UpdateTicket(
                txnState: writer, databaseName: dbname, tableName: "items",
                plainValues: new() { { "value", new(ColumnType.Integer64, 99L) } },
                exprValues: null,
                where: null,
                filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                parameters: null)));
        Assert.IsNotNull(ex, "Concurrent writer must be blocked by the shared lock acquired under Serializable semantics");

        await db.Transactions.RollbackAsync(writer);
        await db.Transactions.RollbackAsync(tx);
    }
}
