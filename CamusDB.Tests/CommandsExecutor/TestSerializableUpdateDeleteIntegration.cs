
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end integration tests verifying that Serializable+ReadWrite UPDATE and DELETE
/// statements acquire an exclusive predicate range lock during the locate phase, blocking
/// concurrent Serializable+ReadWrite readers from entering the same row range while the
/// modification is in flight.
///
/// The gap this closes: TestSerializableUpdateDelete calls AcquireRowRangeLockAsync(exclusive:
/// true) directly. These tests drive the full stack — RowUpdater/RowDeleter → QueryTicket →
/// QueryScanner → AcquireRowRangeLockAsync(exclusive: true) — verifying the wiring works
/// end-to-end rather than by reading the code.
///
/// The exclusive range lock is acquired during the locate scan and held until the transaction
/// commits or rolls back. After executor.Update/Delete returns (but before CommitAsync), the
/// lock is still active, so a concurrent Serializable+RW SELECT that tries to acquire Shared
/// on the same range receives TransactionConflict.
/// </summary>
[TestFixture]
public sealed class TestSerializableUpdateDeleteIntegration : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("value", ColumnType.Integer64)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        KvTransaction setup = await database.Transactions.BeginAsync();
        List<string> ids = new();
        for (int i = 0; i < 3; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);
            InsertTicket insert = new(
                txnState: setup,
                databaseName: dbname,
                tableName: "items",
                values: new() { new() { { "id", new(ColumnType.Id, id) }, { "value", new(ColumnType.Integer64, (long)i) } } }
            );
            await executor.Insert(insert);
        }
        await database.Transactions.CommitAsync(setup);

        return (dbname, database, executor, ids);
    }

    private static async Task<List<QueryResultRow>> RunQuery(
        string dbname, CommandExecutor executor, KvTransaction tx)
    {
        QueryTicket queryTicket = new(
            txnState:     tx,
            databaseName: dbname,
            tableName:    "items",
            index:        null,
            projection:   null,
            where:        null,
            filters:      null,
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);
        return await cursor.ToListAsync();
    }

    // -----------------------------------------------------------------------
    // 1. Serializable+RW UPDATE holds exclusive range lock: concurrent reader blocked
    //
    // RowUpdater constructs QueryTicket(exclusivePredicateLocks: true), which causes
    // QueryScanner to call AcquireRowRangeLockAsync(exclusive: true). The lock is held
    // until CommitAsync. A concurrent Serializable+RW SELECT tries AcquireRowRangeLockAsync
    // (Shared) — Kahuna's X∩S conflict returns AlreadyLocked → TransactionConflict.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_Update_BlocksConcurrentSerializableReader()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupTable();

        // Updater: Serializable+RW UPDATE SET value = 99 (all rows, full table scan locate).
        KvTransaction updater = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        UpdateTicket updateTicket = new(
            txnState:    updater,
            databaseName: dbname,
            tableName:   "items",
            plainValues: new() { { "value", new(ColumnType.Integer64, 99L) } },
            exprValues:  null,
            where:       null,
            filters:     null,
            parameters:  null
        );
        UpdateResult result = await executor.Update(updateTicket);
        Assert.Greater(result.UpdatedRows, 0, "Pre-check: at least one row must be updated");

        // Concurrent Serializable+RW reader: tries to scan the same table while updater
        // still holds the exclusive predicate range lock from its locate phase.
        KvTransaction reader = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => RunQuery(dbname, executor, reader));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Serializable+RW SELECT must fail with TransactionConflict while updater holds exclusive range lock");

        await database.Transactions.RollbackAsync(reader);
        await database.Transactions.CommitAsync(updater);
    }

    // -----------------------------------------------------------------------
    // 2. Serializable+RW DELETE holds exclusive range lock: concurrent reader blocked
    //
    // Uses a non-PK filter so the locate scan goes through ScanUsingTableIndex
    // (not a PK point-lookup via LookupUnique), triggering AcquireRowRangeLockAsync.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_Delete_BlocksConcurrentSerializableReader()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupTable();

        // Deleter: Serializable+RW DELETE WHERE value = 0 (non-PK filter → full row scan).
        KvTransaction deleter = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        DeleteTicket deleteTicket = new(
            txnState:    deleter,
            databaseName: dbname,
            tableName:   "items",
            where:       null,
            filters:     new() { new("value", "=", new(ColumnType.Integer64, 0L)) },
            parameters:  null
        );
        DeleteResult result = await executor.Delete(deleteTicket);
        Assert.AreEqual(1, result.DeletedRows, "Pre-check: one row (value=0) must be deleted");

        // Concurrent Serializable+RW reader must fail while deleter holds exclusive range lock.
        KvTransaction reader = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => RunQuery(dbname, executor, reader));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ex?.Code,
            "Serializable+RW SELECT must fail with TransactionConflict while deleter holds exclusive range lock");

        await database.Transactions.RollbackAsync(reader);
        await database.Transactions.CommitAsync(deleter);
    }

    // -----------------------------------------------------------------------
    // 3. ReadCommitted UPDATE does NOT hold exclusive predicate lock:
    //    concurrent Serializable+RW reader succeeds
    //
    // IsSerializableReadWrite(tx) is false for ReadCommitted → AcquireRowRangeLockAsync
    // returns early without acquiring the Exclusive lock.
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_Update_DoesNotBlockConcurrentSerializableReader()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupTable();

        // ReadCommitted UPDATE: no exclusive range lock acquired.
        KvTransaction updater = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);

        UpdateTicket updateTicket = new(
            txnState:    updater,
            databaseName: dbname,
            tableName:   "items",
            plainValues: new() { { "value", new(ColumnType.Integer64, 99L) } },
            exprValues:  null,
            where:       null,
            filters:     null,
            parameters:  null
        );
        await executor.Update(updateTicket);

        // Concurrent Serializable+RW reader must be able to scan the table.
        KvTransaction reader = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.DoesNotThrowAsync(() => RunQuery(dbname, executor, reader),
            "Serializable+RW SELECT must not be blocked by a ReadCommitted updater (no exclusive range lock)");

        await database.Transactions.CommitAsync(reader);
        await database.Transactions.CommitAsync(updater);
    }

    // -----------------------------------------------------------------------
    // 4. After UPDATE commits, the exclusive range lock is released:
    //    a subsequent Serializable+RW SELECT succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_Update_AfterCommit_ReaderSucceeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupTable();

        KvTransaction updater = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        UpdateTicket updateTicket = new(
            txnState:    updater,
            databaseName: dbname,
            tableName:   "items",
            plainValues: new() { { "value", new(ColumnType.Integer64, 99L) } },
            exprValues:  null,
            where:       null,
            filters:     null,
            parameters:  null
        );
        await executor.Update(updateTicket);
        await database.Transactions.CommitAsync(updater);

        // Exclusive lock is released — a new Serializable+RW reader must now succeed.
        KvTransaction reader = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.DoesNotThrowAsync(() => RunQuery(dbname, executor, reader),
            "Serializable+RW SELECT must succeed once the exclusive holder has committed");
        await database.Transactions.CommitAsync(reader);
    }
}
