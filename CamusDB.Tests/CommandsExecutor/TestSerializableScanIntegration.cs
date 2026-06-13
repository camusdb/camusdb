
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
/// End-to-end integration tests for Serializable read-write scan phantom protection.
///
/// These tests drive the full executor stack — QueryScanner / QueryExecutor →
/// AcquireRowRangeLockAsync / AcquireIndexRangeLockAsync / AcquireBoundedIndexRangeLockAsync —
/// verifying that a real SELECT inside a Serializable+ReadWrite transaction acquires the
/// range lock that blocks concurrent inserts. This closes the gap between the unit tests in
/// TestSerializableRangeLocks (which call the lock methods directly) and the actual wiring
/// through the query executor.
/// </summary>
[TestFixture]
public sealed class TestSerializableScanIntegration : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTable(string tableName = "items")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: tableName,
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
        return (dbname, database, executor);
    }

    private static async Task InsertRow(
        string dbname,
        CommandExecutor executor,
        KvTransaction tx,
        string id,
        long value)
    {
        InsertTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id,       id) },
                    { "value", new(ColumnType.Integer64, value) }
                }
            }
        );
        await executor.Insert(ticket);
    }

    private static async Task<List<QueryResultRow>> RunQuery(
        string dbname,
        CommandExecutor executor,
        KvTransaction tx)
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
    // 1. Full table SELECT in Serializable+RW blocks a phantom insert
    //    — validates the executor wiring through QueryScanner
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_SelectScan_BlocksPhantomInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        // Seed one row so the scan has something to iterate.
        KvTransaction setup = await database.Transactions.BeginAsync();
        await InsertRow(dbname, executor, setup, ObjectIdGenerator.Generate().ToString(), 1L);
        await database.Transactions.CommitAsync(setup);

        // Scanner: Serializable+RW SELECT * drains the cursor, acquiring the row range lock.
        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        List<QueryResultRow> rows = await RunQuery(dbname, executor, scanner);
        Assert.AreEqual(1, rows.Count, "Pre-check: seeded row must be visible to scanner");

        // Concurrent inserter: tries to insert into the same row space while scanner holds lock.
        KvTransaction inserter = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => InsertRow(dbname, executor, inserter, ObjectIdGenerator.Generate().ToString(), 2L));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex?.Code,
            "Insert into the row space must fail with TransactionMustRetry while scanner holds the range lock");

        await database.Transactions.RollbackAsync(inserter);
        await database.Transactions.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 2. Full table SELECT in ReadCommitted does NOT block a concurrent insert
    //    — confirms the default path is unchanged
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReadCommitted_SelectScan_DoesNotBlockInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await InsertRow(dbname, executor, setup, ObjectIdGenerator.Generate().ToString(), 1L);
        await database.Transactions.CommitAsync(setup);

        // ReadCommitted SELECT: no range lock acquired.
        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await RunQuery(dbname, executor, scanner);

        // Concurrent insert must succeed freely.
        KvTransaction inserter = await database.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(
            () => InsertRow(dbname, executor, inserter, ObjectIdGenerator.Generate().ToString(), 2L),
            "Insert must not be blocked by a ReadCommitted scanner");
        Assert.DoesNotThrowAsync(() => database.Transactions.CommitAsync(inserter),
            "Inserter commit must succeed");

        await database.Transactions.CommitAsync(scanner);
    }

    // -----------------------------------------------------------------------
    // 3. Two Serializable+RW SELECT scans coexist (S∩S)
    //    — two scanners can run concurrently without blocking each other
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_TwoSelectScans_Coexist()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await InsertRow(dbname, executor, setup, ObjectIdGenerator.Generate().ToString(), 1L);
        await database.Transactions.CommitAsync(setup);

        KvTransaction s1 = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        KvTransaction s2 = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        // Both scans acquire shared row range locks — must not throw (S∩S compatible).
        Assert.DoesNotThrowAsync(() => RunQuery(dbname, executor, s1),
            "First Serializable+RW scan must succeed");
        Assert.DoesNotThrowAsync(() => RunQuery(dbname, executor, s2),
            "Second Serializable+RW scan must succeed (S∩S compatible)");

        await database.Transactions.CommitAsync(s1);
        await database.Transactions.CommitAsync(s2);
    }

    // -----------------------------------------------------------------------
    // 4. Range lock released after scanner commits — next insert succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_SelectScan_UnblocksInsertAfterCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        KvTransaction setup = await database.Transactions.BeginAsync();
        await InsertRow(dbname, executor, setup, ObjectIdGenerator.Generate().ToString(), 1L);
        await database.Transactions.CommitAsync(setup);

        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await RunQuery(dbname, executor, scanner);
        await database.Transactions.CommitAsync(scanner);

        // Lock released — insert now succeeds.
        KvTransaction inserter = await database.Transactions.BeginAsync();
        await InsertRow(dbname, executor, inserter, ObjectIdGenerator.Generate().ToString(), 2L);
        Assert.DoesNotThrowAsync(() => database.Transactions.CommitAsync(inserter),
            "Insert must succeed after the Serializable+RW scanner has committed");
    }
}
