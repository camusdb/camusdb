/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
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
/// Proves that a caller's cancellation token stops a query at its storage read, not merely at the
/// point the transport next writes a row.
///
/// <para><b>Why the assertions are shaped this way.</b> Counting the rows a cursor delivered before
/// it faulted is the observable difference between a scan that stopped and one that did not: an
/// uncancellable scan delivers every seeded row and never faults, so a test that only asserted "the
/// call ended" would pass either way. Each test here therefore asserts both that the enumeration
/// raised and that it raised after far fewer rows than the table holds.</para>
///
/// <para>Timing plays no part. The consumer itself decides when the query is mid-flight — it takes
/// one row, cancels, and then asks for the next — so there is no delay to tune and no race to lose
/// on a slow machine.</para>
/// </summary>
[TestFixture]
public sealed class TestQueryCancellation : SharedNodeBaseTest
{
    /// <summary>Rows seeded per table. Large enough that "stopped early" is unambiguous.</summary>
    internal const int SeededRows = 300;

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupSeededTable(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            options is null ? await CreateDatabase() : await CreateDatabase(options);

        await SeedItemsTableAsync(dbname, database, executor);
        return (dbname, database, executor);
    }

    /// <summary>
    /// Creates the <c>items</c> table and fills it with <see cref="SeededRows"/> rows. Shared with
    /// the standalone fixture below, which runs the same checks on a per-test node.
    /// </summary>
    internal static async Task SeedItemsTableAsync(
        string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
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
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "value_idx",
                    new ColumnIndexInfo[] { new("value", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);

        KvTransaction seed = await database.Transactions.BeginAsync();
        for (int i = 0; i < SeededRows; i++)
        {
            InsertTicket insert = new(
                txnState: seed,
                databaseName: dbname,
                tableName: "items",
                values: new()
                {
                    new()
                    {
                        { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "value", new(ColumnType.Integer64, (long)i) }
                    }
                }
            );
            await executor.Insert(insert);
        }
        await database.Transactions.CommitAsync(seed);
    }

    internal static QueryTicket BuildTicket(
        string dbname, KvTransaction tx, CancellationToken cancellationToken, string? index = null)
        => new(
            txnState:     tx,
            databaseName: dbname,
            tableName:    "items",
            index:        index,
            projection:   null,
            where:        null,
            filters:      null,
            orderBy:      null,
            limit:        null,
            offset:       null,
            parameters:   null,
            cancellationToken: cancellationToken
        );

    /// <summary>
    /// Drains <paramref name="cursor"/> and cancels <paramref name="cts"/> once
    /// <paramref name="cancelAfter"/> rows have arrived. Returns how many rows were delivered
    /// before the enumeration ended, and the exception it ended with (null when it completed).
    /// </summary>
    internal static async Task<(int delivered, Exception? fault)> DrainAndCancelAsync(
        IAsyncEnumerable<QueryResultRow> cursor, CancellationTokenSource cts, int cancelAfter)
    {
        int delivered = 0;
        try
        {
            await foreach (QueryResultRow _ in cursor)
            {
                delivered++;
                if (delivered == cancelAfter)
                    await cts.CancelAsync();
            }
            return (delivered, null);
        }
        catch (Exception e)
        {
            return (delivered, e);
        }
    }

    // -----------------------------------------------------------------------
    // 1. A full table scan stops at the storage read
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task FullScan_CancelledMidStream_StopsBeforeReadingTheWholeTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        using CancellationTokenSource cts = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, tx, cts.Token));

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: 1);

        Assert.IsInstanceOf<OperationCanceledException>(fault,
            "A cancelled scan must raise OperationCanceledException, not run to completion");
        Assert.Less(delivered, SeededRows,
            "The scan kept reading after the cancel — the token did not reach the storage read");

        await database.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 2. A token cancelled before the query starts reads nothing at all
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task FullScan_TokenAlreadyCancelled_DeliversNoRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, tx, cts.Token));

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: int.MaxValue);

        Assert.IsInstanceOf<OperationCanceledException>(fault, "An already-cancelled scan must raise");
        Assert.AreEqual(0, delivered, "No row may be delivered when the token was cancelled up front");

        await database.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 3. The parallel decode path stops too
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task FullScan_ParallelDecode_CancelledMidStream_Stops()
    {
        // Parallelism is fixed when the engine is constructed, so it must be set on the options the
        // executor is built with. Setting it afterwards would be a no-op the test could not detect.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupSeededTable(Options with { MaxQueryParallelism = 4 });

        using CancellationTokenSource cts = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, tx, cts.Token));

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: 1);

        Assert.IsInstanceOf<OperationCanceledException>(fault,
            "The parallel decode pipeline must observe the request token");
        Assert.Less(delivered, SeededRows, "The parallel scan kept reading after the cancel");

        await database.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 4. An index-driven scan stops
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task IndexScan_CancelledMidStream_Stops()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        using CancellationTokenSource cts = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        // Naming the index forces the index-driven scan, which is a different leaf from the
        // primary-row scan and reaches the store through ScanIndex rather than ScanRows.
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, tx, cts.Token, index: "value_idx"));

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: 1);

        Assert.IsInstanceOf<OperationCanceledException>(fault,
            "A cancelled index scan must raise OperationCanceledException");
        Assert.Less(delivered, SeededRows, "The index scan kept reading after the cancel");

        await database.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 5. A join stops — it never reaches the single-table plan path
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task Join_CancelledMidStream_Stops()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        CreateTableTicket otherTable = new(
            databaseName: dbname,
            tableName: "tags",
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
        await executor.CreateTable(otherTable);

        KvTransaction seed = await database.Transactions.BeginAsync();
        for (int i = 0; i < SeededRows; i++)
        {
            InsertTicket insert = new(
                txnState: seed,
                databaseName: dbname,
                tableName: "tags",
                values: new()
                {
                    new()
                    {
                        { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "value", new(ColumnType.Integer64, (long)i) }
                    }
                }
            );
            await executor.Insert(insert);
        }
        await database.Transactions.CommitAsync(seed);

        using CancellationTokenSource cts = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        ExecuteSQLTicket sqlTicket = new(
            txnState: tx,
            database: dbname,
            sql: "SELECT items.value FROM items INNER JOIN tags ON items.value = tags.value",
            parameters: null,
            principal: null,
            cancellationToken: cts.Token
        );

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(sqlTicket);

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: 1);

        Assert.IsInstanceOf<OperationCanceledException>(fault,
            "A cancelled join must raise — the join executor is a separate path from the single-table plan");
        Assert.Less(delivered, SeededRows, "The join kept reading after the cancel");

        await database.Transactions.RollbackAsync(tx);
    }

    // -----------------------------------------------------------------------
    // 6. A cancelled scan leaks no range lock
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task CancelledSerializableScan_RollsBack_AndReleasesTheRangeLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        using CancellationTokenSource cts = new();

        // A Serializable read-write scan takes a range lock over the row key space. If a cancel
        // could strand that lock, the writer below would be rejected until the lease expired.
        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, scanner, cts.Token));

        (int delivered, Exception? fault) = await DrainAndCancelAsync(cursor, cts, cancelAfter: 1);
        Assert.IsInstanceOf<OperationCanceledException>(fault, "Pre-check: the scan must have been cancelled");
        Assert.Less(delivered, SeededRows, "Pre-check: the scan must have stopped early");

        // Rollback runs with its own token, never the cancelled one.
        await database.Transactions.RollbackAsync(scanner);

        KvTransaction writer = await database.Transactions.BeginAsync();
        InsertTicket insert = new(
            txnState: writer,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "value", new(ColumnType.Integer64, 9999L) }
                }
            }
        );

        Assert.DoesNotThrowAsync(() => executor.Insert(insert),
            "The cancelled scan stranded its range lock — a later writer must not be blocked by it");
        await database.Transactions.CommitAsync(writer);
    }

    // -----------------------------------------------------------------------
    // 7. A cancel does not abort a commit
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task CancelAfterTheRead_StillCommits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        using CancellationTokenSource cts = new();

        KvTransaction tx = await database.Transactions.BeginAsync();
        InsertTicket insert = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "value", new(ColumnType.Integer64, 4242L) }
                }
            }
        );
        await executor.Insert(insert);

        // The client disconnects between the write and the commit. The commit must still run: the
        // transport passes CancellationToken.None to it for exactly this reason.
        await cts.CancelAsync();
        Assert.DoesNotThrowAsync(() => database.Transactions.CommitAsync(tx),
            "A cancelled client must not abort a commit that is already under way");

        // The committed row is visible to a later reader, so the commit really landed.
        KvTransaction reader = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, reader, CancellationToken.None));

        int found = 0;
        await foreach (QueryResultRow row in cursor)
        {
            if (row.Row.TryGetValue("value", out ColumnValue? value) && value.LongValue == 4242L)
                found++;
        }

        Assert.AreEqual(1, found, "The row committed after the cancel must be readable");
        await database.Transactions.RollbackAsync(reader);
    }

    // -----------------------------------------------------------------------
    // 8. An uncancellable token leaves behavior unchanged
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task NoToken_ScanDeliversEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSeededTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(BuildTicket(dbname, tx, CancellationToken.None));

        int delivered = 0;
        await foreach (QueryResultRow _ in cursor)
            delivered++;

        Assert.AreEqual(SeededRows, delivered,
            "A ticket carrying no token must scan the whole table, exactly as before");

        await database.Transactions.RollbackAsync(tx);
    }
}

/// <summary>
/// The same cancellation checks on a per-test embedded node rather than a class-shared one.
///
/// <para>The two fixtures reach storage through different node lifetimes, and a range lock is the
/// one part of the scan whose behavior depends on how the node was brought up. Running the scan and
/// the lock-release check under both shapes keeps a pass on the shared node from standing in for a
/// path it never exercised. It is not a multi-node cluster — no fixture here is.</para>
/// </summary>
[TestFixture]
public sealed class TestQueryCancellationPerTestNode : BaseTest
{
    [Test]
    [NonParallelizable]
    public async Task FullScan_CancelledMidStream_StopsBeforeReadingTheWholeTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await TestQueryCancellation.SeedItemsTableAsync(dbname, database, executor);

        using CancellationTokenSource cts = new();
        KvTransaction tx = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(TestQueryCancellation.BuildTicket(dbname, tx, cts.Token));

        (int delivered, Exception? fault) =
            await TestQueryCancellation.DrainAndCancelAsync(cursor, cts, cancelAfter: 1);

        Assert.IsInstanceOf<OperationCanceledException>(fault, "A cancelled scan must raise");
        Assert.Less(delivered, TestQueryCancellation.SeededRows, "The scan kept reading after the cancel");

        await database.Transactions.RollbackAsync(tx);
    }

    [Test]
    [NonParallelizable]
    public async Task CancelledSerializableScan_RollsBack_AndReleasesTheRangeLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await TestQueryCancellation.SeedItemsTableAsync(dbname, database, executor);

        using CancellationTokenSource cts = new();

        KvTransaction scanner = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.Query(TestQueryCancellation.BuildTicket(dbname, scanner, cts.Token));

        (int delivered, Exception? fault) =
            await TestQueryCancellation.DrainAndCancelAsync(cursor, cts, cancelAfter: 1);
        Assert.IsInstanceOf<OperationCanceledException>(fault, "Pre-check: the scan must have been cancelled");
        Assert.Less(delivered, TestQueryCancellation.SeededRows, "Pre-check: the scan must have stopped early");

        await database.Transactions.RollbackAsync(scanner);

        KvTransaction writer = await database.Transactions.BeginAsync();
        InsertTicket insert = new(
            txnState: writer,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "value", new(ColumnType.Integer64, 9999L) }
                }
            }
        );

        Assert.DoesNotThrowAsync(() => executor.Insert(insert),
            "The cancelled scan stranded its range lock — a later writer must not be blocked by it");
        await database.Transactions.CommitAsync(writer);
    }
}
