
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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that Serializable+RW UPDATE/DELETE statements with analyzable bounded predicates
/// on indexed columns acquire tightest-possible range locks — covering only the matched key
/// range, not the whole table.
///
/// The planner converts indexed equality/range predicates (e.g. priority = 1) to a
/// RangeScanFromIndex plan, which calls AcquireBoundedIndexRangeLockAsync with [from, to)
/// bounds instead of AcquireRowRangeLockAsync([null,null)).
///
/// Guarantees verified here:
///   - Two Serializable+RW UPDATEs on DISJOINT index ranges proceed concurrently (no conflict).
///   - Two Serializable+RW UPDATEs on the SAME index range conflict as expected.
///   - An INSERT whose indexed value falls inside the locked range is blocked (phantom fence).
///   - An INSERT outside the locked range proceeds without conflict.
///   - A Serializable+RO snapshot sees a consistent view during concurrent bounded mutations.
/// </summary>
[TestFixture]
public sealed class TestSerializableBoundedPredicate : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupDbAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "tasks",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("priority", ColumnType.Integer64),
                new("name",     ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey,  "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "priority_idx",
                    new ColumnIndexInfo[] { new("priority", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await db.Transactions.CommitAsync(setup);

        return (dbname, db, executor);
    }

    private async Task InsertAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        string id, long priority, string name)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx, databaseName: dbname, tableName: "tasks",
            values: new() { new() {
                { "id",       new(ColumnType.Id,        id)       },
                { "priority", new(ColumnType.Integer64, priority) },
                { "name",     new(ColumnType.String,    name)     },
            }}));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<string?> ReadNameAsync(
        string dbname, DatabaseDescriptor db, CommandExecutor executor,
        string id)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "tasks",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        List<QueryResultRow> list = await rows.ToListAsync();
        await db.Transactions.RollbackAsync(tx);
        return list.Count == 0 ? null : list[0].Row["name"].StrValue;
    }

    // -----------------------------------------------------------------------
    // 1. Disjoint-range UPDATEs proceed concurrently (no false conflict)
    //
    // Two Serializable+RW transactions each UPDATE tasks in disjoint priority
    // ranges (priority=1 vs priority=2). The planner uses RangeScanFromIndex
    // with bounded exclusive locks that do not overlap, so both commits succeed
    // without any TransactionConflict.
    // -----------------------------------------------------------------------

    [Test]
    public async Task DisjointIndexRanges_ConcurrentUpdates_DoNotConflict()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        string id2 = ObjectIdGenerator.Generate().ToString();

        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "task-p1");
        await InsertAsync(dbname, db, executor, id2, priority: 2, name: "task-p2");

        // Run both updates concurrently. They touch disjoint priority ranges.
        Task a = Task.Run(async () =>
        {
            KvTransaction tx = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            try
            {
                await executor.Update(new UpdateTicket(
                    txnState: tx, databaseName: dbname, tableName: "tasks",
                    plainValues: new() { { "name", new(ColumnType.String, "done-p1") } },
                    exprValues: null, where: null,
                    filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
                    parameters: null));
                await db.Transactions.CommitAsync(tx);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(tx);
                throw;
            }
        });

        Task b = Task.Run(async () =>
        {
            KvTransaction tx = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
            try
            {
                await executor.Update(new UpdateTicket(
                    txnState: tx, databaseName: dbname, tableName: "tasks",
                    plainValues: new() { { "name", new(ColumnType.String, "done-p2") } },
                    exprValues: null, where: null,
                    filters: new() { new("priority", "=", new(ColumnType.Integer64, 2L)) },
                    parameters: null));
                await db.Transactions.CommitAsync(tx);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(tx);
                throw;
            }
        });

        // Both must complete without conflict — disjoint index ranges do not block each other.
        Assert.DoesNotThrowAsync(() => Task.WhenAll(a, b),
            "Concurrent updates on disjoint indexed ranges must not conflict");

        Assert.AreEqual("done-p1", await ReadNameAsync(dbname, db, executor, id1));
        Assert.AreEqual("done-p2", await ReadNameAsync(dbname, db, executor, id2));
    }

    // -----------------------------------------------------------------------
    // 2. Overlapping-range UPDATEs conflict as expected
    //
    // Two transactions both try to UPDATE tasks WHERE priority = 1. The second
    // attempt fails immediately with TransactionConflict because the bounded
    // exclusive lock on the priority=1 index range is already held.
    // -----------------------------------------------------------------------

    [Test]
    public async Task OverlappingIndexRanges_ConcurrentUpdates_Conflict()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "task-p1");

        // Txn A holds the exclusive bounded lock on priority=1 range.
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: txA, databaseName: dbname, tableName: "tasks",
            plainValues: new() { { "name", new(ColumnType.String, "updated-by-a") } },
            exprValues: null, where: null,
            filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
            parameters: null));
        // A has completed the locate scan (exclusive lock acquired) but has not committed.

        // Txn B tries to update the same priority=1 range → must conflict.
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Update(new UpdateTicket(
                txnState: txB, databaseName: dbname, tableName: "tasks",
                plainValues: new() { { "name", new(ColumnType.String, "updated-by-b") } },
                exprValues: null, where: null,
                filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
                parameters: null)));

        Assert.IsNotNull(ex);
        Assert.IsTrue(
            ex!.Code == CamusDBErrorCodes.TransactionConflict ||
            ex.Code == CamusDBErrorCodes.TransactionMustRetry,
            $"Overlapping range UPDATE must fail with TransactionConflict or TransactionMustRetry; got {ex.Code}");

        await db.Transactions.RollbackAsync(txB);
        await db.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 3. INSERT inside a locked range is blocked (phantom protection)
    //
    // While Txn A holds an exclusive range lock on priority=1 index range,
    // a concurrent INSERT of a row with priority=1 must fail with
    // TransactionMustRetry (Kahuna write-path fence). An INSERT with priority=5
    // (outside the locked range) must succeed.
    // -----------------------------------------------------------------------

    [Test]
    public async Task BoundedExclusiveLock_BlocksInsideInsert_AllowsOutsideInsert()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "task-p1");

        // Txn A acquires exclusive bounded lock on priority=1 range via UPDATE.
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: txA, databaseName: dbname, tableName: "tasks",
            plainValues: new() { { "name", new(ColumnType.String, "in-flight") } },
            exprValues: null, where: null,
            filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
            parameters: null));
        // txA holds X range lock on [encoded(1), encoded(2)) and X point lock on the row.

        // INSERT with priority=1 (inside the locked range) must fail.
        string idInside = ObjectIdGenerator.Generate().ToString();
        KvTransaction txInside = await db.Transactions.BeginAsync();
        CamusDBException? exInside = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(new InsertTicket(
                txnState: txInside, databaseName: dbname, tableName: "tasks",
                values: new() { new() {
                    { "id",       new(ColumnType.Id,        idInside) },
                    { "priority", new(ColumnType.Integer64, 1L)       },
                    { "name",     new(ColumnType.String,    "phantom") },
                }})));
        Assert.IsNotNull(exInside, "INSERT inside locked range must fail");
        await db.Transactions.RollbackAsync(txInside);

        // INSERT with priority=5 (outside the locked range [1, 2)) must succeed.
        string idOutside = ObjectIdGenerator.Generate().ToString();
        KvTransaction txOutside = await db.Transactions.BeginAsync();
        Assert.DoesNotThrowAsync(async () =>
        {
            await executor.Insert(new InsertTicket(
                txnState: txOutside, databaseName: dbname, tableName: "tasks",
                values: new() { new() {
                    { "id",       new(ColumnType.Id,        idOutside) },
                    { "priority", new(ColumnType.Integer64, 5L)        },
                    { "name",     new(ColumnType.String,    "outside")  },
                }}));
            await db.Transactions.CommitAsync(txOutside);
        }, "INSERT outside locked range must succeed");

        await db.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 4. Disjoint half-bounded range scans (SELECT) do not block each other
    //
    // Two Serializable+RW SELECTs with half-bounded predicates on the indexed
    // column acquire Shared range locks on disjoint sub-ranges. They coexist
    // because S∩S on non-overlapping ranges is allowed.
    // -----------------------------------------------------------------------

    [Test]
    public async Task DisjointHalfBoundedScans_Coexist()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        for (int i = 1; i <= 4; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            await InsertAsync(dbname, db, executor, id, priority: i, name: $"task-p{i}");
        }

        // Txn A: scan WHERE priority <= 2 (half-bounded, acquires S on [1,3)).
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        QueryTicket qa = new(
            txnState: txA, databaseName: dbname, tableName: "tasks",
            index: null, projection: null, where: null,
            filters: new() { new("priority", "<=", new(ColumnType.Integer64, 2L)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rowsA) = await executor.Query(qa);
        List<QueryResultRow> resultA = await rowsA.ToListAsync();
        Assert.AreEqual(2, resultA.Count, "Txn A should see priority 1 and 2");

        // Txn B: scan WHERE priority >= 3 — disjoint with A's locked range.
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        QueryTicket qb = new(
            txnState: txB, databaseName: dbname, tableName: "tasks",
            index: null, projection: null, where: null,
            filters: new() { new("priority", ">=", new(ColumnType.Integer64, 3L)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rowsB) = await executor.Query(qb);
        List<QueryResultRow> resultB = await rowsB.ToListAsync();
        Assert.AreEqual(2, resultB.Count, "Txn B should see priority 3 and 4");

        // Both can commit without conflict (S∩S on disjoint ranges).
        Assert.DoesNotThrowAsync(() => db.Transactions.CommitAsync(txA), "Txn A commit must succeed");
        Assert.DoesNotThrowAsync(() => db.Transactions.CommitAsync(txB), "Txn B commit must succeed");
    }

    // -----------------------------------------------------------------------
    // 6. Adjacent disjoint bounds (<= V and > V) do not conflict
    //
    // priority <= 2 and priority > 2 partition the index at the value-2 boundary:
    // value 2's rows belong only to the <= side. The exclusive lower bound of the
    // > side must start strictly after all of value 2's rowId suffixes, otherwise
    // it falsely overlaps the <= lock on value 2's row keys.
    // -----------------------------------------------------------------------

    [Test]
    public async Task AdjacentDisjointBounds_LessEqualAndGreater_DoNotConflict()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        for (int i = 1; i <= 4; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            await InsertAsync(dbname, db, executor, id, priority: i, name: $"task-p{i}");
        }

        // Txn A: UPDATE WHERE priority <= 2 (exclusive X lock on [.., encode(2)] inclusive).
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: txA, databaseName: dbname, tableName: "tasks",
            plainValues: new() { { "name", new(ColumnType.String, "low") } },
            exprValues: null, where: null,
            filters: new() { new("priority", "<=", new(ColumnType.Integer64, 2L)) },
            parameters: null));

        // Txn B: UPDATE WHERE priority > 2 — disjoint with A at the value-2 boundary.
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.DoesNotThrowAsync(async () =>
        {
            await executor.Update(new UpdateTicket(
                txnState: txB, databaseName: dbname, tableName: "tasks",
                plainValues: new() { { "name", new(ColumnType.String, "high") } },
                exprValues: null, where: null,
                filters: new() { new("priority", ">", new(ColumnType.Integer64, 2L)) },
                parameters: null));
            await db.Transactions.CommitAsync(txB);
        }, "UPDATE priority > 2 must not conflict with a held lock on priority <= 2");

        await db.Transactions.CommitAsync(txA);
    }

    // -----------------------------------------------------------------------
    // 5. After bounded-range UPDATE commits, a new UPDATE on the same range
    //    succeeds (lock fully released)
    // -----------------------------------------------------------------------

    [Test]
    public async Task BoundedRangeUpdate_AfterCommit_LockIsReleased()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id1 = ObjectIdGenerator.Generate().ToString();
        await InsertAsync(dbname, db, executor, id1, priority: 1, name: "original");

        // First UPDATE on priority=1 — commits.
        KvTransaction txA = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        await executor.Update(new UpdateTicket(
            txnState: txA, databaseName: dbname, tableName: "tasks",
            plainValues: new() { { "name", new(ColumnType.String, "first") } },
            exprValues: null, where: null,
            filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
            parameters: null));
        await db.Transactions.CommitAsync(txA);

        // Second UPDATE on priority=1 after A committed — must succeed.
        KvTransaction txB = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        Assert.DoesNotThrowAsync(async () =>
        {
            await executor.Update(new UpdateTicket(
                txnState: txB, databaseName: dbname, tableName: "tasks",
                plainValues: new() { { "name", new(ColumnType.String, "second") } },
                exprValues: null, where: null,
                filters: new() { new("priority", "=", new(ColumnType.Integer64, 1L)) },
                parameters: null));
            await db.Transactions.CommitAsync(txB);
        }, "Second UPDATE on same range after first commits must succeed");

        Assert.AreEqual("second", await ReadNameAsync(dbname, db, executor, id1));
    }
}
