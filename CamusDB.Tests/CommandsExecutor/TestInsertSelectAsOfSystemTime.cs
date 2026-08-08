
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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Recovering historical data with <c>AS OF SYSTEM TIME</c> in the source of
/// <c>INSERT … SELECT</c> and <c>CREATE TABLE … AS SELECT</c>: the source reads at the requested
/// snapshot while the writes stay live, so a table can be rebuilt as it was before a bad mutation.
/// </summary>
[NonParallelizable]
public sealed class TestInsertSelectAsOfSystemTime : SharedNodeBaseTest
{
    private async Task<(string db, DatabaseDescriptor descriptor, CommandExecutor executor)> SetupOrders()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, customer STRING, total INT64)",
            parameters: null));

        return (dbName, db, executor);
    }

    private static async Task<int> RunNonQuery(
        string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<int> RunDdl(
        string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    private long NowMillis() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

    private async Task<long> SeedAndCaptureSnapshot(string dbName, DatabaseDescriptor db, CommandExecutor executor)
    {
        await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) VALUES " +
            "(gen_id(), \"acme\", 10), (gen_id(), \"acme\", 20), (gen_id(), \"globex\", 30)");

        await Task.Delay(60);
        long snapshotMs = NowMillis();
        await Task.Delay(60);

        return snapshotMs;
    }

    /// <summary>
    /// The headline recovery case: a bad UPDATE, then rebuild the table as it was into a new one. The
    /// recovered table holds the pre-update values and the live table is untouched.
    /// </summary>
    [Test]
    public async Task CreateTableAsSelect_RecoversPreMutationState()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE customer = \"acme\"");

        int recovered = await RunDdl(dbName, db, executor,
            $"CREATE TABLE orders_recovered AS SELECT customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}");

        Assert.AreEqual(3, recovered);

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM orders_recovered");

        CollectionAssert.AreEquivalent(
            new long[] { 10, 20, 30 },
            rows.Select(r => r.Row["total"].LongValue),
            "the recovered totals are the pre-update ones");

        // The live table is untouched by the recovery.
        List<QueryResultRow> live = await RunSelect(dbName, executor, "SELECT * FROM orders");
        Assert.AreEqual(3, live.Count);
        CollectionAssert.AreEquivalent(new long[] { 999, 999, 30 }, live.Select(r => r.Row["total"].LongValue));
    }

    /// <summary>
    /// A snapshot taken before an UPDATE and a DELETE recovers the whole table as it was:
    /// the updated rows come back with their pre-update values, and the deleted row itself comes
    /// back too. Deletes are first-class revisions in the KV store (the tombstone gets its own
    /// revision, preserving the last live value's history record), so a pre-delete snapshot read
    /// resolves the deleted row's live value for as long as revision history is retained.
    /// </summary>
    [Test]
    public async Task DeletedAndUpdatedRowsRecoverAtThePreMutationSnapshot()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE customer = \"acme\"");
        await RunNonQuery(dbName, db, executor, "DELETE FROM orders WHERE customer = \"globex\"");

        await RunDdl(dbName, db, executor,
            $"CREATE TABLE orders_recovered AS SELECT customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}");

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM orders_recovered");

        CollectionAssert.AreEquivalent(
            new long[] { 10, 20, 30 },
            rows.Select(r => r.Row["total"].LongValue),
            "all three rows are recovered with their pre-mutation values, including the deleted one");

        Assert.AreEqual(1, rows.Count(r => r.Row["customer"].StrValue == "globex"),
            "the deleted row is recovered by the pre-delete snapshot");
    }

    /// <summary>
    /// Restoring historical rows back into the live table: the read is historical, the write is live,
    /// and the statement terminates even though source and target are the same table — the snapshot
    /// precedes this statement's own writes, so the scan can never observe them.
    /// </summary>
    [Test]
    [Timeout(120_000)]
    public async Task InsertSelect_RestoresHistoricalRowsIntoTheLiveTable()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE customer = \"acme\"");

        int restored = await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) " +
            $"SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME {snapshotMs} WHERE customer = \"acme\"");

        Assert.AreEqual(2, restored, "the two pre-update acme rows are re-inserted");

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM orders");
        Assert.AreEqual(5, rows.Count, "three live rows plus the two restored ones");
        CollectionAssert.AreEquivalent(
            new long[] { 999, 999, 30, 10, 20 },
            rows.Select(r => r.Row["total"].LongValue));
    }

    /// <summary>
    /// The write side must be live, not historical: a row inserted after the snapshot is still there
    /// once the copy commits. If the whole statement had been rebound onto the snapshot transaction,
    /// the write would have gone somewhere else — or failed as read-only.
    /// </summary>
    [Test]
    public async Task InsertSelect_WritesAreLiveWhileReadsAreHistorical()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE archive (id OBJECT_ID PRIMARY KEY, customer STRING, total INT64)",
            parameters: null));

        // Written after the snapshot: a historical read of `orders` cannot see it, but it must survive
        // in `archive`.
        await RunNonQuery(dbName, db, executor,
            "INSERT INTO archive (id, customer, total) VALUES (gen_id(), \"post-snapshot\", 1)");

        int copied = await RunNonQuery(dbName, db, executor,
            "INSERT INTO archive (id, customer, total) " +
            $"SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}");

        Assert.AreEqual(3, copied);

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM archive");
        Assert.AreEqual(4, rows.Count);
        Assert.AreEqual(1, rows.Count(r => r.Row["customer"].StrValue == "post-snapshot"),
            "the pre-existing live row is still there after the historical copy");
    }

    /// <summary>
    /// The historical source is independent of the writing transaction, so it works inside an explicit
    /// transaction too — which the plain SELECT form refuses, because there it would have to rebind the
    /// caller's own transaction.
    /// </summary>
    [Test]
    public async Task InsertSelect_WorksInsideAnExplicitTransaction()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE total > 0");

        KvTransaction tx = await db.Transactions.BeginAsync();

        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            tx, dbName,
            "INSERT INTO orders (id, customer, total) " +
            $"SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}", null));

        Assert.AreEqual(3, result.ModifiedRows);

        await db.Transactions.RollbackAsync(tx);

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM orders");
        Assert.AreEqual(3, rows.Count, "rolling back the writing transaction discards the recovered rows");
        CollectionAssert.AreEquivalent(new long[] { 999, 999, 999 }, rows.Select(r => r.Row["total"].LongValue));
    }

    /// <summary>A relative offset resolves the same way it does for a plain historical SELECT.</summary>
    [Test]
    public async Task RelativeOffsetSourceIsSupported()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) VALUES (gen_id(), \"acme\", 10)");

        await Task.Delay(1500);
        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE customer = \"acme\"");

        int recovered = await RunDdl(dbName, db, executor,
            "CREATE TABLE recovered AS SELECT customer, total FROM orders AS OF SYSTEM TIME '-1s'");

        Assert.AreEqual(1, recovered);

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM recovered");
        Assert.AreEqual(10, rows[0].Row["total"].LongValue, "the pre-update value is recovered");
    }

    /// <summary>A placeholder-bound snapshot works, so a client can parameterize the recovery point.</summary>
    [Test]
    public async Task ParameterBoundSnapshotIsSupported()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE total > 0");

        KvTransaction tx = await db.Transactions.BeginAsync();
        Dictionary<string, ColumnValue> parameters = new() { ["@ts"] = new ColumnValue(ColumnType.Integer64, snapshotMs) };

        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbName,
            "CREATE TABLE recovered AS SELECT customer, total FROM orders AS OF SYSTEM TIME @ts",
            parameters));

        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(3, result.ModifiedRows);
        CollectionAssert.AreEquivalent(
            new long[] { 10, 20, 30 },
            (await RunSelect(dbName, executor, "SELECT * FROM recovered")).Select(r => r.Row["total"].LongValue));
    }

    /// <summary>
    /// A time-travel copy that reads nothing reports it in the result, not only in the server log — a
    /// remote client cannot see the log, so without this an empty recovery and a successful one look
    /// identical over the wire.
    /// </summary>
    [Test]
    public async Task ZeroRowTimeTravelCopyReportsAWarning()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long beforeAnyRows = NowMillis();
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) VALUES (gen_id(), \"acme\", 10)");

        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbName,
            $"CREATE TABLE nothing_recovered AS SELECT customer FROM orders AS OF SYSTEM TIME {beforeAnyRows}", null));
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(0, ddl.ModifiedRows);
        Assert.IsNotNull(ddl.Warning, "an empty time-travel recovery must say so");
        StringAssert.Contains("reclaimed", ddl.Warning!);
        StringAssert.Contains("nothing_recovered", ddl.Warning!);

        // INSERT ... SELECT reports it the same way.
        KvTransaction tx2 = await db.Transactions.BeginAsync();
        ExecuteNonSQLResult dml = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            tx2, dbName,
            "INSERT INTO orders (id, customer, total) " +
            $"SELECT gen_id(), customer, total FROM orders AS OF SYSTEM TIME {beforeAnyRows}", null));
        await db.Transactions.CommitAsync(tx2);

        Assert.AreEqual(0, dml.ModifiedRows);
        Assert.IsNotNull(dml.Warning);
    }

    /// <summary>A copy that did read rows carries no warning — the signal must stay meaningful.</summary>
    [Test]
    public async Task SuccessfulTimeTravelCopyCarriesNoWarning()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 999 WHERE total > 0");

        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbName,
            $"CREATE TABLE recovered AS SELECT customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}", null));
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(3, ddl.ModifiedRows);
        Assert.IsNull(ddl.Warning);
    }

    /// <summary>A non-time-travel copy that legitimately reads nothing must not be flagged.</summary>
    [Test]
    public async Task ZeroRowLiveCopyCarriesNoWarning()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            tx, dbName, "CREATE TABLE empty_copy AS SELECT customer FROM orders", null));
        await db.Transactions.CommitAsync(tx);

        Assert.AreEqual(0, ddl.ModifiedRows);
        Assert.IsNull(ddl.Warning, "an ordinary empty copy is not suspicious");
    }

    /// <summary>A future snapshot is still refused by the resolver, on this path as on the SELECT one.</summary>
    [Test]
    public async Task FutureSnapshotIsRejected()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        KvTransaction tx = await db.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                tx, dbName, "CREATE TABLE t AS SELECT customer FROM orders AS OF SYSTEM TIME '+10s'", null)));

        Assert.AreEqual(CamusDBErrorCodes.InvalidAsOfSystemTime, ex!.Code);

        await db.Transactions.RollbackAsync(tx);
    }

    /// <summary>
    /// A snapshot from before the table existed reads nothing. That is a legitimate (if unhelpful)
    /// answer rather than an error — the empty table is created and the engine logs why it may be
    /// empty.
    /// </summary>
    [Test]
    public async Task SnapshotBeforeAnyDataProducesAnEmptyTable()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long beforeAnyRows = NowMillis();
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) VALUES (gen_id(), \"acme\", 10)");

        int recovered = await RunDdl(dbName, db, executor,
            $"CREATE TABLE empty_recovery AS SELECT customer, total FROM orders AS OF SYSTEM TIME {beforeAnyRows}");

        Assert.AreEqual(0, recovered);
        Assert.IsEmpty(await RunSelect(dbName, executor, "SELECT * FROM empty_recovery"));
    }

    /// <summary>
    /// The snapshot applies to the whole source query, so an aggregate over history reports the
    /// historical value, not today's.
    /// </summary>
    [Test]
    public async Task AggregateOverHistoryUsesTheSnapshot()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await SetupOrders();

        long snapshotMs = await SeedAndCaptureSnapshot(dbName, db, executor);

        await RunNonQuery(dbName, db, executor, "UPDATE orders SET total = 1000 WHERE total > 0");

        await RunDdl(dbName, db, executor,
            $"CREATE TABLE summed AS SELECT SUM(total) AS total_sum FROM orders AS OF SYSTEM TIME {snapshotMs}");

        List<QueryResultRow> rows = await RunSelect(dbName, executor, "SELECT * FROM summed");
        Assert.AreEqual(60, rows[0].Row["total_sum"].LongValue, "summed at the snapshot (10+20+30), not now (3000)");
    }
}
