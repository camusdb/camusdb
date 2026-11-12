
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A read through a JOIN, a semi-join or a correlated subquery must leave the Serializable read-write
/// locks the same read by primary key leaves: the shared point lock on every row and index entry it
/// read, and the shared predicate lock on every range it scanned. The shape is the Caraxes rw-register
/// join arm: each register row of <c>elle_rw_i64</c> is reached through a reference row of
/// <c>elle_rw_i64_ref</c>. When the finding was made the planner read the reference table with a
/// table-scan leaf, because a join leaf resolved no user parameter, and no join leaf took a predicate
/// lock: a key read as absent through the join could be inserted and committed by another transaction,
/// and a second read of the same key in the same transaction saw it. Elle reported lost updates,
/// non-repeatable reads and G2 cycles. The leaf is now the one-key index scan the single-table read
/// uses; the table-scan shape stays under test through a column no index serves.
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestJoinSerializableLocks : SharedNodeBaseTest
{
    private const string JoinRead = "SELECT r.v FROM elle_rw_i64_ref j JOIN elle_rw_i64 r ON r.k = j.target WHERE j.k = @k";

    private const string JoinReadBySecondary = "SELECT r.v FROM elle_rw_i64_ref j JOIN elle_rw_i64 r ON r.k3 = j.target WHERE j.k = @k";

    /// <summary>The same join reached through a column no index serves, so the reference leaf is a table scan.</summary>
    private const string JoinReadThroughTableScan = "SELECT r.v FROM elle_rw_i64_ref j JOIN elle_rw_i64 r ON r.k = j.target WHERE j.target = @k";

    private const string SemiJoinRead = "SELECT v FROM elle_rw_i64 WHERE k IN (SELECT target FROM elle_rw_i64_ref)";

    private const string ExistsRead = "SELECT v FROM elle_rw_i64 r WHERE EXISTS (SELECT 1 FROM elle_rw_i64_ref j WHERE j.target = r.k)";

    private const int Rows = 20;

    private readonly KeyValueTransactionLocking locking;

    public TestJoinSerializableLocks(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        // This fixture describes the locking form of Serializable reads: a shared lock on each key read,
        // upgraded at the write. It states the setting so that it holds whatever the default is; the form
        // that proves single-key reads at commit is covered by TestSerializableOptimisticPointReads.
        => defaults with { DefaultTransactionLocking = locking, SerializableOptimisticPointLocks = true };

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// The two tables of the rw-register join arm, with register rows 0..19 (v = 100 + k) and one
    /// reference row per register row (target = k).
    /// </summary>
    private async Task<Fixture> Setup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Fixture fixture = new(dbname, database, executor);

        await NonQuery(fixture, "CREATE TABLE elle_rw_i64 (k INT64 PRIMARY KEY, k2 INT64 NOT NULL, k3 INT64 NOT NULL, v INT64 NOT NULL)");
        await NonQuery(fixture, "CREATE INDEX elle_rw_i64_k3 ON elle_rw_i64 (k3)");
        await NonQuery(fixture, "CREATE TABLE elle_rw_i64_ref (k INT64 PRIMARY KEY, target INT64 NOT NULL)");

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int k = 0; k < Rows; k++)
        {
            await UpsertRegister(fixture, txn, k, 100 + k);
            await UpsertRef(fixture, txn, k);
        }
        await database.Transactions.CommitAsync(txn);

        return fixture;
    }

    private static Dictionary<string, ColumnValue> Key(long k) => new() { { "@k", new ColumnValue(ColumnType.Integer64, k) } };

    /// <summary>The register write of the workload: a blind UPSERT of (k, k2 = k, k3 = k, v).</summary>
    private static Task UpsertRegister(Fixture fixture, KvTransaction txn, long k, long v)
    {
        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@k", new ColumnValue(ColumnType.Integer64, k) },
            { "@v", new ColumnValue(ColumnType.Integer64, v) },
        };

        return fixture.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txn, fixture.DbName,
            "UPSERT INTO elle_rw_i64 (k, k2, k3, v) VALUES (@k, @k, @k, @v)", parameters));
    }

    /// <summary>The reference write of the workload: a blind UPSERT of (k, target = k).</summary>
    private static Task UpsertRef(Fixture fixture, KvTransaction txn, long k)
        => fixture.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txn, fixture.DbName,
            "UPSERT INTO elle_rw_i64_ref (k, target) VALUES (@k, @k)", Key(k)));

    private static async Task<List<QueryResultRow>> Run(Fixture fixture, string sql, KvTransaction txn, Dictionary<string, ColumnValue>? parameters = null)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: fixture.DbName, sql: sql, parameters: parameters));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static async Task NonQuery(Fixture fixture, string sql)
    {
        KvTransaction txn = await fixture.Database.Transactions.BeginAsync();
        await fixture.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txn, fixture.DbName, sql, null));
        await fixture.Database.Transactions.CommitAsync(txn);
    }

    private static async Task<long?> ReadThroughJoin(Fixture fixture, KvTransaction txn, long k, string sql = JoinRead)
    {
        List<QueryResultRow> rows = await Run(fixture, sql, txn, Key(k));
        Assert.That(rows.Count, Is.LessThanOrEqualTo(1));
        return rows.Count == 0 ? null : rows[0].Row["v"].LongValue;
    }

    private static async Task<KvTableStore> StoreOf(Fixture fixture, string table)
        => (await fixture.Executor.OpenTable(new OpenTableTicket(fixture.DbName, table))).Store;

    private static Task<KvTransaction> BeginSerializableReadWrite(Fixture fixture)
        => fixture.Database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

    /// <summary>
    /// Writes with <paramref name="write"/> in a fresh transaction and commits it. The write or the
    /// commit must be refused because a Serializable read-write reader holds the lock that fences it.
    /// </summary>
    private static async Task AssertWriteIsFenced(Fixture fixture, System.Func<KvTransaction, Task> write, string because)
    {
        KvTransaction writer = await fixture.Database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await write(writer);
            await fixture.Database.Transactions.CommitAsync(writer);
        }, because);

        Assert.That(ex?.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry).Or.EqualTo(CamusDBErrorCodes.TransactionConflict), because);

        await fixture.Database.Transactions.RollbackIfNotCompletedAsync(writer);
    }

    /// <summary>Writes with <paramref name="write"/> in a fresh transaction and commits it; it must succeed.</summary>
    private static async Task AssertWriteSucceeds(Fixture fixture, System.Func<KvTransaction, Task> write, string because)
    {
        KvTransaction writer = await fixture.Database.Transactions.BeginAsync();

        Assert.DoesNotThrowAsync(async () =>
        {
            await write(writer);
            await fixture.Database.Transactions.CommitAsync(writer);
        }, because);
    }

    // -----------------------------------------------------------------------
    // The shapes under test: the parameterized primary-key filter makes the reference leaf a one-key
    // index scan, a filter on a column no index serves makes it a table scan, and the register side
    // is a unique probe of the primary key in both.
    // -----------------------------------------------------------------------

    [Test]
    public async Task JoinRead_PlanIsIndexLeafWithUniqueProbe()
    {
        Fixture fixture = await Setup();

        KvTransaction txn = await fixture.Database.Transactions.BeginAsync();
        List<string> byKey = (await Run(fixture, "EXPLAIN " + JoinRead, txn, Key(0))).Select(r => r.Row["node"].StrValue!).ToList();
        List<string> byTarget = (await Run(fixture, "EXPLAIN " + JoinReadThroughTableScan, txn, Key(0))).Select(r => r.Row["node"].StrValue!).ToList();
        await fixture.Database.Transactions.CommitAsync(txn);

        Assert.That(byKey, Does.Contain("index-nested-loop-join"), "the register side is probed through its primary key");
        Assert.That(byKey, Does.Contain("index-range-scan"), "a parameterized equality on the reference primary key is a one-key index leaf");
        Assert.That(byKey, Does.Not.Contain("table-scan"), "the reference table is not scanned for a key its index serves");

        Assert.That(byTarget, Does.Contain("index-nested-loop-join"));
        Assert.That(byTarget, Does.Contain("table-scan"), "a filter on a column no index serves is a table-scan leaf");
    }

    // -----------------------------------------------------------------------
    // Lost update through the join: both readers hold the shared point locks of the register row, so
    // the first writer's upgrade is refused while the other reader lives.
    // -----------------------------------------------------------------------

    [Test]
    public async Task JoinRead_PresentRow_SecondWriterIsRefused()
    {
        Fixture fixture = await Setup();

        KvTransaction txA = await BeginSerializableReadWrite(fixture);
        KvTransaction txB = await BeginSerializableReadWrite(fixture);

        Assert.That(await ReadThroughJoin(fixture, txA, 0), Is.EqualTo(100L));
        Assert.That(await ReadThroughJoin(fixture, txB, 0), Is.EqualTo(100L));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => UpsertRegister(fixture, txA, 0, 35));
        Assert.That(ex?.Code, Is.EqualTo(CamusDBErrorCodes.TransactionConflict),
            "the other reader holds a shared lock from its join read, so the writer's upgrade must be refused");

        await fixture.Database.Transactions.RollbackAsync(txA);

        await UpsertRegister(fixture, txB, 0, 29);
        await fixture.Database.Transactions.CommitAsync(txB);

        KvTransaction verify = await fixture.Database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        Assert.That(await ReadThroughJoin(fixture, verify, 0), Is.EqualTo(29L));
        await fixture.Database.Transactions.CommitAsync(verify);
    }

    // -----------------------------------------------------------------------
    // Phantom through the join: the key is absent, so no row lock exists. The leaf's predicate lock
    // is what fences the insert: the one-key index lock of the index leaf, or the whole-table lock of
    // the table-scan leaf.
    // -----------------------------------------------------------------------

    [Test]
    public async Task JoinRead_AbsentRow_ConcurrentInsertIsFenced()
        => await AssertAbsentRowIsFenced(JoinRead);

    [Test]
    public async Task JoinReadThroughTableScan_AbsentRow_ConcurrentInsertIsFenced()
        => await AssertAbsentRowIsFenced(JoinReadThroughTableScan);

    private async Task AssertAbsentRowIsFenced(string sql)
    {
        Fixture fixture = await Setup();

        KvTransaction txA = await BeginSerializableReadWrite(fixture);
        Assert.That(await ReadThroughJoin(fixture, txA, 73, sql), Is.Null);

        await AssertWriteIsFenced(fixture, async writer =>
        {
            await UpsertRegister(fixture, writer, 73, 1115);
            await UpsertRef(fixture, writer, 73);
        }, "an insert of a key a Serializable read-write transaction read as absent through a join must be held back");

        Assert.That(await ReadThroughJoin(fixture, txA, 73, sql), Is.Null, "the second read in the same transaction must still see the key as absent");
        await fixture.Database.Transactions.CommitAsync(txA);

        await AssertWriteSucceeds(fixture, async writer =>
        {
            await UpsertRegister(fixture, writer, 73, 1115);
            await UpsertRef(fixture, writer, 73);
        }, "once the reader has committed the insert must succeed");
    }

    // -----------------------------------------------------------------------
    // The lock set a join read leaves is the one the equivalent single-table reads leave: through the
    // index leaf, one lock on the reference index key and one point lock on each row read; through the
    // table-scan leaf, the whole reference row range; and nothing at all outside Serializable
    // read-write.
    // -----------------------------------------------------------------------

    [Test]
    public async Task JoinRead_LeavesTheSingleTableLockSet()
    {
        Fixture fixture = await Setup();
        TableDescriptor refTable = await fixture.Executor.OpenTable(new OpenTableTicket(fixture.DbName, "elle_rw_i64_ref"));
        KvTableStore refStore = refTable.Store;
        KvTableStore registerStore = await StoreOf(fixture, "elle_rw_i64");
        string refPrimaryIndex = refStore.IndexKeySpace(refTable.Indexes[CamusDBConstants.PrimaryKeyInternalName].KvId);

        KvTransaction txn = await BeginSerializableReadWrite(fixture);
        Assert.That(await ReadThroughJoin(fixture, txn, 3), Is.EqualTo(103L));

        Assert.That(txn.HasWholeBucketLock(refStore.RowKeySpace), Is.False, "the index leaf never locks the whole reference table");
        Assert.That(txn.CountRangeLocksForBucket(refPrimaryIndex), Is.EqualTo(1), "the index leaf must hold exactly one lock on the reference key it read");
        Assert.That(txn.CountPointLocksForBucket(refStore.RowKeySpace), Is.EqualTo(1), "the index leaf must hold exactly one point lock on the reference row it read");
        Assert.That(txn.CountPointLocksForBucket(registerStore.RowKeySpace), Is.EqualTo(1), "the probe must hold exactly one point lock on the register row it read");
        Assert.That(txn.HasWholeBucketLock(registerStore.RowKeySpace), Is.False, "a unique probe never locks the whole register table");

        await fixture.Database.Transactions.CommitAsync(txn);

        KvTransaction scanning = await BeginSerializableReadWrite(fixture);
        Assert.That(await ReadThroughJoin(fixture, scanning, 3, JoinReadThroughTableScan), Is.EqualTo(103L));
        Assert.That(scanning.HasWholeBucketLock(refStore.RowKeySpace), Is.True, "the table-scan leaf must hold the shared lock over the whole reference row range");
        Assert.That(scanning.CountPointLocksForBucket(registerStore.RowKeySpace), Is.EqualTo(1), "the probe must hold exactly one point lock on the register row it read");
        await fixture.Database.Transactions.CommitAsync(scanning);

        KvTransaction readCommitted = await fixture.Database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        Assert.That(await ReadThroughJoin(fixture, readCommitted, 3), Is.EqualTo(103L));
        Assert.That(readCommitted.GetAcquiredRangeLocks(), Is.Empty, "Read Committed takes no lock through a join");
        await fixture.Database.Transactions.CommitAsync(readCommitted);

        KvTransaction snapshot = await fixture.Database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        Assert.That(await ReadThroughJoin(fixture, snapshot, 3), Is.EqualTo(103L));
        Assert.That(snapshot.GetAcquiredRangeLocks(), Is.Empty, "a Serializable read-only snapshot takes no lock through a join");
        await fixture.Database.Transactions.CommitAsync(snapshot);
    }

    // -----------------------------------------------------------------------
    // Phantom through a non-unique probe: the reference row exists, the register row with k3 = 73
    // does not, so the probe of the k3 index finds nothing and no row lock exists. The equality lock
    // the probe takes on the index key is what fences the insert of a register row with that k3.
    // -----------------------------------------------------------------------

    [Test]
    public async Task NonUniqueProbe_AbsentKey_ConcurrentInsertIsFenced()
    {
        Fixture fixture = await Setup();
        fixture.Executor.Statistics.ForceIndexNestedLoopForTesting = true;

        KvTransaction prepare = await fixture.Database.Transactions.BeginAsync();
        await UpsertRef(fixture, prepare, 73);
        await fixture.Database.Transactions.CommitAsync(prepare);

        KvTransaction explain = await fixture.Database.Transactions.BeginAsync();
        List<string> plan = (await Run(fixture, "EXPLAIN " + JoinReadBySecondary, explain, Key(73))).Select(r => r.Row["node"] + " " + r.Row["detail"].StrValue).ToList();
        await fixture.Database.Transactions.CommitAsync(explain);
        Assert.That(plan.Any(line => line.Contains("elle_rw_i64_k3")), Is.True, "the register side must be probed through the non-unique k3 index: " + string.Join(" / ", plan));

        KvTransaction txA = await BeginSerializableReadWrite(fixture);
        Assert.That(await ReadThroughJoin(fixture, txA, 73, JoinReadBySecondary), Is.Null);

        await AssertWriteIsFenced(fixture, writer => UpsertRegister(fixture, writer, 73, 1115),
            "an insert of an index key a Serializable read-write transaction probed as absent must be held back");

        Assert.That(await ReadThroughJoin(fixture, txA, 73, JoinReadBySecondary), Is.Null, "the second probe in the same transaction must still find no entry");
        await fixture.Database.Transactions.CommitAsync(txA);

        await AssertWriteSucceeds(fixture, writer => UpsertRegister(fixture, writer, 73, 1115), "once the reader has committed the insert must succeed");
    }

    // -----------------------------------------------------------------------
    // IN (SELECT …) and EXISTS: the sub-read of the reference table is fenced like a scan of it.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SemiJoinRead_ConcurrentInsertIsFenced()
    {
        Fixture fixture = await Setup();

        KvTransaction txA = await BeginSerializableReadWrite(fixture);
        Assert.That((await Run(fixture, SemiJoinRead, txA)).Count, Is.EqualTo(Rows));

        await AssertWriteIsFenced(fixture, async writer =>
        {
            await UpsertRegister(fixture, writer, 73, 1115);
            await UpsertRef(fixture, writer, 73);
        }, "a row that would join the IN (SELECT …) result of a Serializable read-write transaction must be held back");

        Assert.That((await Run(fixture, SemiJoinRead, txA)).Count, Is.EqualTo(Rows), "the second read in the same transaction must see the same rows");
        await fixture.Database.Transactions.CommitAsync(txA);
    }

    [Test]
    public async Task ExistsRead_ConcurrentInsertIsFenced()
    {
        Fixture fixture = await Setup();

        KvTransaction txA = await BeginSerializableReadWrite(fixture);
        Assert.That((await Run(fixture, ExistsRead, txA)).Count, Is.EqualTo(Rows));

        await AssertWriteIsFenced(fixture, async writer =>
        {
            await UpsertRegister(fixture, writer, 73, 1115);
            await UpsertRef(fixture, writer, 73);
        }, "a row that would satisfy the EXISTS of a Serializable read-write transaction must be held back");

        Assert.That((await Run(fixture, ExistsRead, txA)).Count, Is.EqualTo(Rows), "the second read in the same transaction must see the same rows");
        await fixture.Database.Transactions.CommitAsync(txA);
    }
}
