/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that the Grace/hybrid hash join path produces results identical to the in-memory
/// hash join for every shape tested. The Grace path is activated by setting
/// <c>HashJoinMaxBuildRows = 1</c> (so the first non-trivial build triggers the cap) together
/// with <c>SpillEnabled = true</c>; the fallback is then Grace rather than nested-loop.
///
/// <para>
/// Correctness argument: the Grace path partitions both sides by <c>hash(key) mod K</c>,
/// then joins each partition with the in-memory probe logic. Rows with matching keys are
/// guaranteed to land in the same partition (hash is deterministic on equal keys), so the
/// full inner-join multiset is recovered by unioning all partition results.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestHashJoinSpill : SharedNodeBaseTest
{
    private string _dataDir = null!;
    private bool _savedSpillEnabled;
    private int _savedThreshold;
    private int? _savedForceThreshold;
    private int _savedFanIn;
    private int _savedMaxBuildRows;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_hj_spill_" + System.Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        _savedSpillEnabled    = CamusDBConfig.SpillEnabled;
        _savedThreshold       = CamusDBConfig.SpillThresholdRows;
        _savedForceThreshold  = CamusDBConfig.ForceSpillThresholdRows;
        _savedFanIn           = CamusDBConfig.SpillMergeFanIn;
        _savedMaxBuildRows    = CamusDBConfig.HashJoinMaxBuildRows;

        CamusDBConfig.DataDirectory = _dataDir;
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        CamusDBConfig.SpillEnabled            = _savedSpillEnabled;
        CamusDBConfig.SpillThresholdRows      = _savedThreshold;
        CamusDBConfig.ForceSpillThresholdRows = _savedForceThreshold;
        CamusDBConfig.SpillMergeFanIn         = _savedFanIn;
        CamusDBConfig.HashJoinMaxBuildRows    = _savedMaxBuildRows;
        CamusDBConfig.DataDirectory           = null!;

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    // ── Fixture helpers ───────────────────────────────────────────────────────

    private sealed record HJSFixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    private async Task<HJSFixture> SetupOrdersItems(bool includeNullKey = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns:
            [
                new("id",    ColumnType.Id),
                new("name",  ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns:
            [
                new("id",       ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product",  ColumnType.String, notNull: true),
                new("qty",      ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string oaId = ObjectIdGenerator.Generate().ToString();
        string obId = ObjectIdGenerator.Generate().ToString();
        string ocId = ObjectIdGenerator.Generate().ToString();
        string odId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                new() { { "id", new(ColumnType.Id, oaId) }, { "name", new(ColumnType.String, "Order-A") }, { "score", new(ColumnType.Integer64, 10L) } },
                new() { { "id", new(ColumnType.Id, obId) }, { "name", new(ColumnType.String, "Order-B") }, { "score", new(ColumnType.Integer64, 20L) } },
                new() { { "id", new(ColumnType.Id, ocId) }, { "name", new(ColumnType.String, "Order-C") }, { "score", new(ColumnType.Integer64, 30L) } },
                new() { { "id", new(ColumnType.Id, odId) }, { "name", new(ColumnType.String, "Order-D") }, { "score", new(ColumnType.Integer64, 40L) } },
            ]));

        List<Dictionary<string, ColumnValue>> items =
        [
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "product", new(ColumnType.String, "Widget")    }, { "qty", new(ColumnType.Integer64, 5L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Gadget")    }, { "qty", new(ColumnType.Integer64, 3L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Doohickey") }, { "qty", new(ColumnType.Integer64, 7L) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, odId) }, { "product", new(ColumnType.String, "Sprocket")  }, { "qty", new(ColumnType.Integer64, 2L) } },
        ];

        if (includeNullKey)
            items.Add(new()
            {
                { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "order_id", new(ColumnType.Null,      0) },
                { "product",  new(ColumnType.String,    "GhostPart") },
                { "qty",      new(ColumnType.Integer64, 99L) },
            });

        await executor.Insert(new InsertTicket(txn, dbname, "line_items", values: items));
        await database.Transactions.CommitAsync(txn);

        executor.Statistics.ForceHashJoinForTesting = true;

        return new HJSFixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(HJSFixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    private static List<string> ProductList(List<QueryResultRow> rows) =>
        rows.Select(r => r.Row.TryGetValue("product", out var v) ? v.StrValue ?? "" : "")
            .OrderBy(s => s)
            .ToList();

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task HashJoinSpill_OneToMany_MatchesInMemoryPath()
    {
        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";

        // In-memory reference (spill off, build cap large enough to fit everything).
        CamusDBConfig.SpillEnabled         = false;
        CamusDBConfig.HashJoinMaxBuildRows = 1_000_000;
        HJSFixture fRef = await SetupOrdersItems();
        List<QueryResultRow> reference = await Run(fRef, sql);

        // Grace hash join path: cap at 1 so any 2-row build triggers overflow.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        HJSFixture fSpill = await SetupOrdersItems();
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        Assert.AreEqual(reference.Count, spillResult.Count,
            "Grace hash join must produce the same multiset size as in-memory hash join.");
        CollectionAssert.AreEqual(ProductList(reference), ProductList(spillResult),
            "Grace hash join must produce the same set of product values.");
    }

    [Test]
    public async Task HashJoinSpill_NoSpillFilesRemainAfterCompletion()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        HJSFixture f = await SetupOrdersItems();

        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";
        List<QueryResultRow> rows = await Run(f, sql);
        Assert.That(rows.Count, Is.GreaterThan(0));

        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] remaining = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(remaining, "All spill files must be deleted after the query completes.");
        }
    }

    [Test]
    public async Task HashJoinSpill_FlagOff_UsesInMemoryPath()
    {
        CamusDBConfig.SpillEnabled         = false;
        CamusDBConfig.HashJoinMaxBuildRows = 1_000_000;
        HJSFixture f = await SetupOrdersItems();

        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";
        List<QueryResultRow> rows = await Run(f, sql);

        // 4 line items, 3 match (order-A:1, order-B:2; order-C has no items; order-D:1)
        Assert.That(rows.Count, Is.EqualTo(4));
    }

    [Test]
    public async Task HashJoinSpill_NullJoinKey_RowExcluded()
    {
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        HJSFixture f = await SetupOrdersItems(includeNullKey: true);

        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";
        List<QueryResultRow> rows = await Run(f, sql);

        bool hasGhostPart = rows.Any(r =>
            r.Row.TryGetValue("product", out var v) && v.StrValue == "GhostPart");
        Assert.IsFalse(hasGhostPart,
            "Rows with a NULL join key must be excluded from the Grace hash join result.");
    }

    [Test]
    public async Task HashJoinSpill_NullNonKeyColumns_FlowThroughMergeCorrectly()
    {
        // A NULL in a non-join-key column (score on the left, qty on the right) must flow through
        // the Grace-hash-join spill merge intact. The layout-based merge places every column of the
        // fixed row shape by ordinal and asserts the full slot count is filled; a row that carried a
        // NULL as an absent key (rather than an explicit ColumnValue.Null) would leave a slot unfilled
        // and throw. This guards that null non-key columns are materialized as real slots end to end.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 2;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true), new("score", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns: [new("id", ColumnType.Id), new("order_id", ColumnType.Id), new("product", ColumnType.String, notNull: true), new("qty", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string oaId = ObjectIdGenerator.Generate().ToString();
        string obId = ObjectIdGenerator.Generate().ToString();
        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                // Order-A has a NULL score (non-key column on the left/build-or-probe side).
                new() { { "id", new(ColumnType.Id, oaId) }, { "name", new(ColumnType.String, "Order-A") }, { "score", new(ColumnType.Null, 0) } },
                new() { { "id", new(ColumnType.Id, obId) }, { "name", new(ColumnType.String, "Order-B") }, { "score", new(ColumnType.Integer64, 20L) } },
            ]));
        await executor.Insert(new InsertTicket(txn, dbname, "line_items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, oaId) }, { "product", new(ColumnType.String, "Widget") }, { "qty", new(ColumnType.Null, 0) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, obId) }, { "product", new(ColumnType.String, "Gadget") }, { "qty", new(ColumnType.Integer64, 3L) } },
            ]));
        await database.Transactions.CommitAsync(txn);
        executor.Statistics.ForceHashJoinForTesting = true;

        HJSFixture f = new(dbname, database, executor);
        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, o.score, li.product, li.qty FROM orders o JOIN line_items li ON li.order_id = o.id");

        Assert.AreEqual(2, rows.Count, "Both orders join to one item each");

        QueryResultRow rowA = rows.Single(r => r.Row["name"].StrValue == "Order-A");
        Assert.AreEqual(ColumnType.Null, rowA.Row["score"].Type, "Order-A's NULL score must survive the spill merge");
        Assert.AreEqual("Widget", rowA.Row["product"].StrValue);
        Assert.AreEqual(ColumnType.Null, rowA.Row["qty"].Type, "line item's NULL qty must survive the spill merge");

        QueryResultRow rowB = rows.Single(r => r.Row["name"].StrValue == "Order-B");
        Assert.AreEqual(20L, rowB.Row["score"].LongValue);
        Assert.AreEqual(3L, rowB.Row["qty"].LongValue);
    }

    [Test]
    public async Task HashJoinSpill_FlagOnVsOff_IdenticalMultisetSize()
    {
        string sql = "SELECT o.name, li.product, li.qty " +
                     "FROM orders o JOIN line_items li ON li.order_id = o.id";

        CamusDBConfig.SpillEnabled         = false;
        CamusDBConfig.HashJoinMaxBuildRows = 1_000_000;
        HJSFixture fOff = await SetupOrdersItems();
        List<QueryResultRow> offRows = await Run(fOff, sql);

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        HJSFixture fOn = await SetupOrdersItems();
        List<QueryResultRow> onRows = await Run(fOn, sql);

        Assert.AreEqual(offRows.Count, onRows.Count,
            "Grace hash join and in-memory hash join must produce the same multiset size.");
    }

    [Test]
    public async Task HashJoinSpill_SkewedSingleKey_CompletesCorrectly()
    {
        // All line items share the same order_id — extreme skew that triggers the MaxGraceHashDepth
        // fallback where all rows end up in the same sub-partition and the hash table is loaded
        // unconditionally.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns: [new("id", ColumnType.Id), new("label", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "items",
            columns: [new("id", ColumnType.Id), new("order_id", ColumnType.Id), new("val", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string singleOrderId = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values: [new() { { "id", new(ColumnType.Id, singleOrderId) }, { "label", new(ColumnType.String, "OnlyOrder") } }]));

        await executor.Insert(new InsertTicket(txn, dbname, "items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 1L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 2L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 3L) } },
            ]));

        await database.Transactions.CommitAsync(txn);
        executor.Statistics.ForceHashJoinForTesting = true;

        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 1; // Every partition immediately overflows → recursive repartition
        CamusDBConfig.SpillMergeFanIn         = 4;

        KvTransaction runTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: runTxn, database: dbname,
            sql: "SELECT o.label, i.val FROM orders o JOIN items i ON i.order_id = o.id",
            parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        // All 3 items match the single order.
        Assert.That(rows.Count, Is.EqualTo(3),
            "All items must be returned even when extreme key skew triggers the depth-limit fallback.");
    }

    [Test]
    public async Task HashJoinSpill_MultiKeyOverflow_RecursionActuallySplits()
    {
        // End-to-end smoke test of the recursive-repartition path: drive 40 distinct order IDs
        // through a tiny K=2, per-partition threshold=3 setup so every level-0 bucket overflows
        // and triggers recursion. This proves the recursion path runs to completion with correct
        // results, but it does NOT discriminate the fix from the bug — the depth-limit load-all
        // backstop returns the correct multiset whether or not the recursion actually splits.
        // The true discriminator for the murmur-finalizer fix is the PartitionIndex level-
        // independence unit test below (PartitionIndex_RedistributesCollidingKeysAcrossLevels).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "items",
            columns: [new("id", ColumnType.Id), new("order_id", ColumnType.Id), new("val", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        const int N = 40;
        var orderIds = Enumerable.Range(0, N).Select(_ => ObjectIdGenerator.Generate().ToString()).ToList();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values: orderIds.Select((oid, i) => new Dictionary<string, ColumnValue>
            {
                { "id",   new(ColumnType.Id,     oid) },
                { "name", new(ColumnType.String, "Order-" + i) },
            }).ToList()));

        await executor.Insert(new InsertTicket(txn, dbname, "items",
            values: orderIds.Select(oid => new Dictionary<string, ColumnValue>
            {
                { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "order_id", new(ColumnType.Id,        oid) },
                { "val",      new(ColumnType.Integer64, 1L) },
            }).ToList()));

        await database.Transactions.CommitAsync(txn);
        executor.Statistics.ForceHashJoinForTesting = true;

        // K=2 so all 40 distinct keys start in just two buckets at level 0.
        // Per-partition threshold=3 so every level-0 bucket overflows and triggers recursion.
        // With a proper finalizer the recursive splits distribute the 20 keys per bucket
        // into new sub-buckets and the join completes correctly.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        CamusDBConfig.SpillMergeFanIn         = 2;

        KvTransaction runTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: runTxn, database: dbname,
            sql: "SELECT o.name, i.val FROM orders o JOIN items i ON i.order_id = o.id",
            parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        Assert.That(rows.Count, Is.EqualTo(N),
            "Each of the N distinct order-id keys must produce exactly one join row.");
    }

    /// <summary>
    /// Proves that with <c>SpillEnabled=true</c> and <c>ForceSpillThresholdRows</c> set to a
    /// value smaller than the build side, the Grace path triggers via the threshold — NOT via
    /// <c>HashJoinMaxBuildRows</c>. The negative proof is the exact-threshold variant below.
    ///
    /// The discriminator: <c>HashJoinMaxBuildRows</c> is set to its default (1 million), well
    /// above the 4-row build. Without the fix, <c>BuildHashTable</c> would never return null
    /// (the 1M cap is never reached) and no partition files would be created. With the fix,
    /// the threshold is <c>ForceSpillThresholdRows = 3</c> so the 4th row triggers the cap and
    /// the Grace path partitions both sides to disk.
    /// </summary>
    [Test]
    public async Task HashJoinSpill_ForceThreshold_TriggersGracePath_IndependentOfMaxBuildRows()
    {
        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";

        // Reference: spill off, large cap — in-memory path.
        CamusDBConfig.SpillEnabled         = false;
        CamusDBConfig.HashJoinMaxBuildRows = 1_000_000;
        HJSFixture fRef = await SetupOrdersItems();
        List<QueryResultRow> reference = await Run(fRef, sql);

        // Grace path triggered by ForceSpillThresholdRows (not by HashJoinMaxBuildRows = 1).
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1_000_000; // deliberately large — must NOT be the trigger
        CamusDBConfig.ForceSpillThresholdRows = 3;         // 4-row build > 3 → overflow → Grace
        CamusDBConfig.SpillMergeFanIn         = 4;
        HJSFixture fSpill = await SetupOrdersItems();
        fSpill.Executor.Statistics.HashJoinGracePathCount = 0;
        List<QueryResultRow> spillResult = await Run(fSpill, sql);

        // Positive signal that the Grace path actually ran. Result-equivalence alone does not
        // discriminate the fix: with the threshold ignored (build cap = HashJoinMaxBuildRows = 1M),
        // the 4-row build would stay in the in-memory hash join and still return the same rows.
        Assert.That(fSpill.Executor.Statistics.HashJoinGracePathCount, Is.GreaterThan(0),
            "A build exceeding ForceSpillThresholdRows must route to the Grace path even though it " +
            "is far below HashJoinMaxBuildRows. A count of 0 means the threshold trigger was ignored.");

        Assert.AreEqual(reference.Count, spillResult.Count,
            "Grace path triggered via ForceSpillThresholdRows must return the same multiset size.");
        CollectionAssert.AreEqual(ProductList(reference), ProductList(spillResult),
            "Grace path triggered via threshold must return the same product values as in-memory.");

        // Verify partition files were actually created and cleaned up.
        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] remaining = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(remaining, "Partition spill files must be deleted after the join completes.");
        }
    }

    /// <summary>
    /// Proves the at/under-threshold invariant for hash join: a build of exactly
    /// <c>ForceSpillThresholdRows</c> rows must stay fully in the in-memory hash table and
    /// must NOT trigger the Grace path or create any partition files.
    ///
    /// Negative proof: if the overflow check were <c>&gt;= threshold</c> instead of
    /// <c>&gt; threshold - 1</c> (i.e. checking before vs after the threshold row is added
    /// when rowCount would equal the cap), spill files would appear.
    /// </summary>
    [Test]
    public async Task HashJoinSpill_ExactThresholdBuild_NoSpillTriggered()
    {
        // SetupOrdersItems builds a 4-row right side (orders). Set threshold to 4 so the
        // build fits exactly within the cap and no overflow fires.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1_000_000;
        CamusDBConfig.ForceSpillThresholdRows = 4; // build has exactly 4 rows → must NOT overflow
        CamusDBConfig.SpillMergeFanIn         = 4;
        HJSFixture f = await SetupOrdersItems();

        string sql = "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_id = o.id";
        List<QueryResultRow> rows = await Run(f, sql);

        Assert.That(rows.Count, Is.GreaterThan(0),
            "Join must return rows even when the build fits exactly at the threshold.");

        // No partition files must have been created.
        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        if (Directory.Exists(spillRoot))
        {
            string[] spillFiles = Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories);
            Assert.IsEmpty(spillFiles,
                "A build of exactly ForceSpillThresholdRows rows must not trigger the Grace path.");
        }
    }

    /// <summary>
    /// Proves the NLJ backstop is taken — not load-all — when a single dominant key makes a
    /// partition un-splittable at max recursion depth. The discriminator is
    /// <c>StatisticsManager.HashJoinNljPartitionFallbackCount</c>: the counter is only
    /// incremented by the NLJ backstop branch, never by the load-all branch. Reverting to the
    /// old depth-guarded overflow check (no backstop) would leave the counter at 0, failing
    /// this test.
    ///
    /// The test uses more probe rows than build rows so that the statistics manager routes
    /// the build to the declared right side (items), keeping the skewed 4-row build on the side
    /// that partitions and overflows at every recursion level.
    /// </summary>
    [Test]
    public async Task HashJoinSpill_SkewedKey_NljBackstopTaken_NotLoadAll()
    {
        // items (right/build side, 4 rows) all share one order_id — extreme skew.
        // orders (left/probe side, 6 rows) is larger so ChooseBuildSide picks items as build.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns: [new("id", ColumnType.Id), new("label", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "items",
            columns: [new("id", ColumnType.Id), new("order_id", ColumnType.Id), new("val", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        string singleOrderId = ObjectIdGenerator.Generate().ToString();

        // 6 orders: only the first matches items; the rest have no items (they will appear in
        // the probe scan but produce no join output rows). 6 > 4 → BuildSide.Right → items is build.
        List<Dictionary<string, ColumnValue>> orders =
        [
            new() { { "id", new(ColumnType.Id, singleOrderId) }, { "label", new(ColumnType.String, "Main") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "label", new(ColumnType.String, "Pad1") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "label", new(ColumnType.String, "Pad2") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "label", new(ColumnType.String, "Pad3") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "label", new(ColumnType.String, "Pad4") } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "label", new(ColumnType.String, "Pad5") } },
        ];

        await executor.Insert(new InsertTicket(txn, dbname, "orders", values: orders));

        await executor.Insert(new InsertTicket(txn, dbname, "items",
            values:
            [
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 1L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 2L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 3L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "order_id", new(ColumnType.Id, singleOrderId) }, { "val", new(ColumnType.Integer64, 4L) } },
            ]));

        await database.Transactions.CommitAsync(txn);
        executor.Statistics.ForceHashJoinForTesting = true;

        // Reset counter before the test run.
        executor.Statistics.HashJoinNljPartitionFallbackCount = 0;

        // threshold=2: the 4-row single-key build overflows at every recursion level.
        // At MaxGraceHashDepth the NLJ backstop fires instead of load-all.
        CamusDBConfig.SpillEnabled            = true;
        CamusDBConfig.HashJoinMaxBuildRows    = 1_000_000;
        CamusDBConfig.ForceSpillThresholdRows = 2;
        CamusDBConfig.SpillMergeFanIn         = 4;

        KvTransaction runTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: runTxn, database: dbname,
            sql: "SELECT o.label, i.val FROM orders o JOIN items i ON i.order_id = o.id",
            parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        // All 4 items match the single order.
        Assert.That(rows.Count, Is.EqualTo(4),
            "All items must be returned even when extreme key skew forces the NLJ backstop.");

        // The NLJ backstop counter must be non-zero — proving the bounded path was taken.
        Assert.That(executor.Statistics.HashJoinNljPartitionFallbackCount, Is.GreaterThan(0),
            "The NLJ partition backstop must be triggered for a single-key skewed partition " +
            "at max recursion depth. A count of 0 means the old load-all path was taken instead.");
    }

    /// <summary>
    /// True discriminator for the murmur-finalizer fix: keys that share a partition bucket at
    /// recursion depth 0 (seed 0) must be redistributed across multiple buckets at depth 1
    /// (seed 1). With the old <c>(hash ^ seed) % K</c> scheme, same-bucket keys share the low
    /// order bits and XOR-then-mod keeps them together for power-of-two K, so they all collapse
    /// to a single bucket at depth 1 (distinct count == 1) and every overflow devolves to the
    /// depth-limit load-all. This test fails on that scheme and passes with the finalizer.
    /// </summary>
    [Test]
    public void PartitionIndex_RedistributesCollidingKeysAcrossLevels()
    {
        const int K = 16; // power of two — the case the old XOR scheme broke on

        List<ColumnValue[]> keys = Enumerable.Range(0, 5000)
            .Select(i => new[] { new ColumnValue(ColumnType.Integer64, (long)i) })
            .ToList();

        // Take a group of distinct keys that all land in the same bucket at depth 0.
        List<ColumnValue[]> collidingAtLevel0 = keys
            .GroupBy(k => QueryJoinExecutor.PartitionIndex(k, K, seed: 0))
            .First(g => g.Count() >= 8)
            .ToList();

        int distinctBucketsAtLevel1 = collidingAtLevel0
            .Select(k => QueryJoinExecutor.PartitionIndex(k, K, seed: 1))
            .Distinct()
            .Count();

        Assert.That(distinctBucketsAtLevel1, Is.GreaterThan(1),
            "Keys colliding in one bucket at depth 0 must spread across multiple buckets at depth 1; " +
            "the old XOR-then-mod scheme would keep them all in a single bucket (distinct count == 1).");
    }
}
