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
