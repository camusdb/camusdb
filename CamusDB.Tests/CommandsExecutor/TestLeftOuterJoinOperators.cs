/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The left outer join contract, met by every operator: nested loop, index nested loop over a
/// unique and over a non-unique index, in-memory hash join, the Grace hash join (a low spill
/// threshold), the spill-off nested-loop fallback of the hash join, and the merge join on its
/// streaming and materialized paths. Each case asserts the padded rows, the NULL-key rule and the
/// row count against the same data, so a drift between operators shows up as a failing case rather
/// than a silent difference in rows.
/// </summary>
[NonParallelizable]
public sealed class TestLeftOuterJoinOperators : SharedNodeBaseTest
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_loj_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();
        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    private enum RightIndex { None, Multi, Unique }

    /// <summary>Which operator the planner is forced to, or which fallback the options drive it to.</summary>
    public enum Mode { NestedLoop, IndexNestedLoopUnique, IndexNestedLoopMulti, Hash, Merge, GraceHash, SpillOffNestedLoopFallback }

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// orders(id, name, code) and line_items(id, order_code, product, qty), joined on code.
    ///
    /// orders: A(1) B(2) C(3) D(4) N(NULL).
    /// items (multi): Widget(1), Gadget(2), Doohickey(2), Thing(2), Sprocket(4), GhostPart(NULL).
    /// items (unique): Widget(1), Gadget(2), Sprocket(4), GhostPart(NULL) — one per code.
    /// So A has one match, B has three (one under a unique index), C none, D one, N a NULL key.
    /// </summary>
    private async Task<Fixture> SetupAsync(Mode mode, bool emptyRight = false, bool onlyOrderAHasItems = false)
    {
        RightIndex index = mode switch
        {
            Mode.IndexNestedLoopUnique => RightIndex.Unique,
            Mode.IndexNestedLoopMulti => RightIndex.Multi,
            _ => RightIndex.None,
        };

        CamusDBOptions options = mode switch
        {
            Mode.GraceHash => Options with { SpillEnabled = true, ForceSpillThresholdRows = 2, SpillMergeFanIn = 4, DataDirectory = _dataDir },
            Mode.SpillOffNestedLoopFallback => Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1, DataDirectory = _dataDir },
            _ => Options with { SpillEnabled = false, DataDirectory = _dataDir },
        };

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "orders",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true), new("code", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        ConstraintInfo[] itemConstraints = index switch
        {
            RightIndex.Multi => [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]), new(ConstraintType.IndexMulti, "li_code_idx", [new("order_code", OrderType.Ascending)])],
            RightIndex.Unique => [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]), new(ConstraintType.IndexUnique, "li_code_uidx", [new("order_code", OrderType.Ascending)])],
            _ => [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
        };

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "line_items",
            columns: [new("id", ColumnType.Id), new("order_code", ColumnType.Integer64), new("product", ColumnType.String, notNull: true), new("qty", ColumnType.Integer64)],
            constraints: itemConstraints,
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.Insert(new InsertTicket(txn, dbname, "orders",
            values:
            [
                Order("Order-A", 1L), Order("Order-B", 2L), Order("Order-C", 3L), Order("Order-D", 4L), Order("Order-N", null),
            ]));

        if (!emptyRight)
        {
            List<Dictionary<string, ColumnValue>> items = [Item(1L, "Widget", 5L)];

            if (!onlyOrderAHasItems)
            {
                items.Add(Item(2L, "Gadget", 3L));
                if (index != RightIndex.Unique)
                {
                    items.Add(Item(2L, "Doohickey", 7L));
                    items.Add(Item(2L, "Thing", 1L));
                }
                items.Add(Item(4L, "Sprocket", 2L));
                items.Add(Item(null, "GhostPart", 99L));
            }

            await executor.Insert(new InsertTicket(txn, dbname, "line_items", values: items));
        }

        await database.Transactions.CommitAsync(txn);

        switch (mode)
        {
            case Mode.NestedLoop:
                executor.Statistics.ForceNestedLoopForTesting = true;
                break;
            case Mode.IndexNestedLoopUnique:
            case Mode.IndexNestedLoopMulti:
                executor.Statistics.ForceIndexNestedLoopForTesting = true;
                break;
            case Mode.Hash:
            case Mode.GraceHash:
            case Mode.SpillOffNestedLoopFallback:
                executor.Statistics.ForceHashJoinForTesting = true;
                break;
            case Mode.Merge:
                executor.Statistics.ForceMergeJoinForTesting = true;
                break;
        }

        return new Fixture(dbname, database, executor);
    }

    private static Dictionary<string, ColumnValue> Order(string name, long? code) => new()
    {
        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
        { "name", new(ColumnType.String, name) },
        { "code", code is { } c ? new(ColumnType.Integer64, c) : ColumnValue.Null },
    };

    private static Dictionary<string, ColumnValue> Item(long? code, string product, long qty) => new()
    {
        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
        { "order_code", code is { } c ? new(ColumnType.Integer64, c) : ColumnValue.Null },
        { "product", new(ColumnType.String, product) },
        { "qty", new(ColumnType.Integer64, qty) },
    };

    private static async Task<List<QueryResultRow>> Run(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task<List<string>> Explain(Fixture f, string sql)
    {
        List<QueryResultRow> rows = await Run(f, "EXPLAIN " + sql);
        return rows.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue ?? ""))).ToList();
    }

    private static string Product(QueryResultRow row) =>
        row.Row["product"].Type == ColumnType.Null ? "<null>" : row.Row["product"].StrValue!;

    private static void AssertPadded(QueryResultRow row, string orderName)
    {
        Assert.AreEqual(orderName, row.Row["name"].StrValue);
        Assert.AreEqual(ColumnType.Null, row.Row["product"].Type, $"{orderName}: product must be NULL on a padded row");
        Assert.AreEqual(ColumnType.Null, row.Row["qty"].Type, $"{orderName}: qty must be NULL on a padded row");
    }

    private const string JoinSql =
        "SELECT o.name, li.product, li.qty FROM orders o LEFT JOIN line_items li ON li.order_code = o.code ORDER BY o.name, li.product";

    /// <summary>The operator the plan names; the Grace path and the spill-off fallback are execution-time routes under a hash-join node.</summary>
    private static string OperatorName(Mode mode) => mode switch
    {
        Mode.NestedLoop => "nested-loop-join",
        Mode.IndexNestedLoopUnique or Mode.IndexNestedLoopMulti => "index-nested-loop-join",
        Mode.Hash or Mode.GraceHash or Mode.SpillOffNestedLoopFallback => "hash-join",
        Mode.Merge => "merge-join",
        _ => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoopUnique)]
    [TestCase(Mode.IndexNestedLoopMulti)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    [TestCase(Mode.GraceHash)]
    [TestCase(Mode.SpillOffNestedLoopFallback)]
    public async Task EveryLeftRowIsEmitted_MatchedOncePerMatch_UnmatchedPadded(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        List<string> plan = await Explain(f, JoinSql);
        Assert.IsTrue(plan.Any(l => l.Contains(OperatorName(mode)) && l.Contains("kind=left-outer")),
            $"{mode}: expected a {OperatorName(mode)} with kind=left-outer, got: {string.Join(" | ", plan)}");

        List<QueryResultRow> rows = await Run(f, JoinSql);
        bool unique = mode == Mode.IndexNestedLoopUnique;
        int bMatches = unique ? 1 : 3;

        // 5 left rows + extra matches of B (two more under a non-unique key).
        Assert.AreEqual(5 + (bMatches - 1), rows.Count, $"{mode}: rows = left rows + extra matches; got {string.Join(", ", rows.Select(r => r.Row["name"].StrValue + "/" + Product(r)))}");

        int i = 0;
        Assert.AreEqual("Order-A", rows[i].Row["name"].StrValue);
        Assert.AreEqual("Widget", Product(rows[i]));
        Assert.AreEqual(5L, rows[i++].Row["qty"].LongValue);

        List<string> bProducts = new();
        for (int k = 0; k < bMatches; k++)
        {
            Assert.AreEqual("Order-B", rows[i].Row["name"].StrValue);
            bProducts.Add(Product(rows[i++]));
        }
        CollectionAssert.AreEqual(unique ? new[] { "Gadget" } : new[] { "Doohickey", "Gadget", "Thing" }, bProducts, $"{mode}: B is emitted once per match");

        AssertPadded(rows[i++], "Order-C");

        Assert.AreEqual("Order-D", rows[i].Row["name"].StrValue);
        Assert.AreEqual("Sprocket", Product(rows[i++]));

        AssertPadded(rows[i++], "Order-N");
        Assert.IsFalse(rows.Any(r => Product(r) == "GhostPart"), $"{mode}: a NULL left key must never match a NULL right key");

        if (mode == Mode.GraceHash)
            Assert.Greater(f.Executor.Statistics.HashJoinGracePathCount, 0, "the Grace path must have run");
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoopUnique)]
    [TestCase(Mode.IndexNestedLoopMulti)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    [TestCase(Mode.GraceHash)]
    [TestCase(Mode.SpillOffNestedLoopFallback)]
    public async Task RightRowFailingANonKeyOnConjunct_LeavesTheLeftRowPadded(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        // Only Doohickey (qty 7) passes the residual conjunct: B keeps that one match, every
        // other order is padded even when its key matched a right row.
        const string sql =
            "SELECT o.name, li.product, li.qty FROM orders o LEFT JOIN line_items li " +
            "ON li.order_code = o.code AND li.qty > 6 ORDER BY o.name";

        List<QueryResultRow> rows = await Run(f, sql);
        Assert.AreEqual(5, rows.Count, $"{mode}: one row per order");

        AssertPadded(rows[0], "Order-A");
        if (mode == Mode.IndexNestedLoopUnique)
        {
            AssertPadded(rows[1], "Order-B");
        }
        else
        {
            Assert.AreEqual("Order-B", rows[1].Row["name"].StrValue);
            Assert.AreEqual("Doohickey", Product(rows[1]));
        }
        AssertPadded(rows[2], "Order-C");
        AssertPadded(rows[3], "Order-D");
        AssertPadded(rows[4], "Order-N");
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoopUnique)]
    [TestCase(Mode.IndexNestedLoopMulti)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    [TestCase(Mode.GraceHash)]
    [TestCase(Mode.SpillOffNestedLoopFallback)]
    public async Task ZeroRowRightTable_SelectStar_PadsEveryLeftRow_WithTheMatchedShape(Mode mode)
    {
        const string sql = "SELECT * FROM orders o LEFT JOIN line_items li ON li.order_code = o.code ORDER BY o.name";

        Fixture populated = await SetupAsync(mode);
        List<QueryResultRow> matchedRows = await Run(populated, sql);
        QueryResultRow matched = matchedRows.First(r => r.Row["o.name"].StrValue == "Order-A");
        HashSet<string> matchedKeys = new(matched.Row.Keys, StringComparer.OrdinalIgnoreCase);

        Fixture empty = await SetupAsync(mode, emptyRight: true);
        List<QueryResultRow> rows = await Run(empty, sql);

        Assert.AreEqual(5, rows.Count, $"{mode}: every left row is padded when the right table is empty");

        foreach (QueryResultRow row in rows)
        {
            CollectionAssert.AreEquivalent(matchedKeys, row.Row.Keys.ToList(),
                $"{mode}: a padded row carries exactly the keys a matched row carries");
            Assert.AreEqual(ColumnType.Null, row.Row["li.product"].Type);
            Assert.AreEqual(ColumnType.Null, row.Row["li.order_code"].Type);
            Assert.AreEqual(ColumnType.Null, row.Row["li.qty"].Type);
            Assert.AreEqual(ColumnType.Null, row.Row["li.id"].Type);
            Assert.IsNotNull(row.Row["o.name"].StrValue);
        }

        CollectionAssert.AreEqual(
            new[] { "Order-A", "Order-B", "Order-C", "Order-D", "Order-N" },
            rows.Select(r => r.Row["o.name"].StrValue).ToList());
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    [TestCase(Mode.GraceHash)]
    [TestCase(Mode.SpillOffNestedLoopFallback)]
    public async Task DerivedTableOnTheRight_IsPaddedWithItsOutputColumns(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        // The derived table has one row per code with items: 1, 2, 4 (NULL grouped apart).
        const string sql =
            "SELECT o.name, d.order_code, d.n FROM orders o " +
            "LEFT JOIN (SELECT order_code, COUNT(*) AS n FROM line_items GROUP BY order_code) d ON d.order_code = o.code " +
            "ORDER BY o.name";

        List<QueryResultRow> rows = await Run(f, sql);
        Assert.AreEqual(5, rows.Count, $"{mode}: one row per order, the derived side has one row per code");

        Assert.AreEqual("Order-A", rows[0].Row["name"].StrValue);
        Assert.AreEqual(1L, rows[0].Row["n"].LongValue);
        Assert.AreEqual("Order-B", rows[1].Row["name"].StrValue);
        Assert.AreEqual(3L, rows[1].Row["n"].LongValue);

        Assert.AreEqual("Order-C", rows[2].Row["name"].StrValue);
        Assert.AreEqual(ColumnType.Null, rows[2].Row["order_code"].Type, "padded d.order_code");
        Assert.AreEqual(ColumnType.Null, rows[2].Row["n"].Type, "padded d.n");

        Assert.AreEqual("Order-D", rows[3].Row["name"].StrValue);
        Assert.AreEqual(1L, rows[3].Row["n"].LongValue);

        Assert.AreEqual("Order-N", rows[4].Row["name"].StrValue);
        Assert.AreEqual(ColumnType.Null, rows[4].Row["n"].Type, "a NULL left key is padded, never matched to the NULL group");
    }

    [Test]
    [TestCase(Mode.Merge)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.NestedLoop)]
    public async Task RightSideEndsBeforeTheLeft_EveryTrailingLeftRowIsPadded(Mode mode)
    {
        // Only Order-A has an item, so the right stream ends after the first key while four
        // left rows remain. The streaming merge join must drain them with padding instead of
        // stopping when the right side ends.
        Fixture f = await SetupAsync(mode, onlyOrderAHasItems: true);

        List<QueryResultRow> rows = await Run(f, JoinSql);
        Assert.AreEqual(5, rows.Count, $"{mode}: trailing left rows must not vanish");

        Assert.AreEqual("Widget", Product(rows[0]));
        AssertPadded(rows[1], "Order-B");
        AssertPadded(rows[2], "Order-C");
        AssertPadded(rows[3], "Order-D");
        AssertPadded(rows[4], "Order-N");
    }

    [Test]
    public async Task GraceHash_ProbeRowWithNullKey_IsPaddedExactlyOnce()
    {
        Fixture f = await SetupAsync(Mode.GraceHash);

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product, li.qty FROM orders o LEFT JOIN line_items li ON li.order_code = o.code WHERE o.name = 'Order-N'");

        Assert.Greater(f.Executor.Statistics.HashJoinGracePathCount, 0, "the Grace path must have run");
        Assert.AreEqual(1, rows.Count, "the NULL-keyed probe row is set aside at partitioning and emitted padded once");
        AssertPadded(rows[0], "Order-N");
    }

    [Test]
    public async Task GraceHash_StopsPaddingWhenTheRequestIsCancelled()
    {
        // Once both inputs are partitioned to disk no scan observes the ticket token any more; the
        // partition probe and the NULL-key file must poll it themselves, or an abandoned request
        // keeps reading spill files and padding rows until the end.
        Fixture f = await SetupAsync(Mode.GraceHash);
        await AssertStopsAfterCancel(f, "SELECT o.name, li.product FROM orders o LEFT JOIN line_items li ON li.order_code = o.code");
        Assert.Greater(f.Executor.Statistics.HashJoinGracePathCount, 0, "the Grace path must have run");
    }

    [Test]
    public async Task MaterializedMerge_StopsPaddingWhenTheRequestIsCancelled()
    {
        // A derived right table takes the materialized merge path, whose padding loops run over
        // buffered rows with no scan left to observe the token.
        Fixture f = await SetupAsync(Mode.Merge, emptyRight: true);
        await AssertStopsAfterCancel(f,
            "SELECT o.name, li.product FROM orders o LEFT JOIN (SELECT order_code, product FROM line_items) li ON li.order_code = o.code");
    }

    /// <summary>Reads one row, cancels the ticket and enumerator token, and expects the next read to observe the cancellation.</summary>
    private static async Task AssertStopsAfterCancel(Fixture f, string sql)
    {
        using CancellationTokenSource cts = new();
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null, cancellationToken: cts.Token);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);

        await using IAsyncEnumerator<QueryResultRow> reader = cursor.GetAsyncEnumerator(cts.Token);
        Assert.IsTrue(await reader.MoveNextAsync(), "the first row is produced before the cancellation");

        cts.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await reader.MoveNextAsync(), "the read after the cancellation must not return another row");

        await f.Database.Transactions.RollbackAsync(txn);
    }

    [Test]
    public async Task InnerIndexNestedLoop_NullLeftKey_MatchesNoNullRightKey()
    {
        // Regression guard for the inner join: a NULL lookup value must not probe the non-unique
        // index, where NULL keys are stored and would compare equal to each other.
        Fixture f = await SetupAsync(Mode.IndexNestedLoopMulti);

        List<QueryResultRow> rows = await Run(f,
            "SELECT o.name, li.product FROM orders o JOIN line_items li ON li.order_code = o.code ORDER BY o.name, li.product");

        Assert.AreEqual(5, rows.Count, "A(1) + B(3) + D(1); C and N contribute nothing");
        Assert.IsFalse(rows.Any(r => r.Row["name"].StrValue == "Order-N"));
        Assert.IsFalse(rows.Any(r => Product(r) == "GhostPart"));
    }
}
