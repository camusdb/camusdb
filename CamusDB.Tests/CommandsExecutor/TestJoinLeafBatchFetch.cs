/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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
/// Behavioral guard for the paged primary-row fetch used by the three join leaf scans (index range,
/// IN list, and the forced-index scan a merge join reads its ordered side from).
///
/// <para>
/// Each leaf now buffers row ids in index order and resolves a whole page with one batch call. The
/// page boundary must be invisible: the same query at a page size of 1, 2, 3, 7 and the default must
/// return the same rows, in the same order, with the same residual filtering. The page size is shrunk
/// through <c>IndexScanFetchBatchSize</c> so a small fixture still crosses several page boundaries —
/// the same technique the single-table scanner parity tests use.
/// </para>
/// </summary>
[NonParallelizable]
public sealed class TestJoinLeafBatchFetch : SharedNodeBaseTest
{
    private static readonly int[] PageSizes = [1, 2, 3, 7, 64];

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// boxes  : id (pk), sku string NOT NULL (multi index), region string NOT NULL
    /// parts  : id (pk), sku string NOT NULL (unique index), qty int64 (multi index, nullable),
    ///          grade string NOT NULL
    ///
    /// 40 parts, sku "sku-00".."sku-39", qty 0..39 with every 7th row NULL, grade alternating
    /// "A"/"B". 40 boxes share the same sku values, so a join on sku pairs one-to-one, and the
    /// residual filters below reject rows in the middle of a page rather than at its edge.
    /// </summary>
    private async Task<Fixture> Setup(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "boxes",
            columns:
            [
                new("id", ColumnType.Id),
                new("sku", ColumnType.String, notNull: true),
                new("region", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                // Indexed on both sides so a forced merge join reads each side through an index-ordered
                // leaf scan — the third paged path under test.
                new(ConstraintType.IndexMulti, "boxes_sku_idx", [new("sku", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "parts",
            columns:
            [
                new("id", ColumnType.Id),
                new("sku", ColumnType.String, notNull: true),
                new("qty", ColumnType.Integer64),
                new("grade", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexUnique, "parts_sku_idx", [new("sku", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "parts_qty_idx", [new("qty", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<Dictionary<string, ColumnValue>> parts = [];
        List<Dictionary<string, ColumnValue>> boxes = [];

        for (int i = 0; i < 40; i++)
        {
            string sku = "sku-" + i.ToString("D2");

            parts.Add(new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "sku", new(ColumnType.String, sku) },
                { "qty", i % 7 == 0 ? ColumnValue.Null : new ColumnValue(ColumnType.Integer64, (long)i) },
                { "grade", new(ColumnType.String, i % 2 == 0 ? "A" : "B") },
            });

            boxes.Add(new()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "sku", new(ColumnType.String, sku) },
                { "region", new(ColumnType.String, i % 3 == 0 ? "north" : "south") },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "parts", parts));
        await executor.Insert(new InsertTicket(txn, dbname, "boxes", boxes));

        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(Fixture fixture, string sql, CancellationToken cancellationToken = default)
    {
        KvTransaction txn = await fixture.Database.Transactions.BeginAsync();

        try
        {
            ExecuteSQLTicket ticket = new(
                txnState: txn,
                database: fixture.DbName,
                sql: sql,
                parameters: null,
                cancellationToken: cancellationToken);

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(ticket);

            List<QueryResultRow> rows = [];

            await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken))
                rows.Add(row);

            return rows;
        }
        finally
        {
            await fixture.Database.Transactions.CommitAsync(txn);
        }
    }

    /// <summary>Renders a result set as an order-sensitive list of strings, so parity is exact.</summary>
    private static List<string> Render(IReadOnlyList<QueryResultRow> rows, params string[] columns)
    {
        List<string> rendered = new(rows.Count);

        foreach (QueryResultRow row in rows)
            rendered.Add(string.Join("|", columns.Select(c => row.Row.TryGetValue(c, out ColumnValue? v) ? v.ToString() : "<absent>")));

        return rendered;
    }

    /// <summary>
    /// Runs one query at every page size and asserts each result matches the default-page-size result
    /// exactly, row for row and in order. Returns that reference result so a caller can assert on it.
    /// </summary>
    private async Task<List<string>> AssertPageSizeParity(string sql, string[] columns, bool forceMergeJoin = false)
    {
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = pageSize });

            if (forceMergeJoin)
                fixture.Executor.Statistics.ForceMergeJoinForTesting = true;

            List<string> rendered = Render(await Run(fixture, sql), columns);

            if (reference is null)
                reference = rendered;
            else
                Assert.AreEqual(reference, rendered, $"page size {pageSize} changed the result");
        }

        Assert.IsNotNull(reference);
        return reference!;
    }

    // ── Index range leaf ──────────────────────────────────────────────────────

    [Test]
    public async Task IndexRangeLeaf_PagesIdenticallyAtEveryPageSize()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.sku, p.qty, p.grade FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.qty > 33 ORDER BY p.qty",
            ["sku", "qty", "grade"]);

        // qty 34..39, minus 35 (a NULL row) → 5 rows spanning several pages at page size 1..3.
        Assert.AreEqual(5, rows.Count, "qty 34..39 excluding the NULL at 35");
    }

    [Test]
    public async Task IndexRangeLeaf_ResidualFilterRejectsMidPage()
    {
        // 'grade' is not part of parts_qty_idx, so it stays a residual filter applied after each row
        // is decoded — rejecting rows in the middle of a page, not only at its boundary.
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.qty > 9 AND p.grade = 'B' ORDER BY p.qty",
            ["sku", "qty"]);

        Assert.IsTrue(rows.Count > 3, "the fixture must leave several odd-qty rows past 9");
    }

    [Test]
    public async Task IndexRangeLeaf_EmptyRangeReturnsNoRows()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.qty > 10000 ORDER BY p.qty",
            ["sku", "qty"]);

        Assert.AreEqual(0, rows.Count);
    }

    // ── IN-list leaf, non-unique index ────────────────────────────────────────

    [Test]
    public async Task InListLeaf_NonUniqueIndex_PagesIdenticallyAtEveryPageSize()
    {
        // A page can span two IN-list values, so this covers the case the per-value fetch never hit.
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.qty IN (3, 5, 8, 11, 13, 19, 23, 31) ORDER BY p.qty",
            ["sku", "qty"]);

        Assert.AreEqual(8, rows.Count, "every listed qty exists exactly once in the fixture");
    }

    [Test]
    public async Task InListLeaf_RepeatedValues_DoNotDuplicateRows()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.qty IN (3, 3, 5, 5, 5, 8) ORDER BY p.qty",
            ["sku", "qty"]);

        Assert.AreEqual(3, rows.Count, "the index-side dedup must survive the paged fetch");
    }

    // ── IN-list leaf, unique index ────────────────────────────────────────────

    [Test]
    public async Task InListLeaf_UniqueIndex_PagesIdenticallyAtEveryPageSize()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.region, p.sku, p.grade FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.sku IN ('sku-01', 'sku-04', 'sku-09', 'sku-16', 'sku-25', 'sku-36') " +
            "ORDER BY p.sku",
            ["region", "sku", "grade"]);

        Assert.AreEqual(6, rows.Count);
    }

    [Test]
    public async Task InListLeaf_UniqueIndex_MissingValuesAreSkipped()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT p.sku FROM boxes b JOIN parts p ON p.sku = b.sku " +
            "WHERE p.sku IN ('sku-02', 'nope-1', 'sku-07', 'nope-2', 'nope-3', 'sku-11') " +
            "ORDER BY p.sku",
            ["sku"]);

        Assert.AreEqual(3, rows.Count, "absent IN-list values must not shift the page contents");
    }

    // ── Forced-index leaf (merge join reads its ordered side through it) ──────

    [Test]
    public async Task ForcedIndexLeaf_MergeJoin_PagesIdenticallyAtEveryPageSize()
    {
        List<string> rows = await AssertPageSizeParity(
            "SELECT b.region, p.sku, p.grade FROM parts p JOIN boxes b ON b.sku = p.sku " +
            "ORDER BY p.sku",
            ["region", "sku", "grade"],
            forceMergeJoin: true);

        Assert.AreEqual(40, rows.Count, "every part pairs with exactly one box");
    }

    // ── The paged path must match the plan the assertions assume ──────────────

    [Test]
    public async Task ExplainConfirmsTheLeafShapesUnderTest()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 });

        string rangePlan = await ExplainDetails(fixture,
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku WHERE p.qty > 33");
        StringAssert.Contains("index-range-scan", rangePlan,
            "the qty range predicate must plan an index-range join leaf, or the parity tests above prove nothing");

        string inListPlan = await ExplainDetails(fixture,
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku WHERE p.qty IN (3, 5, 8)");
        StringAssert.Contains("index-in-list", inListPlan,
            "the qty IN list must plan an IN-list join leaf, or the parity tests above prove nothing");

        Fixture mergeFixture = await Setup(Options with { IndexScanFetchBatchSize = 2 });
        mergeFixture.Executor.Statistics.ForceMergeJoinForTesting = true;

        string mergePlan = await ExplainDetails(mergeFixture,
            "SELECT b.region, p.sku FROM parts p JOIN boxes b ON b.sku = p.sku");
        StringAssert.Contains("forced-index", mergePlan,
            "the merge join must read its ordered side through a forced-index leaf");
    }

    private static async Task<string> ExplainDetails(Fixture fixture, string sql)
    {
        List<QueryResultRow> planRows = await Run(fixture, "EXPLAIN " + sql);

        return string.Join(
            "\n",
            planRows.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue ?? v.ToString()))));
    }

    // ── Cancellation must be observed inside a page, not only between pages ──

    [Test]
    public async Task CancelledTicketTokenStopsTheLeafScan()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 });

        using CancellationTokenSource cts = new();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await Run(
            fixture,
            "SELECT b.sku, p.qty FROM boxes b JOIN parts p ON p.sku = b.sku WHERE p.qty > 2 ORDER BY p.qty",
            cts.Token));
    }
}
