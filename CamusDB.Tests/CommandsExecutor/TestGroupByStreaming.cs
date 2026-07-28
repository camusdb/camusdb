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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies GROUP BY streaming aggregation: when an index scan guarantees that all rows of the
/// same group key are adjacent, the planner marks <c>AggregateNode.IsStreamingGroupBy</c> and
/// the executor routes to the O(1)-memory streaming path instead of the hash dictionary.
///
/// Test surface:
///   A. Plan shape — EXPLAIN shows "streaming: true" only when an index scan covers the GROUP BY column.
///   B. Execution correctness — streaming and hash paths produce identical, correct results.
///   C. Multi-column GROUP BY — streaming activates when the index covers all group-key columns.
///   D. HAVING — HAVING clause works correctly after streaming aggregation.
///   E. Fallback — expression GROUP BY, non-indexed column, or nullable column uses hash path.
/// </summary>
[TestFixture]
public sealed class TestGroupByStreaming : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Fixture setup
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a <c>sales</c> table:
    ///   - id     (Id,          PK)
    ///   - status (Integer64,   NOT NULL)  — has a secondary index <c>status_idx</c>
    ///   - region (String,      NOT NULL)  — has a secondary index <c>region_idx</c>
    ///   - score  (Integer64,   nullable)  — has a secondary index <c>score_idx</c>, but nullable
    ///   - amount (Integer64,   NOT NULL)  — no secondary index
    ///
    /// Data: status ∈ {1, 2, 3} each with multiple rows; region ∈ {"east", "west"}.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupSalesAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "sales",
            columns: new ColumnInfo[]
            {
                new("id",     ColumnType.Id),
                new("status", ColumnType.Integer64, notNull: true),
                new("region", ColumnType.String,    notNull: true),
                new("score",  ColumnType.Integer64, notNull: false),
                new("amount", ColumnType.Integer64, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey,  "~pk",        new ColumnIndexInfo[] { new("id",     OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "status_idx", new ColumnIndexInfo[] { new("status", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "region_idx", new ColumnIndexInfo[] { new("region", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "score_idx",  new ColumnIndexInfo[] { new("score",  OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "sr_idx",     new ColumnIndexInfo[] { new("status", OrderType.Ascending), new("region", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        // Insert rows: 3 statuses × 2 regions × 4 rows each.
        KvTransaction ins = await database.Transactions.BeginAsync();
        foreach (int st in new[] { 1, 2, 3 })
        {
            foreach (string rg in new[] { "east", "west" })
            {
                for (int i = 1; i <= 4; i++)
                {
                    await executor.Insert(new InsertTicket(
                        ins, dbname, "sales",
                        new List<Dictionary<string, ColumnValue>>
                        {
                            new()
                            {
                                { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                                { "status", new(ColumnType.Integer64, (long)st)       },
                                { "region", new(ColumnType.String,    rg)             },
                                { "score",  new(ColumnType.Integer64, (long)(st * 10)) },
                                { "amount", new(ColumnType.Integer64, (long)(i * 100)) },
                            }
                        }));
                }
            }
        }
        await database.Transactions.CommitAsync(ins);

        return (dbname, database, executor);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<string> ExplainAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        return string.Join("\n", rows.Select(r =>
        {
            string node   = r.Row.TryGetValue("node",   out ColumnValue? nv) ? (nv?.StrValue ?? "") : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? (dv?.StrValue ?? "") : "";
            return $"{node}({detail})";
        }));
    }

    private static async Task<List<QueryResultRow>> RunSqlAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // A. Plan shape
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task StreamingGroupBy_IndexedNotNullColumn_PlanShowsStreaming()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // No WHERE predicate: the planner forces a full ordered index scan on status_idx
        // (same as streaming DISTINCT) since status is NOT NULL and status_idx covers the GROUP BY.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT status, COUNT(*) FROM sales GROUP BY status");

        Assert.That(plan, Does.Contain("streaming: true"),
            "GROUP BY on indexed NOT NULL column must force status_idx and use streaming path");
    }

    [Test]
    public async Task StreamingGroupBy_NonIndexedColumn_PlanShowsNoStreaming()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // `amount` has no index; no predicate that drives an index scan covering `amount`.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT amount, COUNT(*) FROM sales GROUP BY amount");

        Assert.That(plan, Does.Not.Contain("streaming: true"),
            "GROUP BY on non-indexed column must fall back to hash aggregation");
    }

    [Test]
    public async Task StreamingGroupBy_NullableIndexedColumn_PlanShowsNoStreaming()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // `score` has an index but is nullable — streaming must be blocked to avoid silently
        // dropping the NULL group (index does not hold null-keyed rows).
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT score, COUNT(*) FROM sales WHERE score > 0 GROUP BY score");

        Assert.That(plan, Does.Not.Contain("streaming: true"),
            "GROUP BY on nullable indexed column must not use streaming (NULL group would be dropped)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // B. Execution correctness — streaming path produces identical results
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task StreamingGroupBy_CountPerStatus_CorrectCounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // 3 statuses × 2 regions × 4 rows = 8 rows per status.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt FROM sales GROUP BY status ORDER BY status");

        Assert.AreEqual(3, rows.Count, "Should have 3 groups (status 1, 2, 3)");
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(8L, row.Row["cnt"].LongValue, "Each status group should have 8 rows");
    }

    [Test]
    public async Task StreamingGroupBy_SumPerStatus_CorrectSums()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // amount per row: 100, 200, 300, 400 × 2 regions = 2*(100+200+300+400)=2000 per status.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, SUM(amount) AS total FROM sales GROUP BY status ORDER BY status");

        Assert.AreEqual(3, rows.Count);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(2000L, row.Row["total"].LongValue, "SUM(amount) per status group");
    }

    [Test]
    public async Task StreamingGroupBy_MinMaxPerStatus_CorrectValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, MIN(amount) AS mn, MAX(amount) AS mx FROM sales GROUP BY status ORDER BY status");

        Assert.AreEqual(3, rows.Count);
        foreach (QueryResultRow row in rows)
        {
            Assert.AreEqual(100L, row.Row["mn"].LongValue, "MIN(amount) = 100");
            Assert.AreEqual(400L, row.Row["mx"].LongValue, "MAX(amount) = 400");
        }
    }

    [Test]
    public async Task StreamingGroupBy_SingleGroupViaFilter_CorrectResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // A filter reduces to one group; the streaming path must finalize and emit it.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt FROM sales WHERE status = 2 GROUP BY status");

        Assert.AreEqual(1, rows.Count, "Single group expected");
        Assert.AreEqual(2L, rows[0].Row["status"].LongValue);
        Assert.AreEqual(8L, rows[0].Row["cnt"].LongValue);
    }

    [Test]
    public async Task StreamingGroupBy_EmptyInput_ReturnsNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // Predicate matches no rows; streaming aggregation must yield nothing.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt FROM sales WHERE status = 99 GROUP BY status");

        Assert.AreEqual(0, rows.Count, "No rows expected when predicate matches nothing");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // C. Multi-column GROUP BY
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task StreamingGroupBy_MultiColumn_PlanShowsStreaming()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // sr_idx covers (status, region); no WHERE needed — the planner forces the index scan.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT status, region, COUNT(*) FROM sales GROUP BY status, region");

        Assert.That(plan, Does.Contain("streaming: true"),
            "GROUP BY on (status, region) covered by sr_idx must use streaming path");
    }

    [Test]
    public async Task StreamingGroupBy_MultiColumn_CorrectCounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, region, COUNT(*) AS cnt FROM sales GROUP BY status, region ORDER BY status, region");

        // 3 statuses × 2 regions = 6 groups; 4 rows each.
        Assert.AreEqual(6, rows.Count, "Should have 6 groups");
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(4L, row.Row["cnt"].LongValue, "Each (status, region) group has 4 rows");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // D. HAVING with streaming aggregation
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task StreamingGroupBy_WithHaving_FiltersGroupsCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // Groups: status=1 cnt=8, status=2 cnt=8, status=3 cnt=8; HAVING keeps all (cnt>5).
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt FROM sales GROUP BY status HAVING COUNT(*) > 5 ORDER BY status");

        Assert.AreEqual(3, rows.Count, "All 3 groups pass HAVING cnt > 5");
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(8L, row.Row["cnt"].LongValue);
    }

    [Test]
    public async Task StreamingGroupBy_WithHaving_ExcludesGroupsCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // Each group has SUM(amount)=2000; HAVING SUM(amount) < 500 keeps no groups.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, SUM(amount) AS total FROM sales GROUP BY status HAVING SUM(amount) < 500");

        Assert.AreEqual(0, rows.Count, "No group has SUM(amount) < 500 (each group sums to 2000)");
    }

    [Test]
    public async Task StreamingGroupBy_ForcedIndexScan_AppliesResidualFilterOnNonIndexedColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // Case 2: GROUP BY status forces status_idx (streaming), while the WHERE predicate is on
        // `amount` — a NON-indexed column — so it must be applied as a residual filter *on top of*
        // the forced index scan. If the residual filter were dropped, counts/sums would include the
        // amount<=150 rows. Per status: amounts are {100,200,300,400} × 2 regions; amount>150 keeps
        // {200,300,400} × 2 = 6 rows summing to 2*(200+300+400)=1800.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT status, COUNT(*) AS cnt, SUM(amount) AS total FROM sales WHERE amount > 150 GROUP BY status");
        Assert.That(plan, Does.Contain("streaming: true"),
            "Forced status_idx scan with a residual filter on a non-indexed column must still stream");

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt, SUM(amount) AS total FROM sales WHERE amount > 150 GROUP BY status ORDER BY status");

        Assert.AreEqual(3, rows.Count, "3 status groups");
        foreach (QueryResultRow row in rows)
        {
            Assert.AreEqual(6L, row.Row["cnt"].LongValue,
                "Residual amount>150 must drop the two amount=100 rows per group (8→6)");
            Assert.AreEqual(1800L, row.Row["total"].LongValue,
                "SUM(amount) over the residual-filtered rows = 2*(200+300+400)");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // E. Fallback — non-streaming shapes
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task StreamingGroupBy_ForcedIndexScanWithoutWhereClause_PlanShowsStreamingAndIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // No WHERE clause: planner forces status_idx for GROUP BY streaming, same as DISTINCT.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT status, COUNT(*) FROM sales GROUP BY status");

        Assert.That(plan, Does.Contain("streaming: true"),
            "Planner must force status_idx for GROUP BY on indexed NOT NULL column");
        Assert.That(plan, Does.Contain("status_idx"),
            "Forced index scan must use status_idx");
    }

    [Test]
    public async Task StreamingGroupBy_ExpressionGroupBy_UsesHashPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // GROUP BY expression (not a bare column identifier) → streaming blocked.
        string plan = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT COUNT(*) AS cnt FROM sales GROUP BY status + 0");

        Assert.That(plan, Does.Not.Contain("streaming: true"),
            "Computed GROUP BY expression must fall back to hash aggregation");
    }

    [Test]
    public async Task StreamingGroupBy_StreamingAndHashYieldSameResults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSalesAsync();

        // GROUP BY with streaming (status is indexed NOT NULL) vs hash (amount has no index).
        // Both must return 3 groups with count=8.
        List<QueryResultRow> streamingRows = await RunSqlAsync(executor, database, dbname,
            "SELECT status, COUNT(*) AS cnt FROM sales GROUP BY status ORDER BY status");
        List<QueryResultRow> hashRows = await RunSqlAsync(executor, database, dbname,
            "SELECT amount, COUNT(*) AS cnt FROM sales GROUP BY amount ORDER BY amount");

        Assert.AreEqual(3, streamingRows.Count, "Streaming: 3 status groups");
        Assert.AreEqual(4, hashRows.Count, "Hash: 4 amount groups (100, 200, 300, 400)");

        foreach (QueryResultRow row in streamingRows)
            Assert.AreEqual(8L, row.Row["cnt"].LongValue, "Streaming: each status group has 8 rows");
        foreach (QueryResultRow row in hashRows)
            Assert.AreEqual(6L, row.Row["cnt"].LongValue, "Hash: each amount group has 6 rows (3 statuses × 2 regions)");
    }
}
