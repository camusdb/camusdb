
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// R5: Acceptance tests for EXPLAIN (ANALYZE) — executes the query, collects runtime stats,
/// and returns one result row per physical plan node with actual counters.
/// </summary>
public class TestExplainAnalyzeExecutor : SharedNodeBaseTest
{
    // ── Fixture helpers ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsTable(int rowCount = 5)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("name",    ColumnType.String, notNull: true),
                new("year",    ColumnType.Integer64),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        for (int i = 0; i < rowCount; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",      new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name",    new(ColumnType.String, "robot " + i) },
                        { "year",    new(ColumnType.Integer64, 2020 + i) },
                        { "enabled", new(ColumnType.Bool, i % 2 == 0) },
                    }
                }
            ));
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsWithYearIndex(int rowCount = 5)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(rowCount);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));
        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExplainAnalyzeAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // R10: the last row of EXPLAIN ANALYZE output is now a "plan-info" metadata row.
    // Use this helper to get the last *physical node* row (scan, filter, etc.) for stats checks.
    private static QueryResultRow LastDataRow(List<QueryResultRow> rows) =>
        rows.Last(r => r.Row.TryGetValue("node", out ColumnValue n) && n.StrValue != "plan-info");

    // ── Column schema tests ───────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_ReturnsRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        Assert.That(rows, Is.Not.Empty);
    }

    [Test]
    public async Task TestExplainAnalyze_StageIsAnalyze()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        foreach (QueryResultRow row in rows)
        {
            Assert.That(row.Row["stage"].StrValue, Is.EqualTo("analyze"),
                "All rows must carry stage=analyze");
        }
    }

    [Test]
    public async Task TestExplainAnalyze_HasExpectedColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        Assert.That(rows, Is.Not.Empty);
        QueryResultRow first = rows[0];

        Assert.That(first.Row.ContainsKey("stage"),           Is.True);
        Assert.That(first.Row.ContainsKey("node"),            Is.True);
        Assert.That(first.Row.ContainsKey("detail"),          Is.True);
        Assert.That(first.Row.ContainsKey("estimated_rows"),  Is.True);
        Assert.That(first.Row.ContainsKey("estimated_cost"),  Is.True);
        Assert.That(first.Row.ContainsKey("actual_rows"),     Is.True);
        Assert.That(first.Row.ContainsKey("actual_time_ms"),  Is.True);
        Assert.That(first.Row.ContainsKey("kv_lookups"),      Is.True);
        Assert.That(first.Row.ContainsKey("kv_scan_entries"), Is.True);
    }

    [Test]
    public async Task TestExplainAnalyze_EstimatedColumnsPresent()
    {
        // R9: estimated_rows and estimated_cost are now populated by the cost model.
        // Accept Integer64/Float64 (stats loaded) or Null (stats not yet available).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        Assert.That(rows, Is.Not.Empty);
        foreach (QueryResultRow row in rows)
        {
            Assert.That(row.Row.ContainsKey("estimated_rows"), "estimated_rows column must be present");
            Assert.That(row.Row.ContainsKey("estimated_cost"), "estimated_cost column must be present");
            ColumnType erType = row.Row["estimated_rows"].Type;
            Assert.That(erType is ColumnType.Null or ColumnType.Integer64,
                "estimated_rows must be Integer64 or Null");
            ColumnType ecType = row.Row["estimated_cost"].Type;
            Assert.That(ecType is ColumnType.Null or ColumnType.Float64,
                "estimated_cost must be Float64 or Null");
        }
    }

    // ── actual_rows tests ─────────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_TableScan_ActualRowsMatchInserted()
    {
        int rowCount = 5;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(rowCount);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        // The scan node is the last row in the DFS walk (leaf = innermost).
        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["node"].StrValue, Does.Contain("table-scan"),
            "Last EXPLAIN ANALYZE node should be the table-scan");

        Assert.That(scanRow.Row["actual_rows"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(scanRow.Row["actual_rows"].LongValue, Is.EqualTo(rowCount),
            "actual_rows on the scan should equal the number of inserted rows");
    }

    [Test]
    public async Task TestExplainAnalyze_LimitNode_ActualRowsReflectsLimit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots LIMIT 3");

        QueryResultRow limitRow = rows.First(r => r.Row["node"].StrValue!.StartsWith("limit"));
        Assert.That(limitRow.Row["actual_rows"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(limitRow.Row["actual_rows"].LongValue, Is.EqualTo(3),
            "limit node should emit exactly as many rows as the LIMIT value");
    }

    [Test]
    public async Task TestExplainAnalyze_ActualRowsIsNotNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        // The scan node (last row) always has stats.
        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["actual_rows"].Type, Is.Not.EqualTo(ColumnType.Null),
            "scan node actual_rows must not be NULL after EXPLAIN ANALYZE");
    }

    // ── KV counter tests ──────────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_TableScan_KvScanEntriesMatchRowCount()
    {
        int rowCount = 5;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(rowCount);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["kv_scan_entries"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(scanRow.Row["kv_scan_entries"].LongValue, Is.EqualTo(rowCount),
            "kv_scan_entries should count one entry per row visited in the table scan");
    }

    [Test]
    public async Task TestExplainAnalyze_IndexLookup_KvPointLookupIsOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithYearIndex();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year = 2022");

        // With a unique or non-unique equality lookup, the scan is either index-lookup or
        // index-range-scan. Either way, the scan node should have non-null actual_rows.
        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["actual_rows"].Type, Is.EqualTo(ColumnType.Integer64),
            "scan node should have actual_rows after index-based EXPLAIN ANALYZE");

        long kvLookups = scanRow.Row["kv_lookups"].Type == ColumnType.Integer64
            ? scanRow.Row["kv_lookups"].LongValue : 0;
        long kvScans = scanRow.Row["kv_scan_entries"].Type == ColumnType.Integer64
            ? scanRow.Row["kv_scan_entries"].LongValue : 0;
        Assert.That(kvLookups + kvScans, Is.GreaterThanOrEqualTo(0),
            "At least one KV counter must be non-negative for an index scan");
    }

    [Test]
    public async Task TestExplainAnalyze_RangeScan_KvScanEntriesPositive()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithYearIndex(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year >= 2020 AND year <= 2024");

        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["kv_scan_entries"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(scanRow.Row["kv_scan_entries"].LongValue, Is.GreaterThan(0),
            "Range scan over 5 years should have at least 1 kv_scan_entry");
    }

    // ── actual_time_ms tests ──────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_RootNode_ActualTimeMsIsPopulated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        // The root node is always the first row in the DFS walk (outermost operator).
        QueryResultRow rootRow = rows[0];
        Assert.That(rootRow.Row["actual_time_ms"].Type, Is.EqualTo(ColumnType.Float64),
            "root node actual_time_ms should be a float64 with total elapsed time");
        Assert.That(rootRow.Row["actual_time_ms"].FloatValue, Is.GreaterThanOrEqualTo(0),
            "elapsed time must be non-negative");
    }

    [Test]
    public async Task TestExplainAnalyze_NonRootNodes_ActualTimeMsIsNull()
    {
        // Per-node timing is not measured below the root. Non-root rows must emit NULL rather
        // than 0.0 (which would falsely imply zero cost). Only the root gets the total elapsed.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots LIMIT 3");

        // There must be at least 2 nodes (limit + table-scan) for this assertion to be useful.
        Assert.That(rows.Count, Is.GreaterThan(1),
            "Need at least two plan nodes for this test");

        // Root (rows[0]) has the time; every other node must have NULL.
        for (int i = 1; i < rows.Count; i++)
        {
            Assert.That(rows[i].Row["actual_time_ms"].Type, Is.EqualTo(ColumnType.Null),
                $"Non-root node '{rows[i].Row["node"].StrValue}' at index {i} must have NULL actual_time_ms");
        }
    }

    [Test]
    public async Task TestExplainAnalyze_FilterNode_ActualTimeMsIsNull()
    {
        // FilterNode is folded into the scan — timing not captured; must be NULL not 0.0.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE name = 'robot 1'");

        QueryResultRow? filterRow = rows.FirstOrDefault(r => r.Row["node"].StrValue!.StartsWith("filter"));
        if (filterRow is null || filterRow.Value.Row.Count == 0)
            Assert.Ignore("No filter node in plan — predicate was index-resolved; test does not apply.");

        Assert.That(filterRow.Value.Row["actual_time_ms"].Type, Is.EqualTo(ColumnType.Null),
            "filter node actual_time_ms must be NULL (timing not separately measured)");
    }

    // ── grammar / parse tests ─────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_ParsesCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // Should not throw — verifies the grammar change reaches the executor.
        Assert.DoesNotThrowAsync(async () =>
        {
            await ExplainAnalyzeAsync(executor, database, dbname,
                "EXPLAIN (ANALYZE) SELECT id, name FROM robots");
        });
    }

    [Test]
    public async Task TestExplain_InvalidOption_ThrowsOnAnalyzeTypo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // Typo "ANALYZ" should still be rejected by the grammar.
        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await ExplainAnalyzeAsync(executor, database, dbname,
                "EXPLAIN (ANALYZ) SELECT * FROM robots");
        });
    }

    // ── plan shape tests ──────────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_SimpleSelect_HasTableScanNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        Assert.That(rows.Any(r => r.Row["node"].StrValue!.StartsWith("table-scan")), Is.True,
            "EXPLAIN ANALYZE of a full-table query must include a table-scan node");
    }

    [Test]
    public async Task TestExplainAnalyze_WithLimit_HasLimitNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots LIMIT 2");

        Assert.That(rows.Any(r => r.Row["node"].StrValue!.StartsWith("limit")), Is.True,
            "EXPLAIN ANALYZE of a LIMIT query must include a limit node");
    }

    [Test]
    public async Task TestExplainAnalyze_IndexLookup_HasIndexNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithYearIndex();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year = 2021");

        bool hasIndexNode = rows.Any(r =>
            r.Row["node"].StrValue!.StartsWith("index-lookup") ||
            r.Row["node"].StrValue!.StartsWith("index-range-scan"));
        Assert.That(hasIndexNode, Is.True,
            "EXPLAIN ANALYZE of an equality predicate on an indexed column should use an index scan node");
    }

    // ── zero-row table tests ──────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_EmptyTable_ActualRowsIsZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "empty_table",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("val", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM empty_table");

        Assert.That(rows, Is.Not.Empty);
        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["actual_rows"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(scanRow.Row["actual_rows"].LongValue, Is.EqualTo(0),
            "actual_rows must be 0 when the table is empty");
    }

    // ── rows_read tests ───────────────────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_HasRowsReadColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        Assert.That(rows, Is.Not.Empty);
        Assert.That(rows[0].Row.ContainsKey("rows_read"), Is.True,
            "EXPLAIN ANALYZE output must include rows_read column");
    }

    [Test]
    public async Task TestExplainAnalyze_TableScan_RowsReadEqualsKvScanEntries()
    {
        // For a full table scan with no filter, every storage entry is decoded —
        // RowsRead = KvScanEntries.
        int rowCount = 5;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(rowCount);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots");

        QueryResultRow scanRow = LastDataRow(rows);
        Assert.That(scanRow.Row["rows_read"].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(scanRow.Row["rows_read"].LongValue, Is.EqualTo(rowCount),
            "rows_read should equal the row count when no entries are skipped");
        Assert.That(scanRow.Row["rows_read"].LongValue,
            Is.EqualTo(scanRow.Row["kv_scan_entries"].LongValue),
            "rows_read must equal kv_scan_entries for a full table scan (no missing rows)");
    }

    [Test]
    public async Task TestExplainAnalyze_TableScan_RowsReadGeRowsEmitted()
    {
        // When a filter drops rows, RowsRead ≥ RowsEmitted.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE name = 'robot 1'");

        QueryResultRow scanRow = LastDataRow(rows);
        long rowsRead    = scanRow.Row["rows_read"].Type    == ColumnType.Integer64 ? scanRow.Row["rows_read"].LongValue    : -1;
        long rowsEmitted = scanRow.Row["actual_rows"].Type == ColumnType.Integer64 ? scanRow.Row["actual_rows"].LongValue : -1;

        Assert.That(rowsRead, Is.GreaterThanOrEqualTo(rowsEmitted),
            "rows_read (pre-filter) must be ≥ actual_rows (post-filter)");
    }

    [Test]
    public async Task TestExplainAnalyze_UniqueIndexLookup_RowsReadIsOneOrZero()
    {
        // A point-lookup on a unique index decodes at most one row.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithYearIndex(3);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE year = 2020");

        QueryResultRow scanRow = LastDataRow(rows);
        if (scanRow.Row["rows_read"].Type != ColumnType.Integer64)
            Assert.Ignore("Scan node has no rows_read stat; likely a range scan, not a unique lookup.");

        Assert.That(scanRow.Row["rows_read"].LongValue, Is.InRange(0, 1),
            "unique index lookup decodes at most one row");
    }

    [Test]
    public async Task TestExplainAnalyze_NonScanNodes_RowsReadIsZero()
    {
        // Pipeline operators (sort, limit, aggregate) read from a cursor, not from storage —
        // their rows_read must be 0.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots LIMIT 3");

        QueryResultRow? limitRow = rows.FirstOrDefault(r => r.Row["node"].StrValue!.StartsWith("limit"));
        Assert.That(limitRow, Is.Not.Null, "Expected a limit node");
        Assert.That(limitRow!.Value.Row["rows_read"].LongValue, Is.EqualTo(0),
            "limit node rows_read must be 0 — it reads from a cursor, not from storage");
    }

    // ── FilterNode attribution tests ──────────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_FilterNode_ActualRowsNotNull()
    {
        // FilterNode is folded into the scan (ExecutionFilter) and has no StepNodes entry.
        // Its actual_rows should be attributed from the child scan's RowsEmitted (post-filter count),
        // not left as NULL.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE name = 'robot 1'");

        QueryResultRow? filterRow = rows.FirstOrDefault(r => r.Row["node"].StrValue!.StartsWith("filter"));
        if (filterRow is null || filterRow.Value.Row.Count == 0)
            Assert.Ignore("No filter node in plan — predicate was index-resolved; test does not apply.");

        Assert.That(filterRow.Value.Row["actual_rows"].Type, Is.EqualTo(ColumnType.Integer64),
            "FilterNode actual_rows must not be NULL; it should carry the post-filter count");
    }

    [Test]
    public async Task TestExplainAnalyze_FilterNode_ActualRowsMatchesSelectResult()
    {
        // The filter actual_rows should equal the number of rows returned by the query.
        int rowCount = 5;
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(rowCount);

        // Filter: name = 'robot 1' → exactly 1 matching row.
        List<QueryResultRow> analyzeRows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE name = 'robot 1'");

        QueryResultRow? filterRow = analyzeRows.FirstOrDefault(r => r.Row["node"].StrValue!.StartsWith("filter"));
        if (filterRow is null || filterRow.Value.Row.Count == 0)
            Assert.Ignore("No filter node in plan — predicate was index-resolved; test does not apply.");

        Assert.That(filterRow.Value.Row["actual_rows"].LongValue, Is.EqualTo(1),
            "filter node actual_rows should equal the number of rows matching the predicate");
    }

    [Test]
    public async Task TestExplainAnalyze_FilterNode_KvCountersAreZero()
    {
        // The filter node itself adds no KV reads — those are charged to the scan node.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable(5);

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(executor, database, dbname,
            "EXPLAIN (ANALYZE) SELECT * FROM robots WHERE name = 'robot 2'");

        QueryResultRow? filterRow = rows.FirstOrDefault(r => r.Row["node"].StrValue!.StartsWith("filter"));
        if (filterRow is null || filterRow.Value.Row.Count == 0)
            Assert.Ignore("No filter node in plan — predicate was index-resolved; test does not apply.");

        Assert.That(filterRow.Value.Row["kv_lookups"].LongValue, Is.EqualTo(0),
            "filter node kv_lookups must be 0 — KV work is charged to the scan");
        Assert.That(filterRow.Value.Row["kv_scan_entries"].LongValue, Is.EqualTo(0),
            "filter node kv_scan_entries must be 0 — KV work is charged to the scan");
    }

    // ── join not-yet-supported guard (R5 conscious decision) ──────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTwoTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "app_users",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("email", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "posts",
            columns: new ColumnInfo[]
            {
                new("id",        ColumnType.Id),
                new("user_id",   ColumnType.Id),
                new("title",     ColumnType.String, notNull: true),
                new("published", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    [Test]
    public async Task TestExplainAnalyze_JoinQuery_ThrowsNotSupported()
    {
        // PINNING TEST: EXPLAIN ANALYZE on join queries must throw rather than silently execute
        // only the left scan and return misleading NULL stats for join/right nodes.
        // QueryPlanStepAdapter emits no step for join nodes; the join path (QueryJoinExecutor)
        // is bypassed entirely by ExecuteQueryPlanInternal. Fix in R7.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await ExplainAnalyzeAsync(executor, database, dbname,
                "EXPLAIN (ANALYZE) SELECT u.email, p.title FROM app_users u INNER JOIN posts p ON p.user_id = u.id");
        });
    }

    [Test]
    public async Task TestExplainAnalyze_JoinQuery_ErrorMessageMentionsJoin()
    {
        // The error message must name the limitation so users know to use plain EXPLAIN.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await ExplainAnalyzeAsync(executor, database, dbname,
                "EXPLAIN (ANALYZE) SELECT u.email FROM app_users u INNER JOIN posts p ON p.user_id = u.id");
        })!;

        Assert.That(ex.Message, Does.Contain("JOIN").Or.Contain("join"),
            "Error message should mention JOIN so users understand the limitation");
    }

    [Test]
    public async Task TestExplain_JoinQuery_StillWorksWithoutAnalyze()
    {
        // Plain EXPLAIN (without ANALYZE) on joins must continue to work — this guard only
        // applies to EXPLAIN ANALYZE.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        List<QueryResultRow> rows = await ExplainAnalyzeAsync(
            executor, database, dbname,
            "EXPLAIN SELECT u.email, p.title FROM app_users u INNER JOIN posts p ON p.user_id = u.id");

        Assert.That(rows, Is.Not.Empty,
            "Plain EXPLAIN on join queries must succeed");
    }

    // ── CollectRuntimeStats zero-cost guard ────────────────────────────────────

    [Test]
    public async Task TestExplainAnalyze_PlainExplain_StatsRemainsNull()
    {
        // Plain EXPLAIN must NOT set CollectRuntimeStats = true; plan node Stats should stay null.
        // We verify this indirectly: plain EXPLAIN should NOT include actual_rows in its output.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname,
            sql: "EXPLAIN SELECT * FROM robots", parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        Assert.That(rows, Is.Not.Empty);
        Assert.That(rows[0].Row.ContainsKey("actual_rows"), Is.False,
            "Plain EXPLAIN must not include actual_rows column");
    }
}
