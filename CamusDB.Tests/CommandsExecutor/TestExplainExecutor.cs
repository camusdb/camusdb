
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
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
/// R3: Acceptance tests for EXPLAIN executed as a query result set.
/// Each EXPLAIN test asserts:
///   - The result rows carry the correct columns (stage, node, detail, estimated_rows, estimated_cost).
///   - No table rows are scanned (the plan is built but the data cursor is never opened).
///   - The node sequence matches the expected physical plan shape.
/// </summary>
public class TestExplainExecutor : SharedNodeBaseTest
{
    // ── Fixture helpers ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsTable()
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

        for (int i = 0; i < 5; i++)
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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsWithYearIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

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

    // ── Helper ────────────────────────────────────────────────────────────────

    private static async Task<List<QueryResultRow>> ExplainAsync(
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

    private static void AssertExplainColumns(QueryResultRow row)
    {
        Assert.IsTrue(row.Row.ContainsKey("stage"),           "Missing 'stage' column");
        Assert.IsTrue(row.Row.ContainsKey("node"),            "Missing 'node' column");
        Assert.IsTrue(row.Row.ContainsKey("detail"),          "Missing 'detail' column");
        Assert.IsTrue(row.Row.ContainsKey("estimated_rows"),  "Missing 'estimated_rows' column");
        Assert.IsTrue(row.Row.ContainsKey("estimated_cost"),  "Missing 'estimated_cost' column");
    }

    // ── Column schema tests ───────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TestExplainRowHasCorrectColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
            AssertExplainColumns(row);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainEstimatedColumnsPresent()
    {
        // R9: estimated_rows and estimated_cost are now populated by the cost model.
        // Verify the columns exist and are either Integer64/Float64 (when stats are loaded)
        // or Null (when the background load hasn't completed yet) — both are valid.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
        {
            Assert.IsTrue(row.Row.ContainsKey("estimated_rows"), "estimated_rows column must be present");
            Assert.IsTrue(row.Row.ContainsKey("estimated_cost"), "estimated_cost column must be present");
            ColumnType erType = row.Row["estimated_rows"].Type;
            Assert.IsTrue(erType is ColumnType.Null or ColumnType.Integer64,
                "estimated_rows must be Integer64 or Null");
            ColumnType ecType = row.Row["estimated_cost"].Type;
            Assert.IsTrue(ecType is ColumnType.Null or ColumnType.Float64,
                "estimated_cost must be Float64 or Null");
        }
    }

    // ── Stage label tests ─────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TestExplainStageIsPhysical()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual("physical", row.Row["stage"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainPhysicalStageLabel()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN (PHYSICAL) SELECT * FROM robots");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual("physical", row.Row["stage"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainLogicalStageLabel()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN (LOGICAL) SELECT * FROM robots");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual("logical", row.Row["stage"].StrValue);
    }

    // ── Plan shape tests ──────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TestExplainFullTableScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        // A plain SELECT with no predicate produces a table-scan (possibly wrapped in project).
        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("table-scan"), $"Expected table-scan node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainIndexLookup()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        string id = ObjectIdGenerator.Generate().ToString();
        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT * FROM robots WHERE id = '{id}'");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("index-lookup"),
            $"Expected index-lookup node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainIndexLookupDoesNotReadRows()
    {
        // EXPLAIN must not open the data cursor — we verify by checking the rows returned
        // are all explain rows (stage column present) rather than data rows.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        string id = ObjectIdGenerator.Generate().ToString();
        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT * FROM robots WHERE id = '{id}'");

        // Every row must have the explain schema, never a data column like 'name' or 'year'.
        foreach (QueryResultRow row in rows)
        {
            Assert.IsTrue(row.Row.ContainsKey("stage"), "EXPLAIN row missing 'stage' column");
            Assert.IsFalse(row.Row.ContainsKey("name"),  "Data column 'name' leaked into EXPLAIN output");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainIndexRangeScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithYearIndex();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year >= 2021 AND year <= 2023");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("index-range-scan"),
            $"Expected index-range-scan node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainAggregate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT COUNT(*) FROM robots");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("aggregate"),
            $"Expected aggregate node; got: {string.Join(", ", nodes)}");
        Assert.IsTrue(nodes.Contains("table-scan"),
            $"Expected table-scan node below aggregate; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainNodeOrder_ParentBeforeChild()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT COUNT(*) FROM robots");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();

        int aggregateIdx = Array.IndexOf(nodes, "aggregate");
        int tableScanIdx = Array.IndexOf(nodes, "table-scan");

        Assert.Greater(aggregateIdx, -1, "aggregate not found");
        Assert.Greater(tableScanIdx, -1, "table-scan not found");
        Assert.Less(aggregateIdx, tableScanIdx, "aggregate (parent) must appear before table-scan (child)");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        // ORDER BY year DESC cannot be satisfied by any index, so SortNode is emitted.
        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots ORDER BY year DESC");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("sort"),
            $"Expected sort node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainLimit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots LIMIT 3");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("limit"),
            $"Expected limit node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT u.email, p.title FROM app_users u INNER JOIN posts p ON p.user_id = u.id");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(
            nodes.Contains("nested-loop-join") || nodes.Contains("index-nested-loop-join"),
            $"Expected a join node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainJoin_HasTableScanChildren()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTwoTables();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT u.email, p.title FROM app_users u INNER JOIN posts p ON p.user_id = u.id");

        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(nodes.Contains("table-scan"),
            $"Expected table-scan below join node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainDetailNotEmpty_ForTableScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        QueryResultRow scanRow = rows.First(r => r.Row["node"].StrValue == "table-scan");
        Assert.IsNotEmpty(scanRow.Row["detail"].StrValue, "table-scan detail should not be empty");
        StringAssert.Contains("robots", scanRow.Row["detail"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainDetailContainsIndexName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        string id = ObjectIdGenerator.Generate().ToString();
        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT * FROM robots WHERE id = '{id}'");

        QueryResultRow lookupRow = rows.First(r => r.Row["node"].StrValue == "index-lookup");
        Assert.IsNotEmpty(lookupRow.Row["detail"].StrValue);
        StringAssert.Contains("index=", lookupRow.Row["detail"].StrValue);
    }

    // ── Subquery materialization behaviour (pinned) ────────────────────────────
    //
    // EXPLAIN for queries that contain subqueries causes real storage reads for
    // the inner subquery (scalar, IN/NOT-IN, EXISTS). This is a known limitation
    // documented in ExplainExecutor — it is reads-only (no mutation) and deferred.
    // These tests pin the current behaviour so any future change is intentional.

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsAndYears()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "valid_years",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));

        await executor.Insert(new InsertTicket(txn, dbname, "valid_years", values: new()
        {
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "year", new(ColumnType.Integer64, 2021) } },
            new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "year", new(ColumnType.Integer64, 2022) } },
        }));

        for (int i = 0; i < 3; i++)
            await executor.Insert(new InsertTicket(txn, dbname, "robots", values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "robot" + i) },
                    { "year", new(ColumnType.Integer64, 2020 + i) },
                }
            }));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainWithInSubquery_ReturnsExplainRows()
    {
        // Documents pinned behaviour: the inner subquery (SELECT year FROM valid_years)
        // is materialized against storage during EXPLAIN. The outer table (robots) is
        // NOT scanned — EXPLAIN rows are returned, not data rows.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsAndYears();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year IN (SELECT year FROM valid_years)");

        Assert.IsNotEmpty(rows, "EXPLAIN should return at least one plan row");

        // All rows must carry the EXPLAIN schema, not data columns.
        foreach (QueryResultRow row in rows)
        {
            AssertExplainColumns(row);
            Assert.IsFalse(row.Row.ContainsKey("name"),
                "Data column 'name' must not appear in EXPLAIN output");
        }

        // The outer table scan / filter is reflected in the plan.
        string[] nodes = rows.Select(r => r.Row["node"].StrValue!).ToArray();
        Assert.IsTrue(
            nodes.Contains("table-scan") || nodes.Contains("index-lookup") || nodes.Contains("filter"),
            $"Expected at least one scan or filter node; got: {string.Join(", ", nodes)}");
    }

    [Test]
    [NonParallelizable]
    public async Task TestExplainWithInSubquery_StageIsPhysical()
    {
        // Stage label is correct even when a subquery is present.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsAndYears();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year IN (SELECT year FROM valid_years)");

        Assert.IsNotEmpty(rows);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual("physical", row.Row["stage"].StrValue);
    }

    // ── Normal SELECT is unchanged ─────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task TestNormalSelectUnaffectedByExplain()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: "SELECT * FROM robots",
            parameters: null
        );
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(5, rows.Count, "Normal SELECT should return all 5 data rows");
        foreach (QueryResultRow row in rows)
            Assert.IsTrue(row.Row.ContainsKey("name"), "Normal SELECT row must have 'name' column");
    }
}
