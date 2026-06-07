
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for R15: Index-Driven Value-List IN (<c>x IN (v1, v2, …)</c>).
///
/// Planner tests (no actual DB): use <see cref="QueryPlannerTestContext"/> to verify plan shape.
/// Execution tests: open a real in-memory database to verify result correctness.
/// </summary>
[NonParallelizable]
public class TestIndexInListScan : BaseTest
{
    // ── Planner-only test harness ─────────────────────────────────────────────

    private static QueryPlannerTestContext? ctx;
    private readonly QueryPlanner queryPlanner = new();

    [OneTimeSetUp]
    public void OneTimeSetUp() => ctx = QueryPlannerTestContext.Create();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (ctx is not null)
        {
            await ctx.DisposeAsync().ConfigureAwait(false);
            ctx = null;
        }
    }

    private static QueryTicket PlannerTicket(string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        Assert.AreEqual(NodeType.Select, ast.nodeType);
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);
        ExecuteSQLTicket execTicket = new(
            txnState: ctx!.Txn,
            database: QueryPlannerTestContext.DatabaseName,
            sql: sql,
            parameters: parameters);
        return QueryTicketAdapter.ToQueryTicket(query, execTicket);
    }

    private static PhysicalPlanNode ScanRoot(QueryPlan plan)
    {
        PhysicalPlanNode node = plan.Root;
        while (node.Input is not null)
            node = node.Input;
        return node;
    }

    // ── Planner tests ─────────────────────────────────────────────────────────

    [Test]
    public void PlanUsesIndexInListForUniquePKInList()
    {
        string id1 = QueryPlannerTestContext.SampleRowId;
        string id2 = ObjectIdGenerator.Generate().ToString();
        string id3 = ObjectIdGenerator.Generate().ToString();

        QueryTicket ticket = PlannerTicket(
            $"SELECT * FROM robots WHERE id IN ('{id1}', '{id2}', '{id3}')");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Expected index-in-list scan on unique PK");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, node.Index.Name);
        Assert.AreEqual("id", node.ColumnName);
        Assert.AreEqual(3, node.Values.Count);
    }

    [Test]
    public void PlanUsesIndexInListForNonUniqueYearIndex()
    {
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020, 2021)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Expected index-in-list scan on non-unique year_idx");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual("year_idx", node.Index.Name);
        Assert.AreEqual("year", node.ColumnName);
        Assert.AreEqual(2, node.Values.Count);
    }

    [Test]
    public void PlanUsesTableScanForNonIndexedColumn()
    {
        // The `enabled` column has no index — falls back to full scan + filter.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE enabled IN (true, false)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<TableScanNode>(ScanRoot(plan),
            "Expected table-scan when IN-list column has no index");
    }

    [Test]
    public void PlanUsesIndexInListForStringMultiIndex()
    {
        // R15b: `name_idx` is Multi on `name` (String type). Non-unique String indexes now use
        // an exact-match scan [v, v] inclusive — no successor needed.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE name IN ('Alice', 'Bob')");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Expected index-in-list scan for String-typed non-unique index (R15b)");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual("name_idx", node.Index.Name);
        Assert.AreEqual("name", node.ColumnName);
        Assert.AreEqual(2, node.Values.Count);
    }

    [Test]
    public void PlanPrefersUniquePKInListOverYearRangeScan()
    {
        // R15b finding #1: WHERE id IN (...) AND year > 2019 — the year range scan would normally
        // win TrySelectScan, but the unique PK IN-list (N=3 point-lookups) has bounded cost 2·N
        // and should beat the year range scan of unknown cardinality.
        string id1 = QueryPlannerTestContext.SampleRowId;
        string id2 = ObjectIdGenerator.Generate().ToString();
        string id3 = ObjectIdGenerator.Generate().ToString();

        QueryTicket ticket = PlannerTicket(
            $"SELECT * FROM robots WHERE id IN ('{id1}', '{id2}', '{id3}') AND year > 2019");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Unique PK IN-list (N=3) must beat the year range scan (R15b finding #1)");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual(CamusDBConfig.PrimaryKeyInternalName, node.Index.Name);
        Assert.AreEqual(3, node.Values.Count);

        // year > 2019 must remain as a residual filter (it is not absorbed by the IN-list scan).
        Assert.IsNotNull(plan.ExecutionFilter,
            "year > 2019 must stay as a residual filter when PK IN-list takes over");
    }

    [Test]
    public void InListNullValuesStripped_RemainingValuesIndexed()
    {
        // NULL values in the list must be stripped; the non-null values still drive index seeks.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020, NULL, 2022)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Expected index-in-list even when list contains a NULL");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual(2, node.Values.Count, "NULL should have been stripped from the value list");
    }

    [Test]
    public void AllNullsInListProducesTableScan()
    {
        // An all-NULL list is recognized as an IN-list with zero effective values.
        // The planner skips it (nothing to seek) and falls back to full scan + residual filter.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (NULL)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        // Zero-value IN-list → no seeks → fall back to table scan.
        Assert.IsInstanceOf<TableScanNode>(ScanRoot(plan),
            "All-NULL IN-list should fall back to table-scan");
    }

    [Test]
    public void NotInValueListRemainsResidualFilter()
    {
        // NOT IN (value-list) stays as a residual filter, never an index scan.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year NOT IN (2020, 2021)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<TableScanNode>(ScanRoot(plan),
            "NOT IN (value-list) must NOT produce an IndexInListScanNode");
        Assert.IsNotNull(plan.ExecutionFilter,
            "NOT IN (value-list) must remain in the execution filter");
    }

    [Test]
    public void InListAbsorbedFromFilter_NoResidualForAbsorbedPredicate()
    {
        // When the IN-list is absorbed into an IndexInListScanNode, it must NOT also appear
        // in the execution filter (that would double-evaluate and could reject valid rows).
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020, 2021)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan));
        Assert.IsNull(plan.ExecutionFilter,
            "Absorbed IN-list predicate must not appear in the execution filter");
    }

    [Test]
    public void InListWithAdditionalAndPredicateKeepsResidualFilter()
    {
        // The IN-list drives the scan; the LIKE predicate stays as a residual filter.
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020, 2021) AND name LIKE 'r%'");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan));
        Assert.IsNotNull(plan.ExecutionFilter,
            "Additional AND predicate must remain as a residual filter");
    }

    [Test]
    public void PlanRendererShowsIndexInListNode()
    {
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020, 2021)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        string rendered = PlanRenderer.Render(plan);
        Assert.That(rendered, Does.Contain("index-in-list"),
            "PlanRenderer must emit 'index-in-list' for IndexInListScanNode");
        Assert.That(rendered, Does.Contain("year_idx"),
            "Rendered plan must include the index name");
        Assert.That(rendered, Does.Contain("values=2"),
            "Rendered plan must include the value count");
    }

    [Test]
    public void SingleValueInListUsesIndexInList()
    {
        QueryTicket ticket = PlannerTicket(
            "SELECT * FROM robots WHERE year IN (2020)");

        QueryPlan plan = queryPlanner.GetPlan(ctx!.Database, ctx.Table, ticket);

        Assert.IsInstanceOf<IndexInListScanNode>(ScanRoot(plan),
            "Single-value IN-list should still use index-in-list");

        IndexInListScanNode node = (IndexInListScanNode)ScanRoot(plan);
        Assert.AreEqual(1, node.Values.Count);
    }

    // ── Execution tests ───────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupRobotsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        // Add a secondary index on year.
        AlterIndexTicket alterYearIdx = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "year_idx",
            columns: new ColumnIndexInfo[] { new("year", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex);
        await executor.AlterIndex(alterYearIdx);

        List<string> ids = new();
        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < 10; i++)
        {
            string id = ObjectIdGenerator.Generate().ToString();
            ids.Add(id);

            InsertTicket insertTicket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, id) },
                        { "name", new(ColumnType.String, "robot-" + i) },
                        { "year", new(ColumnType.Integer64, 2020 + i) },
                    }
                });

            await executor.Insert(insertTicket);
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor, ids);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor,
        KvTransaction txn,
        string dbname,
        string sql)
    {
        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    [Test]
    public async Task ExecInListOnPK_ResultsMatchExpected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupRobotsTable();

        string id0 = ids[0];
        string id3 = ids[3];
        string id7 = ids[7];

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            $"SELECT id FROM robots WHERE id IN ('{id0}', '{id3}', '{id7}')");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(3, rows.Count, "Expected exactly 3 rows from the IN-list query");
        HashSet<string> resultIds = rows.Select(r => r.Row["id"].StrValue!).ToHashSet();
        Assert.That(resultIds, Does.Contain(id0));
        Assert.That(resultIds, Does.Contain(id3));
        Assert.That(resultIds, Does.Contain(id7));
    }

    [Test]
    public async Task ExecInListOnYearIndex_CorrectRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT year FROM robots WHERE year IN (2020, 2022, 2024)");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(3, rows.Count, "Expected 3 rows with years 2020, 2022, 2024");
        HashSet<long> years = rows.Select(r => r.Row["year"].LongValue).ToHashSet();
        Assert.That(years, Does.Contain(2020L));
        Assert.That(years, Does.Contain(2022L));
        Assert.That(years, Does.Contain(2024L));
    }

    [Test]
    public async Task ExecInListDuplicateValues_EmitsRowAtMostOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupRobotsTable();

        string id0 = ids[0];

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            $"SELECT id FROM robots WHERE id IN ('{id0}', '{id0}', '{id0}')");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(1, rows.Count, "Duplicate list values must not emit the same row multiple times");
        Assert.AreEqual(id0, rows[0].Row["id"].StrValue);
    }

    [Test]
    public async Task ExecInListWithNullInList_NullMatchesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupRobotsTable();

        string id0 = ids[0];

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            $"SELECT id FROM robots WHERE id IN ('{id0}', NULL)");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(1, rows.Count, "NULL in the IN-list must not match any row");
        Assert.AreEqual(id0, rows[0].Row["id"].StrValue);
    }

    [Test]
    public async Task ExecInListEmptyEffectiveList_ReturnsNoRows()
    {
        // After stripping NULLs the effective list is empty → zero seeks → zero results.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT id FROM robots WHERE year IN (NULL)");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(0, rows.Count, "All-NULL IN-list must return no rows");
    }

    [Test]
    public async Task ExecNotInValueList_CorrectResidualBehavior()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT year FROM robots WHERE year NOT IN (2020, 2021)");

        await database.Transactions.CommitAsync(txn);

        // Years 2022-2029 (8 values) should be returned.
        Assert.AreEqual(8, rows.Count, "NOT IN must exclude only the listed values");
        foreach (QueryResultRow row in rows)
        {
            long y = row.Row["year"].LongValue;
            Assert.AreNotEqual(2020L, y);
            Assert.AreNotEqual(2021L, y);
        }
    }

    [Test]
    public async Task ExecInListMatchesResidualBaselineExactly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupRobotsTable();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<long> years = (await RunSql(executor, txn, dbname,
            "SELECT year FROM robots WHERE year IN (2021, 2023, 2025)"))
            .Select(r => r.Row["year"].LongValue)
            .OrderBy(y => y)
            .ToList();

        await database.Transactions.CommitAsync(txn);

        CollectionAssert.AreEqual(
            new long[] { 2021, 2023, 2025 },
            years,
            "IN-list result must match the expected subset exactly");
    }

    // ── R15b execution tests ──────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupRobotsWithNameIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        // Non-unique index on String column (the R15b target).
        AlterIndexTicket nameIdx = new(
            databaseName: dbname,
            tableName: "robots",
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex);
        await executor.AlterIndex(nameIdx);

        string[] names = ["Alice", "Bob", "Carol", "Dave", "Eve"];
        KvTransaction txn = await database.Transactions.BeginAsync();

        for (int i = 0; i < names.Length; i++)
        {
            InsertTicket insertTicket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, names[i]) },
                        { "year", new(ColumnType.Integer64, 2020 + i) },
                    }
                });
            await executor.Insert(insertTicket);
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    [Test]
    public async Task ExecStringNonUniqueInList_CorrectRows()
    {
        // R15b: non-unique String index uses exact-match scan [v, v] inclusive.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithNameIndex();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT name FROM robots WHERE name IN ('Alice', 'Carol', 'Eve')");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(3, rows.Count, "Expected exactly 3 rows from String non-unique IN-list");
        HashSet<string> resultNames = rows.Select(r => r.Row["name"].StrValue!).ToHashSet();
        Assert.That(resultNames, Does.Contain("Alice"));
        Assert.That(resultNames, Does.Contain("Carol"));
        Assert.That(resultNames, Does.Contain("Eve"));
    }

    [Test]
    public async Task ExecStringNonUniqueInList_NoFalsePositives()
    {
        // R15b: names not in the list must not appear in results.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsWithNameIndex();

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT name FROM robots WHERE name IN ('Alice')");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(1, rows.Count, "Expected exactly 1 row");
        Assert.AreEqual("Alice", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task ExecPKInListWithAndPredicate_CompetesAndFilters()
    {
        // R15b finding #1: id IN (...) AND year > 2021 — PK IN-list wins over the year range scan.
        // The year > 2021 predicate stays as a residual filter.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) = await SetupRobotsTable();

        // ids[0]→year=2020, ids[2]→year=2022, ids[4]→year=2024 (only 2022 and 2024 pass year > 2021)
        string id0 = ids[0];
        string id2 = ids[2];
        string id4 = ids[4];

        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            $"SELECT year FROM robots WHERE id IN ('{id0}', '{id2}', '{id4}') AND year > 2021");

        await database.Transactions.CommitAsync(txn);

        // id0→2020 filtered out by year > 2021; id2→2022 and id4→2024 pass.
        Assert.AreEqual(2, rows.Count, "Expected 2 rows: years 2022 and 2024");
        HashSet<long> years = rows.Select(r => r.Row["year"].LongValue).ToHashSet();
        Assert.That(years, Does.Contain(2022L));
        Assert.That(years, Does.Contain(2024L));
    }
}
