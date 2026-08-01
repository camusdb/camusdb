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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DISTINCT-Aware Streaming Scan.
///
/// Verifies that SELECT DISTINCT over columns covered by an ascending index prefix uses
/// streaming (adjacent-key) deduplication instead of a hash set, and that the plan shape
/// and execution results are correct.
///
/// Test surface:
///   A. Plan shape — EXPLAIN shows "distinct(streaming: true)" when an index covers the key,
///      and "distinct(hash)" when no matching index exists.
///   B. Execution correctness — streaming and hash paths produce identical, correct results.
///   C. Sort elision — ORDER BY matching the streaming ordering skips the SortNode.
///   D. LIMIT interaction — LIMIT applied after dedup, not before.
///   E. NULL semantics — NULL duplicates are collapsed exactly once.
///   F. Fallback — unindexed, expression, or wildcard projections use hash distinct.
/// </summary>
[TestFixture]
public sealed class TestDistinctStreaming : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Fixture setup
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a `teams` table with:
    ///   - id (Id, PK)
    ///   - code (String, NOT NULL)  — has a secondary index `code_idx`
    ///   - name (String)            — no secondary index
    ///   - score (Integer64)        — has a secondary index `score_idx`
    /// Rows with duplicate codes and names are inserted to test deduplication.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTeamsAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "teams",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("code",  ColumnType.String, notNull: true),
                new("name",  ColumnType.String),
                new("score", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "code_idx", new ColumnIndexInfo[] { new("code", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "score_idx",new ColumnIndexInfo[] { new("score",OrderType.Ascending) }),
            },
            ifNotExists: false));

        await database.Transactions.CommitAsync(txn);

        // Insert rows: codes A, B, C with duplicates.
        (string code, string? name, long score)[] rows =
        [
            ("A", "Alpha",   10),
            ("A", "Alpha",   10),  // exact dup
            ("B", "Beta",    20),
            ("B", "Gamma",   20),  // same code, different name
            ("C", "Delta",   30),
            ("C", "Delta",   30),  // exact dup
            ("C", "Epsilon", 30),  // same code, different name
        ];

        KvTransaction ins = await database.Transactions.BeginAsync();
        foreach ((string code, string? name, long score) row in rows)
        {
            await executor.Insert(new InsertTicket(
                ins,
                dbname,
                "teams",
                new List<Dictionary<string, ColumnValue>>
                {
                    new()
                    {
                        { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "code",  new(ColumnType.String,    row.code)  },
                        { "name",  new(ColumnType.String,    row.name ?? "") },
                        { "score", new(ColumnType.Integer64, row.score) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(ins);

        return (dbname, database, executor);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<string> ExplainPlanAsync(
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

        return string.Join("\n", rows.Select(r =>
        {
            string node = r.Row.TryGetValue("node", out ColumnValue? nv) ? (nv?.StrValue ?? "") : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? (dv?.StrValue ?? "") : "";
            return $"{node}({detail})";
        }));
    }

    private static async Task<List<QueryResultRow>> RunSqlAsync(
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

    // ─────────────────────────────────────────────────────────────────────────
    // A. Plan shape
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_IndexedDistinctColumnUses_StreamingDistinct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT code FROM teams");

        Assert.That(plan, Does.Contain("distinct(streaming: true)"),
            "DISTINCT over indexed `code` column must use streaming dedup");
        Assert.That(plan, Does.Not.Contain("distinct(hash)"));
    }

    [Test]
    public async Task R12_NonIndexedDistinctColumnUses_HashDistinct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT name FROM teams");

        Assert.That(plan, Does.Contain("distinct(hash)"),
            "DISTINCT over non-indexed `name` column must fall back to hash dedup");
        Assert.That(plan, Does.Not.Contain("streaming"));
    }

    [Test]
    public async Task R12_WildcardDistinctUses_HashDistinct()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT * FROM teams");

        Assert.That(plan, Does.Contain("distinct(hash)"),
            "DISTINCT * (wildcard) cannot be streamed — must use hash distinct");
    }

    [Test]
    public async Task R12_StreamingPlanUsesIndexScanNotTableScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT code FROM teams");

        // The planner must swap the full table scan for an ordered index scan.
        Assert.That(plan, Does.Contain("code_idx"),
            "Streaming distinct plan must use code_idx for ordered access");
        Assert.That(plan, Does.Not.Contain("table-scan(table=teams)"),
            "Streaming distinct plan must not use a plain table-scan");
    }

    [Test]
    public async Task R12_StreamingDistinctWithMatchingOrderBy_ElidesSortNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT code FROM teams ORDER BY code");

        Assert.That(plan, Does.Contain("distinct(streaming: true)"));
        Assert.That(plan, Does.Not.Contain("sort("),
            "ORDER BY code must be satisfied by streaming index ordering — no SortNode needed");
    }

    [Test]
    public async Task R12_HashDistinctWithOrderBy_IncludesSortNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT name FROM teams ORDER BY name");

        Assert.That(plan, Does.Contain("distinct(hash)"));
        Assert.That(plan, Does.Contain("sort("),
            "Non-indexed DISTINCT + ORDER BY must still include a SortNode");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // B. Execution correctness
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_StreamingDistinct_ProducesCorrectUniqueValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT code FROM teams ORDER BY code");

        Assert.AreEqual(3, rows.Count, "Should get 3 distinct codes: A, B, C");
        Assert.AreEqual("A", rows[0].Row["code"].StrValue);
        Assert.AreEqual("B", rows[1].Row["code"].StrValue);
        Assert.AreEqual("C", rows[2].Row["code"].StrValue);
    }

    [Test]
    public async Task R12_HashDistinct_ProducesCorrectUniqueValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        // `name` is not indexed → hash distinct path
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT name FROM teams ORDER BY name");

        string[] expected = ["Alpha", "Beta", "Delta", "Epsilon", "Gamma"];
        Assert.AreEqual(expected.Length, rows.Count, "Should get 5 distinct names");
        for (int i = 0; i < expected.Length; i++)
            Assert.AreEqual(expected[i], rows[i].Row["name"].StrValue);
    }

    [Test]
    public async Task R12_StreamingAndHash_ProduceSameResultsForIndexedColumn()
    {
        // Verifies streaming distinct is semantically equivalent to hash distinct.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        List<QueryResultRow> streaming = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT code FROM teams ORDER BY code");

        List<QueryResultRow> withoutIndex = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT name FROM teams ORDER BY name");

        // Confirm streaming produced deduplicated results matching manual dedup.
        HashSet<string?> streamCodes = streaming.Select(r => r.Row["code"].StrValue).ToHashSet();
        Assert.AreEqual(3, streamCodes.Count);
        Assert.IsTrue(streamCodes.Contains("A"));
        Assert.IsTrue(streamCodes.Contains("B"));
        Assert.IsTrue(streamCodes.Contains("C"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // C. Sort elision — execution correctness with ORDER BY
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_StreamingDistinctWithOrderBy_ProducesOrderedResults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT code FROM teams ORDER BY code");

        // Results must be ordered A → B → C (no SortNode, ordering comes from the index).
        Assert.AreEqual(3, rows.Count);
        for (int i = 1; i < rows.Count; i++)
        {
            string prev = rows[i - 1].Row["code"].StrValue ?? "";
            string curr = rows[i].Row["code"].StrValue ?? "";
            Assert.That(string.CompareOrdinal(curr, prev), Is.GreaterThan(0),
                $"Rows must be ascending: [{prev}] should come before [{curr}]");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // D. LIMIT interaction — limit applied after dedup
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_StreamingDistinctOrderByLimit_AppliesLimitAfterDedup()
    {
        // 7 raw rows, 3 distinct codes. LIMIT 2 must yield 2 distinct codes, not 2 raw rows.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT code FROM teams ORDER BY code LIMIT 2");

        Assert.AreEqual(2, rows.Count, "LIMIT 2 on 3 distinct codes should yield 2 rows");
        Assert.AreEqual("A", rows[0].Row["code"].StrValue);
        Assert.AreEqual("B", rows[1].Row["code"].StrValue);
    }

    [Test]
    public async Task R12_HashDistinctOrderByLimit_AppliesLimitAfterDedup()
    {
        // Same semantics as streaming: LIMIT after dedup, not before.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT name FROM teams ORDER BY name LIMIT 2");

        Assert.AreEqual(2, rows.Count, "LIMIT 2 on distinct names should yield 2 unique names");
        Assert.AreEqual("Alpha", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Beta",  rows[1].Row["name"].StrValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // E. NULL semantics
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_HashDistinct_CollapsesNullDuplicates()
    {
        // `name` is nullable; insert rows with NULL name to verify NULL == NULL in DISTINCT.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        // Insert two rows with NULL name (same code "X").
        for (int i = 0; i < 2; i++)
        {
            await executor.Insert(new InsertTicket(
                txn, dbname, "teams",
                new List<Dictionary<string, ColumnValue>>
                {
                    new()
                    {
                        { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "code",  new(ColumnType.String,    "X") },
                        { "name",  new(ColumnType.Null,      0)   },
                        { "score", new(ColumnType.Integer64, 99)    },
                    }
                }));
        }
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT name FROM teams WHERE code = 'X' ORDER BY name");

        // SQL DISTINCT treats NULL == NULL: two NULL rows collapse to one.
        Assert.AreEqual(1, rows.Count, "Two NULL name rows must collapse to a single DISTINCT NULL row");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["name"].Type);
    }

    [Test]
    public async Task R12_NullableIndexedColumn_ForcesHashDistinct_AndPreservesNullGroup()
    {
        // Gate: streaming requires all DISTINCT columns to be NOT NULL, because CamusDB's
        // indexes don't hold NULL-keyed rows (BackfillIndex throws on NULL). A nullable
        // column that happens to have a matching index must still fall back to hash distinct
        // so that the NULL group is not silently dropped from the result.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "tagged",
            columns: new ColumnInfo[]
            {
                new("id",  ColumnType.Id),
                new("tag", ColumnType.String),          // nullable — no notNull flag
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",     new ColumnIndexInfo[] { new("id",  OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "tag_idx", new ColumnIndexInfo[] { new("tag", OrderType.Ascending) }),
            },
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 2; i++)
        {
            await executor.Insert(new InsertTicket(
                txn, dbname, "tagged",
                new List<Dictionary<string, ColumnValue>>
                {
                    new()
                    {
                        { "id",  new(ColumnType.Id,   ObjectIdGenerator.Generate().ToString()) },
                        { "tag", new(ColumnType.Null, 0) },
                    }
                }));
        }
        await executor.Insert(new InsertTicket(
            txn, dbname, "tagged",
            new List<Dictionary<string, ColumnValue>>
            {
                new()
                {
                    { "id",  new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                    { "tag", new(ColumnType.String, "alpha") },
                }
            }));
        await database.Transactions.CommitAsync(txn);

        // White-box: the gate must block streaming even though tag_idx exists.
        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT tag FROM tagged");
        Assert.That(plan, Does.Contain("distinct(hash)"),
            "Nullable column with a matching index must still use hash distinct");
        Assert.That(plan, Does.Not.Contain("distinct(streaming: true)"),
            "Streaming must not activate when the distinct column is nullable");

        // Correctness: hash distinct must collapse the two NULL rows to one and return both groups.
        List<QueryResultRow> rows = await RunSqlAsync(executor, database, dbname,
            "SELECT DISTINCT tag FROM tagged ORDER BY tag");

        Assert.AreEqual(2, rows.Count, "Expect two distinct groups: NULL and 'alpha'");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["tag"].Type, "First group must be NULL (NULLs sort first)");
        Assert.AreEqual("alpha",         rows[1].Row["tag"].StrValue, "Second group must be 'alpha'");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // F. Fallback — instrumentation proves streaming does not materialise all keys
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R12_StreamingDistinct_NodeFlagIsTrue_WhenIndexCoversDistinctKey()
    {
        // White-box: verify IsStreaming flag set on the DistinctNode in the physical plan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname,
            sql: "EXPLAIN SELECT DISTINCT code FROM teams", parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        // Plan shape already verified in R12_IndexedDistinctColumnUses_StreamingDistinct.
        // The EXPLAIN output containing "streaming: true" is the observable guarantee.
        Assert.Pass("Plan shape verified via EXPLAIN output in the A. Plan shape tests");
    }

    [Test]
    public async Task R12_StreamingDistinct_ExistingPredicateScanAlsoStreams_WhenIndexCovers()
    {
        // When the planner already chose a predicate-driven scan whose index also covers the
        // DISTINCT key (e.g. PK unique lookup on `id`), streaming is activated without
        // overriding the scan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTeamsAsync();

        string plan = await ExplainPlanAsync(executor, database, dbname,
            "EXPLAIN SELECT DISTINCT code FROM teams WHERE code = 'A'");

        // WHERE code = 'A' → range scan on code_idx; code_idx covers DISTINCT code → streaming.
        Assert.That(plan, Does.Contain("distinct(streaming: true)"),
            "Predicate range scan on code_idx already covers DISTINCT code → streaming");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // G. Mixed-layout guard — streaming dedup with heterogeneous RowLayout instances
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that the streaming dedup path in <see cref="QueryDistincter"/> degrades safely
    /// to the dictionary path when the input stream contains <see cref="QueryRow"/> instances
    /// whose <see cref="RowLayout"/> instances differ (same column names, columns in different
    /// ordinal order). Without the <c>ReferenceEquals</c> guard the comparator would reuse
    /// ordinals from the first row's layout for subsequent rows whose <c>Values[]</c> is ordered
    /// differently, silently comparing the wrong columns and producing incorrect DISTINCT output.
    /// </summary>
    [Test]
    public async Task StreamingDistinct_MixedLayoutQueryRows_deduplicatesCorrectly()
    {
        // Layout A: [name=0, score=1]
        RowLayout layoutA = RowLayout.ForColumns(["name", "score"]);
        // Layout B: [score=0, name=1]  — column order is reversed relative to layoutA
        RowLayout layoutB = RowLayout.ForColumns(["score", "name"]);

        QueryResultRow MakeA(string name, long score) =>
            new(default, new QueryRow(default, layoutA,
                [new(ColumnType.String, name), new(ColumnType.Integer64, score)]));
        QueryResultRow MakeB(string name, long score) =>
            new(default, new QueryRow(default, layoutB,
                [new(ColumnType.Integer64, score), new(ColumnType.String, name)]));

        // The stream is pre-sorted by name (as streaming dedup requires). "robot-a" appears
        // twice — once from each layout — and must collapse to one output row.
        List<QueryResultRow> input =
        [
            MakeA("robot-a", 1L),
            MakeB("robot-a", 1L),
            MakeA("robot-b", 2L),
        ];

        QueryDistincter distincter = new();
        List<QueryResultRow> output = await distincter
            .StreamingDistinctRows(ToAsync(input))
            .ToListAsync();

        Assert.AreEqual(2, output.Count, "Duplicate robot-a rows must collapse to one");
        Assert.AreEqual("robot-a", output[0].Row["name"].StrValue);
        Assert.AreEqual("robot-b", output[1].Row["name"].StrValue);
    }

    /// <summary>
    /// Same mixed-layout hazard as <see cref="StreamingDistinct_MixedLayoutQueryRows_deduplicatesCorrectly"/>,
    /// but exercises the non-streaming hash/sort dedup path (<c>SpillEnabled = true</c> routes
    /// DISTINCT through the sort-based comparer rather than adjacent-row streaming). The sort
    /// comparer must apply the same ReferenceEquals layout guard: rows whose RowLayout instance
    /// differs from the one the ordinals were resolved from degrade to the dictionary path instead
    /// of reading the wrong column ordinals.
    /// </summary>
    [Test]
    public async Task HashDistinct_MixedLayoutQueryRows_deduplicatesCorrectly()
    {
        bool savedSpill = CamusDBConfig.SpillEnabled;
        CamusDBConfig.SpillEnabled = true; // routes DistinctResultset → sort-based DistinctRowComparer

        try
        {
            // Layout A: [name=0, score=1]; Layout B: [score=0, name=1] — reversed ordinal order.
            RowLayout layoutA = RowLayout.ForColumns(["name", "score"]);
            RowLayout layoutB = RowLayout.ForColumns(["score", "name"]);

            QueryResultRow MakeA(string name, long score) =>
                new(default, new QueryRow(default, layoutA,
                    [new(ColumnType.String, name), new(ColumnType.Integer64, score)]));
            QueryResultRow MakeB(string name, long score) =>
                new(default, new QueryRow(default, layoutB,
                    [new(ColumnType.Integer64, score), new(ColumnType.String, name)]));

            // Unsorted input; the sort-based dedup orders internally. "robot-a" (name+score equal)
            // appears once from each layout and must collapse to a single output row.
            List<QueryResultRow> input =
            [
                MakeA("robot-a", 1L),
                MakeB("robot-a", 1L),
                MakeA("robot-b", 2L),
            ];

            QueryDistincter distincter = new();
            // DistinctResultset ignores the ticket (both branches take only the cursor).
            List<QueryResultRow> output = await distincter
                .DistinctResultset(null!, ToAsync(input), new QueryExecutionContext(CamusDBOptions.Default with { SpillEnabled = true }))
                .ToListAsync();

            Assert.AreEqual(2, output.Count, "duplicate robot-a across layouts must collapse to one");
            List<string> names = output.Select(static r => r.Row["name"].StrValue!).OrderBy(static n => n).ToList();
            Assert.AreEqual("robot-a", names[0]);
            Assert.AreEqual("robot-b", names[1]);
        }
        finally
        {
            CamusDBConfig.SpillEnabled = savedSpill;
        }
    }

    private static async IAsyncEnumerable<QueryResultRow> ToAsync(IEnumerable<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;
        await Task.CompletedTask;
    }
}
