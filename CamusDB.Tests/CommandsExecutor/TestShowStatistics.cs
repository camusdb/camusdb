/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>SHOW STATISTICS FOR &lt;table&gt;</c>, driven through
/// <see cref="ExecuteSQLTicket"/> so the statement is exercised the way a console session reaches it —
/// parse, dispatch, statistics read and row projection included.
///
/// <para><c>[NonParallelizable]</c> because each test boots an embedded Kahuna node.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowStatistics : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Creates "robots": id (Id PK), name (String), year (Integer64, indexed).</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

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
                new(ConstraintType.PrimaryKey, "~pk",      new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "year_idx", new ColumnIndexInfo[] { new("year", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task InsertRobots(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int count, int baseYear = 2000)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(baseYear + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<TableDescriptor> OpenTable(DatabaseDescriptor db, string tableName)
    {
        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;
        throw new InvalidOperationException($"Table '{tableName}' not found");
    }

    /// <summary>Runs a row-returning statement and captures both the rows and the declared schema.</summary>
    private static async Task<(List<QueryResultRow> rows, IReadOnlyList<DerivedColumnSchema>? schema)> Query(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null),
            schemaOut: schemaHolder);

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await database.Transactions.CommitAsync(txn);
        return (rows, schemaHolder.Schema);
    }

    private static async Task Analyze(CommandExecutor executor, DatabaseDescriptor database, string dbname, string table)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: $"ANALYZE {table}", parameters: null));
        await foreach (QueryResultRow _ in cursor) { }
        await database.Transactions.CommitAsync(txn);
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.StrValue : null;

    private static long? Number(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.LongValue : null;

    private static QueryResultRow Single(List<QueryResultRow> rows, string kind, string? target = null)
        => rows.Single(r => Text(r, "kind") == kind && (target is null || Text(r, "target") == target));

    // ─────────────────────────────────────────────────────────────────────────
    // Syntax
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>Both spellings are the same statement; TABLE is a noise word.</summary>
    [Test]
    public async Task BothSyntaxFormsAreAccepted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 5);

        (List<QueryResultRow> bare, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");
        (List<QueryResultRow> noiseWord, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR TABLE robots");

        Assert.AreEqual(bare.Count, noiseWord.Count);
        Assert.AreEqual(5, Number(Single(bare, "table"), "estimated_rows"));
        Assert.AreEqual(5, Number(Single(noiseWord, "table"), "estimated_rows"));
    }

    /// <summary>A wrong leading word must explain the statement rather than fail as a parse error.</summary>
    [Test]
    public async Task UnknownLeadingWordIsRejectedWithGuidance()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW STATISTIKS FOR robots"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("SHOW STATISTICS FOR", ex.Message);
    }

    /// <summary>An unknown table is an error, never an empty result — the two must stay distinguishable.</summary>
    [Test]
    public async Task UnknownTableIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW STATISTICS FOR nosuchtable"));
    }

    /// <summary>
    /// The word "statistics" is matched as an identifier, not tokenized as a keyword, so it must
    /// remain usable as a table name, a column name and an alias. This is the entire reason the
    /// grammar validates the word in the parser action instead of adding a token.
    /// </summary>
    [Test]
    public async Task StatisticsRemainsUsableAsAnIdentifier()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "statistics",
            columns: new ColumnInfo[]
            {
                new("id",         ColumnType.Id),
                new("statistics", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        (List<QueryResultRow> selected, _) = await Query(
            executor, database, dbname, "SELECT statistics FROM statistics AS statistics");
        Assert.AreEqual(0, selected.Count);

        // And the statement still resolves a table that happens to carry the same name.
        (List<QueryResultRow> shown, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR statistics");
        Assert.AreEqual("statistics", Text(Single(shown, "table"), "table"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Result content
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A table nothing has been written to still answers, with the table row carrying NULLs. Returning
    /// no rows would be indistinguishable from a statement that matched nothing.
    /// </summary>
    [Test]
    public async Task NeverAnalyzedTableReportsATableRowOfNulls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        QueryResultRow tableRow = Single(rows, "table");
        Assert.AreEqual("robots", Text(tableRow, "table"));
        Assert.IsNull(Text(tableRow, "target"), "the table row has no target");
        Assert.IsNull(Number(tableRow, "distinct_count"));
        Assert.IsNull(Number(tableRow, "histogram_buckets"));
        Assert.IsNull(Text(tableRow, "last_analyzed"), "never analyzed");
        Assert.IsFalse(rows.Any(r => Text(r, "kind") == "column"),
            "no column has been observed yet, so no column row may be emitted");
    }

    /// <summary>
    /// The row count comes from this node's live counters, so inserts are visible before any flush to
    /// storage. Reading only the persisted blob would report a stale zero here.
    /// </summary>
    [Test]
    public async Task RowCountReflectsUnflushedInserts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 20);

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        Assert.AreEqual(20, Number(Single(rows, "table"), "estimated_rows"));
    }

    /// <summary>Every index reports its entry count, keyed by index name.</summary>
    [Test]
    public async Task IndexRowsCarryEntryCounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 12);

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        Assert.AreEqual(12, Number(Single(rows, "index", "~pk"), "estimated_rows"));
        Assert.AreEqual(12, Number(Single(rows, "index", "year_idx"), "estimated_rows"));
    }

    /// <summary>
    /// ANALYZE is what produces histograms and distinct counts, so they must appear only after it runs —
    /// and the analyze timestamp must appear with them.
    /// </summary>
    [Test]
    public async Task AnalyzePublishesColumnEstimatesAndTimestamp()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 20);

        (List<QueryResultRow> before, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");
        Assert.IsNull(Text(Single(before, "table"), "last_analyzed"));
        Assert.IsFalse(before.Any(r => Number(r, "histogram_buckets") is not null),
            "histograms must not exist before ANALYZE");

        await Analyze(executor, database, dbname, "robots");

        (List<QueryResultRow> after, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        QueryResultRow yearRow = Single(after, "column", "year");
        Assert.AreEqual(20, Number(yearRow, "distinct_count"), "20 distinct years were inserted");
        Assert.Greater(Number(yearRow, "histogram_buckets") ?? 0, 0, "ANALYZE must build a histogram");
        Assert.AreEqual("2000", Text(yearRow, "min_value"));
        Assert.AreEqual("2019", Text(yearRow, "max_value"));
        Assert.IsNotNull(Text(Single(after, "table"), "last_analyzed"), "the analyze timestamp must be recorded");
    }

    /// <summary>
    /// Composite indexes produce key-tuple estimates for every prefix, which is what corrects the
    /// independence assumption for correlated equality predicates. They arrive as their own rows.
    /// </summary>
    [Test]
    public async Task CompositeIndexProducesKeyTupleRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "places",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("city", ColumnType.String, notNull: true),
                new("zip",  ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "city_zip_idx", new ColumnIndexInfo[]
                {
                    new("city", OrderType.Ascending),
                    new("zip",  OrderType.Ascending),
                }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 10; i++)
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "places",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "city", new(ColumnType.String, "City" + i) },
                        { "zip",  new(ColumnType.Integer64, (long)(10000 + i)) },
                    }
                }));
        await database.Transactions.CommitAsync(txn);

        await Analyze(executor, database, dbname, "places");

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR places");

        QueryResultRow keyRow = Single(rows, "key", "city,zip");
        Assert.AreEqual(10, Number(keyRow, "distinct_count"), "every (city, zip) pair is distinct");
    }

    /// <summary>
    /// Bounds render as the literal that produced them, using the same formatters the value prints with
    /// elsewhere. Types that carry no ordered payload (Bool) render NULL rather than a fabricated value.
    /// </summary>
    [Test]
    public async Task BoundsRenderPerColumnType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "typed",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("num",   ColumnType.Integer64),
                new("dbl",   ColumnType.Float64),
                new("txt",   ColumnType.String),
                new("day",   ColumnType.Date),
                new("stamp", ColumnType.DateTime),
                new("uid",   ColumnType.Uuid),
                new("flag",  ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",       new ColumnIndexInfo[] { new("id",    OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "num_idx",   new ColumnIndexInfo[] { new("num",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "dbl_idx",   new ColumnIndexInfo[] { new("dbl",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "txt_idx",   new ColumnIndexInfo[] { new("txt",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "day_idx",   new ColumnIndexInfo[] { new("day",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "stamp_idx", new ColumnIndexInfo[] { new("stamp", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "uid_idx",   new ColumnIndexInfo[] { new("uid",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "flag_idx",  new ColumnIndexInfo[] { new("flag",  OrderType.Ascending) }),
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        DateTime firstDay = new(2020, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        DateTime lastDay = new(2021, 3, 4, 0, 0, 0, DateTimeKind.Utc);
        DateTime firstStamp = new(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        DateTime lastStamp = new(2021, 3, 4, 5, 6, 7, DateTimeKind.Utc);
        Guid lowUuid = Guid.Parse("00000000-0000-0000-0000-000000000001");
        Guid highUuid = Guid.Parse("ffffffff-0000-0000-0000-000000000002");

        txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn, databaseName: dbname, tableName: "typed",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "num",   new(ColumnType.Integer64, -7L) },
                    { "dbl",   new(ColumnType.Float64, 1.5d) },
                    { "txt",   new(ColumnType.String, "árbol") },
                    { "day",   new(ColumnType.Date, firstDay.Ticks) },
                    { "stamp", new(ColumnType.DateTime, firstStamp.Ticks) },
                    { "uid",   ColumnValue.FromUuid(lowUuid) },
                    { "flag",  new(ColumnType.Bool, false) },
                }
            }));
        await executor.Insert(new InsertTicket(
            txnState: txn, databaseName: dbname, tableName: "typed",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "num",   new(ColumnType.Integer64, 42L) },
                    { "dbl",   new(ColumnType.Float64, 9.25d) },
                    { "txt",   new(ColumnType.String, "zebra") },
                    { "day",   new(ColumnType.Date, lastDay.Ticks) },
                    { "stamp", new(ColumnType.DateTime, lastStamp.Ticks) },
                    { "uid",   ColumnValue.FromUuid(highUuid) },
                    { "flag",  new(ColumnType.Bool, true) },
                }
            }));
        await database.Transactions.CommitAsync(txn);

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR typed");

        Assert.AreEqual("-7", Text(Single(rows, "column", "num"), "min_value"));
        Assert.AreEqual("42", Text(Single(rows, "column", "num"), "max_value"));
        Assert.AreEqual("1.5", Text(Single(rows, "column", "dbl"), "min_value"));
        Assert.AreEqual("9.25", Text(Single(rows, "column", "dbl"), "max_value"));
        // String bounds are ordinal, matching the order the index itself uses — so a non-ASCII
        // "árbol" (U+00E1) sorts above "zebra" rather than before it, as a linguistic collation
        // would have it. Asserting the ordinal answer here is what keeps the reported bounds
        // consistent with the scan they describe.
        Assert.AreEqual("zebra", Text(Single(rows, "column", "txt"), "min_value"));
        Assert.AreEqual("árbol", Text(Single(rows, "column", "txt"), "max_value"));
        Assert.AreEqual("2020-01-02", Text(Single(rows, "column", "day"), "min_value"));
        Assert.AreEqual("2021-03-04", Text(Single(rows, "column", "day"), "max_value"));
        Assert.AreEqual(firstStamp.ToString("o"), Text(Single(rows, "column", "stamp"), "min_value"));
        Assert.AreEqual(lastStamp.ToString("o"), Text(Single(rows, "column", "stamp"), "max_value"));
        Assert.AreEqual(lowUuid.ToString("D"), Text(Single(rows, "column", "uid"), "min_value"));
        Assert.AreEqual(highUuid.ToString("D"), Text(Single(rows, "column", "uid"), "max_value"));

        // Bool is not an ordered type, so no min/max is tracked and no column row is produced for it.
        Assert.IsFalse(rows.Any(r => Text(r, "kind") == "column" && Text(r, "target") == "flag"),
            "an unordered column must not report bounds it does not have");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Wire shape
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The declared column schema is what both transports send to clients ahead of the positional rows,
    /// so its names, order and types are part of the statement's contract.
    /// </summary>
    [Test]
    public async Task ColumnSchemaIsDeclared()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (_, IReadOnlyList<DerivedColumnSchema>? schema) = await Query(
            executor, database, dbname, "SHOW STATISTICS FOR robots");

        Assert.IsNotNull(schema, "a transport needs the schema to encode positional rows");

        Assert.AreEqual(
            new[] { "table", "kind", "target", "estimated_rows", "distinct_count",
                    "min_value", "max_value", "histogram_buckets", "last_analyzed", "stale_mutations" },
            schema!.Select(c => c.Name).ToArray());

        Assert.AreEqual(
            new[] { ColumnType.String, ColumnType.String, ColumnType.String, ColumnType.Integer64,
                    ColumnType.Integer64, ColumnType.String, ColumnType.String, ColumnType.Integer64,
                    ColumnType.String, ColumnType.Integer64 },
            schema!.Select(c => c.Type).ToArray());
    }

    /// <summary>Rows arrive grouped and ordered so a console reads top-down: table, columns, keys, indexes.</summary>
    [Test]
    public async Task RowsAreEmittedInAStableOrder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 8);
        await Analyze(executor, database, dbname, "robots");

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        List<string> kinds = rows.Select(r => Text(r, "kind")!).ToList();
        Assert.AreEqual("table", kinds[0]);

        List<string> distinctInOrder = kinds.Distinct().ToList();
        List<string> expectedOrder = new[] { "table", "column", "key", "index" }
            .Where(distinctInOrder.Contains).ToList();
        Assert.AreEqual(expectedOrder, distinctInOrder, "kinds must be grouped, not interleaved");

        List<string> indexTargets = rows.Where(r => Text(r, "kind") == "index").Select(r => Text(r, "target")!).ToList();
        Assert.AreEqual(indexTargets.OrderBy(t => t, StringComparer.Ordinal).ToList(), indexTargets);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Storage path
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A node that is not tracking a table answers from the persisted blob — and must not start
    /// tracking it as a side effect. Inspection that makes every inspected table resident is how a
    /// fleet-wide sweep turns into unbounded memory growth.
    /// </summary>
    [Test]
    public async Task ColdCacheReadsStorageWithoutCaching()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 15);
        await Analyze(executor, database, dbname, "robots");

        TableDescriptor table = await OpenTable(database, "robots");
        executor.Statistics.EvictForTesting(database, table);

        int cachedBefore = executor.Statistics.CachedTableCount;

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robots");

        Assert.AreEqual(15, Number(Single(rows, "table"), "estimated_rows"),
            "the persisted blob must answer when the node holds no entry");
        Assert.IsNotNull(Text(Single(rows, "table"), "last_analyzed"));
        Assert.Greater(Number(Single(rows, "column", "year"), "histogram_buckets") ?? 0, 0);
        Assert.AreEqual(cachedBefore, executor.Statistics.CachedTableCount,
            "reading statistics for display must not create a cache entry");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Relations that are not tables
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>REFRESH is not DDL — it writes rows and reports a count — so it takes the non-query path.</summary>
    private static async Task NonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A plain view stores no rows, so it has no statistics of its own. The error must say that
    /// rather than reuse the write path's "cannot be written to", which would be wrong twice over —
    /// this is a read, and the reason has nothing to do with updatability.
    /// </summary>
    [Test]
    public async Task ViewIsRejectedWithItsOwnReason()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 3);
        await Ddl(database, executor, dbname, "CREATE VIEW recent_robots AS SELECT id, year FROM robots");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Query(executor, database, dbname, "SHOW STATISTICS FOR recent_robots"))!;

        StringAssert.Contains("has no statistics of its own", ex.Message);
        StringAssert.DoesNotContain("cannot be written to", ex.Message);
    }

    /// <summary>
    /// A materialized view is a physical relation holding real rows, so it is a valid target and
    /// reports its own statistics — not those of the tables its body reads, and without waiting for
    /// an ANALYZE. The population counts the rows as it writes them; the refresh hands those counts
    /// to the view when it adopts the key-space they describe.
    /// </summary>
    [Test]
    public async Task MaterializedViewReportsItsOwnStatistics()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 7);
        await Ddl(database, executor, dbname, "CREATE MATERIALIZED VIEW robot_years AS SELECT id, year FROM robots");

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robot_years");

        Assert.AreEqual("robot_years", Text(Single(rows, "table"), "table"));
        Assert.AreEqual(7, Number(Single(rows, "table"), "estimated_rows"),
            "the rows the population wrote are the view's own row count");
        Assert.IsNull(Text(Single(rows, "table"), "last_analyzed"),
            "a refresh counts rows; it does not build distributions, so it must not claim to have analyzed");
    }

    /// <summary>
    /// A refresh replaces contents rather than adding to them, so the adopted counts must replace the
    /// view's previous ones. Accumulating instead would inflate the row count by every refresh ever
    /// run — the failure mode of writing the population's counts as a delta onto what the retired
    /// contents left behind.
    /// </summary>
    [Test]
    public async Task RefreshReplacesStatisticsRatherThanAccumulating()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 7);
        await Ddl(database, executor, dbname, "CREATE MATERIALIZED VIEW robot_years AS SELECT id, year FROM robots");

        await InsertRobots(executor, database, dbname, 3, baseYear: 2100);
        await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW robot_years");

        (List<QueryResultRow> rows, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robot_years");

        Assert.AreEqual(10, Number(Single(rows, "table"), "estimated_rows"),
            "the view now holds 10 rows — not 7 + 10 accumulated across two populations");
    }

    /// <summary>
    /// Histograms and the analyze timestamp describe a distribution, and a refresh replaces the data
    /// that distribution described. Carrying them across would be the worst of both worlds: a stale
    /// shape presented as current, under a `last_analyzed` recent enough to be believed.
    /// </summary>
    [Test]
    public async Task RefreshDiscardsDistributionsOfRetiredContents()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await InsertRobots(executor, database, dbname, 12);
        await Ddl(database, executor, dbname, "CREATE MATERIALIZED VIEW robot_years AS SELECT id, year FROM robots");
        await Analyze(executor, database, dbname, "robot_years");

        (List<QueryResultRow> analyzed, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robot_years");
        Assert.IsNotNull(Text(Single(analyzed, "table"), "last_analyzed"), "precondition: the view was analyzed");
        Assert.IsTrue(analyzed.Any(r => Number(r, "histogram_buckets") is > 0), "precondition: a histogram exists");

        await NonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW robot_years");

        (List<QueryResultRow> refreshed, _) = await Query(executor, database, dbname, "SHOW STATISTICS FOR robot_years");

        Assert.AreEqual(12, Number(Single(refreshed, "table"), "estimated_rows"));
        Assert.IsNull(Text(Single(refreshed, "table"), "last_analyzed"),
            "the analyze described contents that no longer exist");
        Assert.IsFalse(refreshed.Any(r => Number(r, "histogram_buckets") is not null),
            "no histogram may survive the contents it described");
        Assert.Greater(Number(Single(refreshed, "table"), "stale_mutations") ?? 0, 0,
            "a freshly repopulated, never-analyzed view must look stale so the background collector picks it up");
    }
}
