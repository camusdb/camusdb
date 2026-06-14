
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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// PC4 acceptance tests — hit/miss counters observable, parameterized-query correctness
/// with a shared cached AST, and schema-binding re-resolution after caching.
/// Counter tests use a local <see cref="SqlParserCache"/> instance (no shared state).
/// Integration tests use <see cref="CommandExecutor"/> via <see cref="CommandsExecutor.SharedNodeBaseTest"/>.
/// </summary>
[TestFixture]
public sealed class TestSQLParserCacheObservability : CommandsExecutor.SharedNodeBaseTest
{
    // ── Counter observability ─────────────────────────────────────────────────

    [Test]
    public async Task HitCounter_Increments_AfterSecondParseOfSameSql()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        const string sql = "SELECT id FROM counter_hit_test WHERE x = 1";

        long hitsBefore = cache.Hits;

        SQLParserProcessor.Parse(sql, cache);   // miss — populates cache
        SQLParserProcessor.Parse(sql, cache);   // hit

        Assert.That(cache.Hits, Is.GreaterThan(hitsBefore),
            "Hits counter must increment on a cache hit");
    }

    [Test]
    public async Task MissCounter_Increments_OnFirstParseOfNewSql()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        string sql = $"SELECT id FROM miss_counter_test WHERE epoch = {System.Environment.TickCount64}";

        long missesBefore = cache.Misses;

        SQLParserProcessor.Parse(sql, cache);

        Assert.That(cache.Misses, Is.GreaterThan(missesBefore),
            "Misses counter must increment on a cache miss (first parse of a new SQL text)");
    }

    [Test]
    public async Task HitCounter_SameReference_ReturnedOnCacheHit()
    {
        await using SqlParserCache cache = new(null, ttlSeconds: 300, maxEntries: 2048, sweepSeconds: 60);
        const string sql = "SELECT id FROM ref_test WHERE active = true";

        NodeAst first  = SQLParserProcessor.Parse(sql, cache);
        NodeAst second = SQLParserProcessor.Parse(sql, cache);   // cache hit

        Assert.That(second, Is.SameAs(first),
            "A cache hit must return the same NodeAst reference, not a fresh parse");
        Assert.That(cache.Hits, Is.GreaterThanOrEqualTo(1));
    }

    // ── Parameterized-query integration: shared AST, per-call parameter values ─

    [Test]
    public async Task ParameterizedQuery_SharedAst_DifferentParamValues_CorrectResults()
    {
        // Arrange — create a table and two rows with distinct integer keys.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "widgets",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("value", ColumnType.Integer64),
                new("label", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        string idA = ObjectIdGenerator.Generate().ToString();
        string idB = ObjectIdGenerator.Generate().ToString();

        await executor.Insert(new InsertTicket(
            txnState: setup,
            databaseName: dbname,
            tableName: "widgets",
            values: new()
            {
                new()
                {
                    { "id",    new(ColumnType.Id, idA) },
                    { "value", new(ColumnType.Integer64, 10L) },
                    { "label", new(ColumnType.String, "ten") },
                },
                new()
                {
                    { "id",    new(ColumnType.Id, idB) },
                    { "value", new(ColumnType.Integer64, 20L) },
                    { "label", new(ColumnType.String, "twenty") },
                },
            }));

        await database.Transactions.CommitAsync(setup);

        const string paramSql = "SELECT label FROM widgets WHERE value = @val";

        // First call — cache miss in CommandExecutor's own cache.
        List<QueryResultRow> rowsA = await RunSelect(executor, database, dbname, paramSql,
            new() { { "@val", new(ColumnType.Integer64, 10L) } });

        Assert.That(rowsA, Has.Count.EqualTo(1));
        Assert.That(rowsA[0].Row["label"].StrValue, Is.EqualTo("ten"),
            "First query must return the row with value=10");

        // Second call with a different parameter value — must return a different row,
        // proving the shared cached AST does not carry over the previous parameter.
        List<QueryResultRow> rowsB = await RunSelect(executor, database, dbname, paramSql,
            new() { { "@val", new(ColumnType.Integer64, 20L) } });

        Assert.That(rowsB, Has.Count.EqualTo(1));
        Assert.That(rowsB[0].Row["label"].StrValue, Is.EqualTo("twenty"),
            "Shared cached AST must not carry over the first call's parameter value");
    }

    // ── Binding re-resolves after caching — new rows are visible ─────────────

    [Test]
    public async Task CachedAst_BindingRunsEachTime_NewRowsVisible()
    {
        // Proves that caching the AST does NOT cache query results or schema snapshots.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(
            txnState: setup,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "first") },
                },
            }));

        await database.Transactions.CommitAsync(setup);

        const string sql = "SELECT name FROM items";

        // First query — cache miss — should return 1 row.
        List<QueryResultRow> before = await RunSelect(executor, database, dbname, sql, null);
        Assert.That(before, Has.Count.EqualTo(1));

        // Insert a second row after the AST is cached.
        KvTransaction txn2 = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn2,
            databaseName: dbname,
            tableName: "items",
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "second") },
                },
            }));
        await database.Transactions.CommitAsync(txn2);

        // Second query — cache hit — must see the new row (AST cached, not results).
        List<QueryResultRow> after = await RunSelect(executor, database, dbname, sql, null);
        Assert.That(after, Has.Count.EqualTo(2),
            "A cached AST must not freeze result-set contents; binding and execution must re-run each call");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<List<QueryResultRow>> RunSelect(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: parameters);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }
}
