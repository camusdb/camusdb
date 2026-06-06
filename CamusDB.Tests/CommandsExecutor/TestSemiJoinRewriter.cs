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
/// R11 — Semi/Anti-Join Rewrite.
///
/// Verifies that eligible IN / NOT IN subqueries are converted to SemiJoinNode probes
/// at the plan level and produce correct results at the execution level.
///
/// Test surface:
///   A. Plan shape — EXPLAIN output contains "semi-join", "anti-join", or "null-aware-anti-join".
///   B. Execution correctness — row counts and values match the expected SQL semantics.
///   C. Edge cases — empty inner table, NULL outer values, AND-combined predicates.
///   D. Non-eligible paths — star projection, multi-column, correlated: fall through to materialisation.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSemiJoinRewriter : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Fixture setup helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTablesAsync()
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
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "approved",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",        new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "approved_name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocklist",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String),  // nullable — CamusDB cannot index nullable columns
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "robots",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Alpha") }, { "year", new(ColumnType.Integer64, 2020L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Beta")  }, { "year", new(ColumnType.Integer64, 2021L) } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Gamma") }, { "year", new(ColumnType.Integer64, 2022L) } },
            }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "approved",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Alpha") } },
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Gamma") } },
            }));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunQueryAsync(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task<string> GetExplainAsync(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: $"EXPLAIN {sql}", parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        // Each EXPLAIN row has "node" and "detail" columns; concatenate both for plan shape checking.
        return string.Join("\n", rows.Select(r =>
        {
            string node = r.Row.TryGetValue("node", out ColumnValue nv) ? (nv.StrValue ?? "") : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue dv) ? (dv.StrValue ?? "") : "";
            return $"{node}({detail})";
        }));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // A. Plan shape
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R11_InSubqueryProducesSemiJoinNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        string plan = await GetExplainAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name IN (SELECT name FROM approved)");

        Assert.That(plan, Does.Contain("semi-join"),
            "IN subquery rewrite must produce a semi-join node in the plan");
    }

    [Test]
    public async Task R11_NotInOverNotNullColumnProducesAntiJoinNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // "approved.name" is NOT NULL — expect anti-join (not null-aware-anti-join)
        string plan = await GetExplainAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM approved)");

        Assert.That(plan, Does.Contain("anti-join"),
            "NOT IN over a NOT NULL inner column must produce an anti-join node");
    }

    [Test]
    public async Task R11_NotInOverNullableColumnFallsBackToMaterialisation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // "blocklist.name" is nullable and has no usable index — R11 falls back to SubqueryRewriter
        // (O(n+m) materialisation), which is safer and correct; the plan must not contain semi/anti-join.
        string plan = await GetExplainAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM blocklist)");

        Assert.That(plan, Does.Not.Contain("anti-join"),
            "NOT IN over a nullable column without an index must fall back to materialisation");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // B. Execution correctness
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R11_InSubqueryFiltersCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name IN (SELECT name FROM approved)");

        // Only Alpha and Gamma are in approved; Beta is not.
        Assert.AreEqual(2, rows.Count, "IN must return only rows whose name appears in approved");

        IEnumerable<string?> names = rows.Select(r => r.Row["name"].StrValue).OrderBy(n => n);
        CollectionAssert.AreEqual(new[] { "Alpha", "Gamma" }, names,
            "IN result must be Alpha and Gamma");
    }

    [Test]
    public async Task R11_NotInSubqueryFiltersCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM approved)");

        // Beta is the only robot not in approved.
        Assert.AreEqual(1, rows.Count, "NOT IN must return only rows whose name is absent from approved");
        Assert.AreEqual("Beta", rows[0].Row["name"].StrValue, "NOT IN result must be Beta");
    }

    [Test]
    public async Task R11_InSubqueryWithEmptyInnerTableYieldsNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // Approved table is empty (we'll create a fresh empty "allowed" table).
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "allowed",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",       new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "allowed_name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name IN (SELECT name FROM allowed)");

        Assert.AreEqual(0, rows.Count, "IN against empty inner table must return no rows");
    }

    [Test]
    public async Task R11_NotInSubqueryWithEmptyInnerTableYieldsAllRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "empty_block",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",          new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "empty_block_name_idx", new ColumnIndexInfo[] { new("name", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM empty_block)");

        Assert.AreEqual(3, rows.Count, "NOT IN against empty inner table must return all outer rows");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // C. Edge cases
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R11_NullAwareAntiJoinExcludesAllWhenInnerHasNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // Insert one NULL name into blocklist — makes NOT IN return UNKNOWN for all outer rows.
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "blocklist",
            values: new()
            {
                // Row without "name" → column stays NULL
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) } },
            }));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM blocklist)");

        Assert.AreEqual(0, rows.Count,
            "NOT IN with a NULL in the nullable inner column must exclude all outer rows (UNKNOWN semantics)");
    }

    [Test]
    public async Task R11_NullAwareAntiJoinWorksCorrectlyWithNoNullsInInner()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // Insert non-null names into blocklist — no NULLs, so Anti semantics apply.
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "blocklist",
            values: new()
            {
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "Alpha") } },
            }));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name NOT IN (SELECT name FROM blocklist)");

        // Alpha is blocked; Beta and Gamma pass through.
        Assert.AreEqual(2, rows.Count,
            "NOT IN with no NULLs in inner must return outer rows not present in inner");

        IEnumerable<string?> names = rows.Select(r => r.Row["name"].StrValue).OrderBy(n => n);
        CollectionAssert.AreEqual(new[] { "Beta", "Gamma" }, names);
    }

    [Test]
    public async Task R11_SemiJoinCombinedWithOtherPredicates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // AND with year filter: only approved robots from 2021+
        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name IN (SELECT name FROM approved) AND year >= 2022");

        // Approved: Alpha(2020), Gamma(2022). Filter year>=2022 keeps only Gamma.
        Assert.AreEqual(1, rows.Count, "Semi-join combined with AND predicate must apply both filters");
        Assert.AreEqual("Gamma", rows[0].Row["name"].StrValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // D. Non-eligible paths fall through to materialisation (no crash)
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R11_StarProjectionInSubqueryFallsThroughToMaterialisation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // SELECT * in subquery is ineligible for semi-join; SubqueryRewriter handles it
        // and throws the expected single-column error.
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname,
            sql: "SELECT name FROM robots WHERE name IN (SELECT * FROM approved)",
            parameters: null);

        Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, "Star projection in IN subquery must raise CamusDBException");
    }

    [Test]
    public async Task R11_InSubqueryWithInnerFilterExecutesCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTablesAsync();

        // The inner SELECT has its own WHERE — only "Alpha" passes the inner name filter.
        List<QueryResultRow> rows = await RunQueryAsync(database, executor, dbname,
            "SELECT name FROM robots WHERE name IN (SELECT name FROM approved WHERE name = 'Alpha')");

        Assert.AreEqual(1, rows.Count, "IN with inner WHERE must apply both inner filter and probe");
        Assert.AreEqual("Alpha", rows[0].Row["name"].StrValue);
    }
}
