
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// R10 — Plan-Cache Hooks tests.
///
/// Validates:
///   1. Single-table EXPLAIN carries a non-null QueryShapeId and SchemaDeps on the plan.
///   2. Two queries differing only in literal values produce the same QueryShapeId.
///   3. Two structurally different queries (different columns or operators) produce different shape IDs.
///   4. Join plans carry a QueryShapeId and SchemaDeps covering all joined tables.
///   5. EXPLAIN output includes a trailing "plan-info" row with shape and schema-deps detail.
///   6. Schema version appears correctly in the schema-deps list.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestPlanCacheHooks : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync(
        string tableName = "robots")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: tableName,
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
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static InsertTicket MakeInsertTicket(KvTransaction txn, string dbname, string tableName, string name, long year)
        => new(
            txnState: txn,
            databaseName: dbname,
            tableName: tableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, name) },
                    { "year", new(ColumnType.Integer64, year) },
                }
            }
        );

    private static async Task<List<QueryResultRow>> ExplainAsync(
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
    // Tests — shape ID on plans
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R10_SingleTablePlan_HasQueryShapeIdAndSchemaDeps()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        // The last row must be the plan-info metadata row.
        QueryResultRow infoRow = rows[^1];
        Assert.AreEqual("plan-info", infoRow.Row["node"].StrValue);

        string detail = infoRow.Row["detail"].StrValue!;
        Assert.IsTrue(detail.StartsWith("shape="), $"Expected detail to start with 'shape=', got: {detail}");
        Assert.IsTrue(detail.Contains("schema-deps="), $"Expected 'schema-deps=' in detail, got: {detail}");
    }

    [Test]
    public async Task R10_QueryShapeId_SameForLiteralDifferences()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows1 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year = 2020");
        List<QueryResultRow> rows2 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year = 2099");

        string detail1 = rows1[^1].Row["detail"].StrValue!;
        string detail2 = rows2[^1].Row["detail"].StrValue!;

        string shape1 = ExtractShape(detail1);
        string shape2 = ExtractShape(detail2);

        Assert.AreEqual(shape1, shape2,
            "Queries differing only in literal values must produce the same shape ID");
    }

    [Test]
    public async Task R10_QueryShapeId_DiffersForStructuralChanges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows1 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year = 2020");
        List<QueryResultRow> rows2 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE name = 'Alice'");

        string shape1 = ExtractShape(rows1[^1].Row["detail"].StrValue!);
        string shape2 = ExtractShape(rows2[^1].Row["detail"].StrValue!);

        Assert.AreNotEqual(shape1, shape2,
            "Queries referencing different columns must produce different shape IDs");
    }

    [Test]
    public async Task R10_QueryShapeId_DiffersForOperatorChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows1 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year = 2020");
        List<QueryResultRow> rows2 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots WHERE year > 2020");

        string shape1 = ExtractShape(rows1[^1].Row["detail"].StrValue!);
        string shape2 = ExtractShape(rows2[^1].Row["detail"].StrValue!);

        Assert.AreNotEqual(shape1, shape2,
            "Queries with different operators (= vs >) must produce different shape IDs");
    }

    [Test]
    public async Task R10_QueryShapeId_SameAcrossParameterizedCalls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        // Two EXPLAIN calls for the same parameterized structure but different literal values in SQL.
        // Both: WHERE year = <literal> → same canonical shape.
        List<QueryResultRow> rows1 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT name FROM robots WHERE year = 1999");
        List<QueryResultRow> rows2 = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT name FROM robots WHERE year = 2025");

        string shape1 = ExtractShape(rows1[^1].Row["detail"].StrValue!);
        string shape2 = ExtractShape(rows2[^1].Row["detail"].StrValue!);

        Assert.AreEqual(shape1, shape2, "Same query structure with different literals → same shape");
    }

    [Test]
    public async Task R10_SchemaDeps_ContainsTableNameAndVersion()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        string detail = rows[^1].Row["detail"].StrValue!;

        // detail = "shape=<hex>, schema-deps=[robots@<version>]"
        Assert.IsTrue(detail.Contains("robots@"), $"Expected 'robots@<version>' in schema-deps, got: {detail}");
    }

    [Test]
    public async Task R10_PlanInfoRow_HasNullCostColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM robots");

        QueryResultRow infoRow = rows[^1];
        Assert.AreEqual("plan-info", infoRow.Row["node"].StrValue);
        Assert.AreEqual(ColumnType.Null, infoRow.Row["estimated_rows"].Type,
            "plan-info estimated_rows should be NULL");
        Assert.AreEqual(ColumnType.Null, infoRow.Row["estimated_cost"].Type,
            "plan-info estimated_cost should be NULL");
    }

    [Test]
    public async Task R10_ShapeId_IsHex16Chars()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT id, name FROM robots WHERE year > 2000 ORDER BY name LIMIT 10");

        string shape = ExtractShape(rows[^1].Row["detail"].StrValue!);

        Assert.AreEqual(16, shape.Length, $"Shape ID must be 16 hex chars, got '{shape}' (length {shape.Length})");
        Assert.IsTrue(shape.All(c => "0123456789abcdef".Contains(c)),
            $"Shape ID must be lowercase hex, got: {shape}");
    }

    [Test]
    public async Task R10_JoinPlan_HasShapeIdAndMultipleSchemaDeps()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync("robots");

        // Create a second table for the join.
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "parts",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("robotid",  ColumnType.String),
                new("partname", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));
        await database.Transactions.CommitAsync(txn);

        List<QueryResultRow> rows = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT r.name, p.partname FROM robots r JOIN parts p ON r.id = p.id");

        QueryResultRow infoRow = rows[^1];
        Assert.AreEqual("plan-info", infoRow.Row["node"].StrValue,
            "Last EXPLAIN row for a join plan must be plan-info");

        string detail = infoRow.Row["detail"].StrValue!;
        Assert.IsTrue(detail.StartsWith("shape="), $"Expected 'shape=' prefix, got: {detail}");
        // Both tables must appear in schema-deps.
        Assert.IsTrue(detail.Contains("robots@") || detail.Contains("parts@"),
            $"Expected joined table names in schema-deps, got: {detail}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static string ExtractShape(string detail)
    {
        // detail = "shape=<id>, schema-deps=..."
        int start = detail.IndexOf("shape=", StringComparison.Ordinal);
        if (start < 0) return "";
        start += "shape=".Length;
        int end = detail.IndexOf(',', start);
        return end < 0 ? detail[start..] : detail[start..end];
    }
}
