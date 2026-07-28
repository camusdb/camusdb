
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the CASE expression: searched/simple evaluation with first-match and
/// short-circuit semantics, only-TRUE-matches, ELSE / no-match-NULL, use in projections, WHERE,
/// aggregates (both directions), derived tables, and CHECK constraints (enforcement + round-trip).
/// </summary>
public sealed class TestCaseExpression : SharedNodeBaseTest
{
    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    // ── FROM-less evaluation ────────────────────────────────────────────────────

    [Test]
    public async Task Searched_FirstMatchWins()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // Both WHENs are true; the first must win.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 1 THEN 'first' WHEN 1 = 1 THEN 'second' ELSE 'else' END");
        Assert.AreEqual("first", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Searched_FallsThroughToElse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 2 THEN 'no' ELSE 'yes' END");
        Assert.AreEqual("yes", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Searched_NoMatchNoElse_IsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 2 THEN 'no' END");
        Assert.AreEqual(ColumnType.Null, rows[0].Row["0"].Type);
    }

    [Test]
    public async Task Searched_NullConditionDoesNotMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // A WHEN whose condition is NULL/UNKNOWN (here NULL = 1) must skip, exactly like a FALSE one.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN NULL = 1 THEN 'matched' ELSE 'skipped' END");
        Assert.AreEqual("skipped", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Simple_EqualityMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END");
        Assert.AreEqual("two", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Simple_NumericWidening()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // A Float64 operand compares equal to an integer WHEN value via the shared CompareValues.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE 1.0 WHEN 1 THEN 'match' ELSE 'no' END");
        Assert.AreEqual("match", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Simple_NullOperand_FallsToElse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // A NULL operand equals no value (UNKNOWN), so no WHEN matches → ELSE.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE NULL WHEN 1 THEN 'one' ELSE 'none' END");
        Assert.AreEqual("none", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task ShortCircuit_LaterWhenNotEvaluatedAfterMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // The second WHEN divides by zero; it must never be evaluated because the first matches.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 1 THEN 'first' WHEN (1 / 0) = 1 THEN 'second' END");
        Assert.AreEqual("first", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task ShortCircuit_UnmatchedThenNotEvaluated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // The THEN of a non-matching WHEN (1/0) must not be evaluated.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 2 THEN (1 / 0) ELSE 'safe' END");
        Assert.AreEqual("safe", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task ResultType_IsChosenBranchRuntimeType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN 1 = 1 THEN 42 ELSE 'text' END");
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["0"].Type);
        Assert.AreEqual(42L, rows[0].Row["0"].LongValue);
    }

    // ── Table-backed: projection, WHERE, aggregates, derived table ──────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupOrders()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("status", ColumnType.String),
                new("amount", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        KvTransaction tx = await database.Transactions.BeginAsync();
        foreach ((string status, long amount) in new[] { ("paid", 100L), ("paid", 50L), ("pending", 30L) })
        {
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "orders",
                values: new() {
                    new() {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "status", new(ColumnType.String, status) },
                        { "amount", new(ColumnType.Integer64, amount) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(tx);
        return (dbname, database, executor);
    }

    [Test]
    public async Task Projection_PerRowValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupOrders();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN status = 'paid' THEN amount ELSE 0 END AS effective FROM orders");

        List<long> effective = rows.Select(r => r.Row["effective"].LongValue).OrderBy(v => v).ToList();
        CollectionAssert.AreEqual(new[] { 0L, 50L, 100L }, effective);
    }

    [Test]
    public async Task Where_CaseAsPredicate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupOrders();
        // Rows where the CASE yields true: paid orders only.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT amount FROM orders WHERE CASE WHEN status = 'paid' THEN true ELSE false END");
        Assert.AreEqual(2, rows.Count);
    }

    [Test]
    public async Task Aggregate_OverCase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupOrders();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS total FROM orders");
        Assert.AreEqual(150L, rows[0].Row["total"].LongValue);
    }

    [Test]
    public async Task Case_OverAggregate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupOrders();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT CASE WHEN COUNT(*) > 0 THEN 'has' ELSE 'none' END AS label FROM orders");
        Assert.AreEqual("has", rows[0].Row["label"].StrValue);
    }

    [Test]
    public async Task DerivedTable_CaseColumnBindsAndReturns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupOrders();
        // The CASE lives in a subquery output column consumed by the outer query — must not crash on typing.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT label FROM (SELECT CASE WHEN status = 'paid' THEN 'Y' ELSE 'N' END AS label FROM orders) s");
        List<string?> labels = rows.Select(r => r.Row["label"].StrValue).OrderBy(v => v).ToList();
        CollectionAssert.AreEqual(new[] { "N", "Y", "Y" }, labels);
    }

    // ── CHECK constraints ───────────────────────────────────────────────────────

    [Test]
    public async Task Check_WithCase_EnforcesAndRoundTrips()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // CHECK: when kind is 'discount' the value must be negative, otherwise non-negative.
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE entries (" +
            "  id int64, " +
            "  kind string, " +
            "  value int64 CHECK (CASE WHEN kind = 'discount' THEN value < 0 ELSE value >= 0 END), " +
            "  PRIMARY KEY (id))");

        // Satisfying rows.
        await Insert(executor, database, dbname, "entries", 1, "discount", -5);
        await Insert(executor, database, dbname, "entries", 2, "charge", 10);

        // Violating row: a discount with a non-negative value.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            Insert(executor, database, dbname, "entries", 3, "discount", 5))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        // Round-trip: the persisted DDL re-renders the CASE faithfully.
        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE TABLE entries");
        string ddl = shown[0].Row["Create Table"].StrValue!;
        Assert.That(ddl, Does.Contain("CASE"));
        Assert.That(ddl, Does.Contain("WHEN"));
        Assert.That(ddl, Does.Contain("END"));
    }

    [Test]
    public async Task Check_CaseWithSubquery_RejectedAtDdl()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        // A subquery inside a CASE branch is not check-legal and must be rejected at CREATE.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecuteDdl(executor, dbname,
            "CREATE TABLE bad (" +
            "  id int64, " +
            "  v int64 CHECK (CASE WHEN v > 0 THEN v > (SELECT 1) ELSE true END), " +
            "  PRIMARY KEY (id))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        ExecuteSQLTicket ticket = new(txnState: null!, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
    }

    private static async Task Insert(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table,
        long id, string kind, long value)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: table,
                values: new() {
                    new() {
                        { "id", new(ColumnType.Integer64, id) },
                        { "kind", new(ColumnType.String, kind) },
                        { "value", new(ColumnType.Integer64, value) },
                    }
                }));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
