
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for compound aggregate projections: scalar expressions whose
/// sub-expressions include aggregate calls (e.g. COALESCE(SUM(x),0), SUM(a)/SUM(b),
/// SUM(x)+1). These forms require the workspace accumulator path in <see cref="QueryAggregator"/>.
/// </summary>
[NonParallelizable]
public class TestCompoundAggregates : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "sales",
            columns: new ColumnInfo[]
            {
                new("id",       ColumnType.Id),
                new("category", ColumnType.String, notNull: true),
                new("amount",   ColumnType.Integer64),   // nullable — allows SUM→NULL on empty group
                new("qty",      ColumnType.Integer64),   // nullable
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);
        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    private static async Task InsertRow(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string category,
        long? amount,
        long? qty)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        InsertTicket ticket = new(
            txnState: txn,
            databaseName: dbname,
            tableName: "sales",
            values: new()
            {
                new()
                {
                    { "id",       new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "category", new(ColumnType.String, category) },
                    { "amount",   amount.HasValue ? new(ColumnType.Integer64, amount.Value) : ColumnValue.Null },
                    { "qty",      qty.HasValue    ? new(ColumnType.Integer64, qty.Value)    : ColumnValue.Null },
                }
            });

        await executor.Insert(ticket);
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: parameters);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return await cursor.ToListAsync();
    }

    // ── COALESCE(SUM(x), 0) — non-empty set ─────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CoalesceSum_NonEmptySet_ReturnsSumValue()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "a", 20, 2);
        await InsertRow(executor, database, dbname, "a", 30, 3);

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT COALESCE(SUM(amount), 0) FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(60L, result.LongValue, "COALESCE(SUM(amount),0) with rows should equal SUM");
    }

    // ── COALESCE(SUM(x), 0) — empty table ───────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CoalesceSum_EmptyTable_ReturnsZeroNotNull()
    {
        var (dbname, database, executor) = await SetupTable();

        // No rows → SUM returns NULL → COALESCE must return 0.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT COALESCE(SUM(amount), 0) FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(ColumnType.Integer64, result.Type, "Empty-set COALESCE(SUM,0) must be Integer64");
        Assert.AreEqual(0L, result.LongValue, "Empty-set COALESCE(SUM,0) must be 0");
    }

    // ── SUM(x) + 1 ───────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SumPlusOne_ReturnsCorrectResult()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 5, 1);
        await InsertRow(executor, database, dbname, "a", 15, 2);

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT SUM(amount) + 1 FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(21L, result.LongValue, "SUM(5+15)+1 should equal 21");
    }

    // ── SUM(a) / SUM(b) — integer division ───────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SumDivSum_IntegerDivision_ReturnsCorrectResult()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 3);
        await InsertRow(executor, database, dbname, "a", 20, 7);

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT SUM(amount) / SUM(qty) FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        // SUM(amount)=30, SUM(qty)=10 → 30/10=3
        Assert.AreEqual(3L, result.LongValue);
    }

    // ── Grouped: SELECT category, COALESCE(SUM(amount), 0) GROUP BY category ─

    [Test]
    [NonParallelizable]
    public async Task GroupedCoalesceSum_ReturnsPerGroupResult()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "a", 20, 2);
        await InsertRow(executor, database, dbname, "b", 5,  1);
        // Null-amount row in group "b" — SUM skips NULLs, result is still 5
        await InsertRow(executor, database, dbname, "b", null, 1);

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, COALESCE(SUM(amount), 0) FROM sales GROUP BY category ORDER BY category");

        Assert.AreEqual(2, rows.Count);

        // Group "a": SUM=30
        Assert.AreEqual("a", rows[0].Row["category"].StrValue);
        Assert.AreEqual(30L, rows[0].Row.Values.Skip(1).First().LongValue);

        // Group "b": SUM=5 (null amount skipped)
        Assert.AreEqual("b", rows[1].Row["category"].StrValue);
        Assert.AreEqual(5L, rows[1].Row.Values.Skip(1).First().LongValue);
    }

    // ── Grouped: group whose only rows have NULL amount → COALESCE yields 0 ──

    [Test]
    [NonParallelizable]
    public async Task GroupedCoalesceSum_AllNullGroup_ReturnsZero()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10,   1);
        await InsertRow(executor, database, dbname, "b", null, 1);  // group "b" amount always null

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, COALESCE(SUM(amount), 0) FROM sales GROUP BY category ORDER BY category");

        Assert.AreEqual(2, rows.Count);

        Assert.AreEqual("a", rows[0].Row["category"].StrValue);
        Assert.AreEqual(10L, rows[0].Row.Values.Skip(1).First().LongValue);

        // SUM of all-NULL group returns NULL → COALESCE returns 0
        Assert.AreEqual("b", rows[1].Row["category"].StrValue);
        Assert.AreEqual(0L, rows[1].Row.Values.Skip(1).First().LongValue);
    }

    // ── Alias on compound aggregate is preserved ─────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CoalesceSum_WithAlias_OutputNameIsAlias()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 7, 1);

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT COALESCE(SUM(amount), 0) AS total FROM sales");

        Assert.AreEqual(1, rows.Count);
        Assert.IsTrue(rows[0].Row.ContainsKey("total"), "output column should be named 'total'");
        Assert.AreEqual(7L, rows[0].Row["total"].LongValue);
    }

    // ── Compound aggregate in a derived table (subquery-in-FROM) ─────────────

    [Test]
    [NonParallelizable]
    public async Task DerivedTable_CoalesceSum_CorrectValueAndType()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "a", 20, 2);

        // End-to-end: a compound aggregate inside a subquery-in-FROM executes and the outer
        // query reads the correct value through the derived table. The asserted .Type here is the
        // runtime value type (Integer64 from the evaluator); the *declared* derived-schema type
        // that DerivedTableSchemaBuilder infers is covered separately by
        // TestDerivedTableSchemaBuilder (which fails if that inference regresses).
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT x.total FROM (SELECT COALESCE(SUM(amount), 0) AS total FROM sales) x");

        Assert.AreEqual(1, rows.Count);
        Assert.IsTrue(rows[0].Row.ContainsKey("total"));
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["total"].Type);
        Assert.AreEqual(30L, rows[0].Row["total"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DerivedTable_CoalesceSum_EmptyInner_ReturnsZero()
    {
        var (dbname, database, executor) = await SetupTable();

        // No rows → inner SUM→NULL → COALESCE→0; outer must still return 0, typed Integer64.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT x.total FROM (SELECT COALESCE(SUM(amount), 0) AS total FROM sales) x");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["total"].Type);
        Assert.AreEqual(0L, rows[0].Row["total"].LongValue);
    }

    // ── Arithmetic-topped compound aggregate in a derived table ──────────────

    [Test]
    [NonParallelizable]
    public async Task DerivedTable_SumPlusOne_TypedAsInteger64()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "a", 20, 2);

        // End-to-end: SUM(amount)+1 (arithmetic-topped compound) inside a derived table executes
        // and the outer query reads the correct value. Declared-schema type inference for this
        // shape is asserted directly in TestDerivedTableSchemaBuilder.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT x.v FROM (SELECT SUM(amount) + 1 AS v FROM sales) x");

        Assert.AreEqual(1, rows.Count);
        Assert.IsTrue(rows[0].Row.ContainsKey("v"));
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["v"].Type);
        Assert.AreEqual(31L, rows[0].Row["v"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DerivedTable_SumDivSum_TypedAsInteger64()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 30, 10);

        // End-to-end: SUM(amount)/SUM(qty) (ExprDiv, both operands bare aggregates) inside a
        // derived table executes and the outer query reads the correct value. Declared-schema
        // type inference for this shape is asserted directly in TestDerivedTableSchemaBuilder.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT x.ratio FROM (SELECT SUM(amount) / SUM(qty) AS ratio FROM sales) x");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["ratio"].Type);
        Assert.AreEqual(3L, rows[0].Row["ratio"].LongValue);
    }

    // ── ORDER BY over compound aggregate ─────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task GroupBy_OrderByCompoundAggregate_SortsCorrectly()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "b", 30, 2);
        await InsertRow(executor, database, dbname, "c", 5,  1);

        // ORDER BY a compound aggregate that is also in SELECT: SUM(amount)+1 AS v.
        // Ordering uses the pre-computed "v" column from the workspace — descending so "b"
        // (31) > "a" (11) > "c" (6).
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, SUM(amount) + 1 AS v FROM sales " +
            "GROUP BY category ORDER BY SUM(amount) + 1 DESC");

        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual("b", rows[0].Row["category"].StrValue);
        Assert.AreEqual(31L, rows[0].Row["v"].LongValue);
        Assert.AreEqual("a", rows[1].Row["category"].StrValue);
        Assert.AreEqual(11L, rows[1].Row["v"].LongValue);
        Assert.AreEqual("c", rows[2].Row["category"].StrValue);
        Assert.AreEqual(6L, rows[2].Row["v"].LongValue);
    }

    // ── HAVING over compound aggregate (global) ───────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Having_CoalesceSum_FiltersGroupsCorrectly()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "a", 20, 2);
        await InsertRow(executor, database, dbname, "b", 5,  1);

        // HAVING COALESCE(SUM(amount),0) > 10 should include group "a" (sum=30) but
        // exclude group "b" (sum=5). The compound expression is not in SELECT so it is
        // added as a hidden projection by QueryHavingWorkspace and looked up during evaluation.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, SUM(amount) FROM sales " +
            "GROUP BY category HAVING COALESCE(SUM(amount), 0) > 10 ORDER BY category");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("a", rows[0].Row["category"].StrValue);
        Assert.AreEqual(30L, rows[0].Row.Values.Skip(1).First().LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Having_SumPlusOne_FiltersGroupsCorrectly()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 10, 1);
        await InsertRow(executor, database, dbname, "b", 3,  1);

        // HAVING SUM(amount)+1 > 5: group "a" (SUM=10 → 11 > 5 ✓), group "b" (SUM=3 → 4 ≤ 5 ✗).
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, SUM(amount) FROM sales " +
            "GROUP BY category HAVING SUM(amount) + 1 > 5 ORDER BY category");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("a", rows[0].Row["category"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Having_CompoundAggInSelect_LooksUpPrecomputedValue()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 100, 1);
        await InsertRow(executor, database, dbname, "b",   5, 1);

        // When the compound aggregate also appears in SELECT it is already in the workspace
        // under its alias; the HAVING evaluator must resolve to the same column ("total").
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT category, COALESCE(SUM(amount), 0) AS total FROM sales " +
            "GROUP BY category HAVING COALESCE(SUM(amount), 0) > 10 ORDER BY category");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("a", rows[0].Row["category"].StrValue);
        Assert.AreEqual(100L, rows[0].Row["total"].LongValue);
    }

    // ── SUM(a)/SUM(b) — zero-guard via COALESCE ──────────────────────────────
    // CASE expressions are not in the CamusDB grammar; the idiomatic zero-guard
    // for division here is COALESCE(SUM(a),0)/COALESCE(SUM(b),1).

    [Test]
    [NonParallelizable]
    public async Task CoalesceSumDivCoalesceSum_EmptyTable_ReturnsZero()
    {
        var (dbname, database, executor) = await SetupTable();

        // No rows: both sums → NULL → COALESCE gives 0/1 = 0.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT COALESCE(SUM(amount), 0) / COALESCE(SUM(qty), 1) FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(0L, result.LongValue, "Empty table: COALESCE-guarded ratio must be 0");
    }

    [Test]
    [NonParallelizable]
    public async Task CoalesceSumDivCoalesceSum_NonEmptyTable_ReturnsRatio()
    {
        var (dbname, database, executor) = await SetupTable();

        await InsertRow(executor, database, dbname, "a", 40, 2);

        // SUM(amount)=40, SUM(qty)=2 → 40/2 = 20.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "SELECT COALESCE(SUM(amount), 0) / COALESCE(SUM(qty), 1) FROM sales");

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(20L, result.LongValue, "Non-empty: COALESCE-guarded ratio must be 20");
    }

    // ── Exact EF shape: backtick identifiers, table alias, @param placeholders ─
    // The SQL parser lowercases all identifiers (including backtick-quoted ones), so column
    // names in the schema must be lowercase to match what the parser produces from EF-generated SQL.

    [Test]
    [NonParallelizable]
    public async Task EfShape_CoalesceSumWithParams_MatchingRows_ReturnsSum()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn0 = await database.Transactions.BeginAsync();
        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "agent_llm_usage",
            // Column names are lowercase: the SQL parser normalises backtick-quoted identifiers
            // (e.g. `CostMinor`) to lowercase, so the schema must match.
            columns: new ColumnInfo[]
            {
                new("id",        ColumnType.Id),
                new("studioid",  ColumnType.String, notNull: true),
                new("createdat", ColumnType.Integer64, notNull: true),
                new("costminor", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);
        await executor.CreateTable(tableTicket);
        await database.Transactions.CommitAsync(txn0);

        async Task Insert(string studioId, long createdAt, long? cost)
        {
            KvTransaction t = await database.Transactions.BeginAsync();
            InsertTicket ins = new(
                txnState: t,
                databaseName: dbname,
                tableName: "agent_llm_usage",
                values: new()
                {
                    new()
                    {
                        { "id",        new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "studioid",  new(ColumnType.String, studioId) },
                        { "createdat", new(ColumnType.Integer64, createdAt) },
                        { "costminor", cost.HasValue ? new(ColumnType.Integer64, cost.Value) : ColumnValue.Null },
                    }
                });
            await executor.Insert(ins);
            await database.Transactions.CommitAsync(t);
        }

        await Insert("studio-1", 1000, 200);
        await Insert("studio-1", 2000, 300);
        await Insert("studio-2", 1500, 999);  // different studio — excluded by WHERE
        await Insert("studio-1", 5000,  50);  // outside time range — excluded by @e

        // The exact SQL shape EF Core generates for SumAsync over a non-nullable projection.
        // Backtick identifiers are lowercased by the parser, matching the schema above.
        string sql =
            "SELECT COALESCE(SUM(`a`.`CostMinor`), 0) " +
            "FROM `agent_llm_usage` AS `a` " +
            "WHERE `a`.`StudioId` = @p AND `a`.`CreatedAt` >= @s AND `a`.`CreatedAt` <= @e";

        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@p", new(ColumnType.String,    "studio-1") },
            { "@s", new(ColumnType.Integer64, 500L) },
            { "@e", new(ColumnType.Integer64, 3000L) },
        };

        // rows at t=1000 (cost=200) and t=2000 (cost=300) match; sum = 500.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname, sql, parameters);

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(ColumnType.Integer64, result.Type);
        Assert.AreEqual(500L, result.LongValue, "EF shape: sum of matching CostMinor rows");
    }

    [Test]
    [NonParallelizable]
    public async Task EfShape_CoalesceSumWithParams_NoMatchingRows_ReturnsZero()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn0 = await database.Transactions.BeginAsync();
        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "agent_llm_usage",
            columns: new ColumnInfo[]
            {
                new("id",        ColumnType.Id),
                new("studioid",  ColumnType.String, notNull: true),
                new("createdat", ColumnType.Integer64, notNull: true),
                new("costminor", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);
        await executor.CreateTable(tableTicket);
        await database.Transactions.CommitAsync(txn0);

        // Insert a row that won't match the WHERE predicate (different studio).
        KvTransaction t = await database.Transactions.BeginAsync();
        InsertTicket ins = new(
            txnState: t,
            databaseName: dbname,
            tableName: "agent_llm_usage",
            values: new()
            {
                new()
                {
                    { "id",        new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "studioid",  new(ColumnType.String, "studio-other") },
                    { "createdat", new(ColumnType.Integer64, 1000L) },
                    { "costminor", new(ColumnType.Integer64, 999L) },
                }
            });
        await executor.Insert(ins);
        await database.Transactions.CommitAsync(t);

        string sql =
            "SELECT COALESCE(SUM(`a`.`CostMinor`), 0) " +
            "FROM `agent_llm_usage` AS `a` " +
            "WHERE `a`.`StudioId` = @p AND `a`.`CreatedAt` >= @s AND `a`.`CreatedAt` <= @e";

        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@p", new(ColumnType.String,    "studio-1") },
            { "@s", new(ColumnType.Integer64, 500L) },
            { "@e", new(ColumnType.Integer64, 3000L) },
        };

        // WHERE matches no rows → SUM returns NULL → COALESCE must return 0.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname, sql, parameters);

        Assert.AreEqual(1, rows.Count);
        ColumnValue result = rows[0].Row.Values.First();
        Assert.AreEqual(ColumnType.Integer64, result.Type);
        Assert.AreEqual(0L, result.LongValue, "EF shape: no matching rows → COALESCE must return 0");
    }
}
