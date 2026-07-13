
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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for FROM-less SELECT (a projection list evaluated against a single synthetic row):
/// scalar/function expressions, aliases, parameters, LIMIT/OFFSET, and the rejected shapes
/// (SELECT *, aggregates, projection subqueries).
/// </summary>
[NonParallelizable]
public sealed class TestFromlessSelect : SharedNodeBaseTest
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

    private static async Task<CamusDBException> AssertThrows(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        return Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;
    }

    [Test]
    public async Task Arithmetic_ReturnsSingleRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 + 1");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["0"].Type);
        Assert.AreEqual(2L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task RegexpSplitToArray_TargetExample()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // The motivating query, both with double-quoted (CamusDB string literal) and single-quoted forms.
        foreach (string sql in new[]
        {
            "SELECT regexp_split_to_array(\"one,two,three\", \",\")",
            "SELECT regexp_split_to_array('one,two,three', ',')",
        })
        {
            List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, sql);
            Assert.AreEqual(1, rows.Count, sql);
            ColumnValue cv = rows[0].Row["0"];
            Assert.AreEqual(ColumnType.Array, cv.Type, sql);
            Assert.AreEqual(3, cv.ArrayValues!.Count, sql);
            Assert.AreEqual("one", cv.ArrayValues[0].StrValue);
            Assert.AreEqual("two", cv.ArrayValues[1].StrValue);
            Assert.AreEqual("three", cv.ArrayValues[2].StrValue);
        }
    }

    [Test]
    public async Task ScalarFunction_ReturnsValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT upper('abc')");
        Assert.AreEqual("ABC", rows[0].Row["0"].StrValue);
    }

    [Test]
    public async Task Alias_NamesTheColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 41 + 1 AS answer");
        Assert.AreEqual(42L, rows[0].Row["answer"].LongValue);
    }

    [Test]
    public async Task MultipleProjections_OrdinalColumnNames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1, 'a', 2 + 3");
        Assert.AreEqual(1L, rows[0].Row["0"].LongValue);
        Assert.AreEqual("a", rows[0].Row["1"].StrValue);
        Assert.AreEqual(5L, rows[0].Row["2"].LongValue);
    }

    [Test]
    public async Task Parameter_Binds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.Integer64, 99L) } };
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT @p", parameters);
        Assert.AreEqual(99L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task LimitZero_ReturnsNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 0");
        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    public async Task LimitOne_ReturnsTheRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 1");
        Assert.AreEqual(1, rows.Count);
    }

    [Test]
    public async Task OffsetOne_SkipsTheRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT 1 LIMIT 1 OFFSET 1");
        Assert.AreEqual(0, rows.Count);
    }

    [Test]
    public async Task SelectStar_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT *");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("FROM"));
    }

    [Test]
    public async Task Aggregate_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = await AssertThrows(executor, database, dbname, "SELECT COUNT(*)");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("FROM"));
    }

    // ── WHERE/GROUP/HAVING/ORDER are not admitted by the FROM-less grammar rule ─────
    // (they only exist on the FROM-carrying alternative), so each is rejected at parse time.

    [Test]
    public async Task WhereClause_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        // Parser-level rejection: the FROM-less select_stmt admits only a field list + LIMIT/OFFSET.
        Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, database, dbname, "SELECT 1 WHERE 1 = 1"));
    }

    [Test]
    public async Task GroupBy_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, database, dbname, "SELECT 1 GROUP BY 1"));
    }

    [Test]
    public async Task OrderBy_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, database, dbname, "SELECT 1 ORDER BY 1"));
    }

    // ── EXPLAIN of a FROM-less SELECT renders a fixed ConstantSource → Projection shape ──

    [Test]
    public async Task Explain_RendersConstantSourceAndProjection()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "EXPLAIN SELECT 41 + 1 AS answer");

        List<string> nodes = rows.Select(r => r.Row["node"].StrValue!).ToList();
        Assert.That(nodes, Does.Contain("constant-source"));
        Assert.That(nodes, Does.Contain("project"));

        QueryResultRow projection = rows.Single(r => r.Row["node"].StrValue == "project");
        Assert.That(projection.Row["detail"].StrValue, Does.Contain("answer"));

        QueryResultRow source = rows.Single(r => r.Row["node"].StrValue == "constant-source");
        Assert.AreEqual(1L, source.Row["estimated_rows"].LongValue);
    }

    [Test]
    public async Task Explain_WithLimit_RendersLimitNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "EXPLAIN SELECT 1 LIMIT 5 OFFSET 2");

        Assert.That(rows.Select(r => r.Row["node"].StrValue), Does.Contain("limit"));
        QueryResultRow limit = rows.Single(r => r.Row["node"].StrValue == "limit");
        Assert.That(limit.Row["detail"].StrValue, Does.Contain("limit=5"));
        Assert.That(limit.Row["detail"].StrValue, Does.Contain("offset=2"));
    }

    [Test]
    public async Task ExplainAnalyze_Rejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = await AssertThrows(executor, database, dbname, "EXPLAIN (ANALYZE) SELECT 1");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        Assert.That(ex.Message, Does.Contain("FROM-less"));
    }

    // ── FS3: uncorrelated projection subqueries (existence-check idiom) ────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAccounts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("email", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        foreach (string email in new[] { "alice@example.com", "bob@example.com" })
        {
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "accounts",
                values: new() {
                    new() {
                        { "id", new(ColumnType.Id, CamusDB.Core.Util.ObjectIds.ObjectIdGenerator.Generate().ToString()) },
                        { "email", new(ColumnType.String, email) },
                    }
                }));
        }

        await database.Transactions.CommitAsync(tx);
        return (dbname, database, executor);
    }

    [Test]
    public async Task Exists_Match_ReturnsTrue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT EXISTS (SELECT 1 FROM accounts WHERE email = 'alice@example.com')");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Bool, rows[0].Row["0"].Type);
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task Exists_NoMatch_ReturnsFalse()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT EXISTS (SELECT 1 FROM accounts WHERE email = 'nobody@example.com')");
        Assert.IsFalse(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task NotExists_NoMatch_ReturnsTrue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT NOT EXISTS (SELECT 1 FROM accounts WHERE email = 'nobody@example.com')");
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task Exists_Parameterized()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@e", new ColumnValue(ColumnType.String, "bob@example.com") }
        };
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT EXISTS (SELECT 1 FROM accounts WHERE email = @e)", parameters);
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task ScalarCount_GreaterThanZero_ReturnsTrue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT (SELECT COUNT(*) FROM accounts WHERE email = 'alice@example.com') > 0");
        Assert.AreEqual(ColumnType.Bool, rows[0].Row["0"].Type);
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }

    [Test]
    public async Task ScalarCount_ReturnsCount()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT (SELECT COUNT(*) FROM accounts)");
        Assert.AreEqual(2L, rows[0].Row["0"].LongValue);
    }

    [Test]
    public async Task ScalarCountOverDerivedTable_GreaterThanZero()
    {
        // Q2 shape: COUNT(*) over a derived table, parameterized, compared to > 0.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts();
        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@e", new ColumnValue(ColumnType.String, "alice@example.com") }
        };
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM accounts WHERE email = @e) AS _e) > 0", parameters);
        Assert.IsTrue(rows[0].Row["0"].BoolValue);
    }
}
