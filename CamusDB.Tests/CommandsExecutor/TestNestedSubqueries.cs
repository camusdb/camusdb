
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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A subquery's own WHERE may contain further subqueries. The inner SELECT used to be bound
/// directly, without the semi-join / subquery-rewrite / EXISTS stages a top-level SELECT runs, so a
/// second level of <c>IN (SELECT …)</c> reached the per-row evaluator unresolved and failed with
/// "IN subquery must be resolved before expression evaluation". These tests drive every statement
/// kind that hosts a subquery (SELECT, FROM-less SELECT, UPDATE, DELETE, EXPLAIN) through two and
/// three levels of nesting, mixing IN, NOT IN, scalar and EXISTS at the inner level.
/// </summary>
public sealed class TestNestedSubqueries : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE regions (id int64 primary key, name string(32) not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (id int64 primary key, regionid int64 not null, name string(32) not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 primary key, customerid int64 not null, amount int64 not null)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO regions (id, name) VALUES (1, 'north'), (2, 'south')");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO customers (id, regionid, name) VALUES (10, 1, 'ann'), (11, 1, 'bob'), (12, 2, 'cid')");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customerid, amount) VALUES (100, 10, 5), (101, 11, 7), (102, 12, 9), (103, 10, 1)");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static List<long> Ids(List<QueryResultRow> rows)
        => rows.Select(r => r.Row["id"].LongValue).OrderBy(x => x).ToList();

    private static async Task<List<long>> OrderIds(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string where,
        Dictionary<string, ColumnValue>? parameters = null)
        => Ids(await ExecQuery(database, executor, dbname, "SELECT id FROM orders WHERE " + where, parameters));

    private const string NorthCustomers =
        "SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = 'north')";

    // ── SELECT ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Select_InInsideIn_TwoLevels()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname, $"customerid IN ({NorthCustomers})");

        Assert.AreEqual(new List<long> { 100, 101, 103 }, ids);
    }

    [Test]
    public async Task Select_InInsideIn_ThreeLevels()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid IN "
            + "(SELECT id FROM regions WHERE id IN (SELECT id FROM regions WHERE name = 'north')))");

        Assert.AreEqual(new List<long> { 100, 101, 103 }, ids);
    }

    [Test]
    public async Task Select_SelfReferencingNestedIn_MatchesReproductionShape()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The shape the defect was reported with: every level reads the same table.
        List<long> ids = await OrderIds(database, executor, dbname,
            "amount IN (SELECT amount FROM orders WHERE amount IN (SELECT amount FROM orders))");

        Assert.AreEqual(new List<long> { 100, 101, 102, 103 }, ids);
    }

    [Test]
    public async Task Select_NotInInsideIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid NOT IN (SELECT id FROM regions WHERE name = 'north'))");

        Assert.AreEqual(new List<long> { 102 }, ids);
    }

    [Test]
    public async Task Select_InInsideNotIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname, $"customerid NOT IN ({NorthCustomers})");

        Assert.AreEqual(new List<long> { 102 }, ids);
    }

    [Test]
    public async Task Select_ScalarInsideIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid = (SELECT id FROM regions WHERE name = 'south'))");

        Assert.AreEqual(new List<long> { 102 }, ids);
    }

    [Test]
    public async Task Select_InInsideScalar()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid = (SELECT id FROM customers WHERE name = 'bob' AND regionid IN (SELECT id FROM regions WHERE name = 'north'))");

        Assert.AreEqual(new List<long> { 101 }, ids);
    }

    [Test]
    public async Task Select_UncorrelatedExistsInsideIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> present = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid = 2 AND EXISTS (SELECT id FROM regions WHERE name = 'south'))");
        List<long> absent = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid = 2 AND EXISTS (SELECT id FROM regions WHERE name = 'west'))");

        Assert.AreEqual(new List<long> { 102 }, present);
        Assert.IsEmpty(absent);
    }

    [Test]
    public async Task Select_CorrelatedExistsInsideIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // The EXISTS correlates with the IN subquery's own table, not with the outer statement.
        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers c WHERE EXISTS "
            + "(SELECT * FROM regions r WHERE r.id = c.regionid AND r.name = 'north'))");

        Assert.AreEqual(new List<long> { 100, 101, 103 }, ids);
    }

    [Test]
    public async Task Select_InInsideUncorrelatedExists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> all = await OrderIds(database, executor, dbname,
            "EXISTS (SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = 'north'))");
        List<long> none = await OrderIds(database, executor, dbname,
            "EXISTS (SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = 'west'))");

        Assert.AreEqual(new List<long> { 100, 101, 102, 103 }, all);
        Assert.IsEmpty(none);
    }

    [Test]
    public async Task Select_NestedInUnderOr_TakesRewritePath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        // An IN under OR is not lifted into a semi-join, so this exercises the materialized path
        // at the outer level with a nested IN at the inner level.
        List<long> ids = await OrderIds(database, executor, dbname,
            "amount = 1 OR customerid IN (SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = 'south'))");

        Assert.AreEqual(new List<long> { 102, 103 }, ids);
    }

    [Test]
    public async Task Select_NestedIn_ParameterAtInnermostLevel()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Dictionary<string, ColumnValue> parameters = new() { { "@name", new ColumnValue(ColumnType.String, "south") } };

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = @name))",
            parameters);

        Assert.AreEqual(new List<long> { 102 }, ids);
    }

    [Test]
    public async Task Select_NestedIn_SeesUncommittedWritesOfSameTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO regions (id, name) VALUES (3, 'east')", parameters: null));
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO customers (id, regionid, name) VALUES (13, 3, 'dee')", parameters: null));
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO orders (id, customerid, amount) VALUES (104, 13, 2)", parameters: null));

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "SELECT id FROM orders WHERE customerid IN (SELECT id FROM customers WHERE regionid IN "
                 + "(SELECT id FROM regions WHERE name = 'east'))",
            parameters: null));
        List<long> ids = Ids(await cursor.ToListAsync());

        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(new List<long> { 104 }, ids);
    }

    [Test]
    public async Task Select_NestedIn_InnerSubqueryEmpty()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<long> ids = await OrderIds(database, executor, dbname,
            "customerid IN (SELECT id FROM customers WHERE regionid IN (SELECT id FROM regions WHERE name = 'west'))");

        Assert.IsEmpty(ids);
    }

    // ── FROM-less SELECT ──────────────────────────────────────────────────────

    [Test]
    public async Task FromlessSelect_NestedInInProjection()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> hit = await ExecQuery(database, executor, dbname,
            $"SELECT 10 IN ({NorthCustomers}) AS hit");
        List<QueryResultRow> miss = await ExecQuery(database, executor, dbname,
            $"SELECT 12 IN ({NorthCustomers}) AS hit");

        Assert.AreEqual(1, hit.Count);
        Assert.IsTrue(hit[0].Row["hit"].BoolValue);
        Assert.AreEqual(1, miss.Count);
        Assert.IsFalse(miss[0].Row["hit"].BoolValue);
    }

    // ── EXPLAIN ───────────────────────────────────────────────────────────────

    [Test]
    public async Task Explain_NestedIn_ProducesPlan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            $"EXPLAIN SELECT id FROM orders WHERE customerid IN ({NorthCustomers})");

        Assert.IsNotEmpty(rows);
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Update_NestedInInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int modified = await ExecNonQuery(database, executor, dbname,
            $"UPDATE orders SET amount = 0 WHERE customerid IN ({NorthCustomers})");

        Assert.AreEqual(3, modified);
        Assert.AreEqual(new List<long> { 100, 101, 103 }, await OrderIds(database, executor, dbname, "amount = 0"));
    }

    [Test]
    public async Task Update_NestedNotInInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int modified = await ExecNonQuery(database, executor, dbname,
            $"UPDATE orders SET amount = 0 WHERE customerid NOT IN ({NorthCustomers})");

        Assert.AreEqual(1, modified);
        Assert.AreEqual(new List<long> { 102 }, await OrderIds(database, executor, dbname, "amount = 0"));
    }

    [Test]
    public async Task Update_SetScalarWithNestedIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int modified = await ExecNonQuery(database, executor, dbname,
            "UPDATE orders SET amount = (SELECT id FROM regions WHERE id IN "
            + "(SELECT regionid FROM customers WHERE name = 'cid')) WHERE id = 100");

        Assert.AreEqual(1, modified);
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT amount FROM orders WHERE id = 100");
        Assert.AreEqual(2, rows[0].Row["amount"].LongValue);
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Delete_NestedInInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            $"DELETE FROM orders WHERE customerid IN ({NorthCustomers})");

        Assert.AreEqual(3, deleted);
        Assert.AreEqual(new List<long> { 102 }, await OrderIds(database, executor, dbname, "amount > 0"));
    }

    [Test]
    public async Task Delete_NestedNotInInWhere()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            "DELETE FROM orders WHERE customerid NOT IN (SELECT id FROM customers WHERE regionid NOT IN "
            + "(SELECT id FROM regions WHERE name = 'north'))");

        Assert.AreEqual(3, deleted);
        Assert.AreEqual(new List<long> { 102 }, await OrderIds(database, executor, dbname, "amount > 0"));
    }

    [Test]
    public async Task Delete_ScalarInsideIn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        int deleted = await ExecNonQuery(database, executor, dbname,
            "DELETE FROM orders WHERE customerid IN (SELECT id FROM customers WHERE regionid = "
            + "(SELECT id FROM regions WHERE name = 'south'))");

        Assert.AreEqual(1, deleted);
        Assert.AreEqual(new List<long> { 100, 101, 103 }, await OrderIds(database, executor, dbname, "amount > 0"));
    }
}
