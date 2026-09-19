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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A SELECT without GROUP BY may carry several aggregates: they all reduce the same single group,
/// in one pass. Before, any second aggregate raised "Aggregations cannot be accompanied by other
/// projections or expressions", so comparing <c>COUNT(*)</c> with <c>COUNT(col)</c> took two
/// statements. A bare column next to an aggregate is still rejected, because without GROUP BY it has
/// no group key to bind to. Each result is checked against the separate single-aggregate statement.
/// </summary>
public sealed class TestGlobalMultipleAggregates : SharedNodeBaseTest
{
    private static async Task Exec(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        if (ddl)
            await executor.ExecuteDDLSQL(ticket);
        else
            await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> Single(DatabaseDescriptor database, CommandExecutor executor, string dbname, string aggregate, string where)
    {
        List<QueryResultRow> rows = await Query(database, executor, dbname, $"SELECT {aggregate} AS v FROM sales{where}");
        Assert.AreEqual(1, rows.Count);
        return rows[0].Row["v"];
    }

    /// <summary>sales: 6 rows, amount has two NULLs, price is float.</summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(database, executor, dbname, "CREATE TABLE sales (id int64 primary key, region string(16), amount int64, price float64)", ddl: true);
        await Exec(database, executor, dbname,
            "INSERT INTO sales (id, region, amount, price) VALUES " +
            "(1, 'north', 10, 1.5), (2, 'north', NULL, 2.5), (3, 'south', 30, 4.0), " +
            "(4, 'south', -5, NULL), (5, 'west', NULL, 0.5), (6, 'west', 20, 3.0)",
            ddl: false);

        return (dbname, database, executor);
    }

    /// <summary><see cref="ColumnValue"/> has no value equality; compare its type and its text form.</summary>
    private static void AssertSameValue(ColumnValue expected, ColumnValue actual, string message)
    {
        Assert.AreEqual(expected.Type, actual.Type, message);
        Assert.AreEqual(expected.ToString(), actual.ToString(), message);
    }

    private static readonly string[] Aggregates =
        ["COUNT(*)", "COUNT(amount)", "SUM(amount)", "AVG(amount)", "MIN(amount)", "MAX(amount)", "SUM(price)", "MAX(region)"];

    [TestCase("")]
    [TestCase(" WHERE amount > 0")]
    [TestCase(" WHERE id > 100")]
    public async Task EveryPair_MatchesTheSingleAggregateStatements(string where)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        for (int i = 0; i < Aggregates.Length; i++)
        {
            for (int j = i + 1; j < Aggregates.Length; j++)
            {
                string sql = $"SELECT {Aggregates[i]} AS a, {Aggregates[j]} AS b FROM sales{where}";
                List<QueryResultRow> rows = await Query(database, executor, dbname, sql);

                Assert.AreEqual(1, rows.Count, sql);
                AssertSameValue(await Single(database, executor, dbname, Aggregates[i], where), rows[0].Row["a"], sql);
                AssertSameValue(await Single(database, executor, dbname, Aggregates[j], where), rows[0].Row["b"], sql);
            }
        }
    }

    [Test]
    public async Task CountStarVersusCountColumn_InOneStatement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT COUNT(*) AS total, COUNT(amount) AS present, COUNT(*) - COUNT(amount) AS missing FROM sales");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(6L, rows[0].Row["total"].LongValue);
        Assert.AreEqual(4L, rows[0].Row["present"].LongValue);
        Assert.AreEqual(2L, rows[0].Row["missing"].LongValue);
    }

    [Test]
    public async Task EmptyInput_ReturnsOneRowOfIdentityValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT COUNT(*) AS c, SUM(amount) AS s, AVG(amount) AS a, MIN(amount) AS lo, MAX(amount) AS hi FROM sales WHERE id > 100");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(0L, rows[0].Row["c"].LongValue);
        foreach (string column in new[] { "s", "a", "lo", "hi" })
            Assert.AreEqual(ColumnType.Null, rows[0].Row[column].Type, column);
    }

    [Test]
    public async Task UnaliasedAggregates_KeepOneColumnEach()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT COUNT(*), SUM(amount), COUNT(*) FROM sales");

        // Two unaliased COUNT(*) must stay two columns, not collapse into one.
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(3, rows[0].Row.Count);
        CollectionAssert.AreEquivalent(
            new[] { 6L, 6L, 55L },
            rows[0].Row.Values.Select(v => v.LongValue).ToArray());
    }

    [Test]
    public async Task WithHavingAndLimit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<QueryResultRow> kept = await Query(database, executor, dbname,
            "SELECT MIN(amount) AS lo, MAX(amount) AS hi FROM sales HAVING COUNT(amount) > 3 LIMIT 5");
        Assert.AreEqual(1, kept.Count);
        Assert.AreEqual(-5L, kept[0].Row["lo"].LongValue);
        Assert.AreEqual(30L, kept[0].Row["hi"].LongValue);

        List<QueryResultRow> dropped = await Query(database, executor, dbname,
            "SELECT MIN(amount) AS lo, MAX(amount) AS hi FROM sales HAVING COUNT(amount) > 4");
        Assert.AreEqual(0, dropped.Count);
    }

    [Test]
    public async Task BareColumnNextToAggregates_IsStillRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(database, executor, dbname, "SELECT COUNT(*), SUM(amount), region FROM sales"))!;

        Assert.AreEqual("Aggregations cannot be accompanied by other projections or expressions.", ex.Message);
    }

    [Test]
    public async Task OverAJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        await Exec(database, executor, dbname, "CREATE TABLE regions (name string(16) primary key, zone int64)", ddl: true);
        await Exec(database, executor, dbname, "INSERT INTO regions (name, zone) VALUES ('north', 1), ('south', 2)", ddl: false);

        List<QueryResultRow> rows = await Query(database, executor, dbname,
            "SELECT COUNT(*) AS c, SUM(s.amount) AS total, MAX(r.zone) AS z FROM sales s JOIN regions r ON s.region = r.name");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(4L, rows[0].Row["c"].LongValue);
        Assert.AreEqual(35L, rows[0].Row["total"].LongValue);
        Assert.AreEqual(2L, rows[0].Row["z"].LongValue);
    }
}
