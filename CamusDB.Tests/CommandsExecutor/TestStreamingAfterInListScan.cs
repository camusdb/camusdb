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
/// An IN list on an indexed column is served by an IN-list index scan, and that scan applies the
/// IN predicate itself: the planner takes the IN conjunct out of the residual filter. Streaming
/// DISTINCT and streaming GROUP BY may replace a scan that delivers rows in no useful order with a
/// full ordered index scan. The IN-list scan must not be replaced that way, because the full scan
/// would no longer apply the IN predicate, and nothing else does.
///
/// <para>The data makes a lost IN filter visible: status 3 occurs only in regions outside the
/// list, so a query that loses the filter returns an extra group.</para>
/// </summary>
[TestFixture]
public sealed class TestStreamingAfterInListScan : BaseTest
{
    private static readonly (long Status, string Region, long Amount)[] Rows =
    [
        (1, "a", 10), (1, "a", 11), (1, "b", 12), (1, "c", 13), (1, "d", 14),
        (2, "a", 20), (2, "b", 21), (2, "b", 22), (2, "e", 23), (2, "f", 24),
        (3, "c", 30), (3, "d", 31), (3, "e", 32), (3, "f", 33), (3, "f", 34),
    ];

    private static readonly string[] InRegions = ["a", "b"];

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "sales",
            columns:
            [
                new("id",     ColumnType.Id),
                new("status", ColumnType.Integer64, notNull: true),
                new("region", ColumnType.String,    notNull: true),
                new("amount", ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",        [new("id",     OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "status_idx", [new("status", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "region_idx", [new("region", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        foreach ((long status, string region, long amount) in Rows)
        {
            await executor.Insert(new InsertTicket(txn, dbname, "sales",
                values: [new()
                {
                    { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "status", new(ColumnType.Integer64, status) },
                    { "region", new(ColumnType.String,    region) },
                    { "amount", new(ColumnType.Integer64, amount) },
                }]));
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static string Plan(List<QueryResultRow> rows) =>
        string.Join("\n", rows.Select(r => $"{r.Row["node"].StrValue}({r.Row["detail"].StrValue})"));

    [Test]
    public async Task GroupBy_OverAnInListScan_KeepsTheInFilter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT status, COUNT(*) AS n FROM sales WHERE region IN ('a', 'b') GROUP BY status";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Dictionary<long, long> expected = Rows
            .Where(r => InRegions.Contains(r.Region))
            .GroupBy(r => r.Status)
            .ToDictionary(g => g.Key, g => (long)g.Count());

        Dictionary<long, long> actual = rows.ToDictionary(r => r.Row["status"].LongValue, r => r.Row["n"].LongValue);

        Assert.That(actual, Is.EquivalentTo(expected), plan);
    }

    [Test]
    public async Task Distinct_OverAnInListScan_KeepsTheInFilter()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT DISTINCT status FROM sales WHERE region IN ('a', 'b')";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        long[] expected = Rows.Where(r => InRegions.Contains(r.Region)).Select(r => r.Status).Distinct().OrderBy(s => s).ToArray();
        long[] actual = rows.Select(r => r.Row["status"].LongValue).OrderBy(s => s).ToArray();

        Assert.That(actual, Is.EqualTo(expected), plan);
    }

    [Test]
    public async Task GroupBy_OverAnInListScan_AfterAnalyze_KeepsTheInFilter()
    {
        // With statistics the cost-based path decides the scan; the result must not change.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        await RunAsync(executor, database, dbname, "ANALYZE sales");

        const string sql = "SELECT status, SUM(amount) AS total FROM sales WHERE region IN ('a', 'b') GROUP BY status";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Dictionary<long, long> expected = Rows
            .Where(r => InRegions.Contains(r.Region))
            .GroupBy(r => r.Status)
            .ToDictionary(g => g.Key, g => g.Sum(r => r.Amount));

        Dictionary<long, long> actual = rows.ToDictionary(r => r.Row["status"].LongValue, r => r.Row["total"].LongValue);

        Assert.That(actual, Is.EquivalentTo(expected), plan);
    }
}
