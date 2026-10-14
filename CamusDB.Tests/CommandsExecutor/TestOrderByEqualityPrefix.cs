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
/// A column that a top-level <c>=</c> conjunct fixes to one value orders nothing, so an index whose
/// leading columns are fixed that way still delivers the requested order of the columns after them.
/// <c>WHERE category = ? ORDER BY created</c> over an index on <c>(category, created)</c> needs no
/// sort, and the LIMIT can then stop the index scan.
/// </summary>
[TestFixture]
public sealed class TestOrderByEqualityPrefix : BaseTest
{
    private const int RowsPerCategory = 10;

    private static readonly string[] Categories = ["a", "b", "c"];

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "events",
            columns:
            [
                new("id",       ColumnType.Id),
                new("category", ColumnType.String,    notNull: true),
                new("created",  ColumnType.Integer64, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk",            [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "category_created", [new("category", OrderType.Ascending), new("created", OrderType.Ascending)]),
            ],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Inserted in descending created order, so an unsorted primary-key scan is not in order.
        foreach (string category in Categories)
        {
            for (int i = RowsPerCategory; i >= 1; i--)
            {
                await executor.Insert(new InsertTicket(txn, dbname, "events",
                    values: [new()
                    {
                        { "id",       new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "category", new(ColumnType.String,    category) },
                        { "created",  new(ColumnType.Integer64, (long)i) },
                    }]));
            }
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

    private static long[] Created(List<QueryResultRow> rows) => rows.Select(r => r.Row["created"].LongValue).ToArray();

    [Test]
    public async Task EqualityOnLeadingColumn_OrderByNextColumn_HasNoSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category = 'b' ORDER BY created";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Contain("category_created"), plan);
        Assert.That(plan, Does.Not.Contain("sort("), plan);
        Assert.AreEqual(Enumerable.Range(1, RowsPerCategory).Select(i => (long)i).ToArray(), Created(rows));
        Assert.That(rows.Select(r => r.Row["category"].StrValue), Is.All.EqualTo("b"));
    }

    [Test]
    public async Task EqualityOnLeadingColumn_OrderByBothColumns_HasNoSort()
    {
        // The constant column may also appear in ORDER BY: it orders nothing.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category = 'b' ORDER BY category, created";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Not.Contain("sort("), plan);
        Assert.AreEqual(Enumerable.Range(1, RowsPerCategory).Select(i => (long)i).ToArray(), Created(rows));
    }

    [Test]
    public async Task EqualityOnLeadingColumn_OrderByNextColumnDescending_KeepsTheSort()
    {
        // The index is ascending on created and there is no backward scan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category = 'b' ORDER BY created DESC";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Contain("sort("), plan);
        Assert.AreEqual(Enumerable.Range(1, RowsPerCategory).Reverse().Select(i => (long)i).ToArray(), Created(rows));
    }

    [Test]
    public async Task RangeOnLeadingColumn_OrderByNextColumn_KeepsTheSort()
    {
        // A range does not fix the column: rows of two categories interleave in created order.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category >= 'b' ORDER BY created";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Contain("sort("), plan);
        Assert.AreEqual(Created(rows).OrderBy(c => c).ToArray(), Created(rows));
        Assert.AreEqual(2 * RowsPerCategory, rows.Count);
    }

    [Test]
    public async Task EqualityWithLimit_StopsTheIndexScan()
    {
        // With the sort gone and no residual filter, the LIMIT goes into the index scan.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category = 'c' ORDER BY created LIMIT 3";

        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);
        List<QueryResultRow> analyze = await RunAsync(executor, database, dbname, "EXPLAIN (ANALYZE) " + sql);

        Assert.AreEqual(new long[] { 1, 2, 3 }, Created(rows));

        string plan = Plan(analyze);
        QueryResultRow scan = analyze.Last(r => r.Row["node"].StrValue != "plan-info");
        Assert.That(plan, Does.Not.Contain("sort("), plan);
        Assert.LessOrEqual(scan.Row["rows_read"].LongValue, 3L, plan);
    }

    [Test]
    public async Task EqualityAndRange_OrderByRangeColumn_HasNoSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        const string sql = "SELECT category, created FROM events WHERE category = 'a' AND created > 6 ORDER BY created";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Not.Contain("sort("), plan);
        Assert.AreEqual(new long[] { 7, 8, 9, 10 }, Created(rows));
    }

    // ── Cost-based choice of the ordered index ────────────────────────────────

    /// <summary>
    /// On an analyzed table the cost-based planner chooses the access path. A predicate on a
    /// column no index leads offers no index candidate, but with ORDER BY and a LIMIT the index that
    /// delivers the order stops after a few rows, while a full scan reads every row and sorts. The
    /// ordered index must win, and the sort must be gone.
    /// </summary>
    [Test]
    public async Task AnalyzedTable_OrderByWithLimit_ChoosesTheOrderedIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        await RunAsync(executor, database, dbname, "ANALYZE events");

        const string sql = "SELECT category, created FROM events WHERE created > 2 ORDER BY category LIMIT 3";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Contain("category_created"), plan);
        Assert.That(plan, Does.Not.Contain("sort("), plan);
        Assert.AreEqual(3, rows.Count);
        Assert.That(rows.Select(r => r.Row["category"].StrValue), Is.All.EqualTo("a"));
        Assert.That(Created(rows), Is.All.GreaterThan(2L));
    }

    /// <summary>Without a LIMIT the ordered index reads every entry and a row each: the full scan and sort stay.</summary>
    [Test]
    public async Task AnalyzedTable_OrderByWithoutLimit_KeepsTheFullScanAndSort()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        await RunAsync(executor, database, dbname, "ANALYZE events");

        const string sql = "SELECT category, created FROM events WHERE created > 2 ORDER BY category";

        string plan = Plan(await RunAsync(executor, database, dbname, "EXPLAIN " + sql));
        List<QueryResultRow> rows = await RunAsync(executor, database, dbname, sql);

        Assert.That(plan, Does.Contain("sort("), plan);
        Assert.AreEqual(Categories.Length * (RowsPerCategory - 2), rows.Count);
        string[] categories = rows.Select(r => r.Row["category"].StrValue!).ToArray();
        Assert.AreEqual(categories.OrderBy(c => c, System.StringComparer.Ordinal).ToArray(), categories);
    }
}
