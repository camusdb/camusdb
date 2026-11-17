/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The outer values that a correlated body reads from each outer row: a <c>LATERAL</c> body, an expression-only
/// <c>LATERAL</c> body and a correlated subquery read the outer names they use, by the name rules of the outer
/// query, from a row of one table, a row of a join, and a row of a derived table whose columns keep qualified
/// row keys. Each test compares the result with the same query written without the correlated body.
/// </summary>
[NonParallelizable]
internal sealed class TestLateralOuterValueReads : SharedNodeBaseTest
{
    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static long?[] Longs(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column].Type == ColumnType.Null ? (long?)null : r.Row[column].LongValue).ToArray();

    /// <summary>A derived table of <paramref name="rows"/> rows with 30 columns <c>c0</c> to <c>c29</c>, <c>cK = g + K</c>.</summary>
    private static string WideRows(int rows)
    {
        StringBuilder columns = new();

        for (int k = 0; k < 30; k++)
        {
            if (k > 0)
                columns.Append(", ");

            columns.Append(k == 0 ? "g AS c0" : $"g + {k} AS c{k}");
        }

        return $"(SELECT {columns} FROM generate_series(1, {rows}) g) t";
    }

    /// <summary>Four chained expression-only <c>LATERAL</c> bodies over <see cref="WideRows"/>; each level reads the left row and the levels before it.</summary>
    private static string ChainedLaterals(int rows, string select) =>
        $"SELECT {select} FROM {WideRows(rows)} " +
        "CROSS JOIN LATERAL (SELECT t.c3 * 2 + t.c17 AS a) l1 " +
        "CROSS JOIN LATERAL (SELECT l1.a - t.c29 AS b) l2 " +
        "CROSS JOIN LATERAL (SELECT l2.b * l1.a % 1000 AS c) l3 " +
        "CROSS JOIN LATERAL (SELECT l3.c + t.c0 AS d) l4";

    /// <summary>The value of <c>l4.d</c> of <see cref="ChainedLaterals"/>, written as one expression of the left row.</summary>
    private const string InlineD = "((t.c3 * 2 + t.c17 - t.c29) * (t.c3 * 2 + t.c17) % 1000 + t.c0)";

    [Test]
    public async Task ChainedExpressionLateralsGiveTheValuesOfTheInlineExpressions()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        List<QueryResultRow> lateral = await Query(executor, database, db, ChainedLaterals(500, "t.c0, l4.d") + " ORDER BY t.c0");
        List<QueryResultRow> inline = await Query(executor, database, db, $"SELECT t.c0, {InlineD} AS d FROM {WideRows(500)} ORDER BY t.c0");

        Assert.That(lateral, Has.Count.EqualTo(500));
        CollectionAssert.AreEqual(Longs(inline, "c0"), Longs(lateral, "c0"));
        CollectionAssert.AreEqual(Longs(inline, "d"), Longs(lateral, "d"));
    }

    [Test]
    public async Task ALateralBodyWithASubqueryReadsEachLeftRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        // FROM in the body keeps the subquery run, which gets the left values as hidden parameters.
        List<QueryResultRow> rows = await Query(executor, database, db,
            $"SELECT t.c0, s.n FROM {WideRows(40)} " +
            "CROSS JOIN LATERAL (SELECT COUNT(*) AS n FROM generate_series(t.c0, t.c29) x WHERE x % 2 = 0) s ORDER BY t.c0");

        Assert.That(rows, Has.Count.EqualTo(40));
        CollectionAssert.AreEqual(Enumerable.Range(1, 40).Select(g => (long?)15).ToArray(), Longs(rows, "n"));
    }

    [Test]
    public async Task ALateralReadsAColumnThatTwoLeftTablesHave()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, l.d FROM items i JOIN items j ON j.id = i.id + 1 " +
            "CROSS JOIN LATERAL (SELECT j.v * 100 + i.v AS d) l WHERE i.id <= 5 ORDER BY i.id");

        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4, 5 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long?[] { 2010, 3020, 4030, 5040, 6050 }, Longs(rows, "d"));
    }

    [Test]
    public async Task ALateralReadsADerivedTableOfAStarOverAJoin()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The cells of a * over a join keep qualified row keys; the body reads them through the derived columns.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT s.id, l.d FROM (SELECT * FROM items i JOIN tags t ON t.item_id = i.id) s " +
            "CROSS JOIN LATERAL (SELECT s.v + s.tid AS d) l ORDER BY s.tid");

        CollectionAssert.AreEqual(new long?[] { 2, 4, 4, 9 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long?[] { 21, 42, 43, 94 }, Longs(rows, "d"));
    }

    [Test]
    public async Task AnExpressionLateralReadsAParameterAndANewLeftValueOnEachRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT g, l.x, m.y FROM generate_series(1, 5) g " +
            "CROSS JOIN LATERAL (SELECT g * @p AS x) l CROSS JOIN LATERAL (SELECT l.x + @p AS y) m ORDER BY g",
            new Dictionary<string, ColumnValue> { ["@p"] = new ColumnValue(ColumnType.Integer64, 7L) });

        CollectionAssert.AreEqual(new long?[] { 7, 14, 21, 28, 35 }, Longs(rows, "x"));
        CollectionAssert.AreEqual(new long?[] { 14, 21, 28, 35, 42 }, Longs(rows, "y"));
    }

    [Test]
    public async Task ACorrelatedSubqueryReadsARowOfOneTableAndARowOfAJoin()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> single = await Query(executor, database, db,
            "SELECT id, (SELECT COUNT(*) FROM tags t WHERE t.item_id = id) AS n FROM items WHERE id <= 5 ORDER BY id");

        CollectionAssert.AreEqual(new long?[] { 0, 1, 0, 2, 0 }, Longs(single, "n"));

        List<QueryResultRow> joined = await Query(executor, database, db,
            "SELECT i.id, (SELECT COUNT(*) FROM tags t WHERE t.item_id = j.id) AS n " +
            "FROM items i JOIN items j ON j.id = i.id + 1 WHERE i.id <= 4 ORDER BY i.id");

        CollectionAssert.AreEqual(new long?[] { 1, 0, 2, 0 }, Longs(joined, "n"));

        List<QueryResultRow> exists = await Query(executor, database, db,
            "SELECT i.id FROM items i JOIN items j ON j.id = i.id + 1 " +
            "WHERE EXISTS (SELECT 1 FROM tags t WHERE t.item_id = j.id) ORDER BY i.id");

        CollectionAssert.AreEqual(new long?[] { 1, 3, 8 }, Longs(exists, "id"));
    }

    /// <summary>Twelve items (<c>v = id * 10</c>), and four tags on items 2, 4 (two tags) and 9.</summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)");
        await Ddl(executor, database, db, "CREATE TABLE tags (tid INT64 PRIMARY KEY, item_id INT64, tag STRING)");
        await Dml(executor, database, db, "INSERT INTO items (id, v) SELECT g, g * 10 FROM generate_series(1, 12) g");
        await Dml(executor, database, db, "INSERT INTO tags (tid, item_id, tag) VALUES (1, 2, 'red'), (2, 4, 'red'), (3, 4, 'blue'), (4, 9, 'green')");
        return (db, database, executor);
    }

    /// <summary>
    /// The time of 64,000 left rows of 30 columns through four chained expression-only <c>LATERAL</c> bodies,
    /// against the same values written inline. Prints both times; the result of the two forms must be the same.
    /// </summary>
    [Test]
    [Explicit("Benchmark: run alone to measure the outer-value reads of chained LATERAL bodies")]
    public async Task BenchmarkChainedExpressionLaterals()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        const int rows = 64_000;
        string lateralSql = ChainedLaterals(rows, "SUM(l4.d) AS s, COUNT(*) AS n");
        string inlineSql = $"SELECT SUM({InlineD}) AS s, COUNT(*) AS n FROM {WideRows(rows)}";

        // Warm the plan and the JIT.
        await Query(executor, database, db, ChainedLaterals(1_000, "SUM(l4.d) AS s"));
        await Query(executor, database, db, inlineSql);

        long?[] lateralResult = [];
        long?[] inlineResult = [];
        List<double> lateralMs = [];
        List<double> inlineMs = [];

        for (int run = 0; run < 3; run++)
        {
            Stopwatch watch = Stopwatch.StartNew();
            List<QueryResultRow> lateral = await Query(executor, database, db, lateralSql);
            lateralMs.Add(watch.Elapsed.TotalMilliseconds);
            lateralResult = [.. Longs(lateral, "s"), .. Longs(lateral, "n")];

            watch.Restart();
            List<QueryResultRow> inline = await Query(executor, database, db, inlineSql);
            inlineMs.Add(watch.Elapsed.TotalMilliseconds);
            inlineResult = [.. Longs(inline, "s"), .. Longs(inline, "n")];
        }

        CollectionAssert.AreEqual(inlineResult, lateralResult);

        TestContext.Out.WriteLine(
            $"chained LATERAL: {string.Join(", ", lateralMs.Select(ms => ms.ToString("F0")))} ms; " +
            $"inline: {string.Join(", ", inlineMs.Select(ms => ms.ToString("F0")))} ms; " +
            $"best LATERAL cost over inline: {(lateralMs.Min() - inlineMs.Min()) / rows / 4 * 1000:F2} us per row per level");
        Console.WriteLine(
            $"chained LATERAL: {string.Join(", ", lateralMs.Select(ms => ms.ToString("F0")))} ms; inline: {string.Join(", ", inlineMs.Select(ms => ms.ToString("F0")))} ms");
    }
}
