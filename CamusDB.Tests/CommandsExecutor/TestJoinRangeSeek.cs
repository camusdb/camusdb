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
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A join with no equality key whose <c>ON</c> predicate bounds the leading column of a right-side
/// index with values of the left row (a "numbers table" join) reads, for each left row, only that
/// index range. Each test compares the seek with the same join written so that no seek applies
/// (<c>n.n + 0</c> in place of <c>n.n</c>), which runs as the plain nested loop. This fixture runs in
/// cluster mode on a shared node; <see cref="TestJoinRangeSeekStandalone"/> runs it in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestJoinRangeSeek : SharedNodeBaseTest
{
    private const int Numbers = 200;

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

    private static string Text(ColumnValue v) => v.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => v.LongValue.ToString(),
        ColumnType.Float64 => v.FloatValue.ToString("R"),
        _ => v.StrValue ?? "",
    };

    private static List<string> Lines(List<QueryResultRow> rows) =>
        rows.Select(r => string.Join("|", r.Row.Values.Select(Text))).ToList();

    private static async Task<string> NestedLoopDetail(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN " + sql);
        return string.Join("\n", plan.Where(r => r.Row["node"].StrValue == "nested-loop-join").Select(r => r.Row["detail"].StrValue));
    }

    /// <summary>
    /// <c>numbers</c> holds 0 .. <see cref="Numbers"/> - 1 under its primary key. <c>spans</c> holds the
    /// ranges: ordinary, empty (lo &gt; hi), NULL ends, fractional ends, out of the table, and one row.
    /// </summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE numbers (n INT64 PRIMARY KEY, sq INT64)");
        await Ddl(executor, database, db, "CREATE TABLE spans (id INT64 PRIMARY KEY, lo INT64 NULL, hi INT64 NULL, flo FLOAT64 NULL, fhi FLOAT64 NULL)");
        await Dml(executor, database, db, $"INSERT INTO numbers (n, sq) SELECT g, g * g FROM generate_series(0, {Numbers - 1}) g");
        await Dml(executor, database, db,
            "INSERT INTO spans (id, lo, hi, flo, fhi) VALUES " +
            "(1, 3, 7, 2.5, 6.5), (2, 10, 10, 9.9, 10.1), (3, 8, 5, 8.0, 5.0), (4, NULL, 4, NULL, 4.0), " +
            "(5, 190, 250, 189.5, 260.0), (6, -5, 2, -4.5, 1.5), (7, 50, 52, 50.0, 52.0)");
        return (db, database, executor);
    }

    private async Task AssertSameAsPlainLoop(
        CommandExecutor executor, DatabaseDescriptor database, string db, string seekSql, Dictionary<string, ColumnValue>? parameters = null)
    {
        string plainSql = seekSql.Replace("n.n ", "n.n + 0 ");

        List<string> seek = Lines(await Query(executor, database, db, seekSql, parameters));
        List<string> plain = Lines(await Query(executor, database, db, plainSql, parameters));

        CollectionAssert.AreEqual(plain, seek, seekSql);
        Assert.That(plain, Is.Not.Empty, "the oracle query must return rows: " + plainSql);

        StringAssert.Contains("range-seek=", await NestedLoopDetail(executor, database, db, seekSql), seekSql);
        StringAssert.DoesNotContain("range-seek=", await NestedLoopDetail(executor, database, db, plainSql), plainSql);
    }

    [Test]
    public async Task ABetweenOfLeftColumnsSeeksTheRightKeyForEachLeftRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n BETWEEN s.lo AND s.hi ORDER BY s.id, n.n");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n BETWEEN s.lo AND s.hi ORDER BY s.id, n.n");

        // span 1: 3..7, span 2: 10, span 5: 190..199, span 6: 0..2, span 7: 50..52. Span 3 is empty
        // (8 > 5) and span 4 has a NULL lower end, so neither gives a row.
        CollectionAssert.AreEqual(
            new long[] { 1, 1, 1, 1, 1, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7 },
            rows.Select(r => r.Row["id"].LongValue).ToArray());
        CollectionAssert.AreEqual(
            new long[] { 3, 4, 5, 6, 7, 10, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 0, 1, 2, 50, 51, 52 },
            rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    public async Task ComparisonsOnBothSidesOfTheKeyAndStrictBoundsSeekToo()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n > s.lo AND s.hi > n.n ORDER BY s.id, n.n");
        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON s.lo <= n.n AND n.n < s.hi AND n.n >= s.lo + 1 ORDER BY s.id, n.n");
    }

    [Test]
    public async Task FractionalBoundsOnAnIntegerKeyKeepTheRowsTheComparisonAccepts()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n >= s.flo AND n.n <= s.fhi ORDER BY s.id, n.n");
        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n > s.flo AND n.n < s.fhi ORDER BY s.id, n.n");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT n.n FROM spans s JOIN numbers n ON n.n >= s.flo AND n.n <= s.fhi WHERE s.id = 1 ORDER BY n.n");
        CollectionAssert.AreEqual(new long[] { 3, 4, 5, 6 }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    public async Task ALeftJoinPadsALeftRowWithAnEmptyOrNullRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s LEFT JOIN numbers n ON n.n BETWEEN s.lo AND s.hi ORDER BY s.id, n.n");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT s.id, n.n FROM spans s LEFT JOIN numbers n ON n.n BETWEEN s.lo AND s.hi WHERE s.id IN (3, 4) ORDER BY s.id");
        CollectionAssert.AreEqual(new long[] { 3, 4 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
        Assert.That(rows.All(r => r.Row["n"].Type == ColumnType.Null), Is.True);
    }

    [Test]
    public async Task BoundsMayUseExpressionsParametersAndARightFilter()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        Dictionary<string, ColumnValue> parameters = new() { ["@w"] = new(ColumnType.Integer64, 2) };

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n BETWEEN CAST(ceil(s.flo) AS int64) AND s.lo + @w " +
            "WHERE n.sq % 2 = 0 ORDER BY s.id, n.n", parameters);
    }

    [Test]
    public async Task ASecondaryIndexWithRepeatedKeysIsSought()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE events (id INT64 PRIMARY KEY, t INT64, label STRING)");
        await Ddl(executor, database, db, "CREATE INDEX ix_events_t ON events (t)");
        await Dml(executor, database, db, "INSERT INTO events (id, t, label) SELECT g, g % 40, 'e' || g FROM generate_series(1, 400) g");

        string sql = "SELECT s.id, e.id AS eid FROM spans s JOIN events e ON e.t BETWEEN s.lo AND s.hi ORDER BY s.id, e.id";
        StringAssert.Contains("range-seek=ix_events_t", await NestedLoopDetail(executor, database, db, sql));

        List<string> seek = Lines(await Query(executor, database, db, sql));
        List<string> plain = Lines(await Query(executor, database, db, sql.Replace("e.t BETWEEN", "e.t + 0 BETWEEN")));
        CollectionAssert.AreEqual(plain, seek);

        // t = id % 40 gives each key 10 events; the spans cover t = 0..2, 3..7 and 10.
        Assert.That(seek, Has.Count.EqualTo(90));
    }

    [Test]
    public async Task AnOpenRangeADescendingKeyOrARightColumnInABoundKeepsThePlainLoop()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE down (id INT64 PRIMARY KEY, k INT64)");
        await Ddl(executor, database, db, "CREATE INDEX ix_down_k ON down (k DESC)");
        await Dml(executor, database, db, "INSERT INTO down (id, k) SELECT g, g FROM generate_series(1, 100) g");

        foreach (string sql in new[]
                 {
                     "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n >= s.lo ORDER BY s.id, n.n",
                     "SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n BETWEEN s.lo AND n.sq ORDER BY s.id, n.n",
                     "SELECT s.id, d.k FROM spans s JOIN down d ON d.k BETWEEN s.lo AND s.hi ORDER BY s.id, d.k",
                 })
        {
            StringAssert.DoesNotContain("range-seek=", await NestedLoopDetail(executor, database, db, sql), sql);
            Assert.That(await Query(executor, database, db, sql), Is.Not.Empty, sql);
        }
    }

    [Test]
    public async Task ExplainAnalyzeReadsOnlyTheRangesOfTheLeftRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN (ANALYZE) SELECT s.id, n.n FROM spans s JOIN numbers n ON n.n BETWEEN s.lo AND s.hi");

        QueryResultRow join = plan.Single(r => r.Row["node"].StrValue == "nested-loop-join");
        Assert.AreEqual(22L, join.Row["actual_rows"].LongValue);

        // 22 index entries for the 22 rows, not 7 x 200 pairs.
        Assert.AreEqual(22L, join.Row["kv_scan_entries"].LongValue);
    }

    [Test]
    public async Task TheSameShapeAgainUsesItsOwnBoundsAndKeepsTheSeek()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        string sql = "SELECT n.n FROM spans s JOIN numbers n ON n.n BETWEEN s.lo AND s.hi WHERE s.id = @id ORDER BY n.n";

        for (int round = 0; round < 2; round++)
        {
            List<QueryResultRow> one = await Query(executor, database, db, sql, new() { ["@id"] = new(ColumnType.Integer64, 1) });
            List<QueryResultRow> seven = await Query(executor, database, db, sql, new() { ["@id"] = new(ColumnType.Integer64, 7) });

            CollectionAssert.AreEqual(new long[] { 3, 4, 5, 6, 7 }, one.Select(r => r.Row["n"].LongValue).ToArray());
            CollectionAssert.AreEqual(new long[] { 50, 51, 52 }, seven.Select(r => r.Row["n"].LongValue).ToArray());
        }
    }

    [Test]
    public async Task ACommaJoinWithTheBoundsInWhereSeeksToo()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "SELECT s.id, n.n FROM spans s, numbers n WHERE n.n BETWEEN s.lo AND s.hi ORDER BY s.id, n.n");
    }

    [Test]
    public async Task ADerivedLeftSideSeeksTheRightKey()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await AssertSameAsPlainLoop(executor, database, db,
            "WITH w AS (SELECT id, lo * 1.0 + 0.5 AS a, hi * 1.0 AS b FROM spans) " +
            "SELECT w.id, n.n FROM w JOIN numbers n ON n.n >= ceil(w.a) AND n.n <= floor(w.b) ORDER BY w.id, n.n");
    }
}

internal sealed class TestJoinRangeSeekStandalone : TestJoinRangeSeek
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
