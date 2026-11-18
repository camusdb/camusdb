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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>LATERAL</c> derived tables end to end, through <see cref="ExecuteSQLTicket"/>: a derived table that
/// reads the <c>FROM</c> items to its left and runs once for each left row, in each join form, with the name
/// rules of PostgreSQL. This fixture runs in cluster mode on a shared node;
/// <see cref="TestLateralJoinsStandalone"/> runs the same tests in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestLateralJoins : SharedNodeBaseTest
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

    private static async Task<ExecuteNonSQLResult> Dml(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
            return result;
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

    private static long[] Longs(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column].LongValue).ToArray();

    private static string?[] Strings(List<QueryResultRow> rows, string column) =>
        rows.Select(r => r.Row[column].Type == ColumnType.Null ? null : r.Row[column].StrValue).ToArray();

    /// <summary>
    /// Twelve items in three groups (<c>grp = id % 3</c>, <c>v = id * 10</c>), and four tags on items 2, 4
    /// (two tags) and 9.
    /// </summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, grp INT64, v INT64)");
        await Ddl(executor, database, db, "CREATE TABLE tags (tid INT64 PRIMARY KEY, item_id INT64, tag STRING)");
        await Dml(executor, database, db, "INSERT INTO items (id, grp, v) SELECT g, g % 3, g * 10 FROM generate_series(1, 12) g");
        await Dml(executor, database, db, "INSERT INTO tags (tid, item_id, tag) VALUES (1, 2, 'red'), (2, 4, 'red'), (3, 4, 'blue'), (4, 9, 'green')");
        return (db, database, executor);
    }

    [Test]
    public async Task ACrossJoinLateralNamesAComputedColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, c.d, c.d + 1 AS e FROM items i CROSS JOIN LATERAL (SELECT i.v * 2 AS d) c WHERE i.id <= 3 ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 20, 40, 60 }, Longs(rows, "d"));
        CollectionAssert.AreEqual(new long[] { 21, 41, 61 }, Longs(rows, "e"));
    }

    [Test]
    public async Task ACommaLateralGivesTheTopRowsOfEachGroup()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT g.grp, t.id FROM (SELECT DISTINCT grp FROM items) g, " +
            "LATERAL (SELECT x.id FROM items x WHERE x.grp = g.grp ORDER BY x.v DESC LIMIT 2) t ORDER BY g.grp, t.id");

        CollectionAssert.AreEqual(new long[] { 0, 0, 1, 1, 2, 2 }, Longs(rows, "grp"));
        CollectionAssert.AreEqual(new long[] { 9, 12, 7, 10, 8, 11 }, Longs(rows, "id"));
    }

    [Test]
    public async Task ALeftJoinLateralKeepsALeftRowWithNoRightRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, t.tag FROM items i LEFT JOIN LATERAL (SELECT tag FROM tags WHERE tags.item_id = i.id) t ON true " +
            "WHERE i.id BETWEEN 2 AND 5 ORDER BY i.id, t.tag");

        CollectionAssert.AreEqual(new long[] { 2, 3, 4, 4, 5 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new string?[] { "red", null, "blue", "red", null }, Strings(rows, "tag"));
    }

    [Test]
    public async Task ABodyOfManyColumnsWithANestedSubqueryThatReadsALeftColumnRuns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The nested subquery reads a left column, so the body binds again for each left row.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, c.k, c.n FROM items i CROSS JOIN LATERAL " +
            "(SELECT i.v AS k, (SELECT COUNT(*) FROM tags t WHERE t.item_id = i.id) AS n) c " +
            "WHERE i.id BETWEEN 2 AND 4 ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 2, 3, 4 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 20, 30, 40 }, Longs(rows, "k"));
        CollectionAssert.AreEqual(new long[] { 1, 0, 2 }, Longs(rows, "n"));

        List<QueryResultRow> unpivot = await Query(executor, database, db,
            "SELECT s.slot, s.n FROM items i CROSS JOIN LATERAL " +
            "(VALUES ('v', i.v), ('tags', (SELECT COUNT(*) FROM tags t WHERE t.item_id = i.id))) AS s (slot, n) " +
            "WHERE i.id = 4 ORDER BY s.slot");

        CollectionAssert.AreEqual(new string?[] { "tags", "v" }, Strings(unpivot, "slot"));
        CollectionAssert.AreEqual(new long[] { 2, 40 }, Longs(unpivot, "n"));
    }

    [Test]
    public async Task AnInnerJoinLateralAppliesItsOnCondition()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, t.tid FROM items i JOIN LATERAL (SELECT tid FROM tags WHERE item_id = i.id) t ON t.tid > 1 ORDER BY t.tid");

        CollectionAssert.AreEqual(new long[] { 4, 4, 9 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 2, 3, 4 }, Longs(rows, "tid"));
    }

    [Test]
    public async Task ALateralAggregateCountsForEachLeftRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM tags WHERE item_id = i.id) c WHERE i.id <= 4 ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 0, 1, 0, 2 }, Longs(rows, "n"));
    }

    [Test]
    public async Task ASecondLateralReadsTheFirst()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, y.b FROM items i, LATERAL (SELECT i.v + 1 AS a) x, LATERAL (SELECT x.a * 2 AS b) y WHERE i.id <= 2 ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 1, 2 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 22, 42 }, Longs(rows, "b"));
    }

    [Test]
    public async Task ALateralThatReadsNoLeftItemIsAPlainDerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, t.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM tags) t WHERE i.id <= 2 ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 1, 2 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 4, 4 }, Longs(rows, "n"));
    }

    [Test]
    public async Task ARightJoinLateralIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id FROM items i RIGHT JOIN LATERAL (SELECT i.v AS w) t ON true"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        StringAssert.Contains("LATERAL", error.Message);
    }

    [Test]
    public async Task ADerivedTableWithoutLateralCannotReadTheItemsToItsLeft()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id, t.w FROM items i, (SELECT i.v AS w) t"));

        StringAssert.Contains("'i'", error!.Message);
    }

    [Test]
    public async Task ALateralCannotReadAnItemToItsRight()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id FROM items i, LATERAL (SELECT j.v AS w) t, items j"));
    }

    [Test]
    public async Task ANameResolvesInsideTheLateralQueryFirst()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // grp is a column of x, so it reads x.grp, not i.grp; item_id = id reads i.id, which tags does not have.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, s.grp, t.n FROM items i, LATERAL (SELECT grp FROM items x WHERE x.id = 1) s, " +
            "LATERAL (SELECT COUNT(*) AS n FROM tags WHERE item_id = id) t WHERE i.id IN (3, 4) ORDER BY i.id");

        CollectionAssert.AreEqual(new long[] { 3, 4 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 1, 1 }, Longs(rows, "grp"));
        CollectionAssert.AreEqual(new long[] { 0, 2 }, Longs(rows, "n"));
    }

    [Test]
    public async Task ALateralReadsAParameterAndANullLeftValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Dml(executor, database, db, "INSERT INTO items (id, grp, v) VALUES (13, NULL, NULL)");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, c.w FROM items i CROSS JOIN LATERAL (SELECT COALESCE(i.v, 0) + @p AS w) c WHERE i.id IN (1, 13) ORDER BY i.id",
            new Dictionary<string, ColumnValue> { ["@p"] = new ColumnValue(ColumnType.Integer64, 5L) });

        CollectionAssert.AreEqual(new long[] { 1, 13 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 15, 5 }, Longs(rows, "w"));
    }

    [Test]
    public async Task ALateralReadsALeftJoinAndALeftDerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT d.id, t.tid, x.s FROM (SELECT id, v FROM items WHERE id <= 4) d JOIN tags t ON t.item_id = d.id " +
            "CROSS JOIN LATERAL (SELECT d.v + t.tid AS s) x ORDER BY t.tid");

        CollectionAssert.AreEqual(new long[] { 2, 4, 4 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 21, 42, 43 }, Longs(rows, "s"));
    }

    [Test]
    public async Task AStarAndAColumnListOverALateral()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> star = await Query(executor, database, db,
            "SELECT * FROM items i, LATERAL (SELECT tag FROM tags WHERE item_id = i.id) t WHERE i.id = 4 ORDER BY t.tag");
        Assert.That(star, Has.Count.EqualTo(2));
        CollectionAssert.AreEqual(new string?[] { "blue", "red" }, Strings(star, "tag"));

        List<QueryResultRow> named = await Query(executor, database, db,
            "SELECT i.id, t.a, t.b FROM items i, LATERAL (SELECT i.v, i.v * 2) AS t (a, b) WHERE i.id = 2");
        CollectionAssert.AreEqual(new long[] { 20 }, Longs(named, "a"));
        CollectionAssert.AreEqual(new long[] { 40 }, Longs(named, "b"));
    }

    [Test]
    public async Task ALateralBodyCanHoldAUnionAndAWindow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> union = await Query(executor, database, db,
            "SELECT i.id, s.n FROM items i, LATERAL (SELECT i.v AS n UNION ALL SELECT i.v + 1) s WHERE i.id = 1 ORDER BY s.n");
        CollectionAssert.AreEqual(new long[] { 10, 11 }, Longs(union, "n"));

        List<QueryResultRow> ranked = await Query(executor, database, db,
            "SELECT g.grp, r.id, r.rn FROM (SELECT DISTINCT grp FROM items) g, " +
            "LATERAL (SELECT id, ROW_NUMBER() OVER (ORDER BY v DESC) AS rn FROM items x WHERE x.grp = g.grp) r " +
            "WHERE r.rn = 1 ORDER BY g.grp");
        CollectionAssert.AreEqual(new long[] { 12, 10, 11 }, Longs(ranked, "id"));
    }

    [Test]
    public async Task ALateralInsideACteAndInsideACorrelatedSubquery()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> cte = await Query(executor, database, db,
            "WITH c AS (SELECT i.id, n.k FROM items i, LATERAL (SELECT COUNT(*) AS k FROM tags WHERE item_id = i.id) n) " +
            "SELECT id FROM c WHERE k > 0 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 9 }, Longs(cte, "id"));

        // The LATERAL body reads t, its left item, and i, the query around the subquery.
        List<QueryResultRow> nested = await Query(executor, database, db,
            "SELECT i.id, (SELECT MAX(s.k) FROM tags t, LATERAL (SELECT t.tid + i.id AS k) s WHERE t.item_id = i.id) AS m " +
            "FROM items i WHERE i.id IN (2, 4) ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 3, 7 }, Longs(nested, "m"));
    }

    [Test]
    public async Task RepeatedLeftValuesRunTheBodyOnceAndAVolatileBodyRunsForEachRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            List<QueryResultRow> rows = await Query(executor, database, db,
                "SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM items x WHERE x.grp = i.grp) c");

            Assert.That(rows, Has.Count.EqualTo(12));
            Assert.That(rows.All(r => r.Row["n"].LongValue == 4), Is.True);
            Assert.That(probe.Runs, Is.EqualTo(3));
            Assert.That(probe.Hits, Is.EqualTo(9));
        }

        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            List<QueryResultRow> rows = await Query(executor, database, db,
                "SELECT i.id, c.r FROM items i, LATERAL (SELECT i.grp + random() AS r) c");

            Assert.That(rows, Has.Count.EqualTo(12));
            Assert.That(probe.Runs, Is.EqualTo(12));
        }
    }

    [Test]
    public async Task ExplainShowsTheLateralJoin()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN SELECT i.id, t.tid FROM items i JOIN LATERAL (SELECT tid FROM tags WHERE item_id = i.id) t ON t.tid = i.id");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("nested-loop-join", text);
        StringAssert.Contains("lateral", text);
    }

    [Test]
    public async Task ExplainAnalyzeRunsALateralJoin()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN (ANALYZE) SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM tags WHERE item_id = i.id) c");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("lateral", text);
    }

    [Test]
    public async Task ALockingReadOverALateralIsRefusedAsForADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id FROM items i, LATERAL (SELECT i.v AS w) s FOR UPDATE"));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refused!.Code);
    }

    [Test]
    public async Task AViewWithALateralKeepsItAndItsTextParsesAgain()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE VIEW tag_counts AS SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM tags WHERE item_id = i.id) c");

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, n FROM tag_counts WHERE n > 0 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 9 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 1, 2, 1 }, Longs(rows, "n"));

        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW tag_counts"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("LATERAL (", shown);

        await Ddl(executor, database, db, shown.Replace("CREATE VIEW", "CREATE OR REPLACE VIEW"));
        string again = (await Query(executor, database, db, "SHOW CREATE VIEW tag_counts"))[0].Row["create view"].StrValue!;
        Assert.That(again, Is.EqualTo(shown));
        Assert.AreEqual(12, (await Query(executor, database, db, "SELECT id FROM tag_counts")).Count);
    }

    [Test]
    public async Task AFunctionInFromReadsALeftColumnWithOrWithoutTheKeyword()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        foreach (string sql in new[]
        {
            "SELECT i.id, g.n FROM items i, generate_series(1, i.id) AS g (n) WHERE i.id <= 3 ORDER BY i.id, g.n",
            "SELECT i.id, g.n FROM items i CROSS JOIN LATERAL generate_series(1, i.id) AS g (n) WHERE i.id <= 3 ORDER BY i.id, g.n",
        })
        {
            List<QueryResultRow> rows = await Query(executor, database, db, sql);

            CollectionAssert.AreEqual(new long[] { 1, 2, 2, 3, 3, 3 }, Longs(rows, "id"), sql);
            CollectionAssert.AreEqual(new long[] { 1, 1, 2, 1, 2, 3 }, Longs(rows, "n"), sql);
        }

        // grp 0 gives an empty series, so the LEFT JOIN pads item 3 with NULL. A one-column function names
        // its column after the alias.
        List<QueryResultRow> padded = await Query(executor, database, db,
            "SELECT i.id, s FROM items i LEFT JOIN LATERAL generate_series(1, i.grp) s ON true WHERE i.id IN (2, 3) ORDER BY i.id, s");
        CollectionAssert.AreEqual(new long[] { 2, 2, 3 }, Longs(padded, "id"));
        Assert.That(padded[2].Row["s"].Type, Is.EqualTo(ColumnType.Null));
    }

    [Test]
    public async Task AFunctionThatNamesAColumnWithNoLeftItemIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT n FROM generate_series(1, id) AS g (n)"));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));
    }

    [Test]
    public async Task AViewWithAFunctionThatReadsALeftColumnKeepsItsText()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE VIEW expanded AS SELECT i.id, g.n FROM items i, generate_series(1, i.grp) AS g (n)");

        Assert.AreEqual(12, (await Query(executor, database, db, "SELECT id FROM expanded")).Count);

        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW expanded"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("generate_series(1, i.grp) AS g(n)", shown);

        await Ddl(executor, database, db, shown.Replace("CREATE VIEW", "CREATE OR REPLACE VIEW"));
        string again = (await Query(executor, database, db, "SHOW CREATE VIEW expanded"))[0].Row["create view"].StrValue!;
        Assert.That(again, Is.EqualTo(shown));
    }

    [Test]
    public async Task TheWriteStatementsReadALateral()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE counts (id INT64 PRIMARY KEY, n INT64)");

        await Dml(executor, database, db,
            "INSERT INTO counts (id, n) SELECT i.id, c.n FROM items i, LATERAL (SELECT COUNT(*) AS n FROM tags WHERE item_id = i.id) c");
        List<QueryResultRow> inserted = await Query(executor, database, db, "SELECT id, n FROM counts WHERE n > 0 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4, 9 }, Longs(inserted, "id"));

        await Dml(executor, database, db,
            "UPDATE counts SET n = s.w FROM items i CROSS JOIN LATERAL (SELECT i.v * 2 AS w) s WHERE i.id = counts.id AND counts.id <= 2");
        List<QueryResultRow> updated = await Query(executor, database, db, "SELECT id, n FROM counts WHERE id <= 3 ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 20, 40, 0 }, Longs(updated, "n"));

        await Ddl(executor, database, db,
            "CREATE TABLE copied AS SELECT i.id, s.w FROM items i CROSS JOIN LATERAL (SELECT i.v + 1 AS w) s WHERE i.id <= 3");
        List<QueryResultRow> copied = await Query(executor, database, db, "SELECT w FROM copied ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 11, 21, 31 }, Longs(copied, "w"));
    }

    /// <summary>
    /// Each row as text, with the type of each cell, for the columns <paramref name="columns"/> in order. Two
    /// reads that must give the same rows give the same text.
    /// </summary>
    private static string[] Render(List<QueryResultRow> rows, params string[] columns) =>
        rows.Select(r => string.Join(" | ", columns.Select(c =>
        {
            ColumnValue v = r.Row[c];
            return v.Type == ColumnType.Null ? "NULL" : $"{v.Type}:{v}";
        }))).ToArray();

    /// <summary>
    /// The SQL of <paramref name="sql"/> with <c>LIMIT 1</c> added to the end of each LATERAL body that the
    /// marker <c>/*b*/</c> closes. The limit keeps the body a subquery that runs for each left row, so a test
    /// can compare the evaluation of a body of expressions with that run.
    /// </summary>
    private static string AsSubqueryRuns(string sql) => sql.Replace("/*b*/", " LIMIT 1");

    [Test]
    public async Task ABodyOfExpressionsRunsNoSubqueryAndGivesTheRowsOfASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        Dictionary<string, ColumnValue> parameters = new() { ["@px"] = new ColumnValue(ColumnType.Float64, 0.25) };

        // v - 50 is zero for item 5, so its zf and wx are NULL.
        const string sql =
            "SELECT i.id, d.denom, z.zf, w.wx FROM items i " +
            "CROSS JOIN LATERAL (SELECT i.v - 50 AS denom/*b*/) AS d " +
            "CROSS JOIN LATERAL (SELECT CASE WHEN d.denom = 0 THEN NULL ELSE 100.0 / d.denom END AS zf/*b*/) AS z " +
            "CROSS JOIN LATERAL (SELECT @px + z.zf AS wx/*b*/) AS w ORDER BY i.id";

        List<QueryResultRow> evaluated;
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            evaluated = await Query(executor, database, db, sql.Replace("/*b*/", ""), parameters);
            Assert.That(probe.Runs, Is.EqualTo(0), "a body of expressions runs no subquery");
        }

        List<QueryResultRow> run;
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            run = await Query(executor, database, db, AsSubqueryRuns(sql), parameters);
            Assert.That(probe.Runs, Is.GreaterThan(0));
        }

        CollectionAssert.AreEqual(Render(run, "id", "denom", "zf", "wx"), Render(evaluated, "id", "denom", "zf", "wx"));
        Assert.That(evaluated, Has.Count.EqualTo(12));
        Assert.That(evaluated[4].Row["zf"].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(evaluated[4].Row["wx"].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(evaluated[0].Row["wx"].FloatValue, Is.EqualTo(0.25 + 100.0 / -40).Within(1e-12));
    }

    [Test]
    public async Task ABodyOfExpressionsKeepsTheTypesNamesAndNullsOfASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // m mixes INT64 and FLOAT64 branches, so its column is FLOAT64 and each cell is widened to it.
        const string sql =
            "SELECT i.id, c.m, c.s, c.g, c.b, c.n, k.a, k.z FROM items i " +
            "CROSS JOIN LATERAL (SELECT CASE WHEN i.id > 6 THEN i.id ELSE 0.5 END AS m, " +
            "CAST(i.v AS STRING) || '-' AS s, i.grp IN (0, 2) AS g, i.v BETWEEN 30 AND 60 AS b, " +
            "NULLIF(i.grp, 1) AS n/*b*/) AS c " +
            "CROSS JOIN LATERAL (SELECT i.v + 1, -i.v/*b*/) AS k (a, z) ORDER BY i.id";
        string[] columns = ["id", "m", "s", "g", "b", "n", "a", "z"];

        List<QueryResultRow> evaluated = await Query(executor, database, db, sql.Replace("/*b*/", ""));
        List<QueryResultRow> run = await Query(executor, database, db, AsSubqueryRuns(sql));

        CollectionAssert.AreEqual(Render(run, columns), Render(evaluated, columns));
        Assert.That(evaluated.All(r => r.Row["m"].Type == ColumnType.Float64), Is.True, "every m cell is FLOAT64");
        Assert.That(evaluated[0].Row["s"].StrValue, Is.EqualTo("10-"));
        Assert.That(evaluated[0].Row["a"].LongValue, Is.EqualTo(11));
        Assert.That(evaluated[0].Row["z"].LongValue, Is.EqualTo(-10));
    }

    [Test]
    public async Task ALeftJoinOfABodyOfExpressionsPadsTheRowsItsOnRefuses()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string sql =
            "SELECT i.id, c.d FROM items i LEFT JOIN LATERAL (SELECT i.v * 2 AS d/*b*/) AS c ON c.d > 60 " +
            "WHERE i.id <= 5 ORDER BY i.id";

        List<QueryResultRow> evaluated = await Query(executor, database, db, sql.Replace("/*b*/", ""));
        List<QueryResultRow> run = await Query(executor, database, db, AsSubqueryRuns(sql));

        CollectionAssert.AreEqual(Render(run, "id", "d"), Render(evaluated, "id", "d"));
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4, 5 }, Longs(evaluated, "id"));
        Assert.That(evaluated.Take(3).All(r => r.Row["d"].Type == ColumnType.Null), Is.True);
        CollectionAssert.AreEqual(new long[] { 80, 100 }, evaluated.Skip(3).Select(r => r.Row["d"].LongValue).ToArray());
    }

    [Test]
    public async Task AnErrorInABodyOfExpressionsIsTheErrorOfASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // i.id - 3 is zero for item 3 only.
        const string sql = "SELECT i.id, c.q FROM items i CROSS JOIN LATERAL (SELECT 100 / (i.id - 3) AS q/*b*/) AS c";

        CamusDBException? evaluated = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db, sql.Replace("/*b*/", "")));
        CamusDBException? run = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db, AsSubqueryRuns(sql)));

        Assert.That(evaluated!.Code, Is.EqualTo(run!.Code));
        Assert.That(evaluated.Message, Is.EqualTo(run.Message));

        // With item 3 left out, no row divides by zero.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, c.q FROM items i CROSS JOIN LATERAL (SELECT 100 / (i.id - 3) AS q) AS c WHERE i.id IN (1, 5) ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { -50, 50 }, Longs(rows, "q"));
    }

    [Test]
    public async Task ExplainNamesTheColumnsOfABodyOfExpressionsAndOtherBodiesKeepTheSubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        string expressions = await PlanText(executor, database, db,
            "EXPLAIN SELECT i.id, c.d, c.e FROM items i CROSS JOIN LATERAL (SELECT i.v * 2 AS d, i.v + 1 AS e) AS c");
        StringAssert.Contains("lateral=expressions(d, e)", expressions);

        // A scalar subquery that reads the left row runs for each left row, but the body does not.
        StringAssert.Contains("lateral=expressions(d)", await PlanText(executor, database, db,
            "EXPLAIN SELECT i.id, c.d FROM items i CROSS JOIN LATERAL (SELECT (SELECT MAX(x.v) FROM items x WHERE x.grp = i.grp) - i.v AS d) AS c"));

        // A LIMIT, an EXISTS that reads the left row, a subquery whose own nested subquery reads the left row and
        // a volatile call each keep the subquery run.
        foreach (string body in new[]
        {
            "SELECT i.v * 2 AS d LIMIT 1",
            "SELECT EXISTS (SELECT 1 FROM items x WHERE x.grp = i.grp AND x.id > i.id) AS d",
            "SELECT (SELECT MAX(x.v) FROM items x WHERE x.grp = (SELECT i.grp)) AS d",
            "SELECT i.v + random() AS d",
        })
        {
            string text = await PlanText(executor, database, db, $"EXPLAIN SELECT i.id, c.d FROM items i CROSS JOIN LATERAL ({body}) AS c");
            StringAssert.Contains(", lateral", text, body);
            StringAssert.DoesNotContain("lateral=expressions", text, body);
        }

        // An uncorrelated subquery runs one time, when the body binds, so the body is still a body of expressions.
        const string uncorrelated =
            "SELECT i.id, c.d FROM items i CROSS JOIN LATERAL (SELECT (SELECT MAX(v) FROM items) - i.v AS d) AS c ORDER BY i.id";
        StringAssert.Contains("lateral=expressions(d)", await PlanText(executor, database, db, "EXPLAIN " + uncorrelated));

        List<QueryResultRow> rows = await Query(executor, database, db, uncorrelated);
        CollectionAssert.AreEqual(Enumerable.Range(1, 12).Select(id => 120L - id * 10).ToArray(), Longs(rows, "d"));
    }

    /// <summary>
    /// The outcome of <paramref name="sql"/>: its rows rendered, or the code and the text of its error. Two forms of
    /// one statement compare equal only when both give the same rows or both fail with the same error.
    /// </summary>
    private static async Task<string[]> Outcome(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, params string[] columns)
    {
        try
        {
            return Render(await Query(executor, database, db, sql), columns);
        }
        catch (CamusDBException error)
        {
            return [$"error {error.Code}: {error.Message}"];
        }
    }

    /// <summary>
    /// The SQL of <paramref name="sql"/> with each <c>/*v*/(VALUES …) AS name (columns)/*v*/</c> written as a
    /// derived table over the same list with a <c>LIMIT</c>, a body that runs for each left row.
    /// </summary>
    private static string ValuesAsSubqueryRuns(string sql)
    {
        string[] parts = sql.Split("/*v*/");
        for (int i = 1; i < parts.Length; i += 2)
        {
            string list = parts[i];
            int alias = list.LastIndexOf(") AS ", System.StringComparison.Ordinal);
            string values = list[..(alias + 1)];
            string name = list[(alias + 5)..];
            parts[i] = $"(SELECT * FROM {values} AS w_{name} LIMIT 1000) AS {name.Split(' ')[0]}";
        }

        return string.Concat(parts);
    }

    [Test]
    public async Task AValuesListOfExpressionsGivesTheRowsOfASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // Column m mixes INT64 and FLOAT64 rows, so each cell is FLOAT64; column t mixes a string and a NULL.
        const string sql =
            "SELECT i.id, s.slot, s.m, s.t FROM items i CROSS JOIN LATERAL /*v*/(VALUES " +
            "('a', i.v, CAST(i.id AS STRING)), ('b', i.v / 4.0, NULL), ('c', CASE WHEN i.grp = 0 THEN NULL ELSE i.grp END, 'x' || CAST(i.id AS STRING))" +
            ") AS s (slot, m, t)/*v*/ ORDER BY i.id, s.slot";
        string[] columns = ["id", "slot", "m", "t"];

        List<QueryResultRow> evaluated;
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            evaluated = await Query(executor, database, db, sql.Replace("/*v*/", ""));
            Assert.That(probe.Runs, Is.EqualTo(0), "a VALUES list of expressions runs no subquery");
        }

        List<QueryResultRow> run;
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            run = await Query(executor, database, db, ValuesAsSubqueryRuns(sql));
            Assert.That(probe.Runs, Is.GreaterThan(0));
        }

        CollectionAssert.AreEqual(Render(run, columns), Render(evaluated, columns));
        Assert.That(evaluated, Has.Count.EqualTo(36));
        Assert.That(evaluated.All(r => r.Row["m"].Type is ColumnType.Float64 or ColumnType.Null), Is.True, "every m cell is FLOAT64 or NULL");
        Assert.That(evaluated[1].Row["m"].FloatValue, Is.EqualTo(2.5));
        Assert.That(evaluated[1].Row["t"].Type, Is.EqualTo(ColumnType.Null));

        StringAssert.Contains("lateral=expressions(slot, m, t)", await PlanText(executor, database, db, "EXPLAIN " + sql.Replace("/*v*/", "")));
    }

    [Test]
    public async Task AScalarSubqueryOfAValuesListRunsOnceForEachValueItReads()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The subquery reads only i.grp, which has 3 values; tag is 'red' for items 2 and 4 only, so the second
        // subquery gives NULL (no row) for every other item.
        const string sql =
            "SELECT i.id, s.slot, s.h, s.tag FROM items i CROSS JOIN LATERAL /*v*/(VALUES " +
            "('top', i.v + COALESCE((SELECT MAX(x.v) FROM items x WHERE x.grp = i.grp), 128), 'none'), " +
            "('mid', 0, COALESCE((SELECT t.tag FROM tags t WHERE t.item_id = i.id AND t.tag = 'red'), 'none'))" +
            ") AS s (slot, h, tag)/*v*/ ORDER BY i.id, s.slot";
        string[] columns = ["id", "slot", "h", "tag"];

        string explain = await PlanText(executor, database, db, "EXPLAIN " + sql.Replace("/*v*/", ""));
        StringAssert.Contains("lateral=expressions(slot, h, tag)", explain);

        List<QueryResultRow> evaluated;
        using (CorrelatedRunProbe.Observe(out CorrelatedRunProbe probe))
        {
            evaluated = await Query(executor, database, db, sql.Replace("/*v*/", ""));

            // 3 runs of the first subquery (one for each group) and 12 of the second (one for each item).
            Assert.That(probe.Runs, Is.EqualTo(15));
            Assert.That(probe.Hits, Is.EqualTo(9));
        }

        List<QueryResultRow> run = await Query(executor, database, db, ValuesAsSubqueryRuns(sql));

        CollectionAssert.AreEqual(Render(run, columns), Render(evaluated, columns));
        Assert.That(evaluated, Has.Count.EqualTo(24));

        // Item 1 is in group 1, whose largest v is 100 (item 10).
        Assert.That(evaluated[1].Row["h"].LongValue, Is.EqualTo(110));
        CollectionAssert.AreEqual(
            new[] { "none", "red", "none", "red" },
            evaluated.Where(r => r.Row["slot"].StrValue == "mid").Take(4).Select(r => r.Row["tag"].StrValue).ToArray());
    }

    [Test]
    public async Task AScalarSubqueryOfABodyOfExpressionsKeepsTheTypeAndTheNullOfASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // The subquery finds no row, so it is a FLOAT64 NULL: COALESCE takes the FLOAT64 type and 7 becomes 7.0,
        // so the division gives 3.5, not 3.
        // The second body has no subquery: a FLOAT64 NULL before the value makes the COALESCE FLOAT64 there too.
        const string sql =
            "SELECT i.id, c.q, c.n, k.r FROM items i CROSS JOIN LATERAL (SELECT " +
            "COALESCE((SELECT CAST(MAX(x.v) AS FLOAT64) FROM items x WHERE x.grp = i.grp AND x.id > 100), 7) / 2 AS q, " +
            "(SELECT x.v FROM items x WHERE x.id = i.id + 100) AS n/*b*/) AS c " +
            "CROSS JOIN LATERAL (SELECT COALESCE(CAST(NULL AS FLOAT64), i.grp) / 2 AS r/*b*/) AS k ORDER BY i.id";
        string[] columns = ["id", "q", "n", "r"];

        List<QueryResultRow> evaluated = await Query(executor, database, db, sql.Replace("/*b*/", ""));
        List<QueryResultRow> run = await Query(executor, database, db, AsSubqueryRuns(sql));

        CollectionAssert.AreEqual(Render(run, columns), Render(evaluated, columns));
        Assert.That(evaluated.All(r => r.Row["q"].Type == ColumnType.Float64 && r.Row["q"].FloatValue == 3.5), Is.True);
        Assert.That(evaluated.All(r => r.Row["n"].Type == ColumnType.Null), Is.True);
        Assert.That(evaluated[0].Row["r"].Type, Is.EqualTo(ColumnType.Float64));
        Assert.That(evaluated[0].Row["r"].FloatValue, Is.EqualTo(0.5));

        string explain = await PlanText(executor, database, db, "EXPLAIN " + sql.Replace("/*b*/", ""));
        StringAssert.Contains("lateral=expressions(q, n)", explain);
        StringAssert.Contains("lateral=expressions(r)", explain);
    }

    [Test]
    public async Task AScalarSubqueryOfAValuesListThatGivesTwoRowsFailsAsInASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // Item 4 has two tags. The run form binds the list for each row and runs each subquery then, also one in a
        // CASE branch that the row does not take, so the error comes for item 4 in both forms.
        const string sql =
            "SELECT i.id, s.k, s.tag FROM items i CROSS JOIN LATERAL /*v*/(VALUES " +
            "(1, CASE WHEN i.id = 4 THEN 'four' ELSE (SELECT t.tag FROM tags t WHERE t.item_id = i.id) END), (2, 'x')" +
            ") AS s (k, tag)/*v*/ WHERE i.id IN (2, 4) ORDER BY i.id, s.k";

        StringAssert.Contains("lateral=expressions(k, tag)", await PlanText(executor, database, db, "EXPLAIN " + sql.Replace("/*v*/", "")));

        string[] evaluated = await Outcome(executor, database, db, sql.Replace("/*v*/", ""), "id", "k", "tag");
        string[] run = await Outcome(executor, database, db, ValuesAsSubqueryRuns(sql), "id", "k", "tag");

        CollectionAssert.AreEqual(run, evaluated);
        Assert.That(evaluated, Has.Length.EqualTo(1));
        StringAssert.Contains("Scalar subquery returned more than one row", evaluated[0]);

        // Without item 4 no subquery gives two rows.
        List<QueryResultRow> rows = await Query(executor, database, db, sql.Replace("/*v*/", "").Replace("IN (2, 4)", "IN (2, 9)"));
        CollectionAssert.AreEqual(new long[] { 2, 2, 9, 9 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 1, 2, 1, 2 }, Longs(rows, "k"));
        CollectionAssert.AreEqual(new[] { "red", "x", "green", "x" }, Strings(rows, "tag"));
    }

    [Test]
    public async Task ABodyWhoseScalarSubqueryCannotBindFailsAsInASubqueryRun()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // A scalar subquery of two columns. Over no left row and over some left rows, the two forms give the
        // same outcome.
        foreach (string filter in new[] { "i.id < 0", "i.id <= 2" })
        {
            string sql =
                "SELECT i.id, s.k FROM items i CROSS JOIN LATERAL /*v*/(VALUES " +
                "((SELECT x.v, x.id FROM items x WHERE x.id = i.id)), (5)) AS s (k)/*v*/ " +
                $"WHERE {filter} ORDER BY i.id, s.k";

            string[] evaluated = await Outcome(executor, database, db, sql.Replace("/*v*/", ""), "id", "k");
            string[] run = await Outcome(executor, database, db, ValuesAsSubqueryRuns(sql), "id", "k");

            CollectionAssert.AreEqual(run, evaluated, filter);
        }
    }

    [Test]
    public async Task ALeftJoinOfAValuesListPadsTheLeftRowsWhoseRowsTheOnRefuses()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        const string sql =
            "SELECT i.id, s.k, s.d FROM items i LEFT JOIN LATERAL /*v*/(VALUES " +
            "(1, i.v * 2), (2, (SELECT MAX(x.v) FROM items x WHERE x.grp = i.grp))) AS s (k, d)/*v*/ ON s.d > 100 " +
            "WHERE i.id <= 6 ORDER BY i.id, s.k";
        string[] columns = ["id", "k", "d"];

        List<QueryResultRow> evaluated = await Query(executor, database, db, sql.Replace("/*v*/", ""));
        List<QueryResultRow> run = await Query(executor, database, db, ValuesAsSubqueryRuns(sql));

        CollectionAssert.AreEqual(Render(run, columns), Render(evaluated, columns));

        // k = 1 passes only for item 6 (v * 2 = 120). k = 2 is the largest v of the group: 100 for group 1
        // (items 1 and 4), which fails, and 110 or 120 for the other groups. So items 1 and 4 are padded.
        CollectionAssert.AreEqual(
            new[] { "1:NULL", "2:2", "3:2", "4:NULL", "5:2", "6:1", "6:2" },
            evaluated.Select(r => $"{r.Row["id"].LongValue}:{(r.Row["k"].Type == ColumnType.Null ? "NULL" : r.Row["k"].LongValue.ToString())}").ToArray());
    }

    [Test]
    public async Task AFunctionInTheFromOfASubqueryReadsTheRowAroundIt()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // grp is 0, 1 or 2, so each item gets the numbers 1 to grp; an item of group 0 gets none.
        List<QueryResultRow> direct = await Query(executor, database, db,
            "SELECT i.id, g AS n FROM items i CROSS JOIN LATERAL generate_series(1, i.grp) g ORDER BY i.id, n");
        Assert.That(direct, Has.Count.EqualTo(12));

        List<QueryResultRow> body = await Query(executor, database, db,
            "SELECT i.id, s.n FROM items i CROSS JOIN LATERAL (SELECT g AS n FROM generate_series(1, i.grp) g) AS s ORDER BY i.id, s.n");
        CollectionAssert.AreEqual(Render(direct, "id", "n"), Render(body, "id", "n"));

        // A left join keeps an item of group 0, whose series is empty.
        List<QueryResultRow> padded = await Query(executor, database, db,
            "SELECT i.id, s.n FROM items i LEFT JOIN LATERAL (SELECT g AS n FROM generate_series(1, i.grp) g) AS s ON true " +
            "WHERE i.id <= 4 ORDER BY i.id, s.n");
        CollectionAssert.AreEqual(new long[] { 1, 2, 2, 3, 4 }, Longs(padded, "id"));
        Assert.That(padded[3].Row["n"].Type, Is.EqualTo(ColumnType.Null));

        List<QueryResultRow> scalar = await Query(executor, database, db,
            "SELECT i.id, (SELECT SUM(g) FROM generate_series(1, i.grp) g) AS total FROM items i ORDER BY i.id");
        CollectionAssert.AreEqual(
            Enumerable.Range(1, 12).Select(id => (id % 3) switch { 0 => "NULL", 1 => "Integer64:1", _ => "Integer64:3" }).ToArray(),
            scalar.Select(r => r.Row["total"].Type == ColumnType.Null ? "NULL" : $"{r.Row["total"].Type}:{r.Row["total"].LongValue}").ToArray());

        List<QueryResultRow> exists = await Query(executor, database, db,
            "SELECT i.id FROM items i WHERE EXISTS (SELECT 1 FROM generate_series(1, i.grp) g WHERE g = 2) ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 2, 5, 8, 11 }, Longs(exists, "id"));

        List<QueryResultRow> membership = await Query(executor, database, db,
            "SELECT i.id FROM items i WHERE i.grp IN (SELECT g FROM generate_series(i.grp, i.grp + 1) g WHERE g > 1) ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 2, 5, 8, 11 }, Longs(membership, "id"));
    }

    [Test]
    public async Task AnUnknownNameInAFunctionInTheFromOfASubqueryIsStillAnUnknownColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? scalar = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id, (SELECT COUNT(*) FROM generate_series(1, i.nope) g) AS c FROM items i"));
        Assert.That(scalar!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));

        CamusDBException? lateral = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id, s.n FROM items i CROSS JOIN LATERAL (SELECT g AS n FROM generate_series(1, x.grp) g) AS s"));
        Assert.That(lateral!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));

        // The function is the only item of the FROM of the statement, so it has no row to read.
        CamusDBException? top = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT g FROM generate_series(1, grp) g"));
        Assert.That(top!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));
    }

    private static async Task<string> PlanText(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        List<QueryResultRow> plan = await Query(executor, database, db, sql);
        return string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
    }
}

/// <summary>The tests of <see cref="TestLateralJoins"/> on an engine in standalone mode.</summary>
[NonParallelizable]
internal sealed class TestLateralJoinsStandalone : TestLateralJoins
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
