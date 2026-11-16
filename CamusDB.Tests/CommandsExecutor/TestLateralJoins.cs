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
