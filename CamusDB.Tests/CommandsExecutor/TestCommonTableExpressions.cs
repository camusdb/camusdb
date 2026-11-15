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
/// <c>WITH</c> end to end, through <see cref="ExecuteSQLTicket"/>: each position a WITH can take, the
/// name rules (a CTE hides a table, sees the CTEs before it, and does not see itself), column lists,
/// the write statements and a view whose body has a WITH.
/// </summary>
[NonParallelizable]
internal sealed class TestCommonTableExpressions : BaseTest
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

    private static async Task<ExecuteNonSQLResult> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
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

    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, grp INT64, v INT64)");
        await Ddl(executor, database, db, "CREATE TABLE tags (tid INT64 PRIMARY KEY, item_id INT64, tag STRING)");
        await Dml(executor, database, db, "INSERT INTO items (id, grp, v) SELECT g, g % 3, g * 10 FROM generate_series(1, 12) g");
        await Dml(executor, database, db, "INSERT INTO tags (tid, item_id, tag) VALUES (1, 2, 'red'), (2, 4, 'red'), (3, 4, 'blue'), (4, 9, 'green')");
        return (db, database, executor);
    }

    [Test]
    public async Task ACteReadsLikeADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH big AS (SELECT id, v FROM items WHERE v > 80) SELECT id FROM big WHERE id < 11 ORDER BY id");

        CollectionAssert.AreEqual(new long[] { 9, 10 }, Longs(rows, "id"));
    }

    [Test]
    public async Task ChainedCtesAndAJoinOfTwoCtes()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH totals AS (SELECT grp, SUM(v) AS total FROM items GROUP BY grp), " +
            "     ranked AS (SELECT grp, total FROM totals WHERE total > 200), " +
            "     counts AS (SELECT grp, COUNT(*) AS n FROM items GROUP BY grp) " +
            "SELECT r.grp, r.total, c.n FROM ranked r JOIN counts c ON c.grp = r.grp ORDER BY r.grp");

        // grp 0: 30+60+90+120 = 300; grp 1: 10+40+70+100 = 220; grp 2: 20+50+80+110 = 260.
        CollectionAssert.AreEqual(new long[] { 0, 1, 2 }, Longs(rows, "grp"));
        CollectionAssert.AreEqual(new long[] { 300, 220, 260 }, Longs(rows, "total"));
        CollectionAssert.AreEqual(new long[] { 4, 4, 4 }, Longs(rows, "n"));
    }

    [Test]
    public async Task ACteReadTwiceGivesTheSameRowsToBothReaders()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH odd AS (SELECT id, v FROM items WHERE id % 2 = 1) " +
            "SELECT a.id AS a_id, b.id AS b_id FROM odd a JOIN odd b ON b.id = a.id + 2 " +
            "WHERE a.id IN (SELECT id FROM odd WHERE v < 60) ORDER BY a.id");

        CollectionAssert.AreEqual(new long[] { 1, 3, 5 }, Longs(rows, "a_id"));
        CollectionAssert.AreEqual(new long[] { 3, 5, 7 }, Longs(rows, "b_id"));
    }

    [Test]
    public async Task ACteHidesATableAndDoesNotSeeItself()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // In its own query, items is the table; after it, items is the CTE.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH items AS (SELECT id FROM items WHERE id <= 2) SELECT COUNT(*) AS n FROM items");

        Assert.AreEqual(2L, rows[0].Row["n"].LongValue);
    }

    [Test]
    public async Task ColumnListsRenameTheFirstColumns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> renamed = await Query(executor, database, db,
            "WITH c (k, value) AS (SELECT id, v FROM items WHERE id <= 2) SELECT k, value FROM c ORDER BY k");
        CollectionAssert.AreEqual(new long[] { 1, 2 }, Longs(renamed, "k"));
        CollectionAssert.AreEqual(new long[] { 10, 20 }, Longs(renamed, "value"));

        // A shorter list keeps the names of the later columns.
        List<QueryResultRow> shorter = await Query(executor, database, db,
            "WITH c (k) AS (SELECT id, v FROM items WHERE id = 3) SELECT c.k, c.v FROM c");
        Assert.AreEqual(3L, shorter[0].Row["k"].LongValue);
        Assert.AreEqual(30L, shorter[0].Row["v"].LongValue);

        // A * over a join, and a set operation, take a list too.
        List<QueryResultRow> star = await Query(executor, database, db,
            "WITH c (a, b, x, y, z, w) AS (SELECT * FROM items i JOIN tags t ON t.item_id = i.id) " +
            "SELECT a, w FROM c ORDER BY a, w");
        CollectionAssert.AreEqual(new long[] { 2, 4, 4, 9 }, Longs(star, "a"));
        CollectionAssert.AreEqual(new[] { "red", "blue", "red", "green" }, star.Select(r => r.Row["w"].StrValue).ToArray());

        List<QueryResultRow> union = await Query(executor, database, db,
            "WITH c (n) AS (SELECT id FROM items WHERE id = 1 UNION ALL SELECT tid FROM tags WHERE tid = 4) SELECT n FROM c ORDER BY n");
        CollectionAssert.AreEqual(new long[] { 1, 4 }, Longs(union, "n"));

        // A derived table takes the same list.
        List<QueryResultRow> derived = await Query(executor, database, db,
            "SELECT a + b AS s FROM (SELECT 1, 2) AS d (a, b)");
        Assert.AreEqual(3L, derived[0].Row["s"].LongValue);
    }

    [Test]
    public async Task AColumnListThatDoesNotFitIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException tooMany = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH c (a, b, x) AS (SELECT id, v FROM items) SELECT a FROM c"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, tooMany.Code);
        StringAssert.Contains("2 columns available but 3 columns specified", tooMany.Message);

        // A list may name a column two times, as in PostgreSQL; only a reference to the name is ambiguous.
        Assert.AreEqual(12, (await Query(executor, database, db, "WITH c (a, a) AS (SELECT id, v FROM items) SELECT 1 AS one FROM c")).Count);
        CamusDBException twice = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH c (a, a) AS (SELECT id, v FROM items) SELECT a FROM c"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, twice.Code);

        // A new name that a later column already has makes the name ambiguous.
        CamusDBException ambiguous = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH c (v) AS (SELECT id, v FROM items) SELECT v FROM c"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ambiguous.Code);
    }

    [Test]
    public async Task AWithInsideASubqueryOrADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> inSubquery = await Query(executor, database, db,
            "SELECT id FROM items WHERE id IN (WITH t AS (SELECT item_id FROM tags WHERE tag = 'red') SELECT item_id FROM t) ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4 }, Longs(inSubquery, "id"));

        List<QueryResultRow> scalar = await Query(executor, database, db,
            "SELECT (WITH m AS (SELECT MAX(v) AS top FROM items) SELECT top FROM m) AS top");
        Assert.AreEqual(120L, scalar[0].Row["top"].LongValue);

        List<QueryResultRow> derived = await Query(executor, database, db,
            "SELECT COUNT(*) AS n FROM (WITH r AS (SELECT item_id FROM tags) SELECT DISTINCT item_id FROM r) d");
        Assert.AreEqual(3L, derived[0].Row["n"].LongValue);

        // A correlated subquery reads a CTE of the statement.
        List<QueryResultRow> correlated = await Query(executor, database, db,
            "WITH t AS (SELECT item_id, tag FROM tags) " +
            "SELECT i.id FROM items i WHERE EXISTS (SELECT 1 FROM t WHERE t.item_id = i.id AND t.tag = 'red') ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 2, 4 }, Longs(correlated, "id"));
    }

    [Test]
    public async Task ACteInsideParenthesesHidesAnOuterCte()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH a AS (SELECT 1 AS n) SELECT n, (WITH a AS (SELECT 2 AS n) SELECT n FROM a) AS inner_n FROM a");

        Assert.AreEqual(1L, rows[0].Row["n"].LongValue);
        Assert.AreEqual(2L, rows[0].Row["inner_n"].LongValue);
    }

    [Test]
    public async Task PlaceholdersInsideACte()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@low"] = new ColumnValue(ColumnType.Integer64, 50L),
            ["@high"] = new ColumnValue(ColumnType.Integer64, 6L),
        };

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH c AS (SELECT id FROM items WHERE v > @low) SELECT a.id FROM c a JOIN c b ON b.id = a.id WHERE a.id < @high + 2 ORDER BY a.id",
            parameters);

        CollectionAssert.AreEqual(new long[] { 6, 7 }, Longs(rows, "id"));
    }

    [Test]
    public async Task WritesTakeAWith()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE copy (id INT64 PRIMARY KEY, v INT64)");

        ExecuteNonSQLResult inserted = await Dml(executor, database, db,
            "WITH src AS (SELECT id, v FROM items WHERE grp = 0) INSERT INTO copy (id, v) SELECT id, v FROM src");
        Assert.AreEqual(4, inserted.ModifiedRows);

        ExecuteNonSQLResult insertedInside = await Dml(executor, database, db,
            "INSERT INTO copy (id, v) WITH src AS (SELECT id, v FROM items WHERE grp = 1) SELECT id, v FROM src");
        Assert.AreEqual(4, insertedInside.ModifiedRows);

        // The target of a write is the table even when a CTE has its name.
        ExecuteNonSQLResult updated = await Dml(executor, database, db,
            "WITH copy AS (SELECT id FROM items WHERE grp = 0) UPDATE copy SET v = 0 WHERE id IN (SELECT id FROM copy)");
        Assert.AreEqual(4, updated.ModifiedRows);

        ExecuteNonSQLResult updatedFrom = await Dml(executor, database, db,
            "WITH d AS (SELECT id, v * 2 AS doubled FROM items) UPDATE copy SET v = d.doubled FROM d WHERE d.id = copy.id AND copy.v > 0");
        Assert.AreEqual(4, updatedFrom.ModifiedRows);

        ExecuteNonSQLResult deletedUsing = await Dml(executor, database, db,
            "WITH gone AS (SELECT id FROM items WHERE id > 9) DELETE FROM copy USING gone WHERE gone.id = copy.id");
        Assert.AreEqual(2, deletedUsing.ModifiedRows);

        ExecuteNonSQLResult deleted = await Dml(executor, database, db,
            "WITH small AS (SELECT id FROM items WHERE id < 4) DELETE FROM copy WHERE id IN (SELECT id FROM small)");
        Assert.AreEqual(2, deleted.ModifiedRows);

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, v FROM copy ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 4, 6, 7, 9 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 80, 0, 140, 0 }, Longs(rows, "v"));
    }

    [Test]
    public async Task AViewAndATableFromAQueryWithAWith()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE VIEW tagged (item, label) AS WITH t (item, label) AS (SELECT item_id, tag FROM tags) " +
            "SELECT item, label FROM t WHERE label <> 'green'");

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT item, label FROM tagged ORDER BY item, label");
        CollectionAssert.AreEqual(new long[] { 2, 4, 4 }, Longs(rows, "item"));

        // The stored body has no WITH: the CTE is written out as a derived table, and the text parses again.
        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW tagged"))[0].Row["create view"].StrValue!;
        StringAssert.DoesNotContain("WITH t", shown);
        StringAssert.Contains("AS t(item, label)", shown);
        await Ddl(executor, database, db, shown.Replace("CREATE VIEW", "CREATE OR REPLACE VIEW"));
        Assert.AreEqual(3, (await Query(executor, database, db, "SELECT item FROM tagged")).Count);

        await Ddl(executor, database, db,
            "CREATE TABLE summary AS WITH s AS (SELECT grp, COUNT(*) AS n FROM items GROUP BY grp) SELECT grp, n FROM s");
        List<QueryResultRow> summary = await Query(executor, database, db, "SELECT grp, n FROM summary ORDER BY grp");
        CollectionAssert.AreEqual(new long[] { 4, 4, 4 }, Longs(summary, "n"));
    }

    [Test]
    public async Task ExplainShowsTheCteAsADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN WITH c AS (SELECT id FROM items WHERE v > 50) SELECT id FROM c");

        Assert.IsNotEmpty(plan);
    }

    [Test]
    public async Task AnUnknownTableInACteIsReported()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH c AS (SELECT id FROM nowhere) SELECT id FROM c"))!;
        StringAssert.Contains("nowhere", missing.Message);

        // A later CTE is not visible to an earlier one, so the name reads a table that does not exist.
        CamusDBException forward = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH a AS (SELECT id FROM b), b AS (SELECT id FROM items) SELECT id FROM a"))!;
        StringAssert.Contains("b", forward.Message);
    }

    [Test]
    public async Task ACteReadTwiceRunsOnce()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // random() is volatile, so two runs of the query would give two values.
        List<QueryResultRow> join = await Query(executor, database, db,
            "WITH r AS (SELECT random() AS x) SELECT a.x AS ax, b.x AS bx FROM r a CROSS JOIN r b");
        Assert.AreEqual(join[0].Row["ax"].FloatValue, join[0].Row["bx"].FloatValue);

        List<QueryResultRow> levels = await Query(executor, database, db,
            "WITH r AS (SELECT random() AS x) SELECT x, (SELECT x FROM r) AS again FROM r");
        Assert.AreEqual(levels[0].Row["x"].FloatValue, levels[0].Row["again"].FloatValue);

        List<QueryResultRow> once = await Query(executor, database, db,
            "WITH r AS MATERIALIZED (SELECT random() AS x) SELECT x FROM r");
        Assert.AreEqual(1, once.Count);
    }

    [Test]
    public async Task AnInlinedCteRunsForEachReader()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> inlined = await Query(executor, database, db,
            "WITH r AS NOT MATERIALIZED (SELECT random() AS x) SELECT a.x AS ax, b.x AS bx FROM r a CROSS JOIN r b");
        Assert.AreNotEqual(inlined[0].Row["ax"].FloatValue, inlined[0].Row["bx"].FloatValue);

        // A WITH in parentheses can read the columns of the query around it, so it is always inlined.
        List<QueryResultRow> nested = await Query(executor, database, db,
            "SELECT * FROM (WITH r AS (SELECT random() AS x) SELECT a.x AS ax, b.x AS bx FROM r a CROSS JOIN r b) d");
        Assert.AreNotEqual(nested[0].Row["ax"].FloatValue, nested[0].Row["bx"].FloatValue);
    }

    [Test]
    public async Task EachReaderFiltersTheKeptRowsItself()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH c AS (SELECT id, v FROM items) " +
            "SELECT (SELECT COUNT(*) FROM c WHERE id > 6) AS big, (SELECT COUNT(*) FROM c) AS total, " +
            "(SELECT MAX(x.v) FROM c x JOIN c y ON y.id = x.id - 1 WHERE y.v < 30) AS top");

        Assert.AreEqual(6L, rows[0].Row["big"].LongValue);
        Assert.AreEqual(12L, rows[0].Row["total"].LongValue);
        Assert.AreEqual(30L, rows[0].Row["top"].LongValue);
    }

    [Test]
    public async Task EachExecutionComputesItsOwnRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        const string sql = "WITH c AS (SELECT COUNT(*) AS n FROM items) SELECT a.n FROM c a, c b";

        // One transaction runs the same text two times: the parsed tree, and so the marker, is the same,
        // but the second run must count the row the transaction inserted in between.
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> first) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            Assert.AreEqual(12L, (await first.ToListAsync())[0].Row["n"].LongValue);

            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO items (id, grp, v) VALUES (100, 0, 0)", null));

            (_, IAsyncEnumerable<QueryResultRow> second) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            Assert.AreEqual(13L, (await second.ToListAsync())[0].Row["n"].LongValue);

            // A reader that stops early ends the statement too.
            (_, IAsyncEnumerable<QueryResultRow> early) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db,
                "WITH c AS (SELECT id FROM items) SELECT a.id FROM c a JOIN c b ON b.id = a.id", null));
            await foreach (QueryResultRow _ in early)
                break;

            (_, IAsyncEnumerable<QueryResultRow> third) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            Assert.AreEqual(13L, (await third.ToListAsync())[0].Row["n"].LongValue);

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task AWriteReadsTheKeptRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();
        await Ddl(executor, database, db, "CREATE TABLE draws (id INT64 PRIMARY KEY, x FLOAT64)");

        ExecuteNonSQLResult inserted = await Dml(executor, database, db,
            "WITH r AS (SELECT random() AS x) INSERT INTO draws (id, x) SELECT 1, x FROM r UNION ALL SELECT 2, x FROM r");
        Assert.AreEqual(2, inserted.ModifiedRows);

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT MIN(x) AS lo, MAX(x) AS hi FROM draws");
        Assert.AreEqual(rows[0].Row["lo"].FloatValue, rows[0].Row["hi"].FloatValue);

        await Ddl(executor, database, db,
            "CREATE TABLE pairs AS WITH r AS (SELECT random() AS x) SELECT a.x AS ax, b.x AS bx FROM r a, r b");
        List<QueryResultRow> pairs = await Query(executor, database, db, "SELECT ax, bx FROM pairs");
        Assert.AreEqual(pairs[0].Row["ax"].FloatValue, pairs[0].Row["bx"].FloatValue);
    }

    [Test]
    public async Task ExplainMarksTheReadersOfAComputedOnceCte()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db,
            "EXPLAIN WITH c AS (SELECT id FROM items) SELECT a.id FROM c a JOIN c b ON b.id = a.id");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("computed_once=c", text);
    }

    [Test]
    public async Task AKeptCteSpills()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await Ddl(executor, database, db, "CREATE TABLE n (id INT64 PRIMARY KEY)");
        await Dml(executor, database, db, "INSERT INTO n (id) SELECT g FROM generate_series(1, 50) g");

        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH c AS (SELECT id FROM n) SELECT COUNT(*) AS pairs FROM c a JOIN c b ON b.id = a.id + 1");

        Assert.AreEqual(49L, rows[0].Row["pairs"].LongValue);
    }

    [Test]
    public async Task ALockingReadOfACteIsRefusedAsForADerivedTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "WITH c AS (SELECT id FROM items) SELECT id FROM c FOR UPDATE"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refused.Code);

        // A CTE that the locking read does not read does not change it.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "WITH c AS (SELECT id FROM tags) SELECT id FROM items WHERE id = 1 FOR UPDATE");
        Assert.AreEqual(1, rows.Count);
    }

    [Test]
    public async Task AQualifiedNameReadsItsOwnDerivedTableInAJoin()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t1 (c0 INT64 PRIMARY KEY, c3 BOOL)");
        await Ddl(executor, database, db, "CREATE TABLE t2 (c0 STRING PRIMARY KEY, c3 NUMERIC NOT NULL)");

        string[] statements =
        [
            "SELECT a1.c3 AS o1 FROM (SELECT * FROM t2) AS a0 INNER JOIN (SELECT * FROM t1) AS a1 ON TRUE",
            "SELECT a1.c3 AS o1 FROM (SELECT c0, c3 FROM t2) AS a0 INNER JOIN (SELECT c0, c3 FROM t1) AS a1 ON TRUE",
            "SELECT a1.c3 AS o1 FROM t2 AS a0 INNER JOIN (SELECT * FROM t1) AS a1 ON TRUE",
            "SELECT a1.c3 AS o1 FROM (SELECT * FROM t2) AS a0 INNER JOIN t1 AS a1 ON TRUE",
            "SELECT a1.c3 AS o1 FROM t2 AS a0 INNER JOIN t1 AS a1 ON TRUE",
            "WITH cte_t1 AS (SELECT * FROM t1), cte_t2 AS (SELECT * FROM t2) SELECT a1.c3 AS o1 FROM cte_t2 AS a0 INNER JOIN cte_t1 AS a1 ON TRUE",
        ];

        // The tables are empty, so the type of the column comes from the schema the statement reports.
        List<string> wrong = [];
        foreach (string sql in statements)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            QuerySchemaHolder schema = new();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null), schemaOut: schema);
            await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);

            if (schema.Schema[0].Type != ColumnType.Bool)
                wrong.Add($"{sql} -> {schema.Schema[0].Type}");
        }

        Assert.IsEmpty(wrong, string.Join("\n", wrong));
    }
}
