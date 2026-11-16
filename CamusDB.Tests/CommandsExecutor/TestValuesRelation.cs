/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
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
/// A <c>VALUES</c> list as a relation in <c>FROM</c>, end to end through <see cref="ExecuteSQLTicket"/>:
/// <c>(VALUES (…), (…)) AS v (a, b)</c> in every relation slot and after <c>LATERAL</c>, with the rules of
/// PostgreSQL. This fixture runs in cluster mode on a shared node; <see cref="TestValuesRelationStandalone"/>
/// runs the same tests in standalone mode.
/// </summary>
[NonParallelizable]
internal class TestValuesRelation : SharedNodeBaseTest
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

    /// <summary>Five items (<c>v = id * 10</c>) and three tags on items 2 and 4.</summary>
    private async Task<(string Db, DatabaseDescriptor Database, CommandExecutor Executor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64 NULL)");
        await Ddl(executor, database, db, "CREATE TABLE tags (tid INT64 PRIMARY KEY, item_id INT64, tag STRING)");
        await Dml(executor, database, db, "INSERT INTO items (id, v) SELECT g, g * 10 FROM generate_series(1, 5) g");
        await Dml(executor, database, db, "INSERT INTO tags (tid, item_id, tag) VALUES (1, 2, 'red'), (2, 4, 'red'), (3, 4, 'blue')");
        return (db, database, executor);
    }

    [Test]
    public async Task AValuesListIsARelationWithTheNamesOfItsColumnList()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT * FROM (VALUES (2, 'b'), (1, 'a'), (3, 'c')) AS v (id, k)");

        // The rows come in the written order.
        CollectionAssert.AreEqual(new long[] { 2, 1, 3 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new[] { "b", "a", "c" }, Strings(rows, "k"));
        CollectionAssert.AreEqual(new[] { "id", "k" }, rows[0].Row.Keys.ToArray());

        // The same statement with other literals gives its own rows: no cache keeps the first literals.
        List<QueryResultRow> other = await Query(executor, database, db,
            "SELECT * FROM (VALUES (7, 'x'), (8, 'y'), (9, 'z')) AS v (id, k)");
        CollectionAssert.AreEqual(new long[] { 7, 8, 9 }, Longs(other, "id"));
        CollectionAssert.AreEqual(new[] { "x", "y", "z" }, Strings(other, "k"));
    }

    [Test]
    public async Task WithoutAColumnListTheColumnsAreColumn1AndSoOn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT * FROM (VALUES (1, 'a')) v");
        CollectionAssert.AreEqual(new[] { "column1", "column2" }, rows[0].Row.Keys.ToArray());

        List<QueryResultRow> qualified = await Query(executor, database, db,
            "SELECT v.column2 FROM (VALUES (1, 'a'), (2, 'b')) AS v WHERE v.column1 = 2");
        CollectionAssert.AreEqual(new[] { "b" }, Strings(qualified, "column2"));

        // A short column list renames the first columns only.
        List<QueryResultRow> partial = await Query(executor, database, db, "SELECT * FROM (VALUES (1, 'a')) AS v (id)");
        CollectionAssert.AreEqual(new[] { "id", "column2" }, partial[0].Row.Keys.ToArray());

        CamusDBException? tooLong = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT * FROM (VALUES (1)) AS v (a, b)"));
        StringAssert.Contains("2 columns specified", tooLong!.Message);
    }

    [Test]
    public async Task AValuesListJoinsATable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> inner = await Query(executor, database, db,
            "SELECT i.id, x.name FROM items i JOIN (VALUES (2, 'two'), (4, 'four'), (9, 'nine')) AS x (id, name) ON x.id = i.id ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 2, 4 }, Longs(inner, "id"));
        CollectionAssert.AreEqual(new[] { "two", "four" }, Strings(inner, "name"));

        List<QueryResultRow> comma = await Query(executor, database, db,
            "SELECT i.v FROM (VALUES (1), (3)) AS x (id), items i WHERE i.id = x.id ORDER BY i.v");
        CollectionAssert.AreEqual(new long[] { 10, 30 }, Longs(comma, "v"));

        List<QueryResultRow> left = await Query(executor, database, db,
            "SELECT x.id, t.tag FROM (VALUES (3), (4)) AS x (id) LEFT JOIN tags t ON t.item_id = x.id ORDER BY x.id, t.tag");
        CollectionAssert.AreEqual(new long[] { 3, 4, 4 }, Longs(left, "id"));
        CollectionAssert.AreEqual(new string?[] { null, "blue", "red" }, Strings(left, "tag"));

        List<QueryResultRow> grouped = await Query(executor, database, db,
            "SELECT k, SUM(n) AS total FROM (VALUES ('a', 1), ('b', 3), ('a', 2)) AS v (k, n) GROUP BY k ORDER BY k");
        CollectionAssert.AreEqual(new[] { "a", "b" }, Strings(grouped, "k"));
        CollectionAssert.AreEqual(new long[] { 3, 3 }, Longs(grouped, "total"));
    }

    [Test]
    public async Task TheColumnTypesFollowTheUnionRule()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT a FROM (VALUES (1), (2.5), (NULL)) AS v (a)");
        Assert.That(rows[0].Row["a"].Type, Is.EqualTo(ColumnType.Float64));
        Assert.That(rows[0].Row["a"].FloatValue, Is.EqualTo(1.0));
        Assert.That(rows[1].Row["a"].FloatValue, Is.EqualTo(2.5));
        Assert.That(rows[2].Row["a"].Type, Is.EqualTo(ColumnType.Null));

        CamusDBException? mismatch = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT a FROM (VALUES (1), ('x')) AS v (a)"));
        Assert.That(mismatch!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        StringAssert.Contains("cannot be combined", mismatch.Message);
    }

    [Test]
    public async Task ItemsAreExpressionsParametersAndSubqueries()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT * FROM (VALUES (1 + 2, upper('a')), (@p, NULL), ((SELECT COUNT(*) FROM items), 'n')) AS v (n, s)",
            new Dictionary<string, ColumnValue> { ["@p"] = new ColumnValue(ColumnType.Integer64, 7L) });

        CollectionAssert.AreEqual(new long[] { 3, 7, 5 }, Longs(rows, "n"));
        CollectionAssert.AreEqual(new string?[] { "A", null, "n" }, Strings(rows, "s"));
    }

    [Test]
    public async Task TheRulesOfAValuesListAreChecked()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        (string Sql, string Message)[] refused =
        [
            ("SELECT * FROM (VALUES (1, 2), (3)) AS v (a, b)", "VALUES lists must all be the same length"),
            ("SELECT * FROM (VALUES (COUNT(*))) AS v (a)", "Aggregate functions are not allowed in VALUES"),
            ("SELECT * FROM (VALUES (row_number() OVER ())) AS v (a)", "Window functions are not allowed in VALUES"),
            ("SELECT * FROM (VALUES (DEFAULT)) AS v (a)", "DEFAULT is not allowed"),
        ];

        foreach ((string sql, string message) in refused)
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db, sql), sql);
            Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), sql);
            StringAssert.Contains(message, error.Message, sql);
        }

        // The alias is required, as in PostgreSQL 16.
        CamusDBException? noAlias = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT * FROM (VALUES (1))"));
        Assert.That(noAlias!.Code, Is.EqualTo(CamusDBErrorCodes.SqlSyntaxError));
    }

    [Test]
    public async Task ALateralValuesListSplitsEachRowIntoSeveralRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i.id, s.k, s.n FROM items i CROSS JOIN LATERAL (VALUES ('id', i.id), ('v', i.v)) AS s (k, n) " +
            "WHERE i.id <= 2 ORDER BY i.id, s.k");

        CollectionAssert.AreEqual(new long[] { 1, 1, 2, 2 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new[] { "id", "v", "id", "v" }, Strings(rows, "k"));
        CollectionAssert.AreEqual(new long[] { 1, 10, 2, 20 }, Longs(rows, "n"));

        List<QueryResultRow> comma = await Query(executor, database, db,
            "SELECT SUM(s.n) AS total FROM items i, LATERAL (VALUES (i.v), (i.v + 1)) AS s (n)");
        Assert.AreEqual(2 * 150 + 5, comma[0].Row["total"].LongValue);
    }

    [Test]
    public async Task AValuesListWithoutLateralCannotReadTheItemsToItsLeft()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db,
            "SELECT i.id, s.n FROM items i, (VALUES (i.v)) AS s (n)"));

        StringAssert.Contains("'i'", error!.Message);
    }

    [Test]
    public async Task AValuesListInACteAndInASubquery()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> cte = await Query(executor, database, db,
            "WITH c AS (SELECT * FROM (VALUES (1), (2), (3)) AS v (n)) SELECT SUM(n) AS total FROM c");
        Assert.AreEqual(6, cte[0].Row["total"].LongValue);

        List<QueryResultRow> inList = await Query(executor, database, db,
            "SELECT id FROM items WHERE id IN (SELECT n FROM (VALUES (2), (4)) AS v (n)) ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 2, 4 }, Longs(inList, "id"));

        List<QueryResultRow> correlated = await Query(executor, database, db,
            "SELECT i.id, (SELECT MAX(x) FROM (VALUES (1), (i.id)) AS w (x)) AS m FROM items i WHERE i.id <= 2 ORDER BY i.id");
        CollectionAssert.AreEqual(new long[] { 1, 2 }, Longs(correlated, "m"));
    }

    [Test]
    public async Task TheWriteStatementsReadAValuesList()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        ExecuteNonSQLResult updated = await Dml(executor, database, db,
            "UPDATE items SET v = x.v FROM (VALUES (1, 5), (2, 6)) AS x (id, v) WHERE x.id = items.id");
        Assert.AreEqual(2, updated.ModifiedRows);

        ExecuteNonSQLResult deleted = await Dml(executor, database, db,
            "DELETE FROM items USING (VALUES (5)) AS x (id) WHERE x.id = items.id");
        Assert.AreEqual(1, deleted.ModifiedRows);

        CollectionAssert.AreEqual(new long[] { 5, 6, 30, 40 },
            Longs(await Query(executor, database, db, "SELECT v FROM items ORDER BY id"), "v"));

        await Dml(executor, database, db, "INSERT INTO items (id, v) SELECT * FROM (VALUES (10, 100), (11, 110)) AS x (id, v)");
        CollectionAssert.AreEqual(new long[] { 100, 110 },
            Longs(await Query(executor, database, db, "SELECT v FROM items WHERE id >= 10 ORDER BY id"), "v"));

        await Ddl(executor, database, db, "CREATE TABLE copied AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS x (id, k)");
        CollectionAssert.AreEqual(new[] { "a", "b" },
            Strings(await Query(executor, database, db, "SELECT k FROM copied ORDER BY id"), "k"));
    }

    [Test]
    public async Task ExplainNamesTheValuesList()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN SELECT * FROM (VALUES (1), (2), (3)) AS v (a)");

        string text = string.Join("\n", plan.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue))));
        StringAssert.Contains("values", text);
        StringAssert.Contains("alias=v, rows=3", text);
    }

    [Test]
    public async Task AViewWithAValuesListKeepsItsTextAndItsTextParsesAgain()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        await Ddl(executor, database, db,
            "CREATE VIEW named AS SELECT i.id, x.name, s.n FROM items i JOIN (VALUES (2, 'two'), (4, 'four')) AS x (id, name) ON x.id = i.id " +
            "CROSS JOIN LATERAL (VALUES (i.v), (i.v + 1)) AS s (n)");

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT id, name, n FROM named ORDER BY id, n");
        CollectionAssert.AreEqual(new long[] { 2, 2, 4, 4 }, Longs(rows, "id"));
        CollectionAssert.AreEqual(new long[] { 20, 21, 40, 41 }, Longs(rows, "n"));

        string shown = (await Query(executor, database, db, "SHOW CREATE VIEW named"))[0].Row["create view"].StrValue!;
        StringAssert.Contains("(VALUES (2, 'two'), (4, 'four')) AS x(id, name)", shown);
        StringAssert.Contains("LATERAL (VALUES (i.v), (i.v + 1)) AS s(n)", shown);

        await Ddl(executor, database, db, shown.Replace("CREATE VIEW", "CREATE OR REPLACE VIEW"));
        string again = (await Query(executor, database, db, "SHOW CREATE VIEW named"))[0].Row["create view"].StrValue!;
        Assert.That(again, Is.EqualTo(shown));
    }

    [Test]
    public async Task ALongValuesListRuns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        // A chain of 10,000 rows nests far deeper than the set-operation limit unless it is rebalanced.
        StringBuilder sql = new("SELECT COUNT(*) AS n, SUM(a) AS total FROM (VALUES ");
        for (int i = 1; i <= 10000; i++)
        {
            if (i > 1)
                sql.Append(", ");
            sql.Append('(').Append(i).Append(')');
        }
        sql.Append(") AS v (a)");

        List<QueryResultRow> rows = await Query(executor, database, db, sql.ToString());
        Assert.AreEqual(10000, rows[0].Row["n"].LongValue);
        Assert.AreEqual(10000L * 10001 / 2, rows[0].Row["total"].LongValue);
    }
}

/// <summary>The <see cref="TestValuesRelation"/> tests in standalone mode.</summary>
[NonParallelizable]
internal sealed class TestValuesRelationStandalone : TestValuesRelation
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
