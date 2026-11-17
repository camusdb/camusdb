/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A select list with <c>*</c> and other items (<c>SELECT *, expr</c>) over fixed-layout input rows emits
/// fixed-layout rows (<see cref="QueryRow"/>). The unit tests run the projector on the same rows two times:
/// as <see cref="QueryRow"/>s, and behind a wrapper that is a dictionary only, which takes the dictionary path.
/// Both runs must give the same keys, in the same order, with the same values. The SQL tests run the same
/// shapes end to end: hidden window columns, a chain of CTEs over a join, and <c>RETURNING *, expr</c>.
/// </summary>
[NonParallelizable]
internal sealed class TestStarProjectionRows : SharedNodeBaseTest
{
    private static List<NodeAst> Items(string selectList) =>
        new SelectQueryCreator()
            .CreateSelectQuery(SQLParserProcessor.Parse($"SELECT {selectList} FROM t"))
            .Projections.Select(item => item.Expression).ToList();

    private static QueryTicket TicketFor(string selectList, Dictionary<string, ColumnValue>? parameters = null) =>
        new(
            txnState: new KvTransaction(Kommander.Time.HLCTimestamp.Zero, "star-projection-test"),
            databaseName: "db",
            tableName: "t",
            index: null,
            projection: Items(selectList),
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: parameters);

    private static async IAsyncEnumerable<QueryResultRow> Stream(IEnumerable<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;

        await Task.CompletedTask;
    }

    /// <summary>A row that is a dictionary and not a <see cref="QueryRow"/>, so the projector takes its dictionary path.</summary>
    private sealed class DictionaryOnly(QueryRow row) : IReadOnlyDictionary<string, ColumnValue>
    {
        public ColumnValue this[string key] => row[key];
        public IEnumerable<string> Keys => row.Keys;
        public IEnumerable<ColumnValue> Values => ((IReadOnlyDictionary<string, ColumnValue>)row).Values;
        public int Count => row.Count;
        public bool ContainsKey(string key) => row.ContainsKey(key);
        public bool TryGetValue(string key, out ColumnValue value) => row.TryGetValue(key, out value);
        public IEnumerator<KeyValuePair<string, ColumnValue>> GetEnumerator() => row.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private static ColumnValue L(long value) => new(ColumnType.Integer64, value);

    private static ColumnValue S(string value) => new(ColumnType.String, value);

    /// <summary>
    /// Projects <paramref name="rows"/> as <see cref="QueryRow"/>s and as dictionary-only rows, checks that the first
    /// run emits <see cref="QueryRow"/>s, and that both runs give the same keys, key order and values.
    /// </summary>
    private static async Task<List<QueryResultRow>> AssertSameAsDictionaryPath(string selectList, List<QueryRow> rows, Dictionary<string, ColumnValue>? parameters = null)
    {
        QueryProjector projector = new();

        List<QueryResultRow> fixedRows = await projector
            .ProjectResultset(TicketFor(selectList, parameters), Stream(rows.Select(r => new QueryResultRow(r.RowId, r))))
            .ToListAsync();

        List<QueryResultRow> dictionaryRows = await projector
            .ProjectResultset(TicketFor(selectList, parameters), Stream(rows.Select(r => new QueryResultRow(r.RowId, new DictionaryOnly(r)))))
            .ToListAsync();

        Assert.That(fixedRows, Has.Count.EqualTo(dictionaryRows.Count));

        for (int i = 0; i < fixedRows.Count; i++)
        {
            Assert.That(fixedRows[i].Row, Is.InstanceOf<QueryRow>(), "row " + i);
            Assert.That(dictionaryRows[i].Row, Is.Not.InstanceOf<QueryRow>(), "row " + i);

            List<KeyValuePair<string, ColumnValue>> got = fixedRows[i].Row.ToList();
            List<KeyValuePair<string, ColumnValue>> want = dictionaryRows[i].Row.ToList();

            Assert.That(got.Select(kv => kv.Key), Is.EqualTo(want.Select(kv => kv.Key)), "keys of row " + i);

            for (int c = 0; c < want.Count; c++)
            {
                Assert.That(got[c].Value.Type, Is.EqualTo(want[c].Value.Type), $"type of {want[c].Key} in row {i}");
                Assert.That(got[c].Value.CompareTo(want[c].Value), Is.EqualTo(0), $"value of {want[c].Key} in row {i}");
            }

            foreach (KeyValuePair<string, ColumnValue> kv in want)
                Assert.That(fixedRows[i].Row.TryGetValue(kv.Key, out _), Is.True, $"lookup of {kv.Key} in row {i}");

            Assert.That(fixedRows[i].RowId, Is.EqualTo(dictionaryRows[i].RowId));
        }

        return fixedRows;
    }

    private static List<QueryRow> TableRows()
    {
        RowLayout layout = RowLayout.ForColumns(["id", "a", "b"]);
        return Enumerable.Range(1, 5)
            .Select(i => new QueryRow(default, layout, [L(i), L(i * 10), S("b" + i)]))
            .ToList();
    }

    [Test]
    public async Task AStarAndAnExpressionGiveTheInputColumnsAndTheExpression()
    {
        List<QueryResultRow> rows = await AssertSameAsDictionaryPath("*, a + id AS s", TableRows());

        Assert.That(rows[2].Row.Keys, Is.EqualTo(new[] { "id", "a", "b", "s" }));
        Assert.That(rows[2].Row["s"].LongValue, Is.EqualTo(33));
    }

    [Test]
    public async Task AnItemNamedLikeAnInputColumnKeepsItsOwnCell()
    {
        // The explicit a and the alias b do not share the cells of the expanded a and b.
        await AssertSameAsDictionaryPath("*, a, a * 2 AS b", TableRows());
    }

    [Test]
    public async Task AStarAtMoreThanOnePositionExpandsEachColumnOneTime()
    {
        List<QueryResultRow> rows = await AssertSameAsDictionaryPath("id * 100 AS x, *, a - 1 AS y, *", TableRows());

        Assert.That(rows[0].Row.Keys, Is.EqualTo(new[] { "x", "id", "a", "b", "y" }));
        Assert.That(rows[0].Row["y"].LongValue, Is.EqualTo(9));
    }

    [Test]
    public async Task TwoInputSlotsWithOneNameGiveOneKeyWithTheLastValue()
    {
        RowLayout layout = RowLayout.ForColumns(["k", "v", "K"]);
        List<QueryRow> rows = [new QueryRow(default, layout, [L(1), L(2), L(3)])];

        List<QueryResultRow> projected = await AssertSameAsDictionaryPath("*, v + 1 AS w", rows);

        Assert.That(projected[0].Row.Keys, Is.EqualTo(new[] { "k", "v", "w" }));
        Assert.That(projected[0].Row["k"].LongValue, Is.EqualTo(3));
    }

    [Test]
    public async Task AStarOverAJoinRowKeepsTheQualifiedKeysAndReadsTheBareNames()
    {
        // A join row: qualified physical names, with bare aliases for the names that only one side has.
        RowLayout layout = new(
            ["t.id", "t.a", "u.id", "u.b"],
            [new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 3)]);
        List<QueryRow> rows = Enumerable.Range(1, 4)
            .Select(i => new QueryRow(default, layout, [L(i), L(i * 10), L(i + 100), L(i * 7)]))
            .ToList();

        List<QueryResultRow> projected = await AssertSameAsDictionaryPath("*, a + b AS s, t.id + u.id AS ids", rows);

        Assert.That(projected[1].Row.Keys, Is.EqualTo(new[] { "t.id", "t.a", "u.id", "u.b", "s", "ids" }));
        Assert.That(projected[1].Row["s"].LongValue, Is.EqualTo(34));
        Assert.That(projected[1].Row["ids"].LongValue, Is.EqualTo(104));

        // The output has only the keys it writes, as the dictionary had: a bare alias of the input is not a key.
        Assert.That(projected[1].Row.ContainsKey("a"), Is.False);
    }

    [Test]
    public async Task RowsOfTwoLayoutsEachGetTheirOwnOutputLayout()
    {
        RowLayout first = RowLayout.ForColumns(["id", "a"]);
        RowLayout second = RowLayout.ForColumns(["a", "id", "extra"]);
        List<QueryRow> rows =
        [
            new QueryRow(default, first, [L(1), L(10)]),
            new QueryRow(default, second, [L(20), L(2), S("e")]),
            new QueryRow(default, first, [L(3), L(30)]),
        ];

        QueryProjector projector = new();
        List<QueryResultRow> projected = await projector
            .ProjectResultset(TicketFor("*, a + id AS s"), Stream(rows.Select(r => new QueryResultRow(r.RowId, r))))
            .ToListAsync();

        Assert.That(projected[0].Row.Keys, Is.EqualTo(new[] { "id", "a", "s" }));
        Assert.That(projected[1].Row.Keys, Is.EqualTo(new[] { "a", "id", "extra", "s" }));
        Assert.That(projected[2].Row.Keys, Is.EqualTo(new[] { "id", "a", "s" }));
        Assert.That(projected.Select(r => r.Row["s"].LongValue), Is.EqualTo(new long[] { 11, 22, 33 }));
    }

    [Test]
    public async Task AStarWithAParameterReadsTheParameter()
    {
        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = L(1000) };
        List<QueryResultRow> rows = await AssertSameAsDictionaryPath("*, a + @p AS s", TableRows(), parameters);

        Assert.That(rows[4].Row["s"].LongValue, Is.EqualTo(1050));
    }

    [Test]
    public void AnErrorOfAnItemComesFromTheSameItem()
    {
        QueryProjector projector = new();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await projector
            .ProjectResultset(TicketFor("*, a / (id - 1) AS q"), Stream(TableRows().Select(r => new QueryResultRow(r.RowId, r))))
            .ToListAsync());

        Assert.That(error!.Message, Does.Contain("zero").IgnoreCase);
    }

    // ── End to end, through SQL ──

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

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> NonQuery(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ReturningRows?.ToList() ?? [];
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seeded()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);

        await Ddl(executor, database, db, "CREATE TABLE items (id INT64 PRIMARY KEY NOT NULL, v INT64 NOT NULL, name STRING)");
        await Ddl(executor, database, db, "CREATE TABLE tags (id INT64 PRIMARY KEY NOT NULL, item_id INT64 NOT NULL, w INT64 NOT NULL)");
        await NonQuery(executor, database, db,
            "INSERT INTO items (id, v, name) VALUES " + string.Join(", ", Enumerable.Range(1, 12).Select(i => $"({i}, {i * 10}, 'n{i}')")));
        await NonQuery(executor, database, db,
            "INSERT INTO tags (id, item_id, w) VALUES (1, 2, 5), (2, 4, 6), (3, 4, 7), (4, 9, 8)");

        return (db, database, executor);
    }

    private static string Render(QueryResultRow row) =>
        string.Join(",", row.Row.Select(kv => kv.Key + "=" + (kv.Value.Type == ColumnType.Null ? "null" : kv.Value.ToString())));

    [Test]
    public async Task ChainedStarCtesOverAJoinGiveTheValuesOfTheExplicitColumns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> star = await Query(executor, database, db,
            "WITH s1 AS (SELECT *, i.v * 2 AS x FROM items i JOIN tags g ON g.item_id = i.id), " +
            "s2 AS (SELECT *, x + w AS y FROM s1) " +
            "SELECT *, y * 3 AS z FROM s2 ORDER BY z");

        List<QueryResultRow> plain = await Query(executor, database, db,
            "SELECT g.id AS gid, i.v * 2 AS x, i.v * 2 + g.w AS y, (i.v * 2 + g.w) * 3 AS z " +
            "FROM items i JOIN tags g ON g.item_id = i.id ORDER BY g.id");

        Assert.That(star, Has.Count.EqualTo(4));
        Assert.That(plain, Has.Count.EqualTo(4));

        for (int i = 0; i < plain.Count; i++)
        {
            Assert.That(star[i].Row["x"].LongValue, Is.EqualTo(plain[i].Row["x"].LongValue), Render(star[i]));
            Assert.That(star[i].Row["y"].LongValue, Is.EqualTo(plain[i].Row["y"].LongValue), Render(star[i]));
            Assert.That(star[i].Row["z"].LongValue, Is.EqualTo(plain[i].Row["z"].LongValue), Render(star[i]));
        }

        Assert.That(star.Select(r => r.Row["z"].LongValue), Is.EqualTo(new long[] { 135, 258, 261, 564 }));
        Assert.That(star.Select(r => r.Row), Is.All.InstanceOf<QueryRow>());
    }

    [Test]
    public async Task AStarOverAWindowedSelectLeavesOutTheHiddenColumns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT *, ROW_NUMBER() OVER (ORDER BY v DESC) AS rn, v + 1 AS v1 FROM items WHERE id <= 3 ORDER BY id");

        Assert.That(rows, Has.Count.EqualTo(3));

        foreach (QueryResultRow row in rows)
            Assert.That(row.Row.Keys.Where(WindowQueryRewriter.IsHiddenColumn), Is.Empty, Render(row));

        Assert.That(rows[0].Row.Keys, Is.EqualTo(new[] { "id", "v", "name", "rn", "v1" }), Render(rows[0]));
        Assert.That(rows.Select(r => r.Row["rn"].LongValue), Is.EqualTo(new long[] { 3, 2, 1 }));
        Assert.That(rows.Select(r => r.Row["v1"].LongValue), Is.EqualTo(new long[] { 11, 21, 31 }));
        Assert.That(rows.Select(r => r.Row), Is.All.InstanceOf<QueryRow>());
    }

    [Test]
    public async Task AStarAndAnItemNamedLikeAColumnGiveBothValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT *, v + 1 AS v, id * 2 AS twice FROM items WHERE id IN (1, 2) ORDER BY id");

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[1].Row["v"].LongValue, Is.EqualTo(20), Render(rows[1]));
        Assert.That(rows[1].Row.Values.Select(v => v.Type == ColumnType.Integer64 ? v.LongValue : -1), Does.Contain(21L), Render(rows[1]));
        Assert.That(rows[1].Row["twice"].LongValue, Is.EqualTo(4));
        Assert.That(rows.Select(r => r.Row), Is.All.InstanceOf<QueryRow>());
    }

    [Test]
    public async Task ReturningAStarAndAnExpressionGivesTheWrittenRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seeded();

        List<QueryResultRow> rows = await NonQuery(executor, database, db,
            "UPDATE items SET v = v + 1 WHERE id <= 3 RETURNING *, v * 10 AS v10");

        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows.OrderBy(r => r.Row["id"].LongValue).Select(r => r.Row["v10"].LongValue), Is.EqualTo(new long[] { 110, 210, 310 }));
        Assert.That(rows[0].Row.Keys, Is.EqualTo(new[] { "id", "v", "name", "v10" }), Render(rows[0]));
    }
}
