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
/// The jsonb operators inside real queries: the lexer rules that keep old SQL meaning what it meant
/// (<c>x&lt;@p</c>, <c>x # -1</c>), precedence, filters, sorts, groups, joins under each strategy,
/// UPDATE through <c>||</c> and <c>-</c>, comparison with a text literal, views (whose bodies render
/// the operators as calls) and the static result types CTAS gives.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestJsonbQueries : BaseTest
{
    public enum Mode { NestedLoop, Hash, Merge }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            return await cursor.ToListAsync();
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupEvents()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE events (id int64 PRIMARY KEY, payload jsonb)");
        await Exec(executor, db,
            "INSERT INTO events (id, payload) VALUES " +
            "(1, '{\"kind\": \"login\", \"user\": {\"id\": 7, \"tags\": [\"vip\"]}, \"n\": 3}'), " +
            "(2, '{\"kind\": \"logout\", \"user\": {\"id\": 7}, \"n\": 1.0}'), " +
            "(3, '{\"kind\": \"login\", \"user\": {\"id\": 9, \"tags\": [\"beta\", \"vip\"]}, \"n\": 2, \"tmp\": true}'), " +
            "(4, '[\"not\", \"an\", \"object\"]'), " +
            "(5, NULL)");
        return (db, executor);
    }

    private static long[] Ids(List<QueryResultRow> rows) => rows.Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToArray();

    [Test]
    public async Task Lexer_KeepsTheOldMeaningOfLessThanPlaceholder_AndOfXorWithASpace()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        Dictionary<string, ColumnValue> p = new() { ["@p"] = new(ColumnType.Integer64, 6) };
        Assert.IsTrue((await Select(executor, db, "SELECT 5<@p AS r", p)).Single().Row["r"].BoolValue, "x<@p is x < @p");
        Assert.IsTrue((await Select(executor, db, "SELECT 5 <@p AS r", p)).Single().Row["r"].BoolValue, "x <@p is x < @p");

        Assert.IsTrue((await Select(executor, db, "SELECT '{\"a\":1}'::jsonb <@'{\"a\":1,\"b\":2}' AS r")).Single().Row["r"].BoolValue);
        Assert.IsTrue((await Select(executor, db, "SELECT '{\"a\":1}'::jsonb <@ '{\"a\":1,\"b\":2}' AS r")).Single().Row["r"].BoolValue);

        Assert.AreEqual(-7L, (await Select(executor, db, "SELECT 6 # -1 AS r")).Single().Row["r"].LongValue, "XOR with a space");
        Assert.AreEqual("{\"a\": {}}", (await Select(executor, db, "SELECT '{\"a\":{\"b\":1}}'::jsonb #-'{a,b}' AS r")).Single().Row["r"].JsonValue);

        Assert.IsTrue((await Select(executor, db, "SELECT '{\"a\":{\"b\":\"x\"}}'::jsonb -> 'a' ->> 'b' = 'x' AS r")).Single().Row["r"].BoolValue,
            "the jsonb operators bind tighter than comparison");
    }

    [Test]
    public async Task Filters_UseContainmentKeysAndFields()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupEvents();

        CollectionAssert.AreEqual(new long[] { 1, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload @> '{\"kind\": \"login\"}'")));
        CollectionAssert.AreEqual(new long[] { 1, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload @> '{\"user\": {\"tags\": [\"vip\"]}}'")));
        CollectionAssert.AreEqual(new long[] { 1, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ->> 'kind' = 'login'")));
        CollectionAssert.AreEqual(new long[] { 1, 2 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload -> 'user' ->> 'id' = '7'")));
        CollectionAssert.AreEqual(new long[] { 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ? 'tmp'")));
        CollectionAssert.AreEqual(new long[] { 3, 4 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ?| array['tmp', 'object']")));
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ?& array['kind', 'user']")));
        CollectionAssert.AreEqual(new long[] { 1, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload #>> '{user,tags,0}' IS NOT NULL")));

        // The operators bind tighter than IS and IN, as applications write these filters.
        CollectionAssert.AreEqual(new long[] { 1, 2, 4 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ->> 'tmp' IS NULL AND payload IS NOT NULL")));
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ->> 'kind' IN ('login', 'logout')")));
        CollectionAssert.AreEqual(new long[] { 2 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload ->> 'kind' NOT IN ('login') AND payload ? 'kind'")));

        // A text literal compares with a document as a document; 1.0 equals 1.
        CollectionAssert.AreEqual(new long[] { 2 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload -> 'n' = '1'")));
        CollectionAssert.AreEqual(new long[] { 4 }, Ids(await Select(executor, db, "SELECT id FROM events WHERE payload = '[\"not\",\"an\",\"object\"]'")));
        CollectionAssert.IsEmpty(await Select(executor, db, "SELECT id FROM events WHERE payload = 'not a document'"));
    }

    [Test]
    public async Task SortsAndGroups_OnExtractedValues()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupEvents();

        List<QueryResultRow> sorted = await Select(executor, db, "SELECT id FROM events WHERE payload ? 'n' ORDER BY payload -> 'n' DESC");
        CollectionAssert.AreEqual(new long[] { 1, 3, 2 }, sorted.Select(r => r.Row["id"].LongValue).ToArray());

        List<QueryResultRow> grouped = await Select(executor, db,
            "SELECT payload ->> 'kind' AS kind, COUNT(*) AS c FROM events GROUP BY payload ->> 'kind'");
        Assert.AreEqual(2L, grouped.Single(r => r.Row["kind"].StrValue == "login").Row["c"].LongValue);
        Assert.AreEqual(1L, grouped.Single(r => r.Row["kind"].StrValue == "logout").Row["c"].LongValue);
        Assert.AreEqual(2L, grouped.Single(r => r.Row["kind"].Type == ColumnType.Null).Row["c"].LongValue);

        List<QueryResultRow> byDocument = await Select(executor, db,
            "SELECT payload -> 'user' -> 'id' AS uid, COUNT(*) AS c FROM events WHERE payload ? 'user' GROUP BY payload -> 'user' -> 'id'");
        Assert.AreEqual(2L, byDocument.Single(r => r.Row["uid"].JsonValue == "7").Row["c"].LongValue);
    }

    [Test]
    public async Task Updates_ConcatenateAndDelete()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupEvents();

        await Exec(executor, db, "UPDATE events SET payload = payload || '{\"seen\": true}' WHERE payload @> '{\"kind\": \"login\"}'");
        await Exec(executor, db, "UPDATE events SET payload = payload - 'tmp' WHERE id = 3");
        await Exec(executor, db, "UPDATE events SET payload = payload - 0 WHERE id = 4");
        await Exec(executor, db, "UPDATE events SET payload = payload #- '{user,tags}' WHERE id = 1");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT id, payload FROM events ORDER BY id");
        Assert.AreEqual("{\"n\": 3, \"kind\": \"login\", \"seen\": true, \"user\": {\"id\": 7}}", rows[0].Row["payload"].JsonValue);
        Assert.AreEqual("{\"n\": 2, \"kind\": \"login\", \"seen\": true, \"user\": {\"id\": 9, \"tags\": [\"beta\", \"vip\"]}}", rows[2].Row["payload"].JsonValue);
        Assert.AreEqual("[\"an\", \"object\"]", rows[3].Row["payload"].JsonValue);
        Assert.AreEqual(ColumnType.Null, rows[4].Row["payload"].Type);
    }

    [Test]
    public async Task View_OverOperators_ReadsBackAndShowsAParsableBody()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupEvents();

        await ExecDDL(executor, db,
            "CREATE VIEW logins AS SELECT id, payload ->> 'kind' AS kind, payload -> 'user' AS u FROM events WHERE payload @> '{\"kind\": \"login\"}'");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT id, kind, u FROM logins ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 3 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
        Assert.AreEqual("login", rows[0].Row["kind"].StrValue);
        Assert.AreEqual("{\"id\": 7, \"tags\": [\"vip\"]}", rows[0].Row["u"].JsonValue);

        // The stored body renders each operator as its function call, which parses again.
        string ddl = (await Select(executor, db, "SHOW CREATE VIEW logins")).Single().Row.Values
            .First(v => v.Type == ColumnType.String && v.StrValue!.Contains("CREATE")).StrValue!;
        StringAssert.Contains("jsonb_contains", ddl);

        (_, DatabaseDescriptor copy, _) = await CreateDatabase();
        await ExecDDL(executor, copy, "CREATE TABLE events (id int64 PRIMARY KEY, payload jsonb)");
        await ExecDDL(executor, copy, ddl);
        await Exec(executor, copy, "INSERT INTO events (id, payload) VALUES (1, '{\"kind\": \"login\", \"user\": 1}')");
        Assert.AreEqual("1", (await Select(executor, copy, "SELECT u FROM logins")).Single().Row["u"].JsonValue);
    }

    [Test]
    public async Task CreateTableAs_TypesEachOperatorResult()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupEvents();

        await ExecDDL(executor, db,
            "CREATE TABLE copy AS SELECT id, payload -> 'user' AS u, payload ->> 'kind' AS k, payload @> '{}' AS c, " +
            "payload || '[1]' AS cat, payload - 'kind' AS del, payload #> '{user,id}' AS p FROM events");

        List<QueryResultRow> columns = await Select(executor, db, "SHOW COLUMNS FROM copy");
        string TypeOf(string name) => columns.Single(r => r.Row["Field"].StrValue == name).Row["Type"].StrValue!;
        Assert.AreEqual("JSONB", TypeOf("u"));
        Assert.AreEqual("STRING", TypeOf("k"));
        Assert.AreEqual("BOOL", TypeOf("c"));
        Assert.AreEqual("JSONB", TypeOf("cat"));
        Assert.AreEqual("JSONB", TypeOf("del"));
        Assert.AreEqual("JSONB", TypeOf("p"));

        List<QueryResultRow> rows = await Select(executor, db, "SELECT id, u, k FROM copy WHERE id = 1");
        Assert.AreEqual("{\"id\": 7, \"tags\": [\"vip\"]}", rows.Single().Row["u"].JsonValue);
        Assert.AreEqual("login", rows.Single().Row["k"].StrValue);
    }

    /// <summary>
    /// A join on a jsonb key under the nested-loop, hash and merge strategies equals the oracle: equal
    /// documents match even when only a number's scale differs, and NULL matches nothing. An index
    /// nested-loop join does not apply: a jsonb column has no ordered index.
    /// </summary>
    [Test]
    public async Task Join_OnADocumentKey_EveryStrategyMatchesTheOracle([Values] Mode mode)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE l (id int64 PRIMARY KEY, k jsonb)");
        await ExecDDL(executor, db, "CREATE TABLE r (id int64 PRIMARY KEY, k jsonb)");
        await Exec(executor, db,
            "INSERT INTO l (id, k) VALUES (1, '{\"a\": 1}'), (2, '[1, 2]'), (3, '1.50'), (4, NULL), (5, '\"x\"'), (6, '{\"b\": [true]}')");
        await Exec(executor, db,
            "INSERT INTO r (id, k) VALUES (101, '{\"a\": 1.0}'), (102, '[1, 2]'), (103, '1.5'), (104, NULL), (105, '\"y\"'), (106, '{\"b\": [true]}'), (107, '[2, 1]')");

        switch (mode)
        {
            case Mode.NestedLoop: executor.Statistics.ForceNestedLoopForTesting = true; break;
            case Mode.Hash: executor.Statistics.ForceHashJoinForTesting = true; break;
            case Mode.Merge: executor.Statistics.ForceMergeJoinForTesting = true; break;
        }

        try
        {
            List<QueryResultRow> rows = await Select(executor, db,
                "SELECT l.id AS lid, r.id AS rid FROM l JOIN r ON l.k = r.k");
            List<(long, long)> pairs = rows.Select(x => (x.Row["lid"].LongValue, x.Row["rid"].LongValue)).OrderBy(p => p).ToList();
            CollectionAssert.AreEqual(new List<(long, long)> { (1, 101), (2, 102), (3, 103), (6, 106) }, pairs);
        }
        finally
        {
            executor.Statistics.ForceNestedLoopForTesting = false;
            executor.Statistics.ForceHashJoinForTesting = false;
            executor.Statistics.ForceMergeJoinForTesting = false;
        }
    }
}
