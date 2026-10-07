/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end acceptance of LEFT, RIGHT and CROSS JOIN through the real entry points: the SQL
/// query, non-query and DDL tickets, and the HTTP query route. Covers the contract (ON before
/// padding, WHERE after), the RIGHT JOIN rewrite, the CROSS JOIN equivalence with the comma form,
/// aggregates and DISTINCT over padded rows, a transaction's own uncommitted writes, views,
/// materialized views, a view over a view, a table rename under a stored body, INSERT ... SELECT,
/// CREATE TABLE ... AS SELECT, and the forms that are refused.
/// </summary>
[NonParallelizable]
public sealed class TestOuterJoinAcceptance : SharedNodeBaseTest
{
    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// a(id, k, name): 1→k1, 2→k2, 3→k3, 4→NULL.
    /// b(id, a_k, x): 10→(1,5), 11→(2,5), 12→(2,NULL), 13→(NULL,7), 14→(99,7).
    /// a LEFT JOIN b ON b.a_k = a.k gives a1/b10, a2/b11, a2/b12, a3/-, a4/-.
    /// </summary>
    private async Task<Fixture> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "a",
            columns: [new("id", ColumnType.Integer64), new("k", ColumnType.Integer64), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "b",
            columns: [new("id", ColumnType.Integer64), new("a_k", ColumnType.Integer64), new("x", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.Insert(new InsertTicket(txn, dbname, "a",
            values: [A(1, 1, "one"), A(2, 2, "two"), A(3, 3, "three"), A(4, null, "four")]));

        await executor.Insert(new InsertTicket(txn, dbname, "b",
            values: [B(10, 1, 5), B(11, 2, 5), B(12, 2, null), B(13, null, 7), B(14, 99, 7)]));

        await database.Transactions.CommitAsync(txn);
        return new Fixture(dbname, database, executor);
    }

    private static ColumnValue I(long? v) => v is { } x ? new(ColumnType.Integer64, x) : ColumnValue.Null;

    private static Dictionary<string, ColumnValue> A(long id, long? k, string name) =>
        new() { { "id", I(id) }, { "k", I(k) }, { "name", new(ColumnType.String, name) } };

    private static Dictionary<string, ColumnValue> B(long id, long? aK, long? x) =>
        new() { { "id", I(id) }, { "a_k", I(aK) }, { "x", I(x) } };

    private static async Task<List<QueryResultRow>> Query(Fixture f, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        List<QueryResultRow> rows = await Query(f, txn, sql, parameters);
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static async Task<List<QueryResultRow>> Query(Fixture f, KvTransaction txn, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: parameters));
        return await cursor.ToListAsync();
    }

    private static async Task Ddl(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
        await f.Database.Transactions.CommitAsync(txn);
    }

    private static async Task<int> NonQuery(Fixture f, KvTransaction txn, string sql)
    {
        ExecuteNonSQLResult result = await f.Executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
        return result.ModifiedRows;
    }

    private static long? Long(QueryResultRow row, string key) =>
        row.Row[key].Type == ColumnType.Null ? null : row.Row[key].LongValue;

    private static List<(long, long?)> Pairs(List<QueryResultRow> rows) =>
        rows.Select(r => (Long(r, "aid")!.Value, Long(r, "bid"))).OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList();

    private static readonly List<(long, long?)> ExpectedPairs = [(1, 10), (2, 11), (2, 12), (3, null), (4, null)];

    private const string LeftSql = "SELECT a.id AS aid, b.id AS bid, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k";

    [Test]
    public async Task MotivatingOrmQuery_WithParameter_PadsTheMissingRow()
    {
        Fixture f = await SetupAsync();

        const string sql = "SELECT a.id AS aid, a.k AS ak, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k WHERE a.id = @aid";

        List<QueryResultRow> present = await Query(f, sql, new() { { "@aid", I(1) } });
        Assert.AreEqual(1, present.Count);
        Assert.AreEqual(5L, Long(present[0], "bx"));

        List<QueryResultRow> missing = await Query(f, sql, new() { { "@aid", I(3) } });
        Assert.AreEqual(1, missing.Count, "the row without a match is still returned");
        Assert.AreEqual(3L, Long(missing[0], "aid"));
        Assert.AreEqual(3L, Long(missing[0], "ak"));
        Assert.IsNull(Long(missing[0], "bx"), "the right column is NULL on the padded row");
    }

    [Test]
    public async Task LeftJoin_AndLeftOuterJoin_ReturnTheSameRows()
    {
        Fixture f = await SetupAsync();

        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, LeftSql)));
        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, LeftSql.Replace("LEFT JOIN", "LEFT OUTER JOIN"))));
    }

    [Test]
    public async Task RightJoin_IsTheSwappedLeftJoin()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> right = await Query(f, "SELECT a.id AS aid, b.id AS bid FROM b RIGHT JOIN a ON b.a_k = a.k");
        Assert.AreEqual(ExpectedPairs, Pairs(right));

        List<QueryResultRow> rightOuter = await Query(f, "SELECT a.id AS aid, b.id AS bid FROM b RIGHT OUTER JOIN a ON b.a_k = a.k");
        Assert.AreEqual(ExpectedPairs, Pairs(rightOuter));
    }

    [Test]
    public async Task RightJoinAfterAnotherJoin_IsRefusedThroughTheQueryEntryPoint()
    {
        Fixture f = await SetupAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            Query(f, "SELECT a.id FROM a JOIN b ON b.a_k = a.k RIGHT JOIN a a2 ON a2.id = a.id"))!;
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, ex.Code);
    }

    [Test]
    public async Task CrossJoin_ReturnsEveryPair_AndEqualsTheCommaForm()
    {
        Fixture f = await SetupAsync();

        static List<(long, long)> Shape(List<QueryResultRow> rows) =>
            rows.Select(r => (Long(r, "aid")!.Value, Long(r, "bid")!.Value)).OrderBy(p => p).ToList();

        List<QueryResultRow> cross = await Query(f, "SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b");
        Assert.AreEqual(4 * 5, cross.Count);

        List<QueryResultRow> comma = await Query(f, "SELECT a.id AS aid, b.id AS bid FROM a, b");
        Assert.AreEqual(Shape(comma), Shape(cross));

        // A cross join among ON joins is an inner join on the literal true.
        List<QueryResultRow> mixed = await Query(f,
            "SELECT a.id AS aid, b.id AS bid, c.id AS cid FROM a JOIN b ON b.a_k = a.k CROSS JOIN a c");
        Assert.AreEqual(3 * 4, mixed.Count, "three matched (a, b) pairs times four rows of c");
    }

    [Test]
    public async Task WhereOnTheNullExtendedSide_FiltersAfterPadding()
    {
        Fixture f = await SetupAsync();

        // IS NULL keeps the padded rows and the matched row whose x is NULL.
        List<QueryResultRow> isNull = await Query(f, LeftSql + " WHERE b.x IS NULL");
        Assert.AreEqual(new List<(long, long?)> { (2, 12), (3, null), (4, null) }, Pairs(isNull));

        // An equality removes every padded row.
        List<QueryResultRow> equal = await Query(f, LeftSql + " WHERE b.x = 5");
        Assert.AreEqual(new List<(long, long?)> { (1, 10), (2, 11) }, Pairs(equal));
    }

    [Test]
    public async Task NullKeys_NeverMatch_OnEitherSide()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await Query(f, LeftSql);
        Assert.IsFalse(rows.Any(r => Long(r, "bid") == 13), "the NULL-keyed b row matches nothing");
        Assert.AreEqual(1, rows.Count(r => Long(r, "aid") == 4), "the NULL-keyed a row is padded once");
        Assert.IsNull(Long(rows.First(r => Long(r, "aid") == 4), "bid"));
    }

    [Test]
    public async Task SelectStar_OverAnEmptyRightTable_PadsEveryRowWithTheMatchedShape()
    {
        Fixture f = await SetupAsync();

        await Ddl(f, "CREATE TABLE c (id INT64 PRIMARY KEY, a_k INT64, y STRING)");

        List<QueryResultRow> rows = await Query(f, "SELECT * FROM a LEFT JOIN c ON c.a_k = a.k ORDER BY a.id");
        Assert.AreEqual(4, rows.Count);

        foreach (QueryResultRow row in rows)
        {
            CollectionAssert.AreEquivalent(new[] { "a.id", "a.k", "a.name", "c.id", "c.a_k", "c.y" }, row.Row.Keys.ToList());
            Assert.AreEqual(ColumnType.Null, row.Row["c.id"].Type);
            Assert.AreEqual(ColumnType.Null, row.Row["c.a_k"].Type);
            Assert.AreEqual(ColumnType.Null, row.Row["c.y"].Type);
        }
    }

    [Test]
    public async Task Aggregates_CountPaddedRowsOnlyWithStar()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> totals = await Query(f,
            "SELECT COUNT(*) AS total, COUNT(b.id) AS matched FROM a LEFT JOIN b ON b.a_k = a.k");
        Assert.AreEqual(1, totals.Count);
        Assert.AreEqual(5L, Long(totals[0], "total"), "COUNT(*) counts the padded rows");
        Assert.AreEqual(3L, Long(totals[0], "matched"), "COUNT(b.id) skips the NULLs of the padded rows");

        List<QueryResultRow> groups = await Query(f,
            "SELECT a.id AS aid, COUNT(b.id) AS n FROM a LEFT JOIN b ON b.a_k = a.k GROUP BY a.id ORDER BY a.id");
        Assert.AreEqual(new List<(long, long?)> { (1, 1), (2, 2), (3, 0), (4, 0) },
            groups.Select(r => (Long(r, "aid")!.Value, Long(r, "n"))).ToList(), "every left group survives, with zero matches where padded");
    }

    [Test]
    public async Task OrderByAndDistinct_OverPaddedRows_FollowTheNullRules()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> ordered = await Query(f, LeftSql + " ORDER BY b.x, a.id");
        // NULL sorts first in this engine: the three NULL x rows (a2/b12, a3, a4) then the two 5s.
        CollectionAssert.AreEqual(new long?[] { null, null, null, 5, 5 }, ordered.Select(r => Long(r, "bx")).ToList());
        CollectionAssert.AreEqual(new long[] { 2, 3, 4, 1, 2 }, ordered.Select(r => Long(r, "aid")!.Value).ToList());

        List<QueryResultRow> distinct = await Query(f, "SELECT DISTINCT b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k ORDER BY b.x");
        CollectionAssert.AreEqual(new long?[] { null, 5 }, distinct.Select(r => Long(r, "bx")).ToList(), "NULLs dedupe as one value");
    }

    [Test]
    public async Task InsideATransaction_TheJoinSeesItsOwnWritesOnBothSides()
    {
        Fixture f = await SetupAsync();

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "a", values: [A(5, 7, "five"), A(6, 8, "six")]));
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "b", values: [B(15, 7, 1)]));

        List<QueryResultRow> rows = await Query(f, txn, LeftSql + " WHERE a.id > 4");
        Assert.AreEqual(new List<(long, long?)> { (5, 15), (6, null) }, Pairs(rows),
            "the uncommitted a rows are joined against the uncommitted b row and padded where none exists");

        await f.Database.Transactions.RollbackAsync(txn);

        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, LeftSql)), "nothing leaked after the rollback");
    }

    [Test]
    public async Task Views_KeepTheKind_AndReturnPaddedRows()
    {
        Fixture f = await SetupAsync();

        await Ddl(f, "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k");

        List<QueryResultRow> shown = await Query(f, "SHOW CREATE VIEW v");
        string body = string.Join(" ", shown.SelectMany(r => r.Row.Values).Select(v => v.StrValue));
        StringAssert.Contains("LEFT OUTER JOIN", body, "the stored body renders the kind");

        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, "SELECT aid, bid, bx FROM v")));

        // A view over the view: the inner body's LEFT JOIN still pads.
        await Ddl(f, "CREATE VIEW v2 AS SELECT aid, bid FROM v WHERE bx IS NULL");
        Assert.AreEqual(new List<(long, long?)> { (2, 12), (3, null), (4, null) }, Pairs(await Query(f, "SELECT aid, bid FROM v2")));

        // The stored body is bound by relation id, so a rename of the right table keeps the kind.
        await Ddl(f, "ALTER TABLE b RENAME TO bb");
        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, "SELECT aid, bid, bx FROM v")), "the view still pads after the rename");
        shown = await Query(f, "SHOW CREATE VIEW v");
        body = string.Join(" ", shown.SelectMany(r => r.Row.Values).Select(v => v.StrValue));
        StringAssert.Contains("LEFT OUTER JOIN", body);
        StringAssert.Contains("bb", body);
    }

    [Test]
    public async Task MaterializedView_RefreshReturnsPaddedRows()
    {
        Fixture f = await SetupAsync();

        await Ddl(f, "CREATE MATERIALIZED VIEW mv AS SELECT a.id AS aid, b.id AS bid, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k");
        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, "SELECT aid, bid, bx FROM mv")));

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        await f.Executor.Insert(new InsertTicket(txn, f.DbName, "a", values: [A(5, 50, "five")]));
        await f.Database.Transactions.CommitAsync(txn);

        KvTransaction refreshTxn = await f.Database.Transactions.BeginAsync();
        await NonQuery(f, refreshTxn, "REFRESH MATERIALIZED VIEW mv");
        await f.Database.Transactions.CommitAsync(refreshTxn);

        List<(long, long?)> refreshed = new(ExpectedPairs) { (5, null) };
        Assert.AreEqual(refreshed, Pairs(await Query(f, "SELECT aid, bid, bx FROM mv")), "the refresh pads the new unmatched row");
    }

    [Test]
    public async Task InsertSelect_AndCreateTableAsSelect_StorePaddedRowsAsNulls()
    {
        Fixture f = await SetupAsync();

        await Ddl(f, "CREATE TABLE t (aid INT64 PRIMARY KEY, bx INT64)");

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        int inserted = await NonQuery(f, txn,
            "INSERT INTO t (aid, bx) SELECT a.id, b.x FROM a LEFT JOIN b ON b.a_k = a.k WHERE a.id <> 2");
        await f.Database.Transactions.CommitAsync(txn);
        Assert.AreEqual(3, inserted, "a1, a3, a4 (a2 excluded to keep the key unique)");

        List<QueryResultRow> stored = await Query(f, "SELECT aid, bx FROM t ORDER BY aid");
        CollectionAssert.AreEqual(new long?[] { 5, null, null }, stored.Select(r => Long(r, "bx")).ToList());

        await Ddl(f, "CREATE TABLE t2 AS SELECT a.id AS aid, b.id AS bid, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k");
        Assert.AreEqual(ExpectedPairs, Pairs(await Query(f, "SELECT aid, bid, bx FROM t2")));
    }

    [Test]
    public async Task HttpQueryRoute_ReturnsThePaddedColumnAsJsonNull()
    {
        Fixture f = await SetupAsync();

        HttpTransactionCoordinator coordinator = new(f.Executor);
        PreparedStatementRegistry registry = new(Options);

        object body = new { databaseName = f.DbName, sql = "SELECT a.id AS aid, b.x AS bx FROM a LEFT JOIN b ON b.a_k = a.k WHERE a.id = 3" };
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();

        ExecuteSQLController controller = new(f.Executor, coordinator, registry, logger, Options)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total);
        int bx = response.Columns.FindIndex(c => string.Equals(c.Name, "bx", StringComparison.OrdinalIgnoreCase));
        Assert.GreaterOrEqual(bx, 0, "the padded right column is present in the column schema");

        string json = JsonSerializer.Serialize(response, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        using JsonDocument doc = JsonDocument.Parse(json);
        JsonElement row = doc.RootElement.GetProperty("rows")[0];
        Assert.AreEqual(JsonValueKind.Number, row[1 - bx].ValueKind, "the preserved column carries its value");
        Assert.AreEqual(JsonValueKind.Null, row[bx].ValueKind, "the padded column is null on the wire");
    }
}
