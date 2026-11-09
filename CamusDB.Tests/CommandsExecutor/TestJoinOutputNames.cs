/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoColType = CamusDB.Grpc.ColumnType;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The names of the output columns of a join, as PostgreSQL gives them: each column has its bare name,
/// a <c>*</c> lists the sources in the order that the FROM clause writes them (a RIGHT JOIN too), and two
/// columns can have one name. A client reads such columns by position, so every case here checks each
/// name and each value by position, and the values are read through the row key of each column, never
/// through its name. A reference that two columns answer is refused as ambiguous, also when it is
/// qualified by a derived table. Every expected result was measured on PostgreSQL 16.11 over the same
/// rows; a window column and an expression column keep the CamusDB position name, so their names are
/// not checked.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestJoinOutputNames : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>The derived table that most cases read: a <c>*</c> over a join whose names do not clash.</summary>
    private const string StarOverJ1K = "(SELECT * FROM j1 JOIN k ON j1.id = k.kid) x";

    private static readonly JsonSerializerOptions WireJson = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private async Task<Db> SetupAsync()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE j1 (id INT64 PRIMARY KEY, a INT64)");
        await Ddl(db, "CREATE TABLE j2 (id INT64 PRIMARY KEY, a INT64)");
        await Ddl(db, "CREATE TABLE k (kid INT64 PRIMARY KEY, b INT64)");
        await Ddl(db, "CREATE TABLE f (id INT64 PRIMARY KEY, a FLOAT64)");

        await Dml(db, "INSERT INTO j1 (id, a) VALUES (1, 10), (2, 20), (3, NULL)");
        await Dml(db, "INSERT INTO j2 (id, a) VALUES (1, 100), (2, 10), (4, 20)");
        await Dml(db, "INSERT INTO k (kid, b) VALUES (1, 7), (2, 8)");
        await Dml(db, "INSERT INTO f (id, a) VALUES (1, 1.5), (2, 2.5)");

        return db;
    }

    // ─── SQL ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task StarOverAJoin_GivesBareNamesInTheWrittenOrder()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT * FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY j1.id",
            ["id", "a", "id", "a"], ordered: true, "1|10|1|100", "2|20|2|10");

        await Expect(db, "SELECT * FROM j1, j2 WHERE j1.id = j2.id ORDER BY j1.id",
            ["id", "a", "id", "a"], ordered: true, "1|10|1|100", "2|20|2|10");

        await Expect(db, "SELECT * FROM j1 a JOIN j1 b ON a.id = b.id ORDER BY a.id",
            ["id", "a", "id", "a"], ordered: true, "1|10|1|10", "2|20|2|20", "3|NULL|3|NULL");

        await Expect(db, "SELECT * FROM k JOIN j1 ON j1.id = k.kid JOIN j2 ON j2.id = j1.id WHERE j2.id = 2",
            ["kid", "b", "id", "a", "id", "a"], ordered: true, "2|8|2|20|2|10");

        await Expect(db, "SELECT *, j1.a FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY j1.id",
            ["id", "a", "id", "a", "a"], ordered: true, "1|10|1|100|10", "2|20|2|10|20");
    }

    [Test]
    public async Task ExplicitColumns_KeepTheirBareNames_WithAliasesAndSwappedOrder()
    {
        Db db = await SetupAsync();

        // The predicate compares the same-named columns, and the two a columns hold different values.
        await Expect(db, "SELECT j1.id, j1.a, j2.a FROM j1 JOIN j2 ON j1.a = j2.a ORDER BY j1.id",
            ["id", "a", "a"], ordered: true, "1|10|10", "2|20|20");

        await Expect(db, "SELECT a.a, b.a FROM j1 a JOIN j2 b ON a.id = b.id ORDER BY a.id",
            ["a", "a"], ordered: true, "10|100", "20|10");

        await Expect(db, "SELECT j2.a, j1.a FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY j1.id",
            ["a", "a"], ordered: true, "100|10", "10|20");

        await Expect(db, "SELECT j1.a AS x, j2.a FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY j1.id",
            ["x", "a"], ordered: true, "10|100", "20|10");

        await Expect(db, "SELECT j1.a, j2.a FROM j1 CROSS JOIN j2",
            ["a", "a"], ordered: false,
            "10|10", "10|20", "10|100", "20|10", "20|20", "20|100", "NULL|10", "NULL|20", "NULL|100");

        await Expect(db, "SELECT DISTINCT j1.a, j2.a FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY 1",
            ["a", "a"], ordered: true, "10|100", "20|10");

        await Expect(db,
            "SELECT j1.a, j2.a FROM j1 JOIN j2 ON j1.id = j2.id UNION ALL " +
            "SELECT j2.a, j1.a FROM j1 JOIN j2 ON j1.id = j2.id",
            ["a", "a"], ordered: false, "10|100", "10|20", "20|10", "100|10");

        // Two columns of one name and of two types keep both types.
        (IReadOnlyList<DerivedColumnSchema> schema, List<string> rows) =
            await Query(db, "SELECT * FROM j1 JOIN f ON j1.id = f.id ORDER BY j1.id");
        CollectionAssert.AreEqual(new[] { "id", "a", "id", "a" }, schema.Select(c => c.Name));
        CollectionAssert.AreEqual(
            new[] { ColumnType.Integer64, ColumnType.Integer64, ColumnType.Integer64, ColumnType.Float64 },
            schema.Select(c => c.Type));
        CollectionAssert.AreEqual(new[] { "1|10|1|1.5", "2|20|2|2.5" }, rows);
    }

    [Test]
    public async Task RightJoin_ListsTheLeftOperandFirst()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT * FROM j1 RIGHT JOIN j2 ON j1.id = j2.id ORDER BY j2.id",
            ["id", "a", "id", "a"], ordered: true, "1|10|1|100", "2|20|2|10", "NULL|NULL|4|20");

        await Expect(db, "SELECT * FROM j1 RIGHT JOIN j2 ON j1.id = j2.id JOIN k ON k.kid = j2.id ORDER BY j2.id",
            ["id", "a", "id", "a", "kid", "b"], ordered: true, "1|10|1|100|1|7", "2|20|2|10|2|8");

        await Expect(db, "SELECT * FROM (SELECT kid AS q FROM k) x JOIN j1 ON j1.id = x.q ORDER BY x.q",
            ["q", "id", "a"], ordered: true, "1|1|10", "2|2|20");
    }

    [Test]
    public async Task AReferenceThatTwoColumnsAnswer_IsAmbiguous()
    {
        Db db = await SetupAsync();

        await ExpectAmbiguous(db, "SELECT id FROM j1 JOIN j2 ON j1.id = j2.id", "id");
        await ExpectAmbiguous(db, "SELECT a FROM (SELECT j1.a, j2.a FROM j1 JOIN j2 ON j1.id = j2.id) d", "a");
        await ExpectAmbiguous(db, "SELECT d.a FROM (SELECT j1.a, j2.a FROM j1 JOIN j2 ON j1.id = j2.id) d", "d.a");
        await ExpectAmbiguous(db, "SELECT d.id FROM (SELECT * FROM j1 JOIN j2 ON j1.id = j2.id) d", "d.id");

        // The ambiguous derived table itself is fine: only a reference by name is refused.
        await Expect(db, "SELECT * FROM (SELECT * FROM j1 JOIN j2 ON j1.id = j2.id) d",
            ["id", "a", "id", "a"], ordered: false, "1|10|1|100", "2|20|2|10");

        await Expect(db, "SELECT * FROM (SELECT *, j1.a FROM j1 JOIN j2 ON j1.id = j2.id) d",
            ["id", "a", "id", "a", "a"], ordered: false, "1|10|1|100|10", "2|20|2|10|20");
    }

    [Test]
    public async Task DerivedTableOverAStarJoin_ReadsEachColumnByItsName()
    {
        Db db = await SetupAsync();

        await Expect(db, $"SELECT * FROM {StarOverJ1K}",
            ["id", "a", "kid", "b"], ordered: false, "1|10|1|7", "2|20|2|8");

        await Expect(db, $"SELECT x.a FROM {StarOverJ1K} ORDER BY 1", ["a"], ordered: true, "10", "20");

        await Expect(db, $"SELECT a, b FROM {StarOverJ1K} ORDER BY 1", ["a", "b"], ordered: true, "10|7", "20|8");

        await Expect(db, $"SELECT x.a + 1 FROM {StarOverJ1K} WHERE x.b > 7", [null], ordered: true, "21");

        await Expect(db, $"SELECT x.a, j2.a FROM {StarOverJ1K} JOIN j2 ON j2.id = x.kid ORDER BY 1",
            ["a", "a"], ordered: true, "10|100", "20|10");

        await Expect(db, $"SELECT * FROM {StarOverJ1K} JOIN j2 ON j2.id = x.kid ORDER BY x.id",
            ["id", "a", "kid", "b", "id", "a"], ordered: true, "1|10|1|7|1|100", "2|20|2|8|2|10");

        await Expect(db, $"SELECT * FROM j2 LEFT JOIN {StarOverJ1K} ON j2.id = x.kid ORDER BY j2.id",
            ["id", "a", "id", "a", "kid", "b"], ordered: true,
            "1|100|1|10|1|7", "2|10|2|20|2|8", "4|20|NULL|NULL|NULL|NULL");

        // The outer j1 is a real table, and the derived table read j1 too: neither may overwrite the other.
        await Expect(db, $"SELECT * FROM {StarOverJ1K} JOIN j1 ON j1.id = 1 ORDER BY x.id",
            ["id", "a", "kid", "b", "id", "a"], ordered: true, "1|10|1|7|1|10", "2|20|2|8|1|10");
    }

    [Test]
    public async Task SubqueriesOverAStarJoin_ReadEachColumnByItsName()
    {
        Db db = await SetupAsync();

        await Expect(db, $"SELECT j2.id FROM j2 WHERE EXISTS (SELECT 1 FROM {StarOverJ1K} WHERE x.kid = j2.id) ORDER BY 1",
            ["id"], ordered: true, "1", "2");

        await Expect(db, $"SELECT x.kid FROM {StarOverJ1K} WHERE EXISTS (SELECT 1 FROM j2 WHERE j2.id = x.kid) ORDER BY 1",
            ["kid"], ordered: true, "1", "2");

        await Expect(db, $"SELECT kid FROM k WHERE kid IN (SELECT x.id FROM {StarOverJ1K}) ORDER BY 1",
            ["kid"], ordered: true, "1", "2");

        await Expect(db, $"SELECT (SELECT x.b FROM {StarOverJ1K} WHERE x.id = j2.id) FROM j2 ORDER BY j2.id",
            ["b"], ordered: true, "7", "8", "NULL");
    }

    [Test]
    public async Task WindowBesideAJoin_KeepsTheBareNames()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT *, row_number() OVER (ORDER BY j1.id) FROM j1 JOIN k ON j1.id = k.kid ORDER BY j1.id",
            ["id", "a", "kid", "b", null], ordered: true, "1|10|1|7|1", "2|20|2|8|2");

        await Expect(db, "SELECT j1.a, j2.a, row_number() OVER (ORDER BY j1.id) FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY j1.id",
            ["a", "a", null], ordered: true, "10|100|1", "20|10|2");
    }

    [Test]
    public async Task ViewsAndCreateTableAs_TakeTheBareNames_AndRefuseTwoOfOne()
    {
        Db db = await SetupAsync();

        await Ddl(db, "CREATE VIEW vk AS SELECT * FROM j1 JOIN k ON j1.id = k.kid");
        await Expect(db, "SELECT * FROM vk ORDER BY id", ["id", "a", "kid", "b"], ordered: true, "1|10|1|7", "2|20|2|8");
        await Expect(db, "SELECT a, kid FROM vk ORDER BY 1", ["a", "kid"], ordered: true, "10|1", "20|2");

        await Ddl(db, "CREATE VIEW vk2 AS SELECT * FROM j1 RIGHT JOIN k ON j1.id = k.kid");
        await Expect(db, "SELECT * FROM vk2 ORDER BY kid", ["id", "a", "kid", "b"], ordered: true, "1|10|1|7", "2|20|2|8");

        await Ddl(db, "CREATE VIEW vok AS SELECT j1.a AS a1, j2.a AS a2 FROM j1 JOIN j2 ON j1.id = j2.id");
        await Expect(db, "SELECT * FROM vok ORDER BY a1", ["a1", "a2"], ordered: true, "10|100", "20|10");

        await Ddl(db, "CREATE TABLE ck AS SELECT * FROM j1 JOIN k ON j1.id = k.kid");
        await Expect(db, "SELECT id, a, kid, b FROM ck ORDER BY kid", ["id", "a", "kid", "b"], ordered: true, "1|10|1|7", "2|20|2|8");

        await Ddl(db, $"CREATE TABLE cx AS SELECT * FROM {StarOverJ1K}");
        await Expect(db, "SELECT id, a, kid, b FROM cx ORDER BY kid", ["id", "a", "kid", "b"], ordered: true, "1|10|1|7", "2|20|2|8");

        await ExpectDdlRefused(db, "CREATE TABLE cj AS SELECT * FROM j1 JOIN j2 ON j1.id = j2.id", "more than one output column named 'id'");
        await ExpectDdlRefused(db, "CREATE VIEW vbad AS SELECT * FROM j1 JOIN j2 ON j1.id = j2.id", "'id' is specified more than once");
        await ExpectDdlRefused(db, "CREATE MATERIALIZED VIEW mvbad AS SELECT * FROM j1 JOIN j2 ON j1.id = j2.id", "'id'");
    }

    // ─── Wire ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task OverHttp_EveryColumnArrivesInOrderWithItsType()
    {
        Db db = await SetupAsync();

        ExecuteSQLController controller = new(db.Executor, new HttpTransactionCoordinator(db.Executor),
            new PreparedStatementRegistry(Options), logger, Options)
        {
            ControllerContext = Context(new { databaseName = db.Name, sql = "SELECT * FROM j1 JOIN f ON j1.id = f.id ORDER BY j1.id" }),
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        CollectionAssert.AreEqual(new[] { "id", "a", "id", "a" }, response.Columns.Select(c => c.Name));
        CollectionAssert.AreEqual(
            new[] { ColumnType.Integer64, ColumnType.Integer64, ColumnType.Integer64, ColumnType.Float64 },
            response.Columns.Select(c => c.Type));

        using JsonDocument doc = JsonDocument.Parse(JsonSerializer.Serialize(response, WireJson));
        List<string> rows = doc.RootElement.GetProperty("rows").EnumerateArray()
            .Select(row => string.Join("|", row.EnumerateArray().Select(JsonCell)))
            .ToList();

        CollectionAssert.AreEqual(new[] { "1|10|1|1.5", "2|20|2|2.5" }, rows);
    }

    [Test]
    public async Task OverGrpc_EveryColumnArrivesInOrderWithItsType()
    {
        Db db = await SetupAsync();

        CamusSqlService service = new(db.Executor, new HttpTransactionCoordinator(db.Executor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await service.ExecuteQuery(
            new SqlRequest { Database = db.Name, Sql = "SELECT * FROM j1 RIGHT JOIN f ON j1.id = f.id ORDER BY f.id" },
            writer, new TestServerCallContext(CancellationToken.None));

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(writer.Written, 2);
        CollectionAssert.AreEqual(new[] { "id", "a", "id", "a" }, schema.Columns.Select(c => c.Name));
        CollectionAssert.AreEqual(
            new[] { ProtoColType.Integer64, ProtoColType.Integer64, ProtoColType.Integer64, ProtoColType.Float64 },
            schema.Columns.Select(c => c.Type));

        List<string> rows = writer.Written.Skip(1)
            .Select(message => string.Join("|", message.Row.Values.Select(GrpcCell)))
            .ToList();

        CollectionAssert.AreEqual(new[] { "1|10|1|1.5", "2|20|2|2.5" }, rows);
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static async Task Expect(Db db, string sql, string?[] names, bool ordered, params string[] rows)
    {
        (IReadOnlyList<DerivedColumnSchema> schema, List<string> actual) = await Query(db, sql);

        Assert.AreEqual(names.Length, schema.Count, $"{sql}: column count ({string.Join(", ", schema.Select(c => c.Name))})");

        for (int i = 0; i < names.Length; i++)
        {
            if (names[i] is { } name)
                Assert.AreEqual(name, schema[i].Name, $"{sql}: the name of column {i}");
        }

        List<string> expected = rows.ToList();

        if (!ordered)
        {
            expected.Sort(StringComparer.Ordinal);
            actual.Sort(StringComparer.Ordinal);
        }

        CollectionAssert.AreEqual(expected, actual, sql);
    }

    private static async Task ExpectAmbiguous(Db db, string sql, string reference)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Query(db, sql), sql)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{sql}: {ex.Message}");
        Assert.AreEqual($"Ambiguous column: {reference}", ex.Message, sql);
    }

    private static async Task ExpectDdlRefused(Db db, string sql, string message)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(db, sql), sql)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{sql}: {ex.Message}");
        StringAssert.Contains(message, ex.Message, sql);
    }

    /// <summary>Each row as its cells in schema order, each cell read through the row key of its column.</summary>
    private static async Task<(IReadOnlyList<DerivedColumnSchema> Schema, List<string> Rows)> Query(Db db, string sql)
    {
        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null), schemaOut: holder);
            List<QueryResultRow> rows = await cursor.ToListAsync();

            List<string> rendered = rows
                .Select(row => string.Join("|", holder.Schema.Select(column => Cell(row.Row[column.RowKey]))))
                .ToList();

            return (holder.Schema, rendered);
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Database.Transactions.CommitAsync(tx);
    }

    /// <summary>A cell as psql prints it, so a row compares with the PostgreSQL measurement as text.</summary>
    internal static string Cell(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString("R", CultureInfo.InvariantCulture),
        ColumnType.Numeric => value.NumericValue!,
        _ => value.StrValue ?? throw new AssertionException($"no text form for a {value.Type} cell"),
    };

    private static string JsonCell(JsonElement cell) => cell.ValueKind switch
    {
        JsonValueKind.Null => "NULL",
        JsonValueKind.String => cell.GetString()!,
        _ => cell.GetRawText(),
    };

    private static string GrpcCell(Value cell) => cell.KindCase switch
    {
        Value.KindOneofCase.NullValue => "NULL",
        Value.KindOneofCase.Int64Value => cell.Int64Value.ToString(CultureInfo.InvariantCulture),
        Value.KindOneofCase.Float64Value => cell.Float64Value.ToString("R", CultureInfo.InvariantCulture),
        _ => throw new AssertionException($"unexpected gRPC value kind {cell.KindCase}"),
    };

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }
}
