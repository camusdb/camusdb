/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The REST result surfaces declare an array column's element type as <c>elementType</c> on the
/// column object. The positional JSON writes an array as a bare JSON array — a DATE element as its
/// ticks, a BYTES element as base64, a FLOAT64 zero as <c>0</c> — so a client that is told only
/// <c>"type":10</c> cannot decode the elements. The field is present only for an array column, and
/// carries the same integer encoding as <c>type</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestRestArrayElementType : BaseTest
{
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    private ILogger<ICamusDB> Logger => logger;

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Controller(CommandExecutor executor, object body) =>
        new(executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), Logger, Options)
        {
            ControllerContext = Context(body)
        };

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
    }

    private async Task<(string dbname, CommandExecutor executor)> SetupArraysTable(bool withRow)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, dbname,
            "CREATE TABLE arrs (id int64 PRIMARY KEY, ad array(date), ab array(bytes), af array(float64), " +
            "au array(uuid), ai array(integer), name string)");

        if (!withRow)
            return (dbname, executor);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txn, dbname, "arrs", new()
        {
            new()
            {
                { "id", new ColumnValue(ColumnType.Integer64, 1L) },
                { "ad", ColumnValue.FromArray(ColumnType.Date, [new ColumnValue(ColumnType.Date, new DateTime(2000, 1, 1).Ticks)]) },
                { "ab", ColumnValue.FromArray(ColumnType.Bytes, [new ColumnValue(new byte[] { 1, 2 })]) },
                { "af", ColumnValue.FromArray(ColumnType.Float64, [new ColumnValue(ColumnType.Float64, 0.0), new ColumnValue(ColumnType.Float64, 1.5)]) },
                { "au", ColumnValue.FromArray(ColumnType.Uuid, [ColumnValue.FromUuid(Guid.NewGuid())]) },
                { "ai", ColumnValue.FromArray(ColumnType.Integer64, [new ColumnValue(ColumnType.Integer64, 7L)]) },
                { "name", new ColumnValue(ColumnType.String, "a") },
            }
        }));
        await database.Transactions.CommitAsync(txn);

        return (dbname, executor);
    }

    private static void AssertArrayColumns(IReadOnlyList<ColumnSchemaDto> columns)
    {
        Dictionary<string, ColumnSchemaDto> byName = columns.ToDictionary(c => c.Name);

        Assert.AreEqual(ColumnType.Array, byName["ad"].Type);
        Assert.AreEqual(ColumnType.Date, byName["ad"].ElementType);
        Assert.AreEqual(ColumnType.Bytes, byName["ab"].ElementType);
        Assert.AreEqual(ColumnType.Float64, byName["af"].ElementType);
        Assert.AreEqual(ColumnType.Uuid, byName["au"].ElementType);
        Assert.AreEqual(ColumnType.Integer64, byName["ai"].ElementType, "ARRAY(integer) is ARRAY(int64)");
        Assert.IsNull(byName["id"].ElementType, "a non-array column carries no element type");
        Assert.IsNull(byName["name"].ElementType);
    }

    /// <summary>
    /// The wire shape, pinned exactly: an array column is <c>{"name":…,"type":10,"elementType":N}</c>
    /// and a non-array column has no <c>elementType</c> key at all. Both client libraries read this
    /// exact field name.
    /// </summary>
    [Test]
    public void ColumnJson_ArrayHasElementType_ScalarOmitsIt()
    {
        List<ColumnSchemaDto> columns =
        [
            new() { Name = "ad", Type = ColumnType.Array, ElementType = ColumnType.Date },
            new() { Name = "id", Type = ColumnType.Integer64 },
        ];

        Assert.AreEqual(
            "[{\"name\":\"ad\",\"type\":10,\"elementType\":8},{\"name\":\"id\",\"type\":2}]",
            JsonSerializer.Serialize(columns, WebJson));
    }

    /// <summary>
    /// <c>/execute-sql-query</c> on a table with array columns: the element type comes from the
    /// table schema, so it is declared even when the result has no rows.
    /// </summary>
    [Test]
    public async Task Query_TableArrayColumns_DeclareElementType_EvenWithNoRows()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: false);

        JsonResult result = await Controller(executor, new { databaseName = dbname, sql = "SELECT * FROM arrs" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(0, response.Total);
        AssertArrayColumns(response.Columns);

        // Named projections resolve the same way as the star expansion.
        result = await Controller(executor, new { databaseName = dbname, sql = "SELECT a.ad AS dates, af FROM arrs a" }).ExecuteSQLQuery();
        response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(ColumnType.Date, response.Columns.Single(c => c.Name == "dates").ElementType);
        Assert.AreEqual(ColumnType.Float64, response.Columns.Single(c => c.Name == "af").ElementType);
    }

    /// <summary>
    /// The element type survives a derived table, and a computed array whose element type the planner
    /// does not know (a function result) takes it from the first array cell in the result.
    /// </summary>
    [Test]
    public async Task Query_DerivedAndComputedArrays_DeclareElementType()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        JsonResult result = await Controller(executor, new
        {
            databaseName = dbname,
            sql = "SELECT d.ad, regexp_split_to_array('p,q', ',') AS ss FROM (SELECT ad FROM arrs) d",
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total);
        Assert.AreEqual(ColumnType.Date, response.Columns.Single(c => c.Name == "ad").ElementType);
        Assert.AreEqual(ColumnType.Array, response.Columns.Single(c => c.Name == "ss").Type);
        Assert.AreEqual(ColumnType.String, response.Columns.Single(c => c.Name == "ss").ElementType);

        // FROM-less: the one row is evaluated before the schema is built.
        result = await Controller(executor, new { databaseName = dbname, sql = "SELECT ARRAY[1, 2] AS ns" }).ExecuteSQLQuery();
        response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(ColumnType.Integer64, response.Columns.Single(c => c.Name == "ns").ElementType);
    }

    /// <summary>
    /// An <c>ARRAY[…]</c> literal projection is declared as an array (it used to be declared a string)
    /// with the element type the literal evaluates to: the type of its first non-NULL element, when
    /// that is a typed literal, a cast or a base-table column. An empty literal is still an array; its
    /// element type comes from the evaluated value when it has one, and is omitted otherwise.
    /// </summary>
    [Test]
    public async Task Query_ArrayLiteralProjection_IsAnArrayWithItsElementType()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        JsonResult result = await Controller(executor, new
        {
            databaseName = dbname,
            sql = "SELECT ARRAY[1, 2] AS ints, ARRAY[NULL, 1.5] AS floats, ARRAY['p'] AS strs, " +
                  "ARRAY['2024-01-01'::date] AS dates, ARRAY[id, 3] AS ids, ARRAY[] AS empty FROM arrs",
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Dictionary<string, ColumnSchemaDto> byName = response.Columns.ToDictionary(c => c.Name);

        foreach (string name in new[] { "ints", "floats", "strs", "dates", "ids", "empty" })
            Assert.AreEqual(ColumnType.Array, byName[name].Type, name);

        Assert.AreEqual(ColumnType.Integer64, byName["ints"].ElementType);
        Assert.AreEqual(ColumnType.Float64, byName["floats"].ElementType, "a leading NULL does not decide the type");
        Assert.AreEqual(ColumnType.String, byName["strs"].ElementType);
        Assert.AreEqual(ColumnType.Date, byName["dates"].ElementType);
        Assert.AreEqual(ColumnType.Integer64, byName["ids"].ElementType, "a base-table column element");
        Assert.IsNull(byName["empty"].ElementType, "an empty array has no element type to report");

        Assert.AreEqual(
            "{\"name\":\"ints\",\"type\":10,\"elementType\":2}",
            JsonSerializer.Serialize(byName["ints"], WebJson));
    }

    /// <summary>An <c>ARRAY[…]</c> literal in a RETURNING list is declared the same way.</summary>
    [Test]
    public async Task NonQuery_ReturningArrayLiteral_IsAnArrayWithItsElementType()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        JsonResult result = await Controller(executor, new
        {
            databaseName = dbname,
            sql = "UPDATE arrs SET name = 'b' WHERE id = 1 RETURNING ARRAY[1.5, 2.5] AS xs, ARRAY[name] AS names",
        }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        ColumnSchemaDto xs = response.Columns!.Single(c => c.Name == "xs");
        ColumnSchemaDto names = response.Columns!.Single(c => c.Name == "names");

        Assert.AreEqual(ColumnType.Array, xs.Type);
        Assert.AreEqual(ColumnType.Float64, xs.ElementType);
        Assert.AreEqual(ColumnType.Array, names.Type);
        Assert.AreEqual(ColumnType.String, names.ElementType);
    }

    /// <summary>
    /// gRPC declares the same column type in its result schema. Its schema has no element-type field:
    /// each array value carries its own.
    /// </summary>
    [Test]
    public async Task Grpc_ArrayLiteralProjection_IsDeclaredAnArray()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, "SELECT ARRAY[1, 2] AS ints, ad FROM arrs", null), schemaOut: schemaHolder);
        await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        CamusDB.Grpc.ResultSchema schema = CamusDB.App.Grpc.CamusSqlService.BuildResultSchema(schemaHolder.Schema);

        Assert.AreEqual(CamusDB.Grpc.ColumnType.Array, schema.Columns.Single(c => c.Name == "ints").Type);
        Assert.AreEqual(CamusDB.Grpc.ColumnType.Array, schema.Columns.Single(c => c.Name == "ad").Type);
    }

    /// <summary>
    /// <c>/execute-sql-non-query</c> with a RETURNING list declares the element type too.
    /// </summary>
    [Test]
    public async Task NonQuery_Returning_DeclaresElementType()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        JsonResult result = await Controller(executor, new
        {
            databaseName = dbname,
            sql = "UPDATE arrs SET name = 'b' WHERE id = 1 RETURNING id, ad, ab, af, au, ai, name",
        }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.IsNotNull(response.Columns);
        AssertArrayColumns(response.Columns!);
    }

    /// <summary>
    /// <c>/execute-sql-query-stream</c> writes the columns in its header line: a table array column is
    /// typed from the schema, and a computed one from the first row, which the stream pulls before it
    /// writes the header.
    /// </summary>
    [Test]
    public async Task Stream_HeaderDeclaresElementType()
    {
        (string dbname, CommandExecutor executor) = await SetupArraysTable(withRow: true);

        ExecuteSQLController controller = Controller(executor, new
        {
            databaseName = dbname,
            sql = "SELECT ad, af, regexp_split_to_array(name, ',') AS xs, name FROM arrs",
        });

        await controller.ExecuteSQLQueryStream();

        Stream body = controller.HttpContext.Response.Body;
        body.Position = 0;
        string[] lines = new StreamReader(body).ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.GreaterOrEqual(lines.Length, 3, string.Join("\n", lines));

        using JsonDocument header = JsonDocument.Parse(lines[0]);
        JsonElement[] columns = header.RootElement.GetProperty("columns").EnumerateArray().ToArray();

        Assert.AreEqual(8, columns.Single(c => c.GetProperty("name").GetString() == "ad").GetProperty("elementType").GetInt32());
        Assert.AreEqual(5, columns.Single(c => c.GetProperty("name").GetString() == "af").GetProperty("elementType").GetInt32());
        Assert.AreEqual(3, columns.Single(c => c.GetProperty("name").GetString() == "xs").GetProperty("elementType").GetInt32());
        Assert.IsFalse(columns.Single(c => c.GetProperty("name").GetString() == "name").TryGetProperty("elementType", out _));

        // The peeked row is still written: one header, one row, one trailer.
        Assert.AreEqual(3, lines.Length, string.Join("\n", lines));
        StringAssert.StartsWith("[", lines[1]);
    }
}
