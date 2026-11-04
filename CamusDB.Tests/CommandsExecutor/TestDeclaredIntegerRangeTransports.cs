/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The declared integer range and the FLOAT32 precision as a client sees them. A value out of range is
/// HTTP 400 and gRPC <c>OUT_OF_RANGE</c> with <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>,
/// on the SQL endpoints and on the typed row endpoints. An HTTP create-table request reads the same type
/// names as SQL. A <c>REAL</c> value and a <c>DOUBLE PRECISION</c> value keep their distinct precision on
/// both wire encodings. The controllers and the service run in-process, with no HTTP pipeline.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestDeclaredIntegerRangeTransports : BaseTest
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private async Task<(string db, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint, i int, r real, d double precision)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, s, i, r, d) VALUES (1, 32767, 2147483647, 16777217, 16777217)", null));
        await database.Transactions.CommitAsync(tx);

        return (db, executor);
    }

    private ExecuteSQLController Sql(CommandExecutor executor, string json) =>
        new(executor, new HttpTransactionCoordinator(executor), new PreparedStatementRegistry(Options), logger, Options)
        {
            ControllerContext = Context(json),
        };

    private static ControllerContext Context(string json)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(json);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private static string Body(string db, string sql) => JsonSerializer.Serialize(new { databaseName = db, sql }, CamelCase);

    private static string Int64(long value) => $"{{\"type\":{(int)CamusDB.Core.Catalogs.Models.ColumnType.Integer64},\"longValue\":{value}}}";

    private CamusSqlService Grpc(CommandExecutor executor) => new(executor, new HttpTransactionCoordinator(executor), logger,
        TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

    [Test]
    public async Task OverHttp_SqlWriteIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, "UPDATE t SET s = 32768 WHERE id = 1")).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    [Test]
    public async Task OverHttp_CastIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, "SELECT CAST(70000 AS smallint) AS x")).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    [Test]
    public async Task OverHttp_TypedInsertIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        string body = $"{{\"databaseName\":\"{db}\",\"tableName\":\"t\",\"values\":{{\"id\":{Int64(2)},\"s\":{Int64(32768)}}}}}";

        JsonResult result = await new InsertController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = Context(body) }.Insert();
        InsertResponse response = (InsertResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    [Test]
    public async Task OverHttp_TypedUpdateIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        string body = $"{{\"databaseName\":\"{db}\",\"tableName\":\"t\",\"values\":{{\"i\":{Int64(2147483648)}}}," +
            $"\"filters\":[{{\"columnName\":\"id\",\"op\":\"=\",\"value\":{Int64(1)}}}]}}";

        JsonResult result = await new UpdateController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = Context(body) }.Update();
        UpdateResponse response = (UpdateResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    /// <summary>The HTTP create-table request has its own table of type names, which must follow the same rule.</summary>
    [TestCase("smallint", "SMALLINT", 32768L, true)]
    [TestCase("int", "INT", 2147483648L, true)]
    [TestCase("integer", "INT", 2147483648L, true)]
    [TestCase("bigint", "INT64", 2147483648L, false)]
    [TestCase("int64", "INT64", 2147483648L, false)]
    public async Task OverHttp_CreateTableReadsTheSameTypeNames(string typeName, string rendered, long value, bool refused)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableRequest request = new()
        {
            DatabaseName = db,
            TableName = "h",
            Columns =
            [
                new CreateTableColumn { Name = "id", Type = "int64", NotNull = true, Primary = true },
                new CreateTableColumn { Name = "v", Type = typeName },
            ],
        };

        CreateTableResponse created = (CreateTableResponse)(await new CreateTableController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = Context(JsonSerializer.Serialize(request, CamelCase)) }.CreateTable()).Value!;
        Assert.AreEqual("ok", created.Status, created.Message);

        JsonResult result = await Sql(executor, Body(db, $"INSERT INTO h (id, v) VALUES (1, {value})")).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        if (refused)
            Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        else
            Assert.AreEqual("ok", response.Status, response.Message);

        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SHOW COLUMNS FROM h", null));
        List<QueryResultRow> columns = await cursor.ToListAsync();
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(rendered, columns.Single(c => c.Row["Field"].StrValue == "v").Row["Type"].StrValue);
    }

    [Test]
    public async Task OverHttp_CreateTableAppliesTheWidthToAnArrayElement()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CreateTableRequest request = new()
        {
            DatabaseName = db,
            TableName = "h",
            Columns =
            [
                new CreateTableColumn { Name = "id", Type = "int64", NotNull = true, Primary = true },
                new CreateTableColumn { Name = "v", Type = "array", ArrayElementType = "smallint" },
            ],
        };

        CreateTableResponse created = (CreateTableResponse)(await new CreateTableController(executor, new HttpTransactionCoordinator(executor), logger, Options)
            { ControllerContext = Context(JsonSerializer.Serialize(request, CamelCase)) }.CreateTable()).Value!;
        Assert.AreEqual("ok", created.Status, created.Message);

        JsonResult result = await Sql(executor, Body(db, "INSERT INTO h (id, v) VALUES (1, ARRAY[32768])")).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
    }

    /// <summary>A narrow integer is an int64 on the wire, and a REAL keeps the value it stored.</summary>
    [Test]
    public async Task OverHttp_RowsCarryTheStoredValues()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, "SELECT s, i, r, d FROM t")).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);

        string json = JsonSerializer.Serialize(response, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement row = document.RootElement.GetProperty("rows").EnumerateArray().Single();

        Assert.AreEqual(32767L, row[0].GetInt64(), json);
        Assert.AreEqual(2147483647L, row[1].GetInt64(), json);
        Assert.AreEqual(16777216.0, row[2].GetDouble(), json);
        Assert.AreEqual(16777217.0, row[3].GetDouble(), json);
    }

    [Test]
    public async Task OverGrpc_RowsCarryTheStoredValuesAndTheirTypes()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await Grpc(executor).ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT s, i, r, d FROM t" }, writer, new TestServerCallContext());

        ResultRow row = writer.Written.Single(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Row;

        Assert.AreEqual(32767L, row.Values[0].Int64Value);
        Assert.AreEqual(2147483647L, row.Values[1].Int64Value);
        Assert.AreEqual(Value.KindOneofCase.Float32Value, row.Values[2].KindCase);
        Assert.AreEqual(16777216f, row.Values[2].Float32Value);
        Assert.AreEqual(Value.KindOneofCase.Float64Value, row.Values[3].KindCase);
        Assert.AreEqual(16777217.0, row.Values[3].Float64Value);
    }

    [Test]
    public async Task OverGrpc_WriteAndCastAreOutOfRange()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        RpcException write = Assert.ThrowsAsync<RpcException>(async () => await Grpc(executor).ExecuteNonQuery(
            new SqlRequest { Database = db, Sql = "UPDATE t SET s = 32768 WHERE id = 1" }, new TestServerCallContext()))!;
        Assert.AreEqual(StatusCode.OutOfRange, write.StatusCode, write.Status.Detail);
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, write.Trailers.GetValue("camus-error-code"));

        RpcException cast = Assert.ThrowsAsync<RpcException>(async () => await Grpc(executor).ExecuteQuery(
            new SqlRequest { Database = db, Sql = "SELECT CAST(70000 AS smallint) AS x" },
            new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()))!;
        Assert.AreEqual(StatusCode.OutOfRange, cast.StatusCode, cast.Status.Detail);
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, cast.Trailers.GetValue("camus-error-code"));
    }

    [Test]
    public async Task OverGrpc_BoundParameterIsJudgedByTheColumn()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        SqlRequest request = new() { Database = db, Sql = "UPDATE t SET i = @p WHERE id = 1" };
        request.Parameters.Add("@p", new Value { Int64Value = 2147483648 });

        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await Grpc(executor).ExecuteNonQuery(request, new TestServerCallContext()))!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, exception.Trailers.GetValue("camus-error-code"));
    }
}
