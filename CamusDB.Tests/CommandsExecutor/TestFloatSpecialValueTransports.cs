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
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Rows that hold ±infinity, NaN and -0, as a client reads them. JSON has no number for a non-finite
/// value, and the HTTP row writer threw in the middle of the response, so such a row was writable but
/// not readable over HTTP. Now the HTTP endpoints write the strings <c>"Infinity"</c>,
/// <c>"-Infinity"</c> and <c>"NaN"</c> in a float column, accept the same strings in a float
/// parameter, and keep -0 a number; gRPC carries the IEEE value in a <c>double</c> field. A float
/// overflow is HTTP 400 and gRPC <c>OUT_OF_RANGE</c> with <see cref="CamusDBErrorCodes.FloatValueOutOfRange"/>.
/// The controllers and the service run in-process, with no HTTP pipeline.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestFloatSpecialValueTransports : BaseTest
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    /// <summary>The (id, f, r) rows of <c>t</c>: ±infinity, NaN, -0 and a plain number.</summary>
    private async Task<(string db, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE t (id int64 PRIMARY KEY, f float64, r float32)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
            "INSERT INTO t (id, f, r) VALUES (1, 'Infinity', 'Infinity'), (2, '-Infinity', '-Infinity'), (3, 'NaN', 'NaN'), (4, -0.0, -0.0), (5, 1.5, 1.5)", null));
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

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    /// <summary>The f and r cells of the five rows, in id order, as the JSON tokens a client reads.</summary>
    private static void AssertSpecialCells(IReadOnlyList<JsonElement> rows, string what)
    {
        Assert.AreEqual(5, rows.Count, what);

        string[] expected = ["\"Infinity\"", "\"-Infinity\"", "\"NaN\"", "-0", "1.5"];
        for (int i = 0; i < 5; i++)
        {
            Assert.AreEqual(i + 1, rows[i][0].GetInt64(), $"{what}: id");
            Assert.AreEqual(expected[i], rows[i][1].GetRawText(), $"{what}: f of row {i + 1}");
            Assert.AreEqual(expected[i], rows[i][2].GetRawText(), $"{what}: r of row {i + 1}");
        }
    }

    [Test]
    public void ErrorCode_MapsToHttp400AndGrpcOutOfRange()
    {
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.FloatValueOutOfRange));
        Assert.AreEqual(StatusCode.OutOfRange, GrpcErrorMapper.GetGrpcStatus(CamusDBErrorCodes.FloatValueOutOfRange));
    }

    [Test]
    public async Task OverHttp_BufferedRows_CarryTheNames()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, "SELECT id, f, r FROM t ORDER BY id")).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);

        // The web defaults of MVC: the row writer must not depend on a named-literal option.
        string json = JsonSerializer.Serialize(response, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        using JsonDocument document = JsonDocument.Parse(json);
        AssertSpecialCells(document.RootElement.GetProperty("rows").EnumerateArray().ToList(), "buffered");
    }

    [Test]
    public async Task OverHttp_StreamedRows_CarryTheNames()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        ExecuteSQLController controller = Sql(executor, Body(db, "SELECT id, f, r FROM t ORDER BY id"));
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);

        List<JsonElement> rows = [];
        foreach (string line in body.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using JsonDocument document = JsonDocument.Parse(line);
            if (document.RootElement.ValueKind == JsonValueKind.Array)
                rows.Add(document.RootElement.Clone());
        }

        AssertSpecialCells(rows, "stream");
        StringAssert.Contains("\"status\":\"ok\"", body, "the stream ended with its trailer");
    }

    [Test]
    public async Task OverHttp_AParameterCarriesTheNames()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        // A float parameter as the HTTP JSON body spells it: JSON has no number for these values.
        string insert = """
            {"databaseName":"DB_NAME","sql":"INSERT INTO t (id, f, r) VALUES (6, @f, @r), (7, @n, @n32)",
             "parameters":{
               "@f":{"type":5,"floatValue":"-Infinity"},
               "@r":{"type":6,"floatValue":"Infinity"},
               "@n":{"type":5,"floatValue":"NaN"},
               "@n32":{"type":6,"floatValue":"NaN"}}}
            """.Replace("DB_NAME", db);
        JsonResult inserted = await Sql(executor, insert).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse insertResponse = (ExecuteNonSQLQueryResponse)inserted.Value!;
        Assert.AreEqual("ok", insertResponse.Status, insertResponse.Message);

        JsonResult result = await Sql(executor, Body(db, "SELECT id, f, r FROM t WHERE id >= 6 ORDER BY id")).ExecuteSQLQuery();
        string json = JsonSerializer.Serialize((ExecuteSQLQueryResponse)result.Value!, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        using JsonDocument document = JsonDocument.Parse(json);
        List<JsonElement> rows = document.RootElement.GetProperty("rows").EnumerateArray().ToList();

        Assert.AreEqual(2, rows.Count, json);
        Assert.AreEqual("\"-Infinity\"", rows[0][1].GetRawText());
        Assert.AreEqual("\"Infinity\"", rows[0][2].GetRawText());
        Assert.AreEqual("\"NaN\"", rows[1][1].GetRawText());
        Assert.AreEqual("\"NaN\"", rows[1][2].GetRawText());
    }

    [Test]
    public async Task OverHttp_AnOverflowIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        foreach ((string sql, bool query) in new[]
        {
            ("SELECT f * 1e308 * 2 AS x FROM t WHERE id = 5", false),
            ("SELECT 1e400 AS x", false),
            ("UPDATE t SET r = 1e300 WHERE id = 5", true),
        })
        {
            ExecuteSQLController controller = Sql(executor, Body(db, sql));
            JsonResult result = query ? await controller.ExecuteNonSQLQuery() : await controller.ExecuteSQLQuery();

            object value = result.Value!;
            string? code = (string?)value.GetType().GetProperty("Code")!.GetValue(value);
            string? message = (string?)value.GetType().GetProperty("Message")!.GetValue(value);
            Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, code, $"{sql}: {message}");
            Assert.AreEqual(400, result.StatusCode, $"{sql}: {message}");
        }
    }

    [Test]
    public async Task OverGrpc_RowsCarryTheIeeeValues_AndAnOverflowIsOutOfRange()
    {
        (string db, CommandExecutor executor) = await SetupAsync();
        CamusSqlService service = new(executor, new HttpTransactionCoordinator(executor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT id, f, r FROM t ORDER BY id" }, writer, new TestServerCallContext());

        List<ResultRow> rows = writer.Written.Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Select(m => m.Row).ToList();
        Assert.AreEqual(5, rows.Count);

        double[] expected = [double.PositiveInfinity, double.NegativeInfinity, double.NaN, -0.0, 1.5];
        for (int i = 0; i < 5; i++)
        {
            double f = rows[i].Values[1].Float64Value;
            float r = rows[i].Values[2].Float32Value;

            if (double.IsNaN(expected[i]))
            {
                Assert.IsTrue(double.IsNaN(f), $"f of row {i + 1}");
                Assert.IsTrue(float.IsNaN(r), $"r of row {i + 1}");
                continue;
            }

            // By bits, so -0 is not taken for 0.
            Assert.AreEqual(BitConverter.DoubleToInt64Bits(expected[i]), BitConverter.DoubleToInt64Bits(f), $"f of row {i + 1}");
            Assert.AreEqual(BitConverter.SingleToInt32Bits((float)expected[i]), BitConverter.SingleToInt32Bits(r), $"r of row {i + 1}");
        }

        // A NaN parameter in a predicate reaches the engine as NaN, and NaN = NaN.
        CapturingStreamWriter<QueryStreamMessage> nanWriter = new();
        SqlRequest byNaN = new() { Database = db, Sql = "SELECT id FROM t WHERE f = @n" };
        byNaN.Parameters.Add("@n", new Value { Float64Value = double.NaN });
        await service.ExecuteQuery(byNaN, nanWriter, new TestServerCallContext());
        Assert.AreEqual(new[] { 3L }, nanWriter.Written.Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row)
            .Select(m => m.Row.Values[0].Int64Value).ToArray());

        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await service.ExecuteQuery(
            new SqlRequest { Database = db, Sql = "SELECT pow(10, 400) AS x" },
            new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()))!;
        Assert.AreEqual(StatusCode.OutOfRange, exception.StatusCode, exception.Status.Detail);
        Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, exception.Trailers.GetValue("camus-error-code"));
    }
}
