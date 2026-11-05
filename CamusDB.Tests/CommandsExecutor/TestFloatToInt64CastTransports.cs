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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A float cast to Integer64 as a client sees it: HTTP and gRPC carry the integer rounded half away
/// from zero, and a float with no integer in range is HTTP 400 and gRPC <c>OUT_OF_RANGE</c> with
/// <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>. A float bound to LIMIT is client input, so
/// it is HTTP 400 with <see cref="CamusDBErrorCodes.InvalidInput"/>, not a server fault. The
/// controllers and the service run in-process, with no HTTP pipeline.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestFloatToInt64CastTransports : BaseTest
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private const string TiesQuery = "SELECT id, CAST(f AS int64) AS a, CAST(r AS int64) AS b FROM t ORDER BY id";

    /// <summary>The integers that <see cref="TiesQuery"/> gives for rows 1 to 3.</summary>
    private static readonly long[] RoundedTies = [3L, -3L, 2L];

    /// <summary>Three ties in <c>t</c>, and one NaN in <c>bad</c>.</summary>
    private async Task<(string db, CommandExecutor executor)> SetupAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE t (id int64 PRIMARY KEY, f float64, r float32)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE bad (id int64 PRIMARY KEY, f float64)", null));
        await database.Transactions.CommitAsync(tx);

        tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, f, r) VALUES (1, 2.5, 2.5), (2, -2.5, -2.5), (3, 1.5, 1.5)", null));
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO bad (id, f) VALUES (1, 'NaN')", null));
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

    private CamusSqlService Grpc(CommandExecutor executor) => new(executor, new HttpTransactionCoordinator(executor), logger,
        TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);

    [Test]
    public async Task OverHttp_RowsCarryTheRoundedIntegers()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, TiesQuery)).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);

        string json = JsonSerializer.Serialize(response, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        using JsonDocument document = JsonDocument.Parse(json);
        List<JsonElement> rows = document.RootElement.GetProperty("rows").EnumerateArray().ToList();

        Assert.AreEqual(3, rows.Count, json);
        for (int i = 0; i < 3; i++)
        {
            Assert.AreEqual(RoundedTies[i], rows[i][1].GetInt64(), $"float64 of row {i + 1}");
            Assert.AreEqual(RoundedTies[i], rows[i][2].GetInt64(), $"float32 of row {i + 1}");
        }
    }

    [Test]
    public async Task OverHttp_NaNIs400WithItsCode()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        JsonResult result = await Sql(executor, Body(db, "SELECT CAST(f AS int64) AS x FROM bad")).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    [Test]
    public async Task OverHttp_AFloatLimitIs400WithInvalidInput()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        string body = """
            {"databaseName":"DB_NAME","sql":"SELECT id FROM t LIMIT @p","parameters":{"@p":{"type":5,"floatValue":1.5}}}
            """.Replace("DB_NAME", db);

        JsonResult result = await Sql(executor, body).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, response.Code, response.Message);
        Assert.AreEqual(400, result.StatusCode, response.Message);
    }

    [Test]
    public async Task OverGrpc_RowsCarryTheRoundedIntegers()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await Grpc(executor).ExecuteQuery(new SqlRequest { Database = db, Sql = TiesQuery }, writer, new TestServerCallContext());

        List<ResultRow> rows = writer.Written.Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Select(m => m.Row).ToList();

        Assert.AreEqual(3, rows.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.AreEqual(RoundedTies[i], rows[i].Values[1].Int64Value, $"float64 of row {i + 1}");
            Assert.AreEqual(RoundedTies[i], rows[i].Values[2].Int64Value, $"float32 of row {i + 1}");
        }
    }

    [Test]
    public async Task OverGrpc_ABoundFloatRoundsAndNaNIsOutOfRange()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        SqlRequest bound = new() { Database = db, Sql = "SELECT CAST(@p AS int64) AS x" };
        bound.Parameters.Add("@p", new Value { Float64Value = -2.5 });

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await Grpc(executor).ExecuteQuery(bound, writer, new TestServerCallContext());
        Assert.AreEqual(-3L, writer.Written.Single(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row).Row.Values[0].Int64Value);

        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await Grpc(executor).ExecuteQuery(
            new SqlRequest { Database = db, Sql = "SELECT CAST(f AS int64) AS x FROM bad" },
            new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()))!;

        Assert.AreEqual(StatusCode.OutOfRange, exception.StatusCode, exception.Status.Detail);
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, exception.Trailers.GetValue("camus-error-code"));
    }

    [Test]
    public async Task OverGrpc_AFloatLimitIsInvalidInput()
    {
        (string db, CommandExecutor executor) = await SetupAsync();

        SqlRequest request = new() { Database = db, Sql = "SELECT id FROM t LIMIT @p" };
        request.Parameters.Add("@p", new Value { Float64Value = 1.5 });

        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await Grpc(executor).ExecuteQuery(
            request, new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext()))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Trailers.GetValue("camus-error-code"), exception.Status.Detail);
    }
}
