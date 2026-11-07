/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using Google.Protobuf;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Half of a surrogate pair at each transport. Over HTTP the JSON reader refuses an escaped half, and
/// the request fails with <c>CADB0400</c> and the JSON path, not with an internal error; any other body
/// that does not parse gets the same answer. Over gRPC a protobuf string is UTF-8, so the client changes
/// half of a pair to U+FFFD before the server can see it; a call that reaches the service with the half
/// still in it is refused. The request bodies are written by hand, because a JSON serializer writes half
/// of a pair as U+FFFD before the request is sent.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestParameterHalfOfAPairTransports : BaseTest
{
    private const string Smile = "\U0001F642";

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    [SetUp]
    public void SetUpTransports()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options, sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
        grpc = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private ExecuteSQLController Sql(string body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(body);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new(executor, coordinator, registry, logger, Options) { ControllerContext = new ControllerContext { HttpContext = http } };
    }

    private async Task<string> CreateDatabaseOverHttp()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await Sql($"{{\"sql\":\"CREATE DATABASE {db}\"}}").ExecuteSQLDDL();
        await Sql($"{{\"databaseName\":\"{db}\",\"sql\":\"CREATE TABLE w (id INT64 PRIMARY KEY, s STRING)\"}}").ExecuteSQLDDL();
        return db;
    }

    /// <summary>A request with one string parameter <c>@p</c>, whose JSON text is <paramref name="escaped"/>.</summary>
    private static string Request(string db, string sql, string escaped) =>
        $"{{\"databaseName\":\"{db}\",\"sql\":\"{sql}\",\"parameters\":{{\"@p\":{{\"type\":{(int)CoreColumnType.String},\"strValue\":\"{escaped}\"}}}}}}";

    private const string BadParameter = "The request body is not valid JSON at $.parameters.@p.strValue";

    [TestCase("\\uD83D", TestName = "Http_TheFirstHalf_IsRefused")]
    [TestCase("\\uDE42", TestName = "Http_TheSecondHalf_IsRefused")]
    [TestCase("a\\uD83Db", TestName = "Http_AHalfBetweenCharacters_IsRefused")]
    public async Task Http_AnEscapedHalf_IsRefusedAsInvalidInput(string escaped)
    {
        string db = await CreateDatabaseOverHttp();

        ExecuteSQLQueryResponse query = (ExecuteSQLQueryResponse)(await Sql(Request(db, "SELECT @p AS x", escaped)).ExecuteSQLQuery()).Value!;
        Assert.That((query.Status, query.Code), Is.EqualTo(("failed", CamusDBErrorCodes.InvalidInput)));
        Assert.That(query.Message, Does.StartWith(BadParameter));

        ExecuteNonSQLQueryResponse write = (ExecuteNonSQLQueryResponse)(await Sql(Request(db, "INSERT INTO w (id, s) VALUES (1, @p)", escaped)).ExecuteNonSQLQuery()).Value!;
        Assert.That((write.Status, write.Code), Is.EqualTo(("failed", CamusDBErrorCodes.InvalidInput)));
        Assert.That(write.Message, Does.StartWith(BadParameter));

        ExecuteDDLSQLResponse ddl = (ExecuteDDLSQLResponse)(await Sql(Request(db, "CREATE TABLE c AS SELECT @p AS x", escaped)).ExecuteSQLDDL()).Value!;
        Assert.That((ddl.Status, ddl.Code), Is.EqualTo(("failed", CamusDBErrorCodes.InvalidInput)));
        Assert.That(ddl.Message, Does.StartWith(BadParameter));

        ExecuteSQLQueryResponse rows = (ExecuteSQLQueryResponse)(await Sql($"{{\"databaseName\":\"{db}\",\"sql\":\"SELECT id FROM w\"}}").ExecuteSQLQuery()).Value!;
        Assert.That(rows.Rows.Rows, Is.Empty);
    }

    [Test]
    public async Task Http_TheStream_RefusesAnEscapedHalfBeforeItsHeader()
    {
        string db = await CreateDatabaseOverHttp();
        ExecuteSQLController controller = Sql(Request(db, "SELECT @p AS x", "\\uD83D"));

        await controller.ExecuteSQLQueryStream();

        HttpResponse response = controller.ControllerContext.HttpContext.Response;
        response.Body.Position = 0;
        string body = await new StreamReader(response.Body).ReadToEndAsync();

        Assert.That(response.StatusCode, Is.EqualTo(400), body);
        Assert.That(body, Does.Contain(CamusDBErrorCodes.InvalidInput));
        Assert.That(body, Does.Contain(BadParameter));
    }

    [TestCase("{", TestName = "Http_AnUnfinishedBody_IsInvalidInput")]
    [TestCase("[1, 2]", TestName = "Http_AnArrayBody_IsInvalidInput")]
    [TestCase("{\"sql\": 5}", TestName = "Http_AWrongPropertyType_IsInvalidInput")]
    public async Task Http_ABodyThatDoesNotParse_IsInvalidInput(string body)
    {
        ExecuteSQLQueryResponse query = (ExecuteSQLQueryResponse)(await Sql(body).ExecuteSQLQuery()).Value!;

        Assert.That((query.Status, query.Code), Is.EqualTo(("failed", CamusDBErrorCodes.InvalidInput)));
        Assert.That(query.Message, Does.StartWith("The request body is not valid JSON"));
        Assert.That(query.Message, Does.Contain("(line 1, byte "));
    }

    [Test]
    public async Task Http_AWholePair_IsReadAndWritten()
    {
        string db = await CreateDatabaseOverHttp();

        ExecuteNonSQLQueryResponse write = (ExecuteNonSQLQueryResponse)(await Sql(Request(db, "INSERT INTO w (id, s) VALUES (1, @p)", "\\uD83D\\uDE42")).ExecuteNonSQLQuery()).Value!;
        Assert.That(write.Status, Is.EqualTo("ok"), write.Message);

        ExecuteSQLQueryResponse query = (ExecuteSQLQueryResponse)(await Sql(Request(db, "SELECT s FROM w WHERE s = @p", "\\uD83D\\uDE42")).ExecuteSQLQuery()).Value!;
        Assert.That(query.Status, Is.EqualTo("ok"), query.Message);
        Assert.That(query.Rows.Rows.Select(r => r.Row["s"].StrValue), Is.EqualTo(new[] { Smile }));
    }

    [Test]
    public async Task Grpc_ACallWithAHalf_IsRefusedAsInvalidArgument()
    {
        string db = await CreateDatabaseOverHttp();

        SqlRequest request = new() { Database = db, Sql = "SELECT @p AS x" };
        request.Parameters.Add("@p", new Value { StringValue = "\uD83D" });

        RpcException? error = Assert.ThrowsAsync<RpcException>(() =>
            grpc.ExecuteQuery(request, new CapturingStreamWriter<QueryStreamMessage>(), new TestServerCallContext(CancellationToken.None)));

        Assert.That(error!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
        Assert.That(error.Status.Detail, Does.Contain("Parameter '@p' holds half of a surrogate pair"));
    }

    [Test]
    public void Grpc_TheWireForm_ChangesAHalfToTheReplacementCharacter()
    {
        // This is the protobuf library, not CamusDB: a string field is UTF-8, so the client encodes half
        // of a pair as U+FFFD, and the server receives a valid character. The test records the measured
        // result that the gRPC protocol document states.
        SqlRequest request = new() { Database = "d", Sql = "SELECT @p" };
        request.Parameters.Add("@p", new Value { StringValue = "a\uD83Db" });
        request.Parameters.Add("@q", new Value { StringValue = Smile });

        SqlRequest received = SqlRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.That(received.Parameters["@p"].StringValue, Is.EqualTo("a�b"));
        Assert.That(received.Parameters["@q"].StringValue, Is.EqualTo(Smile));
    }
}
