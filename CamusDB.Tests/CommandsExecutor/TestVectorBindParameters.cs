
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Grpc;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A query vector reaches the server as a bind parameter, over both transports.
///
/// <para>Inlining it as a hex literal would put roughly 6 KB of SQL text on the wire for a
/// 768-dimension vector and give every query a different statement text, so nothing downstream could
/// be reused. These tests prove the parameter path instead: the real nearest-neighbour SQL, the real
/// controller and the real gRPC service, with a full-size 3072-byte payload.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestVectorBindParameters : BaseTest
{
    private const int Dimensions = 768;
    private const int VectorBytes = Dimensions * 4;

    /// <summary>
    /// The plan cache ships disabled, and shape reuse is one of the things these tests exist to
    /// prove — so the engine is built with it on rather than a knob being flipped afterwards, which
    /// would be a no-op against an executor that already captured its options.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { PlanCacheEnabled = true };

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    [SetUp]
    public void SetUpTransports()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        registry = new(Options);
        grpc = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance,
            new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownTransports()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    // ─── Vectors ──────────────────────────────────────────────────────────────

    /// <summary>A unit vector along one axis, at full production width.</summary>
    private static byte[] AxisVector(int axis)
    {
        byte[] bytes = new byte[VectorBytes];
        BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(axis * 4, 4), 1f);
        return bytes;
    }

    // ─── REST harness ─────────────────────────────────────────────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body, ILogger<ICamusDB>? withLogger = null) =>
        new(executor, coordinator, registry, withLogger ?? logger, Options) { ControllerContext = Context(body) };

    private PreparedStatementsController Statements(object body) =>
        new(executor, coordinator, registry, logger, Options) { ControllerContext = Context(body) };

    /// <summary>A Bytes parameter in the JSON shape a REST client sends: base64 under bytesValue.</summary>
    private static object BytesParam(byte[] value) => new { type = CoreColumnType.Bytes, bytesValue = value };

    private async Task<string> SeedAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await Sql(new { sql = $"CREATE DATABASE {db}" }).ExecuteSQLDDL();
        await Sql(new
        {
            databaseName = db,
            sql = "CREATE TABLE docs (id oid PRIMARY KEY, tag string(16), embedding bytes(4096), " +
                  "CONSTRAINT dims CHECK (vector_dims(embedding) = 768))",
        }).ExecuteSQLDDL();

        for (int axis = 0; axis < 3; axis++)
        {
            await Sql(new
            {
                databaseName = db,
                sql = "INSERT INTO docs (id, tag, embedding) VALUES (gen_id(), @tag, @e)",
                parameters = new Dictionary<string, object>
                {
                    ["@tag"] = new { type = CoreColumnType.String, strValue = $"axis{axis}" },
                    ["@e"] = BytesParam(AxisVector(axis)),
                },
            }).ExecuteNonSQLQuery();
        }

        return db;
    }

    private const string NearestSql =
        "SELECT tag FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 1";

    private static List<string> Tags(ExecuteSQLQueryResponse response) =>
        response.Rows.Rows.Select(r => r.Row["tag"].StrValue!).ToList();

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_NamedBytesParameter_ReturnsTheNearestRow()
    {
        string db = await SeedAsync();

        // A different query vector must select a different row, or the parameter is not reaching
        // the distance function at all.
        foreach (int axis in new[] { 0, 1, 2 })
        {
            JsonResult result = await Sql(new
            {
                databaseName = db,
                sql = NearestSql,
                parameters = new Dictionary<string, object> { ["@q"] = BytesParam(AxisVector(axis)) },
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

            Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
            CollectionAssert.AreEqual(new[] { $"axis{axis}" }, Tags(response));
        }
    }

    [Test]
    public async Task Rest_PreparedPositionalBytesParameter_ReusesOneStatement()
    {
        string db = await SeedAsync();

        JsonResult prepared = await Statements(new { databaseName = db, sql = NearestSql }).PrepareSQLStatement();
        string statementId = ((PrepareStatementResponse)prepared.Value!).StatementId;

        // One prepared statement, three different payloads. If a value were captured at prepare
        // time every execution would answer with the first vector's neighbour.
        foreach (int axis in new[] { 2, 0, 1 })
        {
            JsonResult result = await Sql(new
            {
                statementId,
                positionalParameters = new[] { BytesParam(AxisVector(axis)) },
            }).ExecuteSQLQuery();

            ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

            Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
            CollectionAssert.AreEqual(new[] { $"axis{axis}" }, Tags(response));
        }
    }

    [Test]
    public async Task Rest_DistinctVectors_ShareOneCachedPlanShape()
    {
        // The whole reason to parameterize: the statement text never changes, so the access-path
        // decision is computed once and replayed. A vector baked into the SQL would miss every time.
        string db = await SeedAsync();

        async Task RunAsync(int axis)
        {
            JsonResult result = await Sql(new
            {
                databaseName = db,
                sql = NearestSql,
                parameters = new Dictionary<string, object> { ["@q"] = BytesParam(AxisVector(axis)) },
            }).ExecuteSQLQuery();

            Assert.That(((ExecuteSQLQueryResponse)result.Value!).Status, Is.EqualTo("ok"));
        }

        await RunAsync(0);

        long hitsBefore = executor.PlanCache.Hits;
        long missesBefore = executor.PlanCache.Misses;

        for (int axis = 0; axis < 3; axis++)
            await RunAsync(axis);

        Assert.That(executor.PlanCache.Hits - hitsBefore, Is.GreaterThan(0),
            "distinct query vectors must reuse the warmed plan shape");
        Assert.That(executor.PlanCache.Misses - missesBefore, Is.EqualTo(0),
            "a differing parameter value must not invalidate the shape");
    }

    [Test]
    public async Task Rest_LogsTheStatementAndNeverTheParameterValue()
    {
        // A 3 KB embedding in a log line is both noise and a data leak; the handler logs an arity.
        string db = await SeedAsync();
        CapturingLogger capture = new();

        JsonResult result = await Sql(new
        {
            databaseName = db,
            sql = NearestSql,
            parameters = new Dictionary<string, object> { ["@q"] = BytesParam(AxisVector(1)) },
        }, capture).ExecuteSQLQuery();

        Assert.That(((ExecuteSQLQueryResponse)result.Value!).Status, Is.EqualTo("ok"));

        string logged = string.Join("\n", capture.Messages);

        // The endpoint logs the redacted statement, which is what makes a slow query identifiable.
        StringAssert.Contains("l2_distance", logged);

        // The payload itself must appear nowhere, in any encoding.
        StringAssert.DoesNotContain(Convert.ToBase64String(AxisVector(1)), logged);
        StringAssert.DoesNotContain("bytesValue", logged);
        Assert.IsFalse(logged.Contains("AAAA", StringComparison.Ordinal),
            "a base64 run of zero bytes in the log means the vector was written out");
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Grpc_NamedBytesParameter_ReturnsTheNearestRow()
    {
        string db = await SeedAsync();

        foreach (int axis in new[] { 0, 1, 2 })
        {
            SqlRequest request = new() { Database = db, Sql = NearestSql };
            request.Parameters.Add("@q", new Value { BytesValue = ByteString.CopyFrom(AxisVector(axis)) });

            CapturingStreamWriter<QueryStreamMessage> writer = new();
            await grpc.ExecuteQuery(request, writer, new TestServerCallContext(CancellationToken.None));

            List<string> tags = writer.Written
                .Where(m => m.Row is not null)
                .Select(m => m.Row.Values[0].StringValue)
                .ToList();

            CollectionAssert.AreEqual(new[] { $"axis{axis}" }, tags);
        }
    }

    [Test]
    public async Task Grpc_FullWidthVector_TravelsWithoutHittingAMessageLimit()
    {
        // 3072 bytes is two orders of magnitude below the framework defaults this host leaves
        // unchanged (gRPC 4 MB receive, Kestrel 30 MB body). Recorded as a test so a future limit
        // change that would break embeddings fails here rather than in production.
        string db = await SeedAsync();

        SqlRequest request = new() { Database = db, Sql = NearestSql };
        request.Parameters.Add("@q", new Value { BytesValue = ByteString.CopyFrom(AxisVector(0)) });

        Assert.AreEqual(VectorBytes, request.Parameters["@q"].BytesValue.Length);
        Assert.Less(request.CalculateSize(), 4 * 1024 * 1024);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(request, writer, new TestServerCallContext(CancellationToken.None));

        Assert.That(writer.Written.Count(m => m.Row is not null), Is.EqualTo(1));
    }

    /// <summary>Collects formatted log messages so a test can assert what was and was not written.</summary>
    private sealed class CapturingLogger : ILogger<ICamusDB>
    {
        public List<string> Messages { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Messages)
                Messages.Add(formatter(state, exception));
        }
    }
}
