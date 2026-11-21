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
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Tests.Grpc;
using ProtoQueryStreamMessage = CamusDB.Grpc.QueryStreamMessage;
using ProtoValue = CamusDB.Grpc.Value;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Transport-level coverage for <c>SHOW PARTITIONS</c>: the buffered and the streamed REST query
/// endpoints, the prepared-statement surface, and the gRPC query service.
///
/// <para>The statement is server-level: it opens no database and runs with no transaction. Each
/// transport carries its own routing decision for such a statement, and a statement missing from
/// that decision falls through to the autocommit path, which fails on an empty database name or on
/// the null descriptor. Engine tests cannot see that failure, so these tests drive the
/// transports.</para>
///
/// <para>Many cells of a standalone row are NULL (the follower lag, the hash range of the system
/// partition), so the tests also check that NULL cells survive each wire encoder.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowPartitionsTransports : BaseTest
{
    private const int DataPartitions = 2;

    protected override int NodeInitialPartitions => DataPartitions;

    private static readonly string[] ExpectedColumns =
    [
        "node", "partition_id", "kind", "state", "routing", "hash_start", "hash_end", "generation",
        "leader", "leader_source", "leader_is_local", "hosted_locally", "term", "commit_index",
        "replication_factor", "voters", "learners", "removing", "awaiting_placement",
        "log_ops_per_sec", "wal_queue_depth", "commit_wait_ms", "max_follower_lag", "stalled_followers",
    ];

    private ILogger<ICamusDB> Logger => logger;
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

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body) };

    private PreparedStatementsController Statements(object body) =>
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body) };

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    private async Task<string> CreateDatabaseAsync()
    {
        string db = "sp" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);
        return db;
    }

    private static void AssertPartitionRows(ExecuteSQLQueryResponse response)
    {
        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(ExpectedColumns, response.Columns.Select(c => c.Name).ToArray(),
            "clients decode rows positionally, so the declared schema is the contract");

        Assert.AreEqual(DataPartitions + 1, response.Total);
        CollectionAssert.AreEqual(
            Enumerable.Range(0, DataPartitions + 1).Select(i => (long)i).ToArray(),
            response.Rows.Rows.Select(r => r.Row["partition_id"].LongValue).ToArray());

        // Serializing is where a NULL cell that does not encode would surface.
        string json = JsonSerializer.Serialize(response);
        StringAssert.Contains("null", json, "the follower lag and the system partition's hash range are NULL");
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    /// <summary>The buffered endpoint with no database in scope, the usual way to ask.</summary>
    [Test]
    public async Task RestQueryEndpointAnswersWithNoDatabaseInScope()
    {
        JsonResult result = await Sql(new { sql = "SHOW PARTITIONS" }).ExecuteSQLQuery();

        AssertPartitionRows((ExecuteSQLQueryResponse)result.Value!);
    }

    /// <summary>
    /// The buffered endpoint with a database named. The statement must still skip the autocommit
    /// transaction: a commit on the descriptor it never opened is the failure this guards.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointAnswersWithADatabaseInScope()
    {
        string db = await CreateDatabaseAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = "SHOW PARTITIONS" }).ExecuteSQLQuery();

        AssertPartitionRows((ExecuteSQLQueryResponse)result.Value!);
    }

    /// <summary>The streaming endpoint carries its own copy of the routing decision.</summary>
    [Test]
    public async Task RestStreamingEndpointStreamsPartitionRows()
    {
        ExecuteSQLController controller = Sql(new { sql = "SHOW PARTITIONS" });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("\"max_follower_lag\"", body, "the schema header must precede the rows");
        StringAssert.Contains("\"system\"", body, "the system partition row");
        StringAssert.Contains("\"status\":\"ok\"", body, "the trailer reports the terminal status");
    }

    // ─── Prepared statements ──────────────────────────────────────────────────

    /// <summary>
    /// Both transports register statements through the same preparable set, and a prepared
    /// server-level statement runs from the root node type stored at registration.
    /// </summary>
    [Test]
    public async Task PreparedStatementRunsWithoutADatabase()
    {
        Assert.IsTrue(PreparedStatementBinder.IsPreparable(NodeType.ShowPartitions));

        PreparedStatement direct = PreparedStatementBinder.Create("", "SHOW PARTITIONS");
        Assert.AreEqual(NodeType.ShowPartitions, direct.RootNodeType);
        Assert.AreEqual(0, direct.ParameterCount);

        JsonResult prepareResult = await Statements(new { databaseName = "", sql = "SHOW PARTITIONS" }).PrepareSQLStatement();
        PrepareStatementResponse prepared = (PrepareStatementResponse)prepareResult.Value!;
        Assert.AreEqual("ok", prepared.Status, prepared.Message);

        JsonResult result = await Sql(new { statementId = prepared.StatementId }).ExecuteSQLQuery();

        AssertPartitionRows((ExecuteSQLQueryResponse)result.Value!);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The real gRPC query service: its routing decision, the schema message and the positional rows.
    /// A NULL cell must encode as an explicit null value, or a client reads the wrong column.
    /// </summary>
    [Test]
    public async Task GrpcQueryStreamsSchemaAndNullCells()
    {
        CapturingStreamWriter<ProtoQueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(
            new SqlRequest { Database = "", Sql = "SHOW PARTITIONS" },
            writer,
            new TestServerCallContext(CancellationToken.None));

        List<ProtoQueryStreamMessage> schemas = writer.Written.Where(m => m.Schema is not null).ToList();
        Assert.AreEqual(1, schemas.Count, "exactly one schema message precedes the rows");
        Assert.AreEqual(ExpectedColumns, schemas[0].Schema.Columns.Select(c => c.Name).ToArray());

        List<ProtoQueryStreamMessage> rows = writer.Written.Where(m => m.Row is not null).ToList();
        Assert.AreEqual(DataPartitions + 1, rows.Count);

        int partitionIdIndex = Array.IndexOf(ExpectedColumns, "partition_id");
        int kindIndex = Array.IndexOf(ExpectedColumns, "kind");
        int hashStartIndex = Array.IndexOf(ExpectedColumns, "hash_start");
        int lagIndex = Array.IndexOf(ExpectedColumns, "max_follower_lag");
        int leaderIsLocalIndex = Array.IndexOf(ExpectedColumns, "leader_is_local");

        ProtoQueryStreamMessage system = rows[0];
        Assert.AreEqual(ExpectedColumns.Length, system.Row.Values.Count,
            "a positional row carries one value for each declared column, NULLs included");
        Assert.AreEqual(0, system.Row.Values[partitionIdIndex].Int64Value);
        Assert.AreEqual("system", system.Row.Values[kindIndex].StringValue);
        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, system.Row.Values[hashStartIndex].KindCase);
        Assert.AreEqual(ProtoValue.KindOneofCase.NullValue, system.Row.Values[lagIndex].KindCase);
        Assert.IsTrue(system.Row.Values[leaderIsLocalIndex].BoolValue);

        ProtoQueryStreamMessage data = rows[1];
        Assert.AreEqual(1, data.Row.Values[partitionIdIndex].Int64Value);
        Assert.AreEqual("data", data.Row.Values[kindIndex].StringValue);
        Assert.AreEqual(ProtoValue.KindOneofCase.Int64Value, data.Row.Values[hashStartIndex].KindCase);
    }
}
