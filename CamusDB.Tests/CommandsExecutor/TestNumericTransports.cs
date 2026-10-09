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

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using ProtoColType = CamusDB.Grpc.ColumnType;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// NUMERIC over the client transports: REST (named parameters in, positional JSON rows out) and gRPC
/// (<c>numeric_value</c> in both directions). Each test writes a value that a double cannot hold
/// through the transport, reads it back through the same transport, and filters on it with a
/// parameter, so a transport that rounds through a double, drops the type, or binds the parameter
/// as a string fails.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestNumericTransports : BaseTest
{
    /// <summary>20 integer digits and 9 fraction digits: a double keeps only about 17 of them.</summary>
    private const string Wide = "12345678901234567890.123456789";

    private const string Min = "-99999999999999999999999999999.999999999";

    private const string PastMax = "100000000000000000000000000000";

    private ILogger<ICamusDB> Logger => logger;

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

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
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body) };

    /// <summary>A REST parameter typed NUMERIC: the type number and the decimal text.</summary>
    private static object RestNumeric(string text) => new { type = (int)CoreColumnType.Numeric, strValue = text };

    private async Task<string> CreateDatabaseWithPricesAsync()
    {
        string db = "nt" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db,
            "CREATE TABLE prices (id oid NOT NULL DEFAULT(gen_id()), name string, p numeric, PRIMARY KEY (id))", null));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return db;
    }

    private async Task<List<string?>> ReadPricesAsync(string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT p FROM prices ORDER BY p", null));
            List<string?> values = [];
            await foreach (QueryResultRow row in cursor)
                values.Add(row.Row["p"].NumericValue);
            return values;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_NumericParameter_WritesExactly_AndReadsBackAsCanonicalText()
    {
        string db = await CreateDatabaseWithPricesAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO prices (name, p) VALUES ('wide', @a), ('min', @b), ('zeros', @c)",
            parameters = new Dictionary<string, object>
            {
                ["@a"] = RestNumeric(Wide),
                ["@b"] = RestNumeric(Min),
                ["@c"] = RestNumeric("2.500"),
            },
        }).ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse inserted = (ExecuteNonSQLQueryResponse)insert.Value!;
        Assert.AreEqual("ok", inserted.Status, inserted.Message);
        Assert.AreEqual(3, inserted.Rows);

        CollectionAssert.AreEqual(new[] { Min, "2.5", Wide }, await ReadPricesAsync(db), "stored exactly, canonical form");

        JsonResult query = await Sql(new
        {
            databaseName = db,
            sql = "SELECT name, p FROM prices WHERE p = @p",
            parameters = new Dictionary<string, object> { ["@p"] = RestNumeric(Wide) },
        }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)query.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total, "a NUMERIC parameter filters exactly");
        CollectionAssert.AreEqual(new[] { CoreColumnType.String, CoreColumnType.Numeric }, response.Columns.Select(c => c.Type));

        // On the wire the value is a JSON string, so a JavaScript client does not round it to a double.
        string json = JsonSerializer.Serialize(response, WireJson);
        StringAssert.Contains($"[\"wide\",\"{Wide}\"]", json);
    }

    [Test]
    public async Task Rest_NumericParameterPastTheRange_IsRefused_AndWritesNothing()
    {
        string db = await CreateDatabaseWithPricesAsync();

        JsonResult insert = await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO prices (name, p) VALUES ('big', @a)",
            parameters = new Dictionary<string, object> { ["@a"] = RestNumeric(PastMax) },
        }).ExecuteNonSQLQuery();

        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, ReadErrorCode(insert.Value!));
        Assert.AreEqual(0, (await ReadPricesAsync(db)).Count);
    }

    /// <summary>
    /// The error code of a REST reply. A request body the controller cannot bind is refused before it
    /// has a typed response, so the code may sit on either response shape.
    /// </summary>
    private static string? ReadErrorCode(object value)
    {
        using JsonDocument doc = JsonDocument.Parse(JsonSerializer.Serialize(value, value.GetType(), WireJson));
        return doc.RootElement.TryGetProperty("code", out JsonElement code) ? code.GetString() : null;
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Grpc_NumericValue_WritesExactly_AndReadsBackWithTheNumericType()
    {
        string db = await CreateDatabaseWithPricesAsync();

        SqlRequest insert = new() { Database = db, Sql = "INSERT INTO prices (name, p) VALUES ('wide', @a), ('min', @b)" };
        insert.Parameters.Add("@a", new Value { NumericValue = Wide });
        insert.Parameters.Add("@b", new Value { NumericValue = Min });
        NonQueryReply reply = await grpc.ExecuteNonQuery(insert, new TestServerCallContext());
        Assert.AreEqual(2, reply.AffectedRows);

        CollectionAssert.AreEqual(new[] { Min, Wide }, await ReadPricesAsync(db));

        SqlRequest select = new() { Database = db, Sql = "SELECT name, p FROM prices WHERE p >= @p ORDER BY p" };
        select.Parameters.Add("@p", new Value { NumericValue = Wide });
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(select, writer, new TestServerCallContext(CancellationToken.None));

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(writer.Written, 1, "name", "p");
        Assert.AreEqual(ProtoColType.Numeric, schema.Columns[1].Type);

        Dictionary<string, Value> row = GrpcAssert.RowValues(writer.Written, schema);
        Assert.AreEqual("wide", row["name"].StringValue);
        Assert.AreEqual(Value.KindOneofCase.NumericValue, row["p"].KindCase);
        Assert.AreEqual(Wide, row["p"].NumericValue);
    }

    [Test]
    public async Task Grpc_NumericComputedInTheQuery_IsSentAsNumeric()
    {
        string db = await CreateDatabaseWithPricesAsync();

        SqlRequest insert = new() { Database = db, Sql = "INSERT INTO prices (name, p) VALUES ('a', NUMERIC '0.1'), ('b', NUMERIC '0.2'), ('c', NULL)" };
        await grpc.ExecuteNonQuery(insert, new TestServerCallContext());

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT SUM(p) AS s, AVG(p) AS a, MAX(p) AS m FROM prices" },
            writer, new TestServerCallContext(CancellationToken.None));

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(writer.Written, 1, "s", "a", "m");
        Dictionary<string, Value> row = GrpcAssert.RowValues(writer.Written, schema);

        Assert.AreEqual("0.3", row["s"].NumericValue, "an exact NUMERIC sum, not 0.30000000000000004");
        Assert.AreEqual("0.15", row["a"].NumericValue);
        Assert.AreEqual("0.2", row["m"].NumericValue);
    }

    [Test]
    public async Task Grpc_NumericValuePastTheRange_IsOutOfRange()
    {
        string db = await CreateDatabaseWithPricesAsync();

        SqlRequest insert = new() { Database = db, Sql = "INSERT INTO prices (name, p) VALUES ('big', @a)" };
        insert.Parameters.Add("@a", new Value { NumericValue = PastMax });

        RpcException error = Assert.ThrowsAsync<RpcException>(async () =>
            await grpc.ExecuteNonQuery(insert, new TestServerCallContext()))!;

        Assert.AreEqual(StatusCode.OutOfRange, error.StatusCode);
        Assert.AreEqual(CamusDBErrorCodes.NumericValueOutOfRange, error.Trailers.GetValue("camus-error-code"));
        Assert.AreEqual(0, (await ReadPricesAsync(db)).Count);
    }
}
