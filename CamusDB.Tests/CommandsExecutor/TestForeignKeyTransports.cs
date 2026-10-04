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
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;
using Microsoft.Extensions.Logging;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Grpc;
using CamusDB.Tests.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Foreign keys through the transports a client uses: <c>/create-table</c> with a foreign key in the
/// request body, and each refusal through the SQL endpoints and the gRPC service, with its own code,
/// its HTTP status or gRPC status, and a message that names the constraint. The controllers and the
/// service are driven in-process, with no HTTP pipeline, so the assertions read the exact response
/// objects the controllers return.
/// </summary>
internal sealed class ForeignKeyTransportScenarios
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private readonly CommandExecutor executor;
    private readonly ILogger<ICamusDB> logger;
    private readonly CamusDBOptions options;
    private readonly HttpTransactionCoordinator coordinator;
    private readonly PreparedStatementRegistry registry;
    private readonly CamusSqlService service;

    /// <summary>Wraps the fixture's engine in the controllers' and the gRPC service's dependencies.</summary>
    public ForeignKeyTransportScenarios(CommandExecutor executor, ILogger<ICamusDB> logger, CamusDBOptions options)
    {
        this.executor = executor;
        this.logger = logger;
        this.options = options;
        coordinator = new(executor);
        registry = new(options);
        service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), options);
    }

    public async Task CreateTableOverHttpAddsTheForeignKey()
    {
        string db = await CreateCitiesAsync();

        JsonResult result = await CreateTable(db, "weather", new CreateTableForeignKey
        {
            Name = "weather_city_fk",
            Columns = ["city"],
            ReferencedTable = "cities",
            ReferencedColumns = ["name"],
            OnDelete = "restrict",
            OnUpdate = "no_action",
        });

        CreateTableResponse response = (CreateTableResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.IsNull(result.StatusCode);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        ForeignKeySchema constraint = database.Schema.Tables["weather"].ForeignKeys!.Single();
        Assert.AreEqual("weather_city_fk", constraint.Name);
        Assert.AreEqual(SchemaElementState.Public, constraint.State);
        Assert.AreEqual(ForeignKeyAction.Restrict, constraint.OnDelete);
        Assert.AreEqual(ForeignKeyAction.NoAction, constraint.OnUpdate);

        // The constraint the HTTP request declared is enforced.
        JsonResult orphan = await Sql(db, "INSERT INTO weather (id, city) VALUES (1, 'atlantis')").ExecuteNonSQLQuery();
        AssertFailed(orphan, 409, CamusDBErrorCodes.ForeignKeyViolation, "weather_city_fk");
    }

    public async Task CreateTableOverHttpRefusesInvalidDefinitionsWithTheirCodes()
    {
        string db = await CreateCitiesAsync();

        AssertFailed(await CreateTable(db, "t1", Fk(referencedTable: "elsewhere.cities")), 400, CamusDBErrorCodes.InvalidForeignKeyDefinition, "own database");
        AssertFailed(await CreateTable(db, "t2", Fk(referencedColumns: ["id"])), 400, CamusDBErrorCodes.InvalidForeignKeyDefinition, "t2_fk");
        AssertFailed(await CreateTable(db, "t3", Fk(onDelete: "cascade")), 501, CamusDBErrorCodes.FeatureNotSupported, "CASCADE");
        AssertFailed(await CreateTable(db, "t4", Fk(onDelete: "explode")), 400, CamusDBErrorCodes.InvalidInput, "explode");
        AssertFailed(await CreateTable(db, "t5", Fk(name: "")), 400, CamusDBErrorCodes.InvalidInput, "Foreign key name");

        CreateTableResponse missingParent = (CreateTableResponse)(await CreateTable(db, "t6", Fk(referencedTable: "nowhere"))).Value!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, missingParent.Code, missingParent.Message);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.IsFalse(database.Schema.Tables.Keys.Any(name => name.StartsWith('t') && name.Length == 2), "No refused table may exist");
    }

    public async Task EachViolationOverHttpHasItsOwnCodeAndStatus()
    {
        string db = await CreateCitiesAndWeatherAsync();

        AssertFailed(await Sql(db, "INSERT INTO weather (id, city) VALUES (9, 'atlantis')").ExecuteNonSQLQuery(),
            409, CamusDBErrorCodes.ForeignKeyViolation, "key (city)=(atlantis)");
        AssertFailed(await Sql(db, "DELETE FROM cities WHERE id = 1").ExecuteNonSQLQuery(),
            409, CamusDBErrorCodes.ForeignKeyRestrictDelete, "weather_city_fkey");
        AssertFailed(await Sql(db, "UPDATE cities SET name = 'lima2' WHERE id = 1").ExecuteNonSQLQuery(),
            409, CamusDBErrorCodes.ForeignKeyRestrictUpdate, "weather_city_fkey");

        JsonResult drop = await Sql(db, "DROP TABLE cities").ExecuteSQLDDL();
        ExecuteDDLSQLResponse dropResponse = (ExecuteDDLSQLResponse)drop.Value!;
        Assert.AreEqual(409, drop.StatusCode);
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, dropResponse.Code, dropResponse.Message);

        JsonResult cycle = await Sql(db,
            "ALTER TABLE cities ADD CONSTRAINT cities_back_fk FOREIGN KEY (name) REFERENCES weather (city)").ExecuteSQLDDL();
        ExecuteDDLSQLResponse cycleResponse = (ExecuteDDLSQLResponse)cycle.Value!;
        Assert.AreEqual(400, cycle.StatusCode, cycleResponse.Message);
    }

    public async Task EachViolationOverGrpcHasItsOwnCodeAndStatus()
    {
        string db = await CreateCitiesAndWeatherAsync();

        await AssertRpc(() => service.ExecuteNonQuery(Request(db, "INSERT INTO weather (id, city) VALUES (9, 'atlantis')"), new TestServerCallContext()),
            StatusCode.FailedPrecondition, CamusDBErrorCodes.ForeignKeyViolation);
        await AssertRpc(() => service.ExecuteNonQuery(Request(db, "DELETE FROM cities WHERE id = 1"), new TestServerCallContext()),
            StatusCode.FailedPrecondition, CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await AssertRpc(() => service.ExecuteNonQuery(Request(db, "UPDATE cities SET name = 'lima2' WHERE id = 1"), new TestServerCallContext()),
            StatusCode.FailedPrecondition, CamusDBErrorCodes.ForeignKeyRestrictUpdate);
        await AssertRpc(() => service.ExecuteDdl(Request(db, "DROP TABLE cities"), new TestServerCallContext()),
            StatusCode.FailedPrecondition, CamusDBErrorCodes.DependentObjectsExist);
        await AssertRpc(() => service.ExecuteDdl(Request(db,
                "CREATE TABLE visits (id int64 PRIMARY KEY NOT NULL, city int64 REFERENCES cities (name))"), new TestServerCallContext()),
            StatusCode.InvalidArgument, CamusDBErrorCodes.InvalidForeignKeyDefinition);
        await AssertRpc(() => service.ExecuteDdl(Request(db,
                "ALTER TABLE cities ADD CONSTRAINT cities_back_fk FOREIGN KEY (name) REFERENCES weather (city)"), new TestServerCallContext()),
            StatusCode.InvalidArgument, CamusDBErrorCodes.InvalidForeignKeyDefinition, CamusDBErrorCodes.ForeignKeyCycle);
    }

    /// <summary>
    /// Both tables over HTTP: the parent declares only a primary key, and the child references it
    /// with no column list, which means the parent's primary key.
    /// </summary>
    public async Task ParentAndChildOverHttpReferenceThePrimaryKey()
    {
        string db = "db" + Guid.NewGuid().ToString("n")[..12];
        await executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));

        CreateTableRequest parent = new()
        {
            DatabaseName = db,
            TableName = "regions",
            Columns = [new CreateTableColumn { Name = "code", Type = "string", NotNull = true, Primary = true }],
        };

        CreateTableResponse parentResponse = (CreateTableResponse)(await new CreateTableController(executor, coordinator, logger, options)
            { ControllerContext = Context(parent) }.CreateTable()).Value!;
        Assert.AreEqual("ok", parentResponse.Status, parentResponse.Message);

        CreateTableResponse childResponse = (CreateTableResponse)(await CreateTable(db, "stations",
            new CreateTableForeignKey { Name = "stations_region_fk", Columns = ["city"], ReferencedTable = "regions" })).Value!;
        Assert.AreEqual("ok", childResponse.Status, childResponse.Message);

        await ForeignKeyAlterScenarios.Dml(executor, db, "INSERT INTO regions (code) VALUES ('north')");
        await ForeignKeyAlterScenarios.Dml(executor, db, "INSERT INTO stations (id, city) VALUES (1, 'north')");

        AssertFailed(await Sql(db, "INSERT INTO stations (id, city) VALUES (2, 'south')").ExecuteNonSQLQuery(),
            409, CamusDBErrorCodes.ForeignKeyViolation, "stations_region_fk");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<string> CreateCitiesAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n")[..12];
        await executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));
        await ForeignKeyAlterScenarios.Ddl(executor, db,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await ForeignKeyAlterScenarios.Dml(executor, db, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        return db;
    }

    private async Task<string> CreateCitiesAndWeatherAsync()
    {
        string db = await CreateCitiesAsync();
        await ForeignKeyAlterScenarios.Ddl(executor, db,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");
        await ForeignKeyAlterScenarios.Dml(executor, db, "INSERT INTO weather (id, city) VALUES (1, 'lima')");
        return db;
    }

    private static CreateTableForeignKey Fk(
        string name = "fk", string referencedTable = "cities", string[]? referencedColumns = null, string? onDelete = null) => new()
    {
        Name = name == "fk" ? null : name,
        Columns = ["city"],
        ReferencedTable = referencedTable,
        ReferencedColumns = referencedColumns ?? ["name"],
        OnDelete = onDelete,
    };

    private Task<JsonResult> CreateTable(string db, string table, CreateTableForeignKey foreignKey)
    {
        foreignKey.Name ??= table + "_fk";

        CreateTableRequest request = new()
        {
            DatabaseName = db,
            TableName = table,
            Columns =
            [
                new CreateTableColumn { Name = "id", Type = "int64", NotNull = true, Primary = true },
                new CreateTableColumn { Name = "city", Type = "string" },
            ],
            ForeignKeys = [foreignKey],
        };

        return new CreateTableController(executor, coordinator, logger, options) { ControllerContext = Context(request) }.CreateTable();
    }

    private ExecuteSQLController Sql(string db, string sql) =>
        new(executor, coordinator, registry, logger, options) { ControllerContext = Context(new { databaseName = db, sql }) };

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, CamelCase));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private static SqlRequest Request(string db, string sql) => new() { Database = db, Sql = sql };

    /// <summary>Reads Status, Code and Message from any of the response DTOs, which share those names.</summary>
    private static void AssertFailed(JsonResult result, int status, string code, string messagePart)
    {
        object value = result.Value!;
        Type type = value.GetType();
        string? responseStatus = (string?)type.GetProperty("Status")!.GetValue(value);
        string? responseCode = (string?)type.GetProperty("Code")!.GetValue(value);
        string? message = (string?)type.GetProperty("Message")!.GetValue(value);

        Assert.AreEqual("failed", responseStatus, message);
        Assert.AreEqual(code, responseCode, message);
        Assert.AreEqual(status, result.StatusCode, message);
        Assert.That(message, Does.Contain(messagePart));
    }

    private static async Task AssertRpc<T>(Func<Task<T>> call, StatusCode status, params string[] codes)
    {
        RpcException exception = Assert.ThrowsAsync<RpcException>(async () => await call())!;
        Assert.AreEqual(status, exception.StatusCode, exception.Status.Detail);
        Assert.That(exception.Trailers.GetValue("camus-error-code"), Is.AnyOf(codes), exception.Status.Detail);
    }
}

/// <summary>Foreign keys through HTTP and gRPC on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyTransports : BaseTest
{
    [Test] public Task CreateTableOverHttpAddsTheForeignKey() => Host().CreateTableOverHttpAddsTheForeignKey();
    [Test] public Task CreateTableOverHttpRefusesInvalidDefinitionsWithTheirCodes() => Host().CreateTableOverHttpRefusesInvalidDefinitionsWithTheirCodes();
    [Test] public Task EachViolationOverHttpHasItsOwnCodeAndStatus() => Host().EachViolationOverHttpHasItsOwnCodeAndStatus();
    [Test] public Task EachViolationOverGrpcHasItsOwnCodeAndStatus() => Host().EachViolationOverGrpcHasItsOwnCodeAndStatus();
    [Test] public Task ParentAndChildOverHttpReferenceThePrimaryKey() => Host().ParentAndChildOverHttpReferenceThePrimaryKey();

    private ForeignKeyTransportScenarios Host() => new(CreateCommandExecutor(), logger, Options);
}

/// <summary>Foreign keys through HTTP and gRPC on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyTransportsCluster : SharedNodeBaseTest
{
    [Test] public Task CreateTableOverHttpAddsTheForeignKey() => Host().CreateTableOverHttpAddsTheForeignKey();
    [Test] public Task CreateTableOverHttpRefusesInvalidDefinitionsWithTheirCodes() => Host().CreateTableOverHttpRefusesInvalidDefinitionsWithTheirCodes();
    [Test] public Task EachViolationOverHttpHasItsOwnCodeAndStatus() => Host().EachViolationOverHttpHasItsOwnCodeAndStatus();
    [Test] public Task EachViolationOverGrpcHasItsOwnCodeAndStatus() => Host().EachViolationOverGrpcHasItsOwnCodeAndStatus();
    [Test] public Task ParentAndChildOverHttpReferenceThePrimaryKey() => Host().ParentAndChildOverHttpReferenceThePrimaryKey();

    private ForeignKeyTransportScenarios Host() => new(CreateCommandExecutor(), logger, Options);
}
