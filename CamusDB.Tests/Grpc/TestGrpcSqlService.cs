
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Transactions;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Integration tests for <see cref="CamusSqlService"/>. The service is instantiated with a
/// real embedded Kahuna node (inherited from <see cref="BaseTest"/>) and all RPC methods are
/// invoked directly — no real gRPC transport is needed. The <see cref="TestServerCallContext"/>
/// and <see cref="CapturingStreamWriter{T}"/> stubs provide the gRPC surface.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestGrpcSqlService : BaseTest
{
    private CamusSqlService service = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpGrpcService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(serviceExecutor);
        service = new(serviceExecutor, coordinator, logger);
    }

    [TearDown]
    public async Task TearDownGrpcService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static TestServerCallContext Ctx(CancellationToken ct = default) =>
        new(ct);

    private static SqlRequest DdlRequest(string db, string sql) =>
        new() { Database = db, Sql = sql };

    private static SqlRequest NonQueryRequest(string db, string sql) =>
        new() { Database = db, Sql = sql };

    private static SqlRequest QueryRequest(string db, string sql) =>
        new() { Database = db, Sql = sql };

    private async Task<string> CreateTestDatabaseAsync()
    {
        // Prefix with "db" so the name always starts with a letter and parses as an identifier.
        string dbName = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(DdlRequest("", $"CREATE DATABASE {dbName}"), Ctx());
        return dbName;
    }

    private async Task<string> CreateTestDatabaseWithTableAsync(string createTableSql)
    {
        string dbName = await CreateTestDatabaseAsync();
        await service.ExecuteDdl(DdlRequest(dbName, createTableSql), Ctx());
        return dbName;
    }

    private async Task<(List<QueryStreamMessage> messages, TestServerCallContext ctx)>
        QueryAsync(string db, string sql)
    {
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext ctx = Ctx();
        await service.ExecuteQuery(QueryRequest(db, sql), writer, ctx);
        return (writer.Written, ctx);
    }

    // ─── Ping ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Ping_ReturnsOk()
    {
        PingReply reply = await service.Ping(new PingRequest(), Ctx());
        Assert.That(reply.Message, Is.EqualTo("pong"));
    }

    // ─── DDL ──────────────────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteDdl_CreateDatabase_Succeeds()
    {
        string dbName = "db" + Guid.NewGuid().ToString("n");
        DdlReply reply = await service.ExecuteDdl(
            DdlRequest("", $"CREATE DATABASE {dbName}"), Ctx());

        Assert.That(reply, Is.Not.Null);
    }

    [Test]
    public async Task ExecuteDdl_CreateTable_Succeeds()
    {
        string dbName = await CreateTestDatabaseAsync();
        DdlReply reply = await service.ExecuteDdl(
            DdlRequest(dbName, "CREATE TABLE items (id oid PRIMARY KEY, name string(100))"), Ctx());

        Assert.That(reply, Is.Not.Null);
    }

    [Test]
    public void ExecuteDdl_BadSql_ThrowsInvalidArgument()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ExecuteDdl(DdlRequest("any", "NOT VALID SQL !!!"), Ctx()))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
        Assert.That(ex.Trailers.GetValue("camus-error-code"), Is.Not.Null);
    }

    [Test]
    public async Task ExecuteDdl_DropDatabase_Succeeds()
    {
        string dbName = await CreateTestDatabaseAsync();
        DdlReply reply = await service.ExecuteDdl(
            DdlRequest("", $"DROP DATABASE {dbName}"), Ctx());

        Assert.That(reply, Is.Not.Null);
    }

    // ─── NonQuery (DML) ───────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteNonQuery_Insert_ReturnsAffectedRows()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        NonQueryReply reply = await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'alpha')"), Ctx());

        Assert.That(reply.AffectedRows, Is.EqualTo(1));
    }

    [Test]
    public async Task ExecuteNonQuery_MultipleInserts_ReturnsCorrectCount()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        for (int i = 0; i < 5; i++)
            await service.ExecuteNonQuery(
                NonQueryRequest(dbName, $"INSERT INTO items (id, name) VALUES (gen_id(), 'item{i}')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id, name FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 5, "id", "name");
    }

    [Test]
    public void ExecuteNonQuery_UnknownDatabase_ThrowsNotFound()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ExecuteNonQuery(
                NonQueryRequest("nosuchdb", "INSERT INTO t (id) VALUES (1)"), Ctx()))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    // ─── ExecuteQuery — streaming invariants ──────────────────────────────────

    [Test]
    public async Task ExecuteQuery_EmptyTable_SchemaFirstEvenWithNoRows()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id, name FROM items");

        // Schema must be the first and only message for an empty table.
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 0, "id", "name");
    }

    [Test]
    public async Task ExecuteQuery_WithRows_SchemaFirstThenRows()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'foo')"), Ctx());
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'bar')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id, name FROM items");

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 2, "id", "name");
        Assert.That(schema.Columns.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task ExecuteQuery_SchemaColumnsMatchTableDefinition()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE things (id oid PRIMARY KEY, qty int64, price float64, active bool)");

        (List<QueryStreamMessage> msgs, _) =
            await QueryAsync(dbName, "SELECT id, qty, price, active FROM things");

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, 0, "id", "qty", "price", "active");
        Assert.That(schema.Columns[0].Type, Is.EqualTo(ColumnType.Id));
        Assert.That(schema.Columns[1].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(schema.Columns[2].Type, Is.EqualTo(ColumnType.Float64));
        Assert.That(schema.Columns[3].Type, Is.EqualTo(ColumnType.Bool));
    }

    [Test]
    public async Task ExecuteQuery_CancelMidStream_StreamTerminates()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        for (int i = 0; i < 10; i++)
            await service.ExecuteNonQuery(
                NonQueryRequest(dbName, $"INSERT INTO items (id, name) VALUES (gen_id(), 'item{i}')"), Ctx());

        using CancellationTokenSource cts = new();

        // Sink that cancels the token after writing the schema message.
        CancelAfterSchemaWriter cancelWriter = new(cts);

        TestServerCallContext ctx = Ctx(cts.Token);

        // The service should surface the cancellation (OperationCanceledException or RpcException).
        try
        {
            await service.ExecuteQuery(QueryRequest(dbName, "SELECT id, name FROM items"), cancelWriter, ctx);
        }
        catch (OperationCanceledException) { /* expected */ }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { /* expected */ }

        // At most the schema + one row should have been written before cancel fired.
        Assert.That(cancelWriter.Written.Count, Is.LessThanOrEqualTo(2));
    }

    [Test]
    public void ExecuteQuery_UnknownDatabase_ThrowsNotFound()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            CapturingStreamWriter<QueryStreamMessage> writer = new();
            await service.ExecuteQuery(QueryRequest("nosuchdb", "SELECT 1"), writer, Ctx());
        })!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    // ─── SHOW DATABASES ───────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteQuery_ShowDatabases_ReturnsSchemaAndRows()
    {
        // Create at least one database so SHOW DATABASES has something to return.
        await CreateTestDatabaseAsync();

        (List<QueryStreamMessage> msgs, _) = await QueryAsync("", "SHOW DATABASES");

        // Schema must be first; must have at least one row.
        Assert.That(msgs.Count, Is.GreaterThanOrEqualTo(2));
        Assert.That(msgs[0].PayloadCase,
            Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        Assert.That(msgs.Skip(1).All(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row),
            Is.True);
    }

    // ─── Transaction lifecycle ────────────────────────────────────────────────

    [Test]
    public async Task Transaction_InsertCommit_RowVisible()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        // Start explicit transaction.
        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        // Insert inside the transaction.
        SqlRequest insertReq = new()
        {
            Database  = dbName,
            Sql       = "INSERT INTO items (id, name) VALUES (gen_id(), 'txn-item')",
            TxnHandle = handle,
        };
        NonQueryReply nonQReply = await service.ExecuteNonQuery(insertReq, Ctx());
        Assert.That(nonQReply.AffectedRows, Is.EqualTo(1));

        // Commit.
        CommitReply commitReply = await service.CommitTransaction(handle, Ctx());
        Assert.That(commitReply, Is.Not.Null);

        // Autocommit SELECT should see the inserted row.
        (List<QueryStreamMessage> msgs, _) =
            await QueryAsync(dbName, "SELECT name FROM items WHERE name = 'txn-item'");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1, "name");
    }

    [Test]
    public async Task Transaction_Rollback_RowNotVisible()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        SqlRequest insertReq = new()
        {
            Database  = dbName,
            Sql       = "INSERT INTO items (id, name) VALUES (gen_id(), 'rolled-back')",
            TxnHandle = handle,
        };
        await service.ExecuteNonQuery(insertReq, Ctx());

        await service.RollbackTransaction(handle, Ctx());

        (List<QueryStreamMessage> msgs, _) =
            await QueryAsync(dbName, "SELECT name FROM items WHERE name = 'rolled-back'");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 0);
    }

    [Test]
    public async Task CommitTransaction_Duplicate_ThrowsFailedPrecondition()
    {
        string dbName = await CreateTestDatabaseAsync();

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        // First commit — should succeed.
        await service.CommitTransaction(handle, Ctx());

        // Second commit on the same handle — should be rejected.
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.CommitTransaction(handle, Ctx()))!;

        Assert.That(ex.StatusCode,
            Is.EqualTo(StatusCode.FailedPrecondition).Or.EqualTo(StatusCode.InvalidArgument));
    }

    [Test]
    public async Task Transaction_ExplicitTxnQuery_ResultVisible()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, val int64)");

        // Insert autocommit so data is committed before the read txn.
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, val) VALUES (gen_id(), 42)"), Ctx());

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        SqlRequest qReq = new()
        {
            Database  = dbName,
            Sql       = "SELECT val FROM items",
            TxnHandle = handle,
        };
        await service.ExecuteQuery(qReq, writer, Ctx());
        await service.CommitTransaction(handle, Ctx());

        GrpcAssert.AssertSchemaFirst(writer.Written, expectedRowCount: 1, "val");
        Dictionary<string, Value> row = GrpcAssert.RowValues(writer.Written, writer.Written[0].Schema);
        Assert.That(row["val"].Int64Value, Is.EqualTo(42));
    }

    // ─── Error mapping ────────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteNonQuery_DuplicatePrimaryKey_ThrowsAlreadyExists()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        // Insert once with a known OID value.
        string fixedId = "aabbccdd" + "aabbccdd" + "aabbccdd";  // 24 hex
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, $"INSERT INTO items (id, name) VALUES ('{fixedId}', 'first')"), Ctx());

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ExecuteNonQuery(
                NonQueryRequest(dbName, $"INSERT INTO items (id, name) VALUES ('{fixedId}', 'second')"), Ctx()))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.AlreadyExists));
        Assert.That(ex.Trailers.GetValue("camus-error-code"),
            Is.EqualTo(CamusDBErrorCodes.DuplicatePrimaryKey)
              .Or.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }

    [Test]
    public void ExecuteQuery_BadSql_ThrowsInvalidArgument()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            CapturingStreamWriter<QueryStreamMessage> writer = new();
            await service.ExecuteQuery(QueryRequest("any", "SELEKT * FRUM nowhere"), writer, Ctx());
        })!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
    }

    // ─── Value round-trips ────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteQuery_IntegerValue_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE nums (id oid PRIMARY KEY, n int64)");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO nums (id, n) VALUES (gen_id(), 12345)"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT n FROM nums");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "n");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);
        Assert.That(row["n"].Int64Value, Is.EqualTo(12345));
    }

    [Test]
    public async Task ExecuteQuery_StringValue_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE strs (id oid PRIMARY KEY, s string(200))");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO strs (id, s) VALUES (gen_id(), 'hello world')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT s FROM strs");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "s");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);
        Assert.That(row["s"].StringValue, Is.EqualTo("hello world"));
    }

    [Test]
    public async Task ExecuteQuery_BoolValue_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE flags (id oid PRIMARY KEY, active bool)");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO flags (id, active) VALUES (gen_id(), true)"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT active FROM flags");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "active");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);
        Assert.That(row["active"].BoolValue, Is.True);
    }

    [Test]
    public async Task ExecuteQuery_FloatValue_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE prices (id oid PRIMARY KEY, price float64)");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO prices (id, price) VALUES (gen_id(), 3.14)"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT price FROM prices");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "price");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);
        Assert.That(row["price"].Float64Value, Is.EqualTo(3.14).Within(0.001));
    }

    [Test]
    public async Task ExecuteQuery_NullValue_EncodedAsTypedNull()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100) NULL)");

        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), NULL)"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT name FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "name");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);

        // NULL is encoded as a typed null_value sentinel, not as an absent oneof.
        Assert.That(row["name"].KindCase, Is.EqualTo(Value.KindOneofCase.NullValue));
    }

    [Test]
    public async Task ExecuteQuery_IdValue_UsesIdField()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY)");

        string fixedId = "aabbccddeeff00112233aabb";
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, $"INSERT INTO items (id) VALUES ('{fixedId}')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "id");
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, msgs[0].Schema);

        // ObjectId values must travel in id_value, not string_value.
        Assert.That(row["id"].KindCase, Is.EqualTo(Value.KindOneofCase.IdValue));
        Assert.That(row["id"].IdValue, Is.EqualTo(fixedId));
    }

    // ─── Causal token ─────────────────────────────────────────────────────────

    [Test]
    public async Task ExecuteNonQuery_Autocommit_ReturnsCausalToken()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        NonQueryReply reply = await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'tok')"), Ctx());

        // Autocommit DML must return a non-zero causal token.
        Assert.That(reply.CausalTokenL != 0 || reply.CausalTokenC != 0 || reply.CausalTokenN != 0,
            Is.True, "Expected a non-null causal token from autocommit DML");
    }

    [Test]
    public async Task CommitTransaction_ReturnsCausalToken()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        await service.ExecuteNonQuery(new SqlRequest
        {
            Database  = dbName,
            Sql       = "INSERT INTO items (id, name) VALUES (gen_id(), 'tok2')",
            TxnHandle = handle,
        }, Ctx());

        CommitReply reply = await service.CommitTransaction(handle, Ctx());
        Assert.That(reply.CausalTokenL != 0 || reply.CausalTokenC != 0 || reply.CausalTokenN != 0,
            Is.True, "Expected a non-null causal token from explicit-txn commit");
    }

    [Test]
    public async Task ExecuteQuery_Autocommit_AppendsCausalTokenTrailers()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'x')"), Ctx());

        (_, TestServerCallContext ctx) = await QueryAsync(dbName, "SELECT name FROM items");

        // The autocommit read-only transaction commits after the last row and its causal token is
        // appended to the response trailers (in-band metadata is a batch-only concern; unary uses trailers).
        string? l = ctx.ResponseTrailers.GetValue("camus-causal-token-l");
        Assert.That(l, Is.Not.Null, "Autocommit query must append the causal token to trailers");
        Assert.That(ctx.ResponseTrailers.GetValue("camus-causal-token-c"), Is.Not.Null);
        Assert.That(ctx.ResponseTrailers.GetValue("camus-causal-token-n"), Is.Not.Null);
        Assert.That(long.TryParse(l, out _), Is.True, "Causal token L must be a parseable long");
    }

    // ─── Value round-trips: remaining types ───────────────────────────────────

    [Test]
    public async Task ExecuteQuery_Float32Value_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, v float32)");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO t (id, v) VALUES (gen_id(), 1.25)"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT v FROM t");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "v");
        Value v = GrpcAssert.RowValues(msgs, msgs[0].Schema)["v"];

        Assert.That(v.KindCase, Is.EqualTo(Value.KindOneofCase.Float32Value), "float32 must use float32_value");
        Assert.That(v.Float32Value, Is.EqualTo(1.25f));
    }

    [Test]
    public async Task ExecuteQuery_DateValue_RoundTripsAsRawTicks()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, d date)");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO t (id, d) VALUES (gen_id(), '2026-07-04')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT d FROM t");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "d");
        Value v = GrpcAssert.RowValues(msgs, msgs[0].Schema)["d"];

        Assert.That(v.KindCase, Is.EqualTo(Value.KindOneofCase.DateValue), "date must use date_value (raw ticks)");
        DateTime decoded = new(v.DateValue, DateTimeKind.Utc);
        Assert.That(decoded.Date, Is.EqualTo(new DateTime(2026, 7, 4, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task ExecuteQuery_DateTimeValue_RoundTripsAsRawTicks()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, ts datetime)");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO t (id, ts) VALUES (gen_id(), '2026-03-15T10:20:30Z')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT ts FROM t");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "ts");
        Value v = GrpcAssert.RowValues(msgs, msgs[0].Schema)["ts"];

        Assert.That(v.KindCase, Is.EqualTo(Value.KindOneofCase.DatetimeValue), "datetime must use datetime_value");
        DateTime decoded = new(v.DatetimeValue, DateTimeKind.Utc);
        Assert.That(decoded, Is.EqualTo(new DateTime(2026, 3, 15, 10, 20, 30, DateTimeKind.Utc)));
    }

    [Test]
    public async Task ExecuteQuery_UuidValue_RoundTripsAsSixteenBytes()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, u uuid)");
        Guid uuid = Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, $"INSERT INTO t (id, u) VALUES (gen_id(), '{uuid}')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT u FROM t");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "u");
        Value v = GrpcAssert.RowValues(msgs, msgs[0].Schema)["u"];

        Assert.That(v.KindCase, Is.EqualTo(Value.KindOneofCase.UuidValue), "uuid must use uuid_value");
        Assert.That(v.UuidValue.Length, Is.EqualTo(16), "uuid must serialize as 16 big-endian bytes");
        Assert.That(GrpcValueCodec.FromProto(v).ToGuid(), Is.EqualTo(uuid));
    }

    [Test]
    public async Task ExecuteQuery_BytesValue_RoundTrips()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, b bytes)");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO t (id, b) VALUES (gen_id(), '0xDEADBEEF')"), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT b FROM t");
        GrpcAssert.AssertSchemaFirst(msgs, 1, "b");
        Value v = GrpcAssert.RowValues(msgs, msgs[0].Schema)["b"];

        Assert.That(v.KindCase, Is.EqualTo(Value.KindOneofCase.BytesValue), "bytes must use bytes_value");
        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, v.BytesValue.ToByteArray());
    }

    // ─── Locking / isolation threading ────────────────────────────────────────

    [Test]
    public async Task StartTransaction_OptimisticLocking_ThreadsThroughToTransaction()
    {
        string dbName = await CreateTestDatabaseAsync();

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName, Locking = LockingMode.Optimistic }, Ctx());
        try
        {
            KvTransaction tx = coordinator.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic));
        }
        finally
        {
            await service.RollbackTransaction(handle, Ctx());
        }
    }

    [Test]
    public async Task StartTransaction_DefaultLocking_IsServerDefault()
    {
        string dbName = await CreateTestDatabaseAsync();

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());  // no locking specified
        try
        {
            KvTransaction tx = coordinator.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            Assert.That(tx.Locking, Is.EqualTo(CamusDBConfig.DefaultTransactionLocking),
                "Unset locking must fall back to the server default");
        }
        finally
        {
            await service.RollbackTransaction(handle, Ctx());
        }
    }

    [Test]
    public async Task StartTransaction_ReadCommittedIsolation_ThreadsThroughToTransaction()
    {
        string dbName = await CreateTestDatabaseAsync();

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName, IsolationLevel = IsolationLevel.ReadCommitted }, Ctx());
        try
        {
            KvTransaction tx = coordinator.GetState(handle.TxnIdPt, (uint)handle.TxnIdCounter);
            Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.ReadCommitted));
        }
        finally
        {
            await service.RollbackTransaction(handle, Ctx());
        }
    }

    [Test]
    public async Task ExecuteQuery_ReadCommittedAutocommit_StreamsWithoutRetryWrapper()
    {
        // A non-Serializable autocommit query takes the no-retry branch of RunAutocommitQuery
        // (retry: false). Exercise it end-to-end and assert it still streams schema-first + rows.
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE t (id oid PRIMARY KEY, n int64)");
        await service.ExecuteNonQuery(
            NonQueryRequest(dbName, "INSERT INTO t (id, n) VALUES (gen_id(), 7)"), Ctx());

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        SqlRequest req = new()
        {
            Database       = dbName,
            Sql            = "SELECT n FROM t",
            IsolationLevel = IsolationLevel.ReadCommitted,
        };
        await service.ExecuteQuery(req, writer, Ctx());

        GrpcAssert.AssertSchemaFirst(writer.Written, 1, "n");
        Assert.That(GrpcAssert.RowValues(writer.Written, writer.Written[0].Schema)["n"].Int64Value,
            Is.EqualTo(7));
    }

    // ─── BatchExecute (duplex) ────────────────────────────────────────────────

    private static BatchExecuteRequest BQuery(int id, string db, string sql, TxnHandle? handle = null)
    {
        SqlRequest r = new() { Database = db, Sql = sql };
        if (handle is not null) r.TxnHandle = handle;
        return new BatchExecuteRequest { RequestId = id, Kind = BatchStatementKind.Query, Request = r };
    }

    private static BatchExecuteRequest BNonQuery(int id, string db, string sql, TxnHandle? handle = null)
    {
        SqlRequest r = new() { Database = db, Sql = sql };
        if (handle is not null) r.TxnHandle = handle;
        return new BatchExecuteRequest { RequestId = id, Kind = BatchStatementKind.NonQuery, Request = r };
    }

    private async Task<List<BatchExecuteResponse>> BatchAsync(params BatchExecuteRequest[] reqs)
    {
        FakeAsyncStreamReader<BatchExecuteRequest> reader = new(reqs);
        CapturingStreamWriter<BatchExecuteResponse> writer = new();
        await service.BatchExecute(reader, writer, Ctx());
        return writer.Written;
    }

    private static List<BatchExecuteResponse> ForId(List<BatchExecuteResponse> all, int id) =>
        all.Where(r => r.RequestId == id).ToList();

    private static BatchExecuteRequest BStart(int id, string db)
        => new() { RequestId = id, Kind = BatchStatementKind.Start, Request = new SqlRequest { Database = db } };

    private static BatchExecuteRequest BCommit(int id, string db, TxnHandle handle)
        => new() { RequestId = id, Kind = BatchStatementKind.Commit, Request = new SqlRequest { Database = db, TxnHandle = handle } };

    private static BatchExecuteRequest BRollback(int id, string db, TxnHandle handle)
        => new() { RequestId = id, Kind = BatchStatementKind.Rollback, Request = new SqlRequest { Database = db, TxnHandle = handle } };

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.Error or
        BatchExecuteResponse.PayloadOneofCase.StartReply or
        BatchExecuteResponse.PayloadOneofCase.CommitReply or
        BatchExecuteResponse.PayloadOneofCase.RollbackReply;

    [Test]
    public async Task BatchExecute_StartStatementsCommit_OverOneStream()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        // BEGIN over the stream — learn the server-minted handle from the in-band start_reply.
        reader.Push(BStart(1, dbName));
        BatchExecuteResponse startResp = await writer.WaitFor(m =>
            m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply);
        TxnHandle handle = startResp.StartReply;
        Assert.That(handle.TxnIdPt, Is.GreaterThan(0), "START must return a real handle");

        // Two inserts + COMMIT, all referencing the handle — a whole unit of work on one stream.
        reader.Push(BNonQuery(2, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'a')", handle));
        reader.Push(BNonQuery(3, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'b')", handle));
        reader.Push(BCommit(4, dbName, handle));
        BatchExecuteResponse commitResp = await writer.WaitFor(m =>
            m.RequestId == 4 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.CommitReply);
        reader.Complete();
        await server;

        Assert.That(
            commitResp.CommitReply.CausalTokenL != 0 || commitResp.CommitReply.CausalTokenC != 0
            || commitResp.CommitReply.CausalTokenN != 0,
            Is.True, "COMMIT reply must carry the causal token in-band");

        // Both rows are durable after the batched commit — no unary round-trips were used.
        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 2, "id");
    }

    [Test]
    public async Task BatchExecute_StartedButNotFinalized_RollsBackOnStreamEnd()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        reader.Push(BStart(1, dbName));
        TxnHandle handle = (await writer.WaitFor(m =>
            m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

        reader.Push(BNonQuery(2, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'ghost')", handle));
        await writer.WaitFor(m => m.RequestId == 2 && IsTerminal(m));

        // Half-close WITHOUT a COMMIT/ROLLBACK — teardown must roll the transaction back.
        reader.Complete();
        await server;

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 0, "id");

        // The handle is gone: committing it now must fail (no orphaned open transaction).
        Assert.ThrowsAsync<RpcException>(async () => await service.CommitTransaction(handle, Ctx()));
    }

    [Test]
    public async Task BatchExecute_DoubleCommit_SecondReportsBatchError()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        reader.Push(BStart(1, dbName));
        TxnHandle handle = (await writer.WaitFor(m =>
            m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

        reader.Push(BNonQuery(2, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'x')", handle));
        reader.Push(BCommit(3, dbName, handle));
        await writer.WaitFor(m => m.RequestId == 3 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.CommitReply);

        // Second commit on the same handle — the finalize gate must reject it as an in-band BatchError.
        reader.Push(BCommit(4, dbName, handle));
        BatchExecuteResponse second = await writer.WaitFor(m => m.RequestId == 4 && IsTerminal(m));
        reader.Complete();
        await server;

        Assert.That(second.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
        Assert.That(second.Error.Code, Is.Not.Empty);
    }

    [Test]
    public async Task BatchExecute_TwoTransactions_Interleaved_AreIndependent()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        // Two independent transactions opened on the same stream.
        reader.Push(BStart(1, dbName));
        reader.Push(BStart(2, dbName));
        TxnHandle a = (await writer.WaitFor(m => m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;
        TxnHandle b = (await writer.WaitFor(m => m.RequestId == 2 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

        reader.Push(BNonQuery(3, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'aaa')", a));
        reader.Push(BNonQuery(4, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'bbb')", b));
        await writer.WaitFor(m => m.RequestId == 3 && IsTerminal(m));
        await writer.WaitFor(m => m.RequestId == 4 && IsTerminal(m));

        // Commit A, roll back B — independence means only A's row survives.
        reader.Push(BCommit(5, dbName, a));
        reader.Push(BRollback(6, dbName, b));
        await writer.WaitFor(m => m.RequestId == 5 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.CommitReply);
        await writer.WaitFor(m => m.RequestId == 6 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.RollbackReply);
        reader.Complete();
        await server;

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT name FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1, "name");
        Assert.That(GrpcAssert.RowValues(msgs, msgs[0].Schema, 0)["name"].StringValue, Is.EqualTo("aaa"));
    }

    [Test]
    public async Task BatchExecute_MixedOps_EachCorrelatedByRequestId()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        // request 1 = autocommit INSERT (non-query), request 2 = autocommit SELECT over empty-ish table.
        List<BatchExecuteResponse> all = await BatchAsync(
            BNonQuery(1, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'a')"),
            BQuery(2, dbName, "SELECT id, name FROM items"));

        // request 1: exactly one NonQueryReply terminal.
        List<BatchExecuteResponse> r1 = ForId(all, 1);
        Assert.That(r1.Count, Is.EqualTo(1));
        Assert.That(r1[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery));
        Assert.That(r1[0].NonQuery.AffectedRows, Is.EqualTo(1));

        // request 2: schema first, then a terminal QueryComplete (rows depend on concurrency timing).
        List<BatchExecuteResponse> r2 = ForId(all, 2);
        Assert.That(r2[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Schema));
        Assert.That(r2[^1].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete));
        Assert.That(r2.Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Error), Is.EqualTo(0));
    }

    [Test]
    public async Task BatchExecute_SameHandle_OpsRunInArrivalOrder_ReadYourWrites()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        TxnHandle handle = await service.StartTransaction(
            new StartTxnRequest { Database = dbName }, Ctx());

        // In ONE batch on the SAME handle: insert then select. Arrival-order serialization must make
        // the select see the just-inserted row (read-your-writes within the transaction).
        List<BatchExecuteResponse> all = await BatchAsync(
            BNonQuery(1, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'ryow')", handle),
            BQuery(2, dbName, "SELECT name FROM items WHERE name = 'ryow'", handle));

        await service.CommitTransaction(handle, Ctx());

        Assert.That(ForId(all, 1)[0].NonQuery.AffectedRows, Is.EqualTo(1));

        List<BatchExecuteResponse> r2 = ForId(all, 2);
        int rows = r2.Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Row);
        Assert.That(rows, Is.EqualTo(1), "Same-handle select must see the prior insert in the same batch");
        Assert.That(r2[^1].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete));
    }

    [Test]
    public async Task BatchExecute_EmptyQuery_SchemaThenQueryCompleteNoRows()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        List<BatchExecuteResponse> all = await BatchAsync(
            BQuery(7, dbName, "SELECT id, name FROM items"));

        List<BatchExecuteResponse> r = ForId(all, 7);
        Assert.That(r.Count, Is.EqualTo(2), "Empty result = schema + query_complete, no rows");
        Assert.That(r[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Schema));
        Assert.That(r[1].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete));
    }

    [Test]
    public async Task BatchExecute_FailingOp_ReportsBatchErrorWithoutDisturbingOthers()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        // request 1 = bad SQL (terminal BatchError), request 2 = valid insert (must still succeed).
        List<BatchExecuteResponse> all = await BatchAsync(
            BQuery(1, dbName, "SELEKT nope FRUM nowhere"),
            BNonQuery(2, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'ok')"));

        List<BatchExecuteResponse> r1 = ForId(all, 1);
        Assert.That(r1.Count, Is.EqualTo(1));
        Assert.That(r1[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
        Assert.That(r1[0].Error.Code, Is.Not.Empty, "BatchError must carry the CADBxxxx code in-band");

        List<BatchExecuteResponse> r2 = ForId(all, 2);
        Assert.That(r2[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery));
        Assert.That(r2[0].NonQuery.AffectedRows, Is.EqualTo(1), "A sibling op must be unaffected by another op's failure");
    }

    [Test]
    public async Task BatchExecute_NonQuery_CarriesCausalTokenInBand()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        List<BatchExecuteResponse> all = await BatchAsync(
            BNonQuery(9, dbName, "INSERT INTO items (id, name) VALUES (gen_id(), 'tok')"));

        NonQueryReply reply = ForId(all, 9)[0].NonQuery;
        Assert.That(reply.CausalTokenL != 0 || reply.CausalTokenC != 0 || reply.CausalTokenN != 0,
            Is.True, "Autocommit non-query in a batch must carry its causal token in-band (no trailers)");
    }

    [Test]
    public async Task BatchExecute_ManyAutocommitInserts_AllApplied()
    {
        string dbName = await CreateTestDatabaseWithTableAsync(
            "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");

        BatchExecuteRequest[] inserts = Enumerable.Range(0, 12)
            .Select(i => BNonQuery(i, dbName, $"INSERT INTO items (id, name) VALUES (gen_id(), 'row{i}')"))
            .ToArray();

        List<BatchExecuteResponse> all = await BatchAsync(inserts);

        // Every op reports a NonQueryReply, and all 12 rows are actually present afterwards.
        Assert.That(all.Count(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.NonQuery),
            Is.EqualTo(12));
        Assert.That(all.Any(m => m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Error), Is.False);

        (List<QueryStreamMessage> msgs, _) = await QueryAsync(dbName, "SELECT id FROM items");
        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 12, "id");
    }

}

/// <summary>
/// Stream writer that cancels the supplied token immediately after the first (schema) message,
/// used by cancel-mid-stream tests to exercise the rollback path.
/// </summary>
internal sealed class CancelAfterSchemaWriter : IServerStreamWriter<QueryStreamMessage>
{
    private readonly CancellationTokenSource cts;
    private bool first = true;

    public List<QueryStreamMessage> Written { get; } = new();
    public WriteOptions? WriteOptions { get; set; }

    public CancelAfterSchemaWriter(CancellationTokenSource cts) => this.cts = cts;

    public Task WriteAsync(QueryStreamMessage message) => WriteAsync(message, CancellationToken.None);

    public Task WriteAsync(QueryStreamMessage message, CancellationToken cancellationToken)
    {
        Written.Add(message);
        if (first)
        {
            first = false;
            cts.Cancel();
        }
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }
}
