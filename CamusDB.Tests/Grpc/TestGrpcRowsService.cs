
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

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Integration tests for <see cref="CamusRowsService"/>. The service is instantiated with a real
/// embedded Kahuna node (inherited from <see cref="BaseTest"/>) and all RPC methods are invoked
/// directly — no real gRPC transport is needed. The <see cref="TestServerCallContext"/> and
/// <see cref="CapturingStreamWriter{T}"/> stubs provide the gRPC surface.
/// </summary>
[TestFixture]
public class TestGrpcRowsService : BaseTest
{
    private CamusRowsService rowsService = null!;
    private CamusSqlService sqlService = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpGrpcService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger, CamusDBConfig.Ambient,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(serviceExecutor);
        rowsService = new(serviceExecutor, coordinator, logger, CamusDBConfig.Ambient);
        sqlService   = new(serviceExecutor, coordinator, logger, TestHostApplicationLifetime.Instance, new CamusDB.App.Services.ForegroundRequestGauge(), CamusDBConfig.Ambient);
    }

    [TearDown]
    public async Task TearDownGrpcService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static TestServerCallContext Ctx(CancellationToken ct = default) => new(ct);

    private async Task<string> CreateTestDatabaseAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await sqlService.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, Ctx());
        return db;
    }

    private async Task CreateTestTableAsync(string db, string tableName = "users")
    {
        string sql = $"CREATE TABLE {tableName} (id oid PRIMARY KEY, name string(64), age int64, active bool)";
        await sqlService.ExecuteDdl(new SqlRequest { Database = db, Sql = sql }, Ctx());
    }

    private static InsertRowRequest InsertReq(string db, string table,
        Dictionary<string, Value> values) => new()
    {
        Database = db,
        Table    = table,
        Values   = { values },
    };

    private async Task<(List<QueryStreamMessage>, TestServerCallContext)> QueryRowsAsync(
        string db, string table, IEnumerable<QueryFilter>? filters = null)
    {
        RowQueryRequest req = new() { Database = db, Table = table };
        if (filters is not null)
            foreach (QueryFilter f in filters)
                req.Filters.Add(f);

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext ctx = Ctx();
        await rowsService.Query(req, writer, ctx);
        return (writer.Written, ctx);
    }

    private async Task<(List<QueryStreamMessage>, TestServerCallContext)> QueryByIdAsync(
        string db, string table, string id)
    {
        RowByIdRequest req = new() { Database = db, Table = table, Id = id };
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        TestServerCallContext ctx = Ctx();
        await rowsService.QueryById(req, writer, ctx);
        return (writer.Written, ctx);
    }

    // Generates a valid 24-lowercase-hex ObjectId string from a GUID (no ObjectId allocator needed).
    private static string NewId() => Guid.NewGuid().ToString("N")[..24];

    private static Value Str(string s) => new() { StringValue = s };
    private static Value Int64(long v) => new() { Int64Value = v };
    private static Value Bool(bool b) => new() { BoolValue = b };
    private static Value Id(string id) => new() { IdValue = id };

    // ─── InsertRow ────────────────────────────────────────────────────────────

    [Test]
    public async Task InsertRow_SingleRow_ReturnsOneAffectedRow()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        NonQueryReply reply = await rowsService.InsertRow(InsertReq(db, "users", new()
        {
            ["id"]     = Id(id),
            ["name"]   = Str("Alice"),
            ["age"]    = Int64(30),
            ["active"] = Bool(true),
        }), Ctx());

        Assert.That(reply.AffectedRows, Is.EqualTo(1));
    }

    [Test]
    public async Task InsertRow_MultipleRows_EachReturnsOneAffectedRow()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        for (int i = 0; i < 5; i++)
        {
            string id = NewId();
            NonQueryReply r = await rowsService.InsertRow(InsertReq(db, "users", new()
            {
                ["id"]     = Id(id),
                ["name"]   = Str($"User{i}"),
                ["age"]    = Int64(20 + i),
                ["active"] = Bool(i % 2 == 0),
            }), Ctx());
            Assert.That(r.AffectedRows, Is.EqualTo(1));
        }
    }

    [Test]
    public async Task InsertRow_UnknownTable_Throws()
    {
        string db = await CreateTestDatabaseAsync();
        Assert.ThrowsAsync<RpcException>(async () =>
            await rowsService.InsertRow(InsertReq(db, "nonexistent", new()
            {
                ["id"] = Id(NewId()),
            }), Ctx()));
    }

    // ─── Query ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Query_EmptyTable_EmitsSchemaAndZeroRows()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users");

        // Empty table: schema-only, but it now carries the FULL column layout (derived from the
        // table definition, not row values) so a schema-before-rows consumer still gets FieldCount.
        Assert.That(msgs.Count, Is.EqualTo(1), "Empty result = one schema message, no rows");
        Assert.That(msgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        Assert.That(msgs[0].Schema.Columns.Select(c => c.Name),
            Is.EqualTo(new[] { "id", "name", "age", "active" }));
        Assert.That(msgs[0].Schema.Columns[0].Type, Is.EqualTo(ColumnType.Id));
        Assert.That(msgs[0].Schema.Columns[1].Type, Is.EqualTo(ColumnType.String));
        Assert.That(msgs[0].Schema.Columns[2].Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(msgs[0].Schema.Columns[3].Type, Is.EqualTo(ColumnType.Bool));
    }

    [Test]
    public async Task Query_InsertedRows_StreamsSchemaFirst()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id1 = NewId();
        string id2 = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id1), ["name"] = Str("Alice"), ["age"] = Int64(30), ["active"] = Bool(true) }), Ctx());
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id2), ["name"] = Str("Bob"),   ["age"] = Int64(25), ["active"] = Bool(false) }), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users");

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 2);
        Assert.That(schema.Columns.Select(c => c.Name), Is.SupersetOf(new[] { "id", "name", "age", "active" }));
    }

    [Test]
    public async Task Query_WithFilter_ReturnsMatchingRows()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id1 = NewId();
        string id2 = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id1), ["name"] = Str("Alice"), ["age"] = Int64(30), ["active"] = Bool(true) }), Ctx());
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id2), ["name"] = Str("Bob"),   ["age"] = Int64(25), ["active"] = Bool(false) }), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users",
            [new QueryFilter { ColumnName = "name", Op = "=", Value = Str("Alice") }]);

        GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);
    }

    [Test]
    public async Task Query_SchemaFirst_MessageOrderCorrect()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("X"), ["age"] = Int64(1), ["active"] = Bool(true) }), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users");

        Assert.That(msgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        for (int i = 1; i < msgs.Count; i++)
            Assert.That(msgs[i].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Row));
    }

    // ─── QueryById ────────────────────────────────────────────────────────────

    [Test]
    public async Task QueryById_ExistingRow_ReturnsSchemaAndRow()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new()
        {
            ["id"]     = Id(id),
            ["name"]   = Str("Carol"),
            ["age"]    = Int64(40),
            ["active"] = Bool(true),
        }), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "users", id);

        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);
        Dictionary<string, Value> row = GrpcAssert.RowValues(msgs, schema);
        Assert.That(row["name"].StringValue, Is.EqualTo("Carol"));
        Assert.That(row["age"].Int64Value, Is.EqualTo(40));
    }

    [Test]
    public async Task QueryById_NonExistentId_EmitsSchemaAndZeroRows()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string missingId = NewId();
        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "users", missingId);

        // Must emit the full column schema even when no row matches.
        Assert.That(msgs.Count, Is.EqualTo(1));
        Assert.That(msgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        Assert.That(msgs[0].Schema.Columns.Select(c => c.Name),
            Is.EqualTo(new[] { "id", "name", "age", "active" }));
    }

    [Test]
    public async Task QueryById_SchemaAlwaysFirst()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("Dan"), ["age"] = Int64(22), ["active"] = Bool(false) }), Ctx());

        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "users", id);

        Assert.That(msgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
        if (msgs.Count > 1)
            Assert.That(msgs[1].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Row));
    }

    // ─── UpdateRows ───────────────────────────────────────────────────────────

    [Test]
    public async Task UpdateRows_MatchingFilter_UpdatesRows()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("Eve"), ["age"] = Int64(28), ["active"] = Bool(true) }), Ctx());

        UpdateRowsRequest req = new()
        {
            Database = db,
            Table    = "users",
            Values   = { { "age", Int64(29) } },
            Filters  = { new QueryFilter { ColumnName = "name", Op = "=", Value = Str("Eve") } },
        };
        NonQueryReply reply = await rowsService.UpdateRows(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(1));

        // Verify the value was actually updated.
        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "users", id);
        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);
        Assert.That(GrpcAssert.RowValues(msgs, schema)["age"].Int64Value, Is.EqualTo(29));
    }

    [Test]
    public async Task UpdateRows_NoMatchingFilter_ZeroUpdated()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        UpdateRowsRequest req = new()
        {
            Database = db,
            Table    = "users",
            Values   = { { "age", Int64(99) } },
            Filters  = { new QueryFilter { ColumnName = "name", Op = "=", Value = Str("Nobody") } },
        };
        NonQueryReply reply = await rowsService.UpdateRows(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(0));
    }

    // ─── UpdateById ───────────────────────────────────────────────────────────

    [Test]
    public async Task UpdateById_ExistingRow_UpdatesRow()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("Frank"), ["age"] = Int64(35), ["active"] = Bool(true) }), Ctx());

        UpdateByIdRequest req = new()
        {
            Database = db,
            Table    = "users",
            Id       = id,
            Values   = { { "name", Str("Franklin") } },
        };
        NonQueryReply reply = await rowsService.UpdateById(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(1));

        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "users", id);
        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);
        Assert.That(GrpcAssert.RowValues(msgs, schema)["name"].StringValue, Is.EqualTo("Franklin"));
    }

    [Test]
    public async Task UpdateById_NonExistentId_ZeroUpdated()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        UpdateByIdRequest req = new()
        {
            Database = db,
            Table    = "users",
            Id       = NewId(),
            Values   = { { "name", Str("Ghost") } },
        };
        NonQueryReply reply = await rowsService.UpdateById(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(0));
    }

    // ─── DeleteRows ───────────────────────────────────────────────────────────

    [Test]
    public async Task DeleteRows_MatchingFilter_DeletesRows()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("Grace"), ["age"] = Int64(27), ["active"] = Bool(true) }), Ctx());

        DeleteRowsRequest req = new()
        {
            Database = db,
            Table    = "users",
            Filters  = { new QueryFilter { ColumnName = "name", Op = "=", Value = Str("Grace") } },
        };
        NonQueryReply reply = await rowsService.DeleteRows(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(1));

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users");
        // Empty table after delete: schema + 0 rows.
        Assert.That(msgs.Count, Is.EqualTo(1));
        Assert.That(msgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
    }

    [Test]
    public async Task DeleteRows_NoMatchingFilter_ZeroDeleted()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        DeleteRowsRequest req = new()
        {
            Database = db,
            Table    = "users",
            Filters  = { new QueryFilter { ColumnName = "name", Op = "=", Value = Str("Nobody") } },
        };
        NonQueryReply reply = await rowsService.DeleteRows(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(0));
    }

    // ─── DeleteById ───────────────────────────────────────────────────────────

    [Test]
    public async Task DeleteById_ExistingRow_DeletesRow()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new() { ["id"] = Id(id), ["name"] = Str("Heidi"), ["age"] = Int64(31), ["active"] = Bool(false) }), Ctx());

        RowByIdRequest req = new() { Database = db, Table = "users", Id = id };
        NonQueryReply reply = await rowsService.DeleteById(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(1));

        // Verify gone.
        (List<QueryStreamMessage> after, _) = await QueryByIdAsync(db, "users", id);
        Assert.That(after.Count, Is.EqualTo(1));  // schema only, no row
        Assert.That(after[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
    }

    [Test]
    public async Task DeleteById_NonExistentId_ZeroDeleted()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        RowByIdRequest req = new()
        {
            Database = db,
            Table    = "users",
            Id       = NewId(),
        };
        NonQueryReply reply = await rowsService.DeleteById(req, Ctx());
        Assert.That(reply.AffectedRows, Is.EqualTo(0));
    }

    // ─── Round-trip ───────────────────────────────────────────────────────────

    [Test]
    public async Task RoundTrip_InsertQueryUpdateDelete()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();

        // Insert.
        NonQueryReply ins = await rowsService.InsertRow(InsertReq(db, "users", new()
        {
            ["id"]     = Id(id),
            ["name"]   = Str("Ivan"),
            ["age"]    = Int64(45),
            ["active"] = Bool(true),
        }), Ctx());
        Assert.That(ins.AffectedRows, Is.EqualTo(1));

        // Query by id — verify insert visible.
        (List<QueryStreamMessage> qMsgs, _) = await QueryByIdAsync(db, "users", id);
        ResultSchema schema = GrpcAssert.AssertSchemaFirst(qMsgs, expectedRowCount: 1);
        Assert.That(GrpcAssert.RowValues(qMsgs, schema)["name"].StringValue, Is.EqualTo("Ivan"));

        // Update.
        NonQueryReply upd = await rowsService.UpdateById(new UpdateByIdRequest
        {
            Database = db, Table = "users", Id = id, Values = { { "name", Str("Ivan Updated") } }
        }, Ctx());
        Assert.That(upd.AffectedRows, Is.EqualTo(1));

        // Query again — verify update visible.
        (List<QueryStreamMessage> uMsgs, _) = await QueryByIdAsync(db, "users", id);
        ResultSchema schema2 = GrpcAssert.AssertSchemaFirst(uMsgs, expectedRowCount: 1);
        Assert.That(GrpcAssert.RowValues(uMsgs, schema2)["name"].StringValue, Is.EqualTo("Ivan Updated"));

        // Delete.
        NonQueryReply del = await rowsService.DeleteById(new RowByIdRequest { Database = db, Table = "users", Id = id }, Ctx());
        Assert.That(del.AffectedRows, Is.EqualTo(1));

        // Query — confirm deletion.
        (List<QueryStreamMessage> dMsgs, _) = await QueryByIdAsync(db, "users", id);
        Assert.That(dMsgs.Count, Is.EqualTo(1));
        Assert.That(dMsgs[0].PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema));
    }

    [Test]
    public async Task InsertRow_CausalTokenReturnedOnCommit()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        NonQueryReply reply = await rowsService.InsertRow(InsertReq(db, "users", new()
        {
            ["id"]     = Id(id),
            ["name"]   = Str("Judy"),
            ["age"]    = Int64(50),
            ["active"] = Bool(true),
        }), Ctx());

        Assert.That(reply.AffectedRows, Is.EqualTo(1));
        // Causal token fields should be non-zero after a commit.
        Assert.That(reply.CausalTokenL != 0 || reply.CausalTokenC != 0 || reply.CausalTokenN != 0, Is.True);
    }

    // ─── Primary key not named "id" (UpdateById / DeleteById) ──────────────────

    [Test]
    public async Task UpdateByIdAndDeleteById_ResolveRealPrimaryKeyColumn_NotNamedId()
    {
        // The primary-key column is 'myid', NOT 'id'. UpdateById/DeleteById must resolve the
        // real PK column from the table's index (like QueryById), not filter on a hardcoded "id".
        string db = await CreateTestDatabaseAsync();
        await sqlService.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE things (myid oid PRIMARY KEY, label string(64))",
        }, Ctx());

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "things", new()
        {
            ["myid"] = Id(id),
            ["label"]     = Str("before"),
        }), Ctx());

        NonQueryReply upd = await rowsService.UpdateById(new UpdateByIdRequest
        {
            Database = db, Table = "things", Id = id, Values = { { "label", Str("after") } },
        }, Ctx());
        Assert.That(upd.AffectedRows, Is.EqualTo(1), "UpdateById must match via the real PK column 'myid'");

        (List<QueryStreamMessage> msgs, _) = await QueryByIdAsync(db, "things", id);
        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);
        Assert.That(GrpcAssert.RowValues(msgs, schema)["label"].StringValue, Is.EqualTo("after"));

        NonQueryReply del = await rowsService.DeleteById(
            new RowByIdRequest { Database = db, Table = "things", Id = id }, Ctx());
        Assert.That(del.AffectedRows, Is.EqualTo(1), "DeleteById must match via the real PK column 'myid'");
    }

    // ─── Schema type fidelity + error mapping ──────────────────────────────────

    [Test]
    public async Task Query_NullInFirstRow_SchemaTypeComesFromDefinitionNotValue()
    {
        // A NULL cell has ColumnValue.Type == Null; deriving the schema type from the row value would
        // mislabel the column. The schema must come from the table definition (String here).
        string db = await CreateTestDatabaseAsync();
        await sqlService.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE t (id oid PRIMARY KEY, name string(64) NULL)",
        }, Ctx());

        await rowsService.InsertRow(InsertReq(db, "t", new() { ["id"] = Id(NewId()) }), Ctx());  // name = NULL

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "t");
        ResultSchema schema = GrpcAssert.AssertSchemaFirst(msgs, expectedRowCount: 1);

        ColumnSchema nameCol = schema.Columns.Single(c => c.Name == "name");
        Assert.That(nameCol.Type, Is.EqualTo(ColumnType.String),
            "Column type must be String from the definition, not Null from the row value");
    }

    [Test]
    public async Task Query_SchemaColumnTypesMatchTableDefinition()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        (List<QueryStreamMessage> msgs, _) = await QueryRowsAsync(db, "users");

        ResultSchema schema = msgs[0].Schema;
        Assert.That(schema.Columns.Single(c => c.Name == "id").Type, Is.EqualTo(ColumnType.Id));
        Assert.That(schema.Columns.Single(c => c.Name == "name").Type, Is.EqualTo(ColumnType.String));
        Assert.That(schema.Columns.Single(c => c.Name == "age").Type, Is.EqualTo(ColumnType.Integer64));
        Assert.That(schema.Columns.Single(c => c.Name == "active").Type, Is.EqualTo(ColumnType.Bool));
    }

    [Test]
    public void Query_UnknownDatabase_ThrowsNotFound()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
        {
            CapturingStreamWriter<QueryStreamMessage> writer = new();
            await rowsService.Query(new RowQueryRequest { Database = "nosuchdb", Table = "t" }, writer, Ctx());
        })!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task InsertRow_DuplicatePrimaryKey_ThrowsAlreadyExists()
    {
        string db = await CreateTestDatabaseAsync();
        await CreateTestTableAsync(db);

        string id = NewId();
        await rowsService.InsertRow(InsertReq(db, "users", new()
        {
            ["id"] = Id(id), ["name"] = Str("A"), ["age"] = Int64(1), ["active"] = Bool(true),
        }), Ctx());

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await rowsService.InsertRow(InsertReq(db, "users", new()
            {
                ["id"] = Id(id), ["name"] = Str("B"), ["age"] = Int64(2), ["active"] = Bool(false),
            }), Ctx()))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.AlreadyExists));
        Assert.That(ex.Trailers.GetValue("camus-error-code"),
            Is.EqualTo(CamusDBErrorCodes.DuplicatePrimaryKey)
              .Or.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }
}
