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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Server-side coverage for prepared statements on the duplex <c>BatchExecute</c> stream: registering
/// a statement, executing it positionally, closing it, and every way a bad or stale request must be
/// refused. Driven through the real service over the stream stubs, so the ops travel the same path a
/// client's would — including the per-stream ownership of a handle, which is the whole lifetime model.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcPreparedStatements : BaseTest
{
    private CamusSqlService service = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(serviceExecutor);
        service = new(serviceExecutor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge());
    }

    [TearDown]
    public async Task TearDownService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static TestServerCallContext Ctx() => new();

    private async Task<string> CreateDatabaseWithTableAsync(string createTableSql)
    {
        string dbName = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {dbName}" }, Ctx());
        await service.ExecuteDdl(new SqlRequest { Database = dbName, Sql = createTableSql }, Ctx());
        return dbName;
    }

    private static BatchExecuteRequest BPrepare(int id, string db, string sql)
        => new()
        {
            RequestId = id,
            Kind = BatchStatementKind.Prepare,
            Request = new SqlRequest { Database = db, Sql = sql },
        };

    private static BatchExecuteRequest BClose(int id, int statementId)
        => new()
        {
            RequestId = id,
            Kind = BatchStatementKind.Close,
            Request = new SqlRequest { StatementId = statementId },
        };

    private static BatchExecuteRequest BExecute(
        int id, BatchStatementKind kind, int statementId, params Value[] values)
    {
        SqlRequest request = new() { StatementId = statementId };
        request.PositionalParameters.AddRange(values);
        return new BatchExecuteRequest { RequestId = id, Kind = kind, Request = request };
    }

    private static Value Str(string v) => new() { StringValue = v };
    private static Value Int(long v) => new() { Int64Value = v };

    /// <summary>
    /// Runs a scripted conversation on one stream: <paramref name="script"/> receives a pusher and the
    /// writer, so a test can send follow-up ops only after seeing the reply that names their handle.
    /// </summary>
    private async Task<IReadOnlyList<BatchExecuteResponse>> StreamAsync(
        Func<Action<BatchExecuteRequest>, ObservingStreamWriter<BatchExecuteResponse>, Task> script)
    {
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, Ctx());

        await script(reader.Push, writer);

        reader.Complete();
        await server;
        return writer.Written;
    }

    private static Task<BatchExecuteResponse> WaitFor(
        ObservingStreamWriter<BatchExecuteResponse> writer, int requestId,
        BatchExecuteResponse.PayloadOneofCase payload)
        => writer.WaitFor(m => m.RequestId == requestId && m.PayloadCase == payload);

    private static Task<BatchExecuteResponse> WaitTerminal(
        ObservingStreamWriter<BatchExecuteResponse> writer, int requestId)
        => writer.WaitFor(m => m.RequestId == requestId && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or
            BatchExecuteResponse.PayloadOneofCase.NonQuery or
            BatchExecuteResponse.PayloadOneofCase.Error or
            BatchExecuteResponse.PayloadOneofCase.PrepareReply or
            BatchExecuteResponse.PayloadOneofCase.CloseReply);

    // ─── PREPARE ──────────────────────────────────────────────────────────────

    [Test]
    public async Task Prepare_PublishesParameterNamesInBindingOrder()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)"));
            BatchExecuteResponse reply = await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply);

            Assert.That(reply.PrepareReply.StatementId, Is.GreaterThan(0), "0 is reserved for inline requests");
            Assert.That(reply.PrepareReply.ParameterNames, Is.EqualTo(new[] { "@name", "@year" }));
        });
    }

    [Test]
    public async Task Prepare_RejectsUnparsableSqlAtPrepareTimeNotFirstExecution()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "SELECT FROM WHERE nonsense ("));
            BatchExecuteResponse reply = await WaitTerminal(writer, 1);

            Assert.That(reply.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
        });
    }

    [Test]
    public async Task Prepare_RejectsDdl()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "CREATE TABLE other (id oid PRIMARY KEY)"));
            BatchExecuteResponse reply = await WaitTerminal(writer, 1);

            Assert.That(reply.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(reply.Error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        });
    }

    [Test]
    public async Task Prepare_OverTheStreamCap_FailsWithoutEvictingLiveHandles()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        int saved = CamusDBConfig.GrpcMaxPreparedStatementsPerStream;
        CamusDBConfig.GrpcMaxPreparedStatementsPerStream = 2;
        try
        {
            await StreamAsync(async (push, writer) =>
            {
                push(BPrepare(1, db, "SELECT id FROM robots WHERE year = @a"));
                BatchExecuteResponse first = await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply);
                push(BPrepare(2, db, "SELECT id FROM robots WHERE year = @b"));
                await WaitFor(writer, 2, BatchExecuteResponse.PayloadOneofCase.PrepareReply);

                push(BPrepare(3, db, "SELECT id FROM robots WHERE year = @c"));
                BatchExecuteResponse third = await WaitTerminal(writer, 3);
                Assert.That(third.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
                Assert.That(third.Error.Code, Is.EqualTo(CamusDBErrorCodes.PreparedStatementLimitExceeded));

                // The refusal must not have cost the caller a handle it already holds.
                push(BExecute(4, BatchStatementKind.Query, first.PrepareReply.StatementId, Int(1)));
                BatchExecuteResponse stillWorks = await WaitTerminal(writer, 4);
                Assert.That(stillWorks.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete));
            });
        }
        finally
        {
            CamusDBConfig.GrpcMaxPreparedStatementsPerStream = saved;
        }
    }

    // ─── Prepared execution ───────────────────────────────────────────────────

    [Test]
    public async Task PreparedNonQuery_InsertsAutocommitWithPositionalValues()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)"));
            int stmt = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            push(BExecute(2, BatchStatementKind.NonQuery, stmt, Str("optimus"), Int(1984)));
            push(BExecute(3, BatchStatementKind.NonQuery, stmt, Str("bumblebee"), Int(1985)));
            BatchExecuteResponse a = await WaitTerminal(writer, 2);
            BatchExecuteResponse b = await WaitTerminal(writer, 3);

            Assert.That(a.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), a.Error?.Message);
            Assert.That(b.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), b.Error?.Message);
            Assert.That(a.NonQuery.AffectedRows, Is.EqualTo(1));
        });

        // Both rows are durable and carry the values bound by ordinal, not by accident of order.
        CapturingStreamWriter<QueryStreamMessage> reader = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT name, year FROM robots ORDER BY year" }, reader, Ctx());
        List<string> names = reader.Written
            .Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row)
            .Select(m => m.Row.Values[0].StringValue)
            .ToList();
        Assert.That(names, Is.EqualTo(new[] { "optimus", "bumblebee" }));
    }

    [Test]
    public async Task PreparedQuery_StreamsSchemaThenRowsThenComplete()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)"));
            int insert = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;
            push(BExecute(2, BatchStatementKind.NonQuery, insert, Str("optimus"), Int(1984)));
            await WaitTerminal(writer, 2);

            push(BPrepare(3, db, "SELECT name FROM robots WHERE year = @year"));
            int select = (await WaitFor(writer, 3, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            push(BExecute(4, BatchStatementKind.Query, select, Int(1984)));
            await WaitTerminal(writer, 4);

            List<BatchExecuteResponse> forQuery = writer.Written.Where(m => m.RequestId == 4).ToList();
            Assert.That(forQuery[0].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Schema));
            Assert.That(forQuery[1].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Row));
            Assert.That(forQuery[1].Row.Values[0].StringValue, Is.EqualTo("optimus"));
            Assert.That(forQuery[^1].PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.QueryComplete));
        });
    }

    [Test]
    public async Task PreparedExecution_InsideABatchedTransaction_CommitsWithIt()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(new BatchExecuteRequest
            {
                RequestId = 1,
                Kind = BatchStatementKind.Start,
                Request = new SqlRequest { Database = db },
            });
            TxnHandle handle = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

            push(BPrepare(2, db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)"));
            int stmt = (await WaitFor(writer, 2, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            // The execution rides the transaction's serial chain; the handle names the txn, the
            // statement id names the SQL.
            BatchExecuteRequest exec = BExecute(3, BatchStatementKind.NonQuery, stmt, Str("wall-e"), Int(2008));
            exec.Request.TxnHandle = handle;
            push(exec);
            BatchExecuteResponse execResp = await WaitTerminal(writer, 3);
            Assert.That(execResp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), execResp.Error?.Message);

            push(new BatchExecuteRequest
            {
                RequestId = 4,
                Kind = BatchStatementKind.Commit,
                Request = new SqlRequest { Database = db, TxnHandle = handle },
            });
            await WaitFor(writer, 4, BatchExecuteResponse.PayloadOneofCase.CommitReply);
        });

        CapturingStreamWriter<QueryStreamMessage> reader = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT name FROM robots" }, reader, Ctx());
        Assert.That(reader.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row), Is.EqualTo(1));
    }

    [Test]
    public async Task PreparedUpdateAndDelete_BindPositionally()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)"));
            int insert = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;
            push(BExecute(2, BatchStatementKind.NonQuery, insert, Str("optimus"), Int(1984)));
            push(BExecute(3, BatchStatementKind.NonQuery, insert, Str("wall-e"), Int(2008)));
            await WaitTerminal(writer, 2);
            await WaitTerminal(writer, 3);

            push(BPrepare(4, db, "UPDATE robots SET name = @name WHERE year = @year"));
            int update = (await WaitFor(writer, 4, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;
            push(BExecute(5, BatchStatementKind.NonQuery, update, Str("optimus prime"), Int(1984)));
            BatchExecuteResponse updated = await WaitTerminal(writer, 5);
            Assert.That(updated.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), updated.Error?.Message);
            Assert.That(updated.NonQuery.AffectedRows, Is.EqualTo(1));

            push(BPrepare(6, db, "DELETE FROM robots WHERE year = @year"));
            int delete = (await WaitFor(writer, 6, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;
            push(BExecute(7, BatchStatementKind.NonQuery, delete, Int(2008)));
            BatchExecuteResponse deleted = await WaitTerminal(writer, 7);
            Assert.That(deleted.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), deleted.Error?.Message);
            Assert.That(deleted.NonQuery.AffectedRows, Is.EqualTo(1));
        });

        CapturingStreamWriter<QueryStreamMessage> reader = new();
        await service.ExecuteQuery(new SqlRequest { Database = db, Sql = "SELECT name FROM robots" }, reader, Ctx());
        List<string> names = reader.Written
            .Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row)
            .Select(m => m.Row.Values[0].StringValue)
            .ToList();

        Assert.That(names, Is.EqualTo(new[] { "optimus prime" }));
    }

    // ─── Limits ───────────────────────────────────────────────────────────────

    [Test]
    public void TheStore_RefusesAStatementOverTheStreamsRetainedByteBudget()
    {
        long savedBytes = CamusDBConfig.GrpcMaxPreparedStatementBytesPerStream;
        int savedPerStatement = CamusDBConfig.MaxPreparedStatementBytes;
        CamusDBConfig.GrpcMaxPreparedStatementBytesPerStream = 4096;
        CamusDBConfig.MaxPreparedStatementBytes = 65_536;
        try
        {
            StreamPreparedStatements store = new();
            string padding = new('x', 900);

            int admitted = 0;
            CamusDBException? refused = null;
            for (int i = 0; i < 50 && refused is null; i++)
            {
                try
                {
                    store.Add(PreparedStatementBinder.Create("db", $"SELECT {i} -- {padding}"));
                    admitted++;
                }
                catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.PreparedStatementLimitExceeded)
                {
                    refused = e;
                }
            }

            // The count cap (512) would have let all 50 in; the byte budget is what stops it.
            Assert.That(refused, Is.Not.Null);
            Assert.That(refused!.Message, Does.Contain("retained-byte"));
            Assert.That(admitted, Is.GreaterThan(0).And.LessThan(50));
            Assert.That(store.RetainedBytes, Is.LessThanOrEqualTo(4096));
        }
        finally
        {
            CamusDBConfig.GrpcMaxPreparedStatementBytesPerStream = savedBytes;
            CamusDBConfig.MaxPreparedStatementBytes = savedPerStatement;
        }
    }

    [Test]
    public void TheStore_ReturnsQuotaWhenAStatementIsClosed()
    {
        StreamPreparedStatements store = new();

        int id = store.Add(PreparedStatementBinder.Create("db", "SELECT 1"));
        Assert.That(store.RetainedBytes, Is.GreaterThan(0));

        store.Remove(id);
        Assert.That(store.Count, Is.Zero);
        Assert.That(store.RetainedBytes, Is.Zero, "closing must return exactly what admission took");
    }

    [Test]
    public void TheStore_RefusesAtIdExhaustionRatherThanWrappingIntoUnusableIds()
    {
        // Ids are never reused, so the space is finite. Wrapping would produce negative ids — which
        // every resolve rejects — and eventually collide with a live id, handing a client someone
        // else's statement. The seam starts the counter just below the ceiling.
        StreamPreparedStatements store = new(int.MaxValue - 1);

        Assert.That(store.Add(PreparedStatementBinder.Create("db", "SELECT 1")), Is.EqualTo(int.MaxValue));

        CamusDBException exhausted = Assert.Throws<CamusDBException>(
            () => store.Add(PreparedStatementBinder.Create("db", "SELECT 2")))!;

        Assert.That(exhausted.Code, Is.EqualTo(CamusDBErrorCodes.PreparedStatementLimitExceeded));
        Assert.That(exhausted.Message, Does.Contain("new stream"));
    }

    // ─── Rejections ───────────────────────────────────────────────────────────

    [Test]
    public async Task PreparedExecution_WithWrongValueCount_ReportsBothCounts()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "SELECT name FROM robots WHERE year = @year AND name = @name"));
            int stmt = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            push(BExecute(2, BatchStatementKind.Query, stmt, Int(1984)));
            BatchExecuteResponse resp = await WaitTerminal(writer, 2);

            Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(resp.Error.Message, Does.Contain("2").And.Contain("1"));
        });
    }

    [Test]
    public async Task PreparedExecution_WithUnknownId_IsRefusedWithTheRePrepareCode()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY)");

        await StreamAsync(async (push, writer) =>
        {
            push(BExecute(1, BatchStatementKind.Query, 4242));
            BatchExecuteResponse resp = await WaitTerminal(writer, 1);

            Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        });
    }

    [Test]
    public async Task PreparedExecution_MixedWithInlineFields_IsRefusedRatherThanPreferringOne()
    {
        string db = await CreateDatabaseWithTableAsync(
            "CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "SELECT id FROM robots WHERE year = @year"));
            int stmt = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            BatchExecuteRequest exec = BExecute(2, BatchStatementKind.Query, stmt, Int(1984));
            exec.Request.Sql = "SELECT id FROM robots";           // a second, contradictory statement
            push(exec);
            BatchExecuteResponse resp = await WaitTerminal(writer, 2);

            Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        });
    }

    [Test]
    public async Task InlineExecution_WithPositionalValues_IsRefused()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        await StreamAsync(async (push, writer) =>
        {
            SqlRequest request = new() { Database = db, Sql = "SELECT id FROM robots WHERE year = @year" };
            request.PositionalParameters.Add(Int(1984));
            push(new BatchExecuteRequest { RequestId = 1, Kind = BatchStatementKind.Query, Request = request });

            BatchExecuteResponse resp = await WaitTerminal(writer, 1);
            Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        });
    }

    // ─── CLOSE ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Close_FreesTheHandleAndIsIdempotent()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "SELECT id FROM robots WHERE year = @year"));
            int stmt = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;

            push(BClose(2, stmt));
            Assert.That((await WaitTerminal(writer, 2)).PayloadCase,
                Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.CloseReply));

            // Closing again succeeds — the requested end state already holds.
            push(BClose(3, stmt));
            Assert.That((await WaitTerminal(writer, 3)).PayloadCase,
                Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.CloseReply));

            // …and closing an id that never existed is equally fine.
            push(BClose(4, 99999));
            Assert.That((await WaitTerminal(writer, 4)).PayloadCase,
                Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.CloseReply));

            // But the handle really is gone.
            push(BExecute(5, BatchStatementKind.Query, stmt, Int(1984)));
            BatchExecuteResponse afterClose = await WaitTerminal(writer, 5);
            Assert.That(afterClose.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(afterClose.Error.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        });
    }

    [Test]
    public async Task AHandleFromAnotherStreamIsUnknownHere()
    {
        string db = await CreateDatabaseWithTableAsync("CREATE TABLE robots (id oid PRIMARY KEY, year int64)");

        int stmtOnFirstStream = 0;
        await StreamAsync(async (push, writer) =>
        {
            push(BPrepare(1, db, "SELECT id FROM robots WHERE year = @year"));
            stmtOnFirstStream = (await WaitFor(writer, 1, BatchExecuteResponse.PayloadOneofCase.PrepareReply)).PrepareReply.StatementId;
        });

        // A second stream is a second lifetime: the id means nothing here, which is exactly what a
        // client sees after its transport is rebuilt — its cue to prepare again.
        await StreamAsync(async (push, writer) =>
        {
            push(BExecute(1, BatchStatementKind.Query, stmtOnFirstStream, Int(1984)));
            BatchExecuteResponse resp = await WaitTerminal(writer, 1);

            Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
            Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement));
        });
    }

    // ─── Unary RPCs ───────────────────────────────────────────────────────────

    [Test]
    public void UnaryRpcs_RejectAStatementId()
    {
        SqlRequest request = new() { Database = "whatever", StatementId = 7 };

        Assert.That(
            async () => await service.ExecuteNonQuery(request, Ctx()),
            Throws.Exception,
            "a handle is scoped to the stream that minted it; a unary call has no such stream");
    }
}
