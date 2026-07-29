/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Grpc;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Pins the parse saving that motivates prepared statements, on both transports.
///
/// <para>Both transports parse a statement at the transport layer purely to route it — the SHOW
/// branch on the query paths, the database-scoped check on the non-query path — and that parse does
/// not go through the executor's parser cache, so it is paid in full on every request. A prepared
/// statement records the root node once and skips it entirely.</para>
///
/// <para>These tests assert the property rather than a benchmark number: a warm inline execution
/// still parses, a prepared execution parses <b>zero</b> times. Without them a later refactor could
/// silently reintroduce the transport-side parse and nothing would fail.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestPreparedStatementParseElimination : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CamusSqlService service = null!;
    private PreparedStatementRegistry registry = null!;

    [SetUp]
    public void SetUpService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance, new ForegroundRequestGauge());
        registry = new();
    }

    [TearDown]
    public async Task TearDownService()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new System.IO.MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Response.Body = new System.IO.MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body) =>
        new(executor, coordinator, registry, logger) { ControllerContext = Context(body) };

    private async Task<string> CreateDatabaseWithRowAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await Sql(new { sql = $"CREATE DATABASE {db}" }).ExecuteSQLDDL();
        await Sql(new
        {
            databaseName = db,
            sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)",
        }).ExecuteSQLDDL();
        await Sql(new
        {
            databaseName = db,
            sql = "INSERT INTO robots (id, name, year) VALUES (gen_id(), 'optimus', 1984)",
        }).ExecuteNonSQLQuery();
        return db;
    }

    /// <summary>Parses performed while running <paramref name="action"/>.</summary>
    private static async Task<long> CountParsesAsync(Func<Task> action)
    {
        long before = SQLParserProcessor.TotalParses;
        await action();
        return SQLParserProcessor.TotalParses - before;
    }

    [Test]
    public async Task Rest_PreparedExecutionParsesNothingWhileInlineStillParses()
    {
        string db = await CreateDatabaseWithRowAsync();
        const string sql = "SELECT name FROM robots WHERE year = @year";

        object inlineBody() => new
        {
            databaseName = db,
            sql,
            parameters = new Dictionary<string, object>
            {
                ["@year"] = new { type = CoreColumnType.Integer64, longValue = 1984L },
            },
        };

        // Warm both the executor's parser cache and the plan path, so the measured runs compare
        // steady state against steady state rather than first-call effects.
        await Sql(inlineBody()).ExecuteSQLQuery();

        long inlineParses = await CountParsesAsync(async () => await Sql(inlineBody()).ExecuteSQLQuery());

        PrepareStatementResponse prepared = (PrepareStatementResponse)
            (await new PreparedStatementsController(executor, coordinator, registry, logger)
            {
                ControllerContext = Context(new { databaseName = db, sql }),
            }.PrepareSQLStatement()).Value!;

        object preparedBody() => new
        {
            statementId = prepared.StatementId,
            positionalParameters = new[] { new { type = CoreColumnType.Integer64, longValue = 1984L } },
        };

        await Sql(preparedBody()).ExecuteSQLQuery();   // warm

        long preparedParses = await CountParsesAsync(async () => await Sql(preparedBody()).ExecuteSQLQuery());

        Assert.That(inlineParses, Is.GreaterThan(0),
            "an inline request re-parses at the transport layer to route the statement");
        Assert.That(preparedParses, Is.Zero,
            "a prepared execution must not parse at all — the root node was recorded at prepare time");
    }

    [Test]
    public async Task Grpc_PreparedExecutionParsesNothingWhileInlineStillParses()
    {
        string db = await CreateDatabaseWithRowAsync();
        const string sql = "SELECT name FROM robots WHERE year = @year";

        BatchExecuteRequest Inline(int id)
        {
            SqlRequest request = new() { Database = db, Sql = sql };
            request.Parameters.Add("@year", new Value { Int64Value = 1984 });
            return new BatchExecuteRequest { RequestId = id, Kind = BatchStatementKind.Query, Request = request };
        }

        int statementId = 0;

        await RunStreamAsync(async (push, writer) =>
        {
            // Warm-up inline pass, then a measured one.
            push(Inline(1));
            await WaitTerminal(writer, 1);

            long inlineParses = await CountParsesAsync(async () =>
            {
                push(Inline(2));
                await WaitTerminal(writer, 2);
            });

            push(new BatchExecuteRequest
            {
                RequestId = 3,
                Kind = BatchStatementKind.Prepare,
                Request = new SqlRequest { Database = db, Sql = sql },
            });
            BatchExecuteResponse prepareReply = await writer.WaitFor(m =>
                m.RequestId == 3 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply);
            statementId = prepareReply.PrepareReply.StatementId;

            BatchExecuteRequest Prepared(int id)
            {
                SqlRequest request = new() { StatementId = statementId };
                request.PositionalParameters.Add(new Value { Int64Value = 1984 });
                return new BatchExecuteRequest { RequestId = id, Kind = BatchStatementKind.Query, Request = request };
            }

            push(Prepared(4));
            await WaitTerminal(writer, 4);       // warm

            long preparedParses = await CountParsesAsync(async () =>
            {
                push(Prepared(5));
                await WaitTerminal(writer, 5);
            });

            Assert.That(inlineParses, Is.GreaterThan(0),
                "an inline batched query re-parses to answer the SHOW routing check");
            Assert.That(preparedParses, Is.Zero,
                "a prepared execution must not parse at all");
        });
    }

    // ─── Concurrency ──────────────────────────────────────────────────────────

    [Test]
    public async Task Grpc_ConcurrentExecutionsOfOneStatementEachBindTheirOwnValues()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)",
        }, new TestServerCallContext());

        const int count = 32;

        await RunStreamAsync(async (push, writer) =>
        {
            push(new BatchExecuteRequest
            {
                RequestId = 1,
                Kind = BatchStatementKind.Prepare,
                Request = new SqlRequest
                {
                    Database = db,
                    Sql = "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)",
                },
            });
            int statementId = (await writer.WaitFor(m =>
                m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply))
                .PrepareReply.StatementId;

            // One immutable entry, many concurrent executions: if the entry were shared mutable state
            // (or the binding reused a buffer) the rows would cross-contaminate.
            for (int i = 0; i < count; i++)
            {
                SqlRequest request = new() { StatementId = statementId };
                request.PositionalParameters.Add(new Value { StringValue = $"robot-{i}" });
                request.PositionalParameters.Add(new Value { Int64Value = 2000 + i });
                push(new BatchExecuteRequest { RequestId = 100 + i, Kind = BatchStatementKind.NonQuery, Request = request });
            }

            for (int i = 0; i < count; i++)
            {
                BatchExecuteResponse resp = await WaitTerminal(writer, 100 + i);
                Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.NonQuery), resp.Error?.Message);
            }
        });

        CapturingStreamWriter<QueryStreamMessage> reader = new();
        await service.ExecuteQuery(
            new SqlRequest { Database = db, Sql = "SELECT name, year FROM robots ORDER BY year" }, reader, new TestServerCallContext());

        List<ResultRow> rows = reader.Written
            .Where(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row)
            .Select(m => m.Row)
            .ToList();

        Assert.That(rows.Count, Is.EqualTo(count));
        for (int i = 0; i < count; i++)
        {
            Assert.That(rows[i].Values[0].StringValue, Is.EqualTo($"robot-{i}"));
            Assert.That(rows[i].Values[1].Int64Value, Is.EqualTo(2000 + i));
        }
    }

    [Test]
    public async Task Grpc_CloseWhileExecutionsAreInFlight_NeverCorruptsAnExecution()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE robots (id oid PRIMARY KEY, name string(100), year int64)",
        }, new TestServerCallContext());

        const int count = 16;
        int succeeded = 0, refused = 0;

        await RunStreamAsync(async (push, writer) =>
        {
            push(new BatchExecuteRequest
            {
                RequestId = 1,
                Kind = BatchStatementKind.Prepare,
                Request = new SqlRequest
                {
                    Database = db,
                    Sql = "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)",
                },
            });
            int statementId = (await writer.WaitFor(m =>
                m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.PrepareReply))
                .PrepareReply.StatementId;

            // Fire executions and a CLOSE into the same stream without waiting: each op either runs
            // against the entry it resolved or is cleanly refused. Neither may crash the stream or
            // half-bind a statement.
            for (int i = 0; i < count; i++)
            {
                SqlRequest request = new() { StatementId = statementId };
                request.PositionalParameters.Add(new Value { StringValue = $"robot-{i}" });
                request.PositionalParameters.Add(new Value { Int64Value = 2000 + i });
                push(new BatchExecuteRequest { RequestId = 200 + i, Kind = BatchStatementKind.NonQuery, Request = request });

                if (i == count / 2)
                    push(new BatchExecuteRequest
                    {
                        RequestId = 999,
                        Kind = BatchStatementKind.Close,
                        Request = new SqlRequest { StatementId = statementId },
                    });
            }

            for (int i = 0; i < count; i++)
            {
                BatchExecuteResponse resp = await WaitTerminal(writer, 200 + i);
                if (resp.PayloadCase == BatchExecuteResponse.PayloadOneofCase.NonQuery)
                    succeeded++;
                else
                {
                    Assert.That(resp.PayloadCase, Is.EqualTo(BatchExecuteResponse.PayloadOneofCase.Error));
                    Assert.That(resp.Error.Code, Is.EqualTo(CamusDBErrorCodes.UnknownPreparedStatement),
                        "a racing close may only produce the re-prepare code, never an internal error");
                    refused++;
                }
            }

            await WaitTerminal(writer, 999);
        });

        Assert.That(succeeded + refused, Is.EqualTo(count));

        // Every op that reported success actually wrote its row — no torn or lost bindings.
        CapturingStreamWriter<QueryStreamMessage> reader = new();
        await service.ExecuteQuery(
            new SqlRequest { Database = db, Sql = "SELECT name FROM robots" }, reader, new TestServerCallContext());
        Assert.That(reader.Written.Count(m => m.PayloadCase == QueryStreamMessage.PayloadOneofCase.Row),
            Is.EqualTo(succeeded));
    }

    // ─── Stream helpers ───────────────────────────────────────────────────────

    private async Task RunStreamAsync(
        Func<Action<BatchExecuteRequest>, ObservingStreamWriter<BatchExecuteResponse>, Task> script)
    {
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = service.BatchExecute(reader, writer, new TestServerCallContext());

        await script(reader.Push, writer);

        reader.Complete();
        await server;
    }

    private static Task<BatchExecuteResponse> WaitTerminal(
        ObservingStreamWriter<BatchExecuteResponse> writer, int requestId)
        => writer.WaitFor(m => m.RequestId == requestId && m.PayloadCase is
            BatchExecuteResponse.PayloadOneofCase.QueryComplete or
            BatchExecuteResponse.PayloadOneofCase.NonQuery or
            BatchExecuteResponse.PayloadOneofCase.Error or
            BatchExecuteResponse.PayloadOneofCase.PrepareReply or
            BatchExecuteResponse.PayloadOneofCase.CloseReply);
}
