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

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Tests.Grpc;

using CamusDB.Grpc;

using CoreColumnType = CamusDB.Core.Catalogs.Models.ColumnType;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Transport-level coverage for the account statements: <c>CREATE USER</c>, <c>GRANT</c>,
/// <c>ALTER USER</c> and <c>DROP USER</c>.
///
/// <para>These name their target in the SQL, write only the shared auth keyspace, and return no
/// <c>DatabaseDescriptor</c>. A transport must therefore run them with no transaction and no commit.
/// The batched gRPC non-query path did not: it began a transaction and then handed the descriptor-less
/// result to a commit, so every account statement sent over <c>BatchExecute</c> answered
/// "Internal server error" while the same statement succeeded on every other entry point. That is why
/// these tests drive the transports rather than the executor — the engine always answered correctly.</para>
///
/// <para>The password is bound as a parameter throughout, because that is the reason a caller cannot
/// simply route these to the DDL endpoint: the DDL route of a typical client binds nothing, so the
/// secret would move into the statement text the server parses, logs and traces.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestAccountStatementTransports : BaseTest
{
    private ILogger<ICamusDB> Logger => logger;

    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private PreparedStatementRegistry registry = null!;
    private CamusSqlService grpc = null!;

    /// <summary>
    /// A token server key is the only setting <c>LoginAsync</c> needs; authentication itself stays
    /// off. It lets these tests confirm that the password a caller <em>bound</em> is the one the
    /// engine hashed, which no status code can show.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { AccessTokenServerKey = "test-key" };

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

    // ─── Transports ───────────────────────────────────────────────────────────

    /// <summary>One way a client can send a no-rows statement to this server.</summary>
    private delegate Task RunAccountStatement(string database, string sql, Dictionary<string, ColumnValue>? parameters);

    /// <summary>
    /// Every entry point a client reaches an account statement through. All four must accept all four
    /// statements: a client routes a non-SELECT statement to whichever endpoint it uses for those, so
    /// one entry point that refuses is indistinguishable from a server that lacks the feature.
    /// </summary>
    private (string Name, RunAccountStatement Run)[] Transports() =>
    [
        ("REST /execute-sql-non-query", RestNonQueryAsync),
        ("gRPC ExecuteNonQuery",        GrpcUnaryNonQueryAsync),
        ("gRPC ExecuteDdl",             GrpcUnaryDdlAsync),
        ("gRPC BatchExecute",           GrpcBatchNonQueryAsync),
    ];

    /// <summary>
    /// The controller reads its body with a camel-case naming policy and no case-insensitive
    /// fallback, so the test must encode a bound parameter exactly as a real client does. Encoded
    /// any other way the value silently binds as a typed NULL and the statement fails on the secret.
    /// </summary>
    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

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

    private async Task RestNonQueryAsync(string database, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        ExecuteSQLController controller = new(executor, coordinator, registry, Logger, Options)
        {
            ControllerContext = Context(new { databaseName = database, sql, parameters }),
        };

        JsonResult result = await controller.ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;
        Assert.AreEqual("ok", response.Status, $"{sql} failed over REST: {response.Message}");
    }

    private static SqlRequest Request(string database, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        SqlRequest request = new() { Database = database, Sql = sql };
        if (parameters is not null)
        {
            foreach ((string name, ColumnValue value) in parameters)
                request.Parameters[name] = GrpcValueCodec.ToProto(value);
        }
        return request;
    }

    private async Task GrpcUnaryNonQueryAsync(string database, string sql, Dictionary<string, ColumnValue>? parameters)
        => await grpc.ExecuteNonQuery(Request(database, sql, parameters), new TestServerCallContext());

    private async Task GrpcUnaryDdlAsync(string database, string sql, Dictionary<string, ColumnValue>? parameters)
        => await grpc.ExecuteDdl(Request(database, sql, parameters), new TestServerCallContext());

    private static bool IsTerminal(BatchExecuteResponse m) => m.PayloadCase is
        BatchExecuteResponse.PayloadOneofCase.NonQuery or
        BatchExecuteResponse.PayloadOneofCase.QueryComplete or
        BatchExecuteResponse.PayloadOneofCase.Error or
        BatchExecuteResponse.PayloadOneofCase.StartReply or
        BatchExecuteResponse.PayloadOneofCase.CommitReply or
        BatchExecuteResponse.PayloadOneofCase.RollbackReply;

    /// <summary>
    /// Drives one statement over a real <c>BatchExecute</c> stream — the path that failed. A
    /// <c>BatchError</c> is reported in-band rather than thrown, so it must be asserted on
    /// explicitly; awaiting the call alone would pass while every statement failed.
    /// </summary>
    private async Task GrpcBatchNonQueryAsync(string database, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 1,
            Kind = BatchStatementKind.NonQuery,
            Request = Request(database, sql, parameters),
        });

        BatchExecuteResponse response = await writer.WaitFor(m => m.RequestId == 1 && IsTerminal(m));
        reader.Complete();
        await server;

        Assert.AreNotEqual(
            BatchExecuteResponse.PayloadOneofCase.Error, response.PayloadCase,
            $"{sql} failed over BatchExecute: {response.Error?.Code} {response.Error?.Message}");
    }

    // ─── Fixture helpers ──────────────────────────────────────────────────────

    private async Task<string> CreateDatabaseAsync()
    {
        // Prefixed with a letter so the name lexes as an identifier rather than a number.
        string db = "ad" + Guid.NewGuid().ToString("n")[..8];
        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);
        return db;
    }

    private static Dictionary<string, ColumnValue> Password(string password) =>
        new() { ["@pw"] = new ColumnValue(CoreColumnType.String, password) };

    /// <summary>The privileges <c>SHOW GRANTS</c> reports for the user, or null when it has none.</summary>
    private async Task<string?> GrantedPrivilegesAsync(string user)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: $"SHOW GRANTS FOR {user}", parameters: null));

        await foreach (QueryResultRow row in cursor)
            return row.Row["privileges"].StrValue;

        return null;
    }

    private async Task<bool> UserExistsAsync(string user)
    {
        try
        {
            await GrantedPrivilegesAsync(user);
            return true;
        }
        catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.UserDoesNotExist)
        {
            return false;
        }
    }

    // ─── The provisioning sequence, once per transport ────────────────────────

    /// <summary>
    /// The reproduction, generalized: the exact four statements a control plane issues to provision a
    /// scoped account, run end to end over each transport a client can reach. Only
    /// <c>BatchExecute</c> failed; the other three are the fence that keeps it fixed everywhere.
    ///
    /// <para>Each assertion is on observable state — the grants read back, the password that logs in —
    /// rather than on a status code, so a statement that reports success while doing nothing still
    /// fails here.</para>
    /// </summary>
    [Test]
    public async Task EveryTransportRunsTheAccountProvisioningSequence()
    {
        string db = await CreateDatabaseAsync();

        foreach ((string name, RunAccountStatement run) in Transports())
        {
            string user = "u" + Guid.NewGuid().ToString("n")[..8];

            await run(db, $"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH sha256_password BY @pw",
                Password("first-password-1"));
            Assert.IsTrue(await UserExistsAsync(user), $"{name}: CREATE USER did not create the account");
            Assert.IsNotNull(
                await executor.LoginAsync(user, "first-password-1"),
                $"{name}: the bound password is not the one the engine hashed");

            await run(db, $"GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO {user}", null);
            Assert.AreEqual("SELECT, INSERT, UPDATE, DELETE", await GrantedPrivilegesAsync(user),
                $"{name}: GRANT did not record the privileges");

            await run(db, $"ALTER USER {user} IDENTIFIED WITH sha256_password BY @pw",
                Password("second-password-2"));
            Assert.IsNotNull(
                await executor.LoginAsync(user, "second-password-2"),
                $"{name}: ALTER USER did not apply the new bound password");

            await run(db, $"DROP USER IF EXISTS {user}", null);
            Assert.IsFalse(await UserExistsAsync(user), $"{name}: DROP USER left the account behind");
        }
    }

    /// <summary>
    /// An account statement sent inside an explicit batched transaction. The bypass must be decided
    /// before the transaction handle is consulted: the statement writes the shared auth keyspace
    /// through its own machinery, so the caller's transaction is neither used nor spent, and the
    /// caller can still commit it afterwards.
    /// </summary>
    [Test]
    public async Task AnAccountStatementInsideAnExplicitBatchTransactionLeavesItUsable()
    {
        string db = await CreateDatabaseAsync();
        string user = "u" + Guid.NewGuid().ToString("n")[..8];

        await grpc.ExecuteDdl(
            Request(db, "CREATE TABLE items (id oid PRIMARY KEY, name string(100))", null),
            new TestServerCallContext());

        ChannelAsyncStreamReader<BatchExecuteRequest> reader = new();
        ObservingStreamWriter<BatchExecuteResponse> writer = new();
        Task server = grpc.BatchExecute(reader, writer, new TestServerCallContext());

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 1,
            Kind = BatchStatementKind.Start,
            Request = new SqlRequest { Database = db },
        });
        TxnHandle handle = (await writer.WaitFor(m =>
            m.RequestId == 1 && m.PayloadCase == BatchExecuteResponse.PayloadOneofCase.StartReply)).StartReply;

        SqlRequest account = Request(db, $"CREATE USER {user} IDENTIFIED WITH sha256_password BY @pw",
            Password("in-transaction-1"));
        account.TxnHandle = handle;
        reader.Push(new BatchExecuteRequest { RequestId = 2, Kind = BatchStatementKind.NonQuery, Request = account });

        BatchExecuteResponse accountReply = await writer.WaitFor(m => m.RequestId == 2 && IsTerminal(m));
        Assert.AreNotEqual(
            BatchExecuteResponse.PayloadOneofCase.Error, accountReply.PayloadCase,
            $"CREATE USER in a transaction failed: {accountReply.Error?.Code} {accountReply.Error?.Message}");

        // The caller's transaction was never touched, so it still carries its own work to a commit.
        SqlRequest insert = Request(db, "INSERT INTO items (id, name) VALUES (gen_id(), 'a')", null);
        insert.TxnHandle = handle;
        reader.Push(new BatchExecuteRequest { RequestId = 3, Kind = BatchStatementKind.NonQuery, Request = insert });
        await writer.WaitFor(m => m.RequestId == 3 && IsTerminal(m));

        reader.Push(new BatchExecuteRequest
        {
            RequestId = 4,
            Kind = BatchStatementKind.Commit,
            Request = new SqlRequest { Database = db, TxnHandle = handle },
        });
        BatchExecuteResponse commitReply = await writer.WaitFor(m => m.RequestId == 4 && IsTerminal(m));
        reader.Complete();
        await server;

        Assert.AreEqual(
            BatchExecuteResponse.PayloadOneofCase.CommitReply, commitReply.PayloadCase,
            $"the transaction was not usable after the account statement: {commitReply.Error?.Message}");

        Assert.IsTrue(await UserExistsAsync(user));
        Assert.IsNotNull(await executor.LoginAsync(user, "in-transaction-1"));
    }

    // ─── The shared routing list ──────────────────────────────────────────────

    /// <summary>
    /// Every statement the transports route as a database-scoped mutation really is one: the engine
    /// must answer it with no database open and no transaction, because that is exactly what the
    /// transports hand it on the strength of this list alone.
    ///
    /// <para>Only the account statements are exercised here. The database-lifecycle members of the
    /// same list create and destroy databases, which this fixture cannot assert against without
    /// changing what the other tests see; they are covered by their own fixtures.</para>
    /// </summary>
    [Test]
    public async Task EveryAccountStatementIsRoutedAsADatabaseScopedMutation()
    {
        string db = await CreateDatabaseAsync();
        string user = "u" + Guid.NewGuid().ToString("n")[..8];

        (NodeType Type, string Sql)[] statements =
        [
            (NodeType.CreateUserIfNotExists, $"CREATE USER IF NOT EXISTS {user} IDENTIFIED BY 'Pw123456789012'"),
            (NodeType.Grant,                 $"GRANT SELECT ON {db}.* TO {user}"),
            (NodeType.Revoke,                $"REVOKE SELECT ON {db}.* FROM {user}"),
            (NodeType.AlterUser,             $"ALTER USER {user} IDENTIFIED BY 'Pw123456789013'"),
            (NodeType.DropUserIfExists,      $"DROP USER IF EXISTS {user}"),
        ];

        foreach ((NodeType type, string sql) in statements)
        {
            Assert.AreEqual(type, executor.ParseSql(sql).nodeType, $"{sql} parses to an unexpected node");
            Assert.IsTrue(StatementScope.IsDatabaseScopedMutation(type),
                $"{type} must be routed as a database-scoped mutation");
            Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(type),
                $"{type} must not need a context database");

            // No database and no transaction — exactly what a transport hands one of these.
            await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null));
        }
    }
}
