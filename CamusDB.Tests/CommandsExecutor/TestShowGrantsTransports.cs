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

using ProtoQueryStreamMessage = CamusDB.Grpc.QueryStreamMessage;
using SqlRequest = CamusDB.Grpc.SqlRequest;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Transport-level coverage for <c>SHOW GRANTS</c>: the REST endpoints and the gRPC service a client
/// actually calls, with authentication both off and on.
///
/// <para>The engine has always answered this statement correctly (see
/// <c>TestSqlAuthentication</c>), and every transport still failed it. <c>SHOW GRANTS</c> reads the
/// server-level auth catalog, opens no database, and so returns no <c>DatabaseDescriptor</c> — but it
/// was missing from the list each query entry point carried inline of statements that must run
/// without a transaction. It therefore fell through to the autocommit path, where naming no database
/// made the begin refuse outright and naming one made the commit dereference the null descriptor.
/// Both were reachable by any client, which is why these tests drive the transports rather than the
/// executor.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowGrantsTransports : BaseTest
{
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

    private static ControllerContext Context(object body, string? bearer = null)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return new ControllerContext { HttpContext = http };
    }

    private ExecuteSQLController Sql(object body, string? bearer = null) =>
        new(executor, coordinator, registry, Logger, Options) { ControllerContext = Context(body, bearer) };

    private static string ReadBody(ControllerBase controller)
    {
        Stream body = controller.Response.Body;
        body.Seek(0, SeekOrigin.Begin);
        using StreamReader reader = new(body, Encoding.UTF8, leaveOpen: true);
        return reader.ReadToEnd();
    }

    /// <summary>
    /// The reproduction's fixture: one database, one user, one grant of two privileges on that
    /// database. Everything runs through the engine, so what the tests below exercise is purely the
    /// transport that reads the grants back.
    /// </summary>
    private async Task<(string db, string user)> SeedGrantAsync()
    {
        string db = "gd" + Guid.NewGuid().ToString("n")[..8];
        string user = "u" + Guid.NewGuid().ToString("n")[..8];

        await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        await ServerDdlAsync($"CREATE USER {user} IDENTIFIED BY 'Pw123456789012'");
        await ServerDdlAsync($"GRANT SELECT, INSERT ON {db}.* TO {user}");

        return (db, user);
    }

    private Task ServerDdlAsync(string sql)
        => executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null));

    private static string CellOf(ExecuteSQLQueryResponse response, int row, string column)
    {
        int ordinal = -1;
        for (int i = 0; i < response.Columns.Count; i++)
        {
            if (string.Equals(response.Columns[i].Name, column, StringComparison.Ordinal))
                ordinal = i;
        }

        Assert.AreNotEqual(-1, ordinal, $"the response declares no '{column}' column");
        return response.Rows.Rows[row].Row[column].StrValue!;
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The exact reproduction: <c>/execute-sql-query</c> with a database named, authentication off.
    /// This returned <c>CA0000 "Object reference not set to an instance of an object"</c> — the
    /// commit dereferencing the descriptor the statement never opened.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReturnsGrantRows()
    {
        (string db, string user) = await SeedGrantAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = $"SHOW GRANTS FOR {user}" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(
            new[] { "user", "object", "privileges" },
            response.Columns.Select(c => c.Name).ToArray(),
            "clients decode rows positionally, so the declared schema is the contract");

        Assert.AreEqual(1, response.Total);
        Assert.AreEqual(user, CellOf(response, 0, "user"));
        Assert.AreEqual($"{db}.*", CellOf(response, 0, "object"));
        Assert.AreEqual("SELECT, INSERT", CellOf(response, 0, "privileges"));
    }

    /// <summary>
    /// Grants are server-level, so the statement must answer with no database in scope at all. This
    /// path returned <c>CADB0400 "DatabaseName is required"</c>: the same routing gap seen from the
    /// other side, where the autocommit begin refused the request before the statement ever ran.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointAnswersWithNoDatabaseInScope()
    {
        (string db, string user) = await SeedGrantAsync();

        JsonResult result = await Sql(new { sql = $"SHOW GRANTS FOR {user}" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total);
        Assert.AreEqual($"{db}.*", CellOf(response, 0, "object"));
    }

    /// <summary>
    /// The streaming endpoint carries its own copy of the routing decision, so it needs its own case:
    /// its autocommit branch committed the null descriptor exactly as the buffered one did.
    /// </summary>
    [Test]
    public async Task RestStreamingEndpointStreamsGrantRows()
    {
        (string db, string user) = await SeedGrantAsync();

        ExecuteSQLController controller = Sql(new { databaseName = db, sql = $"SHOW GRANTS FOR {user}" });
        await controller.ExecuteSQLQueryStream();

        string body = ReadBody(controller);
        Assert.AreEqual(200, controller.Response.StatusCode, body);
        StringAssert.Contains("\"privileges\"", body, "the schema header must precede the rows");
        StringAssert.Contains($"{db}.*", body);
        StringAssert.Contains("\"status\":\"ok\"", body, "the trailer reports the terminal status");
    }

    /// <summary>
    /// An unknown user is a domain error, not a crash — the distinction the null-descriptor failure
    /// destroyed, since every call answered <c>CA0000</c> whatever it was asked.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReportsAnUnknownUserAsADomainError()
    {
        (string db, _) = await SeedGrantAsync();

        JsonResult result = await Sql(new { databaseName = db, sql = "SHOW GRANTS FOR nosuchuser" }).ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, response.Code);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The gRPC query path carries its own copy of the routing decision too, and
    /// <c>camus-cli &lt;db&gt; -e "SHOW GRANTS FOR u"</c> reported <c>CADB0400</c> because of it.
    /// Drives the real service so the routing, not just the row encoders, is what is under test.
    /// </summary>
    [Test]
    public async Task GrpcQueryStreamsGrantRows()
    {
        (string db, string user) = await SeedGrantAsync();

        CapturingStreamWriter<ProtoQueryStreamMessage> writer = new();
        await grpc.ExecuteQuery(
            new SqlRequest { Database = db, Sql = $"SHOW GRANTS FOR {user}" },
            writer,
            new TestServerCallContext(CancellationToken.None));

        List<ProtoQueryStreamMessage> schemas = writer.Written.Where(m => m.Schema is not null).ToList();
        Assert.AreEqual(1, schemas.Count, "exactly one schema message precedes the rows");
        Assert.AreEqual(
            new[] { "user", "object", "privileges" },
            schemas[0].Schema.Columns.Select(c => c.Name).ToArray());

        List<ProtoQueryStreamMessage> rows = writer.Written.Where(m => m.Row is not null).ToList();
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(user, rows[0].Row.Values[0].StringValue);
        Assert.AreEqual($"{db}.*", rows[0].Row.Values[1].StringValue);
        Assert.AreEqual("SELECT, INSERT", rows[0].Row.Values[2].StringValue);
    }

    // ─── Authentication on ────────────────────────────────────────────────────

    /// <summary>
    /// The operator's actual case: authentication has just been switched on, and <c>SHOW GRANTS</c> is
    /// the step that confirms each account landed correctly. The routing is independent of
    /// authentication, so this failed identically with a valid superuser token.
    /// </summary>
    [Test]
    public async Task RestQueryEndpointReturnsGrantRowsWithAuthenticationOn()
    {
        CamusDBOptions authOptions = Options with
        {
            AuthenticationEnabled = true,
            AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
            BootstrapSuperuser = "root",
            BootstrapSuperuserPassword = "root-pw",
        };

        CommandExecutor authExecutor = CreateCommandExecutor(authOptions);
        string db = "gd" + Guid.NewGuid().ToString("n")[..8];
        string user = "u" + Guid.NewGuid().ToString("n")[..8];

        await authExecutor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, authExecutor);

        await authExecutor.EnsureBootstrapSuperuserAsync(
            authOptions.BootstrapSuperuser, authOptions.BootstrapSuperuserPassword);
        string token = (await authExecutor.LoginAsync("root", "root-pw")).Token;
        Principal root = await authExecutor.ResolvePrincipalAsync(token);

        await authExecutor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: $"CREATE USER {user} IDENTIFIED BY 'Pw123456789012'",
            parameters: null, principal: root));
        await authExecutor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: $"GRANT SELECT, INSERT ON {db}.* TO {user}",
            parameters: null, principal: root));

        ExecuteSQLController controller = new(
            authExecutor, new HttpTransactionCoordinator(authExecutor),
            new PreparedStatementRegistry(authOptions), Logger, authOptions)
        {
            ControllerContext = Context(new { databaseName = db, sql = $"SHOW GRANTS FOR {user}" }, bearer: token)
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.AreEqual("ok", response.Status, response.Message);
        Assert.AreEqual(1, response.Total);
        Assert.AreEqual($"{db}.*", CellOf(response, 0, "object"));
    }

    /// <summary>
    /// A non-superuser may read their own grants and nobody else's.
    ///
    /// <para>Left open, this statement hands any authenticated caller the whole privilege layout of
    /// the server: which accounts exist, which are superusers, and exactly which databases and tables
    /// each one can reach. That is the same reconnaissance material <c>SHOW VARIABLES</c>,
    /// <c>SHOW ENGINE STATS</c> and <c>SHOW SLOW QUERIES</c> are each superuser-gated to withhold, and
    /// no per-database grant scopes it down. <c>SHOW DATABASES</c> is not a precedent for leaving it
    /// open — that output is filtered to what the caller can already see; this one is not filtered at
    /// all.</para>
    ///
    /// <para>Driven through the transport rather than the executor, because that is the surface a
    /// client reaches and the layer a previous bug in this exact statement hid behind.</para>
    /// </summary>
    [Test]
    public async Task RestQueryEndpointRefusesAnotherUsersGrantsToANonSuperuser()
    {
        CamusDBOptions authOptions = Options with
        {
            AuthenticationEnabled = true,
            AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
            BootstrapSuperuser = "root",
            BootstrapSuperuserPassword = "root-pw",
        };

        CommandExecutor authExecutor = CreateCommandExecutor(authOptions);
        string db = "gd" + Guid.NewGuid().ToString("n")[..8];
        string nosy = "n" + Guid.NewGuid().ToString("n")[..8];
        string other = "o" + Guid.NewGuid().ToString("n")[..8];

        await authExecutor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, authExecutor);

        await authExecutor.EnsureBootstrapSuperuserAsync(
            authOptions.BootstrapSuperuser, authOptions.BootstrapSuperuserPassword);
        string rootToken = (await authExecutor.LoginAsync("root", "root-pw")).Token;
        Principal root = await authExecutor.ResolvePrincipalAsync(rootToken);

        foreach (string account in new[] { nosy, other })
        {
            await authExecutor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: "", sql: $"CREATE USER {account} IDENTIFIED BY 'Pw123456789012'",
                parameters: null, principal: root));
            await authExecutor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: "", sql: $"GRANT SELECT ON {db}.* TO {account}",
                parameters: null, principal: root));
        }

        string nosyToken = (await authExecutor.LoginAsync(nosy, "Pw123456789012")).Token;

        async Task<ExecuteSQLQueryResponse> AsNosy(string sql)
        {
            ExecuteSQLController controller = new(
                authExecutor, new HttpTransactionCoordinator(authExecutor),
                new PreparedStatementRegistry(authOptions), Logger, authOptions)
            {
                ControllerContext = Context(new { sql }, bearer: nosyToken)
            };

            return (ExecuteSQLQueryResponse)(await controller.ExecuteSQLQuery()).Value!;
        }

        // Reading someone else's layout is refused.
        ExecuteSQLQueryResponse denied = await AsNosy($"SHOW GRANTS FOR {other}");
        Assert.AreEqual("failed", denied.Status);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, denied.Code);

        // And the refusal must not double as an account-existence oracle. Naming an account that does
        // not exist has to fail the same way as naming one that does — otherwise the difference
        // between the two answers enumerates the catalog, which is what the gate is for.
        ExecuteSQLQueryResponse absent = await AsNosy("SHOW GRANTS FOR nosuchaccountanywhere");
        Assert.AreEqual(denied.Code, absent.Code);
        Assert.AreEqual(denied.Message, absent.Message);

        // Their own grants still work, both spellings.
        ExecuteSQLQueryResponse ownByName = await AsNosy($"SHOW GRANTS FOR {nosy}");
        Assert.AreEqual("ok", ownByName.Status, ownByName.Message);
        Assert.AreEqual(1, ownByName.Total);

        ExecuteSQLQueryResponse ownBare = await AsNosy("SHOW GRANTS");
        Assert.AreEqual("ok", ownBare.Status, ownBare.Message);
        Assert.AreEqual(1, ownBare.Total);

        // A superuser still reads anyone's.
        ExecuteSQLController asRoot = new(
            authExecutor, new HttpTransactionCoordinator(authExecutor),
            new PreparedStatementRegistry(authOptions), Logger, authOptions)
        {
            ControllerContext = Context(new { sql = $"SHOW GRANTS FOR {other}" }, bearer: rootToken)
        };

        ExecuteSQLQueryResponse allowed = (ExecuteSQLQueryResponse)(await asRoot.ExecuteSQLQuery()).Value!;
        Assert.AreEqual("ok", allowed.Status, allowed.Message);
        Assert.AreEqual(1, allowed.Total);
    }

    // ─── The shared routing list ──────────────────────────────────────────────

    /// <summary>
    /// Every statement the transports route as server-level really is one — it must return no
    /// descriptor, because the transports skip the transaction on the strength of this list alone.
    ///
    /// <para>The converse direction is what actually broke, and no list can assert it against itself:
    /// a statement the executor answers without a database, but which is missing here, still reaches
    /// the autocommit path. The transport cases above pin that for <c>SHOW GRANTS</c>; a new
    /// server-level statement needs the same treatment.</para>
    /// </summary>
    [Test]
    public async Task EveryStatementRoutedAsServerLevelReturnsNoDescriptor()
    {
        (string db, string user) = await SeedGrantAsync();

        (NodeType type, string sql)[] statements =
        [
            (NodeType.ShowDatabases,       "SHOW DATABASES"),
            (NodeType.ShowBranches,        $"SHOW BRANCHES FROM {db}"),
            (NodeType.ShowAncestors,       $"SHOW ANCESTORS FROM {db}"),
            (NodeType.ShowOrphanDatabases, "SHOW ORPHAN DATABASES"),
            (NodeType.ShowGrants,          $"SHOW GRANTS FOR {user}"),
            (NodeType.ShowEngineStats,     "SHOW ENGINE STATS"),
            (NodeType.ShowVariables,       "SHOW VARIABLES"),
            (NodeType.ShowSlowQueries,     "SHOW SLOW QUERIES"),
        ];

        foreach ((NodeType type, string sql) in statements)
        {
            Assert.IsTrue(StatementScope.IsServerLevelQuery(type), $"{type} must be routed as server-level");
            Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(type), $"{type} must not need a context database");

            // No database and no transaction — exactly what the transports hand a server-level
            // statement. A statement that needed either would throw here rather than return rows.
            (DatabaseDescriptor? descriptor, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null));

            Assert.IsNull(descriptor, $"{sql} opened a database the transports assume it does not");

            await foreach (QueryResultRow _ in cursor) { }
        }

        // SHOW CLUSTER SETTINGS belongs to the same family but cannot be run here: this engine is
        // built without a cluster-settings service and refuses the statement. The refusal is raised
        // on the same descriptor-less branch, before any database open, which is what this test
        // cares about — so assert the refusal rather than skipping the statement.
        Assert.IsTrue(StatementScope.IsServerLevelQuery(NodeType.ShowClusterSettings));
        Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(NodeType.ShowClusterSettings));

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: null!, database: "", sql: "SHOW CLUSTER SETTINGS", parameters: null)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, refused.Code);
    }
}
