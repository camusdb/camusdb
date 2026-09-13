/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;
using Microsoft.AspNetCore.Http;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Middleware;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// The per-table privilege check refuses a user's access that declared no privilege, instead of
/// letting it through unchecked.
///
/// <para>The first group drives the check directly with each shape of authorization scope. The sweep
/// at the end is the evidence that matters in production: it runs a wide set of statements as a
/// superuser behind the real authentication middleware. The middleware publishes the caller with no
/// privilege, so any table the engine resolves outside the statement's own authorization scope — in a
/// lazily enumerated cursor, or on a path that forgot to declare what it needs — now fails the
/// statement loudly. Tests that call the executor directly cannot see that class of defect, because
/// their enumeration runs with no principal at all.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTableCheckFailsClosed : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "fail-closed-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
    };

    private CamusSqlService sqlService = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpServices()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        sqlService = new(serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownServices()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    private static TestServerCallContext Ctx(string bearer)
    {
        TestServerCallContext ctx = new();
        ctx.RequestHeaders.Add("authorization", $"Bearer {bearer}");
        return ctx;
    }

    private static SqlRequest Req(string db, string sql) => new() { Database = db, Sql = sql };

    private async Task<(string db, string rootToken, Principal root, Principal noob)> SetupAsync()
    {
        await serviceExecutor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        string rootToken = (await serviceExecutor.LoginAsync("root", "root-password")).Token;

        string db = "failclosed" + Guid.NewGuid().ToString("n");
        await sqlService.ExecuteDdl(Req("", $"CREATE DATABASE {db}"), Ctx(rootToken));
        TrackDatabase(db, serviceExecutor);
        await sqlService.ExecuteDdl(Req(db, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, name string NOT NULL)"), Ctx(rootToken));
        await sqlService.ExecuteDdl(Req("", "CREATE USER noob IDENTIFIED BY 'noob-pw'"), Ctx(rootToken));

        Principal root = await serviceExecutor.ResolvePrincipalAsync(rootToken);
        Principal noob = await serviceExecutor.ResolvePrincipalAsync((await serviceExecutor.LoginAsync("noob", "noob-pw")).Token);
        return (db, rootToken, root, noob);
    }

    private Task<TableDescriptor> OpenItems(string db) => serviceExecutor.OpenTable(new OpenTableTicket(db, "items"));

    // ─── The check itself ─────────────────────────────────────────────────────

    [Test]
    public async Task PrincipalWithNoDeclaredPrivilege_IsRefused_EvenForASuperuser()
    {
        (string db, _, Principal root, _) = await SetupAsync();

        AuthorizationContext.Current = new AuthorizationScope(root, null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await OpenItems(db))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
        StringAssert.Contains("did not declare", ex.Message);
    }

    [Test]
    public async Task DeclaredPrivilege_IsCheckedAsBefore()
    {
        (string db, _, Principal root, Principal noob) = await SetupAsync();

        AuthorizationContext.Current = new AuthorizationScope(root, Privilege.Select);
        Assert.IsNotNull(await OpenItems(db));

        AuthorizationContext.Current = new AuthorizationScope(noob, Privilege.Select);
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await OpenItems(db))!;
        StringAssert.Contains("Missing Select privilege", ex.Message);
    }

    [Test]
    public async Task SuspendedCheck_OpensATableNoGrantNames()
    {
        (string db, _, _, Principal noob) = await SetupAsync();

        AuthorizationContext.Current = new AuthorizationScope(noob, Privilege.Select);

        using (AuthorizationContext.SuspendTableCheck())
            Assert.IsNotNull(await OpenItems(db));

        // The suspension ends with its scope: the same caller is refused again afterwards.
        Assert.ThrowsAsync<CamusDBException>(async () => await OpenItems(db));
    }

    /// <summary>
    /// A phase inside a suspension that names a privilege is reading a table a user named, so the
    /// check must come back on rather than inherit the suspension.
    /// </summary>
    [Test]
    public async Task NarrowingInsideASuspension_ChecksAgain()
    {
        (string db, _, _, Principal noob) = await SetupAsync();

        AuthorizationContext.Current = new AuthorizationScope(noob, null);

        using (AuthorizationContext.SuspendTableCheck())
        using (AuthorizationContext.WithRequiredPrivilege(Privilege.Select))
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await OpenItems(db))!;
            StringAssert.Contains("Missing Select privilege", ex.Message);
        }
    }

    [Test]
    public async Task NoPrincipal_IsEngineWorkAndIsNotChecked()
    {
        (string db, _, _, _) = await SetupAsync();

        AuthorizationContext.Current = default;
        Assert.IsNotNull(await OpenItems(db));
    }

    // ─── Superuser statements behind the middleware ───────────────────────────

    private enum Kind { Query, NonQuery, Ddl }

    /// <summary>
    /// Runs one statement through the gRPC SQL service with the real authentication middleware in
    /// front, which is how every production request arrives. Returns a description of the failure, or
    /// null. A request the middleware itself refuses is a failure too: otherwise the statement never
    /// runs and the sweep passes without evidence.
    /// </summary>
    private async Task<string?> BehindMiddleware(Kind kind, string db, string sql, string bearer)
    {
        string? failure = null;
        bool reached = false;

        AuthenticationMiddleware middleware = new(async _ =>
        {
            reached = true;
            try
            {
                switch (kind)
                {
                    case Kind.Query:
                        await sqlService.ExecuteQuery(Req(db, sql), new CapturingStreamWriter<QueryStreamMessage>(), Ctx(bearer));
                        break;
                    case Kind.NonQuery:
                        await sqlService.ExecuteNonQuery(Req(db, sql), Ctx(bearer));
                        break;
                    default:
                        await sqlService.ExecuteDdl(Req(db, sql), Ctx(bearer));
                        break;
                }
            }
            catch (RpcException e)
            {
                failure = $"{e.StatusCode}: {e.Status.Detail}";
            }
        });

        DefaultHttpContext http = new();
        http.Request.Path = kind switch
        {
            Kind.Query => "/CamusSql/ExecuteQuery",
            Kind.NonQuery => "/CamusSql/ExecuteNonQuery",
            _ => "/CamusSql/ExecuteDdl",
        };
        http.Request.IsHttps = true;
        http.Request.Headers.Authorization = $"Bearer {bearer}";

        await middleware.Invoke(http, serviceExecutor, Options);
        return reached ? failure : $"refused by the middleware with HTTP {http.Response.StatusCode}";
    }

    /// <summary>
    /// Every statement family that resolves a table, run as a superuser behind the middleware, still
    /// succeeds. Each one either declares its privilege, resolves its tables inside the statement's
    /// scope, or is a deliberate suspension — any statement doing none of those fails here.
    /// </summary>
    [Test]
    public async Task SuperuserStatementsThatResolveTables_SucceedBehindTheMiddleware()
    {
        (string db, string root, _, _) = await SetupAsync();

        (Kind Kind, string Sql)[] statements =
        [
            (Kind.NonQuery, "INSERT INTO items (id, name) VALUES (1, 'a')"),
            (Kind.NonQuery, "INSERT INTO items (id, name) VALUES (2, 'b')"),
            (Kind.Ddl,      "CREATE TABLE tags (id int64 PRIMARY KEY NOT NULL, item_id int64 NOT NULL, label string NOT NULL)"),
            (Kind.NonQuery, "INSERT INTO tags (id, item_id, label) VALUES (10, 1, 'x')"),

            (Kind.Query,    "SELECT id, name FROM items"),
            (Kind.Query,    "SELECT i.name, t.label FROM items i INNER JOIN tags t ON t.item_id = i.id"),
            (Kind.Query,    "SELECT id FROM items WHERE EXISTS (SELECT 1 FROM tags WHERE tags.item_id = items.id)"),
            (Kind.Query,    "SELECT id FROM items WHERE id IN (SELECT item_id FROM tags)"),
            (Kind.Query,    "SELECT id FROM items WHERE name = (SELECT label FROM tags LIMIT 1)"),
            (Kind.Query,    "SELECT d.id FROM (SELECT id FROM items) AS d"),
            (Kind.Query,    "EXPLAIN SELECT id FROM items WHERE name = (SELECT label FROM tags LIMIT 1)"),
            (Kind.Query,    "EXPLAIN (LOGICAL) SELECT id FROM items"),
            (Kind.Query,    "EXPLAIN (PHYSICAL) SELECT id FROM items"),
            (Kind.Query,    "EXPLAIN (ANALYZE) SELECT id FROM items"),
            (Kind.Query,    "SHOW COLUMNS FROM items"),
            (Kind.Query,    "SHOW INDEXES FROM items"),
            (Kind.Query,    "SHOW CREATE TABLE items"),
            (Kind.Query,    "ANALYZE items"),
            (Kind.Query,    "SHOW STATISTICS FOR items"),

            (Kind.NonQuery, "UPDATE items SET name = 'c' WHERE id IN (SELECT item_id FROM tags)"),
            (Kind.NonQuery, "INSERT INTO tags (id, item_id, label) SELECT id + 100, id, name FROM items"),
            (Kind.NonQuery, "CREATE TABLE items_copy AS SELECT id, name FROM items"),
            (Kind.NonQuery, "DELETE FROM tags WHERE id > 100"),

            (Kind.Ddl,      "CREATE VIEW v_items AS SELECT id, name FROM items"),
            (Kind.Query,    "SELECT id FROM v_items"),
            (Kind.Ddl,      "CREATE MATERIALIZED VIEW mv_items AS SELECT id, name FROM items"),
            (Kind.NonQuery, "REFRESH MATERIALIZED VIEW mv_items"),
            (Kind.Query,    "SELECT id FROM mv_items"),

            (Kind.Ddl,      "ALTER TABLE items ADD COLUMN note string"),
            (Kind.Ddl,      "CREATE INDEX items_name_idx ON items (name)"),
            (Kind.Ddl,      $"GRANT SELECT ON {db}.items TO noob"),
            (Kind.Ddl,      $"REVOKE SELECT ON {db}.items FROM noob"),
            (Kind.Ddl,      "TRUNCATE TABLE items_copy"),
            (Kind.Ddl,      "DROP TABLE items_copy"),
        ];

        List<string> failures = [];
        foreach ((Kind kind, string sql) in statements)
        {
            string? failure = await BehindMiddleware(kind, db, sql, root);
            if (failure is not null)
                failures.Add($"{sql} -> {failure}");
        }

        Assert.That(failures, Is.Empty, string.Join("\n", failures));

        // The sweep really ran: the statements above left rows and objects behind.
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await sqlService.ExecuteQuery(Req(db, "SELECT id FROM mv_items"), writer, Ctx(root));
        GrpcAssert.AssertSchemaFirst(writer.Written, expectedRowCount: 2);
    }
}
