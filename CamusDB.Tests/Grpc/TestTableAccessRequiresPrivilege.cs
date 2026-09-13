/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Middleware;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// An authenticated user is refused on every path that touches a table they hold no grant on, and a
/// user holding the right grant is let through on the same path.
///
/// <para>Two transports are covered here because both once reached the per-table check with no
/// privilege declared, and that check used to treat "nothing declared" as "nothing to check": the typed
/// gRPC rows service, and the four <c>EXPLAIN</c> forms. <c>EXPLAIN</c> is not a metadata read —
/// preparing a plan runs a scalar subquery against storage and prints its result as a literal — so a
/// refusal is asserted together with the absence of the stored value from everything written.</para>
///
/// <para>Each refusal test has a granted counterpart. A refusal alone also passes when the path is
/// simply broken for everyone.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTableAccessRequiresPrivilege : BaseTest
{
    private const string Secret = "classified";

    private const string SecretRowId = "0123456789abcdef01234567";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "table-access-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
    };

    private CamusSqlService sqlService = null!;
    private CamusRowsService rowsService = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpServices()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        HttpTransactionCoordinator coordinator = new(serviceExecutor);
        sqlService = new(serviceExecutor, coordinator, logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
        rowsService = new(serviceExecutor, coordinator, logger, Options);
    }

    [TearDown]
    public async Task TearDownServices()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    private static TestServerCallContext Ctx(string? bearer = null)
    {
        TestServerCallContext ctx = new();
        if (bearer is not null)
            ctx.RequestHeaders.Add("authorization", $"Bearer {bearer}");
        return ctx;
    }

    private static SqlRequest Req(string db, string sql) => new() { Database = db, Sql = sql };

    private sealed record Fixture(string Db, string Root, string Noob, string Reader);

    /// <summary>
    /// A database with <c>items</c> (one row holding <see cref="Secret"/>) and <c>other</c>. Three
    /// accounts: <c>root</c>, <c>noob</c> with no grant of any kind, and <c>reader</c> with SELECT on
    /// <c>items</c> only.
    /// </summary>
    private async Task<Fixture> SetupAsync()
    {
        await serviceExecutor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        string root = (await serviceExecutor.LoginAsync("root", "root-password")).Token;

        string db = "tableaccess" + Guid.NewGuid().ToString("n");
        await sqlService.ExecuteDdl(Req("", $"CREATE DATABASE {db}"), Ctx(root));
        TrackDatabase(db, serviceExecutor);

        await sqlService.ExecuteDdl(Req(db, "CREATE TABLE items (id oid PRIMARY KEY, secret string NOT NULL)"), Ctx(root));
        await sqlService.ExecuteDdl(Req(db, "CREATE TABLE other (id int64 PRIMARY KEY NOT NULL, secret string NOT NULL)"), Ctx(root));
        await sqlService.ExecuteNonQuery(Req(db, $"INSERT INTO items (id, secret) VALUES (str_id('{SecretRowId}'), '{Secret}')"), Ctx(root));
        await sqlService.ExecuteNonQuery(Req(db, $"INSERT INTO other (id, secret) VALUES (1, '{Secret}')"), Ctx(root));

        await sqlService.ExecuteDdl(Req("", "CREATE USER noob IDENTIFIED BY 'noob-pw'"), Ctx(root));
        await sqlService.ExecuteDdl(Req("", "CREATE USER reader IDENTIFIED BY 'reader-pw'"), Ctx(root));
        await sqlService.ExecuteDdl(Req("", $"GRANT SELECT ON {db}.items TO reader"), Ctx(root));

        string noob = (await serviceExecutor.LoginAsync("noob", "noob-pw")).Token;
        string reader = (await serviceExecutor.LoginAsync("reader", "reader-pw")).Token;
        return new Fixture(db, root, noob, reader);
    }

    private async Task<List<QueryStreamMessage>> QueryAsync(string db, string sql, string bearer)
    {
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await sqlService.ExecuteQuery(Req(db, sql), writer, Ctx(bearer));
        return writer.Written;
    }

    /// <summary>Runs a query expected to be refused and returns everything it wrote before failing.</summary>
    private List<QueryStreamMessage> AssertQueryRefused(string db, string sql, string bearer)
    {
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await sqlService.ExecuteQuery(Req(db, sql), writer, Ctx(bearer)))!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied), sql);
        return writer.Written;
    }

    private static string Dump(IEnumerable<QueryStreamMessage> messages) =>
        string.Join("\n", messages.Select(m => m.ToString()));

    /// <summary>Every row of <c>items</c> as root sees it, for asserting a refused write changed nothing.</summary>
    private async Task<string> ItemsAsRootAsync(Fixture f) =>
        Dump(await QueryAsync(f.Db, "SELECT id, secret FROM items", f.Root));

    // ─── SQL control ──────────────────────────────────────────────────────────

    [Test]
    public async Task PlainSelectWithoutGrant_IsRefused()
    {
        Fixture f = await SetupAsync();
        AssertQueryRefused(f.Db, "SELECT id FROM items", f.Noob);
    }

    // ─── EXPLAIN ──────────────────────────────────────────────────────────────

    private static readonly string[] ExplainForms =
    [
        "EXPLAIN SELECT id, secret FROM items",
        "EXPLAIN (LOGICAL) SELECT id, secret FROM items",
        "EXPLAIN (PHYSICAL) SELECT id, secret FROM items",
        "EXPLAIN (ANALYZE) SELECT id, secret FROM items",
    ];

    [Test]
    public async Task EveryExplainFormWithoutGrant_IsRefused()
    {
        Fixture f = await SetupAsync();

        foreach (string sql in ExplainForms)
        {
            List<QueryStreamMessage> written = AssertQueryRefused(f.Db, sql, f.Noob);
            Assert.That(written, Is.Empty, $"a refused {sql} must write nothing");
        }
    }

    [Test]
    public async Task EveryExplainFormWithSelect_ReturnsAPlan()
    {
        Fixture f = await SetupAsync();

        foreach (string sql in ExplainForms)
        {
            List<QueryStreamMessage> written = await QueryAsync(f.Db, sql, f.Reader);
            Assert.That(written.Count, Is.GreaterThan(1), $"{sql} must return a schema and at least one plan row");
        }
    }

    /// <summary>
    /// The plan printer shows a folded subquery's result as a literal, so an unchecked EXPLAIN would
    /// hand over a stored value. The refusal must come before the subquery runs.
    /// </summary>
    [Test]
    public async Task ExplainSubqueryLiteral_DoesNotLeakTheStoredValue()
    {
        Fixture f = await SetupAsync();

        const string sql = "EXPLAIN SELECT id FROM items WHERE secret = (SELECT secret FROM items LIMIT 1)";
        List<QueryStreamMessage> written = AssertQueryRefused(f.Db, sql, f.Noob);

        Assert.That(Dump(written), Does.Not.Contain(Secret));
    }

    /// <summary>
    /// Select on the outer table is not enough: the subquery reads <c>other</c>, and every table a
    /// plan reads is checked, not only the one after <c>FROM</c>.
    /// </summary>
    [Test]
    public async Task ExplainWithoutGrantOnTheSubqueryTable_IsRefused()
    {
        Fixture f = await SetupAsync();

        const string sql = "EXPLAIN SELECT id FROM items WHERE secret = (SELECT secret FROM other LIMIT 1)";
        List<QueryStreamMessage> written = AssertQueryRefused(f.Db, sql, f.Reader);

        Assert.That(Dump(written), Does.Not.Contain(Secret));
    }

    [Test]
    public async Task ExplainFromlessSelect_NeedsNoGrant()
    {
        Fixture f = await SetupAsync();

        Assert.That(await QueryAsync(f.Db, "SELECT 1", f.Noob), Is.Not.Empty);
        Assert.That(await QueryAsync(f.Db, "EXPLAIN SELECT 1", f.Noob), Is.Not.Empty);
    }

    /// <summary>
    /// A view is read with its owner's rights, so a user granted only the view reaches the rows through
    /// it. EXPLAIN over the view must reach the same verdict as SELECT over it, in both directions.
    /// </summary>
    [Test]
    public async Task ExplainOverAView_MatchesSelectOverTheView()
    {
        Fixture f = await SetupAsync();

        await sqlService.ExecuteDdl(Req(f.Db, "CREATE VIEW v_items AS SELECT id FROM items"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", "CREATE USER viewer IDENTIFIED BY 'viewer-pw'"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", $"GRANT SELECT ON {f.Db}.v_items TO viewer"), Ctx(f.Root));
        string viewer = (await serviceExecutor.LoginAsync("viewer", "viewer-pw")).Token;

        Assert.That(await QueryAsync(f.Db, "SELECT id FROM v_items", viewer), Is.Not.Empty);
        Assert.That(await QueryAsync(f.Db, "EXPLAIN SELECT id FROM v_items", viewer), Is.Not.Empty);

        AssertQueryRefused(f.Db, "SELECT id FROM v_items", f.Noob);
        AssertQueryRefused(f.Db, "EXPLAIN SELECT id FROM v_items", f.Noob);
    }

    [Test]
    public async Task ExplainOverRest_WithoutGrant_Is403()
    {
        Fixture f = await SetupAsync();

        foreach (string sql in ExplainForms)
        {
            ExecuteSQLController controller = new(
                serviceExecutor, new HttpTransactionCoordinator(serviceExecutor),
                new PreparedStatementRegistry(Options), logger, Options)
            {
                ControllerContext = RestContext(JsonSerializer.Serialize(new { databaseName = f.Db, sql }), f.Noob),
            };

            JsonResult result = await controller.ExecuteSQLQuery();
            Assert.That(result.StatusCode, Is.EqualTo(403), sql);
            Assert.That(JsonSerializer.Serialize(result.Value), Does.Not.Contain(Secret), sql);
        }
    }

    private static ControllerContext RestContext(string body, string bearer)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(body);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Request.Headers.Authorization = $"Bearer {bearer}";
        return new ControllerContext { HttpContext = http };
    }

    // ─── gRPC rows service ────────────────────────────────────────────────────

    private static InsertRowRequest InsertReq(string db, TxnHandle? txn = null)
    {
        InsertRowRequest req = new()
        {
            Database = db,
            Table = "items",
            Values = { { "id", new Value { IdValue = "abcdefabcdefabcdefabcdef" } }, { "secret", new Value { StringValue = "pwned" } } },
        };
        if (txn is not null)
            req.TxnHandle = txn;
        return req;
    }

    private static UpdateRowsRequest UpdateRowsReq(string db) => new()
    {
        Database = db,
        Table = "items",
        Values = { { "secret", new Value { StringValue = "pwned" } } },
    };

    private static UpdateByIdRequest UpdateByIdReq(string db) => new()
    {
        Database = db,
        Table = "items",
        Id = SecretRowId,
        Values = { { "secret", new Value { StringValue = "pwned" } } },
    };

    private static DeleteRowsRequest DeleteRowsReq(string db) => new() { Database = db, Table = "items" };

    private static RowByIdRequest ById(string db) => new() { Database = db, Table = "items", Id = SecretRowId };

    private static void AssertDenied(AsyncTestDelegate call, string what)
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(call, what)!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied), what);
    }

    [Test]
    public async Task EveryRowsRpcWithoutGrant_IsRefusedAndChangesNothing()
    {
        Fixture f = await SetupAsync();
        string before = await ItemsAsRootAsync(f);

        CapturingStreamWriter<QueryStreamMessage> queryWriter = new();
        CapturingStreamWriter<QueryStreamMessage> byIdWriter = new();

        AssertDenied(async () => await rowsService.Query(new RowQueryRequest { Database = f.Db, Table = "items" }, queryWriter, Ctx(f.Noob)), "Query");
        AssertDenied(async () => await rowsService.QueryById(ById(f.Db), byIdWriter, Ctx(f.Noob)), "QueryById");
        AssertDenied(async () => await rowsService.InsertRow(InsertReq(f.Db), Ctx(f.Noob)), "InsertRow");
        AssertDenied(async () => await rowsService.UpdateRows(UpdateRowsReq(f.Db), Ctx(f.Noob)), "UpdateRows");
        AssertDenied(async () => await rowsService.UpdateById(UpdateByIdReq(f.Db), Ctx(f.Noob)), "UpdateById");
        AssertDenied(async () => await rowsService.DeleteRows(DeleteRowsReq(f.Db), Ctx(f.Noob)), "DeleteRows");
        AssertDenied(async () => await rowsService.DeleteById(ById(f.Db), Ctx(f.Noob)), "DeleteById");

        // The schema header is built from the table's columns before any row is read; a refused caller
        // must not receive it either.
        Assert.That(queryWriter.Written, Is.Empty, "a refused Query must write nothing, not even the schema");
        Assert.That(byIdWriter.Written, Is.Empty, "a refused QueryById must write nothing, not even the schema");

        Assert.That(await ItemsAsRootAsync(f), Is.EqualTo(before), "a refused write must leave the table unchanged");
    }

    [Test]
    public async Task RowsRpcInsideAnExplicitTransaction_WithoutGrant_IsRefused()
    {
        Fixture f = await SetupAsync();
        string before = await ItemsAsRootAsync(f);

        TxnHandle writeTxn = await sqlService.StartTransaction(new StartTxnRequest { Database = f.Db }, Ctx(f.Noob));
        AssertDenied(async () => await rowsService.InsertRow(InsertReq(f.Db, writeTxn), Ctx(f.Noob)), "InsertRow in a transaction");

        TxnHandle readTxn = await sqlService.StartTransaction(new StartTxnRequest { Database = f.Db }, Ctx(f.Noob));
        RowQueryRequest query = new() { Database = f.Db, Table = "items", TxnHandle = readTxn };
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        AssertDenied(async () => await rowsService.Query(query, writer, Ctx(f.Noob)), "Query in a transaction");
        Assert.That(writer.Written, Is.Empty);

        Assert.That(await ItemsAsRootAsync(f), Is.EqualTo(before));
    }

    [Test]
    public async Task SelectOnlyGrant_ReadsThroughTheRowsApiButCannotWrite()
    {
        Fixture f = await SetupAsync();
        string before = await ItemsAsRootAsync(f);

        CapturingStreamWriter<QueryStreamMessage> queryWriter = new();
        await rowsService.Query(new RowQueryRequest { Database = f.Db, Table = "items" }, queryWriter, Ctx(f.Reader));
        GrpcAssert.AssertSchemaFirst(queryWriter.Written, expectedRowCount: 1);

        CapturingStreamWriter<QueryStreamMessage> byIdWriter = new();
        await rowsService.QueryById(ById(f.Db), byIdWriter, Ctx(f.Reader));
        GrpcAssert.AssertSchemaFirst(byIdWriter.Written, expectedRowCount: 1);

        AssertDenied(async () => await rowsService.InsertRow(InsertReq(f.Db), Ctx(f.Reader)), "InsertRow");
        AssertDenied(async () => await rowsService.UpdateRows(UpdateRowsReq(f.Db), Ctx(f.Reader)), "UpdateRows");
        AssertDenied(async () => await rowsService.UpdateById(UpdateByIdReq(f.Db), Ctx(f.Reader)), "UpdateById");
        AssertDenied(async () => await rowsService.DeleteRows(DeleteRowsReq(f.Db), Ctx(f.Reader)), "DeleteRows");
        AssertDenied(async () => await rowsService.DeleteById(ById(f.Db), Ctx(f.Reader)), "DeleteById");

        Assert.That(await ItemsAsRootAsync(f), Is.EqualTo(before));
    }

    /// <summary>
    /// Each write RPC asks for its own privilege and no other: a grant of exactly that privilege lets
    /// the call through. Checked one privilege at a time so a wrong mapping — say, Update demanding
    /// Insert — cannot hide behind a broader grant.
    /// </summary>
    [Test]
    public async Task EachWriteRpc_IsAllowedByExactlyItsOwnPrivilege()
    {
        Fixture f = await SetupAsync();

        await sqlService.ExecuteDdl(Req("", "CREATE USER inserter IDENTIFIED BY 'inserter-pw'"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", "CREATE USER updater IDENTIFIED BY 'updater-pw'"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", "CREATE USER deleter IDENTIFIED BY 'deleter-pw'"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", $"GRANT INSERT ON {f.Db}.items TO inserter"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", $"GRANT UPDATE ON {f.Db}.items TO updater"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", $"GRANT DELETE ON {f.Db}.items TO deleter"), Ctx(f.Root));
        string inserter = (await serviceExecutor.LoginAsync("inserter", "inserter-pw")).Token;
        string updater = (await serviceExecutor.LoginAsync("updater", "updater-pw")).Token;
        string deleter = (await serviceExecutor.LoginAsync("deleter", "deleter-pw")).Token;

        Assert.That((await rowsService.InsertRow(InsertReq(f.Db), Ctx(inserter))).AffectedRows, Is.EqualTo(1));
        AssertDenied(async () => await rowsService.UpdateById(UpdateByIdReq(f.Db), Ctx(inserter)), "inserter UpdateById");

        Assert.That((await rowsService.UpdateById(UpdateByIdReq(f.Db), Ctx(updater))).AffectedRows, Is.EqualTo(1));
        AssertDenied(async () => await rowsService.DeleteById(ById(f.Db), Ctx(updater)), "updater DeleteById");

        Assert.That((await rowsService.DeleteById(ById(f.Db), Ctx(deleter))).AffectedRows, Is.EqualTo(1));
        AssertDenied(async () => await rowsService.InsertRow(InsertReq(f.Db), Ctx(deleter)), "deleter InsertRow");
    }

    [Test]
    public async Task DatabaseWideGrantAndSuperuser_UseTheRowsApi()
    {
        Fixture f = await SetupAsync();

        await sqlService.ExecuteDdl(Req("", "CREATE USER dbwriter IDENTIFIED BY 'dbwriter-pw'"), Ctx(f.Root));
        await sqlService.ExecuteDdl(Req("", $"GRANT INSERT ON {f.Db}.* TO dbwriter"), Ctx(f.Root));
        string dbwriter = (await serviceExecutor.LoginAsync("dbwriter", "dbwriter-pw")).Token;

        Assert.That((await rowsService.InsertRow(InsertReq(f.Db), Ctx(dbwriter))).AffectedRows, Is.EqualTo(1));
        Assert.That((await rowsService.DeleteById(ById(f.Db), Ctx(f.Root))).AffectedRows, Is.EqualTo(1));
    }

    [Test]
    public async Task RowsRpcWithoutAToken_IsUnauthenticated()
    {
        Fixture f = await SetupAsync();

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await rowsService.InsertRow(InsertReq(f.Db), Ctx(bearer: null)))!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    /// <summary>
    /// In production the authentication middleware runs first and publishes the principal with no
    /// privilege; the service then publishes its own. The two layers must compose: the service's
    /// requirement must win for a refused user, and must not break an allowed one.
    /// </summary>
    [Test]
    public async Task RowsRpcBehindTheMiddleware_EnforcesTheServicePrivilege()
    {
        Fixture f = await SetupAsync();

        async Task<RpcException?> ThroughMiddleware(string bearer)
        {
            RpcException? failure = null;
            AuthenticationMiddleware middleware = new(async _ =>
            {
                try
                {
                    await rowsService.InsertRow(InsertReq(f.Db), Ctx(bearer));
                }
                catch (RpcException e)
                {
                    failure = e;
                }
            });

            DefaultHttpContext http = new();
            http.Request.Path = "/CamusRows/InsertRow";
            http.Request.IsHttps = true;
            http.Request.Headers.Authorization = $"Bearer {bearer}";
            await middleware.Invoke(http, serviceExecutor, Options);
            return failure;
        }

        RpcException? refused = await ThroughMiddleware(f.Noob);
        Assert.That(refused, Is.Not.Null, "a user with no grant must be refused behind the middleware");
        Assert.That(refused!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));

        Assert.That(await ThroughMiddleware(f.Root), Is.Null, "a superuser must be let through behind the middleware");
    }
}
