/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.App.Controllers;
using CamusDB.App.Middleware;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Integration tests for the REST auth surface: <c>/login</c> issues a token, the bearer flow is
/// resolved and enforced by the engine (200 authorized / 401 unauthenticated / 403 denied), and a
/// plaintext connection is refused when TLS is required. Driven through the real controllers with a
/// <see cref="DefaultHttpContext"/>. Toggles the process-wide auth flag, so <c>[NonParallelizable]</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestHttpAuth : BaseTest
{
    /// <summary>
    /// Auth on, with a known signing key and bootstrap superuser — the baseline every test here starts
    /// from. A test that needs auth off, a different token lifetime, or a relaxed transport requirement
    /// derives its own options from this and builds its own engine.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private ILogger<ICamusDB> Logger => logger;

    private static ControllerContext Context(string body, string? bearer, bool https)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(body);
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = https;
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return new ControllerContext { HttpContext = http };
    }

    private async Task<JsonResult> LoginRest(
        CommandExecutor ex, string user, string password, bool https = true, CamusDBOptions? options = null)
    {
        AuthController c = new(ex, new HttpTransactionCoordinator(ex), Logger, options ?? Options)
        {
            ControllerContext = Context(JsonSerializer.Serialize(new { user, password }), bearer: null, https)
        };
        return await c.Login();
    }

    private async Task<string> TokenFor(
        CommandExecutor ex, string user, string password, CamusDBOptions? options = null)
    {
        JsonResult r = await LoginRest(ex, user, password, options: options);
        return ((LoginResponse)r.Value!).Token!;
    }

    private async Task<JsonResult> QueryRest(
        CommandExecutor ex, string db, string sql, string? bearer, bool https = true, CamusDBOptions? options = null)
    {
        CamusDBOptions effective = options ?? Options;
        ExecuteSQLController c = new(ex, new HttpTransactionCoordinator(ex), new PreparedStatementRegistry(effective), Logger, effective)
        {
            ControllerContext = Context(JsonSerializer.Serialize(new { databaseName = db, sql }), bearer, https)
        };
        return await c.ExecuteSQLQuery();
    }

    private async Task<JsonResult> NonQueryRest(
        CommandExecutor ex, string db, string sql, string? bearer, bool https = true, CamusDBOptions? options = null)
    {
        CamusDBOptions effective = options ?? Options;
        ExecuteSQLController c = new(ex, new HttpTransactionCoordinator(ex), new PreparedStatementRegistry(effective), Logger, effective)
        {
            ControllerContext = Context(JsonSerializer.Serialize(new { databaseName = db, sql }), bearer, https)
        };
        return await c.ExecuteNonSQLQuery();
    }

    // Enable auth, create a db + table via the engine as the bootstrap superuser, plus a SELECT-only
    // reader. Returns (db, executor). The REST layer is what the tests exercise.
    private async Task<(string db, CommandExecutor ex)> Setup(CamusDBOptions? options = null)
    {
        CommandExecutor ex = CreateCommandExecutor(options ?? Options);
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync();
        Principal root = await ex.ResolvePrincipalAsync((await ex.LoginAsync("root", "root-pw")).Token);

        var d = await ex.OpenDatabase(db);
        var tx = await d.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL)", null, root));
        await d.Transactions.CommitAsync(tx);

        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: "CREATE USER reader IDENTIFIED BY 'reader-pw'", parameters: null, principal: root));
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: $"GRANT SELECT ON {db}.* TO reader", parameters: null, principal: root));
        return (db, ex);
    }

    [Test]
    public async Task Login_ReturnsToken()
    {
        (_, CommandExecutor ex) = await Setup();
        JsonResult r = await LoginRest(ex, "root", "root-pw");
        LoginResponse resp = (LoginResponse)r.Value!;
        Assert.AreEqual("ok", resp.Status);
        Assert.IsNotNull(resp.Token);
        Assert.IsTrue(resp.Token!.StartsWith("camus_"));
    }

    [Test]
    public async Task Login_ReportsTokenExpiry()
    {
        // A client renews against these fields; without them it must guess a lifetime and guess wrong
        // whenever an operator shortens AccessTokenTtl.
        CamusDBOptions shortTtl = Options with { AccessTokenTtl = TimeSpan.FromMinutes(3) };

        (_, CommandExecutor ex) = await Setup(shortTtl);

        LoginResponse resp = (LoginResponse)(await LoginRest(ex, "root", "root-pw", options: shortTtl)).Value!;

        Assert.IsNotNull(resp.ExpiresInSeconds);
        Assert.That(resp.ExpiresInSeconds!.Value, Is.InRange(150, 180),
            "The reported TTL must follow the configured one, not a fixed default");
        Assert.IsNotNull(resp.ExpiresAtUnixMs);
        Assert.That(resp.ExpiresAtUnixMs!.Value,
            Is.EqualTo(DateTimeOffset.UtcNow.AddMinutes(3).ToUnixTimeMilliseconds()).Within(30_000),
            "The absolute deadline and the TTL must describe the same instant");
    }

    [Test]
    public async Task Logout_ReportsNoExpiry()
    {
        (_, CommandExecutor ex) = await Setup();
        string token = await TokenFor(ex, "root", "root-pw");

        AuthController c = new(ex, new HttpTransactionCoordinator(ex), Logger, Options)
        {
            ControllerContext = Context("", bearer: token, https: true)
        };
        LoginResponse resp = (LoginResponse)(await c.Logout()).Value!;

        Assert.AreEqual("ok", resp.Status);
        Assert.IsNull(resp.ExpiresAtUnixMs, "Logout mints nothing, so it has no expiry to report");
        Assert.IsNull(resp.ExpiresInSeconds);
    }

    [Test]
    public async Task Login_WrongPassword_401()
    {
        (_, CommandExecutor ex) = await Setup();
        JsonResult r = await LoginRest(ex, "root", "nope");
        Assert.AreEqual(401, r.StatusCode);
    }

    [Test]
    public async Task BearerToken_AuthorizesQuery()
    {
        (string db, CommandExecutor ex) = await Setup();
        string token = await TokenFor(ex, "root", "root-pw");
        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", token);
        Assert.AreEqual(200, r.StatusCode ?? 200);
    }

    [Test]
    public async Task NoToken_401()
    {
        (string db, CommandExecutor ex) = await Setup();
        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", bearer: null);
        Assert.AreEqual(401, r.StatusCode);
    }

    [Test]
    public async Task BadToken_401()
    {
        (string db, CommandExecutor ex) = await Setup();
        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", bearer: "camus_bogus.bogus");
        Assert.AreEqual(401, r.StatusCode);
    }

    [Test]
    public async Task DeniedUser_403()
    {
        (string db, CommandExecutor ex) = await Setup();
        string reader = await TokenFor(ex, "reader", "reader-pw");
        // reader has SELECT only — an INSERT must be forbidden.
        JsonResult r = await NonQueryRest(ex, db, "INSERT INTO items (id) VALUES (1)", reader);
        Assert.AreEqual(403, r.StatusCode);
    }

    [Test]
    public async Task Plaintext_Refused_400()
    {
        (_, CommandExecutor ex) = await Setup();
        // Non-HTTPS, non-loopback (DefaultHttpContext has a null remote IP) → refused.
        JsonResult r = await LoginRest(ex, "root", "root-pw", https: false);
        Assert.AreEqual(400, r.StatusCode);
    }

    // ── H1: transport-wide middleware closes the legacy-route bypass ──────────

    private static DefaultHttpContext MiddlewareContext(string path, string? bearer)
    {
        DefaultHttpContext http = new();
        http.Request.Path = path;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return http;
    }

    [Test]
    public async Task LegacyRoute_NoToken_Rejected401()
    {
        (_, CommandExecutor ex) = await Setup();
        bool nextCalled = false;
        AuthenticationMiddleware mw = new(_ => { nextCalled = true; return Task.CompletedTask; });

        DefaultHttpContext http = MiddlewareContext("/insert", bearer: null);
        await mw.Invoke(http, ex, Options);

        Assert.AreEqual(401, http.Response.StatusCode, "an unauthenticated legacy route must be rejected");
        Assert.IsFalse(nextCalled, "the request must not reach the controller");
    }

    [Test]
    public async Task LegacyRoute_ValidToken_PassesWithRoutePrivilege()
    {
        (_, CommandExecutor ex) = await Setup();
        string token = await TokenFor(ex, "root", "root-pw");

        bool nextCalled = false;
        AuthorizationScope captured = default;
        AuthenticationMiddleware mw = new(_ =>
        {
            nextCalled = true;
            captured = AuthorizationContext.Current; // set by the middleware for this request flow
            return Task.CompletedTask;
        });

        DefaultHttpContext http = MiddlewareContext("/insert", token);
        await mw.Invoke(http, ex, Options);

        Assert.IsTrue(nextCalled);
        Assert.IsNotNull(captured.Principal, "the principal must be published for per-table enforcement");
        Assert.AreEqual(Privilege.Insert, captured.RequiredPrivilege, "the route's privilege must be published");
    }

    [Test]
    public async Task InternalRoute_NoNodeSecret_Rejected()
    {
        (_, CommandExecutor ex) = await Setup();
        bool nextCalled = false;
        AuthenticationMiddleware mw = new(_ => { nextCalled = true; return Task.CompletedTask; });

        // No node secret configured (Setup does not set one) → internal forwarding is refused.
        DefaultHttpContext http = MiddlewareContext("/internal/schema-ddl/create-table", bearer: null);
        await mw.Invoke(http, ex, Options);

        Assert.AreEqual(401, http.Response.StatusCode);
        Assert.IsFalse(nextCalled);
    }

    // The transport-wide middleware also fronts the gRPC endpoints, so the gRPC credential-exchange
    // methods must be exempt from it — a caller has no token until Login returns one, and a middleware
    // that demanded one would make gRPC authentication impossible to bootstrap.

    [Test]
    public async Task GrpcLoginRoute_ReachesTheServiceWithoutAToken()
    {
        (_, CommandExecutor ex) = await Setup();
        bool nextCalled = false;
        AuthenticationMiddleware mw = new(_ => { nextCalled = true; return Task.CompletedTask; });

        DefaultHttpContext http = MiddlewareContext("/CamusAuth/Login", bearer: null);
        await mw.Invoke(http, ex, Options);

        Assert.IsTrue(nextCalled, "gRPC login must not require the token it exists to issue");
        Assert.AreNotEqual(401, http.Response.StatusCode);
    }

    [Test]
    public async Task GrpcLogoutRoute_ReachesTheServiceWithoutAToken()
    {
        (_, CommandExecutor ex) = await Setup();
        bool nextCalled = false;
        AuthenticationMiddleware mw = new(_ => { nextCalled = true; return Task.CompletedTask; });

        DefaultHttpContext http = MiddlewareContext("/CamusAuth/Logout", bearer: null);
        await mw.Invoke(http, ex, Options);

        Assert.IsTrue(nextCalled);
        Assert.AreNotEqual(401, http.Response.StatusCode);
    }

    [Test]
    public async Task GrpcDataPlaneRoute_StillRequiresAToken()
    {
        // The exemption must be exactly the auth methods — exempting the whole gRPC surface would leave
        // the data plane open.
        (_, CommandExecutor ex) = await Setup();
        bool nextCalled = false;
        AuthenticationMiddleware mw = new(_ => { nextCalled = true; return Task.CompletedTask; });

        DefaultHttpContext http = MiddlewareContext("/CamusSql/ExecuteQuery", bearer: null);
        await mw.Invoke(http, ex, Options);

        Assert.AreEqual(401, http.Response.StatusCode);
        Assert.IsFalse(nextCalled);
    }

    [Test]
    public async Task Disabled_NoTokenWorks()
    {
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };

        CommandExecutor ex = CreateCommandExecutor(authOff);
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);
        var d = await ex.OpenDatabase(db);
        var tx = await d.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL)", null));
        await d.Transactions.CommitAsync(tx);

        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", bearer: null, https: false, options: authOff);
        Assert.AreEqual(200, r.StatusCode ?? 200);
    }

    // The request contexts below have no RemoteIpAddress, so the loopback exemption does not apply and
    // the TLS requirement is actually exercised.

    [Test]
    public async Task PlaintextRequest_RefusedWhileTlsIsRequired()
    {
        CamusDBOptions tlsRequired = Options with { RequireTlsWhenAuthEnabled = true };

        (string db, CommandExecutor ex) = await Setup(tlsRequired);
        string token = await TokenFor(ex, "root", "root-pw", tlsRequired);

        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", token, https: false, options: tlsRequired);

        Assert.AreEqual(CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.InsecureTransport), r.StatusCode);
    }

    [Test]
    public async Task PlaintextRequest_AllowedOnceTheTlsRequirementIsTurnedOff()
    {
        // The deployment shape this exists for: TLS terminates at an ingress/sidecar, so the node itself
        // only ever sees plaintext on the inside hop and would reject every forwarded request.
        CamusDBOptions tlsRelaxed = Options with { RequireTlsWhenAuthEnabled = false };

        (string db, CommandExecutor ex) = await Setup(tlsRelaxed);
        string token = await TokenFor(ex, "root", "root-pw", tlsRelaxed);

        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", token, https: false, options: tlsRelaxed);

        Assert.AreEqual(200, r.StatusCode ?? 200);
    }

    [Test]
    public async Task TurningOffTlsDoesNotTurnOffAuthentication()
    {
        // Relaxing the transport requirement must not become a back door: credentials are still required.
        CamusDBOptions tlsRelaxed = Options with { RequireTlsWhenAuthEnabled = false };

        (string db, CommandExecutor ex) = await Setup(tlsRelaxed);

        JsonResult r = await QueryRest(ex, db, "SELECT id FROM items", bearer: null, https: false, options: tlsRelaxed);

        Assert.AreEqual(401, r.StatusCode);
    }
}
