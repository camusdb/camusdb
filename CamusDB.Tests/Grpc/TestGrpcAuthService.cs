
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Covers the gRPC credential exchange (<c>CamusAuth.Login</c> / <c>Logout</c>), which exists so a
/// gRPC-only deployment need not expose the HTTP port purely to obtain a token.
///
/// <para>The token a login returns must actually work on the data plane, and the reported expiry must
/// match the server's configured TTL — a client renews against that figure, so a wrong one is worse than
/// none.</para>
///
/// <para>Serial: shares one embedded Kahuna node across the fixture. The auth settings belong to the
/// services each test builds, so the node is the only remaining reason.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcAuthService : BaseTest
{

    /// <summary>
    /// Auth on, with a known signing key and bootstrap superuser — the baseline every test here starts
    /// from. A test needing different auth settings derives its own options and builds its own engine.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-grpc-auth-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
    };
    private CamusAuthService auth = null!;
    private CamusSqlService sql = null!;
    private CommandExecutor serviceExecutor = null!;


    [SetUp]
    public void SetUpGrpcAuthService() => BuildServices(Options);

    [TearDown]
    public async Task TearDownGrpcAuthService()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    /// <summary>
    /// Builds the executor and the two services in front of it under <paramref name="options"/>. They
    /// all fix their configuration when constructed, so a test that needs different auth settings
    /// rebuilds the whole stack rather than changing anything after the fact.
    /// </summary>
    private void BuildServices(CamusDBOptions options)
    {
        serviceExecutor = new CommandExecutor(
            new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        auth = new CamusAuthService(serviceExecutor, logger, options);
        sql  = new CamusSqlService(serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), options);
    }

    private static TestServerCallContext Ctx(string? bearer = null)
    {
        TestServerCallContext ctx = new();
        if (bearer is not null)
            ctx.RequestHeaders.Add("authorization", $"Bearer {bearer}");
        return ctx;
    }

    private async Task EnableAuthAsync(CamusDBOptions? options = null)
    {
        BuildServices(options ?? Options);

        await serviceExecutor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
    }

    private Task<LoginReply> LoginAsync(string user, string password)
        => auth.Login(new LoginRequest { User = user, Password = password }, Ctx());

    // ─── Login ────────────────────────────────────────────────────────────────

    [Test]
    public async Task Login_ReturnsUsableTokenAndExpiry()
    {
        await EnableAuthAsync();

        LoginReply reply = await LoginAsync("root", "root-password");

        Assert.That(reply.Token, Does.StartWith("camus_"));
        Assert.That(reply.ExpiresAtUnixMs, Is.GreaterThan(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
            "The reported deadline must be in the future or a client renews immediately");
        Assert.That(reply.ExpiresInSeconds, Is.GreaterThan(0));
    }

    [Test]
    public async Task Login_ReportedExpiryTracksTheConfiguredTtl()
    {
        // The whole point of reporting expiry is that a client stops guessing, so the figure has to
        // follow the server's TTL rather than any fixed assumption.
        await EnableAuthAsync(Options with { AccessTokenTtl = TimeSpan.FromMinutes(3) });

        LoginReply reply = await LoginAsync("root", "root-password");

        Assert.That(reply.ExpiresInSeconds, Is.InRange(150, 180),
            "A 3-minute TTL must be reported as ~180s, not the 15-minute default");

        long expectedMs = DateTimeOffset.UtcNow.AddMinutes(3).ToUnixTimeMilliseconds();
        Assert.That(reply.ExpiresAtUnixMs, Is.EqualTo(expectedMs).Within(30_000),
            "The absolute deadline and the TTL must describe the same instant");
    }

    [Test]
    public async Task Login_TokenIsAcceptedByTheDataPlane()
    {
        await EnableAuthAsync();
        LoginReply reply = await LoginAsync("root", "root-password");

        string db = "grpcauthsvc" + Guid.NewGuid().ToString("n");
        await sql.ExecuteDdl(new SqlRequest { Database = "", Sql = $"CREATE DATABASE {db}" }, Ctx(reply.Token));
        TrackDatabase(db, serviceExecutor);

        // A token that cannot drive a real statement would make the login RPC pointless.
        Assert.DoesNotThrowAsync(async () => await sql.ExecuteDdl(
            new SqlRequest { Database = db, Sql = "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL)" },
            Ctx(reply.Token)));
    }

    [Test]
    public async Task Login_WrongPassword_Unauthenticated()
    {
        await EnableAuthAsync();

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () => await LoginAsync("root", "wrong"))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public async Task Login_UnknownUser_FailsIdenticallyToAWrongPassword()
    {
        // Uniform failure: distinguishable replies would let an unauthenticated caller enumerate accounts.
        await EnableAuthAsync();

        RpcException unknown = Assert.ThrowsAsync<RpcException>(async () => await LoginAsync("ghost", "whatever"))!;
        RpcException wrongPw = Assert.ThrowsAsync<RpcException>(async () => await LoginAsync("root", "wrong"))!;

        Assert.That(unknown.StatusCode, Is.EqualTo(wrongPw.StatusCode));
        Assert.That(unknown.Status.Detail, Is.EqualTo(wrongPw.Status.Detail));
    }

    [Test]
    public async Task Login_EmptyUser_Unauthenticated()
    {
        await EnableAuthAsync();

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () => await LoginAsync("", "root-password"))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    // ─── Logout ───────────────────────────────────────────────────────────────

    [Test]
    public async Task Logout_RevokesTheTokenForTheDataPlane()
    {
        await EnableAuthAsync();
        LoginReply reply = await LoginAsync("root", "root-password");

        await auth.Logout(new LogoutRequest(), Ctx(reply.Token));

        RpcException ex = Assert.ThrowsAsync<RpcException>(async () => await sql.ExecuteDdl(
            new SqlRequest { Database = "", Sql = "CREATE DATABASE afterlogout" + Guid.NewGuid().ToString("n") },
            Ctx(reply.Token)))!;

        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unauthenticated),
            "A revoked token must stop working on the data plane, not just on the auth service");
    }

    [Test]
    public async Task Logout_WithoutATokenSucceeds()
    {
        // Idempotent by design: the requested end state (that token unusable) already holds, and
        // reporting the difference would tell an unauthenticated caller whether a token was valid.
        await EnableAuthAsync();

        Assert.DoesNotThrowAsync(async () => await auth.Logout(new LogoutRequest(), Ctx()));
    }

    [Test]
    public async Task Logout_IsRepeatable()
    {
        await EnableAuthAsync();
        LoginReply reply = await LoginAsync("root", "root-password");

        await auth.Logout(new LogoutRequest(), Ctx(reply.Token));

        Assert.DoesNotThrowAsync(async () => await auth.Logout(new LogoutRequest(), Ctx(reply.Token)));
    }
}
