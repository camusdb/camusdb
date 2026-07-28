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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Verifies that the gRPC surface resolves the bearer token from request metadata, threads the
/// principal into the engine, and is enforced there: an authorized call succeeds, a missing token
/// maps to <see cref="StatusCode.Unauthenticated"/>, and an insufficiently-privileged call maps to
/// <see cref="StatusCode.PermissionDenied"/>. Toggles the process-wide auth flag, so it is
/// <c>[NonParallelizable]</c> and restores config in teardown.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcSqlAuth : BaseTest
{
    private CamusSqlService service = null!;
    private CommandExecutor serviceExecutor = null!;

    private bool savedEnabled;
    private string savedServerKey = "";
    private string savedBootstrapUser = "";
    private string savedBootstrapPassword = "";

    [SetUp]
    public void SetUpGrpcAuth()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        service = new(serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge());

        savedEnabled = CamusConfig.AuthenticationEnabled;
        savedServerKey = CamusConfig.AccessTokenServerKey;
        savedBootstrapUser = CamusConfig.BootstrapSuperuser;
        savedBootstrapPassword = CamusConfig.BootstrapSuperuserPassword;
    }

    [TearDown]
    public async Task TearDownGrpcAuth()
    {
        CamusConfig.AuthenticationEnabled = savedEnabled;
        CamusConfig.AccessTokenServerKey = savedServerKey;
        CamusConfig.BootstrapSuperuser = savedBootstrapUser;
        CamusConfig.BootstrapSuperuserPassword = savedBootstrapPassword;
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

    // Enables auth, bootstraps a superuser, and returns (dbname, superuser token) with an `items`
    // table already created through the gRPC surface as the superuser.
    private async Task<(string db, string rootToken)> SetupAuthenticated()
    {
        CamusConfig.AccessTokenServerKey = "test-grpc-key";
        CamusConfig.BootstrapSuperuser = "root";
        CamusConfig.BootstrapSuperuserPassword = "root-password";
        CamusConfig.AuthenticationEnabled = true;

        await serviceExecutor.EnsureBootstrapSuperuserAsync();
        string rootToken = (await serviceExecutor.LoginAsync("root", "root-password")).Token;

        string db = "grpcauthdb" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(Req("", $"CREATE DATABASE {db}"), Ctx(rootToken));
        TrackDatabase(db, serviceExecutor);
        await service.ExecuteDdl(Req(db, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, name string NOT NULL)"), Ctx(rootToken));
        return (db, rootToken);
    }

    [Test]
    public async Task AuthDisabled_NoTokenWorks()
    {
        CamusConfig.AuthenticationEnabled = false;

        string db = "grpcauthdb" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(Req("", $"CREATE DATABASE {db}"), Ctx());
        TrackDatabase(db, serviceExecutor);
        await service.ExecuteDdl(Req(db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL)"), Ctx());
        Assert.Pass();
    }

    [Test]
    public async Task NoToken_Unauthenticated()
    {
        (string db, _) = await SetupAuthenticated();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ExecuteQuery(Req(db, "SELECT id FROM items"), writer, Ctx(bearer: null)))!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unauthenticated));
    }

    [Test]
    public async Task SuperuserToken_QueryWorks()
    {
        (string db, string rootToken) = await SetupAuthenticated();

        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await service.ExecuteQuery(Req(db, "SELECT id FROM items"), writer, Ctx(rootToken));
        // No exception = authorized; the schema message is always written first.
        Assert.That(writer.Written.Count, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task GrantedUser_SelectOk_InsertPermissionDenied()
    {
        (string db, string rootToken) = await SetupAuthenticated();

        await service.ExecuteDdl(Req("", "CREATE USER reader IDENTIFIED BY 'reader-pw'"), Ctx(rootToken));
        await service.ExecuteDdl(Req("", $"GRANT SELECT ON {db}.* TO reader"), Ctx(rootToken));
        string readerToken = (await serviceExecutor.LoginAsync("reader", "reader-pw")).Token;

        // SELECT is granted.
        CapturingStreamWriter<QueryStreamMessage> writer = new();
        await service.ExecuteQuery(Req(db, "SELECT id FROM items"), writer, Ctx(readerToken));

        // INSERT is not — the gate rejects it, mapped to PermissionDenied.
        RpcException ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ExecuteNonQuery(Req(db, "INSERT INTO items (id, name) VALUES (1, 'x')"), Ctx(readerToken)))!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
