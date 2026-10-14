/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Diagnostics;
using CamusDB.Grpc;
using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Middleware;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// The node stats call through both transports — gRPC <c>GetNodeStats</c> and REST
/// <c>GET /v1/admin/stats</c> — under authentication: no token is refused before the handler runs, an
/// account without MONITOR is refused with <see cref="CamusDBErrorCodes.InsufficientPrivilege"/>, an
/// account with it (and a superuser) is served, a revoke takes effect, and with authentication off
/// anyone is served. Also pins the REST field names and the foreground-gauge exemption.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNodeStatsTransports : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "node-stats-transport-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private CommandExecutor executor = null!;
    private NodeStatsCollector collector = null!;
    private CamusSqlService service = null!;

    private void BuildServices(CamusDBOptions options)
    {
        executor = new CommandExecutor(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);

        NodeResourceSampler sampler = new(NodeStoragePaths.From(options, TestNode!.Options), NullLogger.Instance);
        sampler.Sample();
        collector = new NodeStatsCollector(executor, TestNode!, sampler, "test-version");

        service = new CamusSqlService(executor, new HttpTransactionCoordinator(executor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), options, routing: null, nodeStats: collector);
    }

    [TearDown]
    public async Task TearDownServices()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    /// <summary>Root, an account with no grants (<c>plain</c>) and an account holding MONITOR (<c>mon</c>).</summary>
    private async Task<(string root, string plain, string mon)> SetupAccountsAsync()
    {
        BuildServices(Options);

        await executor.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        string root = (await executor.LoginAsync("root", "root-pw")).Token;

        await ServerDdl("CREATE USER plain IDENTIFIED BY 'plain-pw'", root);
        await ServerDdl("CREATE USER mon IDENTIFIED BY 'mon-pw'", root);
        await ServerDdl("GRANT MONITOR ON *.* TO mon", root);

        return (root, (await executor.LoginAsync("plain", "plain-pw")).Token, (await executor.LoginAsync("mon", "mon-pw")).Token);
    }

    private async Task ServerDdl(string sql, string token)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: sql, parameters: null, principal: await executor.ResolvePrincipalAsync(token)));

    private static TestServerCallContext Ctx(string? bearer = null)
    {
        TestServerCallContext ctx = new();
        if (bearer is not null)
            ctx.RequestHeaders.Add("authorization", $"Bearer {bearer}");
        return ctx;
    }

    private AdminStatsController Rest(string? bearer, CamusDBOptions? options = null)
    {
        DefaultHttpContext http = new();
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = IPAddress.Parse("10.1.2.3");
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";

        return new AdminStatsController(executor, new HttpTransactionCoordinator(executor), logger, options ?? Options, collector)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    private static void AssertServed(NodeStatsReply reply)
    {
        Assert.AreEqual("test-version", reply.Node.Version);
        Assert.GreaterOrEqual(reply.Disks.Count, 1);
        Assert.AreEqual("data", reply.Disks[0].Role);
        Assert.Greater(reply.Disks[0].TotalBytes, 0);
        Assert.AreEqual(Environment.ProcessorCount, reply.Cpu.ProcessorCount);
        Assert.Greater(reply.Memory.ProcessWorkingSetBytes, 0);
    }

    // ─── gRPC ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Grpc_NoToken_IsUnauthenticated()
    {
        await SetupAccountsAsync();

        RpcException ex = Assert.ThrowsAsync<RpcException>(() => service.GetNodeStats(new NodeStatsRequest(), Ctx()))!;
        Assert.AreEqual(StatusCode.Unauthenticated, ex.StatusCode);
    }

    [Test]
    public async Task Grpc_AccountWithoutMonitor_IsPermissionDenied()
    {
        (_, string plain, _) = await SetupAccountsAsync();

        RpcException ex = Assert.ThrowsAsync<RpcException>(() => service.GetNodeStats(new NodeStatsRequest(), Ctx(plain)))!;
        Assert.AreEqual(StatusCode.PermissionDenied, ex.StatusCode);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Trailers.GetValue("camus-error-code"));
    }

    [Test]
    public async Task Grpc_MonitorAccountAndSuperuser_AreServed()
    {
        (string root, _, string mon) = await SetupAccountsAsync();

        AssertServed(await service.GetNodeStats(new NodeStatsRequest(), Ctx(mon)));
        AssertServed(await service.GetNodeStats(new NodeStatsRequest(), Ctx(root)));
    }

    [Test]
    public async Task Grpc_RevokingMonitor_RefusesTheNextCall()
    {
        (string root, _, string mon) = await SetupAccountsAsync();
        AssertServed(await service.GetNodeStats(new NodeStatsRequest(), Ctx(mon)));

        await ServerDdl("REVOKE MONITOR ON *.* FROM mon", root);

        // The same token: authorization is re-read from the catalog, not frozen into the token.
        RpcException ex = Assert.ThrowsAsync<RpcException>(() => service.GetNodeStats(new NodeStatsRequest(), Ctx(mon)))!;
        Assert.AreEqual(StatusCode.PermissionDenied, ex.StatusCode);
    }

    [Test]
    public async Task Grpc_AuthenticationOff_ServesAnyCaller()
    {
        BuildServices(Options with { AuthenticationEnabled = false });

        AssertServed(await service.GetNodeStats(new NodeStatsRequest(), Ctx()));
    }

    [Test]
    public async Task Grpc_TheReplyCarriesTheCollectorsAnswer()
    {
        (string root, _, _) = await SetupAccountsAsync();

        NodeStats expected = collector.Collect();
        NodeStatsReply reply = await service.GetNodeStats(new NodeStatsRequest(), Ctx(root));

        Assert.AreEqual(expected.Disks.Count, reply.Disks.Count);
        Assert.AreEqual(expected.Disks[0].Path, reply.Disks[0].Path);
        Assert.AreEqual(expected.Disks[0].MinFreeDiskBytes, reply.Disks[0].MinFreeDiskBytes);
        Assert.AreEqual(expected.Disks[0].BelowWatermark, reply.Disks[0].BelowWatermark);
        Assert.AreEqual(expected.Disks[0].EngineBytes, reply.Disks[0].EngineBytes);
        Assert.AreEqual(expected.Node.NodeId, reply.Node.NodeId);
        Assert.AreEqual(expected.Memory.GcMemoryLimitBytes, reply.Memory.GcMemoryLimitBytes);
        Assert.AreEqual(expected.Cpu.LoadAverage15m, reply.Cpu.LoadAverage15M);
        Assert.AreEqual(expected.Errors.Count, reply.Errors.Count);
    }

    [Test]
    public async Task Grpc_GetNodeStats_IsNotCountedAsForegroundLoad()
    {
        await SetupAccountsAsync();

        ForegroundRequestGauge gauge = new();
        ForegroundRequestGaugeInterceptor interceptor = new(gauge);

        int seenByStats = -1;
        await interceptor.UnaryServerHandler<NodeStatsRequest, NodeStatsReply>(
            new NodeStatsRequest(), new TestServerCallContext(method: "/CamusSql/GetNodeStats"),
            (_, _) => { seenByStats = gauge.InFlight; return Task.FromResult(new NodeStatsReply()); });

        int seenByStatement = -1;
        await interceptor.UnaryServerHandler<SqlRequest, NonQueryReply>(
            new SqlRequest(), new TestServerCallContext(method: "/CamusSql/ExecuteNonQuery"),
            (_, _) => { seenByStatement = gauge.InFlight; return Task.FromResult(new NonQueryReply()); });

        Assert.AreEqual(0, seenByStats, "a monitor's poll is not foreground work");
        Assert.AreEqual(1, seenByStatement, "a statement still counts");
        Assert.AreEqual(0, gauge.InFlight);
    }

    // ─── REST ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Rest_AccountWithoutMonitor_Is403WithInsufficientPrivilege()
    {
        (_, string plain, _) = await SetupAccountsAsync();

        JsonResult result = await Rest(plain).GetStats();

        Assert.AreEqual(403, result.StatusCode);
        AdminStatsResponse body = (AdminStatsResponse)result.Value!;
        Assert.AreEqual("failed", body.Status);
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, body.Code);
        Assert.IsNull(body.Disks, "a refusal carries no stats");
    }

    [Test]
    public async Task Rest_MonitorAccountAndSuperuser_AreServed()
    {
        (string root, _, string mon) = await SetupAccountsAsync();

        foreach (string token in new[] { mon, root })
        {
            JsonResult result = await Rest(token).GetStats();

            Assert.IsNull(result.StatusCode, "200");
            AdminStatsResponse body = (AdminStatsResponse)result.Value!;
            Assert.AreEqual("ok", body.Status);
            Assert.AreEqual("data", body.Disks![0].Role);
            Assert.Greater(body.Disks[0].TotalBytes, 0);
            Assert.IsNotNull(body.Cpu);
            Assert.IsNotNull(body.Memory);
            Assert.IsNotNull(body.Errors);
        }
    }

    [Test]
    public async Task Rest_AuthenticationOff_ServesAnyCaller()
    {
        CamusDBOptions open = Options with { AuthenticationEnabled = false };
        BuildServices(open);

        JsonResult result = await Rest(bearer: null, open).GetStats();

        Assert.IsNull(result.StatusCode);
        Assert.AreEqual("ok", ((AdminStatsResponse)result.Value!).Status);
    }

    [Test]
    public async Task Rest_TheRouteIsNotAnonymous_TheMiddlewareDemandsAToken()
    {
        (_, _, string mon) = await SetupAccountsAsync();

        bool reached = false;
        AuthenticationMiddleware middleware = new(_ => { reached = true; return Task.CompletedTask; });

        DefaultHttpContext anonymous = MiddlewareContext("/v1/admin/stats", bearer: null);
        await middleware.Invoke(anonymous, executor, Options);
        Assert.AreEqual(401, anonymous.Response.StatusCode);
        Assert.IsFalse(reached);

        DefaultHttpContext signedIn = MiddlewareContext("/v1/admin/stats", mon);
        await middleware.Invoke(signedIn, executor, Options);
        Assert.IsTrue(reached, "a token passes the middleware; the controller makes the MONITOR decision");
    }

    [Test]
    public async Task Rest_FieldNames_AreTheCamelCaseFormsOfTheProtoFields()
    {
        (string root, _, _) = await SetupAccountsAsync();

        AdminStatsResponse body = (AdminStatsResponse)(await Rest(root).GetStats()).Value!;
        string json = JsonSerializer.Serialize(body, new JsonSerializerOptions(JsonSerializerDefaults.Web));

        foreach (string name in new[]
        {
            "\"status\":\"ok\"", "\"node\":", "\"nodeId\":", "\"sampledAtUnixMs\":", "\"uptimeSeconds\":",
            "\"disks\":", "\"role\":\"data\"", "\"mountPoint\":", "\"totalBytes\":", "\"freeBytes\":",
            "\"availableBytes\":", "\"usedBytes\":", "\"minFreeDiskBytes\":", "\"belowWatermark\":", "\"engineBytes\":",
            "\"cpu\":", "\"processCpuPercent\":", "\"processCpuWindowMs\":", "\"processorCount\":",
            "\"loadAverage1m\":", "\"loadAverage5m\":", "\"loadAverage15m\":",
            "\"memory\":", "\"processWorkingSetBytes\":", "\"gcHeapBytes\":", "\"gcCommittedBytes\":",
            "\"gcMemoryLimitBytes\":", "\"hostTotalBytes\":", "\"hostAvailableBytes\":", "\"errors\":",
        })
            StringAssert.Contains(name, json);

        StringAssert.DoesNotContain("\"code\"", json, "a success carries no error code");
    }

    private static DefaultHttpContext MiddlewareContext(string path, string? bearer)
    {
        DefaultHttpContext http = new();
        http.Request.Path = path;
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = IPAddress.Parse("10.1.2.3");
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return http;
    }
}
