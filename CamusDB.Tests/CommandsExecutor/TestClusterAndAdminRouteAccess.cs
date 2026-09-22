/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.App.Controllers;
using CamusDB.App.Middleware;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Who may read the node's cluster topology, and who may close a database.
///
/// <para>The four detail reads name peer endpoints, partitions, leaders and commit indexes. That is
/// the material <c>SHOW ENGINE STATS</c> is superuser-gated for, so they follow that statement's bar:
/// superuser with authentication on, unrestricted with it off — where any client that reaches the port
/// can already run every statement. The readiness probe stays reachable without a credential, because
/// an orchestrator has none, and withholds the same topology from a caller who is not a superuser.</para>
///
/// <para><c>/close-db</c> joins the database-lifecycle routes: a close rolls back every transaction
/// active in that database on the node, so it is not a read-only convenience.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestClusterAndAdminRouteAccess : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "cluster-route-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    /// <summary>Root plus a plain authenticated account that holds no superuser attribute.</summary>
    private async Task<(CommandExecutor ex, string root, string plain)> SetupAsync(CamusDBOptions? options = null)
    {
        CamusDBOptions effective = options ?? Options;
        CommandExecutor ex = CreateCommandExecutor(effective);

        if (!effective.AuthenticationEnabled)
            return (ex, "", "");

        await ex.EnsureBootstrapSuperuserAsync(effective.BootstrapSuperuser, effective.BootstrapSuperuserPassword);
        string root = (await ex.LoginAsync("root", "root-pw")).Token;

        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: "CREATE USER plain IDENTIFIED BY 'plain-pw'", parameters: null,
            principal: await ex.ResolvePrincipalAsync(root)));

        string plain = (await ex.LoginAsync("plain", "plain-pw")).Token;
        return (ex, root, plain);
    }

    private ClusterController Cluster(CommandExecutor ex, string? bearer, CamusDBOptions? options = null, IPAddress? remote = null)
    {
        CamusDBOptions effective = options ?? Options;
        DefaultHttpContext http = new();
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = remote ?? IPAddress.Parse("10.1.2.3");
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";

        return new ClusterController(ex, new HttpTransactionCoordinator(ex), logger, effective, TestNode!)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    private DashboardController Dashboard(CommandExecutor ex, string? bearer, CamusDBOptions? options = null, IPAddress? remote = null)
    {
        CamusDBOptions effective = options ?? Options;
        DefaultHttpContext http = new();
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = remote ?? IPAddress.Loopback;
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";

        return new DashboardController(
            ex, new HttpTransactionCoordinator(ex), logger, effective, TestNode!,
            new ForegroundRequestGauge(), new PreparedStatementRegistry(effective))
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    // ─── The four detail reads ────────────────────────────────────────────────

    [Test]
    public async Task TopologyReads_AreRefusedToANonSuperuser()
    {
        (CommandExecutor ex, _, string plain) = await SetupAsync();

        ClusterController controller = Cluster(ex, plain);

        foreach ((string name, JsonResult result) in new (string, JsonResult)[]
        {
            ("membership", await controller.GetMembership()),
            ("placement", await controller.GetPlacement()),
            ("backfill-status", await controller.GetBackfillStatus()),
            ("snapshot-status", await controller.GetSnapshotStatus()),
        })
        {
            Assert.AreEqual(403, result.StatusCode, name);
            Assert.IsNotInstanceOf<ClusterMembershipResponse>(result.Value, name);
        }
    }

    [Test]
    public async Task TopologyReads_AreServedToASuperuser()
    {
        (CommandExecutor ex, string root, _) = await SetupAsync();

        ClusterController controller = Cluster(ex, root);

        JsonResult membership = await controller.GetMembership();
        Assert.AreNotEqual(403, membership.StatusCode);
        Assert.IsInstanceOf<ClusterMembershipResponse>(membership.Value);

        Assert.AreNotEqual(403, (await controller.GetPlacement()).StatusCode);
        Assert.AreNotEqual(403, (await controller.GetBackfillStatus()).StatusCode);
        Assert.AreNotEqual(403, (await controller.GetSnapshotStatus()).StatusCode);
    }

    /// <summary>
    /// With authentication off there is no principal to hold anyone to, and every statement — including
    /// <c>SHOW ENGINE STATS</c> — is already open to whoever reaches the port. Gating these reads there
    /// would protect nothing and would break an operator's tooling, so the reads stay open. The fix for
    /// that deployment is authentication, not a loopback rule on four read routes.
    /// </summary>
    [Test]
    public async Task TopologyReads_StayOpenWhenAuthenticationIsOff()
    {
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };
        (CommandExecutor ex, _, _) = await SetupAsync(authOff);

        JsonResult membership = await Cluster(ex, bearer: null, authOff).GetMembership();

        Assert.AreNotEqual(403, membership.StatusCode);
        Assert.IsInstanceOf<ClusterMembershipResponse>(membership.Value);
    }

    // ─── The readiness probe ──────────────────────────────────────────────────

    [Test]
    public async Task HealthProbe_WithoutACredential_ReportsReadinessAndNoTopology()
    {
        (CommandExecutor ex, _, _) = await SetupAsync();

        JsonResult result = await Cluster(ex, bearer: null).GetHealth();
        ClusterHealthResponse health = (ClusterHealthResponse)result.Value!;
        ClusterHealthResponse full = ClusterController.BuildHealth(TestNode!.Raft);

        Assert.AreEqual(full.Ready, health.Ready, "a probe must still learn whether the node serves");
        Assert.AreEqual(full.Ready ? 200 : 503, result.StatusCode);

        Assert.AreEqual("", health.LocalRole, "the node's roster role is topology");
        Assert.AreEqual(0, health.HostedPartitions);
        Assert.IsEmpty(health.StalledPartitions, "a stalled partition's id names a partition");
    }

    [Test]
    public async Task HealthProbe_ForASuperuser_ReportsTheTopologyToo()
    {
        (CommandExecutor ex, string root, _) = await SetupAsync();

        ClusterHealthResponse health = (ClusterHealthResponse)(await Cluster(ex, root).GetHealth()).Value!;
        ClusterHealthResponse full = ClusterController.BuildHealth(TestNode!.Raft);

        Assert.AreEqual(full.LocalRole, health.LocalRole);
        Assert.AreEqual(full.HostedPartitions, health.HostedPartitions);
    }

    [Test]
    public async Task HealthProbe_ForANonSuperuser_WithholdsTheTopology()
    {
        (CommandExecutor ex, _, string plain) = await SetupAsync();

        ClusterHealthResponse health = (ClusterHealthResponse)(await Cluster(ex, plain).GetHealth()).Value!;

        Assert.AreEqual("", health.LocalRole);
        Assert.AreEqual(0, health.HostedPartitions);
    }

    // ─── The middleware in front of them ──────────────────────────────────────

    [Test]
    public async Task Middleware_LetsTheProbeThroughAndStillGuardsTheDetailReads()
    {
        (CommandExecutor ex, _, _) = await SetupAsync();

        bool probeReached = false;
        AuthenticationMiddleware probe = new(_ => { probeReached = true; return Task.CompletedTask; });
        DefaultHttpContext probeContext = MiddlewareContext("/v1/cluster/health");
        await probe.Invoke(probeContext, ex, Options);

        Assert.IsTrue(probeReached, "an orchestrator probe carries no token and must still be answered");
        Assert.AreNotEqual(401, probeContext.Response.StatusCode);

        bool detailReached = false;
        AuthenticationMiddleware detail = new(_ => { detailReached = true; return Task.CompletedTask; });
        DefaultHttpContext detailContext = MiddlewareContext("/v1/cluster/membership");
        await detail.Invoke(detailContext, ex, Options);

        Assert.AreEqual(401, detailContext.Response.StatusCode);
        Assert.IsFalse(detailReached);
    }

    private static DefaultHttpContext MiddlewareContext(string path, string? bearer = null)
    {
        DefaultHttpContext http = new();
        http.Request.Path = path;
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = IPAddress.Parse("10.1.2.3");
        if (bearer is not null)
            http.Request.Headers.Authorization = $"Bearer {bearer}";
        return http;
    }

    // ─── The dashboard's cluster panel ────────────────────────────────────────

    /// <summary>
    /// The panel reads its own dashboard route, because the browser authenticates with the session
    /// cookie and the cluster controller accepts the bearer header only. The bar is the same on both.
    /// </summary>
    [Test]
    public async Task DashboardClusterPanel_NeedsASuperuserAndServesTheRoster()
    {
        (CommandExecutor ex, string root, string plain) = await SetupAsync();

        JsonResult refused = (JsonResult)await Dashboard(ex, plain).GetCluster();
        Assert.AreEqual(403, refused.StatusCode);
        Assert.IsInstanceOf<DashboardFailureResponse>(refused.Value);

        JsonResult served = (JsonResult)await Dashboard(ex, root).GetCluster();
        Assert.IsInstanceOf<ClusterMembershipResponse>(served.Value);
    }

    // ─── Closing a database ───────────────────────────────────────────────────

    // ─── transfer-leadership: the third mutation, held to the same bar as leave ─────────────

    private static ClusterController WithBody(ClusterController controller, string json)
    {
        controller.ControllerContext.HttpContext.Request.Body = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        return controller;
    }

    [Test]
    public async Task TransferLeadership_IsRefusedToANonSuperuser()
    {
        (CommandExecutor ex, _, string plain) = await SetupAsync();

        JsonResult result = await WithBody(Cluster(ex, plain), "{\"partitionId\":1,\"targetEndpoint\":\"10.0.0.3:7072\"}").TransferLeadership();

        Assert.AreEqual(403, result.StatusCode);
        ClusterTransferLeadershipResponse body = (ClusterTransferLeadershipResponse)result.Value!;
        Assert.IsFalse(body.Success);
        Assert.AreEqual("Refused", body.Status);
        StringAssert.Contains("superuser", body.Reason);
    }

    [Test]
    public async Task TransferLeadership_OverTheNetworkWithAuthenticationOff_IsRefused()
    {
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };
        (CommandExecutor ex, _, _) = await SetupAsync(authOff);

        JsonResult result = await WithBody(Cluster(ex, bearer: null, authOff, IPAddress.Parse("10.1.2.3")),
            "{\"partitionId\":1,\"targetEndpoint\":\"10.0.0.3:7072\"}").TransferLeadership();

        Assert.AreEqual(403, result.StatusCode);
        StringAssert.Contains("requires authentication", ((ClusterTransferLeadershipResponse)result.Value!).Reason);
    }

    [Test]
    public async Task TransferLeadership_RejectsAMalformedRequestBeforeConsensus()
    {
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };
        (CommandExecutor ex, _, _) = await SetupAsync(authOff);

        foreach (string json in new[] { "", "{}", "{\"partitionId\":0,\"targetEndpoint\":\"10.0.0.3:7072\"}", "{\"partitionId\":1,\"targetEndpoint\":\" \"}" })
        {
            JsonResult result = await WithBody(Cluster(ex, bearer: null, authOff, IPAddress.Loopback), json).TransferLeadership();

            Assert.AreEqual(400, result.StatusCode, json);
            ClusterTransferLeadershipResponse body = (ClusterTransferLeadershipResponse)result.Value!;
            Assert.IsFalse(body.Success, json);
            StringAssert.Contains("partitionId", body.Reason, json);
        }
    }

    /// <summary>
    /// A well-formed request reaches consensus and comes back as an outcome, never a 500: the test
    /// node hosts no data partition led by anyone, so the answer is a refusal with the request echoed.
    /// </summary>
    [Test]
    public async Task TransferLeadership_ReportsARefusalAsAnOutcome()
    {
        (CommandExecutor ex, string root, _) = await SetupAsync();

        JsonResult result = await WithBody(Cluster(ex, root), "{\"partitionId\":1,\"targetEndpoint\":\"10.0.0.3:7072\"}").TransferLeadership();

        Assert.AreEqual(409, result.StatusCode);
        ClusterTransferLeadershipResponse body = (ClusterTransferLeadershipResponse)result.Value!;
        Assert.IsFalse(body.Success);
        Assert.IsNotEmpty(body.Status);
        Assert.AreNotEqual("Success", body.Status);
        Assert.AreEqual(1, body.PartitionId);
        Assert.AreEqual("10.0.0.3:7072", body.TargetEndpoint);
        Assert.IsNotNull(body.Reason);
    }

    [Test]
    public async Task CloseDatabase_IsRefusedToANonSuperuser()
    {
        (CommandExecutor ex, string root, string plain) = await SetupAsync();

        bool reached = false;
        AuthenticationMiddleware middleware = new(_ => { reached = true; return Task.CompletedTask; });

        DefaultHttpContext refused = MiddlewareContext("/close-db", plain);
        await middleware.Invoke(refused, ex, Options);

        Assert.AreEqual(403, refused.Response.StatusCode);
        Assert.IsFalse(reached, "the close must not reach the controller");

        DefaultHttpContext allowed = MiddlewareContext("/close-db", root);
        await middleware.Invoke(allowed, ex, Options);

        Assert.IsTrue(reached, "a superuser may still close a database");
        Assert.AreNotEqual(403, allowed.Response.StatusCode);
    }
}
