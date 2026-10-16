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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

using Grpc.AspNetCore.Server;
using NUnit.Framework;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.App.Middleware;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kommander.Communication.Grpc;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Who may call the node-to-node gRPC services that Kommander and Kahuna map on every listener.
///
/// <para>With mutual TLS on, only a caller that came over the Raft listener with a certificate that
/// chains to the cluster CA. With it off, everyone while authentication is off, and nobody while it is
/// on. A user token never opens them: Kahuna's key/value and lock services read and write raw rows
/// below the SQL layer, and most of their methods do not check the caller themselves.</para>
///
/// <para>Certificates are built in memory per fixture, so nothing here depends on files or the system
/// trust store.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestRaftPeerAuthentication : BaseTest
{
    private const int RaftPort = 7070;
    private const string NodePath = "/KeyValuer/TrySetKeyValue";
    private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";
    private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";

    private X509Certificate2 clusterCa = null!;
    private X509Certificate2 otherCa = null!;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "raft-peer-test-key-padded-to-the-32-byte-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    [OneTimeSetUp]
    public void CreateAuthorities()
    {
        clusterCa = CreateCa("cluster-ca");
        otherCa = CreateCa("other-ca");
    }

    [OneTimeTearDown]
    public void DisposeAuthorities()
    {
        clusterCa.Dispose();
        otherCa.Dispose();
    }

    // ─── Certificate trust ────────────────────────────────────────────────────

    [Test]
    public void Certificate_FromTheClusterCaWithClientAuthentication_IsTrusted()
    {
        using X509Certificate2 peer = CreateLeaf(clusterCa, "camus-1");

        Assert.IsTrue(Trust().IsTrustedCertificate(peer));
    }

    [Test]
    public void Certificate_FromAnotherCa_IsNotTrusted()
    {
        using X509Certificate2 stranger = CreateLeaf(otherCa, "camus-1");

        Assert.IsFalse(Trust().IsTrustedCertificate(stranger));
    }

    [Test]
    public void Certificate_WithoutClientAuthentication_IsNotTrusted()
    {
        // A server-only certificate from the right CA is not a client credential.
        using X509Certificate2 serverOnly = CreateLeaf(clusterCa, "camus-1", clientAuthentication: false);

        Assert.IsFalse(Trust().IsTrustedCertificate(serverOnly));
    }

    [Test]
    public void Certificate_OutsideItsValidityWindow_IsNotTrusted()
    {
        using X509Certificate2 expired = CreateLeaf(
            clusterCa, "camus-1", notBefore: DateTimeOffset.UtcNow.AddDays(-10), notAfter: DateTimeOffset.UtcNow.AddDays(-1));

        Assert.IsFalse(Trust().IsTrustedCertificate(expired));
    }

    [Test]
    public void Load_ReadsThePemCa()
    {
        string path = Path.Combine(Path.GetTempPath(), $"raft-peer-ca-{Guid.NewGuid():n}.crt");
        try
        {
            File.WriteAllText(path, clusterCa.ExportCertificatePem());
            using X509Certificate2 peer = CreateLeaf(clusterCa, "camus-1");

            Assert.IsTrue(RaftPeerTrust.Load(path, RaftPort).IsTrustedCertificate(peer));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public void Load_RefusesAMissingOrEmptyCaFile()
    {
        string missing = Path.Combine(Path.GetTempPath(), $"raft-peer-ca-{Guid.NewGuid():n}.crt");
        CamusDBException e = Assert.Throws<CamusDBException>(() => RaftPeerTrust.Load(missing, RaftPort))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);

        string empty = Path.Combine(Path.GetTempPath(), $"raft-peer-ca-{Guid.NewGuid():n}.crt");
        try
        {
            File.WriteAllText(empty, "not a certificate\n");
            e = Assert.Throws<CamusDBException>(() => RaftPeerTrust.Load(empty, RaftPort))!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);
            Assert.That(e.Message, Does.Contain("raft_peer_ca"));
        }
        finally
        {
            File.Delete(empty);
        }
    }

    // ─── The middleware ───────────────────────────────────────────────────────

    [Test]
    public async Task MutualTls_AdmitsAPeerOnTheRaftListener()
    {
        using X509Certificate2 peer = CreateLeaf(clusterCa, "camus-1");

        (bool reached, int status) = await InvokeAsync(NodeRequest(peer, RaftPort, withTrust: true), Options);

        Assert.IsTrue(reached, "a verified peer must reach the Raft and Kahuna services");
        Assert.AreNotEqual(401, status);
    }

    [Test]
    public async Task MutualTls_RefusesACallerWithoutACertificate_EvenWithAuthenticationOff()
    {
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };

        (bool reached, int status) = await InvokeAsync(NodeRequest(certificate: null, RaftPort, withTrust: true), authOff);

        Assert.IsFalse(reached, "turning on mutual TLS must close these services whatever the user authentication setting");
        Assert.AreEqual(401, status);
    }

    [Test]
    public async Task MutualTls_RefusesACertificateSeenOnAnotherListener()
    {
        // Only the Raft listener checks a client certificate against the cluster CA, so a certificate
        // that reached any other port proves nothing.
        using X509Certificate2 peer = CreateLeaf(clusterCa, "camus-1");

        (bool reached, int status) = await InvokeAsync(NodeRequest(peer, localPort: 7141, withTrust: true), Options);

        Assert.IsFalse(reached);
        Assert.AreEqual(401, status);
    }

    [Test]
    public async Task MutualTls_RefusesAPlaintextRequest()
    {
        using X509Certificate2 peer = CreateLeaf(clusterCa, "camus-1");
        DefaultHttpContext http = NodeRequest(peer, RaftPort, withTrust: true);
        http.Request.IsHttps = false;

        (bool reached, int status) = await InvokeAsync(http, Options);

        Assert.IsFalse(reached);
        Assert.AreEqual(401, status);
    }

    [Test]
    public async Task UserToken_NeverOpensANodeService()
    {
        // With mutual TLS off and authentication on, a superuser token is still refused: the token
        // path would let a signed-in user write raw rows past every table privilege.
        CommandExecutor ex = CreateCommandExecutor(Options);
        await ex.EnsureBootstrapSuperuserAsync("root", "root-pw");
        string token = (await ex.LoginAsync("root", "root-pw")).Token;

        DefaultHttpContext http = NodeRequest(certificate: null, RaftPort, withTrust: false);
        http.Request.Headers.Authorization = $"Bearer {token}";

        (bool reached, int status) = await InvokeAsync(http, Options, ex);

        Assert.IsFalse(reached);
        Assert.AreEqual(401, status);
    }

    [Test]
    public async Task NoMutualTlsAndNoAuthentication_LeavesNodeServicesOpen()
    {
        // The behavior a cluster without authentication has always had: its peers carry no credential.
        CamusDBOptions authOff = Options with { AuthenticationEnabled = false };

        (bool reached, _) = await InvokeAsync(NodeRequest(certificate: null, RaftPort, withTrust: false), authOff);

        Assert.IsTrue(reached);
    }

    [Test]
    public async Task NodeServicePaths_AreMatchedWithoutRegardToCase()
    {
        // Endpoint routing would send "/keyvaluer/..." to the KeyValuer service, so the check must
        // treat it as the same path.
        DefaultHttpContext http = NodeRequest(certificate: null, RaftPort, withTrust: true);
        http.Request.Path = "/keyvaluer/TrySetKeyValue";

        (bool reached, int status) = await InvokeAsync(http, Options with { AuthenticationEnabled = false });

        Assert.IsFalse(reached);
        Assert.AreEqual(401, status);
    }

    /// <summary>
    /// The list in the middleware must name every gRPC service Kommander and Kahuna map. A service they
    /// add later and the list misses would fall through to the user-token path, so this fails first.
    /// It also fails on a listed name that matches nothing, which is a misspelling or a removed service.
    /// </summary>
    [Test]
    public async Task NodeServicePrefixes_CoverEveryMappedNodeService()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(NodeTransportGate.Disabled);

        await using WebApplication app = builder.Build();
        app.MapGrpcRaftRoutes();
        app.MapGrpcKahunaRoutes();

        List<string> paths = [];
        foreach (EndpointDataSource source in ((IEndpointRouteBuilder)app).DataSources)
        {
            foreach (Endpoint endpoint in source.Endpoints)
            {
                if (endpoint is not RouteEndpoint route || endpoint.Metadata.GetMetadata<GrpcMethodMetadata>() is null)
                    continue;

                string pattern = route.RoutePattern.RawText ?? "";

                // The catch-all routes for unknown methods carry parameters; they are not services.
                if (pattern.Contains('{'))
                    continue;

                paths.Add(pattern.StartsWith('/') ? pattern : "/" + pattern);
            }
        }

        Assert.IsNotEmpty(paths, "no gRPC method was mapped; the test no longer sees the services");

        List<string> uncovered = paths.Where(p => !AuthenticationMiddleware.IsNodeServicePath(p)).ToList();
        Assert.IsEmpty(uncovered, "add these services to AuthenticationMiddleware.NodeServicePrefixes: " + string.Join(", ", uncovered));

        foreach (string prefix in AuthenticationMiddleware.NodeServicePrefixes)
            Assert.IsTrue(
                paths.Any(p => p.StartsWith(prefix, StringComparison.Ordinal)),
                $"'{prefix}' matches no mapped service");
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private RaftPeerTrust Trust() => new([clusterCa], RaftPort);

    private DefaultHttpContext NodeRequest(X509Certificate2? certificate, int localPort, bool withTrust)
    {
        DefaultHttpContext http = new();
        http.Request.Path = NodePath;
        http.Request.IsHttps = true;
        http.Connection.LocalPort = localPort;
        http.Connection.ClientCertificate = certificate;
        http.Response.Body = new MemoryStream();

        if (withTrust)
            http.RequestServices = new ServiceCollection().AddSingleton(Trust()).BuildServiceProvider();

        return http;
    }

    private async Task<(bool reached, int status)> InvokeAsync(DefaultHttpContext http, CamusDBOptions options, CommandExecutor? executor = null)
    {
        bool reached = false;
        AuthenticationMiddleware middleware = new(_ => { reached = true; return Task.CompletedTask; });

        await middleware.Invoke(http, executor ?? CreateCommandExecutor(options), options);

        return (reached, http.Response.StatusCode);
    }

    private static X509Certificate2 CreateCa(string name)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new($"CN={name}", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));

        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(30));
    }

    private static X509Certificate2 CreateLeaf(
        X509Certificate2 issuer,
        string name,
        bool clientAuthentication = true,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new($"CN={name}", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));

        OidCollection usages = new() { new Oid(ServerAuthenticationOid) };
        if (clientAuthentication)
            usages.Add(new Oid(ClientAuthenticationOid));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(usages, false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(issuer, true, false));

        return request.Create(
            issuer,
            notBefore ?? DateTimeOffset.UtcNow.AddDays(-1),
            notAfter ?? DateTimeOffset.UtcNow.AddDays(10),
            RandomNumberGenerator.GetBytes(8));
    }
}
