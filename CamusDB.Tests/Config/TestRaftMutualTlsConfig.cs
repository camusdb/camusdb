/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna;
using Kommander;

namespace CamusDB.Tests.Config;

/// <summary>
/// The configuration half of node-to-node mutual TLS: the two keys are accepted from a file, a node
/// that turns it on without a certificate or a CA stops at startup, the Raft transport receives the
/// client certificate, and a node with authentication on and peers refuses to start without it.
///
/// <para>The last rule matters most. Without it, such a node starts, refuses every consensus call from
/// its peers, and the cluster never forms, with nothing in the logs but rejected calls.</para>
/// </summary>
[TestFixture]
public sealed class TestRaftMutualTlsConfig
{
    private static ConfigDefinition ThreeNodeCluster() => new()
    {
        Mode = "cluster",
        Peers = ["10.0.0.2:7070", "10.0.0.3:7070"],
        RaftCertificate = "/etc/camusdb/tls/certificate.pfx",
    };

    [Test]
    public void Keys_AreReadFromTheConfigFile()
    {
        ConfigDefinition config = new ConfigReader().Read(
            "mode: cluster\n" +
            "raft_certificate: /etc/camusdb/tls/certificate.pfx\n" +
            "raft_mutual_tls_enabled: true\n" +
            "raft_peer_ca: /etc/camusdb/tls/ca.crt\n");

        Assert.IsTrue(config.RaftMutualTlsEnabled);
        Assert.AreEqual("/etc/camusdb/tls/ca.crt", config.RaftPeerCa);
    }

    [Test]
    public void MutualTls_IsOffByDefault()
    {
        ConfigDefinition config = new();

        Assert.IsFalse(config.RaftMutualTlsEnabled);
        Assert.AreEqual("", config.RaftPeerCa);
    }

    [Test]
    public void MutualTls_WithoutRaftCertificate_IsRejected()
    {
        ConfigDefinition config = ThreeNodeCluster();
        config.RaftCertificate = "";
        config.RaftMutualTlsEnabled = true;
        config.RaftPeerCa = "/etc/camusdb/tls/ca.crt";

        CamusDBException e = Assert.Throws<CamusDBException>(config.Validate)!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);
        Assert.That(e.Message, Does.Contain("raft_certificate"));
    }

    [Test]
    public void MutualTls_WithoutPeerCa_IsRejected()
    {
        ConfigDefinition config = ThreeNodeCluster();
        config.RaftMutualTlsEnabled = true;

        CamusDBException e = Assert.Throws<CamusDBException>(config.Validate)!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);
        Assert.That(e.Message, Does.Contain("raft_peer_ca"));
    }

    [Test]
    public void MutualTls_IsInertOnAStandaloneNode()
    {
        // Standalone has no Raft listener and no peers, so there is nothing to require.
        ConfigDefinition config = new() { Mode = "standalone", RaftMutualTlsEnabled = true };

        Assert.DoesNotThrow(config.Validate);
    }

    [Test]
    public void TransportSecurity_IsLeftAtKommandersDefaultWhenOff()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.ClusterBaseline(ThreeNodeCluster(), CamusDBOptions.Default);

        Assert.IsNull(options.TransportSecurity);
    }

    [Test]
    public void TransportSecurity_PresentsTheRaftCertificateWhenOn()
    {
        ConfigDefinition config = ThreeNodeCluster();
        config.RaftMutualTlsEnabled = true;
        config.RaftPeerCa = "/etc/camusdb/tls/ca.crt";

        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildCluster(config, CamusDBOptions.Default);

        Assert.IsNotNull(options.TransportSecurity);
        Assert.AreEqual(RaftNodeAuthenticationMode.MutualTls, options.TransportSecurity!.NodeAuthenticationMode);
        Assert.AreEqual("/etc/camusdb/tls/certificate.pfx", options.TransportSecurity.ClientCertificatePath);
        Assert.IsTrue(options.TransportSecurity.RequireTls);

        // An empty allow-list makes Kommander trust whatever the Raft listener validated. That is the
        // design, and it holds only while the listener checks the chain; see RaftPeerTrust.
        Assert.IsEmpty(options.TransportSecurity.TrustedClientCertificateThumbprints);
    }

    [Test]
    public void AuthenticatedClusterWithPeers_RequiresMutualTls()
    {
        CamusDBException e = Assert.Throws<CamusDBException>(
            () => ClusterAuthenticationStartupCheck.Validate(ThreeNodeCluster(), authenticationEnabled: true))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, e.Code);
        Assert.That(e.Message, Does.Contain("raft_mutual_tls_enabled"));
    }

    [Test]
    public void AuthenticatedClusterWithMutualTls_Starts()
    {
        ConfigDefinition config = ThreeNodeCluster();
        config.RaftMutualTlsEnabled = true;
        config.RaftPeerCa = "/etc/camusdb/tls/ca.crt";

        Assert.DoesNotThrow(() => ClusterAuthenticationStartupCheck.Validate(config, authenticationEnabled: true));
    }

    [Test]
    public void SingleNodeClusterOrNoAuthentication_IsNotAffected()
    {
        // A single-node cluster has no peer traffic to refuse.
        ConfigDefinition alone = new() { Mode = "cluster" };
        Assert.DoesNotThrow(() => ClusterAuthenticationStartupCheck.Validate(alone, authenticationEnabled: true));

        // With authentication off, peer traffic is not refused, so the cluster forms as before.
        Assert.DoesNotThrow(() => ClusterAuthenticationStartupCheck.Validate(ThreeNodeCluster(), authenticationEnabled: false));
    }
}
