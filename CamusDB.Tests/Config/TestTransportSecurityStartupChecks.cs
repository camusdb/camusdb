/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// A cluster whose peer traffic is plaintext starts, forms, and replicates exactly as it should. Only
/// somebody watching the network can tell — and they are not going to mention it. That is what makes
/// these warnings the only thing standing between an operator and a shared node secret on the wire.
///
/// <para>The other half of the contract matters as much: a loopback development cluster must produce
/// nothing at all. A check that fires on a correct setup is noise, and noise is how an operator learns
/// to scroll past the line that mattered.</para>
/// </summary>
[TestFixture]
public sealed class TestTransportSecurityStartupChecks
{
    /// <summary>A three-node cluster on real addresses, for a test to secure or expose one part of.</summary>
    private static ConfigDefinition ExposedCluster() => new()
    {
        Mode = "cluster",
        Peers = ["10.0.0.1:7070", "10.0.0.2:7070", "10.0.0.3:7070"],
        GrpcEnabled = true,
    };

    [Test]
    public void SingleNode_WarnsAboutNothing()
    {
        ConfigDefinition standalone = new() { Mode = "standalone", GrpcEnabled = true };

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(standalone));
    }

    [Test]
    public void LoopbackCluster_WarnsAboutNothing()
    {
        // The everyday development shape: three nodes on one machine. Their traffic never reaches a
        // network, so there is nothing to observe and nothing to say.
        ConfigDefinition local = new()
        {
            Mode = "cluster",
            Peers = ["localhost:7070", "127.0.0.1:7071", "127.0.0.1:7072"],
            GrpcEnabled = true,
        };

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(local));
    }

    [Test]
    public void FullySecuredCluster_WarnsAboutNothing()
    {
        ConfigDefinition secured = ExposedCluster();
        secured.PeerTlsEnabled = true;
        secured.RaftCertificate = "/etc/camusdb/raft.pfx";
        secured.GrpcCertificate = "/etc/camusdb/grpc.pfx";

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(secured));
    }

    [Test]
    public void PlaintextPeerHttp_IsReported_AndNamesTheSettingThatClosesIt()
    {
        ConfigDefinition config = ExposedCluster();
        config.RaftCertificate = "/etc/camusdb/raft.pfx";
        config.GrpcCertificate = "/etc/camusdb/grpc.pfx";

        IReadOnlyList<string> warnings = TransportSecurityStartupChecks.Inspect(config);

        // Exactly one: the other two causes are closed, and reporting them anyway would teach the
        // operator that the list is not worth reading.
        Assert.AreEqual(1, warnings.Count, string.Join(" | ", warnings));
        Assert.That(warnings[0], Does.Contain("node secret"));
        Assert.That(warnings[0], Does.Contain("peer_tls_enabled"));
    }

    /// <summary>
    /// An <c>http_peers</c> entry that spells out <c>https://</c> overrides the setting for that peer
    /// in the resolver, so a deployment that writes every peer out in full is already secure. Warning
    /// at it would be telling the operator to fix something they have already fixed another way.
    /// </summary>
    [Test]
    public void ExplicitHttpsPeers_SuppressTheHttpWarning()
    {
        ConfigDefinition config = ExposedCluster();
        config.HttpPeers = ["https://10.0.0.1:5095", "https://10.0.0.2:5095", "https://10.0.0.3:5095"];
        config.RaftCertificate = "/etc/camusdb/raft.pfx";
        config.GrpcCertificate = "/etc/camusdb/grpc.pfx";

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(config));
    }

    [Test]
    public void MissingRaftCertificate_IsReported()
    {
        ConfigDefinition config = ExposedCluster();
        config.PeerTlsEnabled = true;
        config.GrpcCertificate = "/etc/camusdb/grpc.pfx";

        IReadOnlyList<string> warnings = TransportSecurityStartupChecks.Inspect(config);

        Assert.AreEqual(1, warnings.Count, string.Join(" | ", warnings));
        Assert.That(warnings[0], Does.Contain("raft_certificate"));
    }

    [Test]
    public void MissingGrpcCertificate_IsReported_UnlessTheRaftOneCoversIt()
    {
        ConfigDefinition uncovered = ExposedCluster();
        uncovered.PeerTlsEnabled = true;

        // No certificate at all: both the Raft and the gRPC listener are named, because each is closed
        // by a different setting and an operator fixing only one would rediscover the other.
        IReadOnlyList<string> warnings = TransportSecurityStartupChecks.Inspect(uncovered);
        Assert.AreEqual(2, warnings.Count, string.Join(" | ", warnings));
        Assert.IsTrue(warnings.Any(w => w.Contains("grpc_certificate")), string.Join(" | ", warnings));

        // The gRPC listener falls back to the Raft certificate, so that one alone silences it.
        ConfigDefinition covered = ExposedCluster();
        covered.PeerTlsEnabled = true;
        covered.RaftCertificate = "/etc/camusdb/raft.pfx";

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(covered));
    }

    [Test]
    public void DisabledGrpcListener_IsNotReported()
    {
        ConfigDefinition config = ExposedCluster();
        config.GrpcEnabled = false;
        config.PeerTlsEnabled = true;
        config.RaftCertificate = "/etc/camusdb/raft.pfx";

        Assert.IsEmpty(TransportSecurityStartupChecks.Inspect(config));
    }
}
