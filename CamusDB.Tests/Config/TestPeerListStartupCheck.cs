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

namespace CamusDB.Tests.Config;

/// <summary>
/// A peer list that names the node itself starts the process and then stops partition 0 for good in
/// Kommander's startup handshake, so the cluster never forms. These cases pin that the startup check
/// refuses exactly that list, and nothing that only looks like it.
/// </summary>
[TestFixture]
public sealed class TestPeerListStartupCheck
{
    private static ConfigDefinition Cluster(string raftHost, int raftPort, params string[] peers) => new()
    {
        Mode = "cluster",
        RaftHost = raftHost,
        RaftPort = raftPort,
        Peers = [.. peers],
    };

    private static void AssertRejected(ConfigDefinition config, string selfEntry)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => PeerListStartupCheck.Validate(config))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
        StringAssert.Contains($"'{selfEntry}'", ex.Message);
        StringAssert.Contains("own Raft address", ex.Message);
    }

    [Test]
    public void PeersOnlyList_IsAccepted()
    {
        Assert.DoesNotThrow(() => PeerListStartupCheck.Validate(
            Cluster("192.168.1.10", 7070, "192.168.1.11:7070", "192.168.1.12:7070")));
    }

    [Test]
    public void ListThatIncludesTheNodeItself_IsRejected()
    {
        AssertRejected(
            Cluster("192.168.1.10", 7070, "192.168.1.10:7070", "192.168.1.11:7070", "192.168.1.12:7070"),
            "192.168.1.10:7070");
    }

    [Test]
    public void SelfEntryInAnyPosition_IsRejected()
    {
        AssertRejected(
            Cluster("127.0.0.1", 7074, "127.0.0.1:7070", "127.0.0.1:7072", "127.0.0.1:7074"),
            "127.0.0.1:7074");
    }

    [Test]
    public void SameHostOnAnotherPort_IsAccepted()
    {
        // Three nodes on one machine differ by port only; each one's peers share its host.
        Assert.DoesNotThrow(() => PeerListStartupCheck.Validate(
            Cluster("127.0.0.1", 7072, "127.0.0.1:7070", "127.0.0.1:7074")));
    }

    [Test]
    public void HostCaseAndPortLeadingZero_StillMatchTheNodeItself()
    {
        AssertRejected(Cluster("camus1", 7070, "CAMUS1:07070", "camus2:7070"), "CAMUS1:07070");
    }

    [Test]
    public void BracketedIpv6Peer_MatchesAnUnbracketedRaftHost()
    {
        AssertRejected(Cluster("::1", 7070, "[::1]:7070", "[::1]:7072"), "[::1]:7070");
    }

    [Test]
    public void DifferentSpellingOfTheSameAddress_IsNotResolved()
    {
        // The check is literal, like Kommander's own: it does not resolve names.
        Assert.DoesNotThrow(() => PeerListStartupCheck.Validate(
            Cluster("127.0.0.1", 7070, "localhost:7070", "127.0.0.1:7072")));
    }

    [Test]
    public void JoinSeedListThatIncludesTheNodeItself_IsRejected()
    {
        ConfigDefinition config = Cluster("10.0.0.4", 7070, "10.0.0.1:7070", "10.0.0.4:7070");
        config.JoinExisting = true;

        AssertRejected(config, "10.0.0.4:7070");
    }

    [Test]
    public void StandaloneMode_IsNotChecked()
    {
        Assert.DoesNotThrow(() => PeerListStartupCheck.Validate(new ConfigDefinition
        {
            Mode = "standalone",
            RaftHost = "localhost",
            RaftPort = 7070,
        }));
    }

    [Test]
    [NonParallelizable]
    public void FileListsDefaultPortPeer_CommandLineMovesTheNode_IsAccepted()
    {
        // The file names localhost:7070 as a peer and leaves raft_port at its default 7070. Read alone,
        // that is the node itself; with --raft-port=7072 applied it is a real peer. The file read must
        // not refuse it, and the check on the final configuration must accept it.
        string yml = string.Join('\n',
            "mode: cluster",
            "peers:",
            "  - localhost:7070",
            "  - localhost:7074");

        ConfigDefinition config = null!;
        Assert.DoesNotThrow(() => config = new ConfigReader().Read(yml));

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides { RaftPort = 7072 });
        config.Validate();

        Assert.DoesNotThrow(() => PeerListStartupCheck.Validate(config));
    }

    [Test]
    [NonParallelizable]
    public void CommandLinePeerListThatIncludesTheNodeItself_IsRejected()
    {
        // The documented manual start: every address on --initial-cluster, this node's included.
        ConfigDefinition config = new ConfigReader().Read("mode: cluster");

        ConfigResolver.ApplyCliOverrides(config, new ConfigCliOverrides
        {
            RaftHost = "192.168.1.10",
            RaftPort = 7070,
            Peers = ["192.168.1.10:7070", "192.168.1.11:7070", "192.168.1.12:7070"],
        });
        config.Validate();

        AssertRejected(config, "192.168.1.10:7070");
    }
}
