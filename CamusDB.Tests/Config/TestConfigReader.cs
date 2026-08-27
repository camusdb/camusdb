
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

public class TestConfigReader
{
	public TestConfigReader()
	{
	}

    [Test]
    [NonParallelizable]
    public void TestReadsSchemaAckDefaults()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        Assert.AreEqual(30_000, config.SchemaAckWaitTimeoutMs);
        Assert.AreEqual(30_000, config.SchemaAckLiveNodeLeaseMs);
    }

    [Test]
    [NonParallelizable]
    public void TestReadsSchemaAckOverrides()
    {
        string yml = "schema_ack_wait_timeout_ms: 5000\nschema_ack_live_node_lease_ms: 2000";

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual(5000, config.SchemaAckWaitTimeoutMs);
        Assert.AreEqual(2000, config.SchemaAckLiveNodeLeaseMs);
    }

    [Test]
    [NonParallelizable]
    public void TestReadsPlacementAndBalancerKahunaKeys()
    {
        string yml = string.Join('\n',
            "mode: cluster",
            "kahuna:",
            "  replication_factor: 3",
            "  zone: rack-a",
            "  enable_placement_rebalancer: true",
            "  enable_leader_balancer: true",
            "  leader_balancer_interval_ms: 10000");

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual(3, config.Kahuna.ReplicationFactor);
        Assert.AreEqual("rack-a", config.Kahuna.Zone);
        Assert.AreEqual(true, config.Kahuna.EnablePlacementRebalancer);
        Assert.AreEqual(true, config.Kahuna.EnableLeaderBalancer);
        Assert.AreEqual(10_000, config.Kahuna.LeaderBalancerIntervalMs);
    }

    [Test]
    [NonParallelizable]
    public void TestReadsLoadSplitKahunaKeys()
    {
        // Proves the allow-list spelling and the property spelling agree: an unlisted key is a
        // configuration error, and a listed key that no property matches deserializes to null.
        string yml = string.Join('\n',
            "mode: cluster",
            "kahuna:",
            "  range_split_load_threshold: 250.5",
            "  range_split_load_min_queue_depth: 16",
            "  range_split_load_min_commit_wait_ms: 12.5",
            "  range_split_load_window_ms: 20000",
            "  range_split_load_poll_interval_ms: 2000",
            "  range_split_load_imbalance_max: 0.9",
            "  range_split_settle_window_ms: 30000",
            "  range_split_indivisible_cooldown_ms: 60000",
            "  range_merge_min_size: 7",
            "  range_move_settle_timeout_ms: 4000",
            "  enable_load_reports: true");

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual(250.5, config.Kahuna.RangeSplitLoadThreshold);
        Assert.AreEqual(16, config.Kahuna.RangeSplitLoadMinQueueDepth);
        Assert.AreEqual(12.5, config.Kahuna.RangeSplitLoadMinCommitWaitMs);
        Assert.AreEqual(20_000, config.Kahuna.RangeSplitLoadWindowMs);
        Assert.AreEqual(2_000, config.Kahuna.RangeSplitLoadPollIntervalMs);
        Assert.AreEqual(0.9, config.Kahuna.RangeSplitLoadImbalanceMax);
        Assert.AreEqual(30_000, config.Kahuna.RangeSplitSettleWindowMs);
        Assert.AreEqual(60_000, config.Kahuna.RangeSplitIndivisibleCooldownMs);
        Assert.AreEqual(7, config.Kahuna.RangeMergeMinSize);
        Assert.AreEqual(4_000, config.Kahuna.RangeMoveSettleTimeoutMs);
        Assert.AreEqual(true, config.Kahuna.EnableLoadReports);
    }

    [Test]
    [NonParallelizable]
    public void TestReadsJoinExisting()
    {
        string yml = string.Join('\n',
            "mode: cluster",
            "join_existing: true",
            "peers:",
            "  - 10.0.0.1:7070");

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.AreEqual(true, config.JoinExisting);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsJoinExistingWithoutPeers()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("mode: cluster\njoin_existing: true"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
        StringAssert.Contains("join_existing", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsJoinExistingInStandaloneMode()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("mode: standalone\njoin_existing: true"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
        StringAssert.Contains("cluster mode", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsUnknownMode()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("mode: weird"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsZeroAckTimeout()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("schema_ack_wait_timeout_ms: 0"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsZeroLiveNodeLease()
    {
        // -1 (infinite) and > 0 are allowed; 0 is not.
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("schema_ack_live_node_lease_ms: 0"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsInvalidRaftPort()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("raft_port: 70000"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsHttpPeersCountMismatch()
    {
        string yml =
            "mode: cluster\n" +
            "peers:\n  - host-a:7070\n  - host-b:7070\n" +
            "http_peers:\n  - host-a:5095\n";

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(yml))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestRejectsMalformedPeerEndpoint()
    {
        string yml = "mode: cluster\npeers:\n  - host-a\n";

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read(yml))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public void TestAcceptsWellFormedClusterConfig()
    {
        string yml =
            "mode: cluster\n" +
            "raft_port: 7070\n" +
            "initial_partitions: 4\n" +
            "schema_ack_wait_timeout_ms: 15000\n" +
            "schema_ack_live_node_lease_ms: -1\n" +
            "peers:\n  - host-a:7070\n  - host-b:7070\n" +
            "http_peers:\n  - host-a:5095\n  - host-b:5095\n";

        ConfigDefinition config = new ConfigReader().Read(yml);

        Assert.IsTrue(config.IsClusterMode);
        Assert.AreEqual(4, config.InitialPartitions);
        Assert.AreEqual(2, config.Peers.Count);
        Assert.AreEqual(2, config.HttpPeers.Count);
    }
}
