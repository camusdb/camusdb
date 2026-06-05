
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
    public void TestReader()
    {
        string yml = "buffer_pool_size: 1000";

        var configReader = new ConfigReader();
        var configDefinition = configReader.Read(yml);

        Assert.AreEqual(1000, configDefinition.BufferPoolSize);
    }

    [Test]
    [NonParallelizable]
    public void TestReadsSchemaAckDefaults()
    {
        ConfigDefinition config = new ConfigReader().Read("mode: standalone");

        Assert.AreEqual(30_000, config.SchemaAckWaitTimeoutMs);
        Assert.AreEqual(-1, config.SchemaAckLiveNodeLeaseMs);
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
