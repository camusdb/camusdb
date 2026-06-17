
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;

using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna;

namespace CamusDB.Tests.Config;

[TestFixture]
public sealed class TestEmbeddedKahunaOptionsBuilder
{
    [Test]
    public void EmptyKahunaSection_ReproducesClusterBaseline()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            NodeName = "node-a",
            RaftNodeId = 2,
            RaftHost = "10.0.0.1",
            RaftPort = 7071,
            InitialPartitions = 3,
        };

        EmbeddedKahunaOptions expected = EmbeddedKahunaOptionsBuilder.ClusterBaseline(config);
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config);

        Assert.That(built.NodeName, Is.EqualTo(expected.NodeName));
        Assert.That(built.NodeId, Is.EqualTo(expected.NodeId));
        Assert.That(built.Host, Is.EqualTo(expected.Host));
        Assert.That(built.Port, Is.EqualTo(expected.Port));
        Assert.That(built.InitialPartitions, Is.EqualTo(expected.InitialPartitions));
        Assert.That(built.Storage, Is.EqualTo("sqlite"));
        Assert.That(built.StorageRevision, Is.EqualTo("v1"));
        Assert.That(built.WalStorage, Is.EqualTo("sqlite"));
        Assert.That(built.WalRevision, Is.EqualTo("v1"));
        Assert.That(built.StartElectionTimeout, Is.EqualTo(2000));
        Assert.That(built.EndElectionTimeout, Is.EqualTo(4000));
        Assert.That(built.StoragePath, Is.EqualTo(Path.Combine("/data/camus", "kv")));
        Assert.That(built.WalPath, Is.EqualTo(Path.Combine("/data/camus", "wal")));
    }

    [Test]
    public void EmptyKahunaSection_ReproducesStandaloneBaseline()
    {
        string dataPath = "/tmp/db-one";
        EmbeddedKahunaOptions expected = EmbeddedKahunaOptionsBuilder.StandaloneBaseline(dataPath);
        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone(dataPath, new KahunaOptionsConfig());

        Assert.That(built.Storage, Is.EqualTo(expected.Storage));
        Assert.That(built.StorageRevision, Is.EqualTo(expected.StorageRevision));
        Assert.That(built.WalStorage, Is.EqualTo(expected.WalStorage));
        Assert.That(built.WalRevision, Is.EqualTo(expected.WalRevision));
        Assert.That(built.InitialPartitions, Is.EqualTo(1));
    }

    [Test]
    public void KahunaStorageRocksdb_OverridesStandaloneBaseline()
    {
        KahunaOptionsConfig kahuna = new() { Storage = "rocksdb" };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildStandalone("/tmp/rocksdb-db", kahuna);

        Assert.That(built.Storage, Is.EqualTo("rocksdb"));
        Assert.That(built.StoragePath, Is.EqualTo(Path.Combine("/tmp/rocksdb-db", "kv")));
    }

    [Test]
    public void KahunaElectionTimeouts_OverrideClusterBaseline()
    {
        ConfigDefinition config = new()
        {
            DataDir = "/data/camus",
            Kahuna = new KahunaOptionsConfig
            {
                StartElectionTimeoutMs = 1500,
                EndElectionTimeoutMs = 3500,
            },
        };

        EmbeddedKahunaOptions built = EmbeddedKahunaOptionsBuilder.BuildCluster(config);

        Assert.That(built.StartElectionTimeout, Is.EqualTo(1500));
        Assert.That(built.EndElectionTimeout, Is.EqualTo(3500));
    }
}
