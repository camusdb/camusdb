/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna;

namespace CamusDB.Tests.Config;

/// <summary>
/// End-to-end coverage for the <c>kahuna.wal_shard_*</c> keys: a YAML document is read, turned into
/// embedded options, and used to open a real Raft WAL, and the assertion is made against the options
/// RocksDB itself reports having opened the database with.
///
/// <para>Why it opens a real engine instead of asserting the mapping. The mapping tests next to this
/// one prove the key reaches <see cref="EmbeddedKahunaOptions"/>, and that was true of these knobs
/// in Kommander for a release while no host could set them — the value was carried correctly and
/// then dropped at the last handoff, where nothing was looking. Only the engine's own options dump
/// answers whether a key the operator wrote changed what RocksDB runs.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestWalShardTuningReachesRocksDb
{
    [Test]
    public async Task ALevelBaseOverrideInYaml_ReachesTheOptionsRocksDbOpensWith()
    {
        string log = await OpenEngineAndReadWalLogAsync("kahuna:\n  wal_shard_max_bytes_for_level_base_mb: 2048");

        Assert.That(log, Does.Contain("max_bytes_for_level_base: 2147483648"));
    }

    [Test]
    public async Task AUniversalCompactionOverrideInYaml_ReachesTheOptionsRocksDbOpensWith()
    {
        string log = await OpenEngineAndReadWalLogAsync("kahuna:\n  wal_shard_universal_compaction: true");

        Assert.That(log, Does.Contain("kCompactionStyleUniversal"));
    }

    [Test]
    public async Task NoWalShardKeys_LeaveTheShippedLeveledLayout()
    {
        // The control arm, and the property that matters most: an operator who writes none of these
        // keys must get Kommander's shipped layout. RocksDB's own 256 MiB base level and leveled
        // compaction are what "unset" has to keep meaning.
        string log = await OpenEngineAndReadWalLogAsync("kahuna:\n  storage: rocksdb");

        Assert.That(log, Does.Contain("max_bytes_for_level_base: 268435456"));
        Assert.That(log, Does.Not.Contain("kCompactionStyleUniversal"));
    }

    /// <summary>
    /// Reads <paramref name="yaml"/> the way startup does, opens a standalone engine on a temporary
    /// data directory, closes it, and returns the RocksDB <c>LOG</c> the Raft WAL wrote. The engine
    /// is disposed before the log is read so the file is complete and the directory can be removed.
    /// </summary>
    private static async Task<string> OpenEngineAndReadWalLogAsync(string yaml)
    {
        ConfigDefinition config = new ConfigReader().Read(yaml);

        string dataPath = Path.Combine(Path.GetTempPath(), "camusdb-walshard-" + Path.GetRandomFileName());

        EmbeddedKahunaOptions options =
            EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, config.Kahuna, CamusDBOptions.Default);

        Assert.That(options.WalStorage, Is.EqualTo("rocksdb"), "the RocksDB standalone baseline must use a RocksDB WAL");

        try
        {
            await using (EmbeddedKahuna engine = new(options))
                await engine.StartAsync();

            return await File.ReadAllTextAsync(Path.Combine(options.WalPath, options.WalRevision, "LOG"));
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }
}
