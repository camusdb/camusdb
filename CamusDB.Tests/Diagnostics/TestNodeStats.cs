/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging.Abstractions;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// The node stats answer built against a real engine: every field is present on this platform, the
/// data volume's <c>below_watermark</c> is the write gate's own answer (with the real probe and with an
/// injected one at the threshold), an unreadable value is -1 with an <c>errors</c> entry while the call
/// still answers, a WAL on its own volume is a second entry, and a warm call is far under a
/// millisecond.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNodeStats : BaseTest
{
    /// <summary>A watermark no physical volume satisfies, so the real probe always reports "below".</summary>
    private const long AboveAnyDisk = long.MaxValue;

    private NodeResourceSampler Sampler() =>
        new(NodeStoragePaths.From(Options, TestNode!.Options), NullLogger.Instance);

    private NodeStatsCollector Collector(CommandExecutor executor, NodeResourceSampler sampler) =>
        new(executor, TestNode!, sampler, "test-version");

    /// <summary>An engine whose data-volume monitor answers from <paramref name="probe"/>.</summary>
    private CommandExecutor ExecutorWithProbe(CamusDBOptions options, Func<DiskVolumeReading?> probe)
    {
        DiskSpaceMonitor monitor = new(probe, NullLogger.Instance);
        return new CommandExecutor(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false, diskSpace: monitor);
    }

    private static async Task CreateRobots(CommandExecutor executor, string db)
    {
        await executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "robots",
            columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));
    }

    private static async Task<ExecuteNonSQLResult> Insert(CommandExecutor executor, DatabaseDescriptor database, string db)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: db, sql: "INSERT INTO robots (id, name) VALUES (gen_id(), 'astro')", parameters: null));
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    [Test]
    public async Task Collect_ReportsEveryField_OnThisPlatform()
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
            Assert.Ignore("the node stats probes cover Linux and macOS");

        (_, _, CommandExecutor executor) = await CreateDatabase();
        NodeResourceSampler sampler = Sampler();
        sampler.Sample();

        NodeStats stats = Collector(executor, sampler).Collect();

        CollectionAssert.IsEmpty(stats.Errors, string.Join("; ", stats.Errors.Select(e => $"{e.Field}: {e.Message}")));

        Assert.AreEqual("test-version", stats.Node.Version);
        Assert.IsFalse(stats.Node.ClusterMode);
        Assert.GreaterOrEqual(stats.Node.UptimeSeconds, 0);
        Assert.That(stats.Node.SampledAtUnixMs, Is.EqualTo(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).Within(60_000));

        Assert.AreEqual(1, stats.Disks.Count, "the in-memory test engine has no WAL directory of its own");
        DiskVolumeStats data = stats.Disks[0];
        Assert.AreEqual(DiskVolumeStats.DataRole, data.Role);
        Assert.AreEqual(Options.DataDirectory, data.Path);
        Assert.IsNotEmpty(data.MountPoint);
        Assert.Greater(data.TotalBytes, 0);
        Assert.GreaterOrEqual(data.FreeBytes, 0);
        Assert.GreaterOrEqual(data.AvailableBytes, 0);
        Assert.AreEqual(data.TotalBytes - data.FreeBytes, data.UsedBytes);
        Assert.AreEqual(Options.MinFreeDiskBytes, data.MinFreeDiskBytes);
        Assert.AreEqual(DiskSpaceMonitor.IsBelowWatermark(data.AvailableBytes, data.MinFreeDiskBytes), data.BelowWatermark);
        Assert.AreEqual(0, data.EngineBytes, "an in-memory engine keeps nothing on disk");

        Assert.That(stats.Cpu.ProcessCpuPercent, Is.InRange(0.0, 100.0));
        Assert.Greater(stats.Cpu.ProcessCpuWindowMs, 0);
        Assert.AreEqual(Environment.ProcessorCount, stats.Cpu.ProcessorCount);
        Assert.GreaterOrEqual(stats.Cpu.LoadAverage1m, 0);
        Assert.GreaterOrEqual(stats.Cpu.LoadAverage5m, 0);
        Assert.GreaterOrEqual(stats.Cpu.LoadAverage15m, 0);

        Assert.Greater(stats.Memory.ProcessWorkingSetBytes, 0);
        Assert.Greater(stats.Memory.GcHeapBytes, 0);
        Assert.Greater(stats.Memory.GcCommittedBytes, 0);
        Assert.GreaterOrEqual(stats.Memory.GcMemoryLimitBytes, 0);
        Assert.Greater(stats.Memory.HostTotalBytes, 0);
        Assert.That(stats.Memory.HostAvailableBytes, Is.InRange(1L, stats.Memory.HostTotalBytes));
    }

    [Test]
    public async Task BelowWatermark_AgreesWithTheWriteGate_WithTheRealProbe()
    {
        NodeResourceSampler sampler = Sampler();
        sampler.Sample();

        // A watermark above any disk: the gate refuses the insert and the stats call says "below".
        (string lowDb, DatabaseDescriptor lowDatabase, CommandExecutor lowEngine) =
            await CreateDatabase(Options with { MinFreeDiskBytes = AboveAnyDisk });
        await CreateRobots(lowEngine, lowDb);

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => Insert(lowEngine, lowDatabase, lowDb))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientDiskSpace, refused.Code);

        DiskVolumeStats low = Collector(lowEngine, sampler).Collect().Disks[0];
        Assert.IsTrue(low.BelowWatermark);
        Assert.AreEqual(AboveAnyDisk, low.MinFreeDiskBytes);

        // The default watermark: the insert is admitted and the stats call says "not below".
        (string okDb, DatabaseDescriptor okDatabase, CommandExecutor okEngine) = await CreateDatabase();
        await CreateRobots(okEngine, okDb);

        Assert.AreEqual(1, (await Insert(okEngine, okDatabase, okDb)).ModifiedRows);
        Assert.IsFalse(Collector(okEngine, sampler).Collect().Disks[0].BelowWatermark);
    }

    [TestCase(500_000L, false)]
    [TestCase(499_999L, true)]
    public async Task BelowWatermark_AgreesWithTheWriteGate_AtTheThreshold(long available, bool below)
    {
        CamusDBOptions options = Options with { MinFreeDiskBytes = 500_000 };
        CommandExecutor executor = ExecutorWithProbe(options, () => new DiskVolumeReading(1_000_000, 600_000, available));
        (string db, DatabaseDescriptor database, _) = await CreateDatabaseWith(executor);
        await CreateRobots(executor, db);

        if (below)
        {
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(() => Insert(executor, database, db))!;
            Assert.AreEqual(CamusDBErrorCodes.InsufficientDiskSpace, refused.Code);
        }
        else
        {
            Assert.AreEqual(1, (await Insert(executor, database, db)).ModifiedRows);
        }

        NodeResourceSampler sampler = Sampler();
        sampler.Sample();
        DiskVolumeStats data = Collector(executor, sampler).Collect().Disks[0];

        Assert.AreEqual(below, data.BelowWatermark);
        Assert.AreEqual(1_000_000, data.TotalBytes);
        Assert.AreEqual(600_000, data.FreeBytes);
        Assert.AreEqual(available, data.AvailableBytes);
        Assert.AreEqual(400_000, data.UsedBytes);
        Assert.AreEqual(500_000, data.MinFreeDiskBytes);
    }

    [Test]
    public async Task UnreadableDiskProbe_IsMinusOneWithErrors_AndTheCallStillAnswers()
    {
        CamusDBOptions options = Options with { MinFreeDiskBytes = 500_000 };
        CommandExecutor executor = ExecutorWithProbe(options, () => throw new IOException("volume gone"));
        (string db, DatabaseDescriptor database, _) = await CreateDatabaseWith(executor);
        await CreateRobots(executor, db);

        // The gate fails open on an unreadable volume; the stats call agrees and reports why.
        Assert.AreEqual(1, (await Insert(executor, database, db)).ModifiedRows);

        NodeResourceSampler sampler = Sampler();
        sampler.Sample();
        NodeStats stats = Collector(executor, sampler).Collect();
        DiskVolumeStats data = stats.Disks[0];

        Assert.IsFalse(data.BelowWatermark);
        Assert.AreEqual(-1, data.TotalBytes);
        Assert.AreEqual(-1, data.FreeBytes);
        Assert.AreEqual(-1, data.AvailableBytes);
        Assert.AreEqual(-1, data.UsedBytes);

        foreach (string field in new[] { "disks[data].total_bytes", "disks[data].free_bytes", "disks[data].available_bytes", "disks[data].used_bytes" })
        {
            NodeStatsError? error = stats.Errors.FirstOrDefault(e => e.Field == field);
            Assert.IsNotNull(error, field);
            Assert.AreEqual("volume gone", error!.Message, field);
        }

        // The rest of the answer is unaffected by the failed probe.
        Assert.Greater(stats.Memory.ProcessWorkingSetBytes, 0);
        Assert.AreEqual(Environment.ProcessorCount, stats.Cpu.ProcessorCount);
    }

    [Test]
    public async Task BeforeTheFirstSample_SampledFiguresAreMinusOneWithErrors()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();

        NodeStats stats = Collector(executor, Sampler()).Collect();

        Assert.AreEqual(-1, stats.Cpu.ProcessCpuPercent);
        Assert.AreEqual(-1, stats.Cpu.LoadAverage1m);
        Assert.AreEqual(-1, stats.Memory.GcMemoryLimitBytes);
        Assert.AreEqual(-1, stats.Memory.HostTotalBytes);
        Assert.AreEqual(-1, stats.Memory.HostAvailableBytes);
        Assert.AreEqual(-1, stats.Disks[0].EngineBytes);

        string[] fields = stats.Errors.Select(e => e.Field).ToArray();
        CollectionAssert.IsSubsetOf(new[]
        {
            "cpu.process_cpu_percent", "cpu.load_average_1m", "cpu.load_average_5m", "cpu.load_average_15m",
            "memory.gc_memory_limit_bytes", "memory.host_total_bytes", "memory.host_available_bytes",
            "disks[data].engine_bytes",
        }, fields);

        // What the runtime keeps, and the disk probe, do not depend on the sampler.
        Assert.Greater(stats.Memory.ProcessWorkingSetBytes, 0);
        Assert.Greater(stats.Disks[0].TotalBytes, 0);
    }

    [Test]
    public async Task WalOnItsOwnVolume_IsASecondEntry_AndEngineBytesSplitBetweenThem()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();

        string kv = Path.Combine(Options.DataDirectory, "kv-under-test");
        string wal = Path.Combine(Options.DataDirectory, "wal-under-test");
        Directory.CreateDirectory(Path.Combine(kv, "v2"));
        Directory.CreateDirectory(Path.Combine(wal, "v2"));
        File.WriteAllBytes(Path.Combine(kv, "v2", "000001.sst"), new byte[1000]);
        File.WriteAllBytes(Path.Combine(wal, "v2", "000002.log"), new byte[3000]);

        NodeStoragePaths paths = new(Options.DataDirectory, kv, "rocksdb", wal, "rocksdb");
        string walMount = NodeResourceSampler.ResolveRealPath(wal);

        // The WAL directory is a mount point of its own.
        NodeResourceSampler separate = new(paths, NullLogger.Instance, _ => null,
            static () => TimeSpan.Zero, Stopwatch.GetTimestamp, () => ["/", walMount]);
        separate.Sample();

        NodeStats split = new NodeStatsCollector(executor, separate, "v", false, () => "1", () => "local").Collect();

        Assert.AreEqual(2, split.Disks.Count);
        Assert.AreEqual(1000, split.Disks[0].EngineBytes, "the data volume counts the key/value store only");

        DiskVolumeStats walDisk = split.Disks[1];
        Assert.AreEqual(DiskVolumeStats.WalRole, walDisk.Role);
        Assert.AreEqual(wal, walDisk.Path);
        Assert.AreEqual(walMount, walDisk.MountPoint);
        Assert.AreEqual(3000, walDisk.EngineBytes);
        Assert.AreEqual(0, walDisk.MinFreeDiskBytes, "the write gate does not watch the WAL volume");
        Assert.IsFalse(walDisk.BelowWatermark);
        Assert.Greater(walDisk.TotalBytes, 0, "the WAL volume is probed for real");

        // The same directories on one volume: one entry that counts both.
        NodeResourceSampler shared = new(paths, NullLogger.Instance, _ => null,
            static () => TimeSpan.Zero, Stopwatch.GetTimestamp, () => ["/"]);
        shared.Sample();

        NodeStats one = new NodeStatsCollector(executor, shared, "v", false, () => "1", () => "local").Collect();

        Assert.AreEqual(1, one.Disks.Count);
        Assert.AreEqual(4000, one.Disks[0].EngineBytes);
        Assert.AreEqual("/", one.Disks[0].MountPoint);
    }

    [Test]
    public async Task IdentityFailure_IsAnEmptyStringWithAnError_NotAFailedCall()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();
        NodeResourceSampler sampler = Sampler();
        sampler.Sample();

        NodeStats stats = new NodeStatsCollector(executor, sampler, "v", true,
            () => throw new InvalidOperationException("no node id yet"), () => "host:1").Collect();

        Assert.AreEqual("", stats.Node.NodeId);
        Assert.AreEqual("host:1", stats.Node.Endpoint);
        Assert.IsTrue(stats.Node.ClusterMode);
        Assert.IsTrue(stats.Errors.Any(e => e.Field == "node.node_id" && e.Message == "no node id yet"));
    }

    [Test]
    public async Task OneCall_CostsUnderOneMillisecond_WithAWarmCache()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase();
        NodeResourceSampler sampler = Sampler();
        sampler.Sample();
        NodeStatsCollector collector = Collector(executor, sampler);

        // Warm: the first call pays the data volume's probe; later calls inside two seconds read its cache.
        collector.Collect();

        const int calls = 200;
        long start = Stopwatch.GetTimestamp();
        for (int i = 0; i < calls; i++)
            collector.Collect();
        TimeSpan elapsed = Stopwatch.GetElapsedTime(start);

        double averageMs = elapsed.TotalMilliseconds / calls;
        TestContext.Out.WriteLine($"node stats: {averageMs * 1000:F1} µs per call over {calls} calls");
        Assert.Less(averageMs, 1.0);
    }
}
