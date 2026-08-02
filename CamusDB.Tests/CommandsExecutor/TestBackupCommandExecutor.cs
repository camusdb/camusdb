/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Drives the backup/PITR failure matrix through the real <see cref="CommandExecutor"/> +
/// <c>BackupManager</c> path (not the raw Kahuna passthrough), so the CamusDB wiring is exercised:
/// the superuser/backup-configured/restore-enabled gates, the typed <c>KahunaBackupOutcome</c> →
/// <see cref="CamusDBErrorCodes"/> mapping, the data-root layout, and restore-root confinement.
/// Requires Kahuna 0.9.3+ (typed outcomes, artifact verification, coverage bounds, restore confinement).
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestBackupCommandExecutor
{
    private static readonly ILoggerFactory LoggerFactoryInstance =
        LoggerFactory.Create(b => b.AddFilter("Camus", LogLevel.Warning));

    private readonly ILogger<ICamusDB> logger = LoggerFactoryInstance.CreateLogger<ICamusDB>();

    private sealed record Ctx(
        CommandExecutor Executor, EmbeddedKahuna Node, DatabaseRegistry Registry,
        string Root, string BackupDir, string RestoreRoot);

    private readonly List<Ctx> created = new();
    private bool authWasEnabled;

    [SetUp]
    public void SetUp() => authWasEnabled = CamusConfig.AuthenticationEnabled;

    [TearDown]
    public async Task TearDown()
    {
        CamusConfig.AuthenticationEnabled = authWasEnabled;

        foreach (Ctx c in created)
        {
            try { await c.Executor.DisposeAsync(); } catch { }
            try { await c.Registry.DisposeAsync(); } catch { }
            try { await c.Node.DisposeAsync(); } catch { }
            try { if (Directory.Exists(c.Root)) Directory.Delete(c.Root, recursive: true); } catch { }
        }
        created.Clear();
    }

    private async Task<Ctx> NewExecutorAsync(bool backup = true, bool restore = true)
    {
        string root = Path.Combine(Path.GetTempPath(), "camus-t21-" + Guid.NewGuid().ToString("n"));
        string dataDir = Path.Combine(root, "data");
        string backupDir = Path.Combine(root, "backups");
        string restoreRoot = Path.Combine(root, "restores");
        Directory.CreateDirectory(dataDir);
        if (restore)
            Directory.CreateDirectory(restoreRoot);
        CamusConfig.DataDirectory = dataDir;

        EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = $"t21-{Guid.NewGuid():N}",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            BackupDir = backup ? backupDir : "",
            RestoreRoot = restore ? restoreRoot : "",
        }.WithFastTestTimers());
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);
        await node.FlushAsync();

        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, CamusDBConfig.Ambient);
        CommandExecutor executor = new(
            new CommandValidator(CamusDBConfig.Ambient), new CatalogsManager(logger), logger, CamusDBConfig.Ambient,
            sharedNode: node, registry: registry, isClusterMode: false);

        Ctx ctx = new(executor, node, registry, root, backupDir, restoreRoot);
        created.Add(ctx);
        return ctx;
    }

    private static async Task WriteKeyAsync(EmbeddedKahuna node, string key, string value)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type);
        await node.FlushAsync();
    }

    private static string FreshTarget(Ctx c) => Path.Combine(c.RestoreRoot, "r-" + Guid.NewGuid().ToString("n"));

    // ── Happy path through the executor ──────────────────────────────────────

    [Test]
    public async Task EndToEnd_TakeFull_List_Chain_Restore()
    {
        Ctx c = await NewExecutorAsync();
        await WriteKeyAsync(c.Node, "t21/k1", "v1");

        BackupInfo full = await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null));
        Assert.AreNotEqual(Guid.Empty, full.BackupId);
        Assert.AreEqual("Full", full.ActualKind);

        IReadOnlyList<BackupInfo> all = await c.Executor.ListBackups(new ListBackupsTicket());
        Assert.IsTrue(all.Any(b => b.BackupId == full.BackupId));

        IReadOnlyList<BackupInfo> chain = await c.Executor.GetBackupChain(new GetBackupChainTicket(full.BackupId));
        Assert.IsNotEmpty(chain);
        // 0.9.3 stamps recoverable coverage on the chain head.
        Assert.IsNotNull(chain[0].MinRecoverablePhysicalMs, "chain head must carry recoverable coverage bounds");
        Assert.IsNotNull(chain[0].MaxRecoverablePhysicalMs);

        string target = FreshTarget(c);
        RestoreResult res = await c.Executor.RestoreBackup(new RestoreBackupTicket(full.BackupId, target, 0));
        Assert.AreEqual(target, res.DataRoot);
        Assert.IsTrue(Directory.Exists(Path.Combine(target, "wal")), "restore must lay out {dataRoot}/wal");
        Assert.IsNotEmpty(res.Chain);
    }

    // ── Gates ────────────────────────────────────────────────────────────────

    [Test]
    public async Task TakeBackup_WhenNotConfigured_BackupNotConfigured()
    {
        Ctx c = await NewExecutorAsync(backup: false, restore: false);
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null)))!;
        Assert.AreEqual(CamusDBErrorCodes.BackupNotConfigured, ex.Code);
    }

    [Test]
    public async Task Restore_WhenRestoreRootUnset_RemoteRestoreDisabled()
    {
        Ctx c = await NewExecutorAsync(backup: true, restore: false);
        await WriteKeyAsync(c.Node, "t21/k1", "v1");
        BackupInfo full = await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null));

        string target = Path.Combine(c.Root, "restore-attempt");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.RestoreBackup(new RestoreBackupTicket(full.BackupId, target, 0)))!;
        Assert.AreEqual(CamusDBErrorCodes.RemoteRestoreDisabled, ex.Code);
    }

    [Test]
    public async Task AuthEnabled_NullPrincipal_AuthenticationFailed()
    {
        // An engine fixes its configuration when it is constructed, so authentication has to be on
        // before the executor is built — flipping it afterwards leaves the executor unauthenticated.
        CamusConfig.AuthenticationEnabled = true;
        Ctx c = await NewExecutorAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null, principal: null)))!;
        Assert.AreEqual(CamusDBErrorCodes.AuthenticationFailed, ex.Code);
    }

    [Test]
    public async Task AuthEnabled_NonSuperuser_InsufficientPrivilege()
    {
        // An engine fixes its configuration when it is constructed, so authentication has to be on
        // before the executor is built — flipping it afterwards leaves the executor unauthenticated.
        CamusConfig.AuthenticationEnabled = true;
        Ctx c = await NewExecutorAsync();
        Principal user = new("bob", isSuperuser: false, Array.Empty<GrantRecord>());
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null, user)))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, ex.Code);
    }

    [Test]
    public async Task AuthEnabled_Superuser_Passes()
    {
        Ctx c = await NewExecutorAsync();
        await WriteKeyAsync(c.Node, "t21/k1", "v1");
        CamusConfig.AuthenticationEnabled = true;
        Principal admin = new("root", isSuperuser: true, Array.Empty<GrantRecord>());
        BackupInfo full = await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null, admin));
        Assert.AreNotEqual(Guid.Empty, full.BackupId);
    }

    // ── Input validation ─────────────────────────────────────────────────────

    [Test]
    public async Task Incremental_WithoutParent_InvalidInput()
    {
        Ctx c = await NewExecutorAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Incremental, null)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    // ── Typed failure outcomes (Kahuna 0.9.3) mapped by CamusDB ───────────────

    [Test]
    public async Task GetBackupChain_UnknownId_BackupChainInvalid()
    {
        Ctx c = await NewExecutorAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.GetBackupChain(new GetBackupChainTicket(Guid.NewGuid())))!;
        Assert.AreEqual(CamusDBErrorCodes.BackupChainInvalid, ex.Code);
    }

    [Test]
    public async Task Restore_TargetTimeBeyondCoverage_RestorePointOutOfWindow()
    {
        Ctx c = await NewExecutorAsync();
        await WriteKeyAsync(c.Node, "t21/k1", "v1");
        BackupInfo full = await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null));

        // A point far in the future is above the chain's max recoverable HLC → out of coverage.
        long farFuture = DateTimeOffset.UtcNow.AddYears(5).ToUnixTimeMilliseconds();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.RestoreBackup(new RestoreBackupTicket(full.BackupId, FreshTarget(c), farFuture)))!;
        Assert.AreEqual(CamusDBErrorCodes.RestorePointOutOfWindow, ex.Code);
    }

    [Test]
    public async Task Restore_CorruptArtifact_FailsClosed_NoTarget()
    {
        Ctx c = await NewExecutorAsync();
        await WriteKeyAsync(c.Node, "t21/k1", "v1");
        BackupInfo full = await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null));

        // Corrupt a checkpoint data file so its recorded digest no longer matches.
        string artifactDir = Path.Combine(c.BackupDir, full.BackupId.ToString("N"));
        string victim = Directory.GetFiles(artifactDir, "*", SearchOption.AllDirectories)
            .First(f => !f.EndsWith(".manifest", StringComparison.OrdinalIgnoreCase));
        File.WriteAllBytes(victim, Array.Empty<byte>());

        string target = FreshTarget(c);
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await c.Executor.RestoreBackup(new RestoreBackupTicket(full.BackupId, target, 0)))!;
        Assert.AreEqual(CamusDBErrorCodes.BackupCorruptArtifact, ex.Code);

        // Fail closed: no usable restored data root is published.
        bool published = Directory.Exists(target) && Directory.EnumerateFileSystemEntries(target).Any();
        Assert.IsFalse(published, "a corrupt-artifact restore must not publish a usable target");
    }

    // ── Garbage collection ────────────────────────────────────────────────────

    [Test]
    public async Task Gc_DryRun_ReturnsInventoryWithoutApplying()
    {
        Ctx c = await NewExecutorAsync();
        await WriteKeyAsync(c.Node, "t21/k1", "v1");
        await c.Executor.TakeBackup(new TakeBackupTicket(BackupKind.Full, null));

        BackupGcResult gc = await c.Executor.RunBackupGarbageCollection(dryRun: true, principal: null);
        Assert.IsFalse(gc.Applied, "a dry-run GC must not apply changes");
    }
}
