/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
[NonParallelizable]
public sealed class TestSpillFileManager
{
    private string _dataDir = null!;
    private string _savedInstanceId = null!;
    private bool _savedSpillEnabled;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_spill_tests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
        _savedInstanceId = SpillFileManager.InstanceId;
        _savedSpillEnabled = CamusDBConfig.SpillEnabled;
    }

    [TearDown]
    public void TearDown()
    {
        SpillFileManager.InstanceId = _savedInstanceId;
        CamusDBConfig.SpillEnabled = _savedSpillEnabled;
        CamusDBConfig.ForceSpillThresholdRows = null;

        try { Directory.Delete(_dataDir, recursive: true); }
        catch { /* ignore */ }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SpillScope — normal dispose deletes directory
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Scope_NormalDispose_DirectoryGone()
    {
        SpillScope scope = SpillFileManager.CreateScope(_dataDir);
        string dir = scope.ScopeDirectory;

        Assert.That(Directory.Exists(dir), Is.True, "scope dir should exist after creation");

        await scope.DisposeAsync();

        Assert.That(Directory.Exists(dir), Is.False, "scope dir should be deleted after dispose");
    }

    [Test]
    public async Task Scope_AwaitUsing_DirectoryGone()
    {
        string dir;
        await using (SpillScope scope = SpillFileManager.CreateScope(_dataDir))
        {
            dir = scope.ScopeDirectory;
            Assert.That(Directory.Exists(dir), Is.True);
        }
        Assert.That(Directory.Exists(dir), Is.False);
    }

    [Test]
    public async Task Scope_ExceptionInsideUsing_DirectoryGone()
    {
        string dir = "";
        try
        {
            await using SpillScope scope = SpillFileManager.CreateScope(_dataDir);
            dir = scope.ScopeDirectory;
            throw new InvalidOperationException("test exception");
        }
        catch (InvalidOperationException) { }

        Assert.That(dir, Is.Not.Empty);
        Assert.That(Directory.Exists(dir), Is.False, "scope dir must be deleted even after exception");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SpillScope — OpenWriter / OpenReader round-trip
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Scope_WriteAndRead_ContentPreserved()
    {
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03];

        await using SpillScope scope = SpillFileManager.CreateScope(_dataDir);

        string path = scope.OpenWriter(out FileStream writer);
        await writer.WriteAsync(payload);
        await writer.FlushAsync();
        writer.Close();

        using FileStream reader = scope.OpenReader(path);
        byte[] buf = new byte[payload.Length];
        int read = await reader.ReadAsync(buf);

        Assert.That(read, Is.EqualTo(payload.Length));
        Assert.That(buf, Is.EqualTo(payload));
    }

    [Test]
    public async Task Scope_MultipleWriterFiles_AllDeletedOnDispose()
    {
        string dir;
        string path1, path2;

        await using (SpillScope scope = SpillFileManager.CreateScope(_dataDir))
        {
            dir = scope.ScopeDirectory;
            path1 = scope.OpenWriter(out FileStream w1);
            await w1.WriteAsync(new byte[] { 1 });
            await w1.FlushAsync();
            w1.Close();

            path2 = scope.OpenWriter(out FileStream w2);
            await w2.WriteAsync(new byte[] { 2 });
            await w2.FlushAsync();
            w2.Close();

            Assert.That(File.Exists(path1), Is.True);
            Assert.That(File.Exists(path2), Is.True);
        }

        Assert.That(Directory.Exists(dir), Is.False);
        Assert.That(File.Exists(path1), Is.False);
        Assert.That(File.Exists(path2), Is.False);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SpillFileManager — scopes are under InstanceId subdir
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void CreateScope_ScopeDirIsUnderInstanceDir()
    {
        SpillFileManager.InstanceId = "test-inst";
        SpillScope scope = SpillFileManager.CreateScope(_dataDir);

        string expected = Path.Combine(_dataDir, "tmp", "spill", "test-inst");
        Assert.That(scope.ScopeDirectory, Does.StartWith(expected));

        scope.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Test]
    public void CreateScope_TwoScopes_HaveDistinctDirs()
    {
        SpillScope s1 = SpillFileManager.CreateScope(_dataDir);
        SpillScope s2 = SpillFileManager.CreateScope(_dataDir);

        Assert.That(s1.ScopeDirectory, Is.Not.EqualTo(s2.ScopeDirectory));

        s1.DisposeAsync().AsTask().GetAwaiter().GetResult();
        s2.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SpillFileManager — startup sweep
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void StartupSweep_RemovesThisInstanceOrphanDir()
    {
        SpillFileManager.InstanceId = "sweep-me";

        // Plant orphan under this instance's dir
        string orphanDir = Path.Combine(_dataDir, "tmp", "spill", "sweep-me");
        Directory.CreateDirectory(orphanDir);
        File.WriteAllBytes(Path.Combine(orphanDir, "orphan.spill"), [0x01]);

        Assert.That(Directory.Exists(orphanDir), Is.True);

        SpillFileManager.RunStartupSweep(_dataDir);

        Assert.That(Directory.Exists(orphanDir), Is.False, "sweep must delete own instance dir");
    }

    [Test]
    public void StartupSweep_DoesNotTouchOtherInstanceDir()
    {
        SpillFileManager.InstanceId = "my-instance";

        // Plant orphan under this instance's dir
        string myDir = Path.Combine(_dataDir, "tmp", "spill", "my-instance");
        Directory.CreateDirectory(myDir);
        File.WriteAllBytes(Path.Combine(myDir, "orphan.spill"), [0x01]);

        // Plant file under a DIFFERENT instance's dir
        string otherDir = Path.Combine(_dataDir, "tmp", "spill", "other-instance");
        Directory.CreateDirectory(otherDir);
        string otherFile = Path.Combine(otherDir, "live.spill");
        File.WriteAllBytes(otherFile, [0x02]);

        SpillFileManager.RunStartupSweep(_dataDir);

        Assert.That(Directory.Exists(myDir), Is.False, "own instance dir must be deleted");
        Assert.That(File.Exists(otherFile), Is.True, "other instance's file must be untouched");
    }

    [Test]
    public void StartupSweep_NoInstanceDir_IsNoOp()
    {
        SpillFileManager.InstanceId = "nonexistent-instance";
        Assert.DoesNotThrow(() => SpillFileManager.RunStartupSweep(_dataDir));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // CADB0507 — unwritable temp store
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    [Platform("Unix,Linux,MacOsX")]
    public void CreateScope_UnwritablePath_ThrowsSpillStorageUnavailable()
    {
        // Point the data dir at a path that cannot be created (root-owned, no write perms)
        string badDir = "/proc/camusdb_spill_test_impossible_path";

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SpillFileManager.CreateScope(badDir))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillStorageUnavailable));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Config knobs present and default values
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void Config_DefaultValues()
    {
        Assert.That(CamusDBConfig.SpillEnabled, Is.False, "SpillEnabled must default to false");
        Assert.That(CamusDBConfig.SpillThresholdRows, Is.EqualTo(500_000));
        Assert.That(CamusDBConfig.SpillMergeFanIn, Is.EqualTo(16));
        Assert.That(CamusDBConfig.ForceSpillThresholdRows, Is.Null);
    }

    [Test]
    public void Config_ForceThreshold_OverridesEffective()
    {
        CamusDBConfig.ForceSpillThresholdRows = 10;
        Assert.That(CamusDBConfig.SpillEffectiveThreshold, Is.EqualTo(10));

        CamusDBConfig.ForceSpillThresholdRows = null;
        Assert.That(CamusDBConfig.SpillEffectiveThreshold, Is.EqualTo(CamusDBConfig.SpillThresholdRows));
    }

    [Test]
    public void Config_SpillEnabled_False_ScopesCanStillBeCreated()
    {
        // SpillEnabled=false means OPERATORS won't call CreateScope.
        // The manager itself does not enforce the flag — callers do.
        CamusDBConfig.SpillEnabled = false;

        SpillScope scope = SpillFileManager.CreateScope(_dataDir);
        Assert.That(Directory.Exists(scope.ScopeDirectory), Is.True);
        scope.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
