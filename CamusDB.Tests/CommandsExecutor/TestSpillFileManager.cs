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
        SpillFileManager.ReleaseInstanceLock();
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
    // Instance lock
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void AcquireInstanceLock_CreatesLockFile()
    {
        SpillFileManager.AcquireInstanceLock(_dataDir);

        string lockPath = Path.Combine(_dataDir, "tmp", "spill", SpillFileManager.InstanceId, ".lock");
        Assert.That(File.Exists(lockPath), Is.True, ".lock file must be created");
    }

    [Test]
    public void AcquireInstanceLock_IsIdempotent()
    {
        SpillFileManager.AcquireInstanceLock(_dataDir);
        Assert.DoesNotThrow(() => SpillFileManager.AcquireInstanceLock(_dataDir));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Startup sweep — liveness-based orphan detection
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void StartupSweep_RemovesOrphanDir_NoLockFile()
    {
        // Dir with no .lock file: crashed before acquiring, or old code version.
        string orphanDir = Path.Combine(_dataDir, "tmp", "spill", "orphan-no-lock");
        Directory.CreateDirectory(orphanDir);
        File.WriteAllBytes(Path.Combine(orphanDir, "data.spill"), [0x01]);

        SpillFileManager.AcquireInstanceLock(_dataDir);
        SpillFileManager.RunStartupSweep(_dataDir);

        Assert.That(Directory.Exists(orphanDir), Is.False, "orphan dir with no .lock must be deleted");
    }

    [Test]
    public void StartupSweep_RemovesOrphanDir_ReleasedLock()
    {
        // Dir with a .lock file that is NOT held — simulates a dead process.
        string orphanDir = Path.Combine(_dataDir, "tmp", "spill", "dead-instance");
        Directory.CreateDirectory(orphanDir);
        File.WriteAllBytes(Path.Combine(orphanDir, ".lock"), []);    // file exists but not locked
        File.WriteAllBytes(Path.Combine(orphanDir, "data.spill"), [0x01]);

        SpillFileManager.AcquireInstanceLock(_dataDir);
        SpillFileManager.RunStartupSweep(_dataDir);

        Assert.That(Directory.Exists(orphanDir), Is.False, "orphan dir with released .lock must be deleted");
    }

    [Test]
    public void StartupSweep_DoesNotTouchLiveInstanceDir()
    {
        // Create another dir whose .lock is exclusively held — simulates a live concurrent process.
        string liveDir = Path.Combine(_dataDir, "tmp", "spill", "live-instance");
        Directory.CreateDirectory(liveDir);
        string liveDataFile = Path.Combine(liveDir, "data.spill");
        File.WriteAllBytes(liveDataFile, [0x02]);

        using FileStream liveLock = new(
            Path.Combine(liveDir, ".lock"),
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None);

        SpillFileManager.AcquireInstanceLock(_dataDir);
        SpillFileManager.RunStartupSweep(_dataDir);

        Assert.That(File.Exists(liveDataFile), Is.True, "live instance dir must not be touched");
    }

    [Test]
    public void StartupSweep_DoesNotDeleteOwnInstanceDir()
    {
        SpillFileManager.AcquireInstanceLock(_dataDir);
        SpillFileManager.RunStartupSweep(_dataDir);

        string myDir = Path.Combine(_dataDir, "tmp", "spill", SpillFileManager.InstanceId);
        Assert.That(Directory.Exists(myDir), Is.True, "own instance dir must survive the sweep");
    }

    [Test]
    public void StartupSweep_NoSpillRoot_IsNoOp()
    {
        // No {DataDir}/tmp/spill/ exists yet — sweep must not throw.
        SpillFileManager.AcquireInstanceLock(_dataDir);
        string emptyDir = Path.Combine(Path.GetTempPath(), "camusdb_empty_" + Guid.NewGuid().ToString("N"));
        try
        {
            Assert.DoesNotThrow(() => SpillFileManager.RunStartupSweep(emptyDir));
        }
        finally
        {
            try { Directory.Delete(emptyDir, recursive: true); } catch { }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // CADB0507 — unwritable temp store
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    [Platform("Unix,Linux,MacOsX")]
    public void CreateScope_UnwritablePath_ThrowsSpillStorageUnavailable()
    {
        string badDir = "/proc/camusdb_spill_test_impossible_path";

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SpillFileManager.CreateScope(badDir))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillStorageUnavailable));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SpillRunReader — read-side failure mapping (SP16)
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that opening a missing spill file via <see cref="SpillRunReader.OpenAsync"/>
    /// surfaces a <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> rather than a bare
    /// <see cref="FileNotFoundException"/>.
    ///
    /// <para>Negative proof: removing the try/catch in <c>SpillRunReader.OpenAsync</c> causes
    /// <c>FileStream</c> to throw <c>FileNotFoundException</c> (a raw <c>IOException</c>
    /// subclass), which does NOT satisfy <c>Assert.ThrowsAsync&lt;CamusDBException&gt;</c>.</para>
    /// </summary>
    [Test]
    public async Task SpillRunReader_MissingFile_ThrowsSpillStorageUnavailable()
    {
        string missingPath = Path.Combine(_dataDir, "does_not_exist.spill");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await SpillRunReader.OpenAsync(missingPath))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillStorageUnavailable),
            "A missing spill file must surface as CADB0507, not a raw IOException.");
    }

    /// <summary>
    /// Verifies that an I/O failure during <see cref="SpillRunReader.AdvanceAsync"/> (simulated
    /// by writing a frame-length header that claims 100 payload bytes but providing only 4)
    /// surfaces <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> rather than a raw
    /// <see cref="EndOfStreamException"/>.
    ///
    /// <para>The call chain is <c>OpenAsync</c> → <c>AdvanceAsync</c> →
    /// <c>ReadExactlyAsync</c> → <c>EndOfStreamException</c> (an <c>IOException</c> subclass)
    /// → mapped to CADB0507 by the catch in <c>AdvanceAsync</c>.</para>
    ///
    /// <para>Negative proof: removing the try/catch in <c>AdvanceAsync</c> lets
    /// <c>EndOfStreamException</c> propagate as-is, which does NOT satisfy
    /// <c>Assert.ThrowsAsync&lt;CamusDBException&gt;</c>.</para>
    /// </summary>
    [Test]
    public async Task SpillRunReader_TruncatedPayload_ThrowsSpillStorageUnavailable()
    {
        // Write a valid 4-byte length header claiming 100 payload bytes, then only 4 actual bytes.
        // ReadExactlyAsync will throw EndOfStreamException (IOException subclass) when it cannot
        // fill the promised 100 bytes.
        string truncated = Path.Combine(_dataDir, "truncated.spill");
        await using (FileStream fw = new(truncated, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            byte[] lenBuf = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lenBuf, 100);
            await fw.WriteAsync(lenBuf);
            await fw.WriteAsync(new byte[] { 1, 2, 3, 4 }); // only 4 of the promised 100 bytes
        }

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await SpillRunReader.OpenAsync(truncated, default))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillStorageUnavailable),
            "A truncated spill file must surface as CADB0507, not a raw EndOfStreamException.");
    }

    /// <summary>
    /// Verifies that a spill file truncated within its 4-byte frame-length header (here, a
    /// 2-byte file) also surfaces <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> —
    /// the same failure class as a payload-truncated file, so the two must map identically.
    /// This path throws <c>InvalidDataException</c> (not an <c>IOException</c> subclass), so it
    /// only maps correctly if the read catch covers corrupt/truncated framing too.
    /// </summary>
    [Test]
    public async Task SpillRunReader_TruncatedHeader_ThrowsSpillStorageUnavailable()
    {
        string truncated = Path.Combine(_dataDir, "truncated_header.spill");
        await using (FileStream fw = new(truncated, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            await fw.WriteAsync(new byte[] { 1, 2 }); // only 2 of the 4 header bytes
        }

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await SpillRunReader.OpenAsync(truncated, default))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillStorageUnavailable),
            "A header-truncated spill file must surface as CADB0507, like a payload-truncated one.");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Config knobs
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public void Config_DefaultValues()
    {
        Assert.That(CamusDBConfig.SpillEnabled, Is.False);
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
}
