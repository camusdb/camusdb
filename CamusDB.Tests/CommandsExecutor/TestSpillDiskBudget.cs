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

/// <summary>
/// The byte limits on spill files: the total over all live scopes of one spill root
/// (<see cref="CamusDBOptions.SpillMaxTotalBytes"/>) and the free-space floor
/// (<see cref="CamusDBOptions.MinFreeDiskBytes"/>). The budget tests use a fake free-space reading;
/// the scope tests write real files under a temporary directory.
/// </summary>
[TestFixture]
// Serial: the scope tests use the process-wide spill instance id and the per-root budget registry.
[NonParallelizable]
public sealed class TestSpillDiskBudget
{
    private const long Unlimited = 0;

    private string _dataDir = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_spill_budget_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        try { Directory.Delete(_dataDir, recursive: true); }
        catch { /* ignore */ }
    }

    // ── Total-bytes limit ─────────────────────────────────────────────────────

    [Test]
    public void Reserve_UnderTheLimit_CountsAndReleaseGivesBack()
    {
        SpillDiskBudget budget = new(() => null);

        budget.Reserve(60, maxTotalBytes: 100, minFreeDiskBytes: Unlimited);
        budget.Reserve(40, maxTotalBytes: 100, minFreeDiskBytes: Unlimited);
        Assert.That(budget.UsedBytes, Is.EqualTo(100), "A total exactly at the limit is allowed.");

        budget.Release(100);
        Assert.That(budget.UsedBytes, Is.Zero);
    }

    [Test]
    public void Reserve_PastTheLimit_ThrowsAndReservesNothing()
    {
        SpillDiskBudget budget = new(() => null);
        budget.Reserve(60, maxTotalBytes: 100, minFreeDiskBytes: Unlimited);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => budget.Reserve(41, maxTotalBytes: 100, minFreeDiskBytes: Unlimited))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded));
        Assert.That(budget.UsedBytes, Is.EqualTo(60), "A refused reservation must be given back.");

        budget.Reserve(40, maxTotalBytes: 100, minFreeDiskBytes: Unlimited);
        Assert.That(budget.UsedBytes, Is.EqualTo(100), "A write that fits must still pass after a refusal.");
    }

    [TestCase(0L)]
    [TestCase(-1L)]
    public void Reserve_LimitZeroOrLess_IsOff(long maxTotalBytes)
    {
        SpillDiskBudget budget = new(() => null);

        budget.Reserve(long.MaxValue / 4, maxTotalBytes, minFreeDiskBytes: Unlimited);

        Assert.That(budget.UsedBytes, Is.EqualTo(long.MaxValue / 4));
    }

    // ── Free-space floor ──────────────────────────────────────────────────────

    [Test]
    public void Reserve_EstimateBelowTheFloor_ThrowsInsufficientDiskSpace()
    {
        // A 1000-byte volume that only spill writes fill. Between two probes the estimate subtracts
        // the spill bytes reserved since the reading; a new probe sees the written bytes. Either
        // way the answer is the same, so the test does not depend on timing.
        long written = 0;
        SpillDiskBudget budget = new(() => 1000 - written);

        budget.Reserve(400, Unlimited, minFreeDiskBytes: 500);
        written += 400;

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => budget.Reserve(200, Unlimited, minFreeDiskBytes: 500))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InsufficientDiskSpace));
        Assert.That(budget.UsedBytes, Is.EqualTo(400), "A refused reservation must be given back.");

        budget.Reserve(100, Unlimited, minFreeDiskBytes: 500);
        Assert.That(budget.UsedBytes, Is.EqualTo(500), "Free space exactly at the floor is allowed.");
    }

    [Test]
    public void Reserve_FirstWriteAlreadyBelowTheFloor_Throws()
    {
        SpillDiskBudget budget = new(() => 100);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => budget.Reserve(1, Unlimited, minFreeDiskBytes: 500))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InsufficientDiskSpace));
        Assert.That(budget.UsedBytes, Is.Zero);
    }

    [Test]
    public void Reserve_UnknownOrFailingReading_FailsOpen()
    {
        SpillDiskBudget unknown = new(() => null);
        unknown.Reserve(1_000_000, Unlimited, minFreeDiskBytes: long.MaxValue);

        SpillDiskBudget failing = new(() => throw new IOException("probe failed"));
        failing.Reserve(1_000_000, Unlimited, minFreeDiskBytes: long.MaxValue);

        Assert.That(unknown.UsedBytes, Is.EqualTo(1_000_000));
        Assert.That(failing.UsedBytes, Is.EqualTo(1_000_000));
    }

    [Test]
    public async Task Reserve_BelowTheFloorButSpaceWasFreed_ProbesAgainAndPasses()
    {
        long free = 1000;
        SpillDiskBudget budget = new(() => free);

        budget.Reserve(400, Unlimited, minFreeDiskBytes: 500);

        // Another process frees space. The estimate from the old reading is below the floor, so a
        // new probe must confirm it before the write is refused. The wait passes the short window
        // in which the old reading is trusted.
        free = 10_000;
        await Task.Delay(100);

        budget.Reserve(400, Unlimited, minFreeDiskBytes: 500);
        Assert.That(budget.UsedBytes, Is.EqualTo(800));
    }

    [Test]
    public void Reserve_ReleaseAfterTheReading_AddsSpaceBackToTheEstimate()
    {
        SpillDiskBudget budget = new(() => 1000);

        budget.Reserve(400, Unlimited, minFreeDiskBytes: 500);
        budget.Release(400);

        // The reading did not see the first 400 bytes, and they are deleted now, so the estimate
        // is again 1000 and a 500-byte write leaves exactly the floor.
        budget.Reserve(500, Unlimited, minFreeDiskBytes: 500);
        Assert.That(budget.UsedBytes, Is.EqualTo(500));
    }

    // ── Scopes ────────────────────────────────────────────────────────────────

    private CamusDBOptions Limit(long maxTotalBytes) =>
        CamusDBOptions.Default with { SpillMaxTotalBytes = maxTotalBytes, DataDirectory = _dataDir };

    [Test]
    public async Task Scope_WritePastTheLimit_ThrowsBeforeTheBytesReachTheFile()
    {
        string path;

        await using (SpillScope scope = SpillFileManager.CreateScope(_dataDir, Limit(100)))
        {
            path = scope.OpenWriter(out SpillWriteStream writer);
            writer.Write(new byte[60]);

            CamusDBException ex = Assert.Throws<CamusDBException>(() => writer.Write(new byte[60]))!;
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded));

            await writer.FlushAsync();
            Assert.That(new FileInfo(path).Length, Is.EqualTo(60), "The refused write must not reach the file.");
            Assert.That(scope.ReservedBytes, Is.EqualTo(60));
            Assert.That(SpillFileManager.BudgetFor(_dataDir).UsedBytes, Is.EqualTo(60));
        }

        Assert.That(File.Exists(path), Is.False);
        Assert.That(SpillFileManager.BudgetFor(_dataDir).UsedBytes, Is.Zero,
            "Disposing the scope must give back every byte it reserved.");
    }

    [Test]
    public async Task Scope_EveryWriteOverloadIsCounted()
    {
        await using SpillScope scope = SpillFileManager.CreateScope(_dataDir, Limit(Unlimited));
        scope.OpenWriter(out SpillWriteStream writer);

        writer.Write(new byte[3]);
        writer.Write(new byte[10], 2, 5);
        writer.WriteByte(1);
        await writer.WriteAsync(new byte[7]);
        await writer.WriteAsync(new byte[10], 1, 4);

        Assert.That(scope.ReservedBytes, Is.EqualTo(3 + 5 + 1 + 7 + 4));
    }

    [Test]
    public async Task Scope_TwoScopesOfOneRoot_ShareTheLimit()
    {
        SpillScope a = SpillFileManager.CreateScope(_dataDir, Limit(100));
        SpillScope b = SpillFileManager.CreateScope(_dataDir, Limit(100));
        try
        {
            a.OpenWriter(out SpillWriteStream wa);
            b.OpenWriter(out SpillWriteStream wb);

            wa.Write(new byte[60]);

            CamusDBException ex = Assert.Throws<CamusDBException>(() => wb.Write(new byte[60]))!;
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded),
                "The limit is for the node, so a second query must see the bytes of the first.");

            await a.DisposeAsync();

            wb.Write(new byte[60]);
            Assert.That(SpillFileManager.BudgetFor(_dataDir).UsedBytes, Is.EqualTo(60),
                "After the first scope ends, its bytes must be free for the second.");
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }

        Assert.That(SpillFileManager.BudgetFor(_dataDir).UsedBytes, Is.Zero);
    }

    [Test]
    public async Task Scope_DisposeTwice_ReleasesOnce()
    {
        string other = Path.Combine(_dataDir, "other");
        SpillScope holder = SpillFileManager.CreateScope(other, Limit(Unlimited));
        holder.OpenWriter(out SpillWriteStream hw);
        hw.Write(new byte[50]);

        SpillScope scope = SpillFileManager.CreateScope(other, Limit(Unlimited));
        scope.OpenWriter(out SpillWriteStream w);
        w.Write(new byte[30]);

        await scope.DisposeAsync();
        await scope.DisposeAsync();

        Assert.That(SpillFileManager.BudgetFor(other).UsedBytes, Is.EqualTo(50),
            "A second dispose must not give back the bytes of another scope.");

        await holder.DisposeAsync();
        Assert.That(SpillFileManager.BudgetFor(other).UsedBytes, Is.Zero);
    }

    [Test]
    public async Task Scope_RealVolumeBelowAnImpossibleFloor_ThrowsInsufficientDiskSpace()
    {
        // No volume has this much free space, so the real probe must put the write below the floor.
        CamusDBOptions options = Limit(Unlimited) with { MinFreeDiskBytes = long.MaxValue / 2 };

        await using SpillScope scope = SpillFileManager.CreateScope(_dataDir, options);
        string path = scope.OpenWriter(out SpillWriteStream writer);

        CamusDBException ex = Assert.Throws<CamusDBException>(() => writer.Write(new byte[16]))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InsufficientDiskSpace));

        await writer.FlushAsync();
        Assert.That(new FileInfo(path).Length, Is.Zero);
    }

    [Test]
    public void Config_DefaultTotalLimitIsEightGiB()
    {
        Assert.That(CamusDBOptions.Default.SpillMaxTotalBytes, Is.EqualTo(8L * 1024 * 1024 * 1024));
    }
}
