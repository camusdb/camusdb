/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator.Validators;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for the CamusDB-side backup/restore input guards: the incremental parent-id rule and the
/// restore target-directory and PITR window checks. These are pure validation with no running node.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestBackupValidators
{
    private static long UnixMsNow() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    [Test]
    public void TakeBackup_IncrementalRequiresParent()
    {
        TakeBackupValidator validator = new(CamusDBConfig.Ambient);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => validator.Validate(new TakeBackupTicket(BackupKind.Incremental, parentBackupId: null)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);

        // A valid incremental passes.
        Assert.DoesNotThrow(() => validator.Validate(new TakeBackupTicket(BackupKind.Incremental, Guid.NewGuid())));
    }

    [Test]
    public void TakeBackup_FullMustNotCarryParent()
    {
        TakeBackupValidator validator = new(CamusDBConfig.Ambient);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => validator.Validate(new TakeBackupTicket(BackupKind.Full, Guid.NewGuid())))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);

        Assert.DoesNotThrow(() => validator.Validate(new TakeBackupTicket(BackupKind.Full, parentBackupId: null)));
        Assert.DoesNotThrow(() => validator.Validate(new TakeBackupTicket(BackupKind.Coordinated, parentBackupId: null)));
    }

    [Test]
    public void Restore_RejectsRelativeOrEmptyTargetDir()
    {
        RestoreBackupValidator validator = new(CamusDBConfig.Ambient);

        Assert.Throws<CamusDBException>(
            () => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), "", 0)));
        Assert.Throws<CamusDBException>(
            () => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), "relative/dir", 0)));
    }

    [Test]
    public void Restore_RejectsLiveDataDirectory()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), "camusdb-restore-guard", Guid.NewGuid().ToString("N"));

        // The validator fixes the data directory it protects when it is constructed, so the directory
        // under test is stated as options rather than assigned to a global and restored afterwards.
        RestoreBackupValidator validator = new(CamusDBOptions.Default with { DataDirectory = dataDir });

        foreach (string bad in new[] { dataDir, Path.Combine(dataDir, "kv"), Path.Combine(dataDir, "wal") })
        {
            CamusDBException ex = Assert.Throws<CamusDBException>(
                () => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), bad, 0)))!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"target '{bad}' must be rejected");
        }

        // A sibling directory is fine.
        Assert.DoesNotThrow(
            () => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), Path.Combine(dataDir, "..", "restored"), 0)));
    }

    [Test]
    public void Restore_RejectsNegativeTargetTime_ButDelegatesCoverageToKahuna()
    {
        RestoreBackupValidator validator = new(CamusDBConfig.Ambient);

        string dir = Path.Combine(Path.GetTempPath(), "camusdb-restore-window", Guid.NewGuid().ToString("N"));

        // Negative is a plain input error.
        CamusDBException neg = Assert.Throws<CamusDBException>(
            () => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), dir, -1)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, neg.Code);

        // The validator no longer applies a wall-clock window guard: a far-past or far-future timestamp
        // and the chain-max sentinel (0) all pass here — recoverability against the chain's exact HLC
        // coverage is enforced by Kahuna (TargetOutsideCoverage), not by a heuristic in CamusDB.
        long now = UnixMsNow();
        Assert.DoesNotThrow(() => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), dir, now - 10L * 24 * 3600_000)));
        Assert.DoesNotThrow(() => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), dir, now + 60_000)));
        Assert.DoesNotThrow(() => validator.Validate(new RestoreBackupTicket(Guid.NewGuid(), dir, 0)));
    }
}
