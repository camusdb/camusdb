
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that a branch whose snapshot-hold chain lapses <b>fails closed</b> instead of
/// silently returning incomplete inherited data.
///
/// The hazard under test: a branch reads its ancestors frozen at the fork timestamp, protected by
/// leased Kahuna snapshot-floor holds. Once a hold lapses, revision reclamation may trim the
/// pinned history and an ancestor read then reports reclaimed rows as confirmed absences — a
/// successful, wrong, possibly empty result. These tests exercise the three layers of the fix:
/// the read-side guard that self-detects a lost hold, the renewer sweep that durably marks the
/// loss so later opens fail fast, and the creation-time keep-alive that refuses to publish a
/// branch whose hold lapsed mid-create.
///
/// The lease is shortened to 6 s so the guard's freshness window (half the lease) passes within a
/// test-sized delay. Since Kahuna 1.7.3 protection ends at the hold's REMOVAL from the registry
/// (release, or the reaper's purge), not at bare lease expiry: the fail-closed tests release the
/// hold explicitly to model that removal, while the revival test waits out a real lapse and
/// proves the branch recovers.
/// </summary>
// Serial: boots an embedded Kahuna node per test and uses timing-sensitive lease windows.
[TestFixture, NonParallelizable]
public sealed class TestBranchSnapshotProtection : BaseTest
{
    private const int LeaseMs = 6000;

    /// <summary>Half the lease: past this the guard re-verifies the chain before a read proceeds.</summary>
    private const int PastFreshnessWindowMs = LeaseMs / 2 + 500;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { BranchSnapshotHoldLeaseMs = LeaseMs };

    protected override void ConfigureNodeOptions(EmbeddedKahunaOptions options)
    {
        // Tight retention so parent churn actually reclaims history the branch depends on;
        // the positive-control test proves a live hold protects against exactly this pressure.
        options.RevisionRetention = 2;
        options.RevisionsToKeepCached = 2;
    }

    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string dbName, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor db, string dbName, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await db.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Creates root + one-row table + branch, and confirms the branch sees the row.</summary>
    private async Task<(string rootName, DatabaseDescriptor root, string branchName, DatabaseDescriptor branch, CommandExecutor executor)>
        CreateRootAndBranchWithRow()
    {
        (string rootName, DatabaseDescriptor root, CommandExecutor executor) = await CreateDatabase();

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            null!, rootName, "CREATE TABLE t (k STRING PRIMARY KEY, v STRING)", null));
        await Exec(executor, root, rootName, "INSERT INTO t (k, v) VALUES (\"row\", \"original\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        List<QueryResultRow> rows = await Query(executor, branch, branchName, "SELECT * FROM t");
        Assert.That(rows, Has.Count.EqualTo(1), "sanity: the branch must see the inherited row while protected");

        return (rootName, root, branchName, branch, executor);
    }

    private static CamusDBException AssertFailsClosed(AsyncTestDelegate read, string because)
    {
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(read, because);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.BranchSnapshotProtectionLost),
            "the refusal must carry the definitive lost-protection code, not a generic error");
        return ex;
    }

    /// <summary>
    /// Positive control: with the hold alive, heavy parent churn under retention 2 must NOT make
    /// the inherited row disappear — the snapshot floor is what keeps the fork boundary readable.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HealthyBranch_ParentChurnUnderTightRetention_StillSeesForkRow()
    {
        (string rootName, DatabaseDescriptor root, string branchName, DatabaseDescriptor branch, CommandExecutor executor) =
            await CreateRootAndBranchWithRow();

        for (int i = 0; i < 30; i++)
            await Exec(executor, root, rootName, $"UPDATE t SET v = \"updated{i}\" WHERE k = \"row\"");

        List<QueryResultRow> rows = await Query(executor, branch, branchName, "SELECT * FROM t");
        Assert.That(rows, Has.Count.EqualTo(1), "a protected branch must keep seeing the fork-point row");
        Assert.That(rows[0].Row["v"].StrValue, Is.EqualTo("original"),
            "the branch must see the fork-point value, not a later one");
    }

    /// <summary>
    /// Recovery contract (Kahuna >= 1.7.3): a hold whose lease lapses while it stays registered
    /// keeps constraining reclamation, and the next renew revives it. So a branch that merely
    /// missed renewals — downtime, a renewal outage shorter than the reaper's purge — must come
    /// back healthy: the guard's next verify revives the hold and the read returns the complete
    /// fork-point data. No sweep runs in this fixture, and the reaper's first purge tick (60 s)
    /// is far beyond the test, so the lapse window is deterministic.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LapsedButRegisteredHold_NextReadRevives_HistoryIntact()
    {
        (string rootName, DatabaseDescriptor root, string branchName, DatabaseDescriptor branch, CommandExecutor executor) =
            await CreateRootAndBranchWithRow();

        // Let the lease lapse with nothing renewing it. The hold stays registered.
        await Task.Delay(LeaseMs + 1000);

        // Churn during the lapse: the protective floor must keep the fork-point history readable
        // even though the lease is expired.
        for (int i = 0; i < 30; i++)
            await Exec(executor, root, rootName, $"UPDATE t SET v = \"updated{i}\" WHERE k = \"row\"");

        // The next ancestor read re-verifies; the renew revives the lapsed hold instead of
        // refusing, and the inherited row is intact.
        List<QueryResultRow> rows = await Query(executor, branch, branchName, "SELECT * FROM t");
        Assert.That(rows, Has.Count.EqualTo(1), "a lapsed-but-registered hold must recover, not fail the branch");
        Assert.That(rows[0].Row["v"].StrValue, Is.EqualTo("original"),
            "the fork-point value must survive churn during the lapse");

        Assert.That(await sharedRegistry!.TryGetSnapshotProtectionLostAsync(branch.Id), Is.Null,
            "a recoverable lapse must not be recorded as lost protection");

        (_, _, int liveHolds) = await root.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.GreaterThanOrEqualTo(1), "the revived hold must be live again");
    }

    /// <summary>
    /// The core repro of the silent-empty-result defect, now failing closed: once the hold is gone
    /// (modeling expiry during a renewal outage or downtime), a read that consults ancestry must
    /// refuse with <see cref="CamusDBErrorCodes.BranchSnapshotProtectionLost"/> rather than return
    /// a successful incomplete result. No sweep runs — the branch's own guard self-detects, which
    /// is what protects reads after failover or when the sweeping leader is gone.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LostHold_ReadSelfDetects_FailsClosed_NoSweepNeeded()
    {
        (string rootName, DatabaseDescriptor root, string branchName, DatabaseDescriptor branch, CommandExecutor executor) =
            await CreateRootAndBranchWithRow();

        string holdId = sharedRegistry!.Get(branchName)!.ImmediateParentHoldId;
        Assert.That(holdId, Is.Not.Empty, "sanity: the branch owns a hold");

        // Deterministically model the post-expiry state: a lapsed hold and a released one are
        // identical to Kahuna (renew answers DoesNotExist for both, forever).
        await TestNode!.Kahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None);

        for (int i = 0; i < 30; i++)
            await Exec(executor, root, rootName, $"UPDATE t SET v = \"updated{i}\" WHERE k = \"row\"");

        // Let the guard's freshness window (lease/2) pass so the next ancestor read re-verifies.
        await Task.Delay(PastFreshnessWindowMs);

        AssertFailsClosed(
            () => Query(executor, branch, branchName, "SELECT * FROM t"),
            "a lost hold must not turn inherited rows into a successful empty result");

        // Writes that consult ancestry (unique-key resolution walks the frozen parent) fail too.
        AssertFailsClosed(
            () => Exec(executor, branch, branchName, "INSERT INTO t (k, v) VALUES (\"other\", \"x\")"),
            "a write whose uniqueness check depends on the frozen parent view must also fail closed");

        // The guard durably recorded the loss so later opens fail fast everywhere.
        Assert.That(await sharedRegistry.TryGetSnapshotProtectionLostAsync(branch.Id), Is.Not.Null,
            "the guard must persist the lost-protection marker");
    }

    /// <summary>
    /// The renewer sweep turns a refused renewal into a durable lost marker, and a node that opens
    /// the branch afterwards (fresh descriptor — restart/failover shape) fails immediately with
    /// the definitive error, before paying any read.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LostHold_SweepMarksDurably_FreshOpenFailsFast()
    {
        (string rootName, DatabaseDescriptor root, string branchName, DatabaseDescriptor branch, CommandExecutor executor) =
            await CreateRootAndBranchWithRow();

        string holdId = sharedRegistry!.Get(branchName)!.ImmediateParentHoldId;
        await TestNode!.Kahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None);

        await using SnapshotHoldRenewer renewer = new(TestNode!, sharedRegistry, logger, Options.BranchSnapshotHoldLeaseMs);
        Assert.That(await renewer.RenewDueHoldsAsync(CancellationToken.None), Is.EqualTo(0),
            "the sweep must renew zero holds once the hold is gone");

        Assert.That(await sharedRegistry.TryGetSnapshotProtectionLostAsync(branch.Id), Is.Not.Null,
            "a definitive renewal refusal must durably mark the branch's protection as lost");

        // A separate engine (fresh descriptor cache — the restart/failover shape) opens the branch
        // pre-latched from the marker: the very first read refuses, with no timing dependence.
        CommandExecutor freshExecutor = CreateCommandExecutor();
        DatabaseDescriptor reopened = await freshExecutor.OpenDatabase(branchName);

        AssertFailsClosed(
            () => Query(freshExecutor, reopened, branchName, "SELECT * FROM t"),
            "an open after the durable mark must fail closed immediately");
    }

    /// <summary>
    /// Descendants fail closed too: a grandchild's deeper frozen levels stay readable only while
    /// every ancestor's own hold lives, so losing the middle branch's hold must refuse the
    /// grandchild's reads, not only the middle branch's.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LostAncestorHold_FailsGrandchildClosed()
    {
        (string rootName, DatabaseDescriptor root, string b1Name, DatabaseDescriptor b1, CommandExecutor executor) =
            await CreateRootAndBranchWithRow();

        string b2Name = NewName();
        DatabaseDescriptor b2 = await executor.CreateDatabase(
            new CreateDatabaseTicket(b2Name, ifNotExists: false, branchFrom: b1Name));
        TrackDatabase(b2Name, executor);

        Assert.That(await Query(executor, b2, b2Name, "SELECT * FROM t"), Has.Count.EqualTo(1),
            "sanity: the grandchild sees the inherited row while the chain is protected");

        // Lose the MIDDLE branch's hold (the one pinning the root's history at b1's fork point).
        string b1HoldId = sharedRegistry!.Get(b1Name)!.ImmediateParentHoldId;
        await TestNode!.Kahuna.LocateAndReleaseSnapshotHold(b1HoldId, CancellationToken.None);

        await Task.Delay(PastFreshnessWindowMs);

        AssertFailsClosed(
            () => Query(executor, b2, b2Name, "SELECT * FROM t"),
            "losing an ancestor's hold must fail the grandchild's ancestry reads closed");

        AssertFailsClosed(
            () => Query(executor, b1, b1Name, "SELECT * FROM t"),
            "the middle branch itself must fail closed as well");
    }

    /// <summary>
    /// Creation validates its keep-alive before publishing: a hold lost mid-create (a metadata copy
    /// slower than the lease) must abort the create, retract nothing half-published, and release
    /// the hold — never hand back a registered branch whose frozen view is already unprotected.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HoldLostDuringCreation_AbortsWithoutPublishing()
    {
        (string rootName, DatabaseDescriptor root, CommandExecutor executor) = await CreateDatabase();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            null!, rootName, "CREATE TABLE t (k STRING PRIMARY KEY)", null));

        string branchName = NewName();

        SnapshotHoldLease.LoseEveryHoldForTesting = true;
        try
        {
            Assert.ThrowsAsync<CamusDBException>(
                () => executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)),
                "creation must abort when the fork-point hold is lost before publication");
        }
        finally
        {
            SnapshotHoldLease.LoseEveryHoldForTesting = false;
        }

        Assert.That(sharedRegistry!.Get(branchName), Is.Null,
            "the aborted branch must not be registered");

        (_, _, int liveHolds) = await root.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveHolds, Is.EqualTo(0),
            "the aborted create must release its hold so the parent's history can be reclaimed");
    }
}
