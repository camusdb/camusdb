
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the branch snapshot-floor hold lifecycle: a branch acquires a hold on its immediate
/// parent at <c>forkT</c> on create, the leader-owned renewer keeps it alive, and a leaf drop
/// releases it. State is observed through Kahuna's <c>GetSnapshotFloor</c> introspection so the
/// tests exercise the real hold, not just the persisted id.
/// </summary>
[NonParallelizable]
public sealed class TestBranchSnapshotHold : BaseTest
{
    private async Task<(string rootName, string branchName, DatabaseDescriptor rootDb, CommandExecutor executor)>
        CreateRootAndBranch()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase();

        string branchName = "b_" + System.Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        return (rootName, branchName, rootDb, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task BranchCreate_AcquiresHold_FloorEqualsForkTimestamp()
    {
        (_, string branchName, DatabaseDescriptor rootDb, _) = await CreateRootAndBranch();

        DatabaseRegistryEntry? branchEntry = sharedRegistry!.Get(branchName);
        Assert.That(branchEntry, Is.Not.Null);
        Assert.That(branchEntry!.ImmediateParentHoldId, Is.Not.Empty,
            "Branch must persist the id of the hold it acquired on its parent");

        HLCTimestamp forkT = branchEntry.Ancestors[0].ForkTimestamp;

        (HLCTimestamp floor, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);

        Assert.That(live, Is.GreaterThanOrEqualTo(1), "The branch's hold must be live after creation");
        Assert.That(floor, Is.EqualTo(forkT), "The effective floor must sit exactly at the branch's fork timestamp");
    }

    [Test]
    [NonParallelizable]
    public async Task RenewerSweep_KeepsHoldLiveAtForkTimestamp()
    {
        (_, string branchName, DatabaseDescriptor rootDb, _) = await CreateRootAndBranch();

        HLCTimestamp forkT = sharedRegistry!.Get(branchName)!.Ancestors[0].ForkTimestamp;

        // Drive one renew sweep directly (standalone node leads the registry partition, so the sweep
        // acts). Renewing an already-live hold extends its lease and must not change the floor.
        SnapshotHoldRenewer renewer = new(
            TestNode!, sharedRegistry!, logger, CamusDB.Core.CamusDBConfig.BranchSnapshotHoldLeaseMs);
        int renewed = await renewer.RenewDueHoldsAsync(CancellationToken.None);

        Assert.That(renewed, Is.EqualTo(1),
            "The sweep must successfully renew exactly the one branch hold (proves the leader gate passed and the hold was targeted)");

        (HLCTimestamp floor, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);

        Assert.That(live, Is.GreaterThanOrEqualTo(1), "The hold must stay live after a renew sweep");
        Assert.That(floor, Is.EqualTo(forkT), "Renewal must not move the effective floor");

        await renewer.DisposeAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task BranchDrop_ReleasesHold_FloorClears()
    {
        (_, string branchName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateRootAndBranch();

        // Sanity: the hold is live before the drop.
        (_, int liveBefore) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveBefore, Is.GreaterThanOrEqualTo(1));

        await executor.DropDatabase(new DropDatabaseTicket(branchName));

        (HLCTimestamp floorAfter, int liveAfter) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);

        Assert.That(liveAfter, Is.EqualTo(0), "Dropping the only branch must release its hold");
        Assert.That(floorAfter, Is.EqualTo(HLCTimestamp.Zero), "With no live holds the floor must be cleared");
    }
}
