
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

    /// <summary>
    /// Renaming a branch must carry over its snapshot-hold id. If rename dropped the id, the renewer
    /// would stop renewing the (still-registered) branch and a drop could not release the hold —
    /// the branch would silently lose its frozen parent view once the lease lapsed.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Rename_PreservesSnapshotHoldId_RenewerAndReleaseStillWork()
    {
        (_, string branchName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateRootAndBranch();

        string originalHoldId = sharedRegistry!.Get(branchName)!.ImmediateParentHoldId;
        Assert.That(originalHoldId, Is.Not.Empty, "sanity: the branch owns a hold before rename");

        string renamed = "r_" + System.Guid.NewGuid().ToString("n");
        await executor.RenameDatabase(new RenameDatabaseTicket(branchName, renamed));

        // The hold id survives the rename.
        DatabaseRegistryEntry? renamedEntry = sharedRegistry.Get(renamed);
        Assert.That(renamedEntry, Is.Not.Null);
        Assert.That(renamedEntry!.ImmediateParentHoldId, Is.EqualTo(originalHoldId),
            "rename must preserve the branch's snapshot-hold id");

        // The renewer still finds and renews the renamed branch's hold.
        SnapshotHoldRenewer renewer = new(
            TestNode!, sharedRegistry!, logger, CamusDB.Core.CamusDBConfig.BranchSnapshotHoldLeaseMs);
        int renewed = await renewer.RenewDueHoldsAsync(CancellationToken.None);
        Assert.That(renewed, Is.EqualTo(1), "renewer must still renew the hold after rename");
        await renewer.DisposeAsync();

        // Dropping the renamed branch releases the hold — the floor clears.
        await executor.DropDatabase(new DropDatabaseTicket(renamed));
        (HLCTimestamp floor, int live) = await rootDb.Kahuna.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(live, Is.EqualTo(0), "dropping the renamed branch must release its hold");
        Assert.That(floor, Is.EqualTo(HLCTimestamp.Zero), "with no live holds the floor must be cleared");
    }

    /// <summary>
    /// The snapshot-hold renewer must sweep the persistent KV registry, not only the local in-memory
    /// cache. Branches registered on another node after the sweeping leader's startup load will not
    /// appear in the cache; if the renewer used only <c>registry.List()</c> their holds would never
    /// be renewed and would eventually lapse while the branches are still live.
    ///
    /// Simulates the cross-node scenario on a single Kahuna node by using two separate
    /// <see cref="DatabaseRegistry"/> instances with independent in-memory caches: the branch is
    /// registered via <c>registryB</c> (the "creating node"), while the sweeping renewer is backed
    /// by <c>registryA</c> (the "leader node") whose cache does not include the branch. The renewer
    /// must still find and renew the hold via <see cref="DatabaseRegistry.ScanAllEntriesAsync"/>.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Renewer_PersistentScan_RenewsBranchRegisteredOnAnotherNode()
    {
        // registryA = the "leader" node's registry, opened before the branch is created.
        // Its cache is populated at open time — it will never receive the branch create.
        DatabaseRegistry registryA = sharedRegistry!;

        // registryB = the "creating node" registry, an independent instance on the same Kahuna node.
        // It registers the branch into KV (and its own cache) but does not touch registryA's cache.
        await using DatabaseRegistry registryB = await DatabaseRegistry.OpenAsync(TestNode!);

        // Create a root database using registryA so the hold Kahuna-object (the parent's partition)
        // is reachable. The root itself only needs a name and an id in the KV; use registryB
        // for the branch registration to avoid touching registryA's cache.
        string rootName = "r_" + Guid.NewGuid().ToString("n");
        string rootId = await registryA.AllocateIdAsync();
        await registryA.RegisterAsync(rootName, rootId);
        // registryA registered root — registryB cache is stale here but doesn't matter.

        // Acquire a snapshot-floor hold on the root at a fork timestamp — mirrors CreateBranchDatabaseAsync.
        HLCTimestamp forkT = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(TestNode!.Raft.GetLocalNodeId());
        string branchId = await registryB.AllocateIdAsync();
        (KeyValueResponseType holdType, string holdId, _) = await TestNode!.Kahuna
            .LocateAndAcquireSnapshotHold(branchId, forkT, CamusDB.Core.CamusDBConfig.BranchSnapshotHoldLeaseMs, CancellationToken.None);
        Assert.That(holdType, Is.EqualTo(KeyValueResponseType.Set), "hold acquisition must succeed");

        // Register the branch using registryB so the entry lands in persistent KV but NOT in registryA's cache.
        string branchName = "b_" + Guid.NewGuid().ToString("n");
        List<DatabaseBranchAncestor> ancestors = [new() { DatabaseId = rootId, ForkTimestamp = forkT }];
        await registryB.RegisterAsync(branchName, branchId, ancestors, holdId);

        // Confirm the "cross-node" scenario: registryA does not know about the branch.
        Assert.That(registryA.List().Any(e => e.Name == branchName), Is.False,
            "registryA's cache must not contain the branch — it was registered via registryB");

        // The renewer backed by registryA must still find and renew the hold via the persistent scan.
        SnapshotHoldRenewer renewer = new(
            TestNode!, registryA, logger, CamusDB.Core.CamusDBConfig.BranchSnapshotHoldLeaseMs);
        int renewed = await renewer.RenewDueHoldsAsync(CancellationToken.None);
        await renewer.DisposeAsync();

        Assert.That(renewed, Is.GreaterThanOrEqualTo(1),
            "the renewer must find and renew the hold registered by another node (via persistent KV scan)");

        (HLCTimestamp floorAfter, int liveAfter) = await TestNode!.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.That(liveAfter, Is.GreaterThanOrEqualTo(1),
            "the hold must still be live after the renewer swept the persistent registry");
        Assert.That(floorAfter, Is.EqualTo(forkT),
            "the effective floor must remain at the branch's fork timestamp after renewal");

        // Clean up: release the hold and unregister both databases.
        await TestNode!.Kahuna.LocateAndReleaseSnapshotHold(holdId, CancellationToken.None);
        await registryA.UnregisterAsync(branchName);
        await registryA.UnregisterAsync(rootName);
    }
}
