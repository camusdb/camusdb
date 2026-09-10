/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

using static CamusDB.Tests.CommandsExecutor.BranchDuringRefreshScenarios;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A branch taken while its parent refreshes a materialized view must get the view's published
/// contents and none of the parent's rebuild: no job record, no staging relation, and no background
/// takeover that rewrites the branch's view from later branch writes. Standalone engine.
/// </summary>
[TestFixture, NonParallelizable]
internal sealed class TestBranchDuringMaterializedViewRefresh : BaseTest
{
    private (CommandExecutor executor, string rootName, string branchName) Setup()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string rootName = NewName();
        string branchName = NewName();
        TrackDatabase(rootName, executor);
        TrackDatabase(branchName, executor);
        return (executor, rootName, branchName);
    }

    [Test]
    public async Task ForkMidRefresh_BranchKeepsForkTimeContentsAndInheritsNoRebuild()
    {
        (CommandExecutor executor, string rootName, string branchName) = Setup();
        await BranchDuringRefreshScenarios.ForkMidRefresh_BranchKeepsForkTimeContentsAndInheritsNoRebuild(
            executor, TestNode!.Kahuna, rootName, branchName);
    }

    /// <summary>
    /// <c>WITH NO DATA</c> is the more dangerous statement to inherit: a takeover of it would empty the
    /// branch's view. The branch must keep the populated fork-time contents while the parent ends up
    /// unpopulated.
    /// </summary>
    [Test]
    public async Task ForkMidRefreshWithNoData_BranchKeepsPopulatedContents()
    {
        (CommandExecutor executor, string rootName, string branchName) = Setup();
        DatabaseDescriptor root = await SeedRoot(executor, rootName);

        (DatabaseDescriptor branch, MaterializedViewRefreshJob parentJob) =
            await ForkMidRefresh(executor, root, branchName, "REFRESH MATERIALIZED VIEW mv WITH NO DATA");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Keys(executor, root, "SELECT k FROM mv"));
        Assert.AreEqual(CamusDBErrorCodes.MaterializedViewNotPopulated, error!.Code,
            "the parent's WITH NO DATA refresh must complete normally around the fork");

        await AssertNoRefreshWorkInBranch(TestNode!.Kahuna, branch, parentJob);

        Assert.IsTrue(branch.Schema.Tables["mv"].IsPopulated, "the branch's view forked as populated and stays so");
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"));

        await Exec(executor, branch, "INSERT INTO t VALUES ('branch_only')");
        Assert.AreEqual(0, await executor.ReclaimAbandonedRefreshesForTesting(branchName));
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "an inherited WITH NO DATA run would have emptied the branch's view");
    }

    /// <summary>
    /// The parent's run died before the fork (its record survives with nothing working on it). The
    /// branch inherits nothing; the parent's sweep still restarts the dead run, on the parent only.
    /// </summary>
    [Test]
    public async Task ForkWhileParentHoldsADeadRun_BranchInheritsNothing_ParentStillRecovers()
    {
        (CommandExecutor executor, string rootName, string branchName) = Setup();
        DatabaseDescriptor root = await SeedRoot(executor, rootName);
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        string viewTableId = root.Schema.Tables["mv"].Id!;

        MaterializedViewRefreshJob dead = new()
        {
            JobId = "crashed",
            DatabaseId = root.Id,
            ViewTableId = viewTableId,
            ViewName = "mv",
            StagingTableId = "crashed1",
            StagingName = MaterializedViewNaming.StagingRelationName(viewTableId, "crashed1"),
        };
        await catalogs.PersistRefreshJobAsync(root, dead);

        DatabaseDescriptor branch = await executor.CreateDatabase(new CreateDatabaseTicket(branchName, false, rootName));

        await AssertNoRefreshWorkInBranch(TestNode!.Kahuna, branch, dead);
        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(root, viewTableId),
            "the fork must not disturb the parent's own record");

        await Exec(executor, branch, "INSERT INTO t VALUES ('branch_only')");
        Assert.AreEqual(0, await executor.ReclaimAbandonedRefreshesForTesting(branchName));
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"));

        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(rootName),
            "the parent's sweep must still recover the parent's dead run");
        CollectionAssert.AreEqual(new[] { "a", "b" }, await Keys(executor, root, "SELECT k FROM mv"),
            "the restarted rebuild runs on the parent");
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "the parent's recovery must not reach the branch");
    }

    /// <summary>
    /// The swap already committed at the fork point but the record's removal had not happened yet. The
    /// branch gets the rebuilt storage (its rows all predate the swap) and no record.
    /// </summary>
    [Test]
    public async Task ForkAfterSwapBeforeRecordRemoval_BranchGetsPublishedStorageAndNoRecord()
    {
        (CommandExecutor executor, string rootName, string branchName) = Setup();
        DatabaseDescriptor root = await SeedRoot(executor, rootName);
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();

        await Exec(executor, root, "REFRESH MATERIALIZED VIEW mv");
        TableSchema view = root.Schema.Tables["mv"];
        string publishedStorageId = view.EffectiveStorageId;

        MaterializedViewRefreshJob swapped = new()
        {
            JobId = "swapped",
            DatabaseId = root.Id,
            ViewTableId = view.Id!,
            ViewName = "mv",
            StagingTableId = publishedStorageId,
            StagingName = MaterializedViewNaming.StagingRelationName(view.Id!, publishedStorageId),
        };
        await catalogs.PersistRefreshJobAsync(root, swapped);

        DatabaseDescriptor branch = await executor.CreateDatabase(new CreateDatabaseTicket(branchName, false, rootName));

        CollectionAssert.IsEmpty(await CatalogsManager.ListRefreshJobsAsync(TestNode!.Kahuna, branch.Id));
        Assert.AreEqual(publishedStorageId, branch.Schema.Tables["mv"].EffectiveStorageId,
            "the branch's view names the storage the swap published");
        CollectionAssert.AreEqual(new[] { "a", "b" }, await Keys(executor, branch, "SELECT k FROM mv"));
        Assert.AreEqual(0, await executor.ReclaimAbandonedRefreshesForTesting(branchName));

        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(rootName),
            "the parent's own stale record is still cleaned up by the parent");
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(root, view.Id!));
    }

    /// <summary>
    /// Second line of defense: a record that names another database — as a branch created before the
    /// copier excluded refresh work would carry — is removed by the sweep without a rebuild, while a
    /// record that names the database itself is still restarted.
    /// </summary>
    [Test]
    public async Task ForeignRefreshJobInBranch_IsRemovedWithoutRebuild_OwnJobStillRestarts()
    {
        (CommandExecutor executor, string rootName, string branchName) = Setup();
        DatabaseDescriptor root = await SeedRoot(executor, rootName);
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();

        DatabaseDescriptor branch = await executor.CreateDatabase(new CreateDatabaseTicket(branchName, false, rootName));
        string viewTableId = branch.Schema.Tables["mv"].Id!;
        await Exec(executor, branch, "INSERT INTO t VALUES ('branch_only')");

        // Stand in for a record copied from the parent by an older fork.
        await catalogs.PersistRefreshJobAsync(branch, new MaterializedViewRefreshJob
        {
            JobId = "copied",
            DatabaseId = root.Id,
            ViewTableId = viewTableId,
            ViewName = "mv",
            StagingTableId = "copied1",
            StagingName = MaterializedViewNaming.StagingRelationName(viewTableId, "copied1"),
        });

        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(branchName),
            "the foreign record is handled: removed, not restarted");
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(branch, viewTableId));
        CollectionAssert.AreEqual(new[] { "a" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "a record that belongs to another database must never trigger a rebuild here");

        // Control: the same shape, owned by the branch, is a dead run of its own and is restarted.
        await catalogs.PersistRefreshJobAsync(branch, new MaterializedViewRefreshJob
        {
            JobId = "own",
            DatabaseId = branch.Id,
            ViewTableId = viewTableId,
            ViewName = "mv",
            StagingTableId = "own1",
            StagingName = MaterializedViewNaming.StagingRelationName(viewTableId, "own1"),
        });

        Assert.AreEqual(1, await executor.ReclaimAbandonedRefreshesForTesting(branchName));
        CollectionAssert.AreEqual(new[] { "a", "b", "branch_only" }, await Keys(executor, branch, "SELECT k FROM mv"),
            "the branch's own dead run is restarted from the branch's base table");
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(branch, viewTableId));
    }

    /// <summary>Every refresh the engine starts stamps the database it runs in on its record.</summary>
    [Test]
    public async Task ARefreshStampsItsDatabaseOnTheRecord()
    {
        (CommandExecutor executor, string rootName, _) = Setup();
        DatabaseDescriptor root = await SeedRoot(executor, rootName);
        CatalogsManager catalogs = executor.GetCatalogsManagerForTesting();
        string viewTableId = root.Schema.Tables["mv"].Id!;

        MaterializedViewRefreshJob? inFlight = null;
        Core.CommandsExecutor.Controllers.DDL.MaterializedViewRefresher.AfterStagingForTesting = async () =>
        {
            Core.CommandsExecutor.Controllers.DDL.MaterializedViewRefresher.AfterStagingForTesting = null;
            inFlight = await catalogs.TryGetRefreshJobAsync(root, viewTableId);
        };

        try
        {
            await Exec(executor, root, "REFRESH MATERIALIZED VIEW mv");
        }
        finally
        {
            Core.CommandsExecutor.Controllers.DDL.MaterializedViewRefresher.AfterStagingForTesting = null;
        }

        Assert.IsNotNull(inFlight);
        Assert.AreEqual(root.Id, inFlight!.DatabaseId);
    }
}
