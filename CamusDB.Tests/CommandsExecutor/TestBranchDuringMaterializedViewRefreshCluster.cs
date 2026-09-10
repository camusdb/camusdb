/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The fork-during-refresh scenario on a cluster-mode engine, where the staging relation is created
/// through replicated DDL and the swap is a replicated schema change. The metadata copy reads shared
/// KV at the fork timestamp, so the exclusion of the parent's refresh work must hold here too.
/// </summary>
[TestFixture, NonParallelizable]
internal sealed class TestBranchDuringMaterializedViewRefreshCluster : SharedNodeBaseTest
{
    [Test]
    public async Task ForkMidRefresh_BranchKeepsForkTimeContentsAndInheritsNoRebuild()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string rootName = BranchDuringRefreshScenarios.NewName();
        string branchName = BranchDuringRefreshScenarios.NewName();
        TrackDatabase(rootName, executor);
        TrackDatabase(branchName, executor);

        await BranchDuringRefreshScenarios.ForkMidRefresh_BranchKeepsForkTimeContentsAndInheritsNoRebuild(
            executor, SharedKahuna, rootName, branchName);
    }
}
