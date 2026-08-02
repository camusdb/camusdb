
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Startup drop-intent cleanup must be epoch-scoped: it may only clear this node's markers left by a
/// <em>prior</em> run (crash remnants), never a marker created by the <em>current</em> run — which is a
/// live, in-flight fence. Without this, the startup scrub could delete a fence the concurrently-started
/// reclaimer or an incoming relink just acquired, letting two operations think they hold it.
/// </summary>
internal sealed class TestStartupEpochFence : BaseTest
{
    [Test]
    [NonParallelizable]
    public async Task ClearOwnStaleDropIntents_KeepsCurrentEpochFence_ClearsPriorRunFence()
    {
        // Two registries on the SAME node share the node id but each mints a fresh startup epoch — the
        // second stands in for a process restart.
        DatabaseRegistry current = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        DatabaseRegistry afterRestart = await DatabaseRegistry.OpenAsync(TestNode!, Options);

        try
        {
            string id = "e" + Guid.NewGuid().ToString("n");

            Assert.IsTrue(await current.AcquireDropIntentAsync(id), "sanity: fence acquired");
            Assert.IsTrue(await current.HasDropIntentAsync(id));

            // Same run: the fence is current-epoch (live), so startup cleanup must leave it alone.
            Assert.AreEqual(0, await current.ClearOwnStaleDropIntentsAsync(),
                "current-epoch (live) fence must not be counted as stale");
            Assert.IsTrue(await current.HasDropIntentAsync(id),
                "a live in-flight fence must survive this run's startup cleanup");

            // After a "restart" (new epoch, same node) the same marker is a prior-run remnant and must
            // be cleared so a permanently-abandoned fence never blocks the id forever.
            Assert.GreaterOrEqual(await afterRestart.ClearOwnStaleDropIntentsAsync(), 1,
                "a prior-run stale fence must be reclaimed on restart");
            Assert.IsFalse(await afterRestart.HasDropIntentAsync(id),
                "the prior-run fence must be gone after restart cleanup");
        }
        finally
        {
            await current.DisposeAsync();
            await afterRestart.DisposeAsync();
        }
    }
}
