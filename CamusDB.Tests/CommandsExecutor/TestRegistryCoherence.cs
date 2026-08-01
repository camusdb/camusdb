
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
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cross-node registry cache coherence: a name dropped/renamed/recreated through ONE node's registry must
/// stop resolving to its stale id through ANOTHER node whose in-memory cache was warmed with the old
/// mapping. Two <see cref="DatabaseRegistry"/> instances over the same shared node stand in for two
/// cluster nodes — each keeps its own in-memory cache but shares the Raft-replicated persistent store,
/// which is exactly the split-brain surface the generation stamp closes.
/// </summary>
internal sealed class TestRegistryCoherence : BaseTest
{
    // Cluster mode: two registries stand in for two cluster nodes, so each must run the cross-node
    // generation revalidation on a cache hit — the very path these tests exercise. In standalone mode a
    // hit is trusted without revalidation (there is only one registry), which is not what is under test here.
    private async Task<(DatabaseRegistry a, DatabaseRegistry b)> TwoNodesAsync()
        => (await DatabaseRegistry.OpenAsync(TestNode!, CamusDBConfig.Ambient, isClusterMode: true),
            await DatabaseRegistry.OpenAsync(TestNode!, CamusDBConfig.Ambient, isClusterMode: true));

    /// <summary>
    /// Node B warms a cache HIT for a name, node A drops it — B must revalidate the stale hit to "gone".
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Drop_OnOneNode_RevalidatesStaleHitToGone_OnAnother()
    {
        (DatabaseRegistry a, DatabaseRegistry b) = await TwoNodesAsync();
        try
        {
            string name = "coh_" + Guid.NewGuid().ToString("n");
            string id = await a.AllocateIdAsync();
            await a.RegisterAsync(name, id);

            // Warm B's cache with a real resolution (populates B's byName so the next call is a HIT).
            DatabaseRegistryEntry? warm = await b.TryResolveEntryAsync(name);
            Assert.IsNotNull(warm, "B must see the newly-registered database");
            Assert.AreEqual(id, warm!.Id);

            // Drop through A.
            await a.UnregisterAsync(name);

            // B's cached hit is now stale; it must be revalidated against KV and resolve to nothing.
            Assert.IsNull(await b.TryResolveEntryAsync(name),
                "a name dropped on another node must no longer resolve from a stale cache hit");
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    /// <summary>
    /// After a rename on node A, node B must stop resolving the old name and resolve the new name to the
    /// SAME id (the id is preserved across a rename).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Rename_OnOneNode_MovesResolution_OnAnother()
    {
        (DatabaseRegistry a, DatabaseRegistry b) = await TwoNodesAsync();
        try
        {
            string oldName = "coh_" + Guid.NewGuid().ToString("n");
            string newName = "coh_" + Guid.NewGuid().ToString("n");
            string id = await a.AllocateIdAsync();
            await a.RegisterAsync(oldName, id);

            Assert.AreEqual(id, (await b.TryResolveEntryAsync(oldName))!.Id, "B warms the old-name mapping");

            await a.RenameAsync(oldName, newName);

            Assert.IsNull(await b.TryResolveEntryAsync(oldName),
                "the old name must stop resolving on the other node after a rename");
            Assert.AreEqual(id, (await b.TryResolveEntryAsync(newName))!.Id,
                "the new name must resolve to the same preserved id on the other node");
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    /// <summary>
    /// The dangerous split-brain: a name dropped then recreated (fresh id) on node A must resolve to the
    /// NEW id on node B — never the retained old id, which now points at a detached/orphaned keyspace.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropThenRecreateSameName_OnOneNode_OtherNodeSeesNewId()
    {
        (DatabaseRegistry a, DatabaseRegistry b) = await TwoNodesAsync();
        try
        {
            string name = "coh_" + Guid.NewGuid().ToString("n");
            string firstId = await a.AllocateIdAsync();
            await a.RegisterAsync(name, firstId);

            Assert.AreEqual(firstId, (await b.TryResolveEntryAsync(name))!.Id, "B warms the first mapping");

            await a.UnregisterAsync(name);
            string secondId = await a.AllocateIdAsync();
            Assert.AreNotEqual(firstId, secondId, "sanity: ids are never reused");
            await a.RegisterAsync(name, secondId);

            Assert.AreEqual(secondId, (await b.TryResolveEntryAsync(name))!.Id,
                "the other node must resolve the recreated name to the NEW id, not the stale old id");
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    /// <summary>
    /// Coherence must not over-invalidate: when nothing has changed, a repeated resolve keeps returning
    /// the cached mapping (the generation matched, so no needless revalidation churn).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task StableMapping_ResolvesConsistently_WithoutFalseInvalidation()
    {
        (DatabaseRegistry a, DatabaseRegistry b) = await TwoNodesAsync();
        try
        {
            string name = "coh_" + Guid.NewGuid().ToString("n");
            string id = await a.AllocateIdAsync();
            await a.RegisterAsync(name, id);

            for (int i = 0; i < 5; i++)
                Assert.AreEqual(id, (await b.TryResolveEntryAsync(name))!.Id,
                    "a stable mapping must resolve consistently across repeated reads");
        }
        finally
        {
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }
}
