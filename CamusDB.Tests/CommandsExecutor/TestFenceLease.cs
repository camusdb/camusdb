
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
/// The drop-intent fence (the mutex shared by DROP / RELINK / the orphan GC) carries a bounded lease so
/// that a holder which crashes without releasing it does not block the id forever: the lease lapses and
/// any node can re-acquire. A live holder renews the lease in the background, so a long-running fenced
/// operation is never interrupted. These tests drive that behavior with a deliberately short lease.
/// </summary>
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestFenceLease : BaseTest
{
    /// <summary>
    /// A registry whose fence lease lasts <paramref name="leaseMs"/> and renews every
    /// <paramref name="renewMs"/>. Both are fixed when the registry is opened, so each test opens its
    /// registries with the timings it needs rather than changing them afterwards.
    /// </summary>
    private CamusDBOptions Fence(int leaseMs, int renewMs) =>
        Options with { FenceLeaseMs = leaseMs, FenceLeaseRenewIntervalMs = renewMs };

    /// <summary>
    /// A holder that crashes (its registry is disposed without releasing the fence — the renewer stops
    /// but the marker persists) must not block the id forever: once the lease lapses a different node can
    /// acquire the same fence. This is the permanent-block-after-failover failure the lease fixes.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DeadOwnerLeaseLapses_AllowsAnotherNodeToAcquire()
    {
        // Short lease, and a renew interval LONGER than the lease so a disposed holder cannot keep it
        // alive — after the lease elapses the fence is genuinely free.
        CamusDBOptions fence = Fence(500, 10_000);

        string id = "f" + Guid.NewGuid().ToString("n");

        DatabaseRegistry dead = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        Assert.IsTrue(await dead.AcquireDropIntentAsync(id), "sanity: fence acquired by the first owner");

        DatabaseRegistry other = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        try
        {
            // While the first owner's lease is live, the fence is genuinely held.
            Assert.IsFalse(await other.AcquireDropIntentAsync(id),
                "a live lease must block another acquirer");

            // Simulate a crash: dispose the owner (stops the renewer) WITHOUT releasing the fence.
            await dead.DisposeAsync();

            // The stale marker's lease lapses; the fence must become acquirable again.
            await WaitUntilAsync(async () => await other.AcquireDropIntentAsync(id), timeoutMs: 5_000);
        }
        finally
        {
            await other.ReleaseDropIntentAsync(id);
            await other.DisposeAsync();
        }
    }

    /// <summary>
    /// A live holder renews its lease in the background, so the fence stays held across a span longer than
    /// one lease period — a long fenced operation (a large keyspace purge) is never stolen mid-flight.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LiveHolderRenewsLease_KeepsFenceAcrossLeasePeriods()
    {
        // Renew comfortably inside the lease so the background renewer keeps refreshing it.
        CamusDBOptions fence = Fence(800, 200);

        string id = "f" + Guid.NewGuid().ToString("n");

        DatabaseRegistry holder = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        DatabaseRegistry other = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        try
        {
            Assert.IsTrue(await holder.AcquireDropIntentAsync(id), "sanity: fence acquired");

            // Wait well past two lease periods; the renewer must have kept the lease alive throughout.
            await Task.Delay(fence.FenceLeaseMs * 3);

            Assert.IsFalse(await other.AcquireDropIntentAsync(id),
                "a renewed lease must still block another acquirer after multiple lease periods");
            Assert.IsTrue(await holder.HasDropIntentAsync(id), "the holder must still see its own live fence");
        }
        finally
        {
            await holder.ReleaseDropIntentAsync(id);
            await holder.DisposeAsync();
            await other.DisposeAsync();
        }
    }

    /// <summary>
    /// An explicit release frees the fence immediately — no need to wait for the lease to lapse — and
    /// stops the renewer so it cannot revive the marker afterwards.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Release_FreesFenceImmediately_AndStopsRenewer()
    {
        // Long lease: only an explicit release can free the fence this fast.
        CamusDBOptions fence = Fence(leaseMs: 30_000, renewMs: 1_000);

        string id = "f" + Guid.NewGuid().ToString("n");

        DatabaseRegistry a = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        DatabaseRegistry b = await DatabaseRegistry.OpenAsync(TestNode!, fence);
        try
        {
            Assert.IsTrue(await a.AcquireDropIntentAsync(id));
            Assert.IsFalse(await b.AcquireDropIntentAsync(id), "held before release");

            await a.ReleaseDropIntentAsync(id);

            Assert.IsTrue(await b.AcquireDropIntentAsync(id),
                "an explicitly released fence must be immediately acquirable");

            // The prior holder's renewer must be gone: give it more than a renew interval and confirm the
            // new holder still owns the fence (the old renewer did not resurrect the old owner's marker).
            await Task.Delay(1_500);
            Assert.IsTrue(await b.HasDropIntentAsync(id), "the new holder's fence must survive");
        }
        finally
        {
            await b.ReleaseDropIntentAsync(id);
            await a.DisposeAsync();
            await b.DisposeAsync();
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, int timeoutMs)
    {
        int waited = 0;
        const int step = 100;
        while (waited < timeoutMs)
        {
            if (await condition())
                return;
            await Task.Delay(step);
            waited += step;
        }
        Assert.Fail($"condition not met within {timeoutMs}ms");
    }
}
