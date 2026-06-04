/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="DdlOperationIdCache"/>.
/// Exercises both cache layers (completed result + in-flight TCS) and their
/// interaction without requiring HTTP or a running cluster node.
/// </summary>
[TestFixture]
public sealed class TestDdlOperationIdCache
{
    private static SchemaDdlForwardResponse OkResponse(bool applied = true) =>
        new() { Status = "ok", Applied = applied };

    // ── Layer 1: completed result cache ───────────────────────────────────────

    [Test]
    public void TryGetOrReserve_ReturnsNull_FirstCall()
    {
        DdlOperationIdCache cache = new();
        Task<SchemaDdlForwardResponse?>? result = cache.TryGetOrReserve("op1");
        Assert.IsNull(result, "first caller must receive null (proceed to execute)");
    }

    [Test]
    public async Task TryGetOrReserve_ReturnsCachedTask_AfterSetAndComplete()
    {
        DdlOperationIdCache cache = new();
        _ = cache.TryGetOrReserve("op1"); // register as owner (null return intentionally ignored)
        SchemaDdlForwardResponse resp = OkResponse();
        cache.SetAndComplete("op1", resp);

        Task<SchemaDdlForwardResponse?>? hit = cache.TryGetOrReserve("op1");

        Assert.IsNotNull(hit, "sequential retry must return a non-null task");
        Assert.IsTrue(hit!.IsCompleted, "cached task must be already completed");
        SchemaDdlForwardResponse? result = await hit;
        Assert.AreEqual("ok", result!.Status);
        Assert.IsTrue(result.Applied);
    }

    [Test]
    public void TryGetOrReserve_DifferentOperationIds_BothReturnNull()
    {
        DdlOperationIdCache cache = new();
        Assert.IsNull(cache.TryGetOrReserve("op-a"), "first call for op-a must be null");
        Assert.IsNull(cache.TryGetOrReserve("op-b"), "first call for op-b must be null");
    }

    [Test]
    public async Task SetAndComplete_NotApplied_CachesAppliedFalse()
    {
        DdlOperationIdCache cache = new();
        _ = cache.TryGetOrReserve("op1"); // register as owner (null return intentionally ignored)
        cache.SetAndComplete("op1", new SchemaDdlForwardResponse { Status = "ok", Applied = false });

        Task<SchemaDdlForwardResponse?>? hit = cache.TryGetOrReserve("op1");
        SchemaDdlForwardResponse? result = await hit!;
        Assert.IsFalse(result!.Applied, "applied:false must be preserved in the cache");
    }

    // ── Layer 2: in-flight TCS ────────────────────────────────────────────────

    [Test]
    public async Task TryGetOrReserve_ReturnsPendingTask_ConcurrentDuplicateReceivesOriginalResult()
    {
        DdlOperationIdCache cache = new();

        // First caller registers the TCS.
        Task<SchemaDdlForwardResponse?>? t1 = cache.TryGetOrReserve("op1");
        Assert.IsNull(t1, "first caller must get null (is the owner)");

        // Concurrent duplicate arrives while first is in-flight.
        Task<SchemaDdlForwardResponse?>? t2 = cache.TryGetOrReserve("op1");
        Assert.IsNotNull(t2, "duplicate must receive a non-null (in-flight) task");
        Assert.IsFalse(t2!.IsCompleted, "in-flight task must be pending");

        // Original completes.
        SchemaDdlForwardResponse resp = OkResponse(applied: true);
        cache.SetAndComplete("op1", resp);

        // Duplicate unblocks and receives the same result.
        SchemaDdlForwardResponse? result = await t2;
        Assert.AreEqual("ok", result!.Status);
        Assert.IsTrue(result.Applied);
    }

    [Test]
    public async Task TryGetOrReserve_ThirdConcurrentCaller_AlsoReceivesOriginalResult()
    {
        DdlOperationIdCache cache = new();

        _ = cache.TryGetOrReserve("op1"); // owner (null return intentionally ignored)
        Task<SchemaDdlForwardResponse?>? t2 = cache.TryGetOrReserve("op1"); // dup 1
        Task<SchemaDdlForwardResponse?>? t3 = cache.TryGetOrReserve("op1"); // dup 2

        cache.SetAndComplete("op1", OkResponse());

        SchemaDdlForwardResponse? r2 = await t2!;
        SchemaDdlForwardResponse? r3 = await t3!;

        Assert.AreEqual("ok", r2!.Status);
        Assert.AreEqual("ok", r3!.Status);
    }

    [Test]
    public void FaultAndRelease_FaultsTcs_ConcurrentWaiterThrows()
    {
        DdlOperationIdCache cache = new();
        cache.TryGetOrReserve("op1"); // owner

        Task<SchemaDdlForwardResponse?>? t2 = cache.TryGetOrReserve("op1"); // dup

        CamusDBException ex = new(CamusDBErrorCodes.TableAlreadyExists, "Table exists");
        cache.FaultAndRelease("op1", ex);

        // The duplicate's task must be faulted with the original exception.
        Assert.IsTrue(t2!.IsFaulted);
        CamusDBException? inner = t2.Exception?.GetBaseException() as CamusDBException;
        Assert.IsNotNull(inner);
        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, inner!.Code);
    }

    [Test]
    public void FaultAndRelease_DoesNotCache_AllowsSubsequentRetry()
    {
        DdlOperationIdCache cache = new();
        cache.TryGetOrReserve("op1"); // owner
        cache.FaultAndRelease("op1", new Exception("boom"));

        // A retry with the same opId after a fault must return null (first caller again).
        Task<SchemaDdlForwardResponse?>? retry = cache.TryGetOrReserve("op1");
        Assert.IsNull(retry, "retry after fault must be treated as a new operation");
    }

    // ── SetAndComplete removes in-flight before cache is stale ────────────────

    [Test]
    public async Task SetAndComplete_RemovesTcs_SubsequentCallHitsCacheNotInFlight()
    {
        DdlOperationIdCache cache = new();
        _ = cache.TryGetOrReserve("op1"); // register as owner (null return intentionally ignored)
        cache.SetAndComplete("op1", OkResponse());

        // A second call must hit the result cache (completed task), not register a new TCS.
        Task<SchemaDdlForwardResponse?>? hit = cache.TryGetOrReserve("op1");
        Assert.IsNotNull(hit);
        Assert.IsTrue(hit!.IsCompleted, "must be already-completed from cache, not a new pending TCS");
        await hit; // no exception
    }
}
