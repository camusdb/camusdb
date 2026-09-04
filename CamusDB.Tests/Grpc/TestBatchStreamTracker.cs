
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.App.Grpc;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Covers the per-stream bookkeeping of the duplex batch handler. The important property is a memory
/// property: a stream that runs millions of operations must not retain one task for each of them.
/// A correctness suite cannot see that, so these tests assert the bound and the collection directly.
/// </summary>
public class TestBatchStreamTracker
{
    private static (long Pt, uint Counter) Handle(long id) => (id, 0u);

    [Test]
    public void PredecessorForUnknownHandleIsCompleted()
    {
        BatchStreamTracker tracker = new(maxBuffered: 8);

        Assert.That(tracker.PredecessorFor(Handle(1)).IsCompleted, Is.True);
        Assert.That(tracker.ChainCount, Is.EqualTo(0));
    }

    [Test]
    public void RecordedTailBecomesThePredecessorForTheSameHandle()
    {
        BatchStreamTracker tracker = new(maxBuffered: 8);
        TaskCompletionSource op = new();

        tracker.RecordTail(Handle(1), op.Task);

        Assert.That(tracker.PredecessorFor(Handle(1)), Is.SameAs(op.Task));
        Assert.That(tracker.PredecessorFor(Handle(2)).IsCompleted, Is.True);

        op.SetResult();
    }

    [Test]
    public void ChainMapStaysBoundedAcrossManyFinishedHandles()
    {
        // The map admits twice the buffer limit before it sweeps, so it can never hold more than
        // that. Without the sweep this loop would leave 5,000 entries.
        BatchStreamTracker tracker = new(maxBuffered: 8);

        for (long id = 0; id < 5_000; id++)
            tracker.RecordTail(Handle(id), Task.CompletedTask);

        Assert.That(tracker.ChainCount, Is.LessThanOrEqualTo(16));
    }

    [Test]
    public void SweepKeepsTheTailsThatAreStillRunning()
    {
        BatchStreamTracker tracker = new(maxBuffered: 8);

        // Four handles whose operation has not finished. Ordering still depends on them, so a sweep
        // must not drop them.
        List<TaskCompletionSource> live = new();
        for (long id = 0; id < 4; id++)
        {
            TaskCompletionSource op = new();
            live.Add(op);
            tracker.RecordTail(Handle(id), op.Task);
        }

        // Enough finished handles to force many sweeps.
        for (long id = 100; id < 5_000; id++)
            tracker.RecordTail(Handle(id), Task.CompletedTask);

        for (long id = 0; id < 4; id++)
            Assert.That(tracker.PredecessorFor(Handle(id)), Is.SameAs(live[(int)id].Task),
                "a sweep must not drop the tail of a handle whose operation is still running");

        foreach (TaskCompletionSource op in live)
            op.SetResult();
    }

    [Test]
    public void FinishedTailsAreCollectable()
    {
        // The defect this guards: a finished operation's task stayed reachable for the life of the
        // stream, and each one held its async state machine. After the sweep nothing points at it.
        BatchStreamTracker tracker = new(maxBuffered: 8);

        WeakReference finished = RecordFinishedTail(tracker, 1);

        for (long id = 100; id < 200; id++)
            tracker.RecordTail(Handle(id), Task.CompletedTask);

        for (int attempt = 0; attempt < 3 && finished.IsAlive; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        Assert.That(finished.IsAlive, Is.False, "a swept chain entry must not keep its task alive");
    }

    /// <summary>
    /// Records one finished tail and returns a weak reference to its task. It is a separate method
    /// so no local of the calling test keeps the task alive.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RecordFinishedTail(BatchStreamTracker tracker, long id)
    {
        Task op = Task.FromResult(new object());
        tracker.RecordTail(Handle(id), op);
        return new WeakReference(op);
    }

    [Test]
    public async Task DrainWaitsForTheOperationsThatAreStillRunning()
    {
        BatchStreamTracker tracker = new(maxBuffered: 8);
        TaskCompletionSource first = new();
        TaskCompletionSource second = new();

        tracker.Enter();
        tracker.Enter();
        Assert.That(tracker.OutstandingCount, Is.EqualTo(3), "two operations plus the read loop");

        Task drain = tracker.DrainAsync();

        first.SetResult();
        tracker.Complete(first.Task);
        await Task.Delay(50).ConfigureAwait(false);
        Assert.That(drain.IsCompleted, Is.False, "one operation is still running");

        second.SetResult();
        tracker.Complete(second.Task);
        await drain.ConfigureAwait(false);

        Assert.That(tracker.OutstandingCount, Is.EqualTo(0));
    }

    [Test]
    public async Task DrainCompletesWhenNoOperationRan()
    {
        BatchStreamTracker tracker = new(maxBuffered: 8);

        await tracker.DrainAsync().ConfigureAwait(false);

        Assert.That(tracker.OutstandingCount, Is.EqualTo(0));
    }

    [Test]
    public async Task DrainIsSafeToCallMoreThanOnce()
    {
        // The handler drains on the normal path, on the cancel path, and again in its finally block.
        BatchStreamTracker tracker = new(maxBuffered: 8);

        tracker.Enter();
        tracker.Complete(Task.CompletedTask);

        await tracker.DrainAsync().ConfigureAwait(false);
        await tracker.DrainAsync().ConfigureAwait(false);
        await tracker.DrainAsync().ConfigureAwait(false);

        Assert.That(tracker.OutstandingCount, Is.EqualTo(0));
    }

    [Test]
    public void DrainRaisesTheFirstOperationFailure()
    {
        // Operations report their own outcome in-band and are not expected to fault. If one does,
        // the failure must still reach the caller, as an await of every operation gave before.
        BatchStreamTracker tracker = new(maxBuffered: 8);

        tracker.Enter();
        tracker.Enter();
        tracker.Complete(Task.FromException(new InvalidOperationException("first")));
        tracker.Complete(Task.FromException(new InvalidOperationException("second")));

        InvalidOperationException? failure =
            Assert.ThrowsAsync<InvalidOperationException>(async () => await tracker.DrainAsync());

        Assert.That(failure!.Message, Is.EqualTo("first"));
    }
}
