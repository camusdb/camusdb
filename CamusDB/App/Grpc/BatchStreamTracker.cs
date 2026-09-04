/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.ExceptionServices;

namespace CamusDB.App.Grpc;

/// <summary>
/// Per-stream bookkeeping for the duplex batch handler. It does two jobs: it orders the operations
/// that share one transaction handle, and it lets stream teardown wait for the operations that are
/// still running.
///
/// <para><b>Why it does not keep a list of operations.</b> The obvious shape — collect every
/// operation's <see cref="Task"/> in a list and await <c>Task.WhenAll</c> at teardown — retains one
/// async state machine for each operation until the stream ends. A client pins all of a
/// transaction's operations to one stream and holds that stream open for the whole session, so the
/// list grows with the total operation count and never shrinks. On a loaded node this reached 3.6
/// million retained state machines (673 MB) in 30 minutes and was the largest single part of the
/// heap growth. Teardown does not need a history. It needs the answer to "is any operation still
/// running", which a counter and one completion signal give in constant memory.</para>
///
/// <para><b>Why the chain map stays bounded.</b> The map holds only the most recent operation for
/// each transaction handle. An entry whose operation is already complete orders nothing, so a sweep
/// drops it. The read loop admits at most <c>maxBuffered</c> operations that are started and not
/// complete, so at most that many entries are useful at one time. The sweep runs when the map holds
/// twice that number. Each sweep therefore frees at least <c>maxBuffered</c> entries, which makes
/// the cost constant for each operation and holds the map below twice the buffer limit.</para>
///
/// <para><b>Threading.</b> <see cref="PredecessorFor"/> and <see cref="RecordTail"/> touch the chain
/// map. Call them only from the stream's read loop, which runs one request at a time.
/// <see cref="Enter"/>, <see cref="Complete"/> and <see cref="DrainAsync"/> are safe on any
/// thread.</para>
/// </summary>
internal sealed class BatchStreamTracker
{
    /// <summary>The most recent operation for each transaction handle, or nothing when the handle
    /// has no operation in flight. Confined to the read loop.</summary>
    private readonly Dictionary<(long Pt, uint Counter), Task> chains = new();

    /// <summary>Scratch space for one sweep. Reused so a sweep allocates nothing. Confined to the
    /// read loop.</summary>
    private readonly List<(long Pt, uint Counter)> sweepBuffer = new();

    /// <summary>Map size that starts a sweep. See the class summary for the bound it gives.</summary>
    private readonly int chainSweepThreshold;

    private readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Operations that started and did not complete, plus one reference for the read loop itself.
    /// The read loop's reference keeps the count above zero between two operations, so an empty
    /// moment in the middle of the stream does not signal teardown. <see cref="DrainAsync"/> gives
    /// that reference back.
    /// </summary>
    private int outstanding = 1;

    /// <summary>Guards the read loop's reference so more than one drain call is safe.</summary>
    private int readLoopLeft;

    /// <summary>
    /// The first failure an operation reported, kept so <see cref="DrainAsync"/> can raise it the
    /// way <c>Task.WhenAll</c> did. Operations report their own outcome in-band and are not expected
    /// to fault, so this is a safety net.
    /// </summary>
    private Exception? firstFailure;

    /// <param name="maxBuffered">Highest number of operations the read loop admits before it pauses.</param>
    public BatchStreamTracker(int maxBuffered)
    {
        chainSweepThreshold = Math.Max(8, maxBuffered) * 2;
    }

    /// <summary>Number of chain entries. Test observability only.</summary>
    internal int ChainCount => chains.Count;

    /// <summary>Number of operations that started and did not complete. Test observability only.</summary>
    internal int OutstandingCount => Volatile.Read(ref outstanding);

    /// <summary>
    /// Gives the operation that a new operation for this handle must wait for. The result is
    /// <see cref="Task.CompletedTask"/> when the handle has no earlier operation, or when the
    /// earlier operation already completed and a sweep dropped it. Read-loop only.
    /// </summary>
    public Task PredecessorFor((long Pt, uint Counter) handle)
        => chains.TryGetValue(handle, out Task? previous) ? previous : Task.CompletedTask;

    /// <summary>
    /// Makes this operation the one that the next operation for the same handle waits for.
    /// Read-loop only.
    /// </summary>
    public void RecordTail((long Pt, uint Counter) handle, Task op)
    {
        chains[handle] = op;

        if (chains.Count >= chainSweepThreshold)
            SweepCompletedChains();
    }

    /// <summary>Reserves one outstanding reference. Call it before you create the operation, because
    /// an operation can complete before the caller sees its <see cref="Task"/>.</summary>
    public void Enter() => Interlocked.Increment(ref outstanding);

    /// <summary>
    /// Reports that one operation completed. Call it exactly once for each <see cref="Enter"/>.
    /// </summary>
    public void Complete(Task op)
    {
        if (op.IsFaulted)
        {
            // Read Exception to mark the fault observed. A list of operations got this from
            // Task.WhenAll; without it an unobserved fault reaches
            // TaskScheduler.UnobservedTaskException when the finalizer runs.
            AggregateException aggregate = op.Exception!;
            Interlocked.CompareExchange(ref firstFailure, aggregate.InnerException ?? aggregate, null);
        }

        Release();
    }

    /// <summary>
    /// Gives back the read loop's own reference, then waits for the operations that are still
    /// running. It raises the first operation failure, as an await of every operation did. More
    /// than one call is safe: the later calls wait for the same signal.
    /// </summary>
    public async Task DrainAsync()
    {
        if (Interlocked.Exchange(ref readLoopLeft, 1) == 0)
            Release();

        await drained.Task.ConfigureAwait(false);

        Exception? failure = Volatile.Read(ref firstFailure);
        if (failure is not null)
            ExceptionDispatchInfo.Capture(failure).Throw();
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref outstanding) == 0)
            drained.TrySetResult();
    }

    /// <summary>
    /// Drops the entries whose operation already completed. Such an entry orders nothing: a new
    /// operation for that handle would await a task that is already done. Read-loop only.
    /// </summary>
    private void SweepCompletedChains()
    {
        sweepBuffer.Clear();

        foreach (KeyValuePair<(long Pt, uint Counter), Task> entry in chains)
        {
            if (entry.Value.IsCompleted)
                sweepBuffer.Add(entry.Key);
        }

        for (int i = 0; i < sweepBuffer.Count; i++)
            chains.Remove(sweepBuffer[i]);

        sweepBuffer.Clear();
    }
}
