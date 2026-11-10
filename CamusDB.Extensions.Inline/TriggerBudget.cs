/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace CamusDB.Extensions.Inline;

/// <summary>
/// The per-fire budgets of an inline trigger body: the same checks as <see cref="CallBudget"/>, kept
/// in an object that the server's rewrite passes to every check instead of a thread-static slot.
///
/// <para><b>Why an object and not the thread-static slot.</b> A trigger body can <c>await</c> the
/// SQL it runs, and the thread that continues after the await is not always the thread that
/// started. A thread-static slot would be the budget of another call or none at all. The rewrite
/// gives the body a field that holds this object and makes every tick, array creation and regex
/// timeout a call on it; lambdas and local functions in the body reach it through the object they
/// belong to.</para>
///
/// <para><b>Allocation across an await.</b> The runtime counts allocated bytes per thread, so the
/// count is correct only for one thread between two points. The session that runs the body's SQL is
/// server code: it calls <see cref="Suspend"/> before the statement and <see cref="Resume"/> just
/// before it returns to the body, which banks the bytes used so far and takes a new baseline on the
/// current thread. A tick that runs on another thread than the one of the last baseline takes a new
/// baseline too, so a continuation that changes threads outside a session call loses at most the
/// bytes between the change and the next tick, never the bytes before it.</para>
///
/// <para><b>Time across an await.</b> The time budget is for the body's own work. The time a
/// statement takes inside the session is added to the deadline on <see cref="Resume"/>, so a slow
/// statement is bounded by its own limits and not by the trigger's, and a body that runs SQL in a
/// loop meets the transaction's mutation limit, not this one.</para>
/// </summary>
public sealed class TriggerBudget
{
    private readonly long allocationLimit;

    private readonly int arrayLimit;

    private long deadline;

    private long allocationBase;

    /// <summary>Bytes banked by earlier segments of the body, between suspensions and thread changes.</summary>
    private long allocationBanked;

    private int threadId;

    private long suspendedAt;

    /// <param name="timeoutMilliseconds">The time the body's own work may take; 0 or less for no limit.</param>
    /// <param name="allocationLimitBytes">The managed bytes the body may allocate; 0 or less for no limit.</param>
    /// <param name="arrayLimit">The most elements of an array the body creates; 0 or less for no limit.</param>
    public TriggerBudget(int timeoutMilliseconds, long allocationLimitBytes, int arrayLimit)
    {
        deadline = timeoutMilliseconds > 0 ? Environment.TickCount64 + timeoutMilliseconds : long.MaxValue;
        allocationLimit = allocationLimitBytes > 0 ? allocationLimitBytes : long.MaxValue;
        this.arrayLimit = arrayLimit > 0 ? arrayLimit : int.MaxValue;
        allocationBase = GC.GetAllocatedBytesForCurrentThread();
        threadId = Environment.CurrentManagedThreadId;
    }

    /// <summary>The bytes the body allocated so far, on this thread and banked from earlier segments.</summary>
    private long Allocated
    {
        get
        {
            if (threadId != Environment.CurrentManagedThreadId)
            {
                // Another thread continues the body: its counter has no relation to the baseline.
                allocationBase = GC.GetAllocatedBytesForCurrentThread();
                threadId = Environment.CurrentManagedThreadId;
            }

            return allocationBanked + GC.GetAllocatedBytesForCurrentThread() - allocationBase;
        }
    }

    /// <summary>The check at the start of every loop pass, lambda and local function of the body.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Tick()
    {
        if (Environment.TickCount64 > deadline)
            ThrowTime();

        if (Allocated > allocationLimit)
            ThrowAllocation(allocationLimit);

        RuntimeHelpers.EnsureSufficientExecutionStack();
    }

    /// <summary>Called by the server's session before it runs a statement for the body.</summary>
    public void Suspend()
    {
        allocationBanked = Allocated;
        suspendedAt = Environment.TickCount64;
    }

    /// <summary>Called by the server's session just before it returns to the body.</summary>
    public void Resume()
    {
        allocationBase = GC.GetAllocatedBytesForCurrentThread();
        threadId = Environment.CurrentManagedThreadId;

        if (deadline != long.MaxValue && suspendedAt != 0)
            deadline += Math.Max(0, Environment.TickCount64 - suspendedAt);

        suspendedAt = 0;
    }

    /// <summary>The same as <see cref="NewArray{T}(int)"/> for a size that is a <c>long</c> or a <c>uint</c>.</summary>
    public T[] NewArray<T>(long length)
    {
        if (length > int.MaxValue)
            return NewArray<T>(int.MaxValue);

        return NewArray<T>(length < int.MinValue ? -1 : (int)length);
    }

    /// <summary>The same as <see cref="NewArray{T}(int)"/> for a size that is a <c>ulong</c>.</summary>
    public T[] NewArray<T>(ulong length) => NewArray<T>(length > int.MaxValue ? int.MaxValue : (int)length);

    /// <summary>The replacement of <c>new T[length]</c> in a body: refuses a length over the array limit or the memory left.</summary>
    public T[] NewArray<T>(int length)
    {
        if (length < 0)
            throw new OverflowException("Arithmetic operation resulted in an overflow.");

        if (length > arrayLimit)
            throw new CallBudgetExceededException(
                CallBudgetKind.ArrayLength,
                $"an array of {length} elements is larger than the limit of {arrayLimit} elements");

        if (Allocated + (long)length * Unsafe.SizeOf<T>() > allocationLimit)
            ThrowAllocation(allocationLimit);

        return new T[length];
    }

    /// <summary>The match timeout a body asked for, cut to the time the body has left.</summary>
    public TimeSpan RegexTimeout(TimeSpan requested)
    {
        if (requested <= TimeSpan.Zero || deadline == long.MaxValue)
            return requested;

        long left = deadline - Environment.TickCount64;

        if (left <= 0)
            ThrowTime();

        TimeSpan limit = TimeSpan.FromMilliseconds(left);
        return requested < limit ? requested : limit;
    }

    private static void ThrowTime() =>
        throw new CallBudgetExceededException(CallBudgetKind.Time, "the trigger ran longer than its time limit");

    private static void ThrowAllocation(long limit) =>
        throw new CallBudgetExceededException(
            CallBudgetKind.Allocation, $"the trigger allocated more than its memory limit of {limit} bytes");
}
