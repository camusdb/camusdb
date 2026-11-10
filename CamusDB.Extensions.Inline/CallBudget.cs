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
/// The budget of the previous call on this thread, which <see cref="CallBudget.Enter"/> returns and
/// <see cref="CallBudget.Exit"/> puts back. Opaque to the caller.
/// </summary>
public struct CallBudgetFrame
{
    internal bool Active;

    internal long Deadline;

    internal long AllocationBase;

    internal long AllocationLimit;

    internal int ArrayLimit;

    internal CancellationToken Token;
}

/// <summary>
/// The per-call budgets of an inline SQL function. The server compiles the body of the function with
/// a call to <see cref="Tick"/> at the start of every loop body, lambda and local function, and with
/// <see cref="NewArray{T}(int)"/> in place of every sized array creation. The server's adapter calls

/// <see cref="Enter"/> before the body and <see cref="Exit"/> after it.
///
/// <para><b>What a tick checks.</b> The statement's cancellation token, the wall-clock deadline, the
/// managed bytes this thread allocated since the call started, and the free stack. A failed check
/// throws <see cref="CallBudgetExceededException"/>, or
/// <see cref="InsufficientExecutionStackException"/> for the stack, before the process is at risk: a
/// stack overflow cannot be caught and ends the process, and this check throws while there is still
/// room to unwind. Every tick reads the clock. Reading it on some ticks only would let the call run
/// on for several framework calls after the deadline, and one of them can be a regular expression
/// match that takes its whole timeout.</para>
///
/// <para><b>Why the state is per thread.</b> The body of an inline function runs on one thread from
/// start to end: it cannot await, start a thread or call SQL. A thread-static slot is therefore
/// exact, and it costs no parameter on any user method. The allocation counter is per thread for the
/// same reason. <see cref="Enter"/> returns the slot it replaced, so a call that starts while another
/// is on the stack of the same thread (an adapter entered again) restores it.</para>
///
/// <para><b>What a tick cannot see.</b> A single framework call between two ticks. The server
/// therefore lets the body call only members whose cost is bounded by the size of their inputs, and
/// the deployment sets a heap hard limit as the last line: under it a runaway call fails with a
/// catchable <see cref="OutOfMemoryException"/>.</para>
///
/// <para><b>Fail closed.</b> A tick with no active call throws. Compiled bodies run only under the
/// adapter, so that case is a server error, and running without limits is the wrong answer to
/// it.</para>
/// </summary>
public static class CallBudget
{
    [ThreadStatic]
    private static CallBudgetFrame current;

    /// <summary>
    /// Starts the budgets of one call on this thread and returns the frame to give back to
    /// <see cref="Exit"/>.
    /// </summary>
    /// <param name="timeoutMilliseconds">The wall-clock limit of the call. Zero or less means no limit.</param>
    /// <param name="allocationLimitBytes">The managed bytes the call may allocate. Zero or less means no limit.</param>
    /// <param name="arrayLimit">The largest array, in elements, the call may create. Zero or less means no limit.</param>
    /// <param name="token">The cancellation token of the statement.</param>
    public static CallBudgetFrame Enter(int timeoutMilliseconds, long allocationLimitBytes, int arrayLimit, CancellationToken token)
    {
        CallBudgetFrame previous = current;

        current = new CallBudgetFrame
        {
            Active = true,
            Deadline = timeoutMilliseconds > 0 ? Environment.TickCount64 + timeoutMilliseconds : long.MaxValue,
            AllocationBase = GC.GetAllocatedBytesForCurrentThread(),
            AllocationLimit = allocationLimitBytes > 0 ? allocationLimitBytes : long.MaxValue,
            ArrayLimit = arrayLimit > 0 ? arrayLimit : int.MaxValue,
            Token = token,
        };

        return previous;
    }

    /// <summary>Ends the budgets of the current call and restores the frame <see cref="Enter"/> returned.</summary>
    public static void Exit(CallBudgetFrame previous) => current = previous;

    /// <summary>
    /// Checks the budgets of the current call. The compiled body calls it at the start of every loop
    /// body, lambda and local function.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Tick()
    {
        ref CallBudgetFrame frame = ref current;

        if (!frame.Active)
            ThrowNoCall();

        if (frame.Token.IsCancellationRequested)
            ThrowCancelled(frame.Token);

        if (Environment.TickCount64 > frame.Deadline)
            ThrowTime();

        if (GC.GetAllocatedBytesForCurrentThread() - frame.AllocationBase > frame.AllocationLimit)
            ThrowAllocation(frame.AllocationLimit);

        RuntimeHelpers.EnsureSufficientExecutionStack();
    }

    /// <summary>The same as <see cref="NewArray{T}(int)"/> for a size that is a <c>long</c> or a <c>uint</c>.</summary>
    public static T[] NewArray<T>(long length)
    {
        // "new T[n]" takes an int, a uint, a long or a ulong for n; so does this method, through its
        // overloads. The limit is an int, so a size that is not one is over it.
        if (length > int.MaxValue)
            return NewArray<T>(int.MaxValue);

        return NewArray<T>(length < int.MinValue ? -1 : (int)length);
    }

    /// <summary>The same as <see cref="NewArray{T}(int)"/> for a size that is a <c>ulong</c>.</summary>
    public static T[] NewArray<T>(ulong length) => NewArray<T>(length > int.MaxValue ? int.MaxValue : (int)length);

    /// <summary>
    /// Creates an array of <paramref name="length"/> elements, as <c>new T[length]</c> does, after it
    /// checked the array limit and the allocation budget. The compiled body calls it in place of every
    /// array creation that names a size.
    /// </summary>
    public static T[] NewArray<T>(int length)
    {
        ref CallBudgetFrame frame = ref current;

        if (!frame.Active)
            ThrowNoCall();

        // The same error a negative size gives without the check.
        if (length < 0)
            throw new OverflowException("Arithmetic operation resulted in an overflow.");

        if (length > frame.ArrayLimit)
            throw new CallBudgetExceededException(
                CallBudgetKind.ArrayLength,
                $"an array of {length} elements is larger than the limit of {frame.ArrayLimit} elements");

        long used = GC.GetAllocatedBytesForCurrentThread() - frame.AllocationBase;

        if (used + (long)length * Unsafe.SizeOf<T>() > frame.AllocationLimit)
            ThrowAllocation(frame.AllocationLimit);

        return new T[length];
    }

    /// <summary>
    /// The match timeout a regular expression of the body may use: the smaller of the timeout the body
    /// asked for and the time left before the deadline of the call. A timeout that is not positive is
    /// returned as it is, so the regular expression refuses it with its own error. The compiled body
    /// passes every match timeout through this method, so one match cannot outlive the call.
    /// </summary>
    public static TimeSpan RegexTimeout(TimeSpan requested)
    {
        ref CallBudgetFrame frame = ref current;

        if (!frame.Active)
            ThrowNoCall();

        if (requested <= TimeSpan.Zero || frame.Deadline == long.MaxValue)
            return requested;

        long left = frame.Deadline - Environment.TickCount64;

        if (left <= 0)
            ThrowTime();

        TimeSpan limit = TimeSpan.FromMilliseconds(left);
        return requested < limit ? requested : limit;
    }

    /// <summary>
    /// True for an exception that ends the call because a budget is exhausted, or because the statement
    /// was cancelled. The server adds <c>when (!CallBudget.IsBudgetException(e))</c> to every
    /// <c>catch</c> clause of the body, so the body cannot catch one and run on.
    /// </summary>
    public static bool IsBudgetException(Exception? exception) =>
        exception is CallBudgetExceededException
            or InsufficientExecutionStackException
            or OutOfMemoryException
            or OperationCanceledException;

    private static void ThrowCancelled(CancellationToken token) => throw new OperationCanceledException(token);

    private static void ThrowTime() =>
        throw new CallBudgetExceededException(CallBudgetKind.Time, "the call ran longer than its time limit");

    private static void ThrowAllocation(long limit) =>
        throw new CallBudgetExceededException(
            CallBudgetKind.Allocation, $"the call allocated more than its memory limit of {limit} bytes");

    private static void ThrowNoCall() =>
        throw new InvalidOperationException("An inline function body ran outside a call of the server.");
}
