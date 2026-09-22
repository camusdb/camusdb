/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Diagnostics;
using Kahuna.Shared.Sequences;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Runs a call against Kahuna's sequencer until it stops answering
/// <see cref="SequenceResponseType.MustRetry"/>, bounded by a wall-clock budget.
///
/// <para><c>MustRetry</c> is the sequencer's "this sequence's partition has no confirmed leader
/// right now" answer — a node still joining, an election in flight, or leadership moving while the
/// request was forwarded. It is also what an allocation gets for one block-lease period after the
/// sequence was updated, while the new incarnation is held quiet. None of those is a refusal and
/// none of them changed any state, so a caller that surfaced <c>MustRetry</c> would fail a user's
/// statement over a routine blip.</para>
///
/// <para><b>The budget is time, not attempts.</b> What is being waited on is an election, and an
/// election does not take fewer seconds because a saturated node makes each attempt slower. An
/// attempt cap would shrink the effective budget exactly when the wait was most worth making. The
/// loop stops before a sleep that would overshoot, so it never exceeds the budget by a whole
/// back-off interval, and a non-positive budget yields exactly one attempt.</para>
///
/// <para><b>One budget covers one logical operation, not one round trip.</b> The caller starts the
/// stopwatch and passes the same one to every call that makes up the operation. A per-call budget
/// would silently multiply a user's wait by the number of round trips the operation happens to
/// need.</para>
///
/// <para>This is the single implementation, shared by the id counters in
/// <c>DatabaseRegistry</c> and by <see cref="SequenceAllocator"/>. A second copy is how two retry
/// loops end up with subtly different budgets, and the difference is invisible until a
/// failover.</para>
/// </summary>
internal static class SequenceRetryPolicy
{
    // Capped exponential back-off: 25, 50, 100, 200, 400, 500, 500…
    private const int BaseDelayMs = 25;

    private const int MaxDelayMs = 500;

    internal static int DelayMs(int attempt) =>
        (int)Math.Min((long)BaseDelayMs << Math.Min(attempt, 16), MaxDelayMs);

    /// <summary>
    /// Runs <paramref name="sequenceCall"/> until its response type is not <c>MustRetry</c>, or the
    /// budget is spent. Returns the last response either way — an exhausted budget is reported as
    /// <c>MustRetry</c>, which the caller turns into a transient failure rather than a corruption.
    /// </summary>
    internal static Task<(SequenceResponseType, T)> RetryWhileMustRetryAsync<T>(
        Func<Task<(SequenceResponseType, T)>> sequenceCall,
        ValueStopwatch elapsed,
        int budgetMs)
        => RetryWhileAsync(sequenceCall, static r => r.Item1 == SequenceResponseType.MustRetry, elapsed, budgetMs);

    /// <summary>The same loop for a call whose result is a bare <see cref="SequenceResponseType"/>.</summary>
    internal static Task<SequenceResponseType> RetryWhileMustRetryAsync(
        Func<Task<SequenceResponseType>> sequenceCall,
        ValueStopwatch elapsed,
        int budgetMs)
        => RetryWhileAsync(sequenceCall, static r => r == SequenceResponseType.MustRetry, elapsed, budgetMs);

    /// <summary>
    /// The loop itself, over any call whose result can say "not right now". Shared by the sequencer
    /// callers and by the registry's generation stamp, which fails the same way — the partition has
    /// no confirmed leader at that instant — and so must wait the same way.
    /// </summary>
    internal static async Task<T> RetryWhileAsync<T>(
        Func<Task<T>> call,
        Func<T, bool> shouldRetry,
        ValueStopwatch elapsed,
        int budgetMs)
    {
        int attempt = 0;

        while (true)
        {
            T result = await call().ConfigureAwait(false);

            if (!shouldRetry(result))
                return result;

            int delayMs = DelayMs(attempt++);

            if (elapsed.GetElapsedMilliseconds() + delayMs > budgetMs)
                return result;

            await Task.Delay(delayMs).ConfigureAwait(false);
        }
    }
}
