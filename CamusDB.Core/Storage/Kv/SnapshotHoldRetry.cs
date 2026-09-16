/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Util.Diagnostics;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The retry rule every acquire of a Kahuna MVCC snapshot-floor hold goes through, and the status
/// test the renew paths share with it.
///
/// <para>A snapshot hold is committed on Kahuna's meta partition, so acquiring one needs that
/// partition's confirmed leader. When the node has not finished joining, when no leader is resolved
/// at that instant, when the forwarded call is not answered, or when a revision-prune window
/// overlapped the acquire, Kahuna answers <c>MustRetry</c>. Every one of those is a routine and
/// short-lived state, and none of them says the hold was refused: the answer carries no decision at
/// all. Decoding it as a refusal turns a leader election into a failed <c>CREATE DATABASE … BRANCH
/// FROM</c> or a refused <c>AS OF SYSTEM TIME</c> read, which is what this type exists to
/// prevent.</para>
///
/// <para>The budget is wall-clock time rather than an attempt count, for the reason
/// <see cref="CamusDBOptions.SnapshotHoldRetryBudgetMs"/> records: what is being waited out is an
/// election, which takes seconds and takes no fewer of them on a loaded node where each attempt is
/// slower. Only a transient status is retried — <c>Errored</c> and <c>InvalidInput</c> report a
/// malformed call and answer the same way forever, so they surface at once instead of spending the
/// budget.</para>
/// </summary>
internal static class SnapshotHoldRetry
{
    /// <summary>Capped exponential back-off between attempts: 25, 50, 100, 200, 400, 500, 500…</summary>
    private const int BaseDelayMs = 25;

    private const int MaxDelayMs = 500;

    /// <summary>Remaining acquires the test seam answers as transient. Always 0 in production.</summary>
    private static int injectedTransientAcquires;

    /// <summary>
    /// Whether a snapshot-hold status carries no decision and so must be retried rather than
    /// decoded. <c>MustRetry</c> means the meta partition had no confirmed leader, or the call was
    /// never answered; <c>WaitingForReplication</c> means the commit is still in flight. Neither
    /// says a hold was refused, removed or granted.
    /// </summary>
    internal static bool IsTransient(KeyValueResponseType type)
        => type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication;

    /// <summary>Back-off for attempt <paramref name="attempt"/> (0-based), capped at <see cref="MaxDelayMs"/>.</summary>
    internal static int DelayMs(int attempt)
        => (int)Math.Min((long)BaseDelayMs << Math.Min(attempt, 16), MaxDelayMs);

    /// <summary>
    /// Test-only seam: makes the next <paramref name="count"/> acquires answer <c>MustRetry</c>
    /// without reaching Kahuna, standing in for an election in flight on the meta partition. A
    /// healthy embedded node cannot be made to answer transiently on demand, and the behavior worth
    /// proving is not the acquire RPC but that the callers above ride the answer out. It is
    /// process-wide, so set it from a non-parallelizable test inside a <c>try</c>/<c>finally</c>.
    /// </summary>
    internal static void InjectTransientAcquiresForTesting(int count)
        => Volatile.Write(ref injectedTransientAcquires, count);

    private static bool TakeInjectedTransient()
    {
        while (true)
        {
            int remaining = Volatile.Read(ref injectedTransientAcquires);

            if (remaining <= 0)
                return false;

            if (Interlocked.CompareExchange(ref injectedTransientAcquires, remaining - 1, remaining) == remaining)
                return true;
        }
    }

    /// <summary>
    /// Acquires a snapshot-floor hold at <paramref name="snapshot"/>, riding out a transient answer
    /// for up to <paramref name="budgetMs"/> of wall-clock time. Returns Kahuna's last answer: the
    /// caller decides what a spent budget means, and must report it as retryable rather than as a
    /// refusal — see <see cref="IsTransient"/>.
    ///
    /// <para>A transport failure that reached no node is folded into the same transient answer by
    /// <see cref="KahunaRetryPolicy.InvokeOrTransient{T}"/>: nothing was sent, so nothing was
    /// changed, and acquire is idempotent by (holder, timestamp) in any case.</para>
    ///
    /// <para>The loop stops before a sleep that would overshoot the budget, so it never exceeds it
    /// by a whole back-off interval, and a budget of <c>0</c> or less makes exactly one attempt.</para>
    /// </summary>
    internal static async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireAsync(
        IKahuna kahuna,
        string holderId,
        HLCTimestamp snapshot,
        int leaseMs,
        int budgetMs,
        CancellationToken ct)
    {
        (KeyValueResponseType, string, HLCTimestamp) transient = (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

        ValueStopwatch elapsed = ValueStopwatch.StartNew();
        int attempt = 0;

        while (true)
        {
            (KeyValueResponseType type, string holdId, HLCTimestamp expiry) = TakeInjectedTransient()
                ? transient
                : await KahunaRetryPolicy.InvokeOrTransient(
                    () => kahuna.LocateAndAcquireSnapshotHold(holderId, snapshot, leaseMs, ct),
                    transient,
                    ct).ConfigureAwait(false);

            if (!IsTransient(type))
                return (type, holdId, expiry);

            int delayMs = DelayMs(attempt++);

            if (elapsed.GetElapsedMilliseconds() + delayMs > budgetMs)
                return (type, holdId, expiry);

            ServerDiagnostics.AddKvRetryWait("snapshot_hold_acquire");

            await Task.Delay(delayMs, ct).ConfigureAwait(false);
        }
    }
}
