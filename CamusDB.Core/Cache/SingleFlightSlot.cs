
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// Coordinates a single-flight execution for one cache fingerprint. One caller is designated
/// the <em>owner</em> and executes the full query plan; concurrent callers for the same
/// fingerprint become <em>waiters</em> and block until the owner either publishes an entry or
/// signals failure. If the wait exceeds the configured deadline, the waiter exits and executes
/// independently.
///
/// <para>Instances are created by <see cref="IQueryResultCache.EnterSingleFlight"/>. The owner
/// slot and each waiter slot are distinct objects, but waiters are all backed by the same
/// <c>Task</c> (derived from the owner's <c>TaskCompletionSource</c>) so signalling the TCS
/// wakes all waiters atomically.</para>
///
/// <para><b>The slot carries a completion signal, never rows.</b> It reports only whether an
/// entry was published; a woken waiter must re-probe the cache through the ordinary read path.
/// Handing the owner's result object to waiters would bypass that path: the entry can be evicted
/// by a write that commits between the owner materializing it and signalling, and a waiter that
/// yielded those detached rows would be serving state that predates a committed write — plus it
/// would skip the schema-dependency check and strict validation every other reader performs.</para>
///
/// <para><b>Owner contract:</b> the caller that receives a slot with <see cref="IsOwner"/>
/// <c>= true</c> must call <see cref="IQueryResultCache.ExitSingleFlight"/> exactly once —
/// passing <c>true</c> when an entry was stored, or <c>false</c> on failure (cancellation,
/// generation fence rejection, byte cap exceeded). Failing to call
/// <see cref="IQueryResultCache.ExitSingleFlight"/> leaves waiters blocked until their timeout.
/// </para>
///
/// <para><b>Waiter contract:</b> callers with <see cref="IsOwner"/> <c>= false</c> call
/// <see cref="WaitAsync"/> to block. On a <c>true</c> signal they re-probe the cache and serve the
/// entry only if that probe yields a valid hit — the entry may already be gone. On <c>false</c>,
/// timeout, or cancellation they execute the plan independently.</para>
/// </summary>
public sealed class SingleFlightSlot
{
    private readonly Task<bool> _task;

    /// <summary>
    /// <c>true</c> for the first caller that registered for the fingerprint —
    /// that caller must execute the plan and call
    /// <see cref="IQueryResultCache.ExitSingleFlight"/> when done.
    /// <c>false</c> for every subsequent concurrent caller — they should call
    /// <see cref="WaitAsync"/> and re-probe the cache if a published signal arrives before the
    /// timeout.
    /// </summary>
    public bool IsOwner { get; }

    /// <param name="isOwner">Whether this slot represents the owning caller.</param>
    /// <param name="task">
    /// The <see cref="Task"/> to await in <see cref="WaitAsync"/>, derived from the
    /// owner's <see cref="System.Threading.Tasks.TaskCompletionSource{TResult}"/>.
    /// For the null cache, pass a completed task — the owner never awaits its own slot.
    /// </param>
    internal SingleFlightSlot(bool isOwner, Task<bool> task)
    {
        IsOwner = isOwner;
        _task = task;
    }

    /// <summary>
    /// Blocks until the owner signals completion (published or failed), or until
    /// <paramref name="timeoutMs"/> elapses, or until <paramref name="ct"/> is cancelled.
    ///
    /// <para>Returns <c>true</c> only when the owner reported that it published an entry — the
    /// waiter should then re-probe the cache, which may still miss if a write evicted the entry in
    /// the meantime. Returns <c>false</c> on owner failure, timeout, or <paramref name="ct"/>
    /// cancellation, in which case the waiter executes the plan independently.</para>
    ///
    /// <para>Only meaningful for waiter slots (<see cref="IsOwner"/> <c>= false</c>).
    /// Calling it on an owner slot always returns <c>false</c> immediately because
    /// the owner's task was completed with <c>false</c> at construction time in the null-cache path,
    /// and the owner should never await its own slot in the real cache path.</para>
    /// </summary>
    public async Task<bool> WaitAsync(int timeoutMs, CancellationToken ct)
    {
        if (timeoutMs <= 0)
            return false;

        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(timeoutMs);
        try
        {
            return await _task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
