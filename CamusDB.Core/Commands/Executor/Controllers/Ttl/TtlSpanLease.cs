
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Kahuna;
using Kahuna.Shared.Locks;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Exclusive ownership of one TTL span, built on Kahuna's distributed locks.
///
/// <para><b>Why locks rather than a hand-rolled KV lease.</b> Kahuna already implements exactly this:
/// <c>LocateAndTryLock</c> takes a lease with a native expiry, <c>TryExtendLock</c> and
/// <c>TryUnlock</c> enforce owner equality <em>server-side</em> and answer <c>InvalidOwner</c> when the
/// caller no longer holds the lock, and every acquisition returns a monotonically increasing
/// <b>fencing token</b>. Reimplementing that over raw key-value compare-and-set reproduces the same
/// guarantees more weakly — most importantly, a self-minted GUID can only answer "is this still me?",
/// whereas a monotonic token can be <em>ordered</em>, which is what lets a downstream write reject a
/// stale writer outright instead of merely failing to recognise it.</para>
///
/// <para><b>The owner value is unique per acquisition, deliberately.</b> Kahuna treats a re-lock by the
/// same owner as reentrant and hands back the <em>existing</em> token. Reusing one process-wide owner
/// would therefore make two successive claims indistinguishable, and a renewer left over from the first
/// would happily extend the second — the precise confusion the fencing token exists to prevent.</para>
///
/// <para><b>Renewal is still ours to drive.</b> Kahuna does not auto-extend; a holder that wants to
/// outlive one lease period must say so. The loop here does nothing but call
/// <c>TryExtendLock</c> on a timer and stop the moment Kahuna reports the lock is no longer this
/// owner's — all of the conditional logic lives on the server, where it belongs.</para>
///
/// <para><b>The lease is not a checkpoint.</b> Progress belongs in a separate, non-expiring record.
/// Anything tied to the lock's lifetime dies when the lock does, which is the right lifetime for "who
/// owns this" and exactly the wrong one for "how far did they get".</para>
/// </summary>
internal sealed class TtlSpanLease : IAsyncDisposable
{
    private const int MaxRetries = 10;

    /// <summary>
    /// One live claim: the owner bytes Kahuna compares against, the fencing token it granted, and the
    /// renewer keeping it alive. Held by identity so a renewer that finishes after the resource has been
    /// re-acquired cannot tear down its successor's registration.
    /// </summary>
    private sealed class Claim(byte[] owner, long fencingToken, CancellationTokenSource cts)
    {
        public readonly byte[] Owner = owner;
        public readonly long FencingToken = fencingToken;
        public readonly CancellationTokenSource Cts = cts;
    }

    private readonly IKahuna kahuna;
    private readonly string ownerPrefix;
    private readonly int leaseMs;
    private readonly int renewIntervalMs;

    private readonly ConcurrentDictionary<string, Claim> held = new(StringComparer.Ordinal);

    public TtlSpanLease(IKahuna kahuna, string ownerPrefix, int leaseMs, int renewIntervalMs)
    {
        this.kahuna = kahuna;
        this.ownerPrefix = ownerPrefix;
        this.leaseMs = leaseMs;
        this.renewIntervalMs = renewIntervalMs;
    }

    /// <summary>
    /// Attempts to take the lock on <paramref name="resource"/>, returning Kahuna's fencing token, or
    /// null when another worker holds it or the attempt could not be resolved — treated as "not
    /// acquired" so the caller moves on rather than proceeding unfenced.
    ///
    /// <para>The token must be carried into every write this claim authorizes. Because it increases with
    /// each grant, a later holder's token is always greater, which lets those writes reject a stale
    /// worker by comparison rather than by guesswork.</para>
    /// </summary>
    public async Task<long?> TryAcquireAsync(string resource, CancellationToken cancellationToken = default)
    {
        byte[] owner = System.Text.Encoding.UTF8.GetBytes($"{ownerPrefix}:{Guid.NewGuid():N}");

        int retries = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            (LockResponseType type, long fencingToken) = await kahuna.LocateAndTryLock(
                resource, owner, leaseMs, LockDurability.Persistent, cancellationToken).ConfigureAwait(false);

            if (type == LockResponseType.Locked)
            {
                StartRenewer(resource, owner, fencingToken);
                return fencingToken;
            }

            // Busy = another worker's live lease genuinely holds it. A dead holder's lease would have
            // expired and this grant would have succeeded, so Busy is real contention, not timing.
            if (type == LockResponseType.Busy)
                return null;

            if (type is LockResponseType.MustRetry or LockResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 10, cancellationToken).ConfigureAwait(false);
                continue;
            }

            return null;
        }

        return null;
    }

    /// <summary>
    /// Releases the claim identified by <paramref name="fencingToken"/>, stopping its renewer and
    /// unlocking so the next worker can start immediately rather than waiting out the lease.
    ///
    /// <para>Safe to call late or twice. Kahuna's unlock compares the owner and answers
    /// <c>InvalidOwner</c> for anyone who no longer holds the lock, so a worker that stalled past its
    /// lease cannot release a claim its successor is working under.</para>
    /// </summary>
    public async Task ReleaseAsync(string resource, long fencingToken)
    {
        if (!held.TryGetValue(resource, out Claim? claim) || claim.FencingToken != fencingToken)
            return; // never ours, or already superseded

        if (!held.TryRemove(new KeyValuePair<string, Claim>(resource, claim)))
            return; // a concurrent release or teardown won

        try { await claim.Cts.CancelAsync().ConfigureAwait(false); } catch { }
        claim.Cts.Dispose();

        try
        {
            await kahuna.LocateAndTryUnlock(
                resource, claim.Owner, LockDurability.Persistent, CancellationToken.None).ConfigureAwait(false);
        }
        catch { /* the lease expires on its own */ }
    }

    /// <summary>
    /// Whether this worker's renewer still believes it holds <paramref name="resource"/> under
    /// <paramref name="fencingToken"/>. A local view: it says the renewer has not yet observed a loss.
    /// Use it to abandon long work early, not as proof of ownership at the instant of a write — for that,
    /// compare fencing tokens on the write itself.
    /// </summary>
    public bool StillHeldLocally(string resource, long fencingToken) =>
        held.TryGetValue(resource, out Claim? claim) && claim.FencingToken == fencingToken;

    private void StartRenewer(string resource, byte[] owner, long fencingToken)
    {
        CancellationTokenSource cts = new();
        Claim claim = new(owner, fencingToken, cts);

        // Tear down any lingering registration for this resource. It should not exist — we only just
        // won the lock — but a stale renewer left running would keep extending under an owner we no
        // longer track.
        if (held.TryRemove(resource, out Claim? stale))
        {
            try { stale.Cts.Cancel(); } catch { }
            stale.Cts.Dispose();
        }

        held[resource] = claim;
        _ = RenewLoopAsync(resource, claim, cts.Token);
    }

    private async Task RenewLoopAsync(string resource, Claim claim, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(renewIntervalMs, ct).ConfigureAwait(false);

                if (!await TryExtendOnceAsync(resource, claim, ct).ConfigureAwait(false))
                    break;
            }
        }
        catch (OperationCanceledException) { /* released — normal */ }
        catch { /* best-effort; a missed renewal only shortens the lease */ }
        finally
        {
            // Remove only THIS claim. Removing by resource alone would evict a newer acquisition
            // installed after this loop was cancelled, leaving a live owner without its own bookkeeping.
            if (held.TryRemove(new KeyValuePair<string, Claim>(resource, claim)))
                claim.Cts.Dispose();
        }
    }

    /// <summary>
    /// One extension attempt. Returns false once the lock is definitively no longer ours.
    ///
    /// <para>Transient statuses are retried <em>within</em> the attempt rather than by waiting another
    /// full interval: renewing early exists to leave slack before the lease deadline, and spending that
    /// slack asleep is how a live holder loses a lock it could have kept.</para>
    /// </summary>
    private async Task<bool> TryExtendOnceAsync(string resource, Claim claim, CancellationToken ct)
    {
        int retries = 0;

        while (!ct.IsCancellationRequested)
        {
            (LockResponseType type, _) = await kahuna.LocateAndTryExtendLock(
                resource, claim.Owner, leaseMs, LockDurability.Persistent, ct).ConfigureAwait(false);

            if (type == LockResponseType.Extended)
                return true;

            // InvalidOwner: someone else holds it now. LockDoesNotExist: it lapsed and was cleaned up.
            // Either way this claim is over, and extending further would trample a new owner.
            if (type is LockResponseType.InvalidOwner or LockResponseType.LockDoesNotExist)
                return false;

            if (type is LockResponseType.MustRetry or LockResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 5, ct).ConfigureAwait(false);
                continue;
            }

            // Cannot confirm ownership: stop renewing and let the lease lapse rather than keep acting as
            // owner on an unverified claim.
            return false;
        }

        return false;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<string, Claim> entry in held)
        {
            if (held.TryRemove(new KeyValuePair<string, Claim>(entry.Key, entry.Value)))
            {
                try { await entry.Value.Cts.CancelAsync().ConfigureAwait(false); } catch { }
                entry.Value.Cts.Dispose();
            }
        }
    }
}
