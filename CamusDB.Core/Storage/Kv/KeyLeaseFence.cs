
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// A mutual-exclusion lease over an arbitrary KV key: exactly one holder at a time, and a holder that
/// dies stops blocking everyone else.
///
/// <para><b>Prefer Kahuna's distributed locks for anything new.</b> <c>LocateAndTryLock</c> /
/// <c>TryExtendLock</c> / <c>TryUnlock</c> provide all of this natively — a leased grant, server-side
/// owner checking on extend and unlock, and a <em>monotonic</em> fencing token, which is strictly
/// stronger than the identity token synthesized here (ordering can prove a writer is stale; equality
/// can only fail to recognize it). Row-level TTL uses them; see
/// <c>CommandsExecutor.Controllers.Ttl.TtlSpanLease</c>.</para>
///
/// <para>This type remains because the drop-intent fence it serves is not just a lock: startup recovery
/// enumerates fence markers by KV range scan and reads their values to reclaim this node's own
/// prior-run remnants, and Kahuna's lock API exposes no equivalent enumeration. Moving that fence to
/// locks means first deciding whether the scrub is still needed at all now that leases expire — a
/// behavioural question about deferred drop, not a mechanical port.</para>
///
/// <para><b>Why a lease and not a plain marker.</b> A marker written with <c>SetIfNotExists</c> alone is
/// held forever by whoever wrote it — if that node crashes, nothing can reclaim the resource, because the
/// only thing that would delete the marker is the process that no longer exists. Giving the key a native
/// expiry turns "held" into "held recently": a dead owner's claim lapses on its own. A live owner keeps
/// its claim by re-stamping the expiry in the background, so an operation that legitimately outruns one
/// lease period is never interrupted.</para>
///
/// <para><b>Every acquisition carries a unique fencing token, and every write is conditional on it.</b>
/// This is the part that is easy to get wrong. A token that identifies only the <em>holder</em> cannot
/// distinguish two acquisitions of the same key by the same process, so a holder that stalled past its
/// expiry, lost the key, and then woke up would happily renew — or release — a lease that now belongs to
/// someone else. Releasing is the dangerous one: an unconditional delete by a stalled predecessor
/// silently hands the resource to a third party while its rightful owner is still working. So release is
/// <em>not</em> a delete. It is a compare-and-set of this token onto a near-immediate expiry, which frees
/// the key promptly when we still hold it and does nothing at all when we do not.</para>
///
/// <para><b>Renewal is likewise a compare-and-set on this acquisition's token.</b> A renewal that
/// arrives after the lease lapsed finds a different value (or none) and fails, and the renewer stops
/// rather than stealing back a key it no longer holds.</para>
///
/// <para><b>Transient statuses are not contention.</b> Only a real <c>NotSet</c> means someone else holds
/// a live lease. <c>MustRetry</c>/<c>WaitingForReplication</c> are replication timing, not an owner, and
/// are retried with bounded backoff — treating them as "held" would make acquisition fail spuriously
/// under load, which for a background sweep looks like a job that mysteriously never runs.</para>
///
/// <para><b>The lease is not a checkpoint.</b> Store progress in a separate, non-expiring key. Anything
/// written into the lease value dies with the lease, which is the correct lifetime for "who owns this"
/// and precisely the wrong one for "how far did they get".</para>
/// </summary>
internal sealed class KeyLeaseFence : IAsyncDisposable
{
    private const int MaxRetries = 10;

    /// <summary>
    /// Expiry stamped on the released marker, so a key nobody re-acquires does not linger. Not zero:
    /// zero means "no expiry" to the KV layer, which would make a release pin the key forever — the
    /// exact opposite of releasing it.
    /// </summary>
    private const int ReleaseExpiryMs = 1;

    /// <summary>
    /// Written by a release to mean "this key was deliberately given up and is free to take".
    ///
    /// <para>A release cannot simply delete the key: an unconditional delete by a lapsed predecessor
    /// would destroy its successor's live claim. But it also cannot simply let the lease run out, or an
    /// orderly hand-off would cost a full lease period — the whole point of releasing is that the next
    /// worker starts now. So a release compare-and-sets this marker in place of its own token, and
    /// acquisition treats the marker as claimable by compare-and-set. Both operations stay conditional,
    /// and a released span is available immediately.</para>
    /// </summary>
    private static readonly byte[] FreeMarker = "~camusdb-lease-free"u8.ToArray();

    /// <summary>
    /// One successful acquisition. Identity matters as much as content: renewer teardown removes this
    /// exact instance rather than "whatever is filed under this key", so a renewer that finishes after
    /// the key has been re-acquired cannot dispose its successor's registration.
    /// </summary>
    private sealed class Acquisition(string token, byte[] value, CancellationTokenSource cts)
    {
        public readonly string Token = token;
        public readonly byte[] Value = value;
        public readonly CancellationTokenSource Cts = cts;
    }

    private readonly IKahuna kahuna;
    private readonly byte[] ownerValue;
    private readonly int leaseMs;
    private readonly int renewIntervalMs;

    private readonly ConcurrentDictionary<string, Acquisition> held = new(StringComparer.Ordinal);

    /// <param name="ownerValue">
    /// Identifies this holder — typically node id plus process epoch. It is a <em>prefix</em> of the
    /// stored value, not the whole of it: a per-acquisition token is appended so two acquisitions by
    /// this same holder are still distinguishable.
    /// </param>
    public KeyLeaseFence(IKahuna kahuna, byte[] ownerValue, int leaseMs, int renewIntervalMs)
    {
        this.kahuna = kahuna;
        this.ownerValue = ownerValue;
        this.leaseMs = leaseMs;
        this.renewIntervalMs = renewIntervalMs;
    }

    /// <summary>
    /// Attempts to take the lease on <paramref name="key"/>. Returns this acquisition's fencing token if
    /// this holder now owns the key (and a background renewer has been started), or null if another
    /// holder's live lease has it, or if the attempt could not be resolved — treated as "not acquired" so
    /// the caller retries later rather than proceeding unfenced.
    ///
    /// <para>Callers that write state governed by this lease should keep the returned token and pass it
    /// back via <see cref="StillHeldLocally(string, string)"/> so their writes are tied to the
    /// acquisition that authorized them, not merely to the key.</para>
    /// </summary>
    public async Task<string?> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
    {
        // Minted per attempt, so no two acquisitions of this key — by this holder or any other — ever
        // present the same value to a compare-and-set.
        string token = Guid.NewGuid().ToString("N");
        byte[] value = ComposeValue(token);

        int retries = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, 
                key, 
                value, 
                null, 
                -1,
                KeyValueFlags.SetIfNotExists, 
                leaseMs, 
                KeyValueDurability.Persistent, 
                cancellationToken
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
            {
                StartRenewer(key, token, value);
                return token;
            }

            if (type == KeyValueResponseType.NotSet)
            {
                // The key exists. That is either a live claim — real contention — or the free marker a
                // release left behind, which is ours for the taking. Compare-and-set distinguishes them
                // without a read, and without any window in which two acquirers could both succeed.
                (KeyValueResponseType freeType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, 
                    key, 
                    value, 
                    FreeMarker, 
                    -1,
                    KeyValueFlags.SetIfEqualToValue, 
                    leaseMs, 
                    KeyValueDurability.Persistent, 
                    cancellationToken
                ).ConfigureAwait(false);

                if (freeType == KeyValueResponseType.Set)
                {
                    StartRenewer(key, token, value);
                    return token;
                }

                if (freeType == KeyValueResponseType.NotSet)
                    return null; // a live claim genuinely holds the key

                if (freeType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                    && ++retries < MaxRetries)
                {
                    await Task.Delay(retries * 10, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return null;
            }

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
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
    /// Releases the lease held under <paramref name="token"/>, stopping its renewer and freeing the key
    /// for the next holder — but only if this acquisition still owns it.
    ///
    /// <para>Deliberately a compare-and-set of the free marker rather than a delete. A holder that
    /// stalled past its lease, lost the key, and then reached this line would, with an unconditional
    /// delete, remove a lease its successor is actively working under — handing the resource to a third
    /// holder while two workers believe they own it. The compare makes that case a no-op, and the marker
    /// keeps the hand-off immediate: the next acquirer claims it by compare-and-set instead of waiting
    /// out a lease.</para>
    ///
    /// <para>Passing a token that is not the current acquisition does nothing, which makes a duplicate or
    /// late release harmless.</para>
    ///
    /// <para><b>The marker write retries transient statuses.</b> <c>MustRetry</c> and
    /// <c>WaitingForReplication</c> are replication timing, not an outcome. A release that gave up on the
    /// first transient status would leave the old value in place under its full lease, so the next
    /// acquirer would wait out the whole lease — the observable symptom is a "released" fence that stays
    /// held for tens of seconds. The retry budget matches <see cref="TryAcquireAsync"/>. Only after the
    /// budget is exhausted does the release fall back to the lease lapse.</para>
    /// </summary>
    public async Task ReleaseAsync(string key, string token)
    {
        if (!held.TryGetValue(key, out Acquisition? acquisition) ||
            !string.Equals(acquisition.Token, token, StringComparison.Ordinal))
            return; // never ours, or already superseded — nothing of ours to release

        // Remove this exact registration, and stop renewing before touching the key so the renewer
        // cannot re-stamp the lease after we have expired it.
        if (!held.TryRemove(new KeyValuePair<string, Acquisition>(key, acquisition)))
            return; // a concurrent release/teardown won

        try { await acquisition.Cts.CancelAsync().ConfigureAwait(false); } catch { }
        acquisition.Cts.Dispose();

        int retries = 0;
        while (true)
        {
            try
            {
                (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero,
                    key,
                    FreeMarker,
                    acquisition.Value,
                    -1,
                    KeyValueFlags.SetIfEqualToValue,
                    ReleaseExpiryMs,
                    KeyValueDurability.Persistent,
                    CancellationToken.None
                ).ConfigureAwait(false);

                if (type == KeyValueResponseType.Set)
                    return; // the free marker is in place; the next acquirer claims it immediately

                if (type == KeyValueResponseType.NotSet)
                    return; // the stored value is no longer this acquisition's — nothing of ours to free

                if (type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                    return; // hard failure with an unknown key state — the lease lapses on its own
            }
            catch
            {
                // A transport failure has an unknown outcome. The compare-and-set is idempotent, so a
                // retry is safe: a write that already landed makes the retry a NotSet no-op.
            }

            if (++retries >= MaxRetries)
                return; // budget exhausted — the lease lapses on its own

            await Task.Delay(retries * 10).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Whether this holder's renewer still believes it owns <paramref name="key"/> under
    /// <paramref name="token"/>. A local view, not a KV read: it says the renewer has not yet observed a
    /// loss. Use it to bail out of long work early and to fence writes against the acquisition that
    /// authorized them — never as proof of ownership at the instant of a write, which only a
    /// compare-and-set can give.
    /// </summary>
    public bool StillHeldLocally(string key, string token) =>
        held.TryGetValue(key, out Acquisition? acquisition) &&
        string.Equals(acquisition.Token, token, StringComparison.Ordinal);

    private byte[] ComposeValue(string token)
    {
        byte[] suffix = System.Text.Encoding.UTF8.GetBytes(":" + token);
        byte[] value = new byte[ownerValue.Length + suffix.Length];
        ownerValue.CopyTo(value, 0);
        suffix.CopyTo(value, ownerValue.Length);
        return value;
    }

    private void StartRenewer(string key, string token, byte[] value)
    {
        CancellationTokenSource cts = new();
        Acquisition acquisition = new(token, value, cts);

        // Replace any prior registration for this key. It should not exist — we only just acquired the
        // key, so any predecessor's lease had lapsed — but if one lingers, tearing it down here stops a
        // stale renewer that would otherwise keep running against a key we now own under a new token.
        if (held.TryRemove(key, out Acquisition? stale))
        {
            try { stale.Cts.Cancel(); } catch { }
            stale.Cts.Dispose();
        }

        held[key] = acquisition;
        _ = RenewLoopAsync(key, acquisition, cts.Token);
    }

    private async Task RenewLoopAsync(string key, Acquisition acquisition, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(renewIntervalMs, ct).ConfigureAwait(false);

                if (!await TryRenewOnceAsync(key, acquisition, ct).ConfigureAwait(false))
                    break;
            }
        }
        catch (OperationCanceledException) { /* released — normal */ }
        catch { /* best-effort renewal; a missed renewal only shortens the lease */ }
        finally
        {
            // Remove only THIS registration. Removing by key alone would evict a newer acquisition
            // installed after this loop was cancelled — the ABA that lets a live owner lose its own
            // bookkeeping and then decline to release the key it still holds.
            if (held.TryRemove(new KeyValuePair<string, Acquisition>(key, acquisition)))
                acquisition.Cts.Dispose();
        }
    }

    /// <summary>
    /// One renewal attempt. Returns false when the lease is definitively lost, so the caller stops.
    ///
    /// <para>Transient statuses are retried <em>within this attempt</em> rather than waiting a whole
    /// renew interval: the point of renewing early is to leave slack before the lease deadline, and
    /// spending that slack in a sleep is how a live owner loses a key it could have kept.</para>
    /// </summary>
    private async Task<bool> TryRenewOnceAsync(string key, Acquisition acquisition, CancellationToken ct)
    {
        int retries = 0;

        while (!ct.IsCancellationRequested)
        {
            (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, 
                key, 
                acquisition.Value, 
                acquisition.Value, -1,
                KeyValueFlags.SetIfEqualToValue, 
                leaseMs, 
                KeyValueDurability.Persistent, 
                ct
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
                return true;

            // NotSet means the stored value is no longer this acquisition's token: the lease lapsed and
            // someone else took the key. Renewing further would overwrite their claim.
            if (type == KeyValueResponseType.NotSet)
                return false;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 5, ct).ConfigureAwait(false);
                continue;
            }

            // Exhausted retries or a hard error: we cannot confirm ownership, so stop renewing and let
            // the lease lapse rather than continuing to act as owner on an unverified claim.
            return false;
        }

        return false;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (KeyValuePair<string, Acquisition> entry in held)
        {
            if (held.TryRemove(new KeyValuePair<string, Acquisition>(entry.Key, entry.Value)))
            {
                try { await entry.Value.Cts.CancelAsync().ConfigureAwait(false); } catch { }
                entry.Value.Cts.Dispose();
            }
        }
    }
}
