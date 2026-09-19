/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Auth;

/// <summary>
/// The authority one long-lived stream runs under, from the moment it authenticates until it closes.
///
/// <para><b>Why a stream cannot simply re-present its token.</b> A request-per-call transport attaches
/// the caller's current token to every call, so an expired token is replaced on the next one. A duplex
/// stream authenticates once, in its opening metadata, and carries that one token for as long as it
/// stays open — which is meant to be a whole client session, far longer than a token lives. Resolving
/// that token again per operation therefore fails every stream at the token's expiry, and takes every
/// transaction pinned to the stream down with it. A token's lifetime bounds how long it may be
/// <em>presented</em>; it was never meant to bound a connection that presented it while it was good.</para>
///
/// <para><b>So the authority has two phases.</b> While the token is inside its lifetime the stream is
/// <em>token-bound</em>: each refresh resolves the token, exactly as any other transport would, and a
/// failure means the session was logged out, revoked, or its account was dropped. Once the token's
/// lifetime has run out, the stream looks its session record up one last time. Found and unrevoked,
/// the session ended by the clock alone and the stream becomes <em>account-bound</em>: from then on a
/// refresh re-reads the account, and the stream ends if the account is gone, was re-created, changed
/// its password, or every session was revoked. Not found, the session was ended early, and so is the
/// stream.</para>
///
/// <para><b>That last lookup is on a clock, not on the next operation.</b> The record is the only
/// evidence that separates an expiry from a logout, and the session sweep removes it
/// <see cref="CamusDBOptions.ExpiredSessionRetentionMs"/> after the expiry. An idle stream would find
/// nothing either way, so <see cref="AuthService"/> schedules the lookup for the instant of expiry
/// instead of leaving it to whichever operation happens to come next. A stream that still misses the
/// window is ended: ambiguous evidence is read as a revocation, never as a pass.</para>
///
/// <para>State here is shared by the stream's read loop and that scheduled lookup. The published
/// <see cref="Current"/> snapshot is immutable and swapped whole, so the per-operation fast path reads
/// it without a lock; everything else is touched only under <see cref="Gate"/>.</para>
/// </summary>
public sealed class StreamAuthority : IDisposable
{
    /// <summary>One resolved principal plus what is needed to notice it has gone out of date.</summary>
    /// <param name="Principal">The principal operations run as while this snapshot is current.</param>
    /// <param name="RefreshAtTicks"><see cref="Environment.TickCount64"/> at which it must be resolved again.</param>
    /// <param name="Generation">The catalog generation read <b>before</b> the principal was resolved, so
    /// a change that lands mid-resolve leaves the stamp behind its data and forces one more refresh.</param>
    internal sealed record Snapshot(Principal Principal, long RefreshAtTicks, long Generation);

    internal const int TokenBound = 0;
    internal const int AccountBound = 1;
    internal const int Ended = 2;

    internal readonly string Bearer;
    internal readonly string TokenId;
    internal readonly byte[] SecretMac;
    internal readonly string UserName;
    internal readonly string? UserId;

    /// <summary>The account's credential epoch when the session was issued. A password change moves
    /// the account's epoch away from it, which is how an account-bound stream sees one.</summary>
    internal readonly long CredentialEpoch;

    /// <summary>The session's absolute expiry: the instant the stream stops being token-bound.</summary>
    internal readonly DateTime ExpiresAt;

    /// <summary>The catalog's session-revocation epoch when the stream opened. A later value means
    /// every session was revoked while this stream was open, including the one it opened with.</summary>
    internal readonly long RevocationEpoch;

    /// <summary>Serializes every refresh. Never disposed: a refresh may still hold it when the stream
    /// that owns this object is torn down, and it owns no handle that needs releasing.</summary>
    internal readonly SemaphoreSlim Gate = new(1, 1);

    internal volatile Snapshot? Current;

    internal volatile int Phase = TokenBound;

    /// <summary>The catalog generation at which <see cref="RevocationEpoch"/> was last compared with
    /// storage. The epoch moves only together with the generation, so an unchanged generation makes
    /// the comparison unnecessary. Starts at a value no generation takes. Guarded by <see cref="Gate"/>.</summary>
    internal long RevocationCheckedAtGeneration = -1;

    // Neither source is disposed. Both can be cancelled by a refresh that outlives the stream's own
    // teardown, and neither holds a timer or a wait handle, so there is nothing to release.
    private readonly CancellationTokenSource ended = new();
    private readonly CancellationTokenSource closed = new();

    internal StreamAuthority(
        string bearer,
        string tokenId,
        SessionRecord session,
        Principal principal,
        long revocationEpoch)
    {
        Bearer = bearer;
        TokenId = tokenId;
        SecretMac = session.SecretMac;
        UserName = principal.UserName;
        UserId = principal.UserId;
        CredentialEpoch = session.CredentialEpoch;
        ExpiresAt = session.ExpiresAt;
        RevocationEpoch = revocationEpoch;
    }

    /// <summary>
    /// Fires when the authority is found to be gone — the session was logged out or revoked, or the
    /// account no longer backs it. A stream links this into its read loop so that an idle stream ends
    /// when the scheduled lookup finds that out, rather than lingering until a next operation that may
    /// never come.
    /// </summary>
    public CancellationToken AuthorityEnded => ended.Token;

    /// <summary>True once the authority has ended. It never comes back: the stream must be reopened
    /// with a token that resolves.</summary>
    public bool HasEnded => Phase == Ended;

    /// <summary>Fires when the owning stream closes; stops the scheduled expiry lookup.</summary>
    internal CancellationToken Closed => closed.Token;

    /// <summary>Publishes <see cref="AuthorityEnded"/>. Called outside <see cref="Gate"/>, because a
    /// cancellation callback may run the stream's teardown inline.</summary>
    internal void SignalEnded()
    {
        try { ended.Cancel(); } catch (AggregateException) { /* a listener's failure is not ours */ }
    }

    /// <summary>Called by the owning stream when it closes, for any reason.</summary>
    public void Dispose()
    {
        try { closed.Cancel(); } catch (AggregateException) { /* a listener's failure is not ours */ }
    }
}
