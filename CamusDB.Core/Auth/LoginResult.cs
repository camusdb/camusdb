
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Auth;

/// <summary>
/// Outcome of a successful credential exchange: the opaque bearer token plus the moment it stops being
/// accepted.
///
/// <para>The expiry is reported because a client cannot otherwise know it. The session's absolute
/// deadline is derived server-side from <see cref="CamusDBOptions.AccessTokenTtl"/>, so a driver that is
/// not told it must either guess a lifetime — and guess wrong whenever an operator shortens the TTL — or
/// discover every expiry reactively, by letting a real statement fail authentication first.</para>
///
/// <para><see cref="ExpiresAt"/> is UTC. It is the authoritative value; a seconds-until-expiry figure is
/// derived from it at the transport boundary for clients that would rather renew on a monotonic timer
/// than trust their own wall clock.</para>
/// </summary>
/// <param name="Token">The opaque bearer token to present in <c>Authorization: Bearer</c>.</param>
/// <param name="ExpiresAt">UTC instant after which the token is rejected.</param>
public readonly record struct LoginResult(string Token, DateTime ExpiresAt)
{
    /// <summary>
    /// Whole seconds remaining until <see cref="ExpiresAt"/>, floored at zero, measured from
    /// <paramref name="now"/> (UTC). Rounds <b>down</b> so a client that renews at the reported figure is
    /// always inside the real deadline rather than a fraction of a second past it.
    /// </summary>
    public long SecondsUntilExpiry(DateTime now)
    {
        double seconds = Math.Floor((ExpiresAt - now).TotalSeconds);
        return seconds <= 0 ? 0 : (long)seconds;
    }
}
