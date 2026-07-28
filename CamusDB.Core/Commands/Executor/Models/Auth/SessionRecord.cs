/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A persisted login session, keyed in KV by an opaque random token id
/// (<c>_system/auth/session:{tokenId}</c>). It never stores the bearer secret itself — only a keyed
/// HMAC of it (<see cref="SecretMac"/>) — so a catalog leak does not yield usable tokens.
///
/// <para>Validation of an incoming token recomputes the HMAC and compares it in constant time, then
/// checks that the token has not expired (<see cref="ExpiresAt"/>) or been revoked
/// (<see cref="Revoked"/>), and that the captured epochs still match the user's current epochs —
/// a password change / drop bumps the credential epoch and a grant/revoke bumps the authorization
/// epoch, so a stale token or privilege snapshot is detected and rejected/reloaded.</para>
/// </summary>
public sealed class SessionRecord
{
    /// <summary>Opaque random token id (the indexable half of the bearer token).</summary>
    public string TokenId { get; set; } = "";

    /// <summary>Normalized user name this session authenticates.</summary>
    public string User { get; set; } = "";

    /// <summary><c>HMAC-SHA256(serverKey, tokenId + secret)</c>, base64. Never the raw secret.</summary>
    public byte[] SecretMac { get; set; } = [];

    /// <summary>User credential epoch captured at login; a later change invalidates this session.</summary>
    public long CredentialEpoch { get; set; }

    /// <summary>User authorization epoch captured at login; a later grant/revoke forces a reload.</summary>
    public long AuthorizationEpoch { get; set; }

    /// <summary>UTC issue time.</summary>
    public DateTime IssuedAt { get; set; }

    /// <summary>UTC absolute expiry. A token presented after this is rejected.</summary>
    public DateTime ExpiresAt { get; set; }

    /// <summary>Set when the session was explicitly logged out / revoked before expiry.</summary>
    public bool Revoked { get; set; }
}
