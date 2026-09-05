/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.Auth;

/// <summary>
/// The strength floor for the two operator-supplied authentication secrets: the access-token server
/// key and the node secret. Both are checked at startup so a weak value fails the node rather than
/// the first request that needs it.
///
/// <para><b>Why a floor exists at all.</b> The token design's premise is that a leaked auth catalog
/// yields nothing usable, because the catalog stores only an HMAC keyed by the server key. A guessable
/// key voids that premise entirely: an attacker with the catalog forges tokens offline. The node secret
/// carries the same weight for the peer routes, which accept it in place of a user credential.</para>
///
/// <para><b>Why a length floor and not a denylist.</b> A list of obvious values is easy to circumvent
/// and hard to keep current — <c>changeme</c> fails it, <c>changeme1</c> does not, and neither is
/// stronger than the other. A length floor is one rule, testable, and cannot be argued around.</para>
///
/// <para>The floor is a constant rather than a setting on purpose. A configurable minimum an operator
/// can lower to one byte is not a floor.</para>
/// </summary>
public static class AuthSecretPolicy
{
    /// <summary>
    /// Minimum length of an authentication secret, in bytes of UTF-8. Thirty-two bytes matches the
    /// output width of the SHA-256 HMAC these keys drive, so a shorter key adds no strength that the
    /// construction can carry.
    /// </summary>
    public const int MinimumSecretBytes = 32;

    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.InvalidConfig"/> when <paramref name="secret"/> is shorter
    /// than <see cref="MinimumSecretBytes"/>. An empty value passes: absence is the caller's decision
    /// to make, because an unset node secret means the peer routes are refused (fail-closed) while an
    /// unset token key means no token can be issued at all.
    ///
    /// <para>Measures UTF-8 bytes rather than characters, so a short string of multi-byte characters is
    /// judged by the key material it actually provides. The message names the environment variable and
    /// the requirement, and never the value.</para>
    /// </summary>
    /// <param name="secret">The configured secret. Null or empty is accepted here.</param>
    /// <param name="environmentVariable">The variable an operator sets, named in the failure message.</param>
    public static void EnsureStrongEnough(string? secret, string environmentVariable)
    {
        if (string.IsNullOrEmpty(secret))
            return;

        int bytes = Encoding.UTF8.GetByteCount(secret);
        if (bytes >= MinimumSecretBytes)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidConfig,
            $"'{environmentVariable}' is too weak: it must be at least {MinimumSecretBytes} bytes, but the " +
            $"configured value is {bytes}. Generate one with: openssl rand -hex 32");
    }
}
