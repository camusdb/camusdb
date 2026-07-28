/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Text;

namespace CamusDB.Core.Auth;

/// <summary>
/// Mints and validates opaque bearer tokens of the form <c>camus_&lt;tokenId&gt;.&lt;secret&gt;</c>,
/// where both halves are URL-safe base64 of cryptographically random bytes. The <c>tokenId</c> is the
/// indexable half (the session key); the 256-bit <c>secret</c> makes guessing infeasible.
///
/// <para>The catalog stores only <see cref="ComputeMac"/> of <c>tokenId + secret</c> keyed with a
/// server-side key, never the secret, so a catalog leak does not yield usable tokens. The token string
/// never appears in URLs, SQL, logs, or metrics — only in the <c>Authorization</c> header.</para>
/// </summary>
public static class TokenCodec
{
    private const string Prefix = "camus_";
    private const int TokenIdBytes = 16;
    private const int SecretBytes = 32;

    public readonly record struct MintedToken(string Bearer, string TokenId, string Secret);

    /// <summary>Generates a fresh token. Both halves come from <see cref="RandomNumberGenerator"/>.</summary>
    public static MintedToken Mint()
    {
        string tokenId = Base64Url(RandomNumberGenerator.GetBytes(TokenIdBytes));
        string secret = Base64Url(RandomNumberGenerator.GetBytes(SecretBytes));
        return new MintedToken($"{Prefix}{tokenId}.{secret}", tokenId, secret);
    }

    /// <summary>
    /// Parses a bearer token into its id and secret. Returns false for anything not shaped like a
    /// CamusDB token, so a malformed value is an authentication failure, not an exception.
    /// </summary>
    public static bool TryParse(string? bearer, out string tokenId, out string secret)
    {
        tokenId = "";
        secret = "";

        if (string.IsNullOrEmpty(bearer) || !bearer.StartsWith(Prefix, StringComparison.Ordinal))
            return false;

        string body = bearer[Prefix.Length..];
        int dot = body.IndexOf('.');
        if (dot <= 0 || dot == body.Length - 1)
            return false;

        tokenId = body[..dot];
        secret = body[(dot + 1)..];
        return tokenId.Length > 0 && secret.Length > 0;
    }

    /// <summary>Keyed HMAC-SHA256 of <c>tokenId + secret</c> — the value stored per session.</summary>
    public static byte[] ComputeMac(string serverKey, string tokenId, string secret)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(serverKey);
        byte[] message = Encoding.UTF8.GetBytes(tokenId + secret);
        return HMACSHA256.HashData(keyBytes, message);
    }

    /// <summary>Constant-time comparison of two MACs.</summary>
    public static bool MacEquals(byte[] a, byte[] b) => CryptographicOperations.FixedTimeEquals(a, b);

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
