/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Auth;

/// <summary>
/// Orchestrates authentication: password-verify at login (the only place the expensive KDF runs),
/// opaque bearer-token minting/validation, and per-node caching of the resolved principal. Sits above
/// <see cref="AuthCatalog"/> (which owns durable user/grant/session storage) and is used by the
/// transports to turn credentials into a token and a token into a <see cref="Principal"/>.
///
/// <para>Ordinary requests never run the KDF — they validate the random token's HMAC and read a cached
/// authorization snapshot bounded by <see cref="CamusDBConfig.AuthenticationCacheTtl"/>, so a revoke on
/// another node takes effect within that window. Login is rate-limited per account and the number of
/// concurrent KDF operations is capped, so a login flood cannot exhaust CPU.</para>
/// </summary>
public sealed class AuthService
{
    private readonly Task<AuthCatalog> catalogTask;
    private readonly SemaphoreSlim kdfConcurrency = new(CamusDBConfig.LoginKdfMaxConcurrency, CamusDBConfig.LoginKdfMaxConcurrency);

    // tokenId -> (principal, cache expiry). Bounded by AuthenticationCacheMaxEntries; only successful
    // validations are cached, and never past the token's own lifetime.
    private readonly ConcurrentDictionary<string, (Principal Principal, DateTime Expires)> principalCache = new(StringComparer.Ordinal);

    // Normalized user -> (attempts, window start). Fixed one-minute window.
    private readonly ConcurrentDictionary<string, (int Count, DateTime WindowStart)> loginAttempts = new(StringComparer.Ordinal);

    // A fixed verifier used to spend comparable work on an unknown/passwordless user so a failed login
    // takes about as long whether or not the account exists (reduces enumeration by timing).
    private static readonly Lazy<Credential> DummyCredential = new(() => PasswordHasher.Hash("camusdb-dummy-password"));

    public AuthService(Task<AuthCatalog> catalogTask)
    {
        this.catalogTask = catalogTask;
    }

    private static string Normalize(string name) => name.ToLowerInvariant();

    /// <summary>
    /// Verifies credentials and, on success, issues a short-lived opaque bearer token whose session is
    /// persisted (storing only the token's HMAC). Failure — unknown user, no password, or wrong
    /// password — throws <see cref="CamusDBErrorCodes.AuthenticationFailed"/> with a uniform shape after
    /// spending comparable work. Rate-limited per account; a login flood surfaces as
    /// <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/>.
    /// </summary>
    public async Task<string> LoginAsync(string user, string password)
    {
        if (string.IsNullOrEmpty(CamusDBConfig.AccessTokenServerKey))
            throw new CamusDBException(CamusDBErrorCodes.InvalidConfig, "Access token server key is not configured");

        string normalized = Normalize(user);
        RegisterAttempt(normalized);

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        UserRecord? record = await catalog.TryGetUserAsync(normalized).ConfigureAwait(false);

        bool ok;
        if (!await kdfConcurrency.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false))
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Authentication is busy; retry shortly");
        try
        {
            // Always run a verification (real or dummy) so timing does not reveal whether the account
            // exists or has a password.
            if (record?.Credential is not null)
            {
                ok = PasswordHasher.Verify(password, record.Credential);
            }
            else
            {
                _ = PasswordHasher.Verify(password, DummyCredential.Value); // spend comparable work, discard
                ok = false;
            }
        }
        finally
        {
            kdfConcurrency.Release();
        }

        if (!ok || record is null)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        TokenCodec.MintedToken token = TokenCodec.Mint();
        DateTime now = DateTime.UtcNow;
        SessionRecord session = new()
        {
            TokenId = token.TokenId,
            User = normalized,
            SecretMac = TokenCodec.ComputeMac(CamusDBConfig.AccessTokenServerKey, token.TokenId, token.Secret),
            CredentialEpoch = record.CredentialEpoch,
            AuthorizationEpoch = record.AuthorizationEpoch,
            IssuedAt = now,
            ExpiresAt = now.Add(CamusDBConfig.AccessTokenTtl),
        };
        await catalog.CreateSessionAsync(session).ConfigureAwait(false);

        return token.Bearer;
    }

    /// <summary>
    /// Resolves a bearer token to a <see cref="Principal"/>, or throws
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/>. Serves a bounded, short-TTL cache first;
    /// on a miss it validates the token HMAC (constant-time), expiry/revocation, and that the session's
    /// captured credential epoch still matches the user's — so a password change / drop / logout
    /// invalidates outstanding tokens. Privileges are always reloaded fresh from the catalog.
    /// </summary>
    public async Task<Principal> ResolvePrincipalAsync(string? bearer)
    {
        if (!TokenCodec.TryParse(bearer, out string tokenId, out string secret))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        if (principalCache.TryGetValue(tokenId, out (Principal Principal, DateTime Expires) cached) && cached.Expires > DateTime.UtcNow)
            return cached.Principal;

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        SessionRecord? session = await catalog.TryGetSessionAsync(tokenId).ConfigureAwait(false);
        if (session is null || session.Revoked || session.ExpiresAt <= DateTime.UtcNow)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        byte[] mac = TokenCodec.ComputeMac(CamusDBConfig.AccessTokenServerKey, tokenId, secret);
        if (!TokenCodec.MacEquals(mac, session.SecretMac))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        UserRecord? user = await catalog.TryGetUserAsync(session.User).ConfigureAwait(false);
        if (user is null || user.CredentialEpoch != session.CredentialEpoch)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        IReadOnlyList<GrantRecord> grants = await catalog.ListGrantsAsync(session.User).ConfigureAwait(false);
        Principal principal = new(session.User, user.IsSuperuser, grants);

        CachePrincipal(tokenId, principal);
        return principal;
    }

    /// <summary>Revokes the presented token's session (logout) and evicts its cache entry. Idempotent.</summary>
    public async Task LogoutAsync(string? bearer)
    {
        if (!TokenCodec.TryParse(bearer, out string tokenId, out _))
            return;

        principalCache.TryRemove(tokenId, out _);
        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        await catalog.DeleteSessionAsync(tokenId).ConfigureAwait(false);
    }

    private void CachePrincipal(string tokenId, Principal principal)
    {
        DateTime now = DateTime.UtcNow;
        if (principalCache.Count >= CamusDBConfig.AuthenticationCacheMaxEntries)
        {
            foreach (KeyValuePair<string, (Principal Principal, DateTime Expires)> entry in principalCache)
            {
                if (entry.Value.Expires <= now)
                    principalCache.TryRemove(entry.Key, out _);
            }
            if (principalCache.Count >= CamusDBConfig.AuthenticationCacheMaxEntries)
                return; // still full — skip caching rather than grow unbounded
        }

        principalCache[tokenId] = (principal, now.Add(CamusDBConfig.AuthenticationCacheTtl));
    }

    private void RegisterAttempt(string normalizedUser)
    {
        DateTime now = DateTime.UtcNow;
        (int Count, DateTime WindowStart) updated = loginAttempts.AddOrUpdate(
            normalizedUser,
            _ => (1, now),
            (_, existing) => now - existing.WindowStart > TimeSpan.FromMinutes(1) ? (1, now) : (existing.Count + 1, existing.WindowStart));

        if (updated.Count > CamusDBConfig.LoginMaxAttemptsPerMinute)
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Too many authentication attempts; retry later");
    }
}
