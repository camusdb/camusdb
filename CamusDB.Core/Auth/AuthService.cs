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
/// another node takes effect within that window. A cache hit still re-checks the token's secret HMAC on
/// every request, so the cache never authenticates a token by id alone. Login is rate-limited per
/// account and source and the number of concurrent KDF operations is capped, so a login flood cannot
/// exhaust CPU or memory.</para>
/// </summary>
public sealed class AuthService
{
    private readonly Task<AuthCatalog> catalogTask;
    private readonly SemaphoreSlim kdfConcurrency = new(CamusDBConfig.LoginKdfMaxConcurrency, CamusDBConfig.LoginKdfMaxConcurrency);

    // tokenId -> cached validation. The SecretMac is re-verified on every hit (constant-time), so a
    // warm cache never lets `camus_<id>.<wrong-secret>` authenticate. Bounded by
    // AuthenticationCacheMaxEntries; expiry is capped by the session's own absolute lifetime.
    private readonly ConcurrentDictionary<string, CachedSession> principalCache = new(StringComparer.Ordinal);

    // "account\nsource" -> (attempts, window start). Fixed one-minute window; bounded and self-purging.
    private readonly ConcurrentDictionary<string, (int Count, DateTime WindowStart)> loginAttempts = new(StringComparer.Ordinal);

    // A fixed verifier used to spend comparable work on an unknown/passwordless user so a failed login
    // takes about as long whether or not the account exists (reduces enumeration by timing).
    private static readonly Lazy<Credential> DummyCredential = new(() => PasswordHasher.Hash("camusdb-dummy-password"));

    private static readonly TimeSpan RateWindow = TimeSpan.FromMinutes(1);

    private readonly record struct CachedSession(Principal Principal, byte[] SecretMac, DateTime Expires);

    public AuthService(Task<AuthCatalog> catalogTask)
    {
        this.catalogTask = catalogTask;
    }

    private static string Normalize(string name) => name.ToLowerInvariant();

    /// <summary>
    /// Verifies credentials and, on success, issues a short-lived opaque bearer token whose session is
    /// persisted (storing only the token's HMAC). Failure — unknown user, no password, or wrong
    /// password — throws <see cref="CamusDBErrorCodes.AuthenticationFailed"/> with a uniform shape after
    /// spending comparable work. Rate-limited per (account, <paramref name="source"/>); a login flood
    /// surfaces as <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/>.
    /// </summary>
    public async Task<string> LoginAsync(string user, string password, string source = "")
    {
        if (string.IsNullOrEmpty(CamusDBConfig.AccessTokenServerKey))
            throw new CamusDBException(CamusDBErrorCodes.InvalidConfig, "Access token server key is not configured");

        string normalized = Normalize(user);
        RegisterAttempt(normalized, source);

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        UserRecord? record = await catalog.TryGetUserAsync(normalized).ConfigureAwait(false);

        bool ok;
        if (!await kdfConcurrency.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false))
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Authentication is busy; retry shortly");
        try
        {
            // Always run a verification (real or dummy) so timing does not reveal whether the account
            // exists or has a password. PasswordHasher.Verify caps oversized input before the KDF.
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
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/>. A cache hit still recomputes and
    /// constant-time-compares the token's secret HMAC before trusting it — the cache never authenticates
    /// by token id alone. On a miss it validates the HMAC, expiry/revocation, and that the session's
    /// captured credential epoch still matches the user's, then caches the resolved principal bounded by
    /// the lesser of the session's absolute expiry and the cache TTL.
    /// </summary>
    public async Task<Principal> ResolvePrincipalAsync(string? bearer)
    {
        if (!TokenCodec.TryParse(bearer, out string tokenId, out string secret))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        byte[] presentedMac = TokenCodec.ComputeMac(CamusDBConfig.AccessTokenServerKey, tokenId, secret);

        // Cache hit is trusted ONLY when the presented secret still matches — a wrong secret with a
        // known id falls through to full validation (which also fails).
        if (principalCache.TryGetValue(tokenId, out CachedSession cached)
            && cached.Expires > DateTime.UtcNow
            && TokenCodec.MacEquals(presentedMac, cached.SecretMac))
        {
            return cached.Principal;
        }

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        SessionRecord? session = await catalog.TryGetSessionAsync(tokenId).ConfigureAwait(false);
        if (session is null || session.Revoked || session.ExpiresAt <= DateTime.UtcNow)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        if (!TokenCodec.MacEquals(presentedMac, session.SecretMac))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        UserRecord? user = await catalog.TryGetUserAsync(session.User).ConfigureAwait(false);
        if (user is null || user.CredentialEpoch != session.CredentialEpoch)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        IReadOnlyList<GrantRecord> grants = await catalog.ListGrantsAsync(session.User).ConfigureAwait(false);
        Principal principal = new(session.User, user.IsSuperuser, grants);

        CachePrincipal(tokenId, principal, session.SecretMac, session.ExpiresAt);
        return principal;
    }

    /// <summary>
    /// Revokes the presented token's session (logout). The <b>full</b> token is validated first — a
    /// token id with a wrong/absent secret does not delete anyone's session (that would be a DoS by id
    /// alone). Malformed tokens and already-absent sessions are idempotent no-ops.
    /// </summary>
    public async Task LogoutAsync(string? bearer)
    {
        if (!TokenCodec.TryParse(bearer, out string tokenId, out string secret))
            return;

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        SessionRecord? session = await catalog.TryGetSessionAsync(tokenId).ConfigureAwait(false);
        if (session is null)
            return;

        byte[] presentedMac = TokenCodec.ComputeMac(CamusDBConfig.AccessTokenServerKey, tokenId, secret);
        if (!TokenCodec.MacEquals(presentedMac, session.SecretMac))
            return; // wrong secret — not authorized to revoke this session

        principalCache.TryRemove(tokenId, out _);
        await catalog.DeleteSessionAsync(tokenId).ConfigureAwait(false);
    }

    private void CachePrincipal(string tokenId, Principal principal, byte[] secretMac, DateTime sessionExpiresAt)
    {
        DateTime now = DateTime.UtcNow;
        if (principalCache.Count >= CamusDBConfig.AuthenticationCacheMaxEntries)
        {
            foreach (KeyValuePair<string, CachedSession> entry in principalCache)
            {
                if (entry.Value.Expires <= now)
                    principalCache.TryRemove(entry.Key, out _);
            }
            if (principalCache.Count >= CamusDBConfig.AuthenticationCacheMaxEntries)
                return; // still full — skip caching rather than grow unbounded
        }

        // Never trust the cache past the token's own absolute lifetime.
        DateTime expires = now.Add(CamusDBConfig.AuthenticationCacheTtl);
        if (sessionExpiresAt < expires)
            expires = sessionExpiresAt;

        principalCache[tokenId] = new CachedSession(principal, secretMac, expires);
    }

    private void RegisterAttempt(string account, string source)
    {
        DateTime now = DateTime.UtcNow;

        // Bound the limiter: purge expired windows when it grows, and fail closed (global admission
        // budget) if it is still full, so a flood of unique account/source keys cannot grow memory.
        if (loginAttempts.Count >= CamusDBConfig.LoginRateLimitMaxEntries)
        {
            foreach (KeyValuePair<string, (int Count, DateTime WindowStart)> entry in loginAttempts)
            {
                if (now - entry.Value.WindowStart > RateWindow)
                    loginAttempts.TryRemove(entry.Key, out _);
            }
            if (loginAttempts.Count >= CamusDBConfig.LoginRateLimitMaxEntries)
                throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Authentication is saturated; retry later");
        }

        string key = $"{account}\n{source}";
        (int Count, DateTime WindowStart) updated = loginAttempts.AddOrUpdate(
            key,
            _ => (1, now),
            (_, existing) => now - existing.WindowStart > RateWindow ? (1, now) : (existing.Count + 1, existing.WindowStart));

        if (updated.Count > CamusDBConfig.LoginMaxAttemptsPerMinute)
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Too many authentication attempts; retry later");
    }
}
