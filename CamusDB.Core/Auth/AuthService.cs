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
/// authorization snapshot bounded by <see cref="CamusDBOptions.AuthenticationCacheTtl"/>, so a revoke on
/// another node takes effect within that window. A cache hit still re-checks the token's secret HMAC on
/// every request, so the cache never authenticates a token by id alone. Login is rate-limited per source
/// and per (account, source), and the number of concurrent KDF operations is capped, so a login flood
/// cannot exhaust CPU or memory — see <see cref="RegisterAttempt"/> for why one ceiling is not
/// enough.</para>
/// </summary>
public sealed class AuthService
{
    private readonly Task<AuthCatalog> catalogTask;
    private readonly SemaphoreSlim kdfConcurrency;

    // tokenId -> cached validation. The SecretMac is re-verified on every hit (constant-time), so a
    // warm cache never lets `camus_<id>.<wrong-secret>` authenticate. Bounded by
    // AuthenticationCacheMaxEntries; expiry is capped by the session's own absolute lifetime.
    private readonly ConcurrentDictionary<string, CachedSession> principalCache = new(StringComparer.Ordinal);

    // "account\nsource" -> (attempts, window start). Fixed one-minute window; bounded and self-purging.
    // Its key carries an attacker-chosen account name, so this map alone cannot bound a flood — see
    // RegisterAttempt for why the per-source map below is the one that does.
    private readonly ConcurrentDictionary<string, (int Count, DateTime WindowStart)> loginAttempts = new(StringComparer.Ordinal);

    // source -> (attempts, window start), counted across every account that source names. The key is
    // not chosen by the request, so one caller occupies exactly one entry here no matter what it sends.
    private readonly ConcurrentDictionary<string, (int Count, DateTime WindowStart)> sourceAttempts = new(StringComparer.Ordinal);

    // A fixed verifier used to spend comparable work on an unknown/passwordless user so a failed login
    // takes about as long whether or not the account exists (reduces enumeration by timing).
    // Per-instance because its cost must match this engine's configured iteration count — a dummy
    // that hashes with different work would reintroduce the timing signal it exists to remove.
    private readonly Lazy<Credential> DummyCredential;

    private static readonly TimeSpan RateWindow = TimeSpan.FromMinutes(1);

    /// <summary>
    /// One cached authorization decision, plus everything needed to tell whether it is still the right
    /// one without going back to storage.
    ///
    /// <para><see cref="Generation"/> is the catalog coherence generation the principal was built
    /// under. <see cref="CredentialEpoch"/> and <see cref="AuthorizationEpoch"/> are the account's two
    /// epochs at that moment. Together they answer the question a hit has to answer — "has anything
    /// that produced this changed?" — with two integer comparisons and no storage access. See
    /// <see cref="ResolvePrincipalAsync"/> for how the two checks divide the work.</para>
    /// </summary>
    private readonly record struct CachedSession(
        Principal Principal,
        byte[] SecretMac,
        DateTime Expires,
        long Generation,
        long CredentialEpoch,
        long AuthorizationEpoch);

    /// <summary>Configuration for the engine this service serves; injected, never ambient.</summary>
    private readonly CamusDBOptions options;

    public AuthService(Task<AuthCatalog> catalogTask, CamusDBOptions options)
    {
        this.catalogTask = catalogTask;
        this.options = options;

        // Sized from the injected configuration rather than a field initializer, which cannot see it.
        kdfConcurrency = new SemaphoreSlim(options.LoginKdfMaxConcurrency, options.LoginKdfMaxConcurrency);
        DummyCredential = new Lazy<Credential>(() => PasswordHasher.Hash("camusdb-dummy-password", options.PasswordHashIterations));
    }

    private static string Normalize(string name) => name.ToLowerInvariant();

    /// <summary>
    /// Verifies credentials and, on success, issues a short-lived opaque bearer token whose session is
    /// persisted (storing only the token's HMAC). Failure — unknown user, no password, or wrong
    /// password — throws <see cref="CamusDBErrorCodes.AuthenticationFailed"/> with a uniform shape after
    /// spending comparable work. Rate-limited per <paramref name="source"/> and per
    /// (account, <paramref name="source"/>); a login flood surfaces as
    /// <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/>.
    ///
    /// <para>Returns the session's absolute expiry alongside the token: it is the same deadline
    /// <see cref="ResolvePrincipalAsync"/> later enforces, so a client can renew ahead of it instead of
    /// guessing a lifetime or waiting for a request to fail.</para>
    /// </summary>
    public async Task<LoginResult> LoginAsync(string user, string password, string source = "")
    {
        // Startup already refuses an absent or weak key (see AuthSecretPolicy), so reaching this with
        // an empty one means the engine was built without that path — a test harness, or a host that
        // skipped bootstrap. Kept as a guard rather than removed: minting a token under an empty HMAC
        // key would produce a forgeable session, and failing here is cheaper than discovering that.
        if (string.IsNullOrEmpty(options.AccessTokenServerKey))
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
        DateTime expiresAt = now.Add(options.AccessTokenTtl);
        SessionRecord session = new()
        {
            TokenId = token.TokenId,
            User = normalized,
            SecretMac = TokenCodec.ComputeMac(options.AccessTokenServerKey, token.TokenId, token.Secret),
            CredentialEpoch = record.CredentialEpoch,
            AuthorizationEpoch = record.AuthorizationEpoch,
            IssuedAt = now,
            ExpiresAt = expiresAt,
        };
        await catalog.CreateSessionAsync(session).ConfigureAwait(false);

        return new LoginResult(token.Bearer, expiresAt);
    }

    /// <summary>
    /// Resolves a bearer token to a <see cref="Principal"/>, or throws
    /// <see cref="CamusDBErrorCodes.AuthenticationFailed"/>. A cache hit still recomputes and
    /// constant-time-compares the token's secret HMAC before trusting it — the cache never authenticates
    /// by token id alone. On a miss it validates the HMAC, expiry/revocation, and that the session's
    /// captured credential epoch still matches the user's, then caches the resolved principal bounded by
    /// the lesser of the session's absolute expiry and the cache TTL.
    ///
    /// <para><b>A hit is also checked against the catalog, in two steps, without touching storage.</b>
    /// First the catalog's coherence generation: unchanged means nothing in the catalog has moved since
    /// the entry was built, and this is the hot path — one volatile read of a local field. Second, when
    /// the generation has moved, the account's own two epochs: a grant or revoke advances its
    /// authorization epoch and a password change advances its credential epoch, so an account whose
    /// epochs are both unchanged is unaffected by whatever moved and its entry is restamped rather than
    /// rebuilt. Anything else falls through to the full path.</para>
    ///
    /// <para>That pairing is why an authorization change applies on the very next request on this node
    /// rather than when the cache entry happens to expire, and why one <c>GRANT</c> does not force every
    /// session on the node to re-read its own. It is sound only because the catalog publishes its
    /// generation after the maps that generation describes.</para>
    /// </summary>
    public async Task<Principal> ResolvePrincipalAsync(string? bearer)
    {
        if (!TokenCodec.TryParse(bearer, out string tokenId, out string secret))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        byte[] presentedMac = TokenCodec.ComputeMac(options.AccessTokenServerKey, tokenId, secret);

        // Cache hit is trusted ONLY when the presented secret still matches — a wrong secret with a
        // known id falls through to full validation (which also fails).
        if (principalCache.TryGetValue(tokenId, out CachedSession cached)
            && cached.Expires > DateTime.UtcNow
            && TokenCodec.MacEquals(presentedMac, cached.SecretMac)
            && TryRevalidateAgainstCatalog(tokenId, cached))
        {
            return cached.Principal;
        }

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);

        // Read the stamp BEFORE the records it will describe. A mutation that lands during the reads
        // below then leaves the entry stamped behind its own data, which costs one extra revalidation
        // and is always safe. Stamping afterwards would do the opposite — mark data read before a
        // change as belonging to the generation after it — and the next cache hit would serve it.
        long generationAtRead = catalog.LocalGeneration;

        SessionRecord? session = await catalog.TryGetSessionAsync(tokenId).ConfigureAwait(false);
        if (session is null || session.Revoked || session.ExpiresAt <= DateTime.UtcNow)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        if (!TokenCodec.MacEquals(presentedMac, session.SecretMac))
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        UserRecord? user = await catalog.TryGetUserAsync(session.User).ConfigureAwait(false);
        if (user is null || user.CredentialEpoch != session.CredentialEpoch)
            throw new CamusDBException(CamusDBErrorCodes.AuthenticationFailed, "Authentication failed");

        IReadOnlyList<GrantRecord> grants = await catalog.ListGrantsAsync(session.User).ConfigureAwait(false);
        Principal principal = new(session.User, user.IsSuperuser, grants, user.Id);

        CachePrincipal(tokenId, principal, session.SecretMac, session.ExpiresAt, generationAtRead, user);
        return principal;
    }

    /// <summary>
    /// Decides whether a cache hit may still be served, without a storage read and without awaiting.
    ///
    /// <para>Returns true when the catalog has not moved since the entry was built, and also when it
    /// has moved but this account's credential and authorization epochs are both unchanged — nothing
    /// that produced this principal is different, so the entry is restamped to the current generation
    /// and kept. Returns false when the account's epochs moved, when its record is gone, or when the
    /// catalog is not open yet, all of which the caller answers by resolving from storage.</para>
    ///
    /// <para>Comparing the epochs against the cached maps is correct precisely because the generation
    /// being compared is the generation those maps stand at — the catalog publishes the two in that
    /// order. Reading them the other way round would compare an account's state at one generation with
    /// a stamp from another.</para>
    /// </summary>
    private bool TryRevalidateAgainstCatalog(string tokenId, CachedSession cached)
    {
        if (!catalogTask.IsCompletedSuccessfully)
            return false;

        AuthCatalog catalog = catalogTask.Result;

        long generation = catalog.LocalGeneration;
        if (generation == cached.Generation)
            return true;

        UserRecord? user = catalog.TryGetUserCached(cached.Principal.UserName);
        if (user is null
            || user.CredentialEpoch != cached.CredentialEpoch
            || user.AuthorizationEpoch != cached.AuthorizationEpoch)
        {
            return false;
        }

        // TryUpdate, not an assignment: the entry is rewritten only while it is still exactly the one
        // read above. A flush that cleared the cache between the read and here would otherwise see a
        // pre-flush decision written straight back into it.
        principalCache.TryUpdate(tokenId, cached with { Generation = generation }, cached);
        return true;
    }

    /// <summary>
    /// The coherence generation of this engine's authentication catalog, or zero while the catalog is
    /// still opening.
    ///
    /// <para>Published for a transport that holds a resolved principal for longer than one request — a
    /// long-lived duplex stream — so it can tell in constant time that the catalog moved and re-resolve,
    /// instead of serving an authorization snapshot taken when the stream opened.</para>
    /// </summary>
    public long AuthorizationGeneration =>
        catalogTask.IsCompletedSuccessfully ? catalogTask.Result.LocalGeneration : 0;

    /// <summary>
    /// Drops every cached authorization decision on this node, so the next request for each token is
    /// resolved from the catalog.
    ///
    /// <para>The flush statements need this as a separate step: a flush changes no account's epochs, so
    /// the per-account revalidation in <see cref="TryRevalidateAgainstCatalog"/> would restamp every
    /// entry and keep it. That narrowing is right for an ordinary grant and wrong for an operator
    /// saying "drop what you think you know".</para>
    /// </summary>
    public void DropPrincipalCache() => principalCache.Clear();

    /// <summary>Looks up a user record by name, or null when no such user exists.</summary>
    public async Task<UserRecord?> TryGetUserAsync(string name)
    {
        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);
        return await catalog.TryGetUserAsync(name).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the principal a stored object's <b>owner</b> stands for, or null when that owner can no
    /// longer be established.
    /// </summary>
    /// <remarks>
    /// Verifies the immutable id as well as the name, which is the whole point: a user can be dropped
    /// and a new one created under the same name, and resolving by name alone would silently hand the
    /// old owner's authority to the new account. A mismatch, a missing user, or an owner recorded
    /// before user ids existed all return null so the caller fails closed rather than falling back to
    /// something weaker.
    /// </remarks>
    public async Task<Principal?> TryLoadOwnerPrincipalAsync(string ownerName, string? ownerId)
    {
        if (string.IsNullOrEmpty(ownerName) || string.IsNullOrEmpty(ownerId))
            return null;

        AuthCatalog catalog = await catalogTask.ConfigureAwait(false);

        UserRecord? user = await catalog.TryGetUserAsync(ownerName).ConfigureAwait(false);
        if (user is null || !string.Equals(user.Id, ownerId, StringComparison.Ordinal))
            return null;

        IReadOnlyList<GrantRecord> grants = await catalog.ListGrantsAsync(ownerName).ConfigureAwait(false);
        return new Principal(user.Name, user.IsSuperuser, grants, user.Id);
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

        byte[] presentedMac = TokenCodec.ComputeMac(options.AccessTokenServerKey, tokenId, secret);
        if (!TokenCodec.MacEquals(presentedMac, session.SecretMac))
            return; // wrong secret — not authorized to revoke this session

        principalCache.TryRemove(tokenId, out _);
        await catalog.DeleteSessionAsync(tokenId).ConfigureAwait(false);
    }

    /// <summary>
    /// Stores one resolved authorization decision, stamped with the catalog generation its records were
    /// read under and with the account's two epochs, so <see cref="TryRevalidateAgainstCatalog"/> can
    /// judge a later hit without touching storage.
    /// </summary>
    /// <param name="generationAtRead">
    /// The generation observed <b>before</b> the records were read — see the comment at the read site.
    /// A stamp behind its own data is safe; a stamp ahead of it is not.
    /// </param>
    private void CachePrincipal(
        string tokenId,
        Principal principal,
        byte[] secretMac,
        DateTime sessionExpiresAt,
        long generationAtRead,
        UserRecord user)
    {
        DateTime now = DateTime.UtcNow;
        if (principalCache.Count >= options.AuthenticationCacheMaxEntries)
        {
            foreach (KeyValuePair<string, CachedSession> entry in principalCache)
            {
                if (entry.Value.Expires <= now)
                    principalCache.TryRemove(entry.Key, out _);
            }
            if (principalCache.Count >= options.AuthenticationCacheMaxEntries)
                return; // still full — skip caching rather than grow unbounded
        }

        // Never trust the cache past the token's own absolute lifetime.
        DateTime expires = now.Add(options.AuthenticationCacheTtl);
        if (sessionExpiresAt < expires)
            expires = sessionExpiresAt;

        principalCache[tokenId] = new CachedSession(
            principal, secretMac, expires, generationAtRead, user.CredentialEpoch, user.AuthorizationEpoch);
    }

    /// <summary>
    /// Counts one login attempt against two rolling-minute ceilings, and throws
    /// <see cref="CamusDBErrorCodes.TooManyAuthAttempts"/> when either is exceeded.
    ///
    /// <para><b>Why there are two ceilings, not one.</b> The per-(account, source) counter is the
    /// brute-force stop, but its key contains an account name the caller chooses freely. A caller that
    /// varies the name lands on a fresh key every time, so it never reaches that ceiling and instead
    /// inserts one map entry per attempt. The per-source counter closes that: its key is the connection's
    /// own address, which the request cannot pick, so a flood from one place occupies one entry and trips
    /// one ceiling however many accounts it names.</para>
    ///
    /// <para><b>The two maps must fail differently when full.</b> Filling the per-source map takes that
    /// many distinct addresses, which is an attack in itself, so it fails closed. The per-(account,
    /// source) map is filled by naming accounts, so failing closed there would let one caller refuse
    /// logins for every user on the node; it stops recording new keys instead and leaves admission to
    /// the per-source ceiling, which has already counted the attempt.</para>
    ///
    /// <para>Both counters advance on success as well as failure, so a valid credential replayed in a
    /// loop is bounded too.</para>
    /// </summary>
    private void RegisterAttempt(string account, string source)
    {
        DateTime now = DateTime.UtcNow;

        // The per-source ceiling first: it is the one that holds when the account name varies, and it
        // must count the attempt even if the finer counter below is skipped.
        if (!TryPurgeToCapacity(sourceAttempts, now))
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Authentication is saturated; retry later");

        if (Count(sourceAttempts, source, now) > options.LoginMaxAttemptsPerSourcePerMinute)
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Too many authentication attempts; retry later");

        // A full per-(account, source) map sheds keys it is not already tracking, rather than refusing
        // the request. The attempt is counted above either way, so nothing goes unbounded — only the
        // finer attribution is lost, and only for accounts first seen while a flood is in progress. A
        // key already in the map keeps counting, so an attack cannot mask a brute-force attempt against
        // an account that was being tracked before it started.
        string key = $"{account}\n{source}";
        if (!TryPurgeToCapacity(loginAttempts, now) && !loginAttempts.ContainsKey(key))
            return;

        if (Count(loginAttempts, key, now) > options.LoginMaxAttemptsPerMinute)
            throw new CamusDBException(CamusDBErrorCodes.TooManyAuthAttempts, "Too many authentication attempts; retry later");
    }

    /// <summary>
    /// Advances <paramref name="key"/>'s counter within the current window and returns the new count.
    /// A window older than <see cref="RateWindow"/> restarts at one.
    /// </summary>
    private static int Count(
        ConcurrentDictionary<string, (int Count, DateTime WindowStart)> map, string key, DateTime now)
    {
        return map.AddOrUpdate(
            key,
            _ => (1, now),
            (_, existing) => now - existing.WindowStart > RateWindow ? (1, now) : (existing.Count + 1, existing.WindowStart)).Count;
    }

    /// <summary>
    /// Drops entries whose window has closed once <paramref name="map"/> reaches its configured size,
    /// and reports whether there is room for a new key. False means the purge could not free any, which
    /// each caller answers differently — see <see cref="RegisterAttempt"/>.
    /// </summary>
    private bool TryPurgeToCapacity(
        ConcurrentDictionary<string, (int Count, DateTime WindowStart)> map, DateTime now)
    {
        if (map.Count < options.LoginRateLimitMaxEntries)
            return true;

        foreach (KeyValuePair<string, (int Count, DateTime WindowStart)> entry in map)
        {
            if (now - entry.Value.WindowStart > RateWindow)
                map.TryRemove(entry.Key, out _);
        }

        return map.Count < options.LoginRateLimitMaxEntries;
    }
}
