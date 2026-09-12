/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Time;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Persistent, server-level catalog of database users and their privilege grants.
///
/// <para>Modeled directly on <see cref="DatabaseRegistry"/>: it lives in a reserved <c>_system/auth/</c>
/// key prefix in the single process-level shared Kahuna node, serializes mutations under one
/// <see cref="SemaphoreSlim"/>, serves reads from an in-memory cache rebuilt on load, and advances a
/// Raft-replicated generation stamp after each mutation so other cluster nodes reconcile their caches.
/// It is cross-database — users and grants are not scoped to any one database.</para>
///
/// <para>Every mutation persists inside a <see cref="KvTransaction"/> so acquired locks and modified
/// keys are tracked for commit/rollback; it never calls a raw Kahuna write. Passwords are stored only
/// as salted, iterated verifiers (see <see cref="Auth.PasswordHasher"/>); the cleartext never reaches
/// this class as stored state.</para>
/// </summary>
public sealed class AuthCatalog
{
    private readonly IKahuna kahuna;
    
    private readonly KvTransactionsManager transactions;
    
    private readonly string keyPrefix;
    
    private readonly bool isClusterMode;

    private readonly SemaphoreSlim writeSem = new(1, 1);

    // Normalized user name -> record.
    private readonly ConcurrentDictionary<string, UserRecord> usersByName = new(StringComparer.Ordinal);

    // Normalized user name -> (scopeKey -> grant). Rebuilt from KV on load; mutated under writeSem.
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, GrantRecord>> grantsByUser =
        new(StringComparer.Ordinal);

    // Cross-node cache-coherence stamp — see DatabaseRegistry for the full rationale. A cache hit is
    // trusted while loadedGeneration matches the authoritative sequence; a mutation bumps it. In
    // standalone mode this process owns the only instance, so the generation read is skipped.
    private long loadedGeneration;

    private const int MaxRetries = 10;

    private static string Normalize(string name) => name.ToLowerInvariant();

    // Kahuna routes every key to the keyspace named by the substring before its LAST '/'
    // (KeySpaceRegistry). So every auth key must be "{AuthBucket}/{leaf}" with NO '/' in the leaf, or
    // it lands in a different bucket and a scan of AuthBucket misses it. The grant leaf therefore joins
    // user and scope with ':' (both are '/'-free), never '/'.
    /// <summary>KV routing bucket that holds every auth key.</summary>
    public string AuthBucket => $"{keyPrefix}auth";
    
    private string UserKeyPrefix => $"{keyPrefix}auth/user:";
    
    private string UserKey(string normalizedName) => $"{keyPrefix}auth/user:{normalizedName}";
    
    private string GrantKeyPrefix => $"{keyPrefix}auth/grant:";
    
    private string GrantKey(string normalizedUser, string scopeKey) => $"{keyPrefix}auth/grant:{normalizedUser}:{scopeKey}";
    
    private string SessionKeyPrefix => $"{keyPrefix}auth/session:";
    
    private string SessionKey(string tokenId) => $"{keyPrefix}auth/session:{tokenId}";
    
    private string GenerationKey => $"{keyPrefix}auth/generation";

    private AuthCatalog(IKahuna kahuna, KvTransactionsManager transactions, string keyPrefix, bool isClusterMode)
    {
        this.kahuna = kahuna;
        this.transactions = transactions;
        this.keyPrefix = keyPrefix;
        this.isClusterMode = isClusterMode;
    }

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    /// <summary>
    /// Opens (or creates) the auth catalog against the process-level shared Kahuna node, loading every
    /// user and grant into the in-memory cache. Waits for the shared node to elect partition leaders
    /// before scanning, exactly like <see cref="DatabaseRegistry.OpenAsync"/>, so the eager open during
    /// executor construction cannot race a not-yet-created partition.
    /// </summary>
    public static async Task<AuthCatalog> OpenAsync(EmbeddedKahuna sharedNode, CamusDBOptions options, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        HLCTimestamp MintLocalT(HLCTimestamp? floor)
        {
            if (floor.HasValue && !floor.Value.IsNull()) 
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        }

        KvTransactionsManager txManager = new(sharedNode.Kahuna, options, MintLocalT);
        AuthCatalog catalog = new(sharedNode.Kahuna, txManager, "_system/", isClusterMode);

        await sharedNode.WaitUntilStartedAsync().ConfigureAwait(false);
        await catalog.LoadAsync().ConfigureAwait(false);
        return catalog;
    }

    // -----------------------------------------------------------------------
    // Startup load
    // -----------------------------------------------------------------------

    /// <summary>
    /// Loads the auth catalog into memory at open time, waiting out a cluster that is still
    /// assembling. See <see cref="StartupLoadRetry"/> for why the budget, rather than the exception
    /// type, decides what is worth retrying.
    /// </summary>
    private async Task LoadAsync()
    {
        Stopwatch sw = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                await LoadOnceAsync().ConfigureAwait(false);
                Volatile.Write(ref loadedGeneration, await ReadGenerationAsync().ConfigureAwait(false));
                return;
            }
            catch (Exception ex) when (StartupLoadRetry.ShouldRetry(ex, sw.ElapsedMilliseconds))
            {
                await Task.Delay(StartupLoadRetry.RetryDelayMs).ConfigureAwait(false);
            }
        }
    }

    private async Task LoadOnceAsync()
    {
        (Dictionary<string, UserRecord> users, Dictionary<string, Dictionary<string, GrantRecord>> grants) =
            await ScanAuthKeyspaceAsync().ConfigureAwait(false);

        ReconcileFromScan(users, grants);
    }

    /// <summary>
    /// Reads the whole authentication keyspace once and returns it as fresh maps. The caller decides
    /// what to do with them; nothing here touches the live caches.
    ///
    /// <para><b>It returns a complete picture or it raises.</b> <c>LocateAndScanRange</c> fails the
    /// scan — it does not end the stream — when a page answers anything but a successful read, when a
    /// continuation cursor cannot be decoded, and when a page stays transient for its whole retry
    /// budget. So an exhausted enumeration here means the range was read in full. That is what lets a
    /// listing statement report this catalog without asking whether it saw everything, and it is why
    /// the maps are built fresh: a scan that raises leaves the previous, complete caches in place
    /// rather than a half-filled one.</para>
    /// </summary>
    private async Task<(Dictionary<string, UserRecord> Users, Dictionary<string, Dictionary<string, GrantRecord>> Grants)>
        ScanAuthKeyspaceAsync()
    {
        Dictionary<string, UserRecord> users = new(StringComparer.Ordinal);
        Dictionary<string, Dictionary<string, GrantRecord>> grants = new(StringComparer.Ordinal);

        KvTransaction tx = KvTransaction.CreateReadOnly();

        ConfiguredCancelableAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> cursor = kahuna.LocateAndScanRange(
            tx.TransactionId,
            AuthBucket,
            null,
            true,
            null,
            true,
            1000,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None
        ).ConfigureAwait(false);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in cursor)
        {
            if (entry.Value is null)
                continue;

            if (key.StartsWith(UserKeyPrefix, StringComparison.Ordinal))
            {
                UserRecord user = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.UserRecord);
                users[Normalize(user.Name)] = user;
            }
            else if (key.StartsWith(GrantKeyPrefix, StringComparison.Ordinal))
            {
                GrantRecord grant = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.GrantRecord);
                string normalizedUser = Normalize(grant.User);

                if (!grants.TryGetValue(normalizedUser, out Dictionary<string, GrantRecord>? map))
                    grants[normalizedUser] = map = new Dictionary<string, GrantRecord>(StringComparer.Ordinal);

                map[grant.Scope.ScopeKey()] = grant;
            }
        }

        DropGrantsNamingARetiredUser(users, grants);
        return (users, grants);
    }

    /// <summary>
    /// Brings the live caches to the state <paramref name="users"/> and <paramref name="grants"/>
    /// describe: present keys are upserted, vanished ones removed. The caches are never cleared first,
    /// so a lock-free reader never observes a transiently empty catalog.
    /// </summary>
    /// <param name="generation">
    /// The generation the scan was taken under, published <b>after</b> the maps it describes. Null
    /// leaves the generation alone, for the startup load that publishes it separately.
    /// </param>
    /// <remarks>
    /// The publication order is load-bearing and every mutation in this class follows it too: update
    /// the maps, then move the generation. A resolved-principal cache elsewhere in the process treats
    /// a moved generation as "the maps have changed, re-read them", so a generation that became
    /// visible ahead of its own maps would let a stale authorization snapshot certify itself as
    /// current.
    /// </remarks>
    private void ReconcileFromScan(
        Dictionary<string, UserRecord> users,
        Dictionary<string, Dictionary<string, GrantRecord>> grants,
        long? generation = null)
    {
        foreach ((string normalized, UserRecord user) in users)
            usersByName[normalized] = user;

        foreach ((string normalizedUser, Dictionary<string, GrantRecord> scanned) in grants)
        {
            ConcurrentDictionary<string, GrantRecord> live = GetOrCreateGrantMap(normalizedUser);

            foreach ((string scopeKey, GrantRecord grant) in scanned)
                live[scopeKey] = grant;
        }

        foreach (string normalized in usersByName.Keys.ToList())
        {
            if (!users.ContainsKey(normalized))
            {
                usersByName.TryRemove(normalized, out _);
                grantsByUser.TryRemove(normalized, out _);
            }
        }

        foreach ((string normalizedUser, ConcurrentDictionary<string, GrantRecord> live) in grantsByUser)
        {
            grants.TryGetValue(normalizedUser, out Dictionary<string, GrantRecord>? scanned);

            foreach (string scopeKey in live.Keys.ToList())
            {
                if (scanned is null || !scanned.ContainsKey(scopeKey))
                    live.TryRemove(scopeKey, out _);
            }
        }

        if (generation.HasValue)
            Volatile.Write(ref loadedGeneration, generation.Value);
    }

    /// <summary>
    /// Removes every scanned grant that names a user id other than the one the live record carries.
    /// Such a grant belonged to an earlier account that held this name and outlived its own
    /// <c>DROP USER</c>; adopting it would hand the current account privileges nobody granted it.
    ///
    /// <para>Runs as a pass after the scan rather than inside it, because a grant key sorts before
    /// the user key it refers to, so the owning record is not yet known when the grant is read.</para>
    ///
    /// <para>A grant with no recorded id predates the binding and is kept — see
    /// <see cref="GrantRecord.UserId"/>. A grant whose user has no record at all is also kept: the
    /// user map is what decides whether an account exists, and it is rebuilt by this same scan.</para>
    /// </summary>
    private static void DropGrantsNamingARetiredUser(
        Dictionary<string, UserRecord> users,
        Dictionary<string, Dictionary<string, GrantRecord>> grants)
    {
        foreach ((string normalizedUser, Dictionary<string, GrantRecord> map) in grants)
        {
            if (!users.TryGetValue(normalizedUser, out UserRecord? live) || live.Id is null)
                continue;

            foreach ((string scopeKey, GrantRecord grant) in map.ToList())
            {
                if (grant.UserId is not null && !string.Equals(grant.UserId, live.Id, StringComparison.Ordinal))
                    map.Remove(scopeKey);
            }
        }
    }

    private ConcurrentDictionary<string, GrantRecord> GetOrCreateGrantMap(string normalizedUser) =>
        grantsByUser.GetOrAdd(normalizedUser, static _ => new ConcurrentDictionary<string, GrantRecord>(StringComparer.Ordinal));

    // -----------------------------------------------------------------------
    // Cross-node cache-coherence generation stamp (mirrors DatabaseRegistry)
    // -----------------------------------------------------------------------

    // The coherence generation is a durable KV key incremented WITHIN each mutation's transaction, so it
    // commits atomically with the change. This is the crash-safety fix: a separate post-commit sequence
    // bump could be lost to a crash/leader-loss between the two, leaving other nodes permanently unaware
    // of a committed change (an unbounded stale window). Incrementing in-transaction closes that gap —
    // if the mutation is durable, so is the generation move.

    private async Task<long> ReadGenerationAsync()
    {
        byte[]? value = await GetAuthValueAsync(GenerationKey).ConfigureAwait(false);
        return value is { Length: 8 } ? BitConverter.ToInt64(value) : 0;
    }

    /// <summary>Reads the generation under its lock and writes the next value in the same transaction,
    /// returning the new value. Must be called inside a mutation's transaction so it commits atomically
    /// with the records changed.</summary>
    private async Task<long> IncrementGenerationLockedAsync(KvTransaction tx)
    {
        byte[]? current = await LockAndReadAsync(tx, GenerationKey).ConfigureAwait(false);
        long next = (current is { Length: 8 } ? BitConverter.ToInt64(current) : 0) + 1;
        await SetKeyLockedAsync(tx, GenerationKey, BitConverter.GetBytes(next), ifAbsent: false).ConfigureAwait(false);
        return next;
    }

    /// <summary>
    /// Publishes a generation this node has already applied to its caches, never moving it backwards.
    ///
    /// <para><b>Call it after the maps it describes, never before.</b> A resolved-principal cache
    /// elsewhere in the process reads <see cref="LocalGeneration"/> to decide whether the maps have
    /// changed under it, and then re-reads the maps. A generation published ahead of its own maps would
    /// send that check to the old data and let a stale authorization snapshot certify itself as
    /// current. Every mutation in this class is written in that order for this reason.</para>
    /// </summary>
    private void AdoptGeneration(long generation)
    {
        long current = Volatile.Read(ref loadedGeneration);
        if (generation > current)
            Volatile.Write(ref loadedGeneration, generation);
    }

    /// <summary>
    /// In cluster mode, revalidates the caches against KV when the authoritative generation has moved
    /// past the loaded one. Must be called (via <see cref="EnsureCoherentAsync"/>) at the start of a
    /// read or mutation that needs to see other nodes' writes. Serialized under <see cref="writeSem"/>;
    /// callers that already hold it use <see cref="RevalidateFromKvLockedAsync"/>.
    /// </summary>
    private async Task EnsureCoherentAsync()
    {
        if (!isClusterMode)
            return;

        long authoritative = await ReadGenerationAsync().ConfigureAwait(false);
        if (Volatile.Read(ref loadedGeneration) >= authoritative)
            return;

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            await RevalidateFromKvLockedAsync(authoritative).ConfigureAwait(false);
        }
        finally
        {
            writeSem.Release();
        }
    }

    private async Task RevalidateFromKvLockedAsync(long authoritativeGeneration)
    {
        if (Volatile.Read(ref loadedGeneration) >= authoritativeGeneration)
            return; // another hit already reloaded to at least this generation

        (Dictionary<string, UserRecord> users, Dictionary<string, Dictionary<string, GrantRecord>> grants) =
            await ScanAuthKeyspaceAsync().ConfigureAwait(false);

        ReconcileFromScan(users, grants, authoritativeGeneration);
    }

    // -----------------------------------------------------------------------
    // Read-only queries
    // -----------------------------------------------------------------------

    /// <summary>Returns the user record for <paramref name="name"/>, or null. Cache read; in cluster
    /// mode reconciles first so another node's just-created user is visible.</summary>
    public async Task<UserRecord?> TryGetUserAsync(string name)
    {
        await EnsureCoherentAsync().ConfigureAwait(false);
        return usersByName.GetValueOrDefault(Normalize(name));
    }

    /// <summary>Returns every grant for <paramref name="name"/> (empty if none / unknown user).</summary>
    public async Task<IReadOnlyList<GrantRecord>> ListGrantsAsync(string name)
    {
        await EnsureCoherentAsync().ConfigureAwait(false);

        return grantsByUser.TryGetValue(Normalize(name), out ConcurrentDictionary<string, GrantRecord>? map)
            ? [.. map.Values]
            : [];
    }

    /// <summary>
    /// The coherence generation this node's caches currently stand at. A plain read of a local field:
    /// no storage access, no lock, and nothing to await.
    ///
    /// <para>It exists so a component that caches something <em>derived</em> from this catalog — a
    /// resolved principal, say — can tell in constant time whether the catalog has moved underneath it,
    /// and re-read only then. A change made on this node advances it the moment the change is visible
    /// in the maps; a change made on another node advances it when this node next reconciles, which is
    /// on its next coherent read.</para>
    /// </summary>
    public long LocalGeneration => Volatile.Read(ref loadedGeneration);

    /// <summary>
    /// Reads a user straight from the cache, with no coherence reconciliation.
    ///
    /// <para>For revalidating something already derived from this catalog at a known generation, never
    /// for deciding whether an account exists. The record it returns belongs to
    /// <see cref="LocalGeneration"/>, and a caller that compared it against a generation it read
    /// earlier is comparing like with like. Use <see cref="TryGetUserAsync"/> for everything
    /// else.</para>
    /// </summary>
    public UserRecord? TryGetUserCached(string name) => usersByName.GetValueOrDefault(Normalize(name));

    /// <summary>
    /// Takes a complete, ordered picture of the catalog from storage — not from the cache — and
    /// brings the cache to it.
    ///
    /// <para><b>The listing statements are built on this rather than on the cache because they must
    /// fail closed.</b> An operator inventorying accounts before a storage-revision upgrade would
    /// believe a short list, so the underlying scan raises rather than returning one; this method
    /// propagates that. A cache read could not make the same promise, because nothing tells it whether
    /// the load that filled it saw everything.</para>
    ///
    /// <para>The generation is read <b>before</b> the scan. A mutation that commits on another node in
    /// between therefore leaves the stamp behind the data rather than ahead of it, which costs one
    /// redundant reconciliation later and never certifies unseen data as current.</para>
    ///
    /// <para>Serialized under the write semaphore, so no local mutation interleaves with the scan.
    /// Listings are operator statements, so paying for that is cheaper than reasoning about a torn
    /// picture.</para>
    /// </summary>
    public async Task<AuthCatalogSnapshot> SnapshotAsync()
    {
        await writeSem.WaitAsync().ConfigureAwait(false);

        try
        {
            long generation = await ReadGenerationAsync().ConfigureAwait(false);

            (Dictionary<string, UserRecord> users, Dictionary<string, Dictionary<string, GrantRecord>> grants) =
                await ScanAuthKeyspaceAsync().ConfigureAwait(false);

            ReconcileFromScan(users, grants, generation);

            List<AuthCatalogEntry> entries = new(users.Count);

            foreach (string normalized in users.Keys.Order(StringComparer.Ordinal))
            {
                List<GrantRecord> ordered = [];

                if (grants.TryGetValue(normalized, out Dictionary<string, GrantRecord>? map))
                {
                    foreach (string scopeKey in map.Keys.Order(StringComparer.Ordinal))
                        ordered.Add(map[scopeKey]);
                }

                entries.Add(new AuthCatalogEntry(users[normalized], ordered));
            }

            return new AuthCatalogSnapshot(entries, generation);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Advances the coherence generation and reloads this node's caches from storage authoritatively,
    /// so an authorization change takes effect now instead of when a cached snapshot happens to expire.
    ///
    /// <para><b>The generation bump is how the flush reaches the other nodes.</b> That key is durable
    /// and replicated, so every node observes the move on its next coherent read and discards what it
    /// derived from the older one. There is no node-to-node call to add, and nothing here depends on
    /// which node the operator happened to run the statement on.</para>
    ///
    /// <para>It revokes nothing. Sessions are untouched, so a client whose grants did not change keeps
    /// working across it without logging in again — see <see cref="RevokeAllSessionsAsync"/> for the
    /// statement that does log everyone out.</para>
    /// </summary>
    /// <returns>The new generation.</returns>
    public async Task<long> FlushPrivilegesAsync()
    {
        await writeSem.WaitAsync().ConfigureAwait(false);

        try
        {
            long newGeneration = 0;

            await RunInTransactionAsync(async tx =>
            {
                newGeneration = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            (Dictionary<string, UserRecord> users, Dictionary<string, Dictionary<string, GrantRecord>> grants) =
                await ScanAuthKeyspaceAsync().ConfigureAwait(false);

            ReconcileFromScan(users, grants, newGeneration);
            return newGeneration;
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Deletes every stored login session, so every client must authenticate again, and advances the
    /// coherence generation once so nothing derived from a deleted session stays trusted.
    ///
    /// <para><b>A session this cannot delete fails the statement.</b> That is the opposite of
    /// <see cref="ReapExpiredSessionsAsync"/>, which leaves a contended record for its next sweep, and
    /// the difference is what each one promises. The reaper is housekeeping over records that already
    /// cannot authenticate anyone. This is an operator saying "end every session", and reporting
    /// success while one survives would leave them believing a revocation that did not happen. The
    /// delete is idempotent, so running it again after a failure is the correct and safe response.</para>
    ///
    /// <para>The generation still moves, and the deletes that did succeed still count, even when one
    /// fails — so the sessions that went are not left half-revoked while the caller handles the error.
    /// One transaction per key, so one contended record does not undo the rest.</para>
    ///
    /// <para>The scan is taken first and the deletes follow it, rather than deleting inside the
    /// iteration, because a locked delete against the range being scanned would contend with the
    /// scan's own read of it.</para>
    /// </summary>
    /// <returns>How many sessions were deleted.</returns>
    public async Task<int> RevokeAllSessionsAsync()
    {
        List<string> sessionKeys = [];

        ConfiguredCancelableAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> cursor = kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            AuthBucket,
            null,
            true,
            null,
            true,
            1000,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None
        ).ConfigureAwait(false);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in cursor)
        {
            if (entry.Value is not null && key.StartsWith(SessionKeyPrefix, StringComparison.Ordinal))
                sessionKeys.Add(key);
        }

        int revoked = 0;
        Exception? firstFailure = null;

        foreach (string key in sessionKeys)
        {
            try
            {
                await RunInTransactionAsync(tx => DeleteAuthKey(tx, key)).ConfigureAwait(false);
                revoked++;
            }
            catch (Exception ex)
            {
                // Kept, not rethrown here: the remaining sessions must still go, and the caller must
                // still learn that this one did not.
                firstFailure ??= ex;
            }
        }

        await writeSem.WaitAsync().ConfigureAwait(false);

        try
        {
            long newGeneration = 0;

            await RunInTransactionAsync(async tx =>
            {
                newGeneration = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            AdoptGeneration(newGeneration);
        }
        finally
        {
            writeSem.Release();
        }

        if (firstFailure is not null)
            throw firstFailure;

        return revoked;
    }

    // -----------------------------------------------------------------------
    // Mutations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Creates a user. Throws <see cref="CamusDBErrorCodes.UserAlreadyExists"/> if the name is taken,
    /// unless <paramref name="ifNotExists"/> (then a no-op). <paramref name="credential"/> is null for a
    /// passwordless user.
    /// </summary>
    public async Task CreateUserAsync(string name, Credential? credential, bool ifNotExists, bool isSuperuser = false)
    {
        string normalized = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (isClusterMode)
                await RevalidateFromKvLockedAsync(await ReadGenerationAsync().ConfigureAwait(false)).ConfigureAwait(false);

            if (usersByName.ContainsKey(normalized))
            {
                if (ifNotExists)
                    return;
                throw new CamusDBException(CamusDBErrorCodes.UserAlreadyExists, $"User '{name}' already exists");
            }

            UserRecord record = new()
            {
                // Allocated once and never reused, so anything that records "who owns this" survives
                // the name being dropped and claimed by somebody else.
                Id = ObjectIdGenerator.Generate().ToString(),
                Name = name,
                Credential = credential,
                CredentialEpoch = 0,
                AuthorizationEpoch = 0,
                IsSuperuser = isSuperuser,
                CreatedAt = DateTime.UtcNow,
            };

            byte[] bytes = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.UserRecord);

            long newGen = 0;
            
            await RunInTransactionAsync(async tx =>
            {
                bool written = await WriteAuthKey(tx, UserKey(normalized), bytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                    throw new CamusDBException(CamusDBErrorCodes.UserAlreadyExists, $"User '{name}' already exists");
                
                newGen = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            usersByName[normalized] = record;
            AdoptGeneration(newGen);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Replaces a user's password verifier and advances its credential epoch (invalidating any tokens
    /// in the enforcement phase). Throws <see cref="CamusDBErrorCodes.UserDoesNotExist"/> if unknown.
    /// </summary>
    public async Task SetPasswordAsync(string name, Credential credential)
    {
        ArgumentNullException.ThrowIfNull(credential);
        string normalized = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            UserRecord? updated = null;
            long newGen = 0;
            await RunInTransactionAsync(async tx =>
            {
                // Authoritative read under the user lock: a user dropped on another node is seen as
                // absent here and must not be recreated from a stale copy.
                byte[]? current = await LockAndReadAsync(tx, UserKey(normalized)).ConfigureAwait(false);
                if (current is null)
                    throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{name}' does not exist");

                UserRecord locked = MetaJsonSerializer.Deserialize(current, MetaJsonContext.Default.UserRecord);
                updated = locked.Copy();
                updated.Credential = credential;
                updated.CredentialEpoch = locked.CredentialEpoch + 1;

                byte[] bytes = MetaJsonSerializer.Serialize(updated, MetaJsonContext.Default.UserRecord);
                await SetKeyLockedAsync(tx, UserKey(normalized), bytes, ifAbsent: false).ConfigureAwait(false);
                newGen = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            usersByName[normalized] = updated!;
            AdoptGeneration(newGen);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Removes a user and every grant that references it in one transaction. With
    /// <paramref name="ifExists"/> an unknown user is a no-op; otherwise it throws
    /// <see cref="CamusDBErrorCodes.UserDoesNotExist"/>.
    /// </summary>
    public async Task DropUserAsync(string name, bool ifExists)
    {
        string normalized = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            bool dropped = false;
            long newGen = 0;
            await RunInTransactionAsync(async tx =>
            {
                // Authoritative existence check under the user lock.
                byte[]? current = await LockAndReadAsync(tx, UserKey(normalized)).ConfigureAwait(false);
                if (current is null)
                    return; // not present — decided below

                dropped = true;

                // Enumerate the user's grants authoritatively (not from the possibly-stale cache) so a
                // grant added on another node is not orphaned and inherited by a later same-name user.
                // Holding the user lock blocks concurrent GrantAsync (which also takes it), so the set
                // is stable.
                List<string> grantKeys = await ScanUserGrantKeysAsync(tx, normalized).ConfigureAwait(false);
                List<string> sessionKeys = await ScanUserSessionKeysAsync(tx, normalized).ConfigureAwait(false);

                await DeleteAuthKey(tx, UserKey(normalized)).ConfigureAwait(false);
                
                foreach (string grantKey in grantKeys)
                    await DeleteAuthKey(tx, grantKey).ConfigureAwait(false);
                
                foreach (string sessionKey in sessionKeys)
                    await DeleteAuthKey(tx, sessionKey).ConfigureAwait(false);

                newGen = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            if (!dropped)
            {
                if (ifExists)
                    return;
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{name}' does not exist");
            }

            usersByName.TryRemove(normalized, out _);
            grantsByUser.TryRemove(normalized, out _);
            AdoptGeneration(newGen);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Unions <paramref name="privileges"/> into the user's grant at <paramref name="scope"/> (additive
    /// and idempotent) and advances the user's authorization epoch in the same transaction. The user
    /// must exist. <paramref name="revoke"/> subtracts instead, deleting the grant record when its mask
    /// reaches <see cref="Privilege.None"/>.
    /// </summary>
    public async Task GrantAsync(string user, GrantScope scope, Privilege privileges, bool revoke)
    {
        ArgumentNullException.ThrowIfNull(scope);
        string normalized = Normalize(user);
        string scopeKey = scope.ScopeKey();

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            UserRecord? updatedUser = null;
            GrantRecord? updatedGrant = null;
            bool grantDeleted = false;
            bool noChange = false;
            long newGen = 0;

            await RunInTransactionAsync(async tx =>
            {
                // Fixed lock order (user key, then grant key) avoids reverse-order deadlock. Both records
                // are read UNDER their locks, so the new mask/epoch derive from authoritative values —
                // two concurrent grants cannot lose an update, and a dropped user is not resurrected.
                byte[]? userCurrent = await LockAndReadAsync(tx, UserKey(normalized)).ConfigureAwait(false);
                if (userCurrent is null)
                    throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{user}' does not exist");

                UserRecord lockedUser = MetaJsonSerializer.Deserialize(userCurrent, MetaJsonContext.Default.UserRecord);

                byte[]? grantCurrent = await LockAndReadAsync(tx, GrantKey(normalized, scopeKey)).ConfigureAwait(false);
                Privilege currentMask = grantCurrent is null
                    ? Privilege.None
                    : MetaJsonSerializer.Deserialize(grantCurrent, MetaJsonContext.Default.GrantRecord).Privileges;

                Privilege newMask = revoke ? currentMask & ~privileges : currentMask | privileges;
                if (newMask == currentMask)
                {
                    noChange = true;
                    return; // no observable change — nothing to persist
                }

                updatedUser = lockedUser.Copy();
                updatedUser.AuthorizationEpoch = lockedUser.AuthorizationEpoch + 1;
                
                await SetKeyLockedAsync(tx, 
                    UserKey(normalized),
                    MetaJsonSerializer.Serialize(updatedUser, MetaJsonContext.Default.UserRecord), 
                    ifAbsent: false
                ).ConfigureAwait(false);

                if (newMask == Privilege.None)
                {
                    await DeleteKeyLockedAsync(tx, GrantKey(normalized, scopeKey)).ConfigureAwait(false);
                    grantDeleted = true;
                }
                else
                {
                    // Bind the grant to the user record read under the lock above, not merely to the
                    // name in the key, so a later account that takes this name cannot inherit it.
                    updatedGrant = new GrantRecord { User = normalized, UserId = lockedUser.Id, Scope = scope, Privileges = newMask };
                    
                    await SetKeyLockedAsync(
                        tx, 
                        GrantKey(normalized, scopeKey),
                        MetaJsonSerializer.Serialize(updatedGrant, MetaJsonContext.Default.GrantRecord), 
                        ifAbsent: false
                    ).ConfigureAwait(false);
                }

                newGen = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            if (noChange)
                return;

            usersByName[normalized] = updatedUser!;
            ConcurrentDictionary<string, GrantRecord> map = GetOrCreateGrantMap(normalized);
            if (grantDeleted)
                map.TryRemove(scopeKey, out _);
            else
                map[scopeKey] = updatedGrant!;

            AdoptGeneration(newGen);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>Number of users currently known (cache read; reconciles in cluster mode).</summary>
    public async Task<int> UserCountAsync()
    {
        await EnsureCoherentAsync().ConfigureAwait(false);
        return usersByName.Count;
    }

    /// <summary>
    /// Creates the bootstrap superuser iff the catalog currently has <b>no</b> users, via a
    /// transactional create-if-absent so concurrent node startups yield exactly one winner without
    /// overwriting a password. Returns true if this call created it. When any user already exists it is
    /// a no-op returning false — the operator's own accounts are never overwritten by the bootstrap
    /// secret.
    /// </summary>
    public async Task<bool> TryBootstrapSuperuserAsync(string name, Credential credential)
    {
        ArgumentNullException.ThrowIfNull(credential);
        string normalized = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        
        try
        {
            if (isClusterMode)
                await RevalidateFromKvLockedAsync(await ReadGenerationAsync().ConfigureAwait(false)).ConfigureAwait(false);

            if (usersByName.Count > 0)
                return false;

            UserRecord record = new()
            {
                Id = ObjectIdGenerator.Generate().ToString(),
                Name = name,
                Credential = credential,
                IsSuperuser = true,
                CreatedAt = DateTime.UtcNow,
            };
            
            byte[] bytes = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.UserRecord);

            bool created = false;
            long newGen = 0;
            
            await RunInTransactionAsync(async tx =>
            {
                created = await WriteAuthKey(tx, UserKey(normalized), bytes, ifAbsent: true).ConfigureAwait(false);
                if (created)
                    newGen = await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);

            if (created)
            {
                usersByName[normalized] = record;
                AdoptGeneration(newGen);
            }
            
            return created;
        }
        finally
        {
            writeSem.Release();
        }
    }

    // -----------------------------------------------------------------------
    // Sessions (opaque bearer tokens; looked up on demand, not cached in memory)
    // -----------------------------------------------------------------------

    /// <summary>Persists a login session under its token id. Fails if the id somehow already exists.</summary>
    public async Task CreateSessionAsync(SessionRecord session)
    {
        ArgumentNullException.ThrowIfNull(session);
        
        byte[] bytes = MetaJsonSerializer.Serialize(session, MetaJsonContext.Default.SessionRecord);
        
        await RunInTransactionAsync(async tx =>
        {
            bool written = await WriteAuthKey(tx, SessionKey(session.TokenId), bytes, ifAbsent: true).ConfigureAwait(false);
            if (!written)
                throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, "Session token id collision");
            
        }).ConfigureAwait(false);
    }

    /// <summary>Reads a session directly from KV by token id (no cache) — a token issued on another node
    /// is visible immediately. Returns null when absent.</summary>
    public async Task<SessionRecord?> TryGetSessionAsync(string tokenId)
    {
        byte[]? value = await GetAuthValueAsync(SessionKey(tokenId)).ConfigureAwait(false);
        return value is null ? null : MetaJsonSerializer.Deserialize(value, MetaJsonContext.Default.SessionRecord);
    }

    /// <summary>Deletes a session (logout / revoke). Idempotent.</summary>
    public async Task DeleteSessionAsync(string tokenId)
    {
        await RunInTransactionAsync(tx => DeleteAuthKey(tx, SessionKey(tokenId))).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes every session whose absolute expiry is at or before <paramref name="now"/>, and returns
    /// how many were removed.
    ///
    /// <para>Sessions carry no storage TTL, and the only other deletions are an explicit logout and a
    /// drop of the owning user. With a short token lifetime and re-login as the only refresh, a client
    /// that reconnects on a timer leaves one dead key per reconnection, permanently. The cost is not
    /// only storage: <see cref="ScanUserSessionKeysAsync"/> reads and deserializes this whole family, so
    /// dead sessions make dropping an unrelated user slower for as long as the deployment lives.</para>
    ///
    /// <para><b>Safe to run concurrently on every node.</b> A record is deleted only after its own
    /// expiry has passed, at which point no path can still authenticate it, and the delete is idempotent
    /// — so two nodes sweeping at once, or one sweeping twice, produce the same result as one. That is
    /// why this needs no leader election.</para>
    ///
    /// <para>The scan is taken first and the deletes follow it, rather than deleting inside the
    /// iteration, because a locked delete against the range being scanned would contend with the scan's
    /// own read of it.</para>
    /// </summary>
    /// <param name="now">The instant to judge expiry against; a record expiring exactly now is reaped.</param>
    public async Task<int> ReapExpiredSessionsAsync(DateTime now)
    {
        List<string> expired = [];

        ConfiguredCancelableAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> cursor = kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, 
            AuthBucket, 
            null, 
            true, 
            null, 
            true, 
            1000,
            HLCTimestamp.Zero, 
            KeyValueDurability.Persistent, 
            CancellationToken.None
        ).ConfigureAwait(false);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in cursor)
        {
            if (entry.Value is null || !key.StartsWith(SessionKeyPrefix, StringComparison.Ordinal))
                continue;

            SessionRecord session = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.SessionRecord);
            if (session.ExpiresAt <= now)
                expired.Add(key);
        }

        int reaped = 0;

        foreach (string key in expired)
        {
            // One transaction per key so a single contended record cannot fail the whole sweep. The
            // next sweep retries whatever this one could not take.
            try
            {
                await RunInTransactionAsync(tx => DeleteAuthKey(tx, key)).ConfigureAwait(false);
                reaped++;
            }
            catch (CamusDBException)
            {
                // Left for the next sweep: the record is already expired and cannot authenticate.
            }
        }

        return reaped;
    }

    private async Task<byte[]?> GetAuthValueAsync(string key)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, 
            key, 
            -1,
            HLCTimestamp.Zero, 
            KeyValueDurability.Persistent, 
            CancellationToken.None
        ).ConfigureAwait(false);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? entry.Value : null;
    }

    // -----------------------------------------------------------------------
    // KV helpers (mirror DatabaseRegistry's lock+set / lock+delete pattern)
    // -----------------------------------------------------------------------

    /// <summary>
    /// <b>Test-only.</b> Writes a grant record straight to its key, bypassing <see cref="GrantAsync"/>.
    /// It exists so a test can construct states the public API deliberately cannot produce: a grant
    /// that outlived its owner's <c>DROP USER</c>, and one written before
    /// <see cref="GrantRecord.UserId"/> existed. Production code must go through
    /// <see cref="GrantAsync"/>, which reads the user record under its lock and derives the binding
    /// from it rather than trusting a caller-supplied id.
    /// </summary>
    internal async Task WriteRawGrantForTestingAsync(GrantRecord grant)
    {
        ArgumentNullException.ThrowIfNull(grant);

        string key = GrantKey(Normalize(grant.User), grant.Scope.ScopeKey());

        await writeSem.WaitAsync().ConfigureAwait(false);
        
        try
        {
            await RunInTransactionAsync(async tx =>
            {
                await LockAndReadAsync(tx, key).ConfigureAwait(false);
                
                await SetKeyLockedAsync(
                    tx, 
                    key,
                    MetaJsonSerializer.Serialize(grant, MetaJsonContext.Default.GrantRecord),
                    ifAbsent: false
                ).ConfigureAwait(false);

                await IncrementGenerationLockedAsync(tx).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }
        finally
        {
            writeSem.Release();
        }
    }

    private async Task RunInTransactionAsync(Func<KvTransaction, Task> body)
    {
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, 
            CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        
        try
        {
            await body(tx).ConfigureAwait(false);
            await transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        catch
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Acquires the exclusive lock on <paramref name="key"/> and returns its current value read <b>under
    /// that lock</b> within the transaction, or null if absent. This is the authoritative read for a
    /// read-modify-write: the value cannot change between this read and a subsequent locked write, so
    /// two nodes cannot lose each other's update and a dropped record cannot be resurrected from a stale
    /// pre-lock copy.
    /// </summary>
    private async Task<byte[]?> LockAndReadAsync(KvTransaction tx, string key)
    {
        await AcquireKeyLock(tx, key).ConfigureAwait(false);

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, 
            key, 
            -1,
            HLCTimestamp.Zero, 
            KeyValueDurability.Persistent, 
            CancellationToken.None
        ).ConfigureAwait(false);

        return getType == KeyValueResponseType.Get && entry?.Value is not null ? entry.Value : null;
    }

    /// <summary>Scans the authoritative grant keys for <paramref name="normalizedUser"/> within the
    /// transaction. Callers hold the user-key lock (which grant mutations also take), so the set is
    /// stable for the duration of a drop.</summary>
    private async Task<List<string>> ScanUserGrantKeysAsync(KvTransaction tx, string normalizedUser)
    {
        string prefix = $"{GrantKeyPrefix}{normalizedUser}:";
        List<string> keys = [];

        ConfiguredCancelableAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> cursor = kahuna.LocateAndScanRange(
            tx.TransactionId,
            AuthBucket,
            null,
            true,
            null,
            true,
            1000,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None
        ).ConfigureAwait(false);

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in cursor)
        {
            if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                keys.Add(key);
        }

        return keys;
    }

    /// <summary>Scans the session keys belonging to <paramref name="normalizedUser"/> so a drop can
    /// remove them. Best-effort: a session that survives a race is already unusable (its user record is
    /// gone, so token resolution fails), this just prevents the storage/metadata leak.
    ///
    /// <para>An already-expired record is skipped rather than added to the drop. It cannot authenticate
    /// anyone, and <see cref="ReapExpiredSessionsAsync"/> owns removing it — so between sweeps this path
    /// does not pay to delete keys that are on their way out anyway.</para></summary>
    private async Task<List<string>> ScanUserSessionKeysAsync(KvTransaction tx, string normalizedUser)
    {
        DateTime now = DateTime.UtcNow;
        List<string> keys = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId, AuthBucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (entry.Value is null || !key.StartsWith(SessionKeyPrefix, StringComparison.Ordinal))
                continue;

            SessionRecord s = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.SessionRecord);
            if (s.ExpiresAt <= now)
                continue;

            if (Normalize(s.User) == normalizedUser)
                keys.Add(key);
        }

        return keys;
    }

    private async Task<bool> WriteAuthKey(KvTransaction tx, string key, byte[] value, bool ifAbsent)
    {
        await AcquireKeyLock(tx, key).ConfigureAwait(false);
        return await SetKeyLockedAsync(tx, key, value, ifAbsent).ConfigureAwait(false);
    }

    /// <summary>Sets a key whose lock is <b>already held</b> by this transaction (via
    /// <see cref="LockAndReadAsync"/> or <see cref="AcquireKeyLock"/>). Does not re-acquire the lock.</summary>
    private async Task<bool> SetKeyLockedAsync(KvTransaction tx, string key, byte[] value, bool ifAbsent)
    {
        KeyValueFlags flags = ifAbsent ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;
        KeyValueResponseType setType;
        int setRetries = 0;
        TransactionOperationId setOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (setRetries > 0)
                await Task.Delay(setRetries * 10).ConfigureAwait(false);

            (setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, value, null, -1,
                flags, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: setOperationId).ConfigureAwait(false);
        }
        while (setType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++setRetries < MaxRetries);

        if (setType == KeyValueResponseType.NotSet)
            return false; // ifAbsent=true and key already exists

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to write auth key '{key}': {setType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
        return true;
    }

    private async Task DeleteAuthKey(KvTransaction tx, string key)
    {
        await AcquireKeyLock(tx, key).ConfigureAwait(false);
        await DeleteKeyLockedAsync(tx, key).ConfigureAwait(false);
    }

    /// <summary>Deletes a key whose lock is <b>already held</b> by this transaction. Does not re-acquire.</summary>
    private async Task DeleteKeyLockedAsync(KvTransaction tx, string key)
    {
        KeyValueResponseType deleteType;
        int deleteRetries = 0;
        TransactionOperationId deleteOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (deleteRetries > 0)
                await Task.Delay(deleteRetries * 10).ConfigureAwait(false);

            (deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                tx.TransactionId, key,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: deleteOperationId).ConfigureAwait(false);
        }
        while (deleteType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++deleteRetries < MaxRetries);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to delete auth key '{key}': {deleteType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    private async Task AcquireKeyLock(KvTransaction tx, string key)
    {
        KeyValueResponseType lockType;
        int lockRetries = 0;
        TransactionOperationId lockOperationId = TransactionOperationId.NewRandom();

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
                KeyValueDurability.Persistent, CancellationToken.None,
                coordinatorKey: tx.CoordinatorKey, operationId: lockOperationId).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to lock auth key '{key}': {lockType}");
    }
}
