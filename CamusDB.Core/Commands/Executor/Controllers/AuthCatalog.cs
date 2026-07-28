/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
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
    private string GenerationSequenceKey => $"{keyPrefix}auth/generation";

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
    public static async Task<AuthCatalog> OpenAsync(EmbeddedKahuna sharedNode, bool isClusterMode = false)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(sharedNode.Kahuna, mintLocalT);
        AuthCatalog catalog = new(sharedNode.Kahuna, txManager, "_system/", isClusterMode);

        await sharedNode.WaitUntilStartedAsync().ConfigureAwait(false);
        await catalog.LoadAsync().ConfigureAwait(false);
        return catalog;
    }

    // -----------------------------------------------------------------------
    // Startup load
    // -----------------------------------------------------------------------

    private async Task LoadAsync()
    {
        Stopwatch sw = Stopwatch.StartNew();
        const int retryDelayMs = 200;
        const int maxWaitMs = 30_000;

        while (true)
        {
            try
            {
                await LoadOnceAsync().ConfigureAwait(false);
                Volatile.Write(ref loadedGeneration, await ReadGenerationAsync().ConfigureAwait(false));
                return;
            }
            catch (RaftException) when (sw.ElapsedMilliseconds < maxWaitMs)
            {
                await Task.Delay(retryDelayMs).ConfigureAwait(false);
            }
        }
    }

    private async Task LoadOnceAsync()
    {
        usersByName.Clear();
        grantsByUser.Clear();

        KvTransaction tx = KvTransaction.CreateReadOnly();

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId, AuthBucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            if (key.StartsWith(UserKeyPrefix, StringComparison.Ordinal))
            {
                UserRecord user = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.UserRecord);
                usersByName[Normalize(user.Name)] = user;
            }
            else if (key.StartsWith(GrantKeyPrefix, StringComparison.Ordinal))
            {
                GrantRecord grant = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.GrantRecord);
                GetOrCreateGrantMap(Normalize(grant.User))[grant.Scope.ScopeKey()] = grant;
            }
        }
    }

    private ConcurrentDictionary<string, GrantRecord> GetOrCreateGrantMap(string normalizedUser) =>
        grantsByUser.GetOrAdd(normalizedUser, static _ => new ConcurrentDictionary<string, GrantRecord>(StringComparer.Ordinal));

    // -----------------------------------------------------------------------
    // Cross-node cache-coherence generation stamp (mirrors DatabaseRegistry)
    // -----------------------------------------------------------------------

    private async Task<long> ReadGenerationAsync()
    {
        (SequenceResponseType type, ReadOnlySequenceEntry? entry) = await kahuna.LocateAndGetSequence(
            GenerationSequenceKey, SequenceDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

        return type == SequenceResponseType.Success && entry is not null ? entry.CurrentValue : 0;
    }

    private async Task<long> BumpGenerationAsync()
    {
        (SequenceResponseType createType, _) = await kahuna.LocateAndCreateSequence(
            GenerationSequenceKey, initialValue: 0, increment: 1, maxValue: null,
            SequenceDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

        if (createType is SequenceResponseType.Success or SequenceResponseType.AlreadyExists)
        {
            SequenceResponseType nextType;
            SequenceAllocation allocation = default;
            int retries = 0;
            do
            {
                if (retries > 0)
                    await Task.Delay(retries * 10).ConfigureAwait(false);

                (nextType, allocation) = await kahuna.LocateAndNextSequenceValue(
                    GenerationSequenceKey, null, SequenceDurability.Persistent, CancellationToken.None).ConfigureAwait(false);
            }
            while (nextType == SequenceResponseType.MustRetry && ++retries < MaxRetries);

            if (nextType == SequenceResponseType.Success)
                return allocation.Start;
        }

        return Volatile.Read(ref loadedGeneration);
    }

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

        // Reconcile in place — upsert present keys and remove vanished ones — rather than clearing
        // first, so a concurrent lock-free reader never observes a transiently empty cache.
        HashSet<string> presentUsers = new(StringComparer.Ordinal);
        HashSet<string> presentGrants = new(StringComparer.Ordinal);

        KvTransaction tx = KvTransaction.CreateReadOnly();

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            tx.TransactionId, AuthBucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (entry.Value is null)
                continue;

            if (key.StartsWith(UserKeyPrefix, StringComparison.Ordinal))
            {
                UserRecord user = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.UserRecord);
                string normalized = Normalize(user.Name);
                usersByName[normalized] = user;
                presentUsers.Add(normalized);
            }
            else if (key.StartsWith(GrantKeyPrefix, StringComparison.Ordinal))
            {
                GrantRecord grant = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.GrantRecord);
                string normalizedUser = Normalize(grant.User);
                string scopeKey = grant.Scope.ScopeKey();
                GetOrCreateGrantMap(normalizedUser)[scopeKey] = grant;
                presentGrants.Add($"{normalizedUser}/{scopeKey}");
            }
        }

        foreach (string normalized in usersByName.Keys.ToList())
        {
            if (!presentUsers.Contains(normalized))
            {
                usersByName.TryRemove(normalized, out _);
                grantsByUser.TryRemove(normalized, out _);
            }
        }

        foreach ((string user, ConcurrentDictionary<string, GrantRecord> map) in grantsByUser)
        {
            foreach (string scopeKey in map.Keys.ToList())
            {
                if (!presentGrants.Contains($"{user}/{scopeKey}"))
                    map.TryRemove(scopeKey, out _);
            }
        }

        Volatile.Write(ref loadedGeneration, authoritativeGeneration);
    }

    // -----------------------------------------------------------------------
    // Read-only queries
    // -----------------------------------------------------------------------

    /// <summary>Returns the user record for <paramref name="name"/>, or null. Cache read; in cluster
    /// mode reconciles first so another node's just-created user is visible.</summary>
    public async Task<UserRecord?> TryGetUserAsync(string name)
    {
        await EnsureCoherentAsync().ConfigureAwait(false);
        return usersByName.TryGetValue(Normalize(name), out UserRecord? user) ? user : null;
    }

    /// <summary>Returns every grant for <paramref name="name"/> (empty if none / unknown user).</summary>
    public async Task<IReadOnlyList<GrantRecord>> ListGrantsAsync(string name)
    {
        await EnsureCoherentAsync().ConfigureAwait(false);
        return grantsByUser.TryGetValue(Normalize(name), out ConcurrentDictionary<string, GrantRecord>? map)
            ? [.. map.Values]
            : [];
    }

    // -----------------------------------------------------------------------
    // Mutations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Creates a user. Throws <see cref="CamusDBErrorCodes.UserAlreadyExists"/> if the name is taken,
    /// unless <paramref name="ifNotExists"/> (then a no-op). <paramref name="credential"/> is null for a
    /// passwordless user.
    /// </summary>
    public async Task CreateUserAsync(string name, Credential? credential, bool ifNotExists)
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
                Name = name,
                Credential = credential,
                CredentialEpoch = 0,
                AuthorizationEpoch = 0,
                CreatedAt = DateTime.UtcNow,
            };

            byte[] bytes = MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.UserRecord);

            await RunInTransactionAsync(async tx =>
            {
                bool written = await WriteAuthKey(tx, UserKey(normalized), bytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                    throw new CamusDBException(CamusDBErrorCodes.UserAlreadyExists, $"User '{name}' already exists");
            }).ConfigureAwait(false);

            usersByName[normalized] = record;
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
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
            if (isClusterMode)
                await RevalidateFromKvLockedAsync(await ReadGenerationAsync().ConfigureAwait(false)).ConfigureAwait(false);

            if (!usersByName.TryGetValue(normalized, out UserRecord? existing))
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{name}' does not exist");

            UserRecord updated = existing.Copy();
            updated.Credential = credential;
            updated.CredentialEpoch = existing.CredentialEpoch + 1;

            byte[] bytes = MetaJsonSerializer.Serialize(updated, MetaJsonContext.Default.UserRecord);

            await RunInTransactionAsync(tx => WriteAuthKeyExpectPresent(tx, UserKey(normalized), bytes)).ConfigureAwait(false);

            usersByName[normalized] = updated;
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
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
            if (isClusterMode)
                await RevalidateFromKvLockedAsync(await ReadGenerationAsync().ConfigureAwait(false)).ConfigureAwait(false);

            if (!usersByName.ContainsKey(normalized))
            {
                if (ifExists)
                    return;
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{name}' does not exist");
            }

            List<string> grantKeys = grantsByUser.TryGetValue(normalized, out ConcurrentDictionary<string, GrantRecord>? map)
                ? [.. map.Keys]
                : [];

            await RunInTransactionAsync(async tx =>
            {
                await DeleteAuthKey(tx, UserKey(normalized)).ConfigureAwait(false);
                foreach (string scopeKey in grantKeys)
                    await DeleteAuthKey(tx, GrantKey(normalized, scopeKey)).ConfigureAwait(false);
            }).ConfigureAwait(false);

            usersByName.TryRemove(normalized, out _);
            grantsByUser.TryRemove(normalized, out _);
            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
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
            if (isClusterMode)
                await RevalidateFromKvLockedAsync(await ReadGenerationAsync().ConfigureAwait(false)).ConfigureAwait(false);

            if (!usersByName.TryGetValue(normalized, out UserRecord? existingUser))
                throw new CamusDBException(CamusDBErrorCodes.UserDoesNotExist, $"User '{user}' does not exist");

            ConcurrentDictionary<string, GrantRecord> map = GetOrCreateGrantMap(normalized);
            map.TryGetValue(scopeKey, out GrantRecord? current);
            Privilege currentMask = current?.Privileges ?? Privilege.None;
            Privilege newMask = revoke ? currentMask & ~privileges : currentMask | privileges;

            if (newMask == currentMask)
                return; // no observable change — nothing to persist

            UserRecord updatedUser = existingUser.Copy();
            updatedUser.AuthorizationEpoch = existingUser.AuthorizationEpoch + 1;
            byte[] userBytes = MetaJsonSerializer.Serialize(updatedUser, MetaJsonContext.Default.UserRecord);

            GrantRecord? updatedGrant = newMask == Privilege.None ? null : new GrantRecord
            {
                User = normalized,
                Scope = scope,
                Privileges = newMask,
            };
            byte[]? grantBytes = updatedGrant is null
                ? null
                : MetaJsonSerializer.Serialize(updatedGrant, MetaJsonContext.Default.GrantRecord);

            await RunInTransactionAsync(async tx =>
            {
                await WriteAuthKeyExpectPresent(tx, UserKey(normalized), userBytes).ConfigureAwait(false);
                if (grantBytes is null)
                    await DeleteAuthKey(tx, GrantKey(normalized, scopeKey)).ConfigureAwait(false);
                else
                    await WriteAuthKey(tx, GrantKey(normalized, scopeKey), grantBytes, ifAbsent: false).ConfigureAwait(false);
            }).ConfigureAwait(false);

            usersByName[normalized] = updatedUser;
            if (updatedGrant is null)
                map.TryRemove(scopeKey, out _);
            else
                map[scopeKey] = updatedGrant;

            AdoptGeneration(await BumpGenerationAsync().ConfigureAwait(false));
        }
        finally
        {
            writeSem.Release();
        }
    }

    // -----------------------------------------------------------------------
    // KV helpers (mirror DatabaseRegistry's lock+set / lock+delete pattern)
    // -----------------------------------------------------------------------

    private async Task RunInTransactionAsync(Func<KvTransaction, Task> body)
    {
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
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

    private Task WriteAuthKeyExpectPresent(KvTransaction tx, string key, byte[] value) =>
        WriteAuthKey(tx, key, value, ifAbsent: false);

    private async Task<bool> WriteAuthKey(KvTransaction tx, string key, byte[] value, bool ifAbsent)
    {
        await AcquireKeyLock(tx, key).ConfigureAwait(false);

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
