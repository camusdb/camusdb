
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Persistent registry that maps database names to stable opaque ids.
///
/// Backed by a reserved <c>_system/</c> key prefix in the single process-level shared
/// Kahuna node. Both standalone and cluster modes use the same shared node.
///
/// <para>Every database gets one <see cref="DatabaseRegistryEntry"/> persisted under a
/// name key (<c>dbregistry/db:{name}</c>) holding the full entry. The id→name direction
/// is served from the in-memory <c>byId</c> cache, rebuilt from the name entries on load,
/// so no separate persisted reverse key is needed.</para>
///
/// <para>Thread safety: a single <see cref="SemaphoreSlim"/> serialises all mutating
/// operations.  Read-only queries (<see cref="TryResolveId"/>, <see cref="Get"/>,
/// <see cref="GetById"/>, <see cref="List"/>) read the in-memory cache lock-free.</para>
/// </summary>
public sealed class DatabaseRegistry : IAsyncDisposable
{
    private readonly IKahuna kahuna;
    private readonly KvTransactionsManager transactions;
    private readonly string keyPrefix;

    private readonly SemaphoreSlim writeSem = new(1, 1);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DatabaseRegistryEntry> byId = new(StringComparer.Ordinal);

    private static readonly HashSet<string> ReservedNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "_system",
        "information_schema",
    };

    private const int MaxRetries = 10;

    // Database names are normalised to lowercase at the registry boundary so that the
    // storage key, the in-memory cache, and the filesystem path are all consistent.
    // This mirrors the SQL parser's treatment of unquoted identifiers (table/column names
    // are lower-cased by the grammar's ToLowerInvariant productions).  HTTP callers that
    // send mixed-case names are silently folded — no separate "Foo" / "foo" databases.
    private static string Normalize(string name) => name.ToLowerInvariant();

    private string RegistryBucket => $"{keyPrefix}dbregistry";
    private string NameKeyPrefix => $"{keyPrefix}dbregistry/db:";
    private string NameKey(string name) => $"{keyPrefix}dbregistry/db:{name}";
    private string SequenceKey => $"{keyPrefix}dbregistry/seq";

    private DatabaseRegistry(
        IKahuna kahuna,
        KvTransactionsManager transactions,
        string keyPrefix)
    {
        this.kahuna = kahuna;
        this.transactions = transactions;
        this.keyPrefix = keyPrefix;
    }

    // -----------------------------------------------------------------------
    // Id allocation — compact base62 from a persistent monotonic sequence
    // -----------------------------------------------------------------------

    /// <summary>
    /// Allocates the next database id from the persistent monotonic counter stored in the
    /// shared node's sequence (<c>dbregistry/seq</c> or <c>_system/dbregistry/seq</c> in
    /// cluster mode). The counter only ever moves forward — ids are never reused even after
    /// a DROP, so a recycled name gets a strictly higher id than the dropped database.
    /// The id is returned as a short base-62 string.
    /// </summary>
    public async Task<string> AllocateIdAsync()
    {
        string seqName = SequenceKey;

        // Ensure the sequence exists (idempotent — AlreadyExists is fine)
        (SequenceResponseType createType, _) = await kahuna.LocateAndCreateSequence(
            seqName, initialValue: 0, increment: 1, maxValue: null,
            SequenceDurability.Persistent, CancellationToken.None
        ).ConfigureAwait(false);

        if (createType is not (SequenceResponseType.Success or SequenceResponseType.AlreadyExists))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to ensure database id sequence: {createType}");

        // Advance the counter atomically — cluster-safe across all nodes
        SequenceResponseType nextType;
        SequenceAllocation allocation;
        int retries = 0;
        do
        {
            if (retries > 0)
                await Task.Delay(retries * 10).ConfigureAwait(false);

            (nextType, allocation) = await kahuna.LocateAndNextSequenceValue(
                seqName, null, SequenceDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (nextType == SequenceResponseType.MustRetry && ++retries < MaxRetries);

        if (nextType != SequenceResponseType.Success)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to allocate database id: {nextType}");

        return ToBase62(allocation.Start);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as a base-62 string using the alphabet
    /// <c>0–9 A–Z a–z</c>. The output is the shortest representation with no leading zeros
    /// (value 1 → "1", value 62 → "A0"). Always at least one character.
    /// </summary>
    internal static string ToBase62(long value)
    {
        const string Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        if (value <= 0)
            return "0";

        Span<char> buf = stackalloc char[11]; // ceil(log₆₂(long.MaxValue)) ≤ 11
        int pos = 11;
        while (value > 0)
        {
            buf[--pos] = Alphabet[(int)(value % 62)];
            value /= 62;
        }
        return new string(buf[pos..]);
    }

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    /// <summary>
    /// Opens (or creates) the database registry against the process-level shared Kahuna node.
    /// Registry keys are namespaced under <c>_system/</c> in the shared keyspace.
    /// </summary>
    public static async Task<DatabaseRegistry> OpenAsync(EmbeddedKahuna sharedNode)
    {
        ArgumentNullException.ThrowIfNull(sharedNode);

        Func<HLCTimestamp?, HLCTimestamp> mintLocalT = (floor) =>
        {
            if (floor.HasValue && !floor.Value.IsNull())
                return sharedNode.Raft.HybridLogicalClock.ReceiveEvent(sharedNode.Raft.GetLocalNodeId(), floor.Value);
            return sharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode.Raft.GetLocalNodeId());
        };

        KvTransactionsManager txManager = new(sharedNode.Kahuna, mintLocalT);
        DatabaseRegistry registry = new(sharedNode.Kahuna, txManager, "_system/");
        await registry.LoadAsync().ConfigureAwait(false);
        return registry;
    }

    // -----------------------------------------------------------------------
    // Startup load
    // -----------------------------------------------------------------------

    // byId is rebuilt entirely from the db:{name} entries — each entry carries its Id.
    // There is no separate persisted id→name key; the in-memory byId is authoritative.
    private async Task LoadAsync()
    {
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            string namePrefix = NameKeyPrefix;

            await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
                tx.TransactionId,
                RegistryBucket,
                null, true,
                null, true,
                1000,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None).ConfigureAwait(false))
            {
                if (!key.StartsWith(namePrefix, StringComparison.Ordinal) || entry.Value is null)
                    continue;

                DatabaseRegistryEntry loaded = MetaJsonSerializer.Deserialize(
                    entry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

                byName[loaded.Name] = loaded;
                byId[loaded.Id] = loaded;
            }
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    // -----------------------------------------------------------------------
    // Read-only queries (lock-free, in-memory cache + live-KV fallback)
    // -----------------------------------------------------------------------

    public bool TryResolveId(string name, out string id)
    {
        if (byName.TryGetValue(Normalize(name), out DatabaseRegistryEntry? entry))
        {
            id = entry.Id;
            return true;
        }

        id = "";
        return false;
    }

    /// <summary>
    /// Async variant: checks the in-memory cache first, then falls back to a live Kahuna
    /// read when the name is absent.  Required for multi-node clusters where a database
    /// created on another node has been written to the shared Raft-replicated store but
    /// has not yet been seen by this node's in-memory cache (which is only populated at
    /// <see cref="OpenAsync"/> time).
    /// </summary>
    public async Task<string?> TryResolveIdAsync(string name)
    {
        name = Normalize(name);

        if (byName.TryGetValue(name, out DatabaseRegistryEntry? cached))
            return cached.Id;

        // Cache miss — try a live point-read from the persistent KV store.
        KvTransaction tx = await transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
        ).ConfigureAwait(false);
        try
        {
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? kvEntry) =
                await kahuna.LocateAndTryGetValue(
                    tx.TransactionId, NameKey(name), -1,
                    HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None
                ).ConfigureAwait(false);

            if (getType != KeyValueResponseType.Get || kvEntry?.Value is null)
                return null;

            DatabaseRegistryEntry entry = MetaJsonSerializer.Deserialize(
                kvEntry.Value, MetaJsonContext.Default.DatabaseRegistryEntry);

            // Backfill the local cache so subsequent reads are fast.
            byName[entry.Name] = entry;
            byId[entry.Id] = entry;
            return entry.Id;
        }
        finally
        {
            await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    public DatabaseRegistryEntry? Get(string name) =>
        byName.TryGetValue(Normalize(name), out DatabaseRegistryEntry? e) ? e : null;

    public DatabaseRegistryEntry? GetById(string id) =>
        byId.TryGetValue(id, out DatabaseRegistryEntry? e) ? e : null;

    public IReadOnlyList<DatabaseRegistryEntry> List() => [.. byName.Values];

    // -----------------------------------------------------------------------
    // Mutations (serialised by writeSem)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Atomically registers <paramref name="name"/> → <paramref name="id"/> in the
    /// persistent store and the in-memory cache.
    /// </summary>
    /// <exception cref="CamusDBException">
    ///   <c>DatabaseAlreadyExists</c> if the name is already registered or reserved.
    /// </exception>
    public async Task<DatabaseRegistryEntry> RegisterAsync(string name, string id)
    {
        name = Normalize(name);

        if (ReservedNames.Contains(name))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{name}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (byName.ContainsKey(name))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{name}' is already registered");

            DatabaseRegistryEntry entry = new()
            {
                Id = id,
                Name = name,
                CreatedAt = DateTime.UtcNow
            };

            byte[] entryBytes = MetaJsonSerializer.Serialize(entry, MetaJsonContext.Default.DatabaseRegistryEntry);

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                // ifAbsent=true: write only when key is currently absent (SetIfNotExists).
                // If another node races to register the same name and commits first, this
                // returns false — throw DatabaseAlreadyExists rather than silently overwriting
                // the winning node's entry and splitting the namespace into two id-based spaces.
                bool written = await WriteRegistryKey(tx, NameKey(name), entryBytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database '{name}' is already registered");
                }
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName[name] = entry;
            byId[id] = entry;
            return entry;
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Removes the registry entry for <paramref name="name"/> from both the persistent
    /// store and the in-memory cache. No-op if the name is not registered.
    /// </summary>
    public async Task UnregisterAsync(string name)
    {
        name = Normalize(name);

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!byName.TryGetValue(name, out DatabaseRegistryEntry? entry))
                return;

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                await DeleteRegistryKey(tx, NameKey(name)).ConfigureAwait(false);
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName.TryRemove(name, out _);
            byId.TryRemove(entry.Id, out _);
        }
        finally
        {
            writeSem.Release();
        }
    }

    /// <summary>
    /// Renames <paramref name="oldName"/> to <paramref name="newName"/> atomically.
    /// The id is preserved; only the name entry changes.
    /// </summary>
    /// <exception cref="CamusDBException">
    ///   <c>DatabaseDoesntExist</c> if <paramref name="oldName"/> is not registered;
    ///   <c>DatabaseAlreadyExists</c> if <paramref name="newName"/> is already taken or reserved.
    /// </exception>
    public async Task RenameAsync(string oldName, string newName)
    {
        oldName = Normalize(oldName);
        newName = Normalize(newName);

        if (ReservedNames.Contains(newName))
            throw new CamusDBException(
                CamusDBErrorCodes.DatabaseNameReserved,
                $"'{newName}' is a reserved database name");

        await writeSem.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!byName.TryGetValue(oldName, out DatabaseRegistryEntry? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseDoesntExist,
                    $"Database '{oldName}' is not registered");

            if (byName.ContainsKey(newName))
                throw new CamusDBException(
                    CamusDBErrorCodes.DatabaseAlreadyExists,
                    $"Database '{newName}' is already registered");

            DatabaseRegistryEntry updated = new()
            {
                Id = existing.Id,
                Name = newName,
                CreatedAt = existing.CreatedAt
            };

            byte[] updatedBytes = MetaJsonSerializer.Serialize(updated, MetaJsonContext.Default.DatabaseRegistryEntry);

            KvTransaction tx = await transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            try
            {
                // ifAbsent=true: protect against a concurrent node registering newName between
                // our local byName check above and the KV commit.
                bool written = await WriteRegistryKey(tx, NameKey(newName), updatedBytes, ifAbsent: true).ConfigureAwait(false);
                if (!written)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw new CamusDBException(
                        CamusDBErrorCodes.DatabaseAlreadyExists,
                        $"Database '{newName}' is already registered");
                }
                await DeleteRegistryKey(tx, NameKey(oldName)).ConfigureAwait(false);
                await transactions.CommitAsync(tx).ConfigureAwait(false);
            }
            catch
            {
                await transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }

            byName.TryRemove(oldName, out _);
            byName[newName] = updated;
            byId[existing.Id] = updated;
        }
        finally
        {
            writeSem.Release();
        }
    }

    // -----------------------------------------------------------------------
    // KV helpers — mirror CatalogsManager.WriteMetaKey / DeleteMetaKey
    // -----------------------------------------------------------------------

    /// <summary>
    /// Writes <paramref name="value"/> to <paramref name="key"/> within the supplied transaction.
    /// When <paramref name="ifAbsent"/> is <c>true</c>, uses <see cref="KeyValueFlags.SetIfNotExists"/>:
    /// the write succeeds only when the key is currently absent.  Returns <c>true</c> if the key
    /// was written, <c>false</c> if the key already existed (only possible when <paramref name="ifAbsent"/>
    /// is <c>true</c>; always returns <c>true</c> for plain writes).
    /// </summary>
    private async Task<bool> WriteRegistryKey(KvTransaction tx, string key, byte[] value, bool ifAbsent = false)
    {
        KeyValueResponseType lockType;
        KeyValueDurability lockDurability;
        int lockRetries = 0;

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to lock registry key '{key}': {lockType}");

        tx.TrackLock(key, lockDurability);

        KeyValueFlags flags = ifAbsent ? KeyValueFlags.SetIfNotExists : KeyValueFlags.Set;
        KeyValueResponseType setType;
        int setRetries = 0;

        do
        {
            if (setRetries > 0)
                await Task.Delay(setRetries * 10).ConfigureAwait(false);

            (setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, key, value, null, -1,
                flags, 0,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (setType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++setRetries < MaxRetries);

        // NotSet is only returned when ifAbsent=true and the key already exists — not an error.
        if (setType == KeyValueResponseType.NotSet)
            return false;

        if (setType != KeyValueResponseType.Set)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to write registry key '{key}': {setType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
        return true;
    }

    private async Task DeleteRegistryKey(KvTransaction tx, string key)
    {
        KeyValueResponseType lockType;
        KeyValueDurability lockDurability;
        int lockRetries = 0;

        do
        {
            if (lockRetries > 0)
                await Task.Delay(lockRetries * 10).ConfigureAwait(false);

            (lockType, _, lockDurability, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                tx.TransactionId, key, 0,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (lockType is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.MustRetry
               && ++lockRetries < MaxRetries);

        if (lockType != KeyValueResponseType.Locked)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to lock registry key '{key}': {lockType}");

        tx.TrackLock(key, lockDurability);

        KeyValueResponseType deleteType;
        int deleteRetries = 0;

        do
        {
            if (deleteRetries > 0)
                await Task.Delay(deleteRetries * 10).ConfigureAwait(false);

            (deleteType, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                tx.TransactionId, key,
                KeyValueDurability.Persistent, CancellationToken.None
            ).ConfigureAwait(false);
        }
        while (deleteType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
               && ++deleteRetries < MaxRetries);

        if (deleteType is not (KeyValueResponseType.Deleted or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Failed to delete registry key '{key}': {deleteType}");

        tx.TrackModified(key, KeyValueDurability.Persistent);
    }

    // -----------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        // Roll back any transaction still active on the system store while the node is alive,
        // then dispose the transactions manager so its range-lock heartbeat loops are cancelled.
        // An undisposed manager keeps a heartbeat loop awaiting forever, which roots the manager
        // and the system Kahuna node it references — leaking a whole node per registry instance.
        try
        {
            await transactions.RollbackAllActiveAsync().ConfigureAwait(false);
        }
        catch
        {
            // best-effort: transactions.Dispose() below still cancels any remaining loops
        }

        transactions.Dispose();
        writeSem.Dispose();
    }
}
